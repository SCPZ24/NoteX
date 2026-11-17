// Package rpc provides bounded, authenticated loopback gRPC transports.
package rpc

import (
	"context"
	"crypto/subtle"
	"fmt"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"log/slog"
	"net"
	"time"
)

const rpcTimeout = 5 * time.Second
const maxMessageBytes = 1 << 20

type Server struct {
	Done    <-chan error
	Address string
	server  *grpc.Server
}

// Start registers services before accepting work. Plaintext transport is limited
// to this host; configuration validates literal loopback IP addresses.
func Start(address, secret string, register func(grpc.ServiceRegistrar)) (*Server, error) {
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	if len(secret) < 32 {
		return nil, fmt.Errorf("RPC_TOKEN requires at least 32 characters")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.UnaryInterceptor(serverInterceptor(secret)))
	register(server)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	return &Server{Done: done, Address: listener.Addr().String(), server: server}, nil
}

// Dial reuses one connection and waits at most five seconds for transport readiness.
func Dial(ctx context.Context, address, secret string) (*grpc.ClientConn, error) {
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	return dial(ctx, "passthrough:///"+address, secret, nil)
}

// DialDiscovered uses a connection-scoped etcd directory and round-robin RPCs.
func DialDiscovered(ctx context.Context, secret string, builder resolver.Builder) (*grpc.ClientConn, error) {
	return dial(ctx, builder.Scheme()+":///service", secret, []grpc.DialOption{grpc.WithResolvers(builder), grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`)})
}

func dial(ctx context.Context, target, secret string, extra []grpc.DialOption) (*grpc.ClientConn, error) {
	if len(secret) < 32 {
		return nil, fmt.Errorf("RPC_TOKEN requires at least 32 characters")
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)),
		grpc.WithUnaryInterceptor(clientInterceptor(secret))}
	conn, err := grpc.NewClient(target, append(options, extra...)...)
	if err != nil {
		return nil, err
	}
	startup, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(startup, state) {
			conn.Close()
			return nil, fmt.Errorf("RPC startup: %w", startup.Err())
		}
	}
}

// ValidateAddress restricts unencrypted RPCs to literal loopback addresses.
func ValidateAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("RPC address must use a literal loopback IP and port")
	}
	return nil
}

// Close stops new admission, drains for five seconds, then cancels remaining calls.
func (s *Server) Close() {
	drained := make(chan struct{})
	go func() { s.server.GracefulStop(); close(drained) }()
	timer := time.NewTimer(rpcTimeout)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		s.server.Stop()
		<-drained
	}
}

func clientInterceptor(secret string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Set("x-notex-service-token", secret)
		return invoke(metadata.NewOutgoingContext(ctx, md), method, req, reply, conn, opts...)
	}
}

func serverInterceptor(secret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		tokens := md.Get("x-notex-service-token")
		if len(tokens) != 1 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(secret)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid_service_identity")
		}
		ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		if trace := md.Get("x-request-id"); len(trace) == 1 {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", trace[0])
		}
		response, err := handler(ctx, req)
		if err != nil {
			// Never log request bodies or authentication metadata.
			slog.Warn("Core RPC failed", "method", info.FullMethod, "code", status.Code(err).String(), "request_id", md.Get("x-request-id"))
		}
		return response, err
	}
}
