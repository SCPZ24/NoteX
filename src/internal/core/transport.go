package core

import (
	"context"
	"crypto/rand"
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	transport "github.com/SCPZ24/NoteX/src/internal/platform/rpc"
	"google.golang.org/grpc"
)

// Local preserves the single-process baseline with the same remote transport.
type Local struct {
	Client *Client
	Done   <-chan error
	server *transport.Server
	conn   *grpc.ClientConn
}

func StartLocal(ctx context.Context, service *Service) (*Local, error) {
	secret := rand.Text() + rand.Text()
	server, err := Start("127.0.0.1:0", secret, service)
	if err != nil {
		return nil, err
	}
	client, conn, err := Dial(ctx, server.Address, secret)
	if err != nil {
		server.Close()
		return nil, err
	}
	return &Local{Client: client, Done: server.Done, server: server, conn: conn}, nil
}

func (l *Local) Close() { l.server.Close(); l.conn.Close() }

// Start exposes Core on its configured internal address.
func Start(address, secret string, service *Service) (*transport.Server, error) {
	return transport.Start(address, secret, func(registrar grpc.ServiceRegistrar) {
		corepb.RegisterCoreReadServiceServer(registrar, &rpcServer{service: service})
	})
}

// Dial returns the typed client and its owned reusable connection.
func Dial(ctx context.Context, address, secret string) (*Client, *grpc.ClientConn, error) {
	conn, err := transport.Dial(ctx, address, secret)
	if err != nil {
		return nil, nil, err
	}
	return &Client{rpc: corepb.NewCoreReadServiceClient(conn)}, conn, nil
}
