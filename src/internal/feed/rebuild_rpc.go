package feed

import (
	"context"
	"errors"
	"strconv"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/core"
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	transport "github.com/SCPZ24/NoteX/src/internal/platform/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RebuildClient struct {
	rpc corepb.FeedRebuildServiceClient
}

// DialRebuilder creates the reusable connection owned by its caller.
func DialRebuilder(ctx context.Context, address, secret string) (*RebuildClient, *grpc.ClientConn, error) {
	conn, err := transport.Dial(ctx, address, secret)
	if err != nil {
		return nil, nil, err
	}
	return &RebuildClient{rpc: corepb.NewFeedRebuildServiceClient(conn)}, conn, nil
}

func (c *RebuildClient) CheckReady(ctx context.Context) (*corepb.Deployment, error) {
	return c.rpc.CheckReady(ctx, &corepb.CheckReadyRequest{})
}

func (c *RebuildClient) ensure(ctx context.Context, source cacheSource, policy, prefix string) error {
	_, err := c.rpc.EnsureIndex(ctx, &corepb.EnsureIndexRequest{
		OwnerId: source.owner, AuthorIndex: source.author, Signature: source.signature,
		Generation: source.state.generation, Revision: source.state.revision,
		PolicyId: policy, QueuePrefix: prefix,
	})
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.Aborted:
		return cache.ErrSuperseded
	case codes.ResourceExhausted:
		return cache.ErrBusy
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.FailedPrecondition:
		return core.ErrPolicyMismatch
	default:
		return err
	}
}

type rebuildServer struct {
	corepb.UnimplementedFeedRebuildServiceServer
	service *Service
}

// StartRebuilder exposes only index reconstruction and dependency readiness.
func StartRebuilder(address, secret string, service *Service) (*transport.Server, error) {
	return transport.Start(address, secret, func(registrar grpc.ServiceRegistrar) {
		corepb.RegisterFeedRebuildServiceServer(registrar, &rebuildServer{service: service})
	})
}

func (r *rebuildServer) CheckReady(ctx context.Context, _ *corepb.CheckReadyRequest) (*corepb.Deployment, error) {
	deployment, err := r.service.authority.CheckReady(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "core_unavailable")
	}
	if deployment.PolicyId != r.service.policy || deployment.QueuePrefix != r.service.prefix {
		return nil, status.Error(codes.FailedPrecondition, "policy_mismatch")
	}
	return deployment, nil
}

func (r *rebuildServer) EnsureIndex(ctx context.Context, req *corepb.EnsureIndexRequest) (*corepb.EnsureIndexResponse, error) {
	s := r.service
	if req.PolicyId != s.policy || req.QueuePrefix != s.prefix {
		return nil, status.Error(codes.FailedPrecondition, "policy_mismatch")
	}
	if req.OwnerId <= 0 || len(req.Signature) != 64 || len(req.Generation) != 32 {
		return nil, status.Error(codes.InvalidArgument, "invalid_index_identity")
	}
	if _, err := strconv.ParseUint(req.Revision, 10, 64); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid_revision")
	}
	source := cacheSource{owner: req.OwnerId, author: req.AuthorIndex, signature: req.Signature,
		state: cacheState{generation: req.Generation, revision: req.Revision}}
	var err error
	if source.author {
		if source.signature != s.policy {
			return nil, status.Error(codes.FailedPrecondition, "policy_mismatch")
		}
		err = s.ensureAuthor(ctx, source)
	} else {
		// Different revisions must not join a stale reader snapshot. One waiter
		// canceling never cancels the shared, independently bounded build.
		key := s.cacheKeys(source)[3] + ":" + source.state.generation + ":" + source.state.revision + ":" + source.signature
		err = s.cache.Do(ctx, key, func(task context.Context) error { return s.warmSnapshot(task, source, "") })
	}
	switch {
	case err == nil:
		return &corepb.EnsureIndexResponse{}, nil
	case errors.Is(err, cache.ErrBusy):
		return nil, status.Error(codes.ResourceExhausted, "cache_busy")
	case errors.Is(err, cache.ErrSuperseded):
		return nil, status.Error(codes.Aborted, "build_superseded")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return nil, status.FromContextError(err).Err()
	default:
		return nil, status.Error(codes.Unavailable, "rebuild_unavailable")
	}
}

// NewRebuildClient adapts an application-owned discovered gRPC connection.
func NewRebuildClient(conn *grpc.ClientConn) *RebuildClient {
	return &RebuildClient{rpc: corepb.NewFeedRebuildServiceClient(conn)}
}
