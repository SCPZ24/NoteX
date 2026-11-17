package core

import (
	"context"
	"strings"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	"google.golang.org/grpc/metadata"
)

type rpcServer struct {
	corepb.UnimplementedCoreReadServiceServer
	service *Service
}

func (s *rpcServer) Authenticate(ctx context.Context, _ *corepb.AuthenticateRequest) (*corepb.User, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return nil, rpcError(account.ErrUnauthorized)
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, rpcError(account.ErrUnauthorized)
	}
	user, err := s.service.Authenticate(ctx, parts[1])
	return wireUser(user), rpcError(err)
}

func (s *rpcServer) GetUser(ctx context.Context, req *corepb.GetUserRequest) (*corepb.User, error) {
	user, err := s.service.GetUser(ctx, req.GetUserId())
	return wireUser(user), rpcError(err)
}

func (s *rpcServer) GetFeedRelations(ctx context.Context, req *corepb.GetFeedRelationsRequest) (*corepb.GetFeedRelationsResponse, error) {
	result, err := s.service.GetFeedRelations(ctx, req.GetReaderId())
	response := &corepb.GetFeedRelationsResponse{ExceedsLimit: result.ExceedsLimit}
	for _, relation := range result.Items {
		response.Relations = append(response.Relations, &corepb.Relation{RelationId: relation.ID, AuthorId: relation.AuthorID})
	}
	return response, rpcError(err)
}

func (s *rpcServer) ResolveFeedCandidates(ctx context.Context, req *corepb.ResolveFeedCandidatesRequest) (*corepb.Posts, error) {
	items, err := s.service.ResolveFeedCandidates(ctx, req.GetReaderId(), req.GetPostIds(), int(req.GetMaxResults()))
	return &corepb.Posts{Items: wirePosts(items)}, rpcError(err)
}

func (s *rpcServer) ResolveAuthorCandidates(ctx context.Context, req *corepb.ResolveAuthorCandidatesRequest) (*corepb.Posts, error) {
	items, err := s.service.ResolveAuthorCandidates(ctx, req.GetAuthorId(), req.GetPostIds(), int(req.GetMaxResults()))
	return &corepb.Posts{Items: wirePosts(items)}, rpcError(err)
}

func (s *rpcServer) ReadFeedPage(ctx context.Context, req *corepb.ReadFeedPageRequest) (*corepb.PostPage, error) {
	page, err := s.service.ReadFeedPage(ctx, req.GetReaderId(), req.GetCursor(), int(req.GetPageSize()))
	return wirePage(page), rpcError(err)
}

func (s *rpcServer) ReadAuthorPage(ctx context.Context, req *corepb.ReadAuthorPageRequest) (*corepb.PostPage, error) {
	page, err := s.service.ReadAuthorPage(ctx, req.GetAuthorId(), req.GetCursor(), int(req.GetPageSize()))
	return wirePage(page), rpcError(err)
}

func (s *rpcServer) GetPostState(ctx context.Context, req *corepb.GetPostStateRequest) (*corepb.PostState, error) {
	result, err := s.service.GetPostState(ctx, req.GetPostId())
	return result, rpcError(err)
}
func (s *rpcServer) ListFollowers(ctx context.Context, req *corepb.ListFollowersRequest) (*corepb.FollowerPage, error) {
	result, err := s.service.ListFollowers(ctx, req.GetAuthorId(), req.GetAfter())
	return result, rpcError(err)
}
func (s *rpcServer) CheckRelations(ctx context.Context, req *corepb.CheckRelationsRequest) (*corepb.FollowerPage, error) {
	result, err := s.service.CheckRelations(ctx, req.GetAuthorId(), req.GetRelationIds())
	return result, rpcError(err)
}
func (s *rpcServer) ReadIndexSnapshot(ctx context.Context, req *corepb.ReadIndexSnapshotRequest) (*corepb.IndexSnapshot, error) {
	result, err := s.service.ReadIndexSnapshot(ctx, req.GetOwnerId(), req.GetAuthorIndex(), req.GetPolicyId())
	return result, rpcError(err)
}
func (s *rpcServer) CheckReady(ctx context.Context, _ *corepb.CheckReadyRequest) (*corepb.Deployment, error) {
	result, err := s.service.CheckReady(ctx)
	return result, rpcError(err)
}
