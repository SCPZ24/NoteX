package core

import (
	"context"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"google.golang.org/grpc/metadata"
)

// Client translates the wire contract into the existing domain types.
type Client struct{ rpc corepb.CoreReadServiceClient }

var _ Reader = (*Client)(nil)
var _ Reader = (*Service)(nil)

func (c *Client) Authenticate(ctx context.Context, token string) (account.User, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+token)
	user, err := c.rpc.Authenticate(metadata.NewOutgoingContext(ctx, md), &corepb.AuthenticateRequest{})
	return localUser(user), localError(err)
}

func (c *Client) GetUser(ctx context.Context, id int64) (account.User, error) {
	user, err := c.rpc.GetUser(ctx, &corepb.GetUserRequest{UserId: id})
	return localUser(user), localError(err)
}

func (c *Client) GetFeedRelations(ctx context.Context, reader int64) (Relations, error) {
	response, err := c.rpc.GetFeedRelations(ctx, &corepb.GetFeedRelationsRequest{ReaderId: reader})
	result := Relations{Items: make([]Relation, 0, len(response.GetRelations())), ExceedsLimit: response.GetExceedsLimit()}
	for _, relation := range response.GetRelations() {
		result.Items = append(result.Items, Relation{ID: relation.GetRelationId(), AuthorID: relation.GetAuthorId()})
	}
	return result, localError(err)
}

func (c *Client) ResolveFeedCandidates(ctx context.Context, reader int64, ids []int64, limit int) ([]post.Post, error) {
	if limit < 1 || limit > 101 {
		return nil, post.ErrInvalid
	}
	response, err := c.rpc.ResolveFeedCandidates(ctx, &corepb.ResolveFeedCandidatesRequest{ReaderId: reader, PostIds: ids, MaxResults: int32(limit)})
	return localPosts(response.GetItems()), localError(err)
}

func (c *Client) ResolveAuthorCandidates(ctx context.Context, author int64, ids []int64, limit int) ([]post.Post, error) {
	if limit < 1 || limit > 101 {
		return nil, post.ErrInvalid
	}
	response, err := c.rpc.ResolveAuthorCandidates(ctx, &corepb.ResolveAuthorCandidatesRequest{AuthorId: author, PostIds: ids, MaxResults: int32(limit)})
	return localPosts(response.GetItems()), localError(err)
}

func (c *Client) ReadFeedPage(ctx context.Context, reader int64, cursor string, limit int) (post.Page, error) {
	if limit < 1 || limit > 100 {
		return post.Page{}, post.ErrInvalid
	}
	response, err := c.rpc.ReadFeedPage(ctx, &corepb.ReadFeedPageRequest{ReaderId: reader, Cursor: cursor, PageSize: int32(limit)})
	return localPage(response), localError(err)
}

func (c *Client) ReadAuthorPage(ctx context.Context, author int64, cursor string, limit int) (post.Page, error) {
	if limit < 1 || limit > 100 {
		return post.Page{}, post.ErrInvalid
	}
	response, err := c.rpc.ReadAuthorPage(ctx, &corepb.ReadAuthorPageRequest{AuthorId: author, Cursor: cursor, PageSize: int32(limit)})
	return localPage(response), localError(err)
}

// Feed and List adapt the SQL baseline to the existing HTTP FeedReader contract.
func (c *Client) Feed(ctx context.Context, reader int64, cursor string, limit int) (post.Page, error) {
	return c.ReadFeedPage(ctx, reader, cursor, limit)
}

func (c *Client) List(ctx context.Context, author int64, cursor string, limit int) (post.Page, error) {
	return c.ReadAuthorPage(ctx, author, cursor, limit)
}

func (c *Client) GetPostState(ctx context.Context, id int64) (*corepb.PostState, error) {
	result, err := c.rpc.GetPostState(ctx, &corepb.GetPostStateRequest{PostId: id})
	return result, localError(err)
}
func (c *Client) ListFollowers(ctx context.Context, author int64, after *corepb.FollowerCursor) (*corepb.FollowerPage, error) {
	result, err := c.rpc.ListFollowers(ctx, &corepb.ListFollowersRequest{AuthorId: author, After: after})
	return result, localError(err)
}
func (c *Client) CheckRelations(ctx context.Context, author int64, ids []int64) (*corepb.FollowerPage, error) {
	result, err := c.rpc.CheckRelations(ctx, &corepb.CheckRelationsRequest{AuthorId: author, RelationIds: ids})
	return result, localError(err)
}
func (c *Client) ReadIndexSnapshot(ctx context.Context, owner int64, author bool, policy string) (*corepb.IndexSnapshot, error) {
	result, err := c.rpc.ReadIndexSnapshot(ctx, &corepb.ReadIndexSnapshotRequest{OwnerId: owner, AuthorIndex: author, PolicyId: policy})
	return result, localError(err)
}
func (c *Client) CheckReady(ctx context.Context) (*corepb.Deployment, error) {
	result, err := c.rpc.CheckReady(ctx, &corepb.CheckReadyRequest{})
	return result, localError(err)
}
