package core

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrUnavailable = errors.New("core read service unavailable")

// Status messages are stable machine-readable reasons, never raw SQL errors.
func rpcError(err error) error {
	var networkError net.Error
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request_canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request_timeout")
	case errors.Is(err, ErrPolicyMismatch):
		return status.Error(codes.FailedPrecondition, "policy_mismatch")
	case errors.Is(err, post.ErrNotFound):
		return status.Error(codes.NotFound, "post_not_found")
	case errors.Is(err, post.ErrInvalidCursor):
		return status.Error(codes.InvalidArgument, "invalid_cursor")
	case errors.Is(err, post.ErrInvalid), errors.Is(err, account.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid_input")
	case errors.Is(err, account.ErrUnauthorized):
		return status.Error(codes.Unauthenticated, "unauthorized")
	case errors.Is(err, post.ErrUserNotFound), errors.Is(err, account.ErrNotFound):
		return status.Error(codes.NotFound, "user_not_found")
	case errors.Is(err, cache.ErrBusy):
		return status.Error(codes.ResourceExhausted, "cache_busy")
	case errors.Is(err, driver.ErrBadConn), errors.Is(err, sql.ErrConnDone), errors.As(err, &networkError):
		return status.Error(codes.Unavailable, "core_unavailable")
	default:
		return status.Error(codes.Internal, "internal_error")
	}
}

func localError(err error) error {
	if err == nil {
		return nil
	}
	s := status.Convert(err)
	switch s.Code() {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.InvalidArgument:
		if s.Message() == "invalid_cursor" {
			return post.ErrInvalidCursor
		}
		return post.ErrInvalid
	case codes.Unauthenticated:
		return account.ErrUnauthorized
	case codes.FailedPrecondition:
		return ErrPolicyMismatch
	case codes.NotFound:
		if s.Message() == "post_not_found" {
			return post.ErrNotFound
		}
		return post.ErrUserNotFound
	case codes.ResourceExhausted:
		return cache.ErrBusy
	case codes.Unavailable:
		return ErrUnavailable
	default:
		return err
	}
}

func wireUser(user account.User) *corepb.User {
	return &corepb.User{Id: user.ID, Username: user.Username, CreatedOn: user.CreatedOn}
}

func localUser(user *corepb.User) account.User {
	return account.User{ID: user.GetId(), Username: user.GetUsername(), CreatedOn: user.GetCreatedOn()}
}

func wirePosts(items []post.Post) []*corepb.Post {
	result := make([]*corepb.Post, 0, len(items))
	for _, item := range items {
		result = append(result, &corepb.Post{Id: item.ID, Author: &corepb.Author{Id: item.Author.ID, Username: item.Author.Username}, Content: item.Content, CreatedOn: item.CreatedOn})
	}
	return result
}

func localPosts(items []*corepb.Post) []post.Post {
	result := make([]post.Post, 0, len(items))
	for _, item := range items {
		result = append(result, post.Post{ID: item.GetId(), Author: post.Author{ID: item.GetAuthor().GetId(), Username: item.GetAuthor().GetUsername()}, Content: item.GetContent(), CreatedOn: item.GetCreatedOn()})
	}
	return result
}

func wirePage(page post.Page) *corepb.PostPage {
	return &corepb.PostPage{Items: wirePosts(page.Items), NextCursor: page.NextCursor, HasMore: page.HasMore}
}

func localPage(page *corepb.PostPage) post.Page {
	return post.Page{Items: localPosts(page.GetItems()), NextCursor: page.GetNextCursor(), HasMore: page.GetHasMore()}
}
