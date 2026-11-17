// Package core owns authoritative reads used by Feed over RPC.
package core

import (
	"context"
	"database/sql"
	"errors"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

type Relation struct {
	ID       int64
	AuthorID int64
}

type Relations struct {
	Items        []Relation
	ExceedsLimit bool
}

// Reader has local and gRPC implementations. Application startup installs the
// gRPC implementation; existing in-process callers can use Service directly.
type Reader interface {
	Authenticate(context.Context, string) (account.User, error)
	GetUser(context.Context, int64) (account.User, error)
	GetFeedRelations(context.Context, int64) (Relations, error)
	ResolveFeedCandidates(context.Context, int64, []int64, int) ([]post.Post, error)
	ResolveAuthorCandidates(context.Context, int64, []int64, int) ([]post.Post, error)
	ReadFeedPage(context.Context, int64, string, int) (post.Page, error)
	ReadAuthorPage(context.Context, int64, string, int) (post.Page, error)
	GetPostState(context.Context, int64) (*corepb.PostState, error)
	ListFollowers(context.Context, int64, *corepb.FollowerCursor) (*corepb.FollowerPage, error)
	CheckRelations(context.Context, int64, []int64) (*corepb.FollowerPage, error)
	ReadIndexSnapshot(context.Context, int64, bool, string) (*corepb.IndexSnapshot, error)
	CheckReady(context.Context) (*corepb.Deployment, error)
}

type Service struct {
	db          *sql.DB
	accounts    *account.Service
	posts       *post.Service
	budget      *cache.Store
	policy      string
	prefix      string
	pullAuthors []int64
}

// New shares the existing cache SQL budget when caching is enabled. The caller
// owns the database and store. Authentication retains its existing pool budget.
func New(db *sql.DB, budget *cache.Store) *Service {
	return &Service{db: db, accounts: account.New(db), posts: post.New(db, nil), budget: budget}
}

func (s *Service) query(ctx context.Context, fn func(context.Context) error) error {
	if s.budget != nil {
		return s.budget.SQL(ctx, fn)
	}
	return fn(ctx)
}

func (s *Service) Authenticate(ctx context.Context, token string) (account.User, error) {
	return s.accounts.Authenticate(ctx, token)
}

func (s *Service) GetUser(ctx context.Context, id int64) (account.User, error) {
	if id <= 0 {
		return account.User{}, post.ErrInvalid
	}
	var user account.User
	err := s.query(ctx, func(ctx context.Context) error {
		return s.db.QueryRowContext(ctx, "SELECT id,username,created_on FROM users WHERE id=?", id).
			Scan(&user.ID, &user.Username, &user.CreatedOn)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return account.User{}, post.ErrUserNotFound
	}
	return user, err
}

// GetFeedRelations reads one bounded SQL result, including an existence check
// for readers with no follows. Oversized sets must use authoritative pagination.
func (s *Service) GetFeedRelations(ctx context.Context, reader int64) (Relations, error) {
	result := Relations{Items: make([]Relation, 0)}
	if reader <= 0 {
		return result, post.ErrInvalid
	}
	err := s.query(ctx, func(ctx context.Context) error {
		rows, err := s.db.QueryContext(ctx, `SELECT f.id,f.follow_id FROM users u
 LEFT JOIN following f ON f.user_id=u.id WHERE u.id=? ORDER BY f.id LIMIT 4097`, reader)
		if err != nil {
			return err
		}
		defer rows.Close()
		found := false
		for rows.Next() {
			found = true
			var id, author sql.NullInt64
			if err := rows.Scan(&id, &author); err != nil {
				return err
			}
			if id.Valid {
				result.Items = append(result.Items, Relation{ID: id.Int64, AuthorID: author.Int64})
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if !found {
			return post.ErrUserNotFound
		}
		return nil
	})
	if len(result.Items) > 4096 {
		result.Items = []Relation{}
		result.ExceedsLimit = true
	}
	return result, err
}

func (s *Service) ResolveFeedCandidates(ctx context.Context, reader int64, ids []int64, limit int) ([]post.Post, error) {
	var items []post.Post
	err := s.query(ctx, func(ctx context.Context) error {
		var err error
		items, err = s.posts.FeedCandidates(ctx, reader, ids, limit)
		return err
	})
	return items, err
}

func (s *Service) ResolveAuthorCandidates(ctx context.Context, author int64, ids []int64, limit int) ([]post.Post, error) {
	var items []post.Post
	err := s.query(ctx, func(ctx context.Context) error {
		var err error
		items, err = s.posts.AuthorCandidates(ctx, author, ids, limit)
		return err
	})
	return items, err
}

func (s *Service) ReadFeedPage(ctx context.Context, reader int64, cursor string, limit int) (post.Page, error) {
	var page post.Page
	err := s.query(ctx, func(ctx context.Context) error {
		var err error
		page, err = s.posts.Feed(ctx, reader, cursor, limit)
		return err
	})
	return page, err
}

func (s *Service) ReadAuthorPage(ctx context.Context, author int64, cursor string, limit int) (post.Page, error) {
	var page post.Page
	err := s.query(ctx, func(ctx context.Context) error {
		var err error
		page, err = s.posts.List(ctx, author, cursor, limit)
		return err
	})
	return page, err
}
