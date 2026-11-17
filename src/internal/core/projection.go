package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

var ErrPolicyMismatch = errors.New("Feed deployment configuration differs")

// IndexPolicy identifies the frozen projection policy across all processes.
func IndexPolicy(mode string, authors []int64) string {
	unique := make(map[int64]bool)
	if mode == "hybrid" {
		for _, id := range authors {
			unique[id] = true
		}
	}
	ids := make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, strconv.FormatInt(id, 10))
	}
	sort.Strings(ids)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(mode+":"+strings.Join(ids, ","))))
}

// ConfigureIndex freezes snapshot policy before the service starts accepting RPCs.
func (s *Service) ConfigureIndex(mode string, authors []int64, prefix string) {
	s.policy = IndexPolicy(mode, authors)
	s.prefix = prefix
	s.pullAuthors = nil
	if mode == "hybrid" {
		seen := make(map[int64]bool)
		for _, id := range authors {
			if !seen[id] {
				s.pullAuthors = append(s.pullAuthors, id)
				seen[id] = true
			}
		}
		sort.Slice(s.pullAuthors, func(i, j int) bool { return s.pullAuthors[i] < s.pullAuthors[j] })
	}
}

func (s *Service) CheckReady(ctx context.Context) (*corepb.Deployment, error) {
	if err := s.db.PingContext(ctx); err != nil {
		return nil, err
	}
	return &corepb.Deployment{PolicyId: s.policy, QueuePrefix: s.prefix}, nil
}

// GetPostState includes soft-deleted rows so workers can reconcile current state.
func (s *Service) GetPostState(ctx context.Context, id int64) (*corepb.PostState, error) {
	if id <= 0 {
		return nil, post.ErrInvalid
	}
	item := &corepb.PostState{}
	var deleted sql.NullInt64
	err := s.query(ctx, func(ctx context.Context) error {
		return s.db.QueryRowContext(ctx, "SELECT id,author_id,created_on,deleted_on FROM posts WHERE id=?", id).
			Scan(&item.PostId, &item.AuthorId, &item.CreatedOn, &deleted)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, post.ErrNotFound
	}
	item.Deleted = deleted.Valid
	return item, err
}

// ListFollowers returns one bounded page. The caller persists this exact page's
// recipients and continuation together before publishing any child tasks.
func (s *Service) ListFollowers(ctx context.Context, author int64, after *corepb.FollowerCursor) (*corepb.FollowerPage, error) {
	if author <= 0 || (after != nil && (after.CreatedOn <= 0 || after.RelationId <= 0)) {
		return nil, post.ErrInvalid
	}
	query := "SELECT user_id,id,created_on FROM following WHERE follow_id=?"
	args := []any{author}
	if after != nil {
		query += " AND (created_on<? OR (created_on=? AND id<?))"
		args = append(args, after.CreatedOn, after.CreatedOn, after.RelationId)
	}
	query += " ORDER BY created_on DESC,id DESC LIMIT 501"
	page, err := s.followers(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if len(page.Items) > 500 {
		page.HasMore = true
		page.Items = page.Items[:500]
		last := page.Items[len(page.Items)-1]
		page.Next = &corepb.FollowerCursor{CreatedOn: last.CreatedOn, RelationId: last.RelationId}
	}
	return page, nil
}

func (s *Service) CheckRelations(ctx context.Context, author int64, ids []int64) (*corepb.FollowerPage, error) {
	if author <= 0 || len(ids) == 0 || len(ids) > 100 {
		return nil, post.ErrInvalid
	}
	args := []any{author}
	for _, id := range ids {
		if id < 0 {
			return nil, post.ErrInvalid
		}
		args = append(args, id)
	}
	query := "SELECT user_id,id,created_on FROM following WHERE follow_id=? AND id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")"
	return s.followers(ctx, query, args)
}

func (s *Service) followers(ctx context.Context, query string, args []any) (*corepb.FollowerPage, error) {
	page := &corepb.FollowerPage{}
	err := s.query(ctx, func(ctx context.Context) error {
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			item := &corepb.Follower{}
			if err := rows.Scan(&item.UserId, &item.RelationId, &item.CreatedOn); err != nil {
				return err
			}
			page.Items = append(page.Items, item)
		}
		return rows.Err()
	})
	return page, err
}

// ReadIndexSnapshot uses Core's frozen push/pull policy and at most 501 rows.
// Redis generation/revision/token validation remains with the rebuilder.
func (s *Service) ReadIndexSnapshot(ctx context.Context, owner int64, author bool, policy string) (*corepb.IndexSnapshot, error) {
	if owner <= 0 {
		return nil, post.ErrInvalid
	}
	if policy != s.policy {
		return nil, ErrPolicyMismatch
	}
	query := `SELECT p.id,p.created_on,0 FROM posts p WHERE p.deleted_on IS NULL AND p.author_id=?`
	args := []any{owner}
	if !author {
		query = `SELECT p.id,p.created_on,CASE WHEN p.author_id=? THEN 0 ELSE f.id END
 FROM posts p LEFT JOIN following f ON f.user_id=? AND f.follow_id=p.author_id
 WHERE p.deleted_on IS NULL AND (p.author_id=? OR f.id IS NOT NULL)`
		args = []any{owner, owner, owner}
		if len(s.pullAuthors) > 0 {
			query += " AND p.author_id NOT IN (" + strings.TrimSuffix(strings.Repeat("?,", len(s.pullAuthors)), ",") + ")"
			for _, id := range s.pullAuthors {
				args = append(args, id)
			}
		}
	}
	query += " ORDER BY p.created_on DESC,p.id DESC LIMIT 501"
	result := &corepb.IndexSnapshot{}
	err := s.query(ctx, func(ctx context.Context) error {
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			item := &corepb.SnapshotEntry{}
			if err := rows.Scan(&item.PostId, &item.CreatedOn, &item.RelationId); err != nil {
				return err
			}
			result.Items = append(result.Items, item)
		}
		return rows.Err()
	})
	return result, err
}
