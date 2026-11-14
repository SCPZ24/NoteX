package post

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid      = errors.New("invalid post input or page size")
	ErrNotFound     = errors.New("post not found")
	ErrUserNotFound = errors.New("user not found")
	ErrForbidden    = errors.New("only the author can delete a post")
)

type Author struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type Post struct {
	ID        int64  `json:"id"`
	Author    Author `json:"author"`
	Content   string `json:"content"`
	CreatedOn int64  `json:"created_on"`
}

type Page struct {
	Items      []Post `json:"items"`
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

type Service struct{ db *sql.DB }

func New(db *sql.DB) *Service { return &Service{db: db} }

func (s *Service) Create(ctx context.Context, actor int64, content string) (Post, error) {
	content = strings.TrimSpace(content)
	if actor <= 0 || !utf8.ValidString(content) || content == "" || utf8.RuneCountInString(content) > 500 {
		return Post{}, ErrInvalid
	}
	var author Author
	err := s.db.QueryRowContext(ctx, "SELECT id, username FROM users WHERE id = ?", actor).
		Scan(&author.ID, &author.Username)
	if errors.Is(err, sql.ErrNoRows) {
		return Post{}, ErrUserNotFound
	}
	if err != nil {
		return Post{}, err
	}
	item := Post{Author: author, Content: content, CreatedOn: time.Now().Unix()}
	result, err := s.db.ExecContext(ctx,
		"INSERT INTO posts (author_id, content, created_on) VALUES (?, ?, ?)",
		actor, item.Content, item.CreatedOn)
	if err != nil {
		return Post{}, err
	}
	item.ID, err = result.LastInsertId()
	return item, err
}

func (s *Service) Get(ctx context.Context, id int64) (Post, error) {
	if id <= 0 {
		return Post{}, ErrInvalid
	}
	var item Post
	err := s.db.QueryRowContext(ctx, `SELECT p.id, u.id, u.username, p.content, p.created_on
		FROM posts p JOIN users u ON u.id = p.author_id
		WHERE p.id = ? AND p.deleted_on IS NULL`, id).
		Scan(&item.ID, &item.Author.ID, &item.Author.Username, &item.Content, &item.CreatedOn)
	if errors.Is(err, sql.ErrNoRows) {
		return Post{}, ErrNotFound
	}
	return item, err
}

func (s *Service) Delete(ctx context.Context, actor, id int64) error {
	if actor <= 0 || id <= 0 {
		return ErrInvalid
	}
	// Include deleted posts so the author can repeat a successful deletion.
	var author int64
	err := s.db.QueryRowContext(ctx, "SELECT author_id FROM posts WHERE id = ?", id).Scan(&author)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if actor != author {
		return ErrForbidden
	}
	// The author is immutable. The conditional update also preserves the first
	// deletion timestamp when multiple delete requests execute concurrently.
	_, err = s.db.ExecContext(ctx,
		"UPDATE posts SET deleted_on = ? WHERE id = ? AND author_id = ? AND deleted_on IS NULL",
		time.Now().Unix(), id, actor)
	return err
}

func (s *Service) List(ctx context.Context, owner int64, after string, limit int) (Page, error) {
	return s.list(ctx, owner, "user_posts", after, limit)
}

func (s *Service) Feed(ctx context.Context, actor int64, after string, limit int) (Page, error) {
	return s.list(ctx, actor, "feed", after, limit)
}

// Both callers use the same ordering, row format and pagination rules. Kind is
// selected by these methods; request text never supplies the SQL predicate.
func (s *Service) list(ctx context.Context, owner int64, kind, after string, limit int) (Page, error) {
	page := Page{Items: make([]Post, 0)}
	if owner <= 0 || limit < 1 || limit > 100 {
		return page, ErrInvalid
	}
	query := `SELECT p.id, u.id, u.username, p.content, p.created_on
		FROM posts p JOIN users u ON u.id = p.author_id
		WHERE p.deleted_on IS NULL`
	var args []any
	if kind == "feed" {
		// Filter against authoritative relationships before LIMIT. EXISTS keeps a
		// post as a single row even when it qualifies through more than one source.
		query += ` AND (p.author_id = ? OR EXISTS (
			SELECT 1 FROM following f WHERE f.user_id = ? AND f.follow_id = p.author_id))`
		args = []any{owner, owner}
	} else {
		query += " AND p.author_id = ?"
		args = []any{owner}
	}
	if after != "" {
		boundary, err := decodeCursor(after, owner, kind)
		if err != nil {
			return page, err
		}
		query += " AND (p.created_on < ? OR (p.created_on = ? AND p.id < ?))"
		args = append(args, boundary.CreatedOn, boundary.CreatedOn, boundary.ID)
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)", owner).
		Scan(&exists); err != nil {
		return page, err
	}
	if !exists {
		return page, ErrUserNotFound
	}
	query += " ORDER BY p.created_on DESC, p.id DESC LIMIT ?"
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var item Post
		if err := rows.Scan(&item.ID, &item.Author.ID, &item.Author.Username, &item.Content, &item.CreatedOn); err != nil {
			return page, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	if len(page.Items) > limit {
		page.HasMore = true
		page.Items = page.Items[:limit]
		last := page.Items[len(page.Items)-1]
		page.NextCursor, err = encodeCursor(cursor{Owner: owner, Kind: kind, CreatedOn: last.CreatedOn, ID: last.ID})
		if err != nil {
			return page, err
		}
	}
	return page, nil
}
