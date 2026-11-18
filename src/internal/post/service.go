package post

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/go-sql-driver/mysql"
)

var (
	ErrInvalid         = errors.New("invalid post input or page size")
	ErrNotFound        = errors.New("post not found")
	ErrUserNotFound    = errors.New("user not found")
	ErrForbidden       = errors.New("only the author can delete a post")
	ErrRequestConflict = errors.New("publication key already used with different content")
)

var requestKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

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

type Service struct {
	selfPages     atomic.Uint64
	selfPageNanos atomic.Uint64
	feedPages     atomic.Uint64
	feedPageNanos atomic.Uint64
	adaptiveLikes bool
	warmAuthor    func(context.Context, int64) error
	db            *sql.DB
	wake          chan<- struct{}
	cache         *cache.Store
}

func (s *Service) UseAdaptiveLikes(warm func(context.Context, int64) error) {
	s.adaptiveLikes = true
	s.warmAuthor = warm
}

func New(db *sql.DB, wake chan<- struct{}) *Service { return &Service{db: db, wake: wake} }

func (s *Service) Create(ctx context.Context, actor int64, requestKey, content string) (Post, error) {
	content = strings.TrimSpace(content)
	if actor <= 0 || !requestKeyPattern.MatchString(requestKey) || !utf8.ValidString(content) || content == "" || utf8.RuneCountInString(content) > 500 {
		return Post{}, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Post{}, err
	}
	defer tx.Rollback()

	createdOn := time.Now().Unix()
	// The unique key arbitrates concurrent requests. All writes use this Tx,
	// so the reservation is committed only with the post and its saved result.
	_, err = tx.ExecContext(ctx,
		"INSERT INTO post_requests (user_id, request_key, content, created_on) VALUES (?, ?, ?, ?)",
		actor, requestKey, content, createdOn)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062:
			if err := tx.Rollback(); err != nil {
				return Post{}, err
			}
			// Read outside the losing transaction to see the winner's committed
			// result, independently of any REPEATABLE READ snapshot.
			return s.replay(ctx, actor, requestKey, content)
		case 1452:
			return Post{}, ErrUserNotFound
		}
	}
	if err != nil {
		return Post{}, err
	}
	var author Author
	err = tx.QueryRowContext(ctx, "SELECT id, username FROM users WHERE id = ?", actor).
		Scan(&author.ID, &author.Username)
	if errors.Is(err, sql.ErrNoRows) {
		return Post{}, ErrUserNotFound
	}
	if err != nil {
		return Post{}, err
	}
	item := Post{Author: author, Content: content, CreatedOn: createdOn}
	result, err := tx.ExecContext(ctx,
		"INSERT INTO posts (author_id, content, created_on) VALUES (?, ?, ?)",
		actor, item.Content, item.CreatedOn)
	if err != nil {
		return Post{}, err
	}
	item.ID, err = result.LastInsertId()
	if err != nil {
		return Post{}, err
	}
	response, err := json.Marshal(item)
	if err != nil {
		return Post{}, err
	}
	_, err = tx.ExecContext(ctx,
		"UPDATE post_requests SET post_id = ?, response_json = ? WHERE user_id = ? AND request_key = ?",
		item.ID, string(response), actor, requestKey)
	if err != nil {
		return Post{}, err
	}
	if err := outbox.RecordPost(ctx, tx, outbox.PostCreated, item.ID, createdOn); err != nil {
		return Post{}, err
	}
	// A lost COMMIT reply can leave the outcome unknown to Go. Return the error
	// and let the client retry with the same key to discover the durable result.
	if err := tx.Commit(); err != nil {
		return Post{}, err
	}
	outbox.Notify(s.wake)
	return item, nil
}

func (s *Service) replay(ctx context.Context, actor int64, requestKey, content string) (Post, error) {
	var savedContent string
	var response []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT content, response_json FROM post_requests WHERE user_id = ? AND request_key = ?",
		actor, requestKey).Scan(&savedContent, &response)
	if err != nil {
		return Post{}, err
	}
	if savedContent != content {
		return Post{}, ErrRequestConflict
	}
	var item Post
	if err := json.Unmarshal(response, &item); err != nil {
		return Post{}, err
	}
	return item, nil
}

func (s *Service) Get(ctx context.Context, id int64) (Post, error) {
	if id <= 0 {
		return Post{}, ErrInvalid
	}
	if s.cache != nil {
		return s.cachedGet(ctx, id)
	}
	return s.getSQL(ctx, id)
}

func (s *Service) getSQL(ctx context.Context, id int64) (Post, error) {
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Include deleted posts so the author can repeat a successful deletion.
	var author int64
	err = tx.QueryRowContext(ctx, "SELECT author_id FROM posts WHERE id = ?", id).Scan(&author)
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
	deletedOn := time.Now().Unix()
	result, err := tx.ExecContext(ctx,
		"UPDATE posts SET deleted_on = ? WHERE id = ? AND author_id = ? AND deleted_on IS NULL",
		deletedOn, id, actor)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed > 0 {
		if err := outbox.RecordPost(ctx, tx, outbox.PostDeleted, id, deletedOn); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if changed > 0 {
		outbox.Notify(s.wake)
	}
	return s.InvalidateBody(ctx, id)
}

func (s *Service) List(ctx context.Context, owner int64, after string, limit int) (Page, error) {
	return s.list(ctx, owner, "user_posts", after, limit)
}

func (s *Service) Feed(ctx context.Context, actor int64, after string, limit int) (Page, error) {
	start := time.Now()
	page, err := s.list(ctx, actor, "feed", after, limit)
	s.feedPages.Add(1)
	s.feedPageNanos.Add(uint64(time.Since(start)))
	return page, err
}

// FeedPageStats returns cumulative timings for authoritative Feed pages on this service.
func (s *Service) FeedPageStats() map[string]uint64 {
	return map[string]uint64{
		"calls":      s.feedPages.Load(),
		"ns":         s.feedPageNanos.Load(),
		"self_calls": s.selfPages.Load(),
		"self_ns":    s.selfPageNanos.Load(),
	}
}

const postPageOrder = " ORDER BY p.created_on DESC, p.id DESC LIMIT ?"

// Both callers use the same ordering, row format and pagination rules. Kind is
// selected by these methods; request text never supplies the SQL predicate.
func (s *Service) list(ctx context.Context, owner int64, kind, after string, limit int) (Page, error) {
	page := Page{Items: make([]Post, 0)}
	if owner <= 0 || limit < 1 || limit > 100 {
		return page, ErrInvalid
	}
	var cursorFilter string
	var cursorArgs []any
	if after != "" {
		boundary, err := DecodeCursor(after, owner, kind)
		if err != nil {
			return page, err
		}
		cursorFilter = " AND (p.created_on < ? OR (p.created_on = ? AND p.id < ?))"
		cursorArgs = []any{boundary.CreatedOn, boundary.CreatedOn, boundary.ID}
	}
	query := `SELECT p.id,u.id,u.username,p.content,p.created_on
        FROM posts p JOIN users u ON u.id=p.author_id WHERE p.deleted_on IS NULL`
	args := []any{owner}
	if kind == "feed" {
		// Check membership and fetch a self-only page within one statement snapshot.
		// A reader with follows gets one marker row and uses the authoritative query below.
		started := time.Now()
		handled, err := s.selfFeedPage(ctx, owner, cursorFilter, cursorArgs, limit+1, &page)
		if err != nil {
			return page, err
		}
		if handled {
			result, err := finishPage(page, owner, kind, limit)
			s.selfPages.Add(1)
			s.selfPageNanos.Add(uint64(time.Since(started)))
			return result, err
		}
		query += ` AND (p.author_id = ? OR EXISTS (
            SELECT 1 FROM following f WHERE f.user_id = ? AND f.follow_id = p.author_id))`
		args = append(args, owner)
	} else {
		var exists bool
		if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)", owner).Scan(&exists); err != nil {
			return page, err
		}
		if !exists {
			return page, ErrUserNotFound
		}
		query += " AND p.author_id = ?"
	}
	query += cursorFilter + postPageOrder
	args = append(args, cursorArgs...)
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
	return finishPage(page, owner, kind, limit)
}

// selfFeedPage returns handled for every existing reader with no follows, including
// an empty page. Apply the limit inside the author index scan before the outer join.
func (s *Service) selfFeedPage(ctx context.Context, owner int64, cursorFilter string, cursorArgs []any, count int, page *Page) (bool, error) {
	query := `WITH rel AS (SELECT EXISTS(SELECT 1 FROM following WHERE user_id=?) AS has_follows)
        SELECT u.id,u.username,rel.has_follows,own.id,own.content,own.created_on
        FROM users u CROSS JOIN rel LEFT JOIN (
            SELECT p.id,p.content,p.created_on FROM posts p
            WHERE p.author_id=? AND p.deleted_on IS NULL AND NOT (SELECT has_follows FROM rel)` +
		cursorFilter + postPageOrder + `
        ) own ON TRUE WHERE u.id=? ORDER BY own.created_on DESC,own.id DESC`
	args := append([]any{owner, owner}, cursorArgs...)
	args = append(args, count, owner)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		found = true
		var author Author
		var hasFollows bool
		var id, created sql.NullInt64
		var content sql.NullString
		if err := rows.Scan(&author.ID, &author.Username, &hasFollows, &id, &content, &created); err != nil {
			return false, err
		}
		if hasFollows {
			return false, nil
		}
		if id.Valid {
			page.Items = append(page.Items, Post{ID: id.Int64, Author: author, Content: content.String, CreatedOn: created.Int64})
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if !found {
		return false, ErrUserNotFound
	}
	return true, nil
}

func finishPage(page Page, owner int64, kind string, limit int) (Page, error) {
	if len(page.Items) > limit {
		page.HasMore = true
		page.Items = page.Items[:limit]
		last := page.Items[len(page.Items)-1]
		cursor, err := EncodeCursor(Cursor{Owner: owner, Kind: kind, CreatedOn: last.CreatedOn, ID: last.ID})
		if err != nil {
			return page, err
		}
		page.NextCursor = cursor
	}
	return page, nil
}

// FeedCandidates loads a bounded set of Redis candidates against current SQL
// relationships and deletion state. It returns only authoritative visible posts.
func (s *Service) FeedCandidates(ctx context.Context, actor int64, ids []int64, limit int) ([]Post, error) {
	return s.candidates(ctx, actor, "feed", ids, limit)
}

// AuthorCandidates validates author ownership as well as deletion state.
func (s *Service) AuthorCandidates(ctx context.Context, author int64, ids []int64, limit int) ([]Post, error) {
	return s.candidates(ctx, author, "user_posts", ids, limit)
}

func (s *Service) candidates(ctx context.Context, actor int64, kind string, ids []int64, limit int) ([]Post, error) {
	if actor <= 0 || len(ids) == 0 || len(ids) > 256 || limit < 1 || limit > 101 {
		return nil, ErrInvalid
	}
	predicate := "p.author_id=?"
	args := []any{actor}
	if kind == "feed" {
		predicate = "(p.author_id=? OR EXISTS(SELECT 1 FROM following f WHERE f.user_id=? AND f.follow_id=p.author_id))"
		args = append(args, actor)
	}
	placeholders := make([]string, 0, len(ids))
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, ErrInvalid
		}
		if !seen[id] {
			seen[id] = true
			placeholders = append(placeholders, "?")
			args = append(args, id)
		}
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT p.id,u.id,u.username,p.content,p.created_on FROM posts p JOIN users u ON u.id=p.author_id
 WHERE p.deleted_on IS NULL AND `+predicate+`
 AND p.id IN (`+strings.Join(placeholders, ",")+`) ORDER BY p.created_on DESC,p.id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Post, 0, len(ids))
	for rows.Next() {
		var item Post
		if err := rows.Scan(&item.ID, &item.Author.ID, &item.Author.Username, &item.Content, &item.CreatedOn); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
