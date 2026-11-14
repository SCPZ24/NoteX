package follow

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	ErrInvalid      = errors.New("invalid relationship or page size")
	ErrUserNotFound = errors.New("user not found")
	ErrBusy         = errors.New("relationship write contention")
)

const (
	Following = "following"
	Followers = "followers"
)

type Item struct {
	ID        int64  `json:"id"`
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	CreatedOn int64  `json:"created_on"`
}

type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

type Service struct{ db *sql.DB }

func New(db *sql.DB) *Service { return &Service{db: db} }

func (s *Service) Follow(ctx context.Context, actor, target int64) error {
	if actor <= 0 || target <= 0 || actor == target {
		return ErrInvalid
	}
	// The unique index arbitrates concurrent inserts. A retry leaves the original
	// relationship ID and creation time unchanged. Foreign keys check both users.
	err := s.execRelationship(ctx,
		"INSERT INTO following (user_id, follow_id, created_on) VALUES (?, ?, ?)",
		actor, target, time.Now().Unix())
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062: // The only supplied unique value is the user pair; ID is generated.
			return nil
		case 1452:
			return ErrUserNotFound
		}
	}
	return err
}

func (s *Service) Unfollow(ctx context.Context, actor, target int64) error {
	if actor <= 0 || target <= 0 || actor == target {
		return ErrInvalid
	}
	err := s.execRelationship(ctx,
		"DELETE FROM following WHERE user_id = ? AND follow_id = ?", actor, target)
	return err
}

// Each call executes one autocommit statement. InnoDB rolls back the statement's
// transaction on deadlock (1213), so that specific failure can be retried safely.
func (s *Service) execRelationship(ctx context.Context, query string, args ...any) error {
	for attempt := 0; attempt < 5; attempt++ {
		_, err := s.db.ExecContext(ctx, query, args...)
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1213 {
			return err
		}
		if attempt == 4 {
			return ErrBusy
		}
		timer := time.NewTimer(5 * time.Millisecond * time.Duration(1<<attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrBusy
}

func (s *Service) IsFollowing(ctx context.Context, actor, target int64) (bool, error) {
	if actor <= 0 || target <= 0 {
		return false, ErrInvalid
	}
	if err := s.requireUser(ctx, target); err != nil {
		return false, err
	}
	var exists bool
	err := s.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM following WHERE user_id = ? AND follow_id = ?)", actor, target).
		Scan(&exists)
	return exists, err
}

func (s *Service) requireUser(ctx context.Context, id int64) error {
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)", id).
		Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrUserNotFound
	}
	return nil
}

func (s *Service) List(ctx context.Context, owner int64, direction, after string, limit int) (Page, error) {
	page := Page{Items: make([]Item, 0)}
	if owner <= 0 || limit < 1 || limit > 100 {
		return page, ErrInvalid
	}
	// Column names come exclusively from this whitelist, never from request text.
	var ownerColumn, otherColumn string
	switch direction {
	case Following:
		ownerColumn, otherColumn = "user_id", "follow_id"
	case Followers:
		ownerColumn, otherColumn = "follow_id", "user_id"
	default:
		return page, ErrInvalid
	}
	query := `SELECT f.id, u.id, u.username, f.created_on
		FROM following f JOIN users u ON u.id = f.` + otherColumn +
		` WHERE f.` + ownerColumn + ` = ?`
	args := []any{owner}
	if after != "" {
		boundary, err := decodeCursor(after, owner, direction)
		if err != nil {
			return page, err
		}
		query += " AND (f.created_on < ? OR (f.created_on = ? AND f.id < ?))"
		args = append(args, boundary.CreatedOn, boundary.CreatedOn, boundary.ID)
	}
	if err := s.requireUser(ctx, owner); err != nil {
		return page, err
	}
	query += " ORDER BY f.created_on DESC, f.id DESC LIMIT ?"
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.ID, &item.UserID, &item.Username, &item.CreatedOn); err != nil {
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
		page.NextCursor, err = encodeCursor(cursor{Owner: owner, Direction: direction, CreatedOn: last.CreatedOn, ID: last.ID})
		if err != nil {
			return page, err
		}
	}
	return page, nil
}
