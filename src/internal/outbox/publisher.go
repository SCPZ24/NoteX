package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	pollInterval = 5 * time.Second
	batchSize    = 32
)

type event struct {
	ID        int64           `json:"event_id"`
	Type      string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedOn int64           `json:"created_on"`
	Attempts  uint64          `json:"-"`
}

// Run scans at startup, on coalesced wakeups, and on a recovery timer. All SQL
// queries finish before network publishing; no database lock spans a broker wait.
func Run(ctx context.Context, db *sql.DB, wake <-chan struct{}, url, prefix string) {
	b := &broker{url: url, prefix: prefix}
	defer b.close()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if err := drain(ctx, db, b); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("outbox scan failed", "error", err)
			// New HTTP requests cannot bypass the recovery delay during an outage.
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func drain(ctx context.Context, db *sql.DB, b *broker) error {
	for ctx.Err() == nil {
		events, err := pending(ctx, db)
		if err != nil {
			return err
		}
		for _, item := range events {
			if err := deliver(ctx, db, b, item); err != nil {
				return err
			}
		}
		if len(events) < batchSize {
			return nil
		}
	}
	return ctx.Err()
}

func pending(ctx context.Context, db *sql.DB) ([]event, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id, event_type, payload, created_on, attempts
		FROM outbox_events WHERE published_on IS NULL AND failed_on IS NULL
		AND next_attempt_on <= ? ORDER BY next_attempt_on, id LIMIT ?`, time.Now().Unix(), batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []event
	for rows.Next() {
		var item event
		if err := rows.Scan(&item.ID, &item.Type, &item.Payload, &item.CreatedOn, &item.Attempts); err != nil {
			return nil, err
		}
		events = append(events, item)
	}
	return events, rows.Err()
}

func (e event) destinations(prefix string) ([]string, error) {
	switch e.Type {
	case PostCreated, PostDeleted:
		var payload PostPayload
		if err := json.Unmarshal(e.Payload, &payload); err != nil || payload.ActorID <= 0 || payload.PostID <= 0 {
			return nil, errors.New("invalid post event payload")
		}
		return []string{prefix + ".feed", prefix + ".search"}, nil
	case FollowCreated, FollowDeleted:
		var payload FollowPayload
		if err := json.Unmarshal(e.Payload, &payload); err != nil || payload.ActorID <= 0 || payload.TargetUserID <= 0 || payload.ActorID == payload.TargetUserID {
			return nil, errors.New("invalid follow event payload")
		}
		return []string{prefix + ".feed"}, nil
	default:
		return nil, fmt.Errorf("unsupported event type %q", e.Type)
	}
}

func deliver(ctx context.Context, db *sql.DB, b *broker, item event) error {
	queues, err := item.destinations(b.prefix)
	if err != nil {
		_, saveErr := update(ctx, db, `UPDATE outbox_events SET failed_on = ?, last_error = ?
			WHERE id = ? AND published_on IS NULL AND failed_on IS NULL`, time.Now().Unix(), err.Error(), item.ID)
		return saveErr
	}
	body, err := json.Marshal(item)
	if err != nil {
		return err
	}
	result, err := update(ctx, db, `UPDATE outbox_events SET attempts = attempts + 1
		WHERE id = ? AND published_on IS NULL AND failed_on IS NULL`, item.ID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed == 0 {
		return err
	}
	err = b.connect(ctx)
	for _, queue := range queues {
		if err != nil {
			break
		}
		err = b.publish(ctx, queue, item.ID, body)
	}
	if err != nil {
		b.close()
		// Persist a capped exponential delay. A failed target may cause already
		// confirmed targets to receive the same event again on the next attempt.
		delay := time.Second << min(item.Attempts, 5)
		_, saveErr := update(ctx, db, `UPDATE outbox_events SET next_attempt_on = ?, last_error = ?
			WHERE id = ? AND published_on IS NULL AND failed_on IS NULL`,
			time.Now().Add(delay).Unix(), err.Error(), item.ID)
		if saveErr != nil {
			return saveErr
		}
		return err
	}
	_, err = update(ctx, db, `UPDATE outbox_events SET published_on = ?, last_error = NULL
		WHERE id = ? AND published_on IS NULL AND failed_on IS NULL`, time.Now().Unix(), item.ID)
	if err != nil {
		_, saveErr := update(ctx, db, `UPDATE outbox_events SET last_error = ?
			WHERE id = ? AND published_on IS NULL AND failed_on IS NULL`, "record publication: "+err.Error(), item.ID)
		return errors.Join(err, saveErr)
	}
	return err
}

func update(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return db.ExecContext(ctx, query, args...)
}
