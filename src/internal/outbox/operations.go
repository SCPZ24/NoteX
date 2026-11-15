package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

type Record struct {
	ID            int64   `json:"id"`
	Consumer      string  `json:"consumer,omitempty"`
	EventID       *int64  `json:"event_id"`
	Status        string  `json:"status"`
	Attempts      uint64  `json:"attempts"`
	CycleAttempts uint32  `json:"cycle_attempts"`
	NextAttemptOn int64   `json:"next_attempt_on"`
	LastError     *string `json:"last_error"`
	Body          []byte  `json:"body_base64"`
}

type ReplayRecord struct {
	ID            int64  `json:"id"`
	Reason        string `json:"reason"`
	PreviousError string `json:"previous_error"`
	CreatedOn     int64  `json:"created_on"`
}

// Inspect returns bounded, ID-paginated records. A positive id selects one row.
// Body is base64 in JSON so malformed input bytes remain lossless.
func Inspect(ctx context.Context, db *sql.DB, scope, status string, id, after int64, limit int) ([]Record, error) {
	if id < 0 || after < 0 || limit < 1 || limit > 100 {
		return nil, errors.New("id/after must be nonnegative; limit must be 1-100")
	}
	if status != "" && status != "pending" && status != "failed" && status != "completed" {
		return nil, errors.New("status must be pending, failed, or completed")
	}
	var query string
	switch scope {
	case "outbox":
		query = `SELECT id,'' AS consumer,id AS event_id,
			CASE WHEN published_on IS NOT NULL THEN 'completed' WHEN failed_on IS NOT NULL THEN 'failed' ELSE 'pending' END AS status,
			attempts,0 AS cycle_attempts,next_attempt_on,last_error,payload AS body FROM outbox_events`
	case "consumer":
		query = `SELECT id,consumer,event_id,status,attempts,cycle_attempts,next_attempt_on,last_error,body FROM consumption_jobs`
	default:
		return nil, errors.New("scope must be outbox or consumer")
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT * FROM ("+query+") AS records WHERE (?=0 OR id=?) AND id>? AND (?='' OR status=?) ORDER BY id LIMIT ?", id, id, after, status, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]Record, 0)
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.ID, &record.Consumer, &record.EventID, &record.Status, &record.Attempts,
			&record.CycleAttempts, &record.NextAttemptOn, &record.LastError, &record.Body); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func ReplayHistory(ctx context.Context, db *sql.DB, scope string, id int64) ([]ReplayRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id,reason,previous_error,created_on FROM event_replays
		WHERE scope=? AND record_id=? ORDER BY id DESC LIMIT 100`, scope, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]ReplayRecord, 0)
	for rows.Next() {
		var record ReplayRecord
		if err := rows.Scan(&record.ID, &record.Reason, &record.PreviousError, &record.CreatedOn); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// Replay atomically schedules the original identity and records the operator's
// reason. Completed jobs cannot be reset. Invalid originals must be repaired at
// their source; quarantined bytes stay available as evidence.
func Replay(ctx context.Context, db *sql.DB, scope string, id int64, reason string) error {
	reason = strings.TrimSpace(reason)
	if id <= 0 || !utf8.ValidString(reason) || reason == "" || utf8.RuneCountInString(reason) > 1024 {
		return errors.New("positive id and a reason of 1-1024 characters are required")
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var lastError sql.NullString
	switch scope {
	case "outbox":
		var item Event
		var published sql.NullInt64
		err = tx.QueryRowContext(ctx, `SELECT id,event_type,payload,created_on,published_on,last_error
			FROM outbox_events WHERE id=? FOR UPDATE`, id).
			Scan(&item.ID, &item.Type, &item.Payload, &item.CreatedOn, &published, &lastError)
		if err != nil {
			return err
		}
		if published.Valid || !lastError.Valid {
			return errors.New("outbox replay requires an unpublished event with a recorded failure")
		}
		if _, err := item.destinations(""); err != nil {
			return fmt.Errorf("repair the source event before replay: %w", err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE outbox_events SET failed_on=NULL,next_attempt_on=0 WHERE id=?`, id)
	case "consumer":
		var consumer, key, status, messageID string
		var body []byte
		err = tx.QueryRowContext(ctx, `SELECT consumer,event_key,status,message_id,body,last_error
			FROM consumption_jobs WHERE id=? FOR UPDATE`, id).Scan(&consumer, &key, &status, &messageID, &body, &lastError)
		if err != nil {
			return err
		}
		if status != "failed" {
			return errors.New("consumer replay requires a failed job")
		}
		if _, err := decodeDelivery(consumer, messageID, body); err != nil {
			return fmt.Errorf("repair the source message before replay: %w", err)
		}
		if !strings.HasPrefix(key, "event:") {
			return errors.New("quarantined identity conflict requires a corrected source message")
		}
		_, err = tx.ExecContext(ctx, `UPDATE consumption_jobs SET status='pending',cycle_attempts=0,
			next_attempt_on=0,failed_on=NULL WHERE id=?`, id)
	default:
		return errors.New("scope must be outbox or consumer")
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO event_replays (scope,record_id,reason,previous_error,created_on) VALUES (?,?,?,?,?)`,
		scope, id, reason, lastError.String, time.Now().Unix())
	if err != nil {
		return err
	}
	return tx.Commit()
}
