package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Notify coalesces wakeups. Durable work remains in MySQL if the channel is full.
func Notify(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

const (
	PostCreated   = "post.created"
	PostDeleted   = "post.deleted"
	FollowCreated = "follow.created"
	FollowDeleted = "follow.deleted"
)

type PostPayload struct {
	PostID int64 `json:"post_id"`
}

type FollowPayload struct {
	ActorID      int64 `json:"actor_id"`
	TargetUserID int64 `json:"target_user_id"`
	RelationID   int64 `json:"relation_id"`
}

// RecordPost requires the caller's business transaction. The caller owns
// Commit/Rollback, so a failed event insert also rolls back the business change.
func RecordPost(ctx context.Context, tx *sql.Tx, eventType string, postID, createdOn int64) error {
	payload, err := json.Marshal(PostPayload{PostID: postID})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (event_type, payload, created_on) VALUES (?, ?, ?)",
		eventType, string(payload), createdOn)
	return err
}

// RecordFollow preserves the exact relationship identity in the business transaction.
func RecordFollow(ctx context.Context, tx *sql.Tx, eventType string, actor, target, relationID, createdOn int64) error {
	payload, err := json.Marshal(FollowPayload{ActorID: actor, TargetUserID: target, RelationID: relationID})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (event_type, payload, created_on) VALUES (?, ?, ?)",
		eventType, string(payload), createdOn)
	return err
}
