package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
)

const (
	PostCreated   = "post.created"
	PostDeleted   = "post.deleted"
	FollowCreated = "follow.created"
	FollowDeleted = "follow.deleted"
)

type PostPayload struct {
	ActorID int64 `json:"actor_id"`
	PostID  int64 `json:"post_id"`
}

type FollowPayload struct {
	ActorID      int64 `json:"actor_id"`
	TargetUserID int64 `json:"target_user_id"`
}

// RecordPost requires the caller's business transaction. The caller owns
// Commit/Rollback, so a failed event insert also rolls back the business change.
func RecordPost(ctx context.Context, tx *sql.Tx, eventType string, actor, postID, createdOn int64) error {
	payload, err := json.Marshal(PostPayload{ActorID: actor, PostID: postID})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (event_type, payload, created_on) VALUES (?, ?, ?)",
		eventType, string(payload), createdOn)
	return err
}

// RecordFollow keeps both user IDs after the relationship row is deleted.
func RecordFollow(ctx context.Context, tx *sql.Tx, eventType string, actor, target, createdOn int64) error {
	payload, err := json.Marshal(FollowPayload{ActorID: actor, TargetUserID: target})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (event_type, payload, created_on) VALUES (?, ?, ?)",
		eventType, string(payload), createdOn)
	return err
}
