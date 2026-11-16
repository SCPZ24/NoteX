package feed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
)

const (
	planPageSize = 500
	chunkSize    = 100
	pipelineSize = 32
)

type recipient struct {
	UserID     int64 `json:"user_id"`
	RelationID int64 `json:"relation_id"`
}

type taskBody struct {
	PostID     int64       `json:"post_id"`
	After      *Entry      `json:"after,omitempty"` // Entry.ID is following.id for this cursor.
	Recipients []recipient `json:"recipients,omitempty"`
}

type task struct {
	ID          int64
	RootEventID int64
	Kind        string
	Body        taskBody
	Children    []byte
	CreatedOn   int64
}

// Run owns one root consumer, one planner and one chunk worker. Each publishing
// role owns its AMQP connection; all roles share the bounded Redis/SQL pools.
func (s *Service) Run(ctx context.Context, url string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	results := make(chan error, 2)
	plannerPublisher := outbox.NewTaskPublisher(url, s.prefix)
	defer plannerPublisher.Close()
	rootPublisher := outbox.NewTaskPublisher(url, s.prefix)
	defer rootPublisher.Close()
	workers.Add(2)
	go func() {
		defer workers.Done()
		results <- outbox.RunExternalConsumer(ctx, s.db, url, s.prefix, "feed.plan", func(ctx context.Context, event outbox.Event) error {
			return s.plan(ctx, event, plannerPublisher.Publish)
		})
		cancel()
	}()
	go func() {
		defer workers.Done()
		results <- outbox.RunExternalConsumer(ctx, s.db, url, s.prefix, "feed.chunk", s.distribute)
		cancel()
	}()
	err := outbox.RunExternalConsumer(ctx, s.db, url, s.prefix, "feed", func(ctx context.Context, event outbox.Event) error {
		return s.handleRoot(ctx, event, rootPublisher.Publish)
	})
	cancel()
	workers.Wait()
	return errors.Join(err, <-results, <-results)
}

func (s *Service) handleRoot(ctx context.Context, event outbox.Event, publish func(context.Context, outbox.Event) error) error {
	if err := s.Handle(ctx, event); err != nil {
		return err
	}
	var payload outbox.PostPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	body, err := json.Marshal(taskBody{PostID: payload.PostID})
	if err != nil {
		return err
	}
	// Repeated roots retain the first task's identity, body and creation time.
	_, err = s.db.ExecContext(ctx, `INSERT INTO feed_tasks(root_event_id,parent_id,ordinal,kind,body,created_on)
 VALUES(?,0,0,'feed.plan',?,?) ON DUPLICATE KEY UPDATE id=id`, event.ID, body, event.CreatedOn)
	if err != nil {
		return err
	}
	var id int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM feed_tasks WHERE root_event_id=? AND parent_id=0 AND ordinal=0`, event.ID).Scan(&id); err != nil {
		return err
	}
	return s.publishTasks(ctx, []int64{id}, publish)
}

// A page's recipients and continuation are frozen in one transaction. Replays
// publish these exact children even when the relationship table has changed.
func (s *Service) plan(ctx context.Context, event outbox.Event, publish func(context.Context, outbox.Event) error) error {
	children, err := s.preparePage(ctx, event)
	if err != nil {
		return err
	}
	return s.publishTasks(ctx, children, publish)
}

func (s *Service) preparePage(ctx context.Context, event outbox.Event) ([]int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, err := loadTask(ctx, tx, event, true)
	if err != nil {
		return nil, err
	}
	if current.Kind != "feed.plan" {
		return nil, fmt.Errorf("%w: expected planning task", outbox.ErrPermanent)
	}
	if current.Children != nil {
		var children []int64
		if err := json.Unmarshal(current.Children, &children); err != nil {
			return nil, err
		}
		return children, nil
	}
	var author int64
	if err := tx.QueryRowContext(ctx, "SELECT author_id FROM posts WHERE id=?", current.Body.PostID).Scan(&author); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: planning post missing", outbox.ErrPermanent)
		}
		return nil, err
	}
	query := "SELECT user_id,id,created_on FROM following WHERE follow_id=?"
	args := []any{author}
	if after := current.Body.After; after != nil {
		if after.ID <= 0 || after.CreatedOn <= 0 {
			return nil, fmt.Errorf("%w: invalid planning cursor", outbox.ErrPermanent)
		}
		query += " AND (created_on<? OR (created_on=? AND id<?))"
		args = append(args, after.CreatedOn, after.CreatedOn, after.ID)
	}
	query += " ORDER BY created_on DESC,id DESC LIMIT ?"
	args = append(args, planPageSize+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	recipients := make([]recipient, 0, planPageSize+1)
	var boundary Entry
	hasMore := false
	for rows.Next() {
		var target recipient
		var created int64
		if err := rows.Scan(&target.UserID, &target.RelationID, &created); err != nil {
			rows.Close()
			return nil, err
		}
		if len(recipients) == planPageSize {
			hasMore = true
			break
		}
		recipients = append(recipients, target)
		boundary = Entry{ID: target.RelationID, CreatedOn: created}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if current.Body.After == nil {
		recipients = append(recipients, recipient{UserID: author})
	}
	children := make([]int64, 0, 7)
	insert := func(kind string, body taskBody) error {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO feed_tasks(root_event_id,parent_id,ordinal,kind,body,created_on) VALUES(?,?,?,?,?,?)`,
			current.RootEventID, current.ID, len(children), kind, encoded, time.Now().Unix())
		if err != nil {
			return err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return err
		}
		children = append(children, id)
		return nil
	}
	for offset := 0; offset < len(recipients); offset += chunkSize {
		end := min(offset+chunkSize, len(recipients))
		if err := insert("feed.chunk", taskBody{PostID: current.Body.PostID, Recipients: recipients[offset:end]}); err != nil {
			return nil, err
		}
	}
	if hasMore {
		if err := insert("feed.plan", taskBody{PostID: current.Body.PostID, After: &boundary}); err != nil {
			return nil, err
		}
	}
	encoded, err := json.Marshal(children)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE feed_tasks SET children=? WHERE id=?", encoded, current.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return children, nil
}

// Only a page's bounded children are read; there is no whole-fanout materialization.
func (s *Service) publishTasks(ctx context.Context, ids []int64, publish func(context.Context, outbox.Event) error) error {
	if len(ids) > 7 {
		return fmt.Errorf("%w: oversized page handoff", outbox.ErrPermanent)
	}
	for _, id := range ids {
		var event outbox.Event
		event.ID = id
		if err := s.db.QueryRowContext(ctx, "SELECT kind,created_on FROM feed_tasks WHERE id=?", id).Scan(&event.Type, &event.CreatedOn); err != nil {
			return err
		}
		event.Payload = json.RawMessage(fmt.Sprintf(`{"task_id":%d}`, id))
		if err := publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// *sql.DB and *sql.Tx share this narrow query surface; planning needs a row lock.
type taskQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadTask(ctx context.Context, query taskQuerier, event outbox.Event, lock bool) (task, error) {
	var payload struct {
		TaskID int64 `json:"task_id"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.TaskID <= 0 || payload.TaskID != event.ID {
		return task{}, fmt.Errorf("%w: invalid task identity", outbox.ErrPermanent)
	}
	statement := "SELECT id,root_event_id,kind,body,children,created_on FROM feed_tasks WHERE id=?"
	if lock {
		statement += " FOR UPDATE"
	}
	var current task
	var body []byte
	err := query.QueryRowContext(ctx, statement, payload.TaskID).Scan(&current.ID, &current.RootEventID, &current.Kind, &body, &current.Children, &current.CreatedOn)
	if errors.Is(err, sql.ErrNoRows) {
		return task{}, fmt.Errorf("%w: task missing", outbox.ErrPermanent)
	}
	if err != nil {
		return task{}, err
	}
	if current.Kind != event.Type || current.CreatedOn != event.CreatedOn {
		return task{}, fmt.Errorf("%w: task envelope conflict", outbox.ErrPermanent)
	}
	if err := json.Unmarshal(body, &current.Body); err != nil || current.Body.PostID <= 0 {
		return task{}, fmt.Errorf("%w: invalid task body", outbox.ErrPermanent)
	}
	return current, nil
}
