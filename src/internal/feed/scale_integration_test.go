package feed

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

// Opt-in F capacity fixture. Every follower is active and checked individually.
func verifyScale(t *testing.T, db *sql.DB, s *Service, url, prefix string) {
	n, err := strconv.Atoi(os.Getenv("NOTEX_SCALE_FOLLOWERS"))
	if err != nil || (n != 1000 && n != 10000 && n != 100000) {
		t.Fatal("invalid F size")
	}
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO users(id,username,password_hash,created_on) VALUES(400000,'scale-author','fixture',1)")
	for start := 0; start < n; start += 1000 {
		var users, relations []string
		for i := start; i < min(start+1000, n); i++ {
			id := 500000 + i
			users = append(users, fmt.Sprintf("(%d,'scale-%d','fixture',1)", id, id))
			relations = append(relations, fmt.Sprintf("(%d,400000,1)", id))
		}
		exec("INSERT INTO users(id,username,password_hash,created_on) VALUES" + strings.Join(users, ","))
		exec("INSERT INTO following(user_id,follow_id,created_on) VALUES" + strings.Join(relations, ","))
	}
	targets := []recipient{{UserID: 400000}}
	for i := 0; i < n; i++ {
		targets = append(targets, recipient{UserID: int64(500000 + i)})
	}
	fixtureActiveReaders(t, s, targets)
	publisher := outbox.NewTaskPublisher(url, prefix)
	defer publisher.Close()
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Run(workerCtx, url) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for round := 1; round <= 3; round++ {
		start := time.Now()
		item, err := post.New(db, nil).Create(ctx, 400000, fmt.Sprintf("scale-%d", round), "scale publication")
		if err != nil {
			t.Fatal(err)
		}
		var event outbox.Event
		if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE event_type='post.created' AND payload->>'$.post_id'=?", item.ID).Scan(&event.ID, &event.Type, &event.Payload, &event.CreatedOn); err != nil {
			t.Fatal(err)
		}
		if err := s.handleRoot(ctx, event, publisher.Publish); err != nil {
			t.Fatal(err)
		}
		ordinary, err := post.New(db, nil).Create(ctx, 1, fmt.Sprintf("ordinary-%d", round), "ordinary publication")
		if err != nil {
			t.Fatal(err)
		}
		var ordinaryEvent outbox.Event
		if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE event_type='post.created' AND payload->>'$.post_id'=?", ordinary.ID).Scan(&ordinaryEvent.ID, &ordinaryEvent.Type, &ordinaryEvent.Payload, &ordinaryEvent.CreatedOn); err != nil {
			t.Fatal(err)
		}
		if err := s.handleRoot(ctx, ordinaryEvent, publisher.Publish); err != nil {
			t.Fatal(err)
		}
		var ordinaryElapsed time.Duration
		deadline := time.Now().Add(10 * time.Minute)
		for {
			if ordinaryElapsed == 0 && s.redis.ZScore(ctx, s.readerKeys(1)[0], member(Entry{ID: ordinary.ID, CreatedOn: ordinary.CreatedOn})).Err() == nil {
				ordinaryElapsed = time.Since(start)
			}
			var plans, pending int
			err := db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(j.status IS NULL OR j.status<>'completed'),0) FROM feed_tasks t LEFT JOIN consumption_jobs j ON j.consumer=CONCAT(?,'.',t.kind) AND j.event_id=t.id WHERE t.root_event_id=?`, prefix, event.ID).Scan(&plans, &pending)
			if err != nil {
				t.Fatal(err)
			}
			if plans > 0 && pending == 0 && ordinaryElapsed > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("F convergence timeout")
			}
			time.Sleep(100 * time.Millisecond)
		}
		elapsed := time.Since(start)
		t.Logf("ordinary_task_visible_ms=%.3f", float64(ordinaryElapsed.Microseconds())/1000)
		entry := member(Entry{ID: item.ID, CreatedOn: item.CreatedOn})
		for start := 0; start < len(targets); start += pipelineSize {
			pipe := s.redis.Pipeline()
			for _, target := range targets[start:min(start+pipelineSize, len(targets))] {
				pipe.ZScore(ctx, s.readerKeys(target.UserID)[0], entry)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatal("missing F membership", err)
			}
		}
		t.Logf("F followers=%d round=%d exact_memberships=%d publication_to_complete_ms=%.3f redis_memory=%s", n, round, n+1, float64(elapsed.Microseconds())/1000, s.redis.Info(ctx, "memory").Val())
	}
}
