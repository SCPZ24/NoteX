package feed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/post"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// The hook records actual batches handed to go-redis, including NOSCRIPT fallback.
type pipelineObserver struct {
	maximum atomic.Int64
	batches atomic.Int64
	evals   atomic.Int64
}

func (h *pipelineObserver) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *pipelineObserver) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *pipelineObserver) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		size := int64(len(cmds))
		for old := h.maximum.Load(); size > old; old = h.maximum.Load() {
			if h.maximum.CompareAndSwap(old, size) {
				break
			}
		}
		h.batches.Add(1)
		for _, cmd := range cmds {
			if cmd.Name() == "eval" {
				h.evals.Add(1)
			}
		}
		return next(ctx, cmds)
	}
}

func verifyFanout(t *testing.T, db *sql.DB, s *Service, url, prefix string) {
	ctx := context.Background()
	execute := func(t *testing.T, query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := db.QueryRow(query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	wait := func(t *testing.T, label string, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			if ready() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("timed out: " + label)
	}
	execute(t, "INSERT INTO users(id,username,password_hash,created_on) VALUES(3000,'fanout-author','fixture',1)")
	for offset := 0; offset < 1203; offset += 100 {
		var users, relations []string
		for i := offset; i < min(offset+100, 1203); i++ {
			id := 4001 + i
			users = append(users, fmt.Sprintf("(%d,'fanout-%d','fixture',1)", id, id))
			relations = append(relations, fmt.Sprintf("(%d,3000,1)", id))
		}
		execute(t, "INSERT INTO users(id,username,password_hash,created_on) VALUES"+strings.Join(users, ","))
		execute(t, "INSERT INTO following(user_id,follow_id,created_on) VALUES"+strings.Join(relations, ","))
	}
	publisher := outbox.NewTaskPublisher(url, prefix)
	defer publisher.Close()
	observer := &pipelineObserver{}
	s.redis.AddHook(observer)
	posts := post.New(db, nil)
	makeRoot := func(t *testing.T, key string) (post.Post, outbox.Event) {
		t.Helper()
		item, err := posts.Create(ctx, 3000, key, key)
		if err != nil {
			t.Fatal(err)
		}
		var root outbox.Event
		if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE event_type='post.created' AND payload->>'$.post_id'=?", item.ID).Scan(&root.ID, &root.Type, &root.Payload, &root.CreatedOn); err != nil {
			t.Fatal(err)
		}
		return item, root
	}
	firstPlan := func(t *testing.T, root outbox.Event) outbox.Event {
		t.Helper()
		var event outbox.Event
		if err := s.handleRoot(ctx, root, func(_ context.Context, e outbox.Event) error { event = e; return nil }); err != nil {
			t.Fatal(err)
		}
		return event
	}
	taskEvent := func(t *testing.T, id int64) outbox.Event {
		t.Helper()
		event := outbox.Event{ID: id, Payload: json.RawMessage(fmt.Sprintf(`{"task_id":%d}`, id))}
		if err := db.QueryRow("SELECT kind,created_on FROM feed_tasks WHERE id=?", id).Scan(&event.Type, &event.CreatedOn); err != nil {
			t.Fatal(err)
		}
		return event
	}
	firstChunk := func(t *testing.T, root outbox.Event) outbox.Event {
		t.Helper()
		children, err := s.preparePage(ctx, firstPlan(t, root))
		if err != nil {
			t.Fatal(err)
		}
		return taskEvent(t, children[0])
	}
	drainQueues := func(t *testing.T) []string {
		t.Helper()
		conn, err := amqp.Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		channel, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		var identities []string
		for _, suffix := range []string{".feed.plan", ".feed.chunk"} {
			for {
				msg, ok, err := channel.Get(prefix+suffix, true)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				identities = append(identities, suffix+":"+msg.MessageId)
			}
		}
		return identities
	}
	startChunks := func(t *testing.T, handler outbox.ExternalHandler) func() {
		t.Helper()
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := outbox.RunExternalConsumer(workerCtx, db, url, prefix, "feed.chunk", handler); err != nil {
				t.Error(err)
			}
		}()
		stop := func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("chunk worker stop timed out")
			}
		}
		t.Cleanup(stop)
		return stop
	}
	startFeed := func(t *testing.T) func() {
		t.Helper()
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := s.Run(workerCtx, url); err != nil {
				t.Error(err)
			}
		}()
		stop := func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Fatal("Feed workers stop timed out")
			}
		}
		t.Cleanup(stop)
		return stop
	}
	visible := func(service *Service, reader int64, item post.Post) bool {
		return service.redis.ZScore(ctx, service.readerKeys(reader)[0], member(Entry{ID: item.ID, CreatedOn: item.CreatedOn})).Err() == nil
	}
	assertTargets := func(t *testing.T, item post.Post) {
		t.Helper()
		expected := map[int64]bool{3000: true}
		rows, err := db.Query("SELECT user_id FROM following WHERE follow_id=3000")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			expected[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		actual := map[int64]bool{}
		iterator := s.redis.Scan(ctx, 0, prefix+":reader:*:posts", 100).Iterator()
		for iterator.Next(ctx) {
			key := iterator.Val()
			if s.redis.ZScore(ctx, key, member(Entry{ID: item.ID, CreatedOn: item.CreatedOn})).Err() != nil {
				continue
			}
			raw := strings.TrimSuffix(strings.TrimPrefix(key, prefix+":reader:{"), "}:posts")
			id, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			actual[id] = true
		}
		if err := iterator.Err(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("recipient mismatch actual=%d expected=%d", len(actual), len(expected))
		}
	}
	waitRoot := func(t *testing.T, root outbox.Event) {
		t.Helper()
		wait(t, "all frozen tasks completed", func() bool {
			return count(t, `SELECT COUNT(*) FROM feed_tasks t JOIN consumption_jobs j ON j.event_id=t.id AND j.consumer=CONCAT(?,'.',t.kind) WHERE t.root_event_id=? AND j.status='completed'`, prefix, root.ID) == 17
		})
		if count(t, "SELECT COUNT(*) FROM feed_tasks WHERE root_event_id=? AND kind='feed.plan'", root.ID) != 3 || count(t, "SELECT COUNT(*) FROM feed_tasks WHERE root_event_id=? AND kind='feed.chunk'", root.ID) != 14 {
			t.Fatal("unexpected page/chunk count")
		}
	}

	t.Run("20 partial confirmed handoffs reuse frozen recipients and successor", func(t *testing.T) {
		for round := 0; round < 20; round++ {
			_, root := makeRoot(t, fmt.Sprintf("handoff-%d", round))
			event := firstPlan(t, root)
			published := 0
			err := s.plan(ctx, event, func(ctx context.Context, child outbox.Event) error {
				if published == 2 {
					return errors.New("injected exit after two confirms")
				}
				if err := publisher.Publish(ctx, child); err != nil {
					return err
				}
				published++
				return nil
			})
			if err == nil {
				t.Fatal("partial handoff unexpectedly completed")
			}
			if len(drainQueues(t)) != 2 {
				t.Fatal("confirmed children missing")
			}
			before, err := s.preparePage(ctx, event)
			if err != nil {
				t.Fatal(err)
			}
			var frozen []byte
			if err := db.QueryRow("SELECT body FROM feed_tasks WHERE id=?", before[0]).Scan(&frozen); err != nil {
				t.Fatal(err)
			}
			var payload taskBody
			if err := json.Unmarshal(frozen, &payload); err != nil {
				t.Fatal(err)
			}
			removed := payload.Recipients[0]
			execute(t, "DELETE FROM following WHERE id=?", removed.RelationID)
			if err := s.plan(ctx, event, publisher.Publish); err != nil {
				t.Fatal(err)
			}
			after, err := s.preparePage(ctx, event)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || len(drainQueues(t)) != 7 {
				t.Fatal("handoff changed identity or lost successor")
			}
			var replayBody []byte
			if err := db.QueryRow("SELECT body FROM feed_tasks WHERE id=?", before[0]).Scan(&replayBody); err != nil {
				t.Fatal(err)
			}
			if string(frozen) != string(replayBody) {
				t.Fatal("frozen recipients changed")
			}
			execute(t, "INSERT INTO following(id,user_id,follow_id,created_on) VALUES(?,?,3000,1)", removed.RelationID, removed.UserID)
		}
	})

	t.Run("20 partial pipelines recover and every batch stays bounded", func(t *testing.T) {
		item, root := makeRoot(t, "partial-pipeline")
		event := firstChunk(t, root)
		current, err := loadTask(ctx, db, event, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.redis.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		for round := 0; round < 20; round++ {
			isolated := &Service{db: db, redis: s.redis, prefix: fmt.Sprintf("%s:partial:%d", prefix, round)}
			broken := isolated.readerKeys(current.Body.Recipients[0].UserID)[0]
			if err := s.redis.Set(ctx, broken, "wrong type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := isolated.distribute(ctx, event); err == nil {
				t.Fatal("partial Redis failure ignored")
			}
			// A cold EVALSHA pass can fail before writes; retry warms the bounded EVAL batch.
			if !visible(isolated, current.Body.Recipients[1].UserID, item) {
				t.Fatal("expected partially successful pipeline")
			}
			if err := s.redis.Del(ctx, broken).Err(); err != nil {
				t.Fatal(err)
			}
			for repeat := 0; repeat < 2; repeat++ {
				if err := isolated.distribute(ctx, event); err != nil {
					t.Fatal(err)
				}
			}
			for _, target := range current.Body.Recipients {
				if !visible(isolated, target.UserID, item) || s.redis.ZCard(ctx, isolated.readerKeys(target.UserID)[0]).Val() != 1 {
					t.Fatal("chunk recovery lost or duplicated member")
				}
			}
		}
		if observer.maximum.Load() > pipelineSize || observer.evals.Load() == 0 {
			t.Fatal("pipeline cap or cold-script fallback untested")
		}
		if err := s.writeReaderBatch(ctx, Entry{ID: item.ID, CreatedOn: item.CreatedOn}, make([]recipient, pipelineSize+1), false); err == nil {
			t.Fatal("oversized pipeline accepted")
		}
	})

	t.Run("20 exits after reader effects before completion and ACK", func(t *testing.T) {
		for round := 0; round < 20; round++ {
			item, root := makeRoot(t, fmt.Sprintf("reader-exit-%d", round))
			event := firstChunk(t, root)
			if err := publisher.Publish(ctx, event); err != nil {
				t.Fatal(err)
			}
			workerCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				done <- outbox.RunExternalConsumer(workerCtx, db, url, prefix, "feed.chunk", func(ctx context.Context, e outbox.Event) error {
					if err := s.distribute(ctx, e); err != nil {
						return err
					}
					cancel()
					return nil
				})
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				cancel()
				t.Fatal("worker did not reach effect")
			}
			if count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND event_id=? AND status='pending' AND attempts=1", prefix+".feed.chunk", event.ID) != 1 {
				t.Fatal("claim lost before ACK")
			}
			execute(t, "UPDATE consumption_jobs SET next_attempt_on=0 WHERE consumer=? AND event_id=?", prefix+".feed.chunk", event.ID)
			stop := startChunks(t, s.distribute)
			wait(t, "reader effect recovered", func() bool {
				return count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND event_id=? AND status='completed' AND attempts=2", prefix+".feed.chunk", event.ID) == 1
			})
			stop()
			current, err := loadTask(ctx, db, event, false)
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range current.Body.Recipients {
				if !visible(s, target.UserID, item) {
					t.Fatal("recovered chunk missing reader")
				}
			}
		}
	})

	t.Run("20 deletes reject stale reader additions", func(t *testing.T) {
		item, root := makeRoot(t, "reader-delete")
		event := firstChunk(t, root)
		current, err := loadTask(ctx, db, event, false)
		if err != nil {
			t.Fatal(err)
		}
		targets := current.Body.Recipients[:pipelineSize]
		for round := 0; round < 20; round++ {
			isolated := &Service{db: db, redis: s.redis, prefix: fmt.Sprintf("%s:deletion:%d", prefix, round)}
			entry := Entry{ID: item.ID, CreatedOn: item.CreatedOn}
			if err := isolated.writeReaderBatch(ctx, entry, targets, false); err != nil {
				t.Fatal(err)
			}
			if err := isolated.writeReaderBatch(ctx, entry, targets, true); err != nil {
				t.Fatal(err)
			}
			if err := isolated.writeReaderBatch(ctx, entry, targets, false); err != nil {
				t.Fatal(err)
			}
			for _, target := range targets {
				if visible(isolated, target.UserID, item) {
					t.Fatal("deleted reader member resurrected")
				}
			}
		}
	})

	t.Run("1203 followers plus self complete through frozen pages", func(t *testing.T) {
		item, root := makeRoot(t, "full-fanout")
		stop := startFeed(t)
		defer stop()
		if err := s.handleRoot(ctx, root, publisher.Publish); err != nil {
			t.Fatal(err)
		}
		waitRoot(t, root)
		assertTargets(t, item)
		t.Log("1203 followers + self, 3 plan tasks, 14 chunks, 1204 exact reader memberships")
		if err := posts.Delete(ctx, 3000, item.ID); err != nil {
			t.Fatal(err)
		}
		var deletion outbox.Event
		if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE event_type='post.deleted' AND payload->>'$.post_id'=?", item.ID).Scan(&deletion.ID, &deletion.Type, &deletion.Payload, &deletion.CreatedOn); err != nil {
			t.Fatal(err)
		}
		if err := s.handleRoot(ctx, deletion, publisher.Publish); err != nil {
			t.Fatal(err)
		}
		waitRoot(t, deletion)
		for reader := int64(4001); reader <= 5203; reader++ {
			if visible(s, reader, item) {
				t.Fatal("deleted post remains in reader index")
			}
		}
		if visible(s, 3000, item) {
			t.Fatal("deleted self post remains")
		}
	})

	t.Run("unroutable chunk preserves frozen handoff", func(t *testing.T) {
		_, root := makeRoot(t, "unroutable-chunk")
		event := firstPlan(t, root)
		// Keep the publishing connection open, then remove a destination.
		if err := publisher.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
		drainQueues(t)
		conn, err := amqp.Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		channel, err := conn.Channel()
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		_, err = channel.QueueDelete(prefix+".feed.chunk", false, false, false)
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = s.plan(ctx, event, publisher.Publish)
		if err == nil || !strings.Contains(err.Error(), "unroutable") {
			t.Fatalf("unroutable publication accepted: %v", err)
		}
		if err := s.plan(ctx, event, publisher.Publish); err != nil {
			t.Fatal(err)
		}
		if len(drainQueues(t)) != 7 {
			t.Fatal("reconnected handoff lost work")
		}
	})

	if os.Getenv("NOTEX_FANOUT_MQ_OUTAGE") == "1" {
		t.Run("MQ outage retains root intent and resumes complete fanout", func(t *testing.T) {
			// Earlier roots were driven explicitly in focused fault fixtures.
			execute(t, "UPDATE outbox_events SET published_on=? WHERE published_on IS NULL", time.Now().Unix())
			compose := func(action string) {
				t.Helper()
				command := exec.Command("docker", "compose", action, "rabbitmq")
				command.Dir = "../../.."
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("MQ %s: %v %s", action, err, output)
				}
			}
			publisher.Close()
			compose("stop")
			stopped := time.Now()
			defer compose("start")
			item, root := makeRoot(t, "mq-outage")
			stop := startFeed(t)
			defer stop()
			pubCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() { defer close(done); outbox.Run(pubCtx, db, nil, url, prefix) }()
			defer func() { cancel(); <-done }()
			wait(t, "root retry intent saved", func() bool {
				return count(t, "SELECT COUNT(*) FROM outbox_events WHERE id=? AND published_on IS NULL AND attempts>0 AND last_error IS NOT NULL", root.ID) == 1
			})
			for time.Since(stopped) < 60*time.Second {
				time.Sleep(time.Second)
			}
			compose("start")
			waitRoot(t, root)
			assertTargets(t, item)
			t.Logf("MQ stopped for at least 60s, all 1204 recipients recovered; stop-to-convergence %.2fs", time.Since(stopped).Seconds())
		})
	}
	t.Logf("observed pipeline maximum=%d commands, batches=%d, cold EVAL commands=%d", observer.maximum.Load(), observer.batches.Load(), observer.evals.Load())
}
