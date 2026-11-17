package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Retains five completed rows in the caller's existing test database. The old
// handler is paused after claiming, a newer claimant finishes, then the old
// handler returns a permanent failure. Its stale identity must affect no row.
func TestDistributedLateCompletion(t *testing.T) {
	dsn := os.Getenv("NOTEX_DISTRIBUTED_DSN")
	if dsn == "" {
		t.Skip("set NOTEX_DISTRIBUTED_DSN to the retained Sprint 5 test database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx := context.Background()
	consumer := fmt.Sprintf("notex_guard_%d.feed.chunk.s000", time.Now().UnixNano())
	for round := int64(1); round <= 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			event := Event{ID: round, Type: "feed.chunk", CreatedOn: 1700000000, ShardCount: 4,
				Payload: json.RawMessage(fmt.Sprintf(`{"task_id":%d}`, round))}
			body, _ := json.Marshal(event)
			id, err := receive(ctx, db, consumer, strconv.FormatInt(round, 10), body)
			if err != nil {
				t.Fatal(err)
			}
			claimed, release, done := make(chan struct{}), make(chan struct{}, 1), make(chan error, 1)
			defer close(release)
			lateCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				done <- processExternal(lateCtx, db, consumer, id, func(ctx context.Context, event Event) error {
					close(claimed)
					select {
					case <-release:
					case <-ctx.Done():
					}
					return ErrPermanent
				})
			}()
			select {
			case <-claimed:
			case <-time.After(5 * time.Second):
				t.Fatal("first claimant did not start")
			}
			if _, err := db.Exec("UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id); err != nil {
				t.Fatal(err)
			}
			if err := processExternal(ctx, db, consumer, id, func(context.Context, Event) error { return nil }); err != nil {
				t.Fatal(err)
			}
			// Let the old external call finish while its parent is still live.
			release <- struct{}{}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			var status string
			var attempts int
			if err := db.QueryRow("SELECT status,attempts FROM consumption_jobs WHERE id=?", id).Scan(&status, &attempts); err != nil {
				t.Fatal(err)
			}
			if status != "completed" || attempts != 2 {
				t.Fatal("stale completion changed newer claim", status, attempts)
			}
			t.Log("newer completed claim survived old permanent failure")
		})
	}
}

// Hold each real publisher confirm until the real SAC consumer has committed
// receipt and completion. Releasing the stale confirmation must preserve the
// cleared receipt token. One queue and five small rows cover all five windows.
func TestDistributedConfirmAfterReceipt(t *testing.T) {
	dsn, url := os.Getenv("NOTEX_DISTRIBUTED_DSN"), os.Getenv("NOTEX_DISTRIBUTED_MQ")
	if dsn == "" || url == "" {
		t.Skip("set NOTEX_DISTRIBUTED_DSN and NOTEX_DISTRIBUTED_MQ")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("notex_confirm_%d", time.Now().UnixNano())
	queue := prefix + "." + ShardName(0)
	for id := int64(1); id <= 5; id++ {
		event := Event{ID: id, Type: "feed.chunk", CreatedOn: 1700000000, ShardCount: 1,
			Payload: json.RawMessage(fmt.Sprintf(`{"task_id":%d}`, id))}
		body, _ := json.Marshal(event)
		if _, err := receive(ctx, db, queue, strconv.FormatInt(id, 10), body); err != nil {
			t.Fatal(err)
		}
	}
	b := &broker{url: url, prefix: prefix, extraQueues: []string{ShardName(0)}}
	defer b.close()
	if err := b.connect(ctx); err != nil {
		t.Fatal(err)
	}
	actual := b.confirms
	delayed := make(chan amqp.Confirmation)
	b.confirms = delayed
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- RunShard(ctx, db, url, prefix, 0, 100, func(context.Context, Event) error { return nil }, nil)
	}()
	defer func() {
		cancel()
		if err := <-consumerDone; err != nil {
			t.Error(err)
		}
	}()
	publishDone := make(chan error, 1)
	publishFinished := make(chan struct{})
	go func() {
		defer close(publishFinished)
		publishDone <- republishDue(ctx, db, b, queue)
	}()
	defer func() { cancel(); <-publishFinished }()
	for id := int64(1); id <= 5; id++ {
		var confirmation amqp.Confirmation
		select {
		case confirmation = <-actual:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		for {
			var status, token string
			var after int64
			if err := db.QueryRowContext(ctx, "SELECT status,retry_publish_after,retry_publish_token FROM consumption_jobs WHERE consumer=? AND event_id=?", queue, id).Scan(&status, &after, &token); err != nil {
				t.Fatal(err)
			}
			if status == "completed" && after == 0 && token == "" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case delayed <- confirmation:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	var completed int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND status='completed' AND attempts=1 AND retry_publish_after=0 AND retry_publish_token=''", queue).Scan(&completed); err != nil || completed != 5 {
		t.Fatal("late confirm overwrote receipt", completed, err)
	}
	t.Log("five real confirms released after completed receipts", queue)
}

// Two retry publishers and two SAC subscriptions share one retained quorum
// queue. Each identity fails once, then must complete once with its retry token
// cleared by receipt, including when confirmation arrives after that receipt.
func TestDistributedShardRetry(t *testing.T) {
	dsn, url := os.Getenv("NOTEX_DISTRIBUTED_DSN"), os.Getenv("NOTEX_DISTRIBUTED_MQ")
	if dsn == "" || url == "" {
		t.Skip("set NOTEX_DISTRIBUTED_DSN and NOTEX_DISTRIBUTED_MQ")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("notex_retry_%d", time.Now().UnixNano())
	queue := prefix + "." + ShardName(0)
	for id := int64(1); id <= 5; id++ {
		event := Event{ID: id, Type: "feed.chunk", CreatedOn: 1700000000, ShardCount: 1,
			Payload: json.RawMessage(fmt.Sprintf(`{"task_id":%d}`, id))}
		body, _ := json.Marshal(event)
		if _, err := receive(ctx, db, queue, strconv.FormatInt(id, 10), body); err != nil {
			t.Fatal(err)
		}
	}
	var active atomic.Int64
	var overlap atomic.Bool
	var mu sync.Mutex
	calls := map[int64]int{}
	handler := func(ctx context.Context, event Event) error {
		n := active.Add(1)
		defer active.Add(-1)
		if n > 1 {
			overlap.Store(true)
		}
		mu.Lock()
		calls[event.ID]++
		attempt := calls[event.ID]
		mu.Unlock()
		if attempt == 1 {
			return errors.New("injected retryable effect failure")
		}
		return nil
	}
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	for _, priority := range []int{100, 10} {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := RunShard(ctx, db, url, prefix, 0, priority, handler, nil); err != nil {
				t.Error(err)
			}
		}()
		go func() { defer wg.Done(); RunShardRetries(ctx, db, url, prefix, 1) }()
	}
	for {
		var completed int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND status='completed' AND attempts=2 AND retry_publish_after=0 AND retry_publish_token=''", queue).Scan(&completed)
		if err != nil {
			t.Fatal(err)
		}
		if completed == 5 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("sharded retry did not converge", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if overlap.Load() {
		t.Fatal("SAC did not serialize effects")
	}
	mu.Lock()
	defer mu.Unlock()
	for id := int64(1); id <= 5; id++ {
		if calls[id] != 2 {
			t.Fatal("unexpected attempts", id, calls[id])
		}
	}
	t.Log("five identities retried twice, two competing publishers, one active handler", queue)
}
