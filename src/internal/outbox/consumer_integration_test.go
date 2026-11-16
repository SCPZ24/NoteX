package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Opt-in tests use isolated database/queues without stopping the shared broker.
func TestConsumerIntegration(t *testing.T) {
	if os.Getenv("NOTEX_CONSUMER_INTEGRATION") != "1" {
		t.Skip("set NOTEX_CONSUMER_INTEGRATION=1 with MySQL and RabbitMQ running")
	}
	ctx := context.Background()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid TEST_MYSQL_DSN")
	}
	cfg.MultiStatements = true
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = operationTimeout, operationTimeout, operationTimeout
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	prefix := fmt.Sprintf("notex_dummy_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + prefix); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE " + prefix); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = prefix
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	execute := func(query string, args ...any) sql.Result {
		t.Helper()
		result, err := db.Exec(query, args...)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var result int
		if err := db.QueryRow(query, args...).Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	migrations, err := filepath.Glob("../../../migrations/*.sql")
	if err != nil || len(migrations) != 5 {
		t.Fatalf("migration inventory: %v %v", migrations, err)
	}
	for _, path := range migrations {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		execute(string(body))
	}
	body := func(id int64) []byte {
		t.Helper()
		result, err := json.Marshal(Event{ID: id, Type: PostCreated,
			Payload: json.RawMessage(`{"post_id":99}`), CreatedOn: 1})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	feed, search := prefix+".feed", prefix+".search"
	receiveEvent := func(consumer string, eventID int64) int64 {
		t.Helper()
		id, err := receive(ctx, db, consumer, strconv.FormatInt(eventID, 10), body(eventID))
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	process := func(consumer string, id int64, handler SQLHandler) {
		t.Helper()
		if err := processJob(ctx, db, consumer, id, handler); err != nil {
			t.Fatal(err)
		}
	}
	waitFor := func(label string, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if ready() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("timed out: " + label)
	}

	t.Run("duplicate and concurrent SQL effects", func(t *testing.T) {
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() {
				id, err := receive(ctx, db, feed, "1", body(1))
				if err == nil {
					err = processJob(ctx, db, feed, id, DummyHandler(feed))
				}
				if err != nil {
					t.Error(err)
				}
			})
		}
		workers.Wait()
		if count("SELECT COUNT(*) FROM dummy_effects WHERE consumer=? AND event_id=1", feed) != 1 ||
			count("SELECT attempts FROM consumption_jobs WHERE consumer=? AND event_id=1", feed) != 1 {
			t.Fatal("duplicate delivery repeated the effect")
		}
		id := receiveEvent(search, 1)
		process(search, id, DummyHandler(search))
		if count("SELECT COUNT(*) FROM dummy_effects WHERE event_id=1") != 2 {
			t.Fatal("consumer identities are not independent")
		}
	})

	t.Run("partial SQL rollback and retry", func(t *testing.T) {
		id := receiveEvent(feed, 2)
		process(feed, id, func(ctx context.Context, tx *sql.Tx, item Event) error {
			if err := DummyHandler(feed)(ctx, tx, item); err != nil {
				return err
			}
			return errors.New("temporary failure after partial SQL effect")
		})
		if count("SELECT COUNT(*) FROM dummy_effects WHERE event_id=2") != 0 ||
			count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='pending' AND attempts=1 AND next_attempt_on>UNIX_TIMESTAMP()", id) != 1 {
			t.Fatal("partial effect or retry state incorrect")
		}
		process(feed, id, DummyHandler(feed))
		if count("SELECT attempts FROM consumption_jobs WHERE id=?", id) != 1 {
			t.Fatal("duplicate delivery bypassed backoff")
		}
		execute("UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id)
		if err := retryConsumption(ctx, db, feed, func(ctx context.Context, id int64) error { return processJob(ctx, db, feed, id, DummyHandler(feed)) }); err != nil {
			t.Fatal(err)
		}
		if count("SELECT COUNT(*) FROM dummy_effects WHERE event_id=2") != 1 {
			t.Fatal("retry effect missing")
		}
	})

	t.Run("bounded retries permanent failure and explicit replay", func(t *testing.T) {
		id := receiveEvent(search, 3)
		for range maxConsumerAttempts {
			execute("UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id)
			process(search, id, func(context.Context, *sql.Tx, Event) error { return errors.New("temporarily unavailable") })
		}
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='failed' AND attempts=5", id) != 1 {
			t.Fatal("retry limit did not quarantine job")
		}
		process(search, id, DummyHandler(search))
		if count("SELECT COUNT(*) FROM dummy_effects WHERE event_id=3") != 0 {
			t.Fatal("failed job retried automatically")
		}
		var successes atomic.Int32
		var workers sync.WaitGroup
		for range 2 {
			workers.Go(func() {
				if Replay(ctx, db, "consumer", id, "dependency recovered") == nil {
					successes.Add(1)
				}
			})
		}
		workers.Wait()
		if successes.Load() != 1 {
			t.Fatal("concurrent replay must schedule only once")
		}
		process(search, id, DummyHandler(search))
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='completed' AND attempts=6 AND cycle_attempts=1", id) != 1 {
			t.Fatal("replay lost attempt history")
		}
		if err := Replay(ctx, db, "consumer", id, "already complete"); err == nil {
			t.Fatal("completed job was replayed")
		}
		history, err := ReplayHistory(ctx, db, "consumer", id)
		if err != nil || len(history) != 1 || history[0].Reason != "dependency recovered" {
			t.Fatalf("replay history: %v %v", history, err)
		}
		id = receiveEvent(feed, 4)
		process(feed, id, func(context.Context, *sql.Tx, Event) error {
			return fmt.Errorf("unsupported operation: %w", ErrPermanent)
		})
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='failed' AND attempts=1", id) != 1 {
			t.Fatal("permanent failure did not stop immediately")
		}
	})

	t.Run("malformed and conflicting messages remain inspectable", func(t *testing.T) {
		inputs := []struct {
			messageID string
			body      []byte
		}{
			{"", nil},
			{"", []byte{0xff, '{'}},
			{"999", body(5)},
			{"5", []byte(`{"event_id":5,"event_type":"unknown","created_on":1}`)},
			{"1", bytes.Replace(body(1), []byte("99"), []byte("100"), 1)},
		}
		for _, input := range inputs {
			id, err := receive(ctx, db, feed, input.messageID, input.body)
			if err != nil {
				t.Fatal(err)
			}
			again, err := receive(ctx, db, feed, input.messageID, input.body)
			if err != nil || again != id {
				t.Fatal("invalid input fingerprint is unstable")
			}
			records, err := Inspect(ctx, db, "consumer", "failed", id, 0, 1)
			if err != nil || len(records) != 1 || records[0].Status != "failed" || !bytes.Equal(records[0].Body, input.body) {
				t.Fatalf("quarantine lost bytes: %v %v", records, err)
			}
			if err := Replay(ctx, db, "consumer", id, "try invalid input"); err == nil {
				t.Fatal("invalid/conflicting input accepted for replay")
			}
		}
	})

	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		url = "amqp://notex:notex-local@127.0.0.1:5673/"
	}
	b := &broker{url: url, prefix: prefix}
	if err := b.connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.close()
	defer func() {
		for _, queue := range []string{feed, search} {
			if _, err := b.channel.QueueDelete(queue, false, false, false); err != nil {
				t.Error(err)
			}
		}
	}()
	publish := func(queue string, eventID int64) {
		t.Helper()
		if err := b.publish(ctx, queue, eventID, body(eventID)); err != nil {
			t.Fatal(err)
		}
	}
	start := func(name string, handler SQLHandler) func() {
		t.Helper()
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- RunConsumer(workerCtx, db, url, prefix, name, handler) }()
		return func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("consumer failed to stop")
			}
		}
	}

	t.Run("broker redelivery after commit before ACK", func(t *testing.T) {
		publish(feed, 10)
		connection, err := amqp.Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		channel, err := connection.Channel()
		if err != nil {
			t.Fatal(err)
		}
		delivery, ok, err := channel.Get(feed, false)
		if err != nil || !ok {
			t.Fatal("missing delivery", err)
		}
		id, err := receive(ctx, db, feed, delivery.MessageId, delivery.Body)
		if err != nil {
			t.Fatal(err)
		}
		process(feed, id, DummyHandler(feed))
		connection.Close() // Deliberately omit ACK after the committed effect.
		stop := start("feed", DummyHandler(feed))
		defer stop()
		// A later sentinel proves the restarted consumer passed the redelivery.
		publish(feed, 11)
		waitFor("redelivery and sentinel", func() bool {
			return count("SELECT COUNT(*) FROM dummy_effects WHERE consumer=? AND event_id=11", feed) == 1
		})
		if count("SELECT attempts FROM consumption_jobs WHERE id=?", id) != 1 {
			t.Fatal("ACK loss repeated an effect")
		}
	})

	t.Run("database failure leaves broker delivery recoverable", func(t *testing.T) {
		execute(`CREATE TRIGGER reject_receipt BEFORE INSERT ON consumption_jobs FOR EACH ROW
			SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='receipt unavailable'`)
		publish(search, 12)
		failedBroker := &broker{url: url, prefix: prefix}
		sessionCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := consumeSession(sessionCtx, db, failedBroker, "search", func(ctx context.Context, id int64) error {
			return processJob(ctx, db, search, id, DummyHandler(search))
		})
		cancel()
		failedBroker.close()
		if err == nil || !strings.Contains(err.Error(), "receipt unavailable") {
			t.Fatalf("expected receipt database failure, got %v", err)
		}
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE event_id=12") != 0 {
			t.Fatal("receipt unexpectedly persisted")
		}
		execute("DROP TRIGGER reject_receipt")
		stop := start("search", DummyHandler(search))
		defer stop()
		waitFor("unacknowledged message recovered", func() bool {
			return count("SELECT COUNT(*) FROM dummy_effects WHERE consumer=? AND event_id=12", search) == 1
		})
	})

	t.Run("durable retry survives worker restart and isolates consumers", func(t *testing.T) {
		var failed atomic.Bool
		stop := start("search", func(context.Context, *sql.Tx, Event) error {
			failed.Store(true)
			return errors.New("search temporarily unavailable")
		})
		publish(search, 13)
		waitFor("durable failed attempt", func() bool {
			return failed.Load() && count("SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND event_id=13 AND attempts=1", search) == 1
		})
		stop()
		publish(feed, 13)
		stopFeed := start("feed", DummyHandler(feed))
		defer stopFeed()
		waitFor("independent feed effect", func() bool {
			return count("SELECT COUNT(*) FROM dummy_effects WHERE consumer=? AND event_id=13", feed) == 1
		})
		execute("UPDATE consumption_jobs SET next_attempt_on=0 WHERE consumer=? AND event_id=13", search)
		stop = start("search", DummyHandler(search))
		defer stop()
		waitFor("persisted search retry", func() bool {
			return count("SELECT COUNT(*) FROM dummy_effects WHERE consumer=? AND event_id=13", search) == 1
		})
	})

	t.Run("malformed broker delivery is recorded before ACK", func(t *testing.T) {
		invalid := []byte("{invalid JSON")
		if err := b.publish(ctx, feed, 14, invalid); err != nil {
			t.Fatal(err)
		}
		publish(feed, 15)
		publish(feed, 15)
		stop := start("feed", DummyHandler(feed))
		defer stop()
		publish(feed, 16)
		waitFor("poison message and duplicates passed", func() bool {
			return count("SELECT COUNT(*) FROM dummy_effects WHERE consumer=? AND event_id=16", feed) == 1
		})
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND message_id='14' AND status='failed'", feed) != 1 ||
			count("SELECT attempts FROM consumption_jobs WHERE consumer=? AND event_id=15", feed) != 1 {
			t.Fatal("quarantine/duplicate ACK behavior incorrect")
		}
	})

	t.Run("persisted retry progresses while broker is unreachable", func(t *testing.T) {
		id := receiveEvent(search, 17)
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- RunConsumer(workerCtx, db, "amqp://127.0.0.1:1/", prefix, "search", DummyHandler(search))
		}()
		defer func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("offline retry worker did not stop")
			}
		}()
		waitFor("retry independent of broker connection", func() bool {
			return count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='completed'", id) == 1
		})
	})

	t.Run("outbox failure repair replay and actual publication", func(t *testing.T) {
		result := execute(`INSERT INTO outbox_events(event_type,payload,created_on) VALUES ('post.created','{}',1)`)
		id, _ := result.LastInsertId()
		item := Event{ID: id, Type: PostCreated, Payload: json.RawMessage(`{}`), CreatedOn: 1}
		if err := deliver(ctx, db, b, item); err != nil {
			t.Fatal(err)
		}
		if err := Replay(ctx, db, "outbox", id, "still invalid"); err == nil {
			t.Fatal("invalid outbox replay accepted")
		}
		execute(`UPDATE outbox_events SET payload='{"post_id":99}' WHERE id=?`, id)
		if err := Replay(ctx, db, "outbox", id, "source payload repaired"); err != nil {
			t.Fatal(err)
		}
		if err := drain(ctx, db, b); err != nil {
			t.Fatal(err)
		}
		if count("SELECT COUNT(*) FROM outbox_events WHERE id=? AND published_on IS NOT NULL", id) != 1 {
			t.Fatal("replayed outbox was not published")
		}
		if count("SELECT COUNT(*) FROM event_replays WHERE scope='outbox' AND record_id=?", id) != 1 {
			t.Fatal("outbox replay audit missing")
		}
	})
	t.Run("CLI inspect replay and explicit dummy process", func(t *testing.T) {
		binary := filepath.Join(t.TempDir(), "notex-events")
		build := exec.Command("go", "build", "-p", "2", "-o", binary, "../../cmd/events")
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build events CLI: %v %s", err, output)
		}
		env := append(os.Environ(), "MYSQL_DSN="+cfg.FormatDSN(), "RABBITMQ_URL="+url,
			"RABBITMQ_QUEUE_PREFIX="+prefix)
		command := func(args ...string) []byte {
			t.Helper()
			command := exec.Command(binary, args...)
			command.Env = env
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("CLI %v: %v %s", args, err, output)
			}
			return output
		}
		id := receiveEvent(search, 18)
		process(search, id, func(context.Context, *sql.Tx, Event) error { return ErrPermanent })
		var result struct {
			Records []Record       `json:"records"`
			Replays []ReplayRecord `json:"replays"`
		}
		if err := json.Unmarshal(command("list", "--id", strconv.FormatInt(id, 10), "--status", "failed"), &result); err != nil || len(result.Records) != 1 {
			t.Fatalf("CLI list: %v %+v", err, result)
		}
		command("replay", "--id", strconv.FormatInt(id, 10), "--reason", "CLI recovery check")
		if err := json.Unmarshal(command("list", "--id", strconv.FormatInt(id, 10)), &result); err != nil || len(result.Replays) != 1 || result.Replays[0].PreviousError != ErrPermanent.Error() {
			t.Fatalf("CLI replay audit: %v %+v", err, result)
		}
		// Also exercise the outbox projection in the inspection SQL.
		command("list", "--scope", "outbox", "--status", "completed")
		worker := exec.Command(binary, "dummy")
		worker.Env = env
		var output bytes.Buffer
		worker.Stdout, worker.Stderr = &output, &output
		if err := worker.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- worker.Wait() }()
		defer func() {
			worker.Process.Signal(os.Interrupt)
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("dummy exit: %v %s", err, output.String())
				}
			case <-time.After(10 * time.Second):
				worker.Process.Kill()
				<-done
				t.Error("dummy CLI failed to stop")
			}
		}()
		publish(feed, 19)
		publish(search, 19)
		waitFor("CLI replays and consumes both queues", func() bool {
			return count("SELECT COUNT(*) FROM dummy_effects WHERE event_id=18") == 1 &&
				count("SELECT COUNT(*) FROM dummy_effects WHERE event_id=19") == 2
		})
	})

	t.Run("external claim releases SQL and fences expired workers", func(t *testing.T) {
		id := receiveEvent(feed, 100)
		entered, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		db.SetMaxOpenConns(1)
		defer db.SetMaxOpenConns(10)
		go func() {
			done <- processExternal(ctx, db, feed, id, func(context.Context, Event) error {
				close(entered)
				<-release
				return nil
			})
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("claim failed to enter external handler")
		}
		// With a one-connection pool this update also proves the effect holds no SQL transaction.
		queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := db.ExecContext(queryCtx, "UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id)
		cancel()
		if err != nil {
			close(release)
			<-done
			t.Fatal(err)
		}
		err = processExternal(ctx, db, feed, id, func(context.Context, Event) error { return ErrPermanent })
		close(release)
		firstErr := <-done
		if err != nil || firstErr != nil || count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='failed' AND attempts=2", id) != 1 {
			t.Fatalf("old worker overwrote new claim: %v %v", err, firstErr)
		}
	})

	t.Run("external bounded retries waiting and replay", func(t *testing.T) {
		id := receiveEvent(feed, 101)
		for attempt := 1; attempt <= maxConsumerAttempts; attempt++ {
			execute("UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id)
			if err := processExternal(ctx, db, feed, id, func(context.Context, Event) error { return errors.New("remote timeout") }); err != nil {
				t.Fatal(err)
			}
		}
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='failed' AND attempts=5", id) != 1 {
			t.Fatal("external retry bound missing")
		}
		if err := Replay(ctx, db, "consumer", id, "dependency restored"); err != nil {
			t.Fatal(err)
		}
		if err := processExternal(ctx, db, feed, id, func(context.Context, Event) error { return ErrWaiting }); err != nil {
			t.Fatal(err)
		}
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='waiting'", id) != 1 {
			t.Fatal("waiting task lost")
		}
		if err := Replay(ctx, db, "consumer", id, "next stage available"); err != nil {
			t.Fatal(err)
		}
		if err := processExternal(ctx, db, feed, id, func(context.Context, Event) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='completed' AND attempts=7", id) != 1 {
			t.Fatal("external replay lost attempt identity")
		}
	})

	t.Run("expired final external claim is auditable", func(t *testing.T) {
		id := receiveEvent(feed, 102)
		for range maxConsumerAttempts {
			execute("UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id)
			_, attempt, _, err := claimExternal(ctx, db, feed, id)
			if err != nil || attempt == 0 {
				t.Fatal("claim not retained", err)
			}
		}
		execute("UPDATE consumption_jobs SET next_attempt_on=0 WHERE id=?", id)
		if err := processExternal(ctx, db, feed, id, func(context.Context, Event) error { t.Fatal("exhausted task executed"); return nil }); err != nil {
			t.Fatal(err)
		}
		if count("SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='failed' AND attempts=5", id) != 1 {
			t.Fatal("abandoned task not failed")
		}
	})

}
