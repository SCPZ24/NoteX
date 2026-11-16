package feed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Uses an isolated database, queues and Redis namespace. The optional outage
// scenario stops only the newly configured Compose Redis for at least 60 seconds.
func TestAuthorIndexIntegration(t *testing.T) {
	if os.Getenv("NOTEX_FEED_INTEGRATION") != "1" {
		t.Skip("set NOTEX_FEED_INTEGRATION=1")
	}
	ctx := context.Background()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MultiStatements = true
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	admin.SetMaxOpenConns(1)
	defer admin.Close()
	prefix := fmt.Sprintf("notex_feed_%d", time.Now().UnixNano())
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
	db.SetMaxOpenConns(4)
	defer db.Close()
	execute := func(t *testing.T, query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var result int
		if err := db.QueryRow(query, args...).Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	files, err := filepath.Glob("../../../migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		execute(t, string(body))
	}
	execute(t, "INSERT INTO users(id,username,password_hash,created_on) VALUES(1,'author','fixture',1),(2,'reader','fixture',1)")
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6380/0"
	}
	service, err := New(db, redisURL, prefix, "push", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if err := service.redis.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	policy, err := service.redis.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil || policy["maxmemory-policy"] != "noeviction" {
		t.Fatal("test Redis requires noeviction", err)
	}
	defer func() {
		var keys []string
		iterator := service.redis.Scan(ctx, 0, prefix+":*", 100).Iterator()
		for iterator.Next(ctx) {
			keys = append(keys, iterator.Val())
		}
		if err := iterator.Err(); err != nil {
			t.Error(err)
			return
		}
		if len(keys) > 0 {
			if err := service.redis.Del(ctx, keys...).Err(); err != nil {
				t.Error(err)
			}
		}
	}()
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		url = "amqp://notex:notex-local@127.0.0.1:5673/"
	}
	connection, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".feed", ".search", ".feed.plan", ".feed.chunk"} {
		if _, err := channel.QueueDeclare(prefix+suffix, true, false, false, false, amqp.Table{"x-queue-type": "classic"}); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		cleanupConnection, err := amqp.Dial(url)
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanupConnection.Close()
		cleanupChannel, err := cleanupConnection.Channel()
		if err != nil {
			t.Error(err)
			return
		}
		for _, suffix := range []string{".feed", ".search", ".feed.plan", ".feed.chunk"} {
			if _, err := cleanupChannel.QueueDelete(prefix+suffix, false, false, false); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := channel.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	wait := func(t *testing.T, label string, timeout time.Duration, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if ready() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("timed out: " + label)
	}
	publish := func(t *testing.T, event outbox.Event) {
		t.Helper()
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := channel.PublishWithContext(ctx, "", prefix+".feed", true, false,
			amqp.Publishing{MessageId: strconv.FormatInt(event.ID, 10), Body: body, DeliveryMode: amqp.Persistent}); err != nil {
			t.Fatal(err)
		}
		select {
		case confirmed := <-confirms:
			if !confirmed.Ack {
				t.Fatal("publication rejected")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("publication confirmation timeout")
		}
	}
	start := func(t *testing.T, handler outbox.ExternalHandler) (context.CancelFunc, func()) {
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := outbox.RunExternalConsumer(workerCtx, db, url, prefix, "feed", handler); err != nil {
				t.Error(err)
			}
		}()
		stop := func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Feed consumer shutdown timeout")
			}
		}
		t.Cleanup(stop)
		return cancel, stop
	}
	visible := func(item post.Post) bool {
		return service.redis.ZScore(ctx, service.keys(item.Author.ID)[0], member(Entry{item.ID, item.CreatedOn})).Err() == nil
	}
	posts := post.New(db, nil)
	create := func(t *testing.T, key string) (post.Post, outbox.Event) {
		t.Helper()
		item, err := posts.Create(ctx, 1, key, key)
		if err != nil {
			t.Fatal(err)
		}
		var event outbox.Event
		if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE event_type='post.created' AND payload->>'$.post_id'=?", item.ID).
			Scan(&event.ID, &event.Type, &event.Payload, &event.CreatedOn); err != nil {
			t.Fatal(err)
		}
		return item, event
	}

	t.Run("exact int64 ordering same-second pagination duplicates and empty", func(t *testing.T) {
		entries := []Entry{{ID: 9007199254740993, CreatedOn: 1700000000}, {ID: 9007199254740992, CreatedOn: 1700000000}, {ID: 9223372036854775807, CreatedOn: 1700000001}}
		for _, entry := range entries {
			for range 2 {
				if err := service.apply(ctx, 10, entry, false, fixtureAuthorGeneration(t, service, 10)); err != nil {
					t.Fatal(err)
				}
			}
		}
		first, err := service.ReadAuthor(ctx, 10, nil, 2)
		if err != nil || !reflect.DeepEqual(first, []Entry{entries[2], entries[0]}) {
			t.Fatalf("first page: %v %v", first, err)
		}
		second, err := service.ReadAuthor(ctx, 10, &first[1], 2)
		if err != nil || !reflect.DeepEqual(second, []Entry{entries[1]}) {
			t.Fatalf("second page: %v %v", second, err)
		}
		empty, err := service.ReadAuthor(ctx, 11, nil, 2)
		if err != nil || empty == nil || len(empty) != 0 {
			t.Fatal("empty index", err)
		}
		if _, err := service.ReadAuthor(ctx, 10, &Entry{}, 2); err == nil {
			t.Fatal("invalid cursor accepted")
		}
	})

	t.Run("20 late adds cannot resurrect deleted posts", func(t *testing.T) {
		for round := range 20 {
			entry := Entry{ID: int64(round + 1), CreatedOn: 1700000000}
			if err := service.apply(ctx, 20, entry, false, fixtureAuthorGeneration(t, service, 20)); err != nil {
				t.Fatal(err)
			}
			if err := service.apply(ctx, 20, entry, true, fixtureAuthorGeneration(t, service, 20)); err != nil {
				t.Fatal(err)
			}
			if err := service.apply(ctx, 20, entry, false, fixtureAuthorGeneration(t, service, 20)); err != nil {
				t.Fatal(err)
			}
		}
		entries, err := service.ReadAuthor(ctx, 20, nil, 100)
		if err != nil || len(entries) != 0 {
			t.Fatal("deleted posts resurrected", err)
		}
	})

	t.Run("20 exits after Redis effect before completion and ACK", func(t *testing.T) {
		for round := range 20 {
			item, event := create(t, fmt.Sprintf("ack-loss-%d", round))
			publish(t, event)
			effectDone := make(chan struct{})
			workerCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				done <- outbox.RunExternalConsumer(workerCtx, db, url, prefix, "feed", func(effectCtx context.Context, e outbox.Event) error {
					if err := service.Handle(effectCtx, e); err != nil {
						return err
					}
					cancel() // The durable claim remains, with no completed write or broker ACK.
					close(effectDone)
					return nil
				})
			}()
			select {
			case <-effectDone:
			case <-time.After(10 * time.Second):
				cancel()
				t.Fatal("effect not reached")
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !visible(item) || count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE event_id=? AND status='pending' AND attempts=1", event.ID) != 1 {
				t.Fatal("uncertain effect not recoverable")
			}
			// Advance the persisted lease deadline to exercise takeover without a 30s sleep per round.
			execute(t, "UPDATE consumption_jobs SET next_attempt_on=0 WHERE event_id=?", event.ID)
			_, stop := start(t, service.Handle)
			wait(t, "effect reconciled", 10*time.Second, func() bool {
				return count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE event_id=? AND status='completed' AND attempts=2", event.ID) == 1
			})
			stop()
			if !visible(item) {
				t.Fatal("recovery lost index member")
			}
		}
	})

	t.Run("20 completion writes fail after Redis succeeds", func(t *testing.T) {
		failures := make(chan struct{}, 20)
		logger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, testLogWriter(func(body []byte) (int, error) {
			if bytes.Contains(body, []byte(prefix+".feed")) && bytes.Contains(body, []byte("injected finish failure")) {
				failures <- struct{}{}
			}
			return len(body), nil
		})), nil)))
		defer slog.SetDefault(logger)
		t.Cleanup(func() { execute(t, "DROP TRIGGER IF EXISTS reject_finish") })
		for round := range 20 {
			item, event := create(t, fmt.Sprintf("finish-failure-%d", round))
			execute(t, `CREATE TRIGGER reject_finish BEFORE UPDATE ON consumption_jobs FOR EACH ROW
    BEGIN IF OLD.status='pending' AND NEW.status='completed' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected finish failure'; END IF; END`)
			publish(t, event)
			_, stop := start(t, service.Handle)
			select {
			case <-failures:
			case <-time.After(10 * time.Second):
				t.Fatal("SQL finish failure not observed")
			}
			stop()
			if !visible(item) {
				t.Fatal("SQL finish failed before Redis effect")
			}
			execute(t, "DROP TRIGGER reject_finish")
			if count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE event_id=? AND status='pending'", event.ID) != 1 {
				t.Fatal("failed finish lost task")
			}
			execute(t, "UPDATE consumption_jobs SET next_attempt_on=0 WHERE event_id=?", event.ID)
			_, stop = start(t, service.Handle)
			wait(t, "failed finish recovered", 10*time.Second, func() bool {
				return count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE event_id=? AND status='completed'", event.ID) == 1
			})
			stop()
			if !visible(item) {
				t.Fatal("recovery lost effect")
			}
		}
	})

	t.Run("normal API startup publication deletion waiting and Redis recovery", func(t *testing.T) {
		binary := filepath.Join(t.TempDir(), "notex-api")
		build := exec.Command("go", "build", "-p", "2", "-o", binary, "../..")
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build API: %v %s", err, output)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		listener.Close()
		token := strings.Repeat("a", 64)
		hash := sha256.Sum256([]byte(token))
		execute(t, "INSERT INTO sessions(token_hash,user_id,expires_on) VALUES(?,?,?)", hash[:], 1, time.Now().Add(time.Hour).Unix())
		worker := exec.Command(binary)
		worker.Env = append(os.Environ(), "MYSQL_DSN="+cfg.FormatDSN(), "HTTP_ADDR="+address, "RABBITMQ_URL="+url,
			"RABBITMQ_QUEUE_PREFIX="+prefix, "REDIS_URL="+redisURL, "FEED_MODE=push", "FEED_PULL_AUTHORS=", "GOMAXPROCS=2", "GOMEMLIMIT=512MiB", "GIN_MODE=release")
		var output bytes.Buffer
		worker.Stdout, worker.Stderr = &output, &output
		if err := worker.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			worker.Process.Signal(os.Interrupt)
			done := make(chan error, 1)
			go func() { done <- worker.Wait() }()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("API exit: %v %s", err, output.String())
				}
			case <-time.After(15 * time.Second):
				worker.Process.Kill()
				<-done
				t.Error("API shutdown timeout")
			}
		}()
		client := &http.Client{Timeout: 5 * time.Second}
		wait(t, "API ready", 10*time.Second, func() bool {
			response, err := client.Get("http://" + address + "/readyz")
			if err != nil {
				return false
			}
			response.Body.Close()
			return response.StatusCode == 200
		})
		request := func(method, path, key, body string, status int) []byte {
			t.Helper()
			req, err := http.NewRequest(method, "http://"+address+path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", key)
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			result, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != status {
				t.Fatalf("%s %s: %d %s %v", method, path, response.StatusCode, result, err)
			}
			return result
		}
		request("GET", "/api/v1/feed", "", "", 200)
		var item post.Post
		publishedAt := time.Now()
		if err := json.Unmarshal(request("POST", "/api/v1/posts", "http-post", `{"content":"HTTP projection"}`, 201), &item); err != nil {
			t.Fatal(err)
		}
		wait(t, "HTTP post indexed", 10*time.Second, func() bool { return visible(item) })
		wait(t, "HTTP post delivered to own reader index", 10*time.Second, func() bool {
			return service.redis.ZScore(ctx, service.readerKeys(1)[0], member(Entry{ID: item.ID, CreatedOn: item.CreatedOn})).Err() == nil
		})
		t.Logf("HTTP request to observed author index visibility %.3f ms (20ms polling)", float64(time.Since(publishedAt).Microseconds())/1000)
		request("DELETE", fmt.Sprintf("/api/v1/posts/%d", item.ID), "", "", 200)
		wait(t, "HTTP deletion indexed", 10*time.Second, func() bool {
			return service.redis.SIsMember(ctx, service.keys(1)[1], strconv.FormatInt(item.ID, 10)).Val() && !visible(item)
		})
		request("PUT", "/api/v1/users/2/follow", "", "", 200)
		wait(t, "relationship completed", 10*time.Second, func() bool {
			return count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND status='completed' AND body LIKE '%follow.created%'", prefix+".feed") == 1
		})
		if os.Getenv("NOTEX_REDIS_OUTAGE") != "1" {
			return
		}
		compose := func(action string) {
			t.Helper()
			command := exec.Command("docker", "compose", action, "redis")
			command.Dir = "../../.."
			if result, err := command.CombinedOutput(); err != nil {
				t.Fatalf("Redis %s: %v %s", action, err, result)
			}
		}
		compose("stop")
		stopped := time.Now()
		defer compose("start")
		if err := json.Unmarshal(request("POST", "/api/v1/posts", "redis-outage", `{"content":"survives Redis outage"}`, 201), &item); err != nil {
			t.Fatal(err)
		}
		request("GET", "/readyz", "", "", 200)
		var fallbackPage post.Page
		if err := json.Unmarshal(request("GET", "/api/v1/feed", "", "", 200), &fallbackPage); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, candidate := range fallbackPage.Items {
			found = found || candidate.ID == item.ID
		}
		if !found {
			t.Fatal("HTTP SQL fallback missed committed publication during Redis outage")
		}
		var eventID int64
		if err := db.QueryRow("SELECT id FROM outbox_events WHERE event_type='post.created' AND payload->>'$.post_id'=?", item.ID).Scan(&eventID); err != nil {
			t.Fatal(err)
		}
		wait(t, "Redis retries exhausted", 55*time.Second, func() bool {
			return count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE event_id=? AND status='failed' AND attempts=5", eventID) == 1
		})
		if remaining := 60*time.Second - time.Since(stopped); remaining > 0 {
			time.Sleep(remaining)
		}
		compose("start")
		wait(t, "Redis restarted", 10*time.Second, func() bool { return service.redis.Ping(ctx).Err() == nil })
		var jobID int64
		if err := db.QueryRow("SELECT id FROM consumption_jobs WHERE event_id=?", eventID).Scan(&jobID); err != nil {
			t.Fatal(err)
		}
		eventsBinary := filepath.Join(t.TempDir(), "notex-events")
		build = exec.Command("go", "build", "-p", "2", "-o", eventsBinary, "../../cmd/events")
		if result, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build events: %v %s", err, result)
		}
		replay := exec.Command(eventsBinary, "replay", "--scope", "consumer", "--id", strconv.FormatInt(jobID, 10), "--reason", "Redis restored after 60s outage")
		replay.Env = worker.Env
		if result, err := replay.CombinedOutput(); err != nil {
			t.Fatalf("CLI replay: %v %s", err, result)
		}
		wait(t, "replayed Redis effect", 10*time.Second, func() bool {
			return visible(item) && count(t, "SELECT COUNT(*) FROM consumption_jobs WHERE id=? AND status='completed' AND attempts=6", jobID) == 1
		})
		if count(t, "SELECT COUNT(*) FROM event_replays WHERE record_id=?", jobID) != 1 {
			t.Fatal("replay audit missing")
		}
		t.Logf("Redis outage lasted at least %.2fs, API remained available, CLI replay completed", time.Since(stopped).Seconds())
	})
	t.Run("complete author pagination matches authoritative SQL", func(t *testing.T) {
		rows, err := db.Query("SELECT id,created_on FROM posts WHERE author_id=1 AND deleted_on IS NULL ORDER BY created_on DESC,id DESC")
		if err != nil {
			t.Fatal(err)
		}
		var expected []Entry
		for rows.Next() {
			var entry Entry
			if err := rows.Scan(&entry.ID, &entry.CreatedOn); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			expected = append(expected, entry)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		var actual []Entry
		var before *Entry
		for {
			page, err := service.ReadAuthor(ctx, 1, before, 7)
			if err != nil {
				t.Fatal(err)
			}
			actual = append(actual, page...)
			if len(actual) > len(expected) {
				t.Fatal("author pagination failed to terminate")
			}
			if len(page) < 7 {
				break
			}
			last := page[len(page)-1]
			before = &last
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("author index mismatch: actual=%v expected=%v", actual, expected)
		}
		indexBytes, err := service.redis.MemoryUsage(ctx, service.keys(1)[0]).Result()
		if err != nil {
			t.Fatal(err)
		}
		markerBytes, err := service.redis.MemoryUsage(ctx, service.keys(1)[1]).Result()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("author=1 active_posts=%d index_bytes=%d deletion_marker_bytes=%d", len(actual), indexBytes, markerBytes)
	})

	if os.Getenv("NOTEX_FANOUT_INTEGRATION") == "1" {
		t.Run("stage four fanout", func(t *testing.T) { verifyFanout(t, db, service, url, prefix) })
	}
	if os.Getenv("NOTEX_SCALE_FOLLOWERS") != "" {
		t.Run("F scale", func(t *testing.T) { verifyScale(t, db, service, url, prefix) })
	}
	if os.Getenv("NOTEX_READING_INTEGRATION") == "1" {
		t.Run("stage five reading", func(t *testing.T) { verifyReading(t, db, service, redisURL, url, prefix) })
	}
}

// Capture the actual injected finish error, independent of reconnect timing.
type testLogWriter func([]byte) (int, error)

func (w testLogWriter) Write(body []byte) (int, error) { return w(body) }

func fixtureAuthorGeneration(t *testing.T, s *Service, author int64) string {
	t.Helper()
	sources := []cacheSource{{owner: author, author: true, signature: s.policy}}
	if err := s.touchSources(context.Background(), sources); err != nil {
		t.Fatal(err)
	}
	return sources[0].state.generation
}

// Stage-four fixtures explicitly establish active empty reader caches before
// publication; cold/ready initialization itself is covered by stage-five tests.
func fixtureActiveReaders(t *testing.T, s *Service, targets []recipient) {
	t.Helper()
	ctx := context.Background()
	sources := make([]cacheSource, 0, len(targets))
	for _, target := range targets {
		sources = append(sources, cacheSource{owner: target.UserID, signature: s.policy})
	}
	if err := s.touchSources(ctx, sources); err != nil {
		t.Fatal(err)
	}
	for _, source := range sources {
		if err := s.redis.HSet(ctx, s.cacheKeys(source)[3], "ready", "1").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.readerGenerations(ctx, targets); err != nil {
		t.Fatal(err)
	}
}
