package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNotifyCoalesces(t *testing.T) {
	wake := make(chan struct{}, 1)
	for range 10000 {
		outbox.Notify(wake)
	}
	if len(wake) != 1 {
		t.Fatal("expected one coalesced notification")
	}
	outbox.Notify(nil)
}

type message struct {
	ID        int64            `json:"event_id"`
	Type      string           `json:"event_type"`
	Payload   map[string]int64 `json:"payload"`
	CreatedOn int64            `json:"created_on"`
}

// Explicit opt-in: uses isolated database/queues and stops only this project's
// Compose broker for 60 seconds, retaining its data volume.
func TestPublisherIntegration(t *testing.T) {
	if os.Getenv("NOTEX_INTEGRATION") != "1" {
		t.Skip("set NOTEX_INTEGRATION=1 with the local Compose broker running")
	}
	root := filepath.Join("..", "..", "..")
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid TEST_MYSQL_DSN")
	}
	cfg.MultiStatements = true
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("notex_delivery_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE " + name); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = name
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
	for _, migration := range []string{"0001_accounts_follow.sql", "0002_posts.sql", "0003_post_requests.sql", "0004_outbox_events.sql"} {
		body, err := os.ReadFile(filepath.Join(root, "migrations", migration))
		if err != nil {
			t.Fatal(err)
		}
		execute(string(body))
	}
	execute("INSERT INTO users (id, username, password_hash, created_on) VALUES (1,'publisher','test',1),(2,'reader','test',1)")
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		url = "amqp://notex:notex-local@127.0.0.1:5673/"
	}
	connect := func() (*amqp.Connection, *amqp.Channel) {
		t.Helper()
		connection, err := amqp.Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		channel, err := connection.Channel()
		if err != nil {
			connection.Close()
			t.Fatal(err)
		}
		return connection, channel
	}
	compose := func(action string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "docker", "compose", action, "rabbitmq")
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("broker %s: %v %s", action, err, output)
		}
	}
	waitFor := func(label string, timeout time.Duration, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if ready() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("timed out: " + label)
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var value int
		if err := db.QueryRow(query, args...).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	wake := make(chan struct{}, 1)
	posts := post.New(db, wake)
	follows := follow.New(db, wake)
	var cancelWorker context.CancelFunc
	var workerDone chan struct{}
	start := func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancelWorker = cancel
		workerDone = make(chan struct{})
		go func() { defer close(workerDone); outbox.Run(ctx, db, wake, url, name) }()
	}
	stop := func() {
		if cancelWorker == nil {
			return
		}
		cancelWorker()
		select {
		case <-workerDone:
		case <-time.After(10 * time.Second):
			t.Error("publisher failed to stop")
		}
		cancelWorker = nil
	}
	defer stop()
	defer func() {
		stop()
		connection, channel := connect()
		defer connection.Close()
		for _, suffix := range []string{".feed", ".search"} {
			if _, err := channel.QueueDelete(name+suffix, false, false, false); err != nil {
				t.Error(err)
			}
		}
	}()
	create := func(key string) post.Post {
		t.Helper()
		item, err := posts.Create(context.Background(), 1, key, "delivery "+key)
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	awaitPublished := func() {
		waitFor("all pending events confirmed", 10*time.Minute, func() bool {
			return count("SELECT COUNT(*) FROM outbox_events WHERE published_on IS NULL AND failed_on IS NULL") == 0
		})
	}
	read := func(suffix string) map[int64]int {
		t.Helper()
		connection, channel := connect()
		defer connection.Close()
		seen := map[int64]int{}
		for {
			delivery, ok, err := channel.Get(name+suffix, true)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				return seen
			}
			var actual message
			if err := json.Unmarshal(delivery.Body, &actual); err != nil {
				t.Fatal(err)
			}
			var expected message
			var payload []byte
			if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE id=?", actual.ID).
				Scan(&expected.ID, &expected.Type, &payload, &expected.CreatedOn); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(payload, &expected.Payload); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, expected) || delivery.MessageId != strconv.FormatInt(actual.ID, 10) || delivery.DeliveryMode != amqp.Persistent {
				t.Fatalf("event %d did not match durable identity/content", actual.ID)
			}
			if suffix == ".search" && !strings.HasPrefix(actual.Type, "post.") {
				t.Fatal("relationship sent to search")
			}
			seen[actual.ID]++
		}
	}
	// Startup recovers an event even when its notification has disappeared.
	first := create("startup")
	<-wake
	start()
	awaitPublished()
	if len(read(".feed")) != 1 || len(read(".search")) != 1 {
		t.Fatal("startup delivery missing")
	}
	if err := follows.Follow(context.Background(), 2, 1); err != nil {
		t.Fatal(err)
	}
	awaitPublished()
	if len(read(".feed")) != 1 || len(read(".search")) != 0 {
		t.Fatal("relationship routing")
	}
	t.Log("startup recovery, service notification and independent routing passed")

	// Reserve a smaller event ID without committing it; a larger ID is delivered first.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := outbox.RecordPost(context.Background(), tx, outbox.PostCreated, 1, first.ID, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	create("later-id")
	awaitPublished()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// No channel notification: recovery timer must discover this late commit.
	awaitPublished()
	if len(read(".feed")) != 2 || len(read(".search")) != 2 {
		t.Fatal("late commit skipped")
	}
	t.Log("late smaller ID recovered by periodic scan")

	// Removing one destination must not be hidden by a successful delivery to another.
	connection, channel := connect()
	if _, err := channel.QueueDelete(name+".search", false, false, false); err != nil {
		t.Fatal(err)
	}
	connection.Close()
	create("unroutable")
	waitFor("unroutable persisted", 10*time.Second, func() bool {
		return count("SELECT COUNT(*) FROM outbox_events WHERE published_on IS NULL AND last_error LIKE 'unroutable%'") == 1
	})
	awaitPublished()
	feed, search := read(".feed"), read(".search")
	if len(feed) != 1 || len(search) != 1 {
		t.Fatal("route recovery missing")
	}
	for id, n := range feed {
		if n != 2 || search[id] != 1 {
			t.Fatal("partial route retry identity")
		}
	}
	t.Log("mandatory return persisted; missing queue recovered; partial delivery safely repeated")

	// Confirmed messages survive a worker restart when MySQL cannot record success.
	for round := 0; round < 20; round++ {
		execute(`CREATE TRIGGER fail_publish_mark BEFORE UPDATE ON outbox_events FOR EACH ROW
			BEGIN IF NEW.published_on IS NOT NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected publish mark failure'; END IF; END`)
		item := create(fmt.Sprintf("confirm-%d", round))
		waitFor("both broker confirms before failed DB mark", 10*time.Second, func() bool {
			return count("SELECT COUNT(*) FROM outbox_events WHERE payload->>'$.post_id'=? AND last_error LIKE 'record publication:%'", item.ID) == 1
		})
		stop()
		if count("SELECT COUNT(*) FROM outbox_events WHERE payload->>'$.post_id'=? AND published_on IS NULL", item.ID) != 1 {
			t.Fatal("failed mark lost pending event")
		}
		execute("DROP TRIGGER fail_publish_mark")
		start()
		awaitPublished()
		feed, search = read(".feed"), read(".search")
		if len(feed) != 1 || len(search) != 1 {
			t.Fatal("confirm recovery missing")
		}
		for id, n := range feed {
			if n != 2 || search[id] != 2 {
				t.Fatal("confirm recovery must preserve duplicate ID")
			}
		}
	}
	t.Log("20 rounds of confirmed delivery / failed DB mark / worker restart passed")

	execute("INSERT INTO outbox_events (event_type,payload,created_on) VALUES ('unknown', '{}', 1), ('post.created','{}',1)")
	outbox.Notify(wake)
	waitFor("invalid records isolated", 10*time.Second, func() bool {
		return count("SELECT COUNT(*) FROM outbox_events WHERE failed_on IS NOT NULL AND last_error IS NOT NULL") == 2
	})

	// Keep one confirmed event in both durable queues across the broker restart.
	create("before-broker-stop")
	awaitPublished()
	compose("stop")
	brokerStopped := true
	defer func() {
		if brokerStopped {
			compose("start")
		}
	}()
	downAt := time.Now()
	for i := 0; i < 10000; i++ {
		create(fmt.Sprintf("offline-%d", i))
		if i%2000 == 0 {
			t.Logf("created %d/10000 events while broker is down", i)
		}
	}
	if count("SELECT COUNT(*) FROM outbox_events WHERE published_on IS NULL AND failed_on IS NULL") != 10000 {
		t.Fatal("offline work missing")
	}
	if count("SELECT COUNT(*) FROM outbox_events WHERE last_error IS NOT NULL AND failed_on IS NULL") == 0 {
		t.Fatal("connection failure not recorded")
	}
	if remaining := 60*time.Second - time.Since(downAt); remaining > 0 {
		time.Sleep(remaining)
	}
	stop()
	compose("start")
	brokerStopped = false
	start()
	awaitPublished()
	feed, search = read(".feed"), read(".search")
	if len(feed) != 10001 || len(search) != 10001 {
		t.Fatalf("recovered identities feed=%d search=%d", len(feed), len(search))
	}
	for id := range feed {
		if search[id] == 0 {
			t.Fatalf("event %d missing from search", id)
		}
	}
	t.Log("broker stopped >=60s, 10000 service transactions retained, restart recovered both queues and previously confirmed messages")
	stop()
}
