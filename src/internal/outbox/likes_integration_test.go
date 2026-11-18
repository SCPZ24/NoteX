package outbox_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAdaptiveLikesIntegration(t *testing.T) {
	if os.Getenv("NOTEX_LIKE_INTEGRATION") != "1" {
		t.Skip("set NOTEX_LIKE_INTEGRATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	prefix := fmt.Sprintf("notex_like_test_%d", time.Now().UnixNano())
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
	cfg, err := mysql.ParseDSN(dsn)
	check(err)
	cfg.MultiStatements = true
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	check(err)
	defer admin.Close()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+prefix)
	check(err)
	defer func() {
		_, err := admin.Exec("DROP DATABASE " + prefix)
		if err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = prefix
	db, err := sql.Open("mysql", cfg.FormatDSN())
	check(err)
	defer db.Close()
	db.SetMaxOpenConns(4)
	for _, file := range []string{"0001_accounts_follow.sql", "0002_posts.sql", "0008_post_likes.sql"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", file))
		check(err)
		_, err = db.ExecContext(ctx, string(body))
		check(err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO users(id,username,password_hash,created_on) VALUES (1,'author','x',1),(32768,'viewer','x',1); INSERT INTO posts(id,author_id,content,created_on) VALUES (1,1,'sample',1),(2,1,'reads',1)")
	check(err)
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6380/0"
	}
	store, err := cache.Open(url, prefix)
	check(err)
	defer store.Redis.Close()
	defer store.Close()
	defer func() {
		keys, err := store.Redis.Keys(context.Background(), prefix+":*").Result()
		if err == nil && len(keys) > 0 {
			store.Redis.Del(context.Background(), keys...)
		}
	}()
	brokerURL := os.Getenv("TEST_RABBITMQ_URL")
	if brokerURL == "" {
		brokerURL = "amqp://notex:notex-local@127.0.0.1:5673/"
	}
	connection, err := amqp.Dial(brokerURL)
	check(err)
	defer connection.Close()
	channel, err := connection.Channel()
	check(err)
	defer channel.Close()
	defer func() {
		for _, name := range []string{"likes", "feed", "search"} {
			cleanup, err := connection.Channel()
			if err == nil {
				cleanup.QueueDelete(prefix+"."+name, false, false, false)
				cleanup.Close()
			}
		}
	}()
	service := post.New(db, nil)
	service.UseCache(store)
	warmed := 0
	service.UseAdaptiveLikes(func(context.Context, int64) error { warmed++; return nil })
	for count := 1; count <= 257; count++ {
		async, err := service.SetLike(ctx, 1, 1, true)
		check(err)
		if async != (count == 257) {
			t.Fatalf("write %d async=%v", count, async)
		}
	}
	if warmed != 1 {
		t.Fatal("missing promotion")
	}
	for count := 1; count <= 2049; count++ {
		_, err := service.GetDetail(ctx, 2, 1)
		check(err)
		if count == 2048 && warmed != 1 {
			t.Fatal("read gate opened early")
		}
	}
	if warmed != 2 {
		t.Fatal("read gate did not open")
	}
	async, err := service.SetLike(ctx, 1, 32768, true)
	check(err)
	if !async {
		t.Fatal("second segment not hot")
	}
	var writers sync.WaitGroup
	for range 12 {
		writers.Go(func() {
			_, err := service.SetLike(ctx, 1, 32768, true)
			if err != nil {
				t.Error(err)
			}
		})
	}
	writers.Wait()
	_, err = service.SetLike(ctx, 1, 32768, false)
	check(err)
	detail, err := service.GetDetail(ctx, 1, 32768)
	check(err)
	if detail.LikedByMe {
		t.Fatal("cancel not visible before relay")
	}
	stream := prefix + ":like:stream"
	if n := store.Redis.XLen(ctx, stream).Val(); n != 2 {
		t.Fatalf("expected two state changes, got %d", n)
	}
	base := prefix + ":like:1"
	cool := func() {
		t.Helper()
		check(store.Redis.Del(ctx, base+":reads", base+":writes").Err())
		check(store.Redis.Do(ctx, "ZADD", prefix+":like:cool", 0, "1").Err())
		check(service.CoolLikes(ctx))
	}
	cool()
	if store.Redis.Exists(ctx, base+":hot").Val() != 1 {
		t.Fatal("cooled while pending")
	}
	task, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { stop(); workers.Wait() }()
	workers.Go(func() { outbox.RunLikeRelay(task, store.Redis, brokerURL, prefix) })
	wait := func(label string, fn func() bool) {
		t.Helper()
		for !fn() {
			select {
			case <-ctx.Done():
				t.Fatal(label, ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	wait("relay confirmation", func() bool { return store.Redis.XLen(ctx, stream).Val() == 0 })
	probe, err := connection.Channel()
	check(err)
	deliveries, err := probe.Consume(prefix+".likes", "", false, false, false, false, nil)
	check(err)
	var first amqp.Delivery
	select {
	case first = <-deliveries:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	check(probe.Close())
	workers.Go(func() { outbox.RunLikeWorker(task, db, store.Redis, brokerURL, prefix) })
	wait("batch commit", func() bool { return store.Redis.Exists(ctx, base+":pending").Val() == 0 })
	cool()
	if store.Redis.Exists(ctx, base+":hot", base+":bits", base+":ready").Val() != 0 {
		t.Fatal("did not reclaim both bitmaps")
	}
	async, err = service.SetLike(ctx, 1, 32768, true)
	check(err)
	if async {
		t.Fatal("not cold after cooldown")
	}
	check(channel.PublishWithContext(ctx, "", prefix+".likes", true, false, amqp.Publishing{Body: first.Body, DeliveryMode: amqp.Persistent}))
	marker, err := service.SetLike(ctx, 2, 32768, true)
	check(err)
	if !marker {
		t.Fatal("read-hot post was not promoted")
	}
	wait("replayed batch and barrier", func() bool { return store.Redis.Exists(ctx, prefix+":like:2:pending").Val() == 0 })
	var count int
	check(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM post_likes WHERE user_id=32768").Scan(&count))
	if count != 2 {
		t.Fatal("old hot delivery overwrote newer cold write", count)
	}
}
