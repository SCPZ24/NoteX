package feed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
)

func verifyReading(t *testing.T, db *sql.DB, base *Service, redisURL, url, prefix string) {
	ctx := context.Background()
	const actor int64 = 10000
	execute := func(t *testing.T, q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	execute(t, "INSERT INTO users(id,username,password_hash,created_on) VALUES(10000,'read-user','fixture',1),(13000,'empty-reader','fixture',1)")
	var users, relations []string
	pull := make([]int64, 0, 65)
	for i := 1; i <= 66; i++ {
		id := 11000 + i
		users = append(users, fmt.Sprintf("(%d,'read-author-%d','fixture',1)", id, id))
		if i <= 65 {
			pull = append(pull, int64(id))
		}
		if i != 65 {
			relations = append(relations, fmt.Sprintf("(10000,%d,1)", id))
		}
	}
	execute(t, "INSERT INTO users(id,username,password_hash,created_on) VALUES"+strings.Join(users, ","))
	execute(t, "INSERT INTO following(user_id,follow_id,created_on) VALUES"+strings.Join(relations, ","))
	for start := 0; start < 800; start += 100 {
		var values []string
		for i := start; i < start+100; i++ {
			author := 11001 + i%66
			if i%100 == 0 {
				author = 10000
			}
			values = append(values, fmt.Sprintf("(%d,%d,'reading fixture',%d)", 200000+i, author, 1700000000+i/5))
		}
		execute(t, "INSERT INTO posts(id,author_id,content,created_on) VALUES"+strings.Join(values, ","))
	}
	makeService := func(t *testing.T, suffix, mode string, authors []int64) *Service {
		t.Helper()
		service, err := New(db, redisURL, prefix+":"+suffix, mode, authors)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { service.Close() })
		return service
	}
	push := makeService(t, "read-push", "push", nil)
	hybrid := makeService(t, "read-hybrid", "hybrid", pull)
	observer := &pipelineObserver{}
	push.redis.AddHook(observer)
	hybrid.redis.AddHook(observer)
	sqlFeed := post.New(db, nil)
	collect := func(t *testing.T, reader interface {
		Feed(context.Context, int64, string, int) (post.Page, error)
	}) []post.Post {
		t.Helper()
		var result []post.Post
		cursor := ""
		seen := map[string]bool{}
		for pages := 0; pages < 100; pages++ {
			page, err := reader.Feed(ctx, actor, cursor, 50)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Items) > 50 || page.Items == nil {
				t.Fatal("invalid page shape")
			}
			result = append(result, page.Items...)
			if !page.HasMore {
				if page.NextCursor != "" {
					t.Fatal("terminal cursor retained")
				}
				return result
			}
			if page.NextCursor == "" || seen[page.NextCursor] {
				t.Fatal("pagination failed to advance")
			}
			seen[page.NextCursor] = true
			cursor = page.NextCursor
		}
		t.Fatal("pagination budget exceeded")
		return nil
	}
	warmAll := func(t *testing.T, s *Service) {
		t.Helper()
		sources, reason, err := s.sources(ctx, actor)
		if err != nil || reason != "" {
			t.Fatalf("sources %s %v", reason, err)
		}
		if err := s.touchSources(ctx, sources); err != nil {
			t.Fatal(err)
		}
		for _, source := range sources {
			if !source.state.ready {
				if err := s.warmSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	samePage := func(t *testing.T, s *Service, limit int) {
		t.Helper()
		actual, err := s.Feed(ctx, actor, "", limit)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := sqlFeed.Feed(ctx, actor, "", limit)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual.Items, expected.Items) || actual.HasMore != expected.HasMore {
			t.Fatalf("page differs actual=%v expected=%v", actual, expected)
		}
	}

	t.Run("push complete pagination matches SQL and crosses retained history", func(t *testing.T) {
		actual := collect(t, push)
		expected := collect(t, sqlFeed)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("full pagination differs actual=%d expected=%d", len(actual), len(expected))
		}
		keys := push.cacheKeys(cacheSource{owner: actor})
		if push.redis.ZCard(ctx, keys[0]).Val() != 500 || push.redis.HLen(ctx, keys[2]).Val() != 500 {
			t.Fatal("reader retention bound missing")
		}
		stats := push.Stats()
		if stats["cache"] == 0 || stats["sql_cold"] == 0 || stats["sql_history"] == 0 {
			t.Fatal("read paths not exercised", stats)
		}
		t.Logf("push SQL-equal posts=%d paths=%v", len(actual), stats)
	})
	t.Run("64 pull authors plus reader use bounded mixed reads", func(t *testing.T) {
		for tries := 0; tries < 20 && hybrid.Stats()["cache"] == 0; tries++ {
			samePage(t, hybrid, 50)
		}
		if hybrid.Stats()["cache"] == 0 {
			t.Fatal("mixed cache never became ready")
		}
		if !reflect.DeepEqual(collect(t, hybrid), collect(t, sqlFeed)) {
			t.Fatal("mixed full pagination differs")
		}
		if hybrid.Stats()["sql_sources"] != 0 {
			t.Fatal("64 sources incorrectly rejected")
		}
		t.Logf("64 authors + reader paths=%v", hybrid.Stats())
	})
	t.Run("small pages merge truncated sources at a common frontier", func(t *testing.T) {
		cursor := ""
		before := hybrid.Stats()["cache"]
		for i := 0; i < 80; i++ {
			actual, err := hybrid.Feed(ctx, actor, cursor, 1)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := sqlFeed.Feed(ctx, actor, cursor, 1)
			if err != nil || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("small page %d differs actual=%v expected=%v err=%v", i, actual, expected, err)
			}
			cursor = actual.NextCursor
		}
		if hybrid.Stats()["cache"] != before+80 {
			t.Fatal("small page frontier unexpectedly fell back")
		}
	})
	t.Run("65th pull author selects SQL before candidate reads", func(t *testing.T) {
		execute(t, "INSERT INTO following(user_id,follow_id,created_on) VALUES(10000,11065,1)")
		before := hybrid.Stats()["sql_sources"]
		samePage(t, hybrid, 50)
		if hybrid.Stats()["sql_sources"] != before+1 {
			t.Fatal("65-source fallback missing")
		}
		execute(t, "DELETE FROM following WHERE user_id=10000 AND follow_id=11065")
	})
	t.Run("refollow changes coverage identity and returns historical posts", func(t *testing.T) {
		execute(t, "DELETE FROM following WHERE user_id=10000 AND follow_id=11001")
		execute(t, "INSERT INTO following(user_id,follow_id,created_on) VALUES(10000,11001,1)")
		before := hybrid.Stats()["sql_cold"]
		samePage(t, hybrid, 50)
		if hybrid.Stats()["sql_cold"] != before+1 {
			t.Fatal("old relationship coverage reused")
		}
		samePage(t, hybrid, 50)
	})
	t.Run("deletions filter candidates and duplicate sources deduplicate", func(t *testing.T) {
		warmAll(t, hybrid)
		expected, err := sqlFeed.Feed(ctx, actor, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, 40)
		for _, item := range expected.Items[:40] {
			ids = append(ids, strconv.FormatInt(item.ID, 10))
		}
		execute(t, "UPDATE posts SET deleted_on=1 WHERE id IN ("+strings.Join(ids, ",")+")")
		before := hybrid.Stats()["cache"]
		samePage(t, hybrid, 50)
		if hybrid.Stats()["cache"] != before+1 {
			t.Fatal("filtered page did not use cache")
		}
		execute(t, "UPDATE posts SET deleted_on=NULL WHERE id IN ("+strings.Join(ids, ",")+")")
		var entry Entry
		var relation int64
		if err := db.QueryRow("SELECT id,created_on FROM posts WHERE author_id=11001 ORDER BY created_on DESC,id DESC LIMIT 1").Scan(&entry.ID, &entry.CreatedOn); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT id FROM following WHERE user_id=10000 AND follow_id=11001").Scan(&relation); err != nil {
			t.Fatal(err)
		}
		targets := []recipient{{UserID: actor, RelationID: relation}}
		if err := hybrid.readerGenerations(ctx, targets); err != nil {
			t.Fatal(err)
		}
		if err := hybrid.writeReaderBatch(ctx, entry, targets, false); err != nil {
			t.Fatal(err)
		}
		samePage(t, hybrid, 50)
	})
	t.Run("invalid candidates exhaust a bounded budget then use SQL", func(t *testing.T) {
		warmAll(t, push)
		expected, err := sqlFeed.Feed(ctx, actor, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, 24)
		for _, item := range expected.Items[:24] {
			ids = append(ids, strconv.FormatInt(item.ID, 10))
		}
		execute(t, "UPDATE posts SET deleted_on=1 WHERE id IN ("+strings.Join(ids, ",")+")")
		before := push.Stats()["sql_budget"]
		samePage(t, push, 1)
		if push.Stats()["sql_budget"] != before+1 {
			t.Fatal("read budget fallback missing")
		}
		execute(t, "UPDATE posts SET deleted_on=NULL WHERE id IN ("+strings.Join(ids, ",")+")")
	})
	t.Run("20 snapshot races and expired generations reject stale work", func(t *testing.T) {
		race := makeService(t, "read-race", "push", nil)
		var entry Entry
		if err := db.QueryRow("SELECT id,created_on FROM posts WHERE author_id=11066 ORDER BY id DESC LIMIT 1").Scan(&entry.ID, &entry.CreatedOn); err != nil {
			t.Fatal(err)
		}
		var relation int64
		if err := db.QueryRow("SELECT id FROM following WHERE user_id=10000 AND follow_id=11066").Scan(&relation); err != nil {
			t.Fatal(err)
		}
		for round := 0; round < 20; round++ {
			sources, reason, err := race.sources(ctx, actor)
			if err != nil || reason != "" {
				t.Fatal(reason, err)
			}
			keys := race.cacheKeys(sources[0])
			if err := race.redis.Del(ctx, keys...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := race.touchSources(ctx, sources); err != nil {
				t.Fatal(err)
			}
			snapshot := sources[0].state
			targets := []recipient{{UserID: actor, RelationID: relation, generation: snapshot.generation}}
			if err := race.writeReaderBatch(ctx, entry, targets, false); err != nil {
				t.Fatal(err)
			}
			installed, err := installCache.Run(ctx, race.redis, keys, snapshot.generation, snapshot.revision, "", 0).Int()
			if err != nil || installed != 0 {
				t.Fatal("concurrent write overwritten by snapshot", err)
			}
			for _, key := range keys {
				if err := race.redis.PExpireAt(ctx, key, time.Now().Add(-time.Second)).Err(); err != nil {
					t.Fatal(err)
				}
			}
			race.renewReader(ctx, actor)
			if err := race.writeReaderBatch(ctx, entry, targets, false); err != nil {
				t.Fatal(err)
			}
			if race.redis.ZCard(ctx, keys[0]).Val() != 0 {
				t.Fatal("old generation resurrected member")
			}
			samePage(t, race, 50)
		}
	})
	t.Run("reader activity renews TTL and background writes preserve deadline", func(t *testing.T) {
		warmAll(t, push)
		keys := push.cacheKeys(cacheSource{owner: actor})
		for _, key := range keys {
			if err := push.redis.PExpire(ctx, key, 10*time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
		}
		var entry Entry
		var relation int64
		if err := db.QueryRow("SELECT id,created_on FROM posts WHERE author_id=11066 ORDER BY id DESC LIMIT 1").Scan(&entry.ID, &entry.CreatedOn); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT id FROM following WHERE user_id=10000 AND follow_id=11066").Scan(&relation); err != nil {
			t.Fatal(err)
		}
		targets := []recipient{{UserID: actor, RelationID: relation}}
		if err := push.readerGenerations(ctx, targets); err != nil {
			t.Fatal(err)
		}
		if err := push.writeReaderBatch(ctx, entry, targets, false); err != nil {
			t.Fatal(err)
		}
		ttl, err := push.redis.PTTL(ctx, keys[3]).Result()
		if err != nil || ttl > 10*time.Minute {
			t.Fatal("background write extended activity", ttl, err)
		}
		samePage(t, push, 50)
		ttl, err = push.redis.PTTL(ctx, keys[3]).Result()
		if err != nil || ttl < 6*24*time.Hour {
			t.Fatal("read did not renew activity", ttl, err)
		}
		for _, key := range keys {
			if err := push.redis.PExpireAt(ctx, key, time.Now().Add(-time.Second)).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if err := push.readerGenerations(ctx, targets); err != nil {
			t.Fatal(err)
		}
		if err := push.writeReaderBatch(ctx, entry, targets, false); err != nil {
			t.Fatal(err)
		}
		if push.redis.Exists(ctx, keys...).Val() != 0 {
			t.Fatal("inactive reader recreated by writer")
		}
		samePage(t, push, 50)
	})
	t.Run("missing structures cold-fallback and ready empty cache", func(t *testing.T) {
		warmAll(t, push)
		before := push.Stats()["sql_cold"]
		if err := push.redis.Del(ctx, push.readerKeys(actor)[0]).Err(); err != nil {
			t.Fatal(err)
		}
		samePage(t, push, 50)
		if push.Stats()["sql_cold"] != before+1 {
			t.Fatal("partial cache treated as complete")
		}
		for i := 0; i < 2; i++ {
			page, err := push.Feed(ctx, 13000, "", 50)
			if err != nil || page.Items == nil || len(page.Items) != 0 || page.HasMore {
				t.Fatal("empty cache", page, err)
			}
		}
	})
	t.Run("author retention cap and conservative historical coverage", func(t *testing.T) {
		bounded := makeService(t, "author-bound", "hybrid", pull)
		generation := fixtureAuthorGeneration(t, bounded, 11001)
		for i := int64(1); i <= 510; i++ {
			if err := bounded.apply(ctx, 11001, Entry{ID: 900000 + i, CreatedOn: 1800000000}, false, generation); err != nil {
				t.Fatal(err)
			}
		}
		keys := bounded.cacheKeys(cacheSource{owner: 11001, author: true})
		if bounded.redis.ZCard(ctx, keys[0]).Val() != 500 || bounded.redis.HGet(ctx, keys[3], "floor").Val() == "" {
			t.Fatal("author retention failed")
		}
		if bounded.redis.HGet(ctx, keys[3], "ready").Val() != "0" {
			t.Fatal("partial author events marked complete")
		}
	})
	t.Run("Redis unavailable falls back and malformed cursor stays invalid", func(t *testing.T) {
		broken, err := New(db, "redis://127.0.0.1:1/0", prefix+":unavailable", "push", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer broken.Close()
		samePage(t, broken, 50)
		if broken.Stats()["sql_redis"] != 1 {
			t.Fatal("Redis fallback missing")
		}
		if _, err := push.Feed(ctx, actor, "invalid", 50); err != post.ErrInvalidCursor {
			t.Fatal("invalid cursor accepted", err)
		}
		if _, err := push.Feed(ctx, actor, "", 101); err != post.ErrInvalid {
			t.Fatal("invalid limit accepted", err)
		}
	})
	t.Run("real HTTP hybrid startup writes and reads both author strategies", func(t *testing.T) {
		verifyHybridHTTP(t, db, base, redisURL, url, prefix, pull, actor)
	})
	if observer.maximum.Load() > pipelineSize {
		t.Fatal("read pipeline exceeded 32 commands")
	}
	t.Logf("push paths=%v hybrid paths=%v maximum read pipeline=%d", push.Stats(), hybrid.Stats(), observer.maximum.Load())
}

func verifyHybridHTTP(t *testing.T, db *sql.DB, base *Service, redisURL, url, prefix string, pull []int64, actor int64) {
	ctx := context.Background()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT DATABASE()").Scan(&cfg.DBName); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "notex-hybrid-api")
	if output, err := exec.Command("go", "build", "-p", "2", "-o", binary, "../..").CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	tokens := map[int64]string{actor: strings.Repeat("b", 64), 11001: strings.Repeat("c", 64)}
	for id, token := range tokens {
		hash := sha256.Sum256([]byte(token))
		if _, err := db.Exec("INSERT INTO sessions(token_hash,user_id,expires_on) VALUES(?,?,?)", hash[:], id, time.Now().Add(time.Hour).Unix()); err != nil {
			t.Fatal(err)
		}
	}
	ids := make([]string, len(pull))
	for i, id := range pull {
		ids[i] = strconv.FormatInt(id, 10)
	}
	command := exec.Command(binary)
	command.Env = append(os.Environ(), "MYSQL_DSN="+cfg.FormatDSN(), "HTTP_ADDR="+address, "RABBITMQ_URL="+url, "REDIS_URL="+redisURL, "RABBITMQ_QUEUE_PREFIX="+prefix, "FEED_MODE=hybrid", "FEED_PULL_AUTHORS="+strings.Join(ids, ","), "GOMAXPROCS=2", "GOMEMLIMIT=512MiB", "GIN_MODE=release")
	var log bytes.Buffer
	command.Stdout = &log
	command.Stderr = &log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		command.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("API exit %v %s", err, log.String())
			}
		case <-time.After(15 * time.Second):
			command.Process.Kill()
			<-done
			t.Error("API stop timeout")
		}
	}()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := client.Get("http://" + address + "/readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("HTTP readiness timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	request := func(method, path string, user int64, key, body string, status int) []byte {
		t.Helper()
		req, err := http.NewRequest(method, "http://"+address+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tokens[user])
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		result, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode != status {
			t.Fatalf("HTTP %s %s status=%d result=%s err=%v", method, path, response.StatusCode, result, err)
		}
		return result
	}
	for i := 0; i < 20; i++ {
		var page post.Page
		if err := json.Unmarshal(request("GET", "/api/v1/feed?limit=50", actor, "", "", 200), &page); err != nil {
			t.Fatal(err)
		}
		expected, err := post.New(db, nil).Feed(ctx, actor, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(page.Items, expected.Items) {
			t.Fatal("HTTP hybrid differs from SQL")
		}
	}
	for _, author := range []int64{actor, 11001} {
		var item post.Post
		if err := json.Unmarshal(request("POST", "/api/v1/posts", author, fmt.Sprintf("http-stage5-%d", author), `{"content":"stage five HTTP"}`, 201), &item); err != nil {
			t.Fatal(err)
		}
		var event outbox.Event
		if err := db.QueryRow("SELECT id,event_type,payload,created_on FROM outbox_events WHERE event_type='post.created' AND payload->>'$.post_id'=?", item.ID).Scan(&event.ID, &event.Type, &event.Payload, &event.CreatedOn); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			var completed int
			if err := db.QueryRow("SELECT COUNT(*) FROM consumption_jobs WHERE consumer=? AND event_id=? AND status='completed'", prefix+".feed", event.ID).Scan(&completed); err != nil {
				t.Fatal(err)
			}
			key := base.readerKeys(actor)[0]
			if author == 11001 {
				key = base.keys(author)[0]
			}
			if completed == 1 && base.redis.ZScore(ctx, key, member(Entry{ID: item.ID, CreatedOn: item.CreatedOn})).Err() == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("HTTP event projection timeout")
			}
			time.Sleep(20 * time.Millisecond)
		}
		if author == 11001 {
			var plans int
			if err := db.QueryRow("SELECT COUNT(*) FROM feed_tasks WHERE root_event_id=?", event.ID).Scan(&plans); err != nil {
				t.Fatal(err)
			}
			if plans != 0 {
				t.Fatal("pull author entered fanout")
			}
		}
		var page post.Page
		if err := json.Unmarshal(request("GET", "/api/v1/feed?limit=50", actor, "", "", 200), &page); err != nil {
			t.Fatal(err)
		}
		expected, err := post.New(db, nil).Feed(ctx, actor, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(page.Items, expected.Items) {
			t.Fatal("HTTP publication missing from hybrid page")
		}
	}
	request("GET", "/api/v1/feed?cursor=invalid", actor, "", "", 400)
}
