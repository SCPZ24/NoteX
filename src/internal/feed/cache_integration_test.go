package feed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// TestCacheProcess is a real second application process, not a second client in
// the same process. Only its parent sets the isolated DSN and namespace.
func TestCacheProcess(t *testing.T) {
	if os.Getenv("NOTEX_CACHE_CHILD") != "1" {
		t.Skip("parent-controlled subprocess")
	}
	db, err := sql.Open("mysql", os.Getenv("NOTEX_CACHE_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	s, err := New(db, os.Getenv("TEST_REDIS_URL"), os.Getenv("NOTEX_CACHE_PREFIX"), "hybrid", []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.ensureAuthor(context.Background(), cacheSource{owner: 1, author: true, signature: s.policy}); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(s.Cache().Stats())
	fmt.Printf("CACHE_STATS %s\n", data)
}

func TestCacheRecoveryIntegration(t *testing.T) {
	if os.Getenv("NOTEX_CACHE_INTEGRATION") != "1" {
		t.Skip("set NOTEX_CACHE_INTEGRATION=1")
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
	cfg.Timeout = 2 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	admin.SetMaxOpenConns(1)
	prefix := fmt.Sprintf("notex_cache_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + prefix); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP DATABASE " + prefix)
	cfg.DBName = prefix
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(6)
	files, _ := filepath.Glob("../../../migrations/*.sql")
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(data)); err != nil {
			t.Fatal(err)
		}
	}
	execSQL := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	execSQL("INSERT INTO users(id,username,password_hash,created_on) VALUES(1,'author','fixture',1),(2,'reader','fixture',1),(3,'empty','fixture',1)")
	execSQL("INSERT INTO following(user_id,follow_id,created_on) VALUES(2,1,1)")
	for start := 1; start <= 620; start += 100 {
		var values []string
		for id := start; id < start+100 && id <= 620; id++ {
			values = append(values, fmt.Sprintf("(%d,1,'body-%d',%d)", id, id, 1700000000+id/3))
		}
		execSQL("INSERT INTO posts(id,author_id,content,created_on) VALUES" + strings.Join(values, ","))
	}
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6380/0"
	}
	s, err := New(db, redisURL, prefix, "hybrid", []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer func() {
		iter := s.redis.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			s.redis.Del(ctx, iter.Val())
		}
	}()
	posts := post.New(db, nil)
	posts.UseCache(s.Cache())
	baseline := post.New(db, nil)

	t.Run("author list lazy build and historical pagination", func(t *testing.T) {
		var actual, expected []post.Post
		for cursor := ""; ; {
			page, err := s.List(ctx, 1, cursor, 37)
			if err != nil {
				t.Fatal(err)
			}
			actual = append(actual, page.Items...)
			if !page.HasMore {
				break
			}
			cursor = page.NextCursor
		}
		for cursor := ""; ; {
			page, err := baseline.List(ctx, 1, cursor, 37)
			if err != nil {
				t.Fatal(err)
			}
			expected = append(expected, page.Items...)
			if !page.HasMore {
				break
			}
			cursor = page.NextCursor
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("actual=%d expected=%d", len(actual), len(expected))
		}
		if s.redis.ZCard(ctx, s.cacheKeys(cacheSource{owner: 1, author: true})[0]).Val() != 500 {
			t.Fatal("retention")
		}
		before := s.Cache().Stats()["author_built"]
		for i := 0; i < 2; i++ {
			page, err := s.List(ctx, 3, "", 20)
			if err != nil || len(page.Items) != 0 {
				t.Fatalf("empty page %v %v", page, err)
			}
		}
		if s.Cache().Stats()["author_built"] != before+1 {
			t.Fatal("empty index rebuilt repeatedly")
		}
	})

	t.Run("two real processes one author rebuild", func(t *testing.T) {
		keys := s.cacheKeys(cacheSource{owner: 1, author: true})
		keys = append(keys, keys[3]+":build")
		if err := s.redis.Del(ctx, keys...).Err(); err != nil {
			t.Fatal(err)
		}
		// Hold SQL briefly to make both process-local builders overlap in Redis.
		lock, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lock.ExecContext(ctx, "LOCK TABLES posts WRITE"); err != nil {
			lock.Close()
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		outputs := make([][]byte, 2)
		errs := make([]error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCacheProcess$", "-test.v")
				cmd.Env = append(os.Environ(), "NOTEX_CACHE_CHILD=1", "NOTEX_CACHE_DSN="+cfg.FormatDSN(), "NOTEX_CACHE_PREFIX="+prefix, "TEST_REDIS_URL="+redisURL)
				outputs[i], errs[i] = cmd.CombinedOutput()
			}(i)
		}
		time.Sleep(100 * time.Millisecond)
		_, err = lock.ExecContext(ctx, "UNLOCK TABLES")
		lock.Close()
		if err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		var builds uint64
		for i, output := range outputs {
			if errs[i] != nil {
				t.Fatalf("child %d: %v %s", i, errs[i], output)
			}
			for _, line := range strings.Split(string(output), "\n") {
				if strings.HasPrefix(line, "CACHE_STATS ") {
					var stats map[string]uint64
					if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "CACHE_STATS ")), &stats); err != nil {
						t.Fatal(err)
					}
					builds += stats["author_built"]
					t.Logf("process %d: %v", i, stats)
				}
			}
		}
		if builds != 1 {
			t.Fatalf("actual builds=%d", builds)
		}
	})

	t.Run("author stale lease and revision reject", func(t *testing.T) {
		source := cacheSource{owner: 1, author: true, signature: s.policy}
		sources := []cacheSource{source}
		if err := s.touchSources(ctx, sources); err != nil {
			t.Fatal(err)
		}
		source = sources[0]
		key := s.cacheKeys(source)[3] + ":build"
		token, owned, err := s.cache.Claim(ctx, key)
		if err != nil || !owned {
			t.Fatalf("claim %v %v", owned, err)
		}
		if err := s.redis.HIncrBy(ctx, s.cacheKeys(source)[3], "revision", 1).Err(); err != nil {
			t.Fatal(err)
		}
		if err := s.warmSnapshot(ctx, source, token); err == nil {
			t.Fatal("accepted old revision")
		}
		s.cache.Release(ctx, key, token)
		if err := s.warmSnapshot(ctx, source, token); err == nil {
			t.Fatal("accepted expired lease")
		}
	})

	t.Run("body hit no SQL and synchronous deletion", func(t *testing.T) {
		expected, err := posts.Get(ctx, 620)
		if err != nil {
			t.Fatal(err)
		}
		disconnected, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		disconnected.Close()
		cached := post.New(disconnected, nil)
		cached.UseCache(s.Cache())
		actual, err := cached.Get(ctx, 620)
		if err != nil || actual != expected {
			t.Fatalf("cache hit depended on SQL: %v", err)
		}
		if err := posts.Delete(ctx, 1, 620); err != nil {
			t.Fatal(err)
		}
		if _, err := cached.Get(ctx, 620); !errors.Is(err, post.ErrNotFound) {
			t.Fatalf("deleted body returned: %v", err)
		}
		if err := posts.Delete(ctx, 1, 620); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Redis unavailable bounded fallback and deletion retry", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, MaxRetries: -1})
		bad := cache.New(client, prefix)
		defer bad.Close()
		defer client.Close()
		degraded := post.New(db, nil)
		degraded.UseCache(bad)
		if _, err := degraded.Get(ctx, 619); err != nil {
			t.Fatalf("fallback failed: %v", err)
		}
		if err := degraded.Delete(ctx, 1, 619); !errors.Is(err, post.ErrCacheUnavailable) {
			t.Fatalf("delete incorrectly acknowledged: %v", err)
		}
		var deleted bool
		if err := db.QueryRow("SELECT deleted_on IS NOT NULL FROM posts WHERE id=619").Scan(&deleted); err != nil || !deleted {
			t.Fatal("SQL deletion not committed", err)
		}
		if err := posts.Delete(ctx, 1, 619); err != nil {
			t.Fatal("retry failed", err)
		}
		if _, err := posts.Get(ctx, 619); !errors.Is(err, post.ErrNotFound) {
			t.Fatal(err)
		}
	})
	if container := os.Getenv("NOTEX_CACHE_OUTAGE_CONTAINER"); container != "" {
		t.Run("isolated Redis 60 second outage and restart", func(t *testing.T) {
			if !strings.HasPrefix(container, "notex-cache-test-") {
				t.Fatal("outage requires an isolated test container")
			}
			before, err := s.cache.BodyBase(ctx, "618")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := posts.Get(ctx, 618); err != nil {
				t.Fatal(err)
			}
			docker := func(action string) {
				t.Helper()
				if data, err := exec.Command("docker", action, container).CombinedOutput(); err != nil {
					t.Fatalf("docker %s: %v %s", action, err, data)
				}
			}
			docker("stop")
			restarted := false
			defer func() {
				if !restarted {
					exec.Command("docker", "start", container).Run()
				}
			}()
			start := time.Now()
			reads, writes, deletes := 0, 0, 0
			for time.Since(start) < 60*time.Second {
				round := time.Now()
				if _, err := posts.Get(ctx, 618); err != nil {
					t.Fatal("outage detail", err)
				}
				if _, err := s.Feed(ctx, 2, "", 20); err != nil {
					t.Fatal("outage Feed", err)
				}
				reads += 2
				item, err := posts.Create(ctx, 1, fmt.Sprintf("outage-%d", writes), "written during Redis outage")
				if err != nil {
					t.Fatal(err)
				}
				writes++
				if err := posts.Delete(ctx, 1, item.ID); !errors.Is(err, post.ErrCacheUnavailable) {
					t.Fatal("delete should report unconfirmed invalidation", err)
				}
				deletes++
				if writes%10 == 0 {
					t.Logf("outage elapsed=%s reads=%d committed_writes=%d unconfirmed_deletes=%d", time.Since(start).Round(time.Millisecond), reads, writes, deletes)
				}
				if remaining := time.Second - time.Since(round); remaining > 0 {
					time.Sleep(remaining)
				}
			}
			docker("start")
			restarted = true
			deadline := time.Now().Add(10 * time.Second)
			for s.redis.Ping(ctx).Err() != nil {
				if time.Now().After(deadline) {
					t.Fatal("Redis restart deadline")
				}
				time.Sleep(50 * time.Millisecond)
			}
			after, err := s.cache.BodyBase(ctx, "618")
			if err != nil || after == before {
				t.Fatal("Redis run identity did not change", err)
			}
			recoverStart := time.Now()
			if _, err := posts.Get(ctx, 618); err != nil {
				t.Fatal(err)
			}
			if _, err := s.List(ctx, 1, "", 20); err != nil {
				t.Fatal(err)
			}
			t.Logf("outage duration >=60s; reads=%d writes=%d unconfirmed_deletes=%d recovery=%s", reads, writes, deletes, time.Since(recoverStart))
		})
	}
	t.Logf("cache counters: %v", s.Cache().Stats())
}
