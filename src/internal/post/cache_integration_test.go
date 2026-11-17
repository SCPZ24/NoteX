package post

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/redis/go-redis/v9"
)

func TestBodyCacheProtocol(t *testing.T) {
	if os.Getenv("NOTEX_CACHE_INTEGRATION") != "1" {
		t.Skip("set NOTEX_CACHE_INTEGRATION=1")
	}
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6380/0"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	prefix := fmt.Sprintf("notex_body_test_%d", time.Now().UnixNano())
	store := cache.New(client, prefix)
	defer store.Close()
	s := New(nil, nil)
	s.UseCache(store)
	ctx := context.Background()
	defer func() {
		iter := client.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
	}()
	for round := int64(1); round <= 20; round++ {
		t.Run(strconv.FormatInt(round, 10), func(t *testing.T) {
			base, err := store.BodyBase(ctx, strconv.FormatInt(round, 10))
			if err != nil {
				t.Fatal(err)
			}
			keys := bodyKeys(base)
			old, ok, err := store.Claim(ctx, keys[2])
			if err != nil || !ok {
				t.Fatal(err)
			}
			data, _ := json.Marshal(Post{ID: round, Author: Author{ID: 1, Username: "author"}, Content: "stale", CreatedOn: 1})
			// Simulate a request holding the previous run namespace across restart.
			oldBase := strings.Replace(base, ":body:", ":body:obsolete-", 1)
			if err := client.Set(ctx, bodyKeys(oldBase)[0], data, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			if _, state, err := s.readBody(ctx, oldBase, round); err != nil || state != "miss" {
				t.Fatalf("read restored old namespace: %s %v", state, err)
			}
			// Deletion occurs after SQL read but before the old builder's installation.
			if err := s.InvalidateBody(ctx, round); err != nil {
				t.Fatal(err)
			}
			applied, err := installBody.Run(ctx, client, keys, old, data, 60000).Int()
			if err != nil || applied != 0 {
				t.Fatalf("old builder accepted: %d %v", applied, err)
			}
			if _, state, err := s.readBody(ctx, base, round); state != "deleted" || !errors.Is(err, ErrNotFound) {
				t.Fatalf("state=%s err=%v", state, err)
			}
			// Removing metadata cannot resurrect an old builder, even after tombstone expiry.
			client.Del(ctx, keys...)
			current, ok, err := store.Claim(ctx, keys[2])
			if err != nil || !ok {
				t.Fatal(err)
			}
			applied, err = installBody.Run(ctx, client, keys, old, data, 60000).Int()
			if err != nil || applied != 0 {
				t.Fatal("old token replaced new owner", err)
			}
			store.Release(ctx, keys[2], old)
			if client.Get(ctx, keys[2]).Val() != current {
				t.Fatal("old owner released new lease")
			}
			applied, err = installBody.Run(ctx, client, keys, current, data, 60000).Int()
			if err != nil || applied != 1 {
				t.Fatal("new owner could not install", err)
			}
			if err := s.InvalidateBody(ctx, round); err != nil {
				t.Fatal(err)
			}
			if _, state, err := s.readBody(ctx, base, round); state != "deleted" || !errors.Is(err, ErrNotFound) {
				t.Fatal("delete did not hide installed body", err)
			}
		})
	}
}
