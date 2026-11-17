package feed

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Reuses the running Redis and a dedicated, expiring namespace. No DB fixture
// or container lifecycle is involved in these deterministic late-write windows.
func TestDistributedLateCacheWrites(t *testing.T) {
	url := os.Getenv("NOTEX_DISTRIBUTED_REDIS")
	if url == "" {
		t.Skip("set NOTEX_DISTRIBUTED_REDIS for the shared local Redis")
	}
	service, err := New(nil, url, fmt.Sprintf("notex_s5_guard_%d", time.Now().UnixNano()), "push", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	ctx := context.Background()
	for round := int64(1); round <= 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			sources := []cacheSource{{owner: round}}
			if err := service.touchSources(ctx, sources); err != nil {
				t.Fatal(err)
			}
			keys := service.cacheKeys(sources[0])
			t.Cleanup(func() {
				for _, key := range keys {
					service.redis.Expire(ctx, key, time.Minute)
				}
			})
			snapshot := sources[0].state
			entry := Entry{ID: round, CreatedOn: 1700000000}
			targets := []recipient{{UserID: round, RelationID: 100 + round, generation: snapshot.generation}}
			if err := service.writeReaderBatch(ctx, entry, targets, false); err != nil {
				t.Fatal(err)
			}
			installed, err := installCache.Run(ctx, service.redis, keys, snapshot.generation, snapshot.revision, "", 0).Int()
			if err != nil || installed != 0 {
				t.Fatal("late snapshot overwrote concurrent effect", installed, err)
			}
			if err := service.writeReaderBatch(ctx, entry, targets, true); err != nil {
				t.Fatal(err)
			}
			if err := service.writeReaderBatch(ctx, entry, targets, false); err != nil {
				t.Fatal(err)
			}
			if service.redis.ZCard(ctx, keys[0]).Val() != 0 {
				t.Fatal("late write resurrected deleted post")
			}
			if err := service.redis.HSet(ctx, keys[3], fmt.Sprintf("blocked:%d", targets[0].RelationID), 1).Err(); err != nil {
				t.Fatal(err)
			}
			if err := service.writeReaderBatch(ctx, Entry{ID: round + 50, CreatedOn: entry.CreatedOn}, targets, false); err != nil {
				t.Fatal(err)
			}
			if service.redis.ZCard(ctx, keys[0]).Val() != 0 {
				t.Fatal("late writer restored a blocked relationship")
			}
			// A changed lifecycle must fence a writer that captured the old one.
			targets[0].RelationID += 1000
			if err := service.redis.HSet(ctx, keys[3], "generation", "new-generation").Err(); err != nil {
				t.Fatal(err)
			}
			if err := service.writeReaderBatch(ctx, Entry{ID: round + 100, CreatedOn: entry.CreatedOn}, targets, false); err != nil {
				t.Fatal(err)
			}
			if service.redis.ZCard(ctx, keys[0]).Val() != 0 {
				t.Fatal("expired generation wrote into replacement cache")
			}
			t.Log("revision, deletion, relationship and generation fences held")
		})
	}
}
