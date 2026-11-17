package post

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/redis/go-redis/v9"
)

var ErrCacheUnavailable = errors.New("deletion committed; cache invalidation unconfirmed")

// UseCache enables lazy detail caching. The caller owns the shared store.
func (s *Service) UseCache(store *cache.Store) { s.cache = store }

var readBody = redis.NewScript(`
local run=string.match(redis.call('INFO','server'),'run_id:(%w+)')
if not run or not string.find(KEYS[1],':body:'..run..':',1,true) then return {'miss',''} end
if redis.call('EXISTS',KEYS[2])==1 then return {'deleted',''} end
local body=redis.call('GET',KEYS[1])
if body then return {'hit',body} end
return {'miss',''}
`)
var installBody = redis.NewScript(`
if redis.call('GET',KEYS[3])~=ARGV[1] or redis.call('EXISTS',KEYS[2])==1 then return 0 end
redis.call('SET',KEYS[1],ARGV[2],'PX',ARGV[3])
redis.call('DEL',KEYS[3])
return 1
`)
var deleteBody = redis.NewScript(`
redis.call('SET',KEYS[2],'1','PX',ARGV[1])
redis.call('DEL',KEYS[1],KEYS[3])
return 1
`)

func bodyKeys(base string) []string {
	return []string{base + ":value", base + ":deleted", base + ":build"}
}

func (s *Service) readBody(ctx context.Context, base string, id int64) (Post, string, error) {
	values, err := readBody.Run(ctx, s.cache.Redis, bodyKeys(base)).StringSlice()
	if err != nil {
		return Post{}, "", err
	}
	if len(values) != 2 {
		return Post{}, "", errors.New("invalid cache reply")
	}
	if values[0] == "deleted" {
		return Post{}, "deleted", ErrNotFound
	}
	if values[0] == "miss" {
		return Post{}, "miss", nil
	}
	var item Post
	if err := json.Unmarshal([]byte(values[1]), &item); err != nil {
		return Post{}, "", err
	}
	if item.ID != id || item.Author.ID <= 0 || item.CreatedOn <= 0 {
		return Post{}, "", errors.New("invalid body cache identity")
	}
	return item, "hit", nil
}

// cachedGet never returns a shared builder's SQL snapshot: a new waiter may have
// joined after deletion. Each request rereads the atomic tombstone/body pair.
func (s *Service) cachedGet(ctx context.Context, id int64) (Post, error) {
	base, err := s.cache.BodyBase(ctx, strconv.FormatInt(id, 10))
	if err == nil {
		item, state, readErr := s.readBody(ctx, base, id)
		if state == "hit" {
			s.cache.Count("body_hit")
			return item, nil
		}
		if state == "deleted" {
			return Post{}, ErrNotFound
		}
		if readErr == nil {
			s.cache.Count("body_miss")
			buildErr := s.cache.Do(ctx, base, func(task context.Context) error { return s.buildBody(task, base, id) })
			if ctx.Err() != nil {
				return Post{}, ctx.Err()
			}
			if errors.Is(buildErr, cache.ErrBusy) {
				return Post{}, buildErr
			}
			// Resolve the running Redis identity again after a possible restart.
			if current, e := s.cache.BodyBase(ctx, strconv.FormatInt(id, 10)); e == nil {
				item, state, _ = s.readBody(ctx, current, id)
				if state == "hit" {
					return item, nil
				}
				if state == "deleted" {
					return Post{}, ErrNotFound
				}
			}
		}
	}
	s.cache.Count("body_sql_fallback")
	var item Post
	err = s.cache.SQL(ctx, func(query context.Context) error { var e error; item, e = s.getSQL(query, id); return e })
	return item, err
}

func (s *Service) buildBody(ctx context.Context, base string, id int64) error {
	_, state, err := s.readBody(ctx, base, id)
	if err != nil {
		return err
	}
	if state != "miss" {
		return nil
	}
	keys := bodyKeys(base)
	token, owned, err := s.cache.Claim(ctx, keys[2])
	if err != nil {
		return err
	}
	if !owned {
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return cache.ErrSuperseded
			case <-tick.C:
				_, state, err = s.readBody(ctx, base, id)
				if err != nil {
					return err
				}
				if state != "miss" {
					return nil
				}
			}
		}
	}
	defer s.cache.Release(ctx, keys[2], token)
	// A previous owner may have filled between our initial miss and claim.
	_, state, err = s.readBody(ctx, base, id)
	if err != nil {
		return err
	}
	if state != "miss" {
		return nil
	}
	var item Post
	err = s.cache.SQL(ctx, func(query context.Context) error { var e error; item, e = s.getSQL(query, id); return e })
	if err != nil {
		return err
	}
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	ttl := 10*time.Minute + time.Duration(id%31)*time.Second
	n, err := installBody.Run(ctx, s.cache.Redis, keys, token, data, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		s.cache.Count("body_commit_rejected")
		return cache.ErrSuperseded
	}
	s.cache.Count("body_built")
	return nil
}

// InvalidateBody acknowledges deletion only after its Redis tombstone is known
// to be installed. It is used both synchronously and by durable event replay.
func (s *Service) InvalidateBody(ctx context.Context, id int64) error {
	if s.cache == nil {
		return nil
	}
	// A restart between INFO and the deletion script changes the namespace.
	// Only acknowledge after observing the same run identity on both sides.
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		var base, current string
		base, err = s.cache.BodyBase(ctx, strconv.FormatInt(id, 10))
		if err != nil {
			break
		}
		err = deleteBody.Run(ctx, s.cache.Redis, bodyKeys(base), (11 * time.Minute).Milliseconds()).Err()
		if err != nil {
			break
		}
		current, err = s.cache.BodyBase(ctx, strconv.FormatInt(id, 10))
		if err != nil {
			break
		}
		if current == base {
			s.cache.Count("body_deleted")
			return nil
		}
		err = cache.ErrSuperseded
	}
	return fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
}
