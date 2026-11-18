package post

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/redis/go-redis/v9"
)

func (s *Service) likeBase(id int64) string {
	return fmt.Sprintf("%s:like:%d", s.cache.Prefix, id)
}

var observeHeat = redis.NewScript(`
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000000+tonumber(clock[2])
local cap=tonumber(ARGV[1])
redis.call('LPUSH',KEYS[1],string.format('%.0f',now))
redis.call('LTRIM',KEYS[1],0,cap-1)
redis.call('EXPIRE',KEYS[1],193)
if redis.call('EXISTS',KEYS[2])==1 then return 1 end
local oldest=redis.call('LINDEX',KEYS[1],cap-1)
if oldest and tonumber(oldest)>now-8000000 then return 2 end
return 0`)

var promoteLike = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==1 then return 0 end
redis.call('DEL',KEYS[2],KEYS[3])
redis.call('SET',KEYS[1],ARGV[1])
redis.call('ZADD',KEYS[4],redis.call('TIME')[1]+192,ARGV[2])
return 1`)

var coolLike = redis.NewScript(`
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000000+tonumber(clock[2])
local function recent(key,index)
 local stamp=redis.call('LINDEX',key,index)
 return stamp and tonumber(stamp)>now-192000000
end
if recent(KEYS[1],15) or recent(KEYS[2],127) or redis.call('EXISTS',KEYS[6])==1 then
 redis.call('ZADD',KEYS[7],tonumber(clock[1])+8,ARGV[1])
 return 0
end
redis.call('DEL',KEYS[3],KEYS[4],KEYS[5])
redis.call('ZREM',KEYS[7],ARGV[1])
return 1`)

func (s *Service) heat(ctx context.Context, item Post, write bool) (bool, error) {
	if !s.adaptiveLikes {
		return false, nil
	}
	base := s.likeBase(item.ID)
	key, limit := base+":reads", 2049
	if write {
		key, limit = base+":writes", 257
	}
	state, err := observeHeat.Run(ctx, s.cache.Redis, []string{key, base + ":hot"}, limit).Int()
	if err != nil {
		return false, errors.Join(ErrLikeUnavailable, err)
	}
	if state != 2 {
		return state == 1, nil
	}
	tx, err := s.lockPost(ctx, item.ID)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	token, err := cache.Token()
	if err != nil {
		return false, err
	}
	promoted, err := promoteLike.Run(ctx, s.cache.Redis, []string{base + ":hot", base + ":ready", base + ":bits", s.cache.Prefix + ":like:cool"}, token, item.ID).Int()
	if err != nil {
		return false, errors.Join(ErrLikeUnavailable, err)
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	if promoted == 1 && s.warmAuthor != nil {
		if err := s.warmAuthor(ctx, item.Author.ID); err != nil {
			s.cache.Count("like_author_warm_failed")
		}
	}
	return true, nil
}

func (s *Service) lockPost(ctx context.Context, id int64) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	var found int
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM posts WHERE id=? AND deleted_on IS NULL FOR UPDATE", id).Scan(&found)
	if err != nil {
		tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return tx, nil
}

func (s *Service) CoolLikes(ctx context.Context) error {
	queue := s.cache.Prefix + ":like:cool"
	now, err := s.cache.Redis.Time(ctx).Result()
	if err != nil {
		return err
	}
	ids, err := s.cache.Redis.ZRangeByScore(ctx, queue, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprint(now.Unix()), Count: 64}).Result()
	if err != nil {
		return err
	}
	for _, id := range ids {
		base := s.cache.Prefix + ":like:" + id
		err = coolLike.Run(ctx, s.cache.Redis, []string{base + ":writes", base + ":reads", base + ":hot", base + ":ready", base + ":bits", base + ":pending", queue}, id).Err()
		if err != nil {
			return err
		}
	}
	return nil
}
