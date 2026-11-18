package post

import (
	"context"
	"errors"
	"fmt"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/redis/go-redis/v9"
)

const segmentBits int64 = 4096 * 8

const maxBitmapUser int64 = (1 << 20) * 8

var ErrLikeUnavailable = errors.New("like state temporarily unavailable")

type Detail struct {
	Post
	LikedByMe bool `json:"liked_by_me"`
}

var accessLike = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==0 then return -2 end
if redis.call('GETBIT',KEYS[2],ARGV[1])==0 then return -1 end
local bit=redis.call('GETBIT',KEYS[3],ARGV[2])
if ARGV[3]=='read' or tonumber(ARGV[3])==bit then return bit end
local id=redis.call('XADD',KEYS[4],'*','post',ARGV[4],'user',ARGV[2],'state',ARGV[3])
redis.call('SET',KEYS[5],id)
redis.call('SETBIT',KEYS[3],ARGV[2],ARGV[3])
return tonumber(ARGV[3])`)

var installSegment = redis.NewScript(`
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('GET',KEYS[4])~=ARGV[2] then return 0 end
if redis.call('GETBIT',KEYS[2],ARGV[3])==0 then
 redis.call('SETRANGE',KEYS[3],tonumber(ARGV[3])*4096,ARGV[4])
 redis.call('SETBIT',KEYS[2],ARGV[3],1)
end
return 1`)

func (s *Service) GetDetail(ctx context.Context, id, user int64) (Detail, error) {
	item, err := s.Get(ctx, id)
	if err != nil {
		return Detail{}, err
	}
	liked, _, err := s.likeAccess(ctx, item, user, "read")
	return Detail{Post: item, LikedByMe: liked}, err
}

func (s *Service) SetLike(ctx context.Context, id, user int64, liked bool) (bool, error) {
	item, err := s.Get(ctx, id)
	if err != nil {
		return false, err
	}
	action := "0"
	if liked {
		action = "1"
	}
	_, async, err := s.likeAccess(ctx, item, user, action)
	return async, err
}

func (s *Service) likeAccess(ctx context.Context, item Post, user int64, action string) (bool, bool, error) {
	if user <= 0 {
		return false, false, ErrInvalid
	}
	hot, err := s.heat(ctx, item, action != "read")
	if err != nil {
		return false, false, err
	}
	if user >= maxBitmapUser {
		hot = false
	}
	for attempt := 0; attempt < 5; attempt++ {
		if !hot {
			if action == "read" {
				var liked bool
				err = s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM post_likes WHERE post_id=? AND user_id=?)", item.ID, user).Scan(&liked)
				return liked, false, err
			}
			switched, err := s.coldLike(ctx, item.ID, user, action == "1")
			if err != nil || !switched {
				return action == "1", false, err
			}
			hot = true
		}
		base := s.likeBase(item.ID)
		segment := user / segmentBits
		result, err := accessLike.Run(ctx, s.cache.Redis, []string{base + ":hot", base + ":ready", base + ":bits", s.cache.Prefix + ":like:stream", base + ":pending"}, segment, user, action, item.ID).Int()
		if err != nil {
			return false, false, errors.Join(ErrLikeUnavailable, err)
		}
		switch result {
		case -2:
			hot = false
		case -1:
			key := fmt.Sprintf("%s:build:%d", base, segment)
			err = s.cache.Do(ctx, key, func(task context.Context) error { return s.buildSegment(task, item.ID, segment, key) })
			if err != nil && !errors.Is(err, cache.ErrSuperseded) {
				return false, false, errors.Join(ErrLikeUnavailable, err)
			}
		default:
			return result == 1, true, nil
		}
	}
	return false, false, ErrLikeUnavailable
}

func (s *Service) coldLike(ctx context.Context, id, user int64, liked bool) (bool, error) {
	tx, err := s.lockPost(ctx, id)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if user < maxBitmapUser && s.adaptiveLikes {
		active, err := s.cache.Redis.Exists(ctx, s.likeBase(id)+":hot").Result()
		if err != nil {
			return false, errors.Join(ErrLikeUnavailable, err)
		}
		if active != 0 {
			return true, nil
		}
	}
	if liked {
		_, err = tx.ExecContext(ctx, "INSERT INTO post_likes(post_id,user_id) VALUES(?,?) ON DUPLICATE KEY UPDATE user_id=post_likes.user_id", id, user)
	} else {
		_, err = tx.ExecContext(ctx, "DELETE FROM post_likes WHERE post_id=? AND user_id=?", id, user)
	}
	if err != nil {
		return false, err
	}
	return false, tx.Commit()
}

func (s *Service) buildSegment(ctx context.Context, id, segment int64, key string) error {
	base := s.likeBase(id)
	gen, err := s.cache.Redis.Get(ctx, base+":hot").Result()
	if errors.Is(err, redis.Nil) {
		return cache.ErrSuperseded
	}
	if err != nil {
		return err
	}
	token, owned, err := s.cache.Claim(ctx, key)
	if err != nil {
		return err
	}
	if !owned {
		return cache.ErrBusy
	}
	defer s.cache.Release(ctx, key, token)
	bits := make([]byte, segmentBits/8)
	start := segment * segmentBits
	err = s.cache.SQL(ctx, func(query context.Context) error {
		rows, err := s.db.QueryContext(query, "SELECT user_id FROM post_likes WHERE post_id=? AND user_id>=? AND user_id<?", id, start, start+segmentBits)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var user int64
			if err := rows.Scan(&user); err != nil {
				return err
			}
			offset := user - start
			bits[offset/8] |= 1 << uint(7-offset%8)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	installed, err := installSegment.Run(ctx, s.cache.Redis, []string{base + ":hot", base + ":ready", base + ":bits", key}, gen, token, segment, bits).Int()
	if err == nil && installed == 0 {
		return cache.ErrSuperseded
	}
	return err
}
