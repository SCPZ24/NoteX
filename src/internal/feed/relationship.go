package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/redis/go-redis/v9"
)

// Invalidate only the old relationship's writes. Keep other members and the
// reader's activity deadline. The revision fences SQL snapshots already running.
var closeRelationship = redis.NewScript(`
if redis.call('EXISTS',KEYS[4])==0 then return 0 end
redis.call('HSET',KEYS[4],'blocked:'..ARGV[1],'1')
redis.call('HINCRBY',KEYS[4],'revision',1)
for _,value in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do
 local id=string.gsub(string.sub(value,22),'^0+','')
 if redis.call('HGET',KEYS[3],id)==ARGV[1] then
  redis.call('ZREM',KEYS[1],value)
  redis.call('HDEL',KEYS[3],id)
 end
end
redis.call('HSET',KEYS[4],'count',redis.call('ZCARD',KEYS[1]))
return 1
`)

// Reconcile all currently committed relationships in one bounded SQL snapshot.
// Applying its difference preserves unrelated Redis members and publishes the
// coverage signature only after every member is installed atomically.
func (s *Service) reconcileRelationship(ctx context.Context, event outbox.Event) error {
	var payload outbox.FollowPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.ActorID <= 0 || payload.TargetUserID <= 0 || payload.RelationID <= 0 {
		return fmt.Errorf("%w: invalid relationship identity", outbox.ErrPermanent)
	}
	keys := s.cacheKeys(cacheSource{owner: payload.ActorID})
	key := keys[3]
	if event.Type == outbox.FollowDeleted {
		if err := closeRelationship.Run(ctx, s.redis, keys, strconv.FormatInt(payload.RelationID, 10)).Err(); err != nil {
			return err
		}
	}
	values, err := s.redis.HMGet(ctx, key, "generation", "ready", "floor", "count", "revision", "signature").Result()
	if err != nil {
		return err
	}
	if values[0] == nil {
		return nil
	}
	state, err := parseCacheState(values)
	if err != nil {
		return err
	}
	sources, _, err := s.sources(ctx, payload.ActorID)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return nil
	} // Oversized relationship sets always read SQL.
	sources[0].state = state
	return s.warmSource(ctx, sources[0])
}
