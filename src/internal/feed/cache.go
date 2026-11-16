package feed

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	activeWindow    = 7 * 24 * time.Hour
	retainedPosts   = 500
	maxPullAuthors  = 64
	candidateBudget = 8192
	maxReadRounds   = 8
)

type cacheSource struct {
	owner     int64
	author    bool
	signature string
	state     cacheState
}
type cacheState struct {
	generation string
	signature  string
	ready      bool
	floor      string
	count      int
	revision   string
}
type scriptCall struct {
	keys []string
	args []any
}

func (s *Service) cacheKeys(source cacheSource) []string {
	if source.author {
		base := fmt.Sprintf("%s:author:{%d}", s.prefix, source.owner)
		return []string{base + ":posts", base + ":deleted", base + ":relations", base + ":cache"}
	}
	return append(s.readerKeys(source.owner), fmt.Sprintf("%s:reader:{%d}:cache", s.prefix, source.owner))
}

// Touching is an HTTP activity operation (authors also renew on publication).
// A fresh generation atomically invalidates old workers before clearing data.
var touchCache = redis.NewScript(`
local gen=redis.call('HGET',KEYS[4],'generation')
local signature=redis.call('HGET',KEYS[4],'signature')
local valid=gen and (ARGV[4]=='reader' or signature==ARGV[1])
if valid and redis.call('HGET',KEYS[4],'ready')=='1' then
 local n=tonumber(redis.call('HGET',KEYS[4],'count') or '-1')
 valid=redis.call('ZCARD',KEYS[1])==n
 if ARGV[4]=='reader' then valid=valid and redis.call('HLEN',KEYS[3])==n end
end
if not valid then
 redis.call('DEL',unpack(KEYS))
 gen=ARGV[2]
 redis.call('HSET',KEYS[4],'generation',gen,'signature',ARGV[1],'ready','0','floor','','count','0','revision','0')
end
for _,key in ipairs(KEYS) do redis.call('PEXPIRE',key,ARGV[3]) end
return {gen,redis.call('HGET',KEYS[4],'ready'),redis.call('HGET',KEYS[4],'floor'),redis.call('HGET',KEYS[4],'count'),redis.call('HGET',KEYS[4],'revision'),redis.call('HGET',KEYS[4],'signature')}
`)

func (s *Service) touchSources(ctx context.Context, sources []cacheSource) error {
	for start := 0; start < len(sources); start += pipelineSize {
		end := min(start+pipelineSize, len(sources))
		calls := make([]scriptCall, 0, end-start)
		for _, source := range sources[start:end] {
			var token [16]byte
			if _, err := rand.Read(token[:]); err != nil {
				return err
			}
			kind := "reader"
			if source.author {
				kind = "author"
			}
			calls = append(calls, scriptCall{s.cacheKeys(source), []any{source.signature, fmt.Sprintf("%x", token), activeWindow.Milliseconds(), kind}})
		}
		commands, err := s.scriptBatch(ctx, touchCache, calls)
		if err != nil {
			return err
		}
		for i, command := range commands {
			values, err := command.Slice()
			if err != nil {
				return err
			}
			state, err := parseCacheState(values)
			if err != nil {
				return err
			}
			sources[start+i].state = state
		}
	}
	return nil
}

func parseCacheState(values []any) (cacheState, error) {
	if len(values) < 5 {
		return cacheState{}, fmt.Errorf("invalid cache metadata")
	}
	state := cacheState{generation: fmt.Sprint(values[0]), ready: fmt.Sprint(values[1]) == "1", floor: fmt.Sprint(values[2]), revision: fmt.Sprint(values[4])}
	if len(values) > 5 {
		state.signature = fmt.Sprint(values[5])
	}
	count, err := strconv.Atoi(fmt.Sprint(values[3]))
	state.count = count
	if err != nil || count < 0 || count > retainedPosts || state.generation == "" {
		return cacheState{}, fmt.Errorf("invalid cache metadata")
	}
	if state.floor != "" {
		if _, err := parseMember(state.floor); err != nil {
			return cacheState{}, err
		}
	}
	return state, nil
}

// Each script is bounded by one post update and trimming at most one overflow.
// Revision is advanced before effects so partial script failure invalidates a
// concurrent SQL snapshot. A writer carries its generation from before SQL reads.
var cacheUpdate = redis.NewScript(`
if redis.call('HGET',KEYS[4],'generation')~=ARGV[5] then return 0 end
local ttl=redis.call('PTTL',KEYS[4])
if ttl<=0 then return 0 end
redis.call('HINCRBY',KEYS[4],'revision',1)
local floor=redis.call('HGET',KEYS[4],'floor') or ''
if floor~='' and ARGV[2]<floor then return 0 end
if ARGV[3]=='deleted' then
 redis.call('SADD',KEYS[2],ARGV[1])
 redis.call('PEXPIRE',KEYS[2],ttl)
 redis.call('ZREM',KEYS[1],ARGV[2])
 redis.call('HDEL',KEYS[3],ARGV[1])
elseif redis.call('SISMEMBER',KEYS[2],ARGV[1])==0 and redis.call('HEXISTS',KEYS[4],'blocked:'..ARGV[4])==0 then
 redis.call('ZADD',KEYS[1],0,ARGV[2])
 redis.call('PEXPIRE',KEYS[1],ttl)
 if ARGV[4]~='' then redis.call('HSET',KEYS[3],ARGV[1],ARGV[4]); redis.call('PEXPIRE',KEYS[3],ttl) end
end
local n=redis.call('ZCARD',KEYS[1])
if n>tonumber(ARGV[6]) then
 local removed=redis.call('ZRANGE',KEYS[1],0,n-tonumber(ARGV[6])-1)
 for _,value in ipairs(removed) do
  redis.call('ZREM',KEYS[1],value)
  local id=string.gsub(string.sub(value,22),'^0+','')
  redis.call('HDEL',KEYS[3],id)
 end
 local oldest=redis.call('ZRANGE',KEYS[1],0,0)[1]
 if oldest and oldest>floor then redis.call('HSET',KEYS[4],'floor',oldest) end
end
redis.call('HSET',KEYS[4],'count',redis.call('ZCARD',KEYS[1]))
for _,key in ipairs(KEYS) do redis.call('PEXPIRE',key,ttl) end
return 1
`)

// SQL snapshot installation is guarded by both generation and revision. At most
// 500 members are installed. Any event applied during the snapshot forces retry.
var installCache = redis.NewScript(`
if redis.call('HGET',KEYS[4],'generation')~=ARGV[1] or redis.call('HGET',KEYS[4],'revision')~=ARGV[2] then return 0 end
local ttl=redis.call('PTTL',KEYS[4])
if ttl<=0 then return 0 end
local n=tonumber(ARGV[4])
if not n or n<0 or n>500 or (#ARGV~=4+n*3 and #ARGV~=5+n*3) then return redis.error_reply('invalid cache snapshot') end
redis.call('HSET',KEYS[4],'ready','0')
redis.call('HINCRBY',KEYS[4],'revision',1)
local desired={}
for i=0,n-1 do desired[ARGV[5+i*3]]=true end
local old=redis.call('ZRANGE',KEYS[1],0,-1)
for _,value in ipairs(old) do
 if not desired[value] then
  redis.call('ZREM',KEYS[1],value)
  local id=string.gsub(string.sub(value,22),'^0+','')
  redis.call('HDEL',KEYS[3],id)
 end
end
for i=0,n-1 do
 local at=5+i*3
 redis.call('ZADD',KEYS[1],0,ARGV[at])
 if i==0 then redis.call('PEXPIRE',KEYS[1],ttl) end
 if ARGV[at+2]~='' then redis.call('HSET',KEYS[3],ARGV[at+1],ARGV[at+2]); if i==0 then redis.call('PEXPIRE',KEYS[3],ttl) end end
end
redis.call('HSET',KEYS[4],'floor',ARGV[3],'count',n,'ready','1')
if #ARGV==5+n*3 then redis.call('HSET',KEYS[4],'signature',ARGV[5+n*3]) end
for _,key in ipairs(KEYS) do redis.call('PEXPIRE',key,ttl) end
return 1
`)

// Every Redis pipeline, including script-cache fallback, has at most 32 commands.
func (s *Service) scriptBatch(ctx context.Context, script *redis.Script, calls []scriptCall) ([]*redis.Cmd, error) {
	if len(calls) == 0 || len(calls) > pipelineSize {
		return nil, fmt.Errorf("invalid pipeline size")
	}
	pipe := s.redis.Pipeline()
	commands := make([]*redis.Cmd, 0, len(calls))
	for _, call := range calls {
		commands = append(commands, script.EvalSha(ctx, pipe, call.keys, call.args...))
	}
	_, err := pipe.Exec(ctx)
	if err != nil && !redis.HasErrorPrefix(err, "NOSCRIPT") {
		return nil, err
	}
	fallback := s.redis.Pipeline()
	for i, command := range commands {
		if redis.HasErrorPrefix(command.Err(), "NOSCRIPT") {
			commands[i] = script.Eval(ctx, fallback, calls[i].keys, calls[i].args...)
		} else if command.Err() != nil {
			return nil, command.Err()
		}
	}
	if fallback.Len() > 0 {
		if _, err := fallback.Exec(ctx); err != nil {
			return nil, err
		}
	}
	return commands, nil
}

// Read-only token acquisition does not renew reader activity. This runs before
// fetching authoritative post/relationship state for the associated update.
func (s *Service) readerGenerations(ctx context.Context, targets []recipient) error {
	for start := 0; start < len(targets); start += pipelineSize {
		end := min(start+pipelineSize, len(targets))
		pipe := s.redis.Pipeline()
		commands := make([]*redis.StringCmd, 0, end-start)
		for _, target := range targets[start:end] {
			commands = append(commands, pipe.HGet(ctx, s.cacheKeys(cacheSource{owner: target.UserID})[3], "generation"))
		}
		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return err
		}
		for i, command := range commands {
			value, err := command.Result()
			if err != nil && err != redis.Nil {
				return err
			}
			targets[start+i].generation = value
		}
	}
	return nil
}
