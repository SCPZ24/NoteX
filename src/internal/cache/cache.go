// Package cache bounds process-local work and coordinates disposable Redis builds.
package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrBusy = errors.New("cache capacity exhausted")
var ErrSuperseded = errors.New("cache build superseded")

const BuildTimeout = 2 * time.Second
const Lease = 3 * time.Second

type flight struct {
	done chan struct{}
	err  error
}

// Store is shared by body and author caches within one application instance.
// Its bounded flight table implements singleflight without a dependency or an
// unbounded goroutine queue. It stores no completed content in process memory.
type Store struct {
	Redis    *redis.Client
	Prefix   string
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	flights  map[string]*flight
	closed   bool
	wg       sync.WaitGroup
	callers  chan struct{}
	sql      chan struct{}
	queue    chan struct{}
	counters sync.Map
}

// Open creates the bounded Redis client used by cache-owning processes.
// The caller closes Store before closing Store.Redis.
func Open(url, prefix string) (*Store, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, errors.New("invalid REDIS_URL")
	}
	options.DialTimeout = 2 * time.Second
	options.ReadTimeout = 2 * time.Second
	options.WriteTimeout = 2 * time.Second
	options.PoolTimeout = 2 * time.Second
	options.PoolSize = 4
	options.MaxRetries = -1
	options.ContextTimeoutEnabled = true
	return New(redis.NewClient(options), prefix), nil
}

func New(r *redis.Client, prefix string) *Store {
	ctx, cancel := context.WithCancel(context.Background())
	return &Store{Redis: r, Prefix: prefix, ctx: ctx, cancel: cancel, flights: make(map[string]*flight), callers: make(chan struct{}, 64), sql: make(chan struct{}, 4), queue: make(chan struct{}, 16)}
}

// Close cancels shared tasks and waits before their Redis/SQL dependencies close.
func (s *Store) Close() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Store) Count(name string) {
	v, _ := s.counters.LoadOrStore(name, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(1)
}
func (s *Store) Stats() map[string]uint64 {
	result := map[string]uint64{}
	s.counters.Range(func(k, v any) bool { result[k.(string)] = v.(*atomic.Uint64).Load(); return true })
	return result
}

// Do starts immediately or joins an overlapping key. A caller cancellation does
// not cancel the shared task; shutdown and BuildTimeout do. Admission precedes
// task creation, and the flight table itself is bounded even if callers cancel.
func (s *Store) Do(ctx context.Context, key string, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.callers <- struct{}{}:
		defer func() { <-s.callers }()
	default:
		s.Count("rejected")
		return ErrBusy
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return context.Canceled
	}
	f, exists := s.flights[key]
	if !exists {
		if len(s.flights) >= 64 {
			s.mu.Unlock()
			s.Count("rejected")
			return ErrBusy
		}
		f = &flight{done: make(chan struct{})}
		s.flights[key] = f
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			task, cancel := context.WithTimeout(s.ctx, BuildTimeout)
			defer cancel()
			err := fn(task)
			s.mu.Lock()
			f.err = err
			delete(s.flights, key)
			close(f.done)
			s.mu.Unlock()
		}()
		s.Count("build_started")
	} else {
		s.Count("shared_wait")
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.Count("wait_canceled")
		return ctx.Err()
	case <-f.done:
		return f.err
	}
}

// SQL bounds execution and queueing, including fallback. Never hold this permit
// while waiting for Redis or another build. The caller supplies a query deadline.
func (s *Store) SQL(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.sql <- struct{}{}:
	default:
		select {
		case s.queue <- struct{}{}:
		default:
			s.Count("sql_rejected")
			return ErrBusy
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case s.sql <- struct{}{}:
			timer.Stop()
			<-s.queue
		case <-timer.C:
			<-s.queue
			s.Count("sql_rejected")
			return ErrBusy
		case <-ctx.Done():
			timer.Stop()
			<-s.queue
			return ctx.Err()
		}
	}
	defer func() { <-s.sql }()
	s.Count("sql_operations")
	return fn(ctx)
}

func Token() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Claim grants a short, non-renewing lease. Redis failures are not cache misses.
func (s *Store) Claim(ctx context.Context, key string) (string, bool, error) {
	token, err := Token()
	if err != nil {
		return "", false, err
	}
	ok, err := s.Redis.SetNX(ctx, key, token, Lease).Result()
	if err == nil && !ok {
		s.Count("lease_contended")
	}
	return token, ok, err
}

var release = redis.NewScript(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`)

func (s *Store) Release(ctx context.Context, key, token string) {
	_ = release.Run(ctx, s.Redis, []string{key}, token).Err()
}

// BodyBase namespaces data by Redis process identity. A restored old AOF/RDB
// must not make an old body visible without its newer deletion marker. INFO is
// deliberately checked per operation; caching run_id locally defeats recovery.
// In-place restore/replica rollback requires an operational cache purge as well.
func (s *Store) BodyBase(ctx context.Context, id string) (string, error) {
	info, err := s.Redis.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			return s.Prefix + ":body:" + strings.TrimSpace(strings.TrimPrefix(line, "run_id:")) + ":{" + id + "}", nil
		}
	}
	return "", errors.New("Redis run_id missing")
}
