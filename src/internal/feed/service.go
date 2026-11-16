package feed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/redis/go-redis/v9"
)

// Entry is the immutable identity and ordering data stored in an author index.
type Entry struct {
	ID        int64 `json:"id"`
	CreatedOn int64 `json:"created_on"`
}

type Service struct {
	db     *sql.DB
	redis  *redis.Client
	prefix string
}

// New creates a bounded client without requiring Redis to be online at startup.
// The Redis instance must use noeviction; deletion markers live with the index.
func New(db *sql.DB, redisURL, prefix string) (*Service, error) {
	options, err := redis.ParseURL(redisURL)
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
	return &Service{db: db, redis: redis.NewClient(options), prefix: prefix}, nil
}

// Close releases the Redis client after the consumer has stopped.
func (s *Service) Close() error { return s.redis.Close() }

// Handle reconciles a post's current authoritative state with its author index.
// Relationship roots remain durably waiting until the fanout stage can handle them.
func (s *Service) Handle(ctx context.Context, event outbox.Event) error {
	switch event.Type {
	case outbox.FollowCreated, outbox.FollowDeleted:
		return fmt.Errorf("%w: relationship projection awaits fanout stage", outbox.ErrWaiting)
	case outbox.PostCreated, outbox.PostDeleted:
	default:
		return fmt.Errorf("%w: unsupported feed event type", outbox.ErrPermanent)
	}
	var payload outbox.PostPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.PostID <= 0 {
		return fmt.Errorf("%w: invalid post identity", outbox.ErrPermanent)
	}
	var entry Entry
	var author int64
	var deleted sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT id,author_id,created_on,deleted_on FROM posts WHERE id=?", payload.PostID).
		Scan(&entry.ID, &author, &entry.CreatedOn, &deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: authoritative post %d missing", outbox.ErrPermanent, payload.PostID)
	}
	if err != nil {
		return err
	}
	return s.apply(ctx, author, entry, deleted.Valid)
}

// Both keys share an author hash tag. IDs remain strings inside Lua, preserving
// int64 precision. Mark deletion before removing membership so a partially
// failed script cannot permit an old add to resurrect a deleted post.
var updateAuthor = redis.NewScript(`
if ARGV[3] == 'deleted' then
 redis.call('SADD', KEYS[2], ARGV[1])
 redis.call('ZREM', KEYS[1], ARGV[2])
 return 1
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
 return 0
end
redis.call('ZADD', KEYS[1], 0, ARGV[2])
return 1
`)

func (s *Service) keys(author int64) []string {
	base := fmt.Sprintf("%s:author:{%d}", s.prefix, author)
	return []string{base + ":posts", base + ":deleted"}
}

func member(entry Entry) string {
	return fmt.Sprintf("%020d:%020d", entry.CreatedOn, entry.ID)
}

func (s *Service) apply(ctx context.Context, author int64, entry Entry, deleted bool) error {
	if author <= 0 || entry.ID <= 0 || entry.CreatedOn <= 0 {
		return fmt.Errorf("%w: invalid author index entry", outbox.ErrPermanent)
	}
	state := "active"
	if deleted {
		state = "deleted"
	}
	return updateAuthor.Run(ctx, s.redis, s.keys(author), strconv.FormatInt(entry.ID, 10), member(entry), state).Err()
}

// ReadAuthor returns at most limit entries, descending by (created_on, post_id).
// before is exclusive. Callers still validate current relationships/deletions
// against MySQL before serving these candidates as a user-visible Feed.
func (s *Service) ReadAuthor(ctx context.Context, author int64, before *Entry, limit int) ([]Entry, error) {
	if author <= 0 || limit < 1 || limit > 100 {
		return nil, errors.New("invalid author or page size")
	}
	maximum := "+"
	if before != nil {
		if before.ID <= 0 || before.CreatedOn <= 0 {
			return nil, errors.New("invalid author index cursor")
		}
		maximum = "(" + member(*before)
	}
	values, err := s.redis.ZRevRangeByLex(ctx, s.keys(author)[0], &redis.ZRangeBy{Max: maximum, Min: "-", Count: int64(limit)}).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(values))
	for _, value := range values {
		if len(value) != 41 || value[20] != ':' {
			return nil, errors.New("invalid stored author index member")
		}
		created, timeErr := strconv.ParseInt(value[:20], 10, 64)
		id, idErr := strconv.ParseInt(value[21:], 10, 64)
		entry := Entry{ID: id, CreatedOn: created}
		if timeErr != nil || idErr != nil || created <= 0 || id <= 0 || member(entry) != value {
			return nil, errors.New("invalid stored author index ordering")
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
