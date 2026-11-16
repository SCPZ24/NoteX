package feed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/redis/go-redis/v9"
)

// All keys share a reader hash tag. Relationship identity is retained for later
// lifecycle-specific cleanup. Each script affects exactly one reader/post pair.
var updateReader = redis.NewScript(`
if ARGV[3] == 'deleted' then
 redis.call('SADD', KEYS[2], ARGV[1])
 redis.call('ZREM', KEYS[1], ARGV[2])
 redis.call('HDEL', KEYS[3], ARGV[1])
 return 1
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
 return 0
end
redis.call('ZADD', KEYS[1], 0, ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[4])
return 1
`)

func (s *Service) readerKeys(reader int64) []string {
	base := fmt.Sprintf("%s:reader:{%d}", s.prefix, reader)
	return []string{base + ":posts", base + ":deleted", base + ":relations"}
}

func (s *Service) distribute(ctx context.Context, event outbox.Event) error {
	current, err := loadTask(ctx, s.db, event, false)
	if err != nil {
		return err
	}
	targets := current.Body.Recipients
	if current.Kind != "feed.chunk" || len(targets) == 0 || len(targets) > chunkSize {
		return fmt.Errorf("%w: invalid distribution chunk", outbox.ErrPermanent)
	}
	var entry Entry
	var author int64
	var deleted sql.NullInt64
	err = s.db.QueryRowContext(ctx, "SELECT id,author_id,created_on,deleted_on FROM posts WHERE id=?", current.Body.PostID).
		Scan(&entry.ID, &author, &entry.CreatedOn, &deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: distribution post missing", outbox.ErrPermanent)
	}
	if err != nil {
		return err
	}
	if author <= 0 || entry.ID <= 0 || entry.CreatedOn <= 0 {
		return fmt.Errorf("%w: invalid distribution post", outbox.ErrPermanent)
	}
	for _, target := range targets {
		if target.UserID <= 0 || target.RelationID < 0 || (target.RelationID == 0 && target.UserID != author) {
			return fmt.Errorf("%w: invalid recipient", outbox.ErrPermanent)
		}
	}
	// Deletion can remove a previously delivered member even after an unfollow.
	// Active additions check the exact currently existing relationship identity.
	if !deleted.Valid {
		args := []any{author}
		for _, target := range targets {
			args = append(args, target.RelationID)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(targets)), ",")
		rows, err := s.db.QueryContext(ctx, "SELECT id,user_id FROM following WHERE follow_id=? AND id IN ("+placeholders+")", args...)
		if err != nil {
			return err
		}
		active := make(map[int64]int64, len(targets))
		for rows.Next() {
			var relation, user int64
			if err := rows.Scan(&relation, &user); err != nil {
				rows.Close()
				return err
			}
			active[relation] = user
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		eligible := make([]recipient, 0, len(targets))
		for _, target := range targets {
			if (target.UserID == author && target.RelationID == 0) || active[target.RelationID] == target.UserID {
				eligible = append(eligible, target)
			}
		}
		targets = eligible
	}
	for offset := 0; offset < len(targets); offset += pipelineSize {
		end := min(offset+pipelineSize, len(targets))
		if err := s.writeReaderBatch(ctx, entry, targets[offset:end], deleted.Valid); err != nil {
			return err
		}
	}
	return nil
}

// Each Exec contains at most 32 fixed-size scripts. Only NOSCRIPT responses are
// resent with EVAL, in a second equally bounded batch. Uncertain I/O or partial
// errors return to durable chunk retry; successful effects tolerate repetition.
func (s *Service) writeReaderBatch(ctx context.Context, entry Entry, targets []recipient, deleted bool) error {
	if len(targets) == 0 || len(targets) > pipelineSize {
		return fmt.Errorf("%w: invalid pipeline size", outbox.ErrPermanent)
	}
	state := "active"
	if deleted {
		state = "deleted"
	}
	args := func(target recipient) []any {
		return []any{strconv.FormatInt(entry.ID, 10), member(entry), state, strconv.FormatInt(target.RelationID, 10)}
	}
	pipe := s.redis.Pipeline()
	commands := make([]*redis.Cmd, 0, len(targets))
	for _, target := range targets {
		commands = append(commands, updateReader.EvalSha(ctx, pipe, s.readerKeys(target.UserID), args(target)...))
	}
	_, err := pipe.Exec(ctx)
	if err != nil && !redis.HasErrorPrefix(err, "NOSCRIPT") {
		return err
	}
	fallback := s.redis.Pipeline()
	for i, command := range commands {
		if redis.HasErrorPrefix(command.Err(), "NOSCRIPT") {
			updateReader.Eval(ctx, fallback, s.readerKeys(targets[i].UserID), args(targets[i])...)
		} else if command.Err() != nil {
			return command.Err()
		}
	}
	if fallback.Len() > 0 {
		_, err = fallback.Exec(ctx)
		return err
	}
	return nil
}
