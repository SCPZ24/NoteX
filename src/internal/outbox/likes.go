package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type likeIntent struct {
	Post  int64
	User  int64
	State int
	ID    string
}

func likePosition(id string) (int64, int64, error) {
	left, right, ok := strings.Cut(id, "-")
	ms, err := strconv.ParseInt(left, 10, 64)
	seq, seqErr := strconv.ParseInt(right, 10, 64)
	if !ok || err != nil || seqErr != nil || ms <= 0 || seq < 0 {
		return 0, 0, errors.New("invalid like stream position")
	}
	return ms, seq, nil
}

func RunLikeRelay(ctx context.Context, client *redis.Client, url, prefix string) {
	broker := broker{url: url, prefix: prefix, extraQueues: []string{"likes"}}
	defer broker.close()
	stream := prefix + ":like:stream"
	for ctx.Err() == nil {
		entries, err := client.XRangeN(ctx, stream, "-", "+", 64).Result()
		if err == nil && len(entries) > 0 {
			err = relayLikes(ctx, client, &broker, stream, entries)
			if err == nil {
				continue
			}
		}
		delay := 25 * time.Millisecond
		if err != nil {
			broker.close()
			slog.Warn("like relay retrying", "error", err)
			delay = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func relayLikes(ctx context.Context, client *redis.Client, publisher *broker, stream string, entries []redis.XMessage) error {
	intents := make([]likeIntent, 0, len(entries))
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		post, postErr := strconv.ParseInt(fmt.Sprint(entry.Values["post"]), 10, 64)
		user, userErr := strconv.ParseInt(fmt.Sprint(entry.Values["user"]), 10, 64)
		state, stateErr := strconv.Atoi(fmt.Sprint(entry.Values["state"]))
		if postErr != nil || userErr != nil || stateErr != nil || post <= 0 || user <= 0 || (state != 0 && state != 1) {
			return errors.New("invalid like stream entry")
		}
		intents = append(intents, likeIntent{post, user, state, entry.ID})
		ids = append(ids, entry.ID)
	}
	body, err := json.Marshal(intents)
	if err != nil {
		return err
	}
	if err = publisher.connect(ctx); err != nil {
		return err
	}
	ms, _, err := likePosition(entries[0].ID)
	if err != nil {
		return err
	}
	if err = publisher.publish(ctx, publisher.prefix+".likes", ms, body); err != nil {
		return err
	}
	return client.XDel(ctx, stream, ids...).Err()
}

func RunLikeWorker(ctx context.Context, db *sql.DB, client *redis.Client, url, prefix string) {
	for ctx.Err() == nil {
		err := consumeLikes(ctx, db, client, url, prefix)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("like worker retrying", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consumeLikes(ctx context.Context, db *sql.DB, client *redis.Client, url, prefix string) error {
	connection := broker{url: url, prefix: prefix, extraQueues: []string{"likes"}}
	defer connection.close()
	if err := connection.connect(ctx); err != nil {
		return err
	}
	socket := connection.socket
	stop := context.AfterFunc(ctx, func() { socket.Close() })
	defer stop()
	if err := connection.channel.Qos(8, 0, false); err != nil {
		return err
	}
	deliveries, err := connection.channel.Consume(prefix+".likes", "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		var first amqp.Delivery
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, open := <-deliveries:
			if !open {
				return errors.New("like queue closed")
			}
			first = delivery
		}
		batch := []amqp.Delivery{first}
		timer := time.NewTimer(20 * time.Millisecond)
	collect:
		for len(batch) < 8 {
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				break collect
			case delivery, open := <-deliveries:
				if !open {
					timer.Stop()
					return errors.New("like queue closed")
				}
				batch = append(batch, delivery)
			}
		}
		timer.Stop()
		var intents []likeIntent
		for _, delivery := range batch {
			var group []likeIntent
			if err := json.Unmarshal(delivery.Body, &group); err != nil {
				return err
			}
			if len(group) == 0 || len(group) > 64 {
				return errors.New("invalid like batch")
			}
			intents = append(intents, group...)
		}
		if err := applyLikes(ctx, db, prefix, intents); err != nil {
			return err
		}
		if err := finishLikes(ctx, client, prefix, intents); err != nil {
			return err
		}
		if err := batch[len(batch)-1].Ack(true); err != nil {
			return err
		}
	}
}

var finishLike = redis.NewScript(`
if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end
return 0`)

func finishLikes(ctx context.Context, client *redis.Client, prefix string, intents []likeIntent) error {
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, intent := range intents {
			finishLike.Eval(ctx, pipe, []string{fmt.Sprintf("%s:like:%d:pending", prefix, intent.Post)}, intent.ID)
		}
		return nil
	})
	return err
}

func applyLikes(ctx context.Context, db *sql.DB, prefix string, intents []likeIntent) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO like_checkpoints VALUES(?,0,0) ON DUPLICATE KEY UPDATE queue_prefix=like_checkpoints.queue_prefix", prefix)
	if err != nil {
		return err
	}
	var ms, seq int64
	err = tx.QueryRowContext(ctx, "SELECT stream_ms,stream_seq FROM like_checkpoints WHERE queue_prefix=? FOR UPDATE", prefix).Scan(&ms, &seq)
	if err != nil {
		return err
	}
	type pair struct{ post, user int64 }
	final := make(map[pair]int)
	for _, intent := range intents {
		nextMS, nextSeq, err := likePosition(intent.ID)
		if err != nil {
			return err
		}
		if intent.Post <= 0 || intent.User <= 0 || (intent.State != 0 && intent.State != 1) {
			return errors.New("invalid like message")
		}
		if nextMS < ms || nextMS == ms && nextSeq <= seq {
			continue
		}
		final[pair{intent.Post, intent.User}] = intent.State
		ms, seq = nextMS, nextSeq
	}
	for _, state := range []int{0, 1} {
		var args []any
		for key, value := range final {
			if value == state {
				args = append(args, key.post, key.user)
			}
		}
		if len(args) == 0 {
			continue
		}
		values := strings.TrimSuffix(strings.Repeat("(?,?),", len(args)/2), ",")
		query := "DELETE FROM post_likes WHERE (post_id,user_id) IN (" + values + ")"
		if state == 1 {
			query = "INSERT INTO post_likes(post_id,user_id) VALUES " + values + " ON DUPLICATE KEY UPDATE user_id=post_likes.user_id"
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE like_checkpoints SET stream_ms=?,stream_seq=? WHERE queue_prefix=?", ms, seq, prefix); err != nil {
		return err
	}
	return tx.Commit()
}
