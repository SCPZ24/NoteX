package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ShardName is stable across worker-count changes. Count belongs to deployment
// topology; the task envelope and durable task body retain it for replay.
func ShardName(shard int) string { return fmt.Sprintf("feed.chunk.s%03d", shard) }

func isShard(name string) bool {
	if !strings.HasPrefix(name, "feed.chunk.s") {
		return false
	}
	id, err := strconv.Atoi(strings.TrimPrefix(name, "feed.chunk.s"))
	return err == nil && id >= 0 && id < 32 && name == ShardName(id)
}

// RunShard consumes one quorum/SAC queue serially. SQL retries are published
// back into that queue, so standby subscriptions never execute effects locally.
// Cancellation stops admission, with a bounded window for the current effect.
func RunShard(ctx context.Context, db *sql.DB, url, prefix string, shard, priority int, handler ExternalHandler, subscribed func()) error {
	name := ShardName(shard)
	if !isShard(name) || handler == nil {
		return errors.New("invalid shard consumer")
	}
	consumer := prefix + "." + name
	for ctx.Err() == nil {
		err := shardSession(ctx, db, url, prefix, name, consumer, priority, handler, subscribed)
		if ctx.Err() != nil {
			return nil
		}
		slog.Warn("shard session interrupted", "queue", consumer, "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
	return nil
}

func shardSession(ctx context.Context, db *sql.DB, url, prefix, name, consumer string, priority int, handler ExternalHandler, subscribed func()) error {
	b := &broker{url: url, prefix: prefix, extraQueues: []string{name}}
	defer b.close()
	if err := b.connect(ctx); err != nil {
		return err
	}
	socket := b.socket
	setup, cancel := context.WithTimeout(ctx, operationTimeout)
	stopSetup := context.AfterFunc(setup, func() { socket.Close() })
	err := b.channel.Qos(1, 0, false)
	var deliveries <-chan amqp.Delivery
	if err == nil {
		deliveries, err = b.channel.Consume(consumer, "", false, false, false, false, amqp.Table{"x-priority": int32(priority)})
	}
	stopSetup()
	cancel()
	if err != nil {
		return err
	}
	if subscribed != nil {
		subscribed()
	}
	slog.Info("shard subscribed", "queue", consumer, "priority", priority)
	defer slog.Info("shard subscription closed", "queue", consumer)
	// Keep the current handler alive during drain. Socket failure cancels it
	// immediately; shutdown cancels it after the same five-second drain budget.
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	drainDone := make(chan struct{})
	defer close(drainDone)
	go func() {
		select {
		case <-ctx.Done():
			slog.Info("shard drain started", "queue", consumer, "deadline_ms", operationTimeout.Milliseconds())
		case <-drainDone:
			return
		}
		select {
		case <-time.After(operationTimeout):
			cancelWork()
			socket.Close()
		case <-drainDone:
		}
	}()
	closed := b.channel.NotifyClose(make(chan *amqp.Error, 1))
	connectionDone := make(chan struct{})
	defer close(connectionDone)
	go func() {
		select {
		case <-closed:
			cancelWork()
		case <-connectionDone:
		}
	}()
	subscribedAt := time.Now()
	firstDelivery := true
	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-work.Done():
			return work.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("shard delivery stream closed")
			}
			if ctx.Err() != nil {
				return nil
			}
			if firstDelivery {
				slog.Info("shard first delivery", "queue", consumer, "priority", priority, "since_subscribe_ms", time.Since(subscribedAt).Milliseconds(), "event_id", delivery.MessageId)
				firstDelivery = false
			}
			id, err := receive(work, db, consumer, delivery.MessageId, delivery.Body)
			if err != nil {
				return err
			}
			// Receipt takes retry ownership back from MQ before attempting work.
			// A late publisher confirm cannot overwrite this reset token.
			if _, err = update(work, db, `UPDATE consumption_jobs SET retry_publish_after=0,retry_publish_token='' WHERE id=? AND consumer=?`, id, consumer); err != nil {
				return err
			}
			if delivery.Redelivered {
				slog.Info("shard redelivery", "queue", consumer, "event_id", delivery.MessageId)
			}
			if err = processExternal(work, db, consumer, id, handler); err != nil {
				return err
			}
			ack, cancel := context.WithTimeout(work, operationTimeout)
			stop := context.AfterFunc(ack, func() { socket.Close() })
			err = delivery.Ack(false)
			stop()
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

// RunShardRetries republishes due durable jobs with their original bytes and
// identities. Multiple publishers may duplicate receipts; claimExternal handles
// duplication. No row lock is held across publisher confirms.
func RunShardRetries(ctx context.Context, db *sql.DB, url, prefix string, count int) {
	b := &broker{url: url, prefix: prefix}
	for shard := 0; shard < count; shard++ {
		b.extraQueues = append(b.extraQueues, ShardName(shard))
	}
	defer b.close()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		for shard := 0; shard < count && ctx.Err() == nil; shard++ {
			queue := prefix + "." + ShardName(shard)
			if err := republishDue(ctx, db, b, queue); err != nil {
				b.close()
				slog.Warn("shard retry publish failed", "queue", queue, "error", err)
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func republishDue(ctx context.Context, db *sql.DB, b *broker, queue string) error {
	query, cancel := context.WithTimeout(ctx, operationTimeout)
	now := time.Now().Unix()
	rows, err := db.QueryContext(query, `SELECT id,event_id,body FROM consumption_jobs WHERE consumer=? AND status='pending' AND next_attempt_on<=? AND retry_publish_after<=? ORDER BY next_attempt_on,id LIMIT ?`, queue, now, now, batchSize)
	if err != nil {
		cancel()
		return err
	}
	type pending struct {
		jobID int64
		id    int64
		body  []byte
	}
	var jobs []pending
	for rows.Next() {
		var job pending
		if err = rows.Scan(&job.jobID, &job.id, &job.body); err != nil {
			break
		}
		jobs = append(jobs, job)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	cancel()
	if err != nil || len(jobs) == 0 {
		return err
	}
	if err = b.connect(ctx); err != nil {
		return err
	}
	for _, job := range jobs {
		token := rand.Text()
		now = time.Now().Unix()
		result, err := update(ctx, db, `UPDATE consumption_jobs SET retry_publish_after=?,retry_publish_token=? WHERE id=? AND consumer=? AND status='pending' AND next_attempt_on<=? AND retry_publish_after<=?`, now+30, token, job.jobID, queue, now, now)
		if err != nil {
			return err
		}
		claimed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if claimed == 0 {
			continue
		}
		if err = b.publish(ctx, queue, job.id, job.body); err != nil {
			return err
		}
		if _, err = update(ctx, db, `UPDATE consumption_jobs SET retry_publish_after=? WHERE id=? AND retry_publish_token=?`, int64(math.MaxInt64), job.jobID, token); err != nil {
			return err
		}
	}
	return nil
}
