package outbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxConsumerAttempts = 5

// ErrPermanent marks a handler error that needs explicit operator intervention.
var ErrPermanent = errors.New("permanent consumption failure")

// SQLHandler writes effects using the supplied transaction. Returning an error
// rolls back those effects. External Redis/search writes need their own recovery
// protocol before they can use a completed job as proof of completion.
type SQLHandler func(context.Context, *sql.Tx, Event) error

// RunConsumer owns one AMQP connection and processes one delivery at a time.
// A durable job survives both ACK loss and a later broker outage.
func RunConsumer(ctx context.Context, db *sql.DB, url, prefix, name string, handler SQLHandler) error {
	if (name != "feed" && name != "search") || handler == nil {
		return errors.New("consumer requires feed/search and a handler")
	}
	return runConsumer(ctx, db, url, prefix, name, func(ctx context.Context, id int64) error {
		return processJob(ctx, db, prefix+"."+name, id, handler)
	})
}

func runConsumer(ctx context.Context, db *sql.DB, url, prefix, name string, process func(context.Context, int64) error) error {
	b := &broker{url: url, prefix: prefix}
	defer b.close()
	consumer := prefix + "." + name
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		err := retryConsumption(ctx, db, consumer, process)
		if err == nil {
			err = consumeSession(ctx, db, b, name, process)
		}
		b.close()
		if ctx.Err() != nil {
			return nil
		}
		slog.Error("consumer interrupted", "consumer", consumer, "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}

func consumeSession(ctx context.Context, db *sql.DB, b *broker, name string, process func(context.Context, int64) error) error {
	if err := b.connect(ctx); err != nil {
		return err
	}
	// Bound AMQP RPCs during setup, then keep only cancellation on the session.
	setupCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	socket := b.socket
	stopSetup := context.AfterFunc(setupCtx, func() { socket.Close() })
	consumer := b.prefix + "." + name
	err := b.channel.Qos(1, 0, false)
	var deliveries <-chan amqp.Delivery
	if err == nil {
		deliveries, err = b.channel.Consume(consumer, "", false, false, false, false, nil)
	}
	stopSetup()
	cancel()
	if err != nil {
		return err
	}
	stopClose := context.AfterFunc(ctx, func() { socket.Close() })
	defer stopClose()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := retryConsumption(ctx, db, consumer, process); err != nil {
				return err
			}
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("RabbitMQ delivery channel closed")
			}
			id, err := receive(ctx, db, consumer, delivery.MessageId, delivery.Body)
			if err != nil {
				return err // Connection close requeues the unacknowledged delivery.
			}
			if err := process(ctx, id); err != nil {
				return err
			}
			// Success, scheduled retry, or quarantine is now durable in MySQL.
			ackCtx, cancel := context.WithTimeout(ctx, operationTimeout)
			stopAck := context.AfterFunc(ackCtx, func() { socket.Close() })
			err = delivery.Ack(false)
			stopAck()
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func decodeDelivery(consumer, messageID string, body []byte) (Event, error) {
	var item Event
	if err := json.Unmarshal(body, &item); err != nil {
		return item, errors.New("invalid event JSON")
	}
	if item.ID <= 0 || item.CreatedOn <= 0 || messageID != strconv.FormatInt(item.ID, 10) {
		return item, errors.New("invalid event identity or creation time")
	}
	queues, err := item.destinations("")
	if err != nil {
		return item, err
	}
	for _, suffix := range queues {
		if strings.HasSuffix(consumer, suffix) {
			return item, nil
		}
	}
	return item, errors.New("event does not belong to this consumer")
}

// receive preserves the original bytes, including malformed JSON. Valid events
// deduplicate by consumer/event ID; invalid/conflicting inputs use a fingerprint.
func receive(ctx context.Context, db *sql.DB, consumer, messageID string, body []byte) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if body == nil {
		body = []byte{} // Preserve an empty delivery as an empty BLOB, not SQL NULL.
	}
	item, invalid := decodeDelivery(consumer, messageID, body)
	key := "event:" + strconv.FormatInt(item.ID, 10)
	if invalid != nil {
		return quarantine(ctx, db, consumer, messageID, body, invalid)
	}
	_, err := db.ExecContext(ctx, `INSERT INTO consumption_jobs
		(consumer,event_key,event_id,message_id,body,status,created_on)
		VALUES (?,?,?,?,?,'pending',?) ON DUPLICATE KEY UPDATE id=id`,
		consumer, key, item.ID, messageID, body, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	var id int64
	var saved []byte
	if err := db.QueryRowContext(ctx, `SELECT id,body FROM consumption_jobs WHERE consumer=? AND event_key=?`,
		consumer, key).Scan(&id, &saved); err != nil {
		return 0, err
	}
	if !bytes.Equal(saved, body) {
		return quarantine(ctx, db, consumer, messageID, body, errors.New("event ID reused with different content"))
	}
	return id, nil
}

func quarantine(ctx context.Context, db *sql.DB, consumer, messageID string, body []byte, cause error) (int64, error) {
	fingerprint := sha256.New()
	fingerprint.Write([]byte(messageID))
	fingerprint.Write([]byte{0})
	fingerprint.Write(body)
	key := fmt.Sprintf("invalid:%x", fingerprint.Sum(nil))
	_, err := db.ExecContext(ctx, `INSERT INTO consumption_jobs
		(consumer,event_key,message_id,body,status,last_error,created_on,failed_on)
		VALUES (?,?,?,?,'failed',?,?,?) ON DUPLICATE KEY UPDATE id=id`,
		consumer, key, messageID, body, cause.Error(), time.Now().Unix(), time.Now().Unix())
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRowContext(ctx, `SELECT id FROM consumption_jobs WHERE consumer=? AND event_key=?`, consumer, key).Scan(&id)
	return id, err
}

func retryConsumption(ctx context.Context, db *sql.DB, consumer string, process func(context.Context, int64) error) error {
	queryCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	rows, err := db.QueryContext(queryCtx, `SELECT id FROM consumption_jobs
		WHERE consumer=? AND status='pending' AND next_attempt_on<=?
		ORDER BY next_attempt_on,id LIMIT ?`, consumer, time.Now().Unix(), batchSize)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := process(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func processJob(ctx context.Context, db *sql.DB, consumer string, id int64, handler SQLHandler) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status, messageID string
	var body []byte
	var cycleAttempts uint32
	var nextAttempt int64
	// This row lock serializes duplicate deliveries and concurrent replay/retry.
	err = tx.QueryRowContext(ctx, `SELECT status,message_id,body,cycle_attempts,next_attempt_on
		FROM consumption_jobs WHERE id=? AND consumer=? FOR UPDATE`, id, consumer).
		Scan(&status, &messageID, &body, &cycleAttempts, &nextAttempt)
	if err != nil {
		return err
	}
	if status != "pending" || nextAttempt > time.Now().Unix() {
		return nil
	}
	item, err := decodeDelivery(consumer, messageID, body)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT effect"); err != nil {
		return err
	}
	handleErr := handler(ctx, tx, item)
	if handleErr == nil {
		_, err = tx.ExecContext(ctx, `UPDATE consumption_jobs SET status='completed', completed_on=?,
			attempts=attempts+1, cycle_attempts=cycle_attempts+1, last_error=NULL WHERE id=?`, time.Now().Unix(), id)
	} else {
		// SQL-only partial effects are undone before retaining the failed attempt.
		// A deadlock/connection loss may roll back the whole tx; in that case the
		// error escapes and MQ or the existing pending job retains recovery work.
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT effect"); err != nil {
			return err
		}
		status = "pending"
		var failedOn any
		if errors.Is(handleErr, ErrPermanent) || cycleAttempts+1 >= maxConsumerAttempts {
			status = "failed"
			failedOn = time.Now().Unix()
		}
		delay := time.Second << min(cycleAttempts, 5)
		_, err = tx.ExecContext(ctx, `UPDATE consumption_jobs SET status=?, failed_on=?,
			attempts=attempts+1, cycle_attempts=cycle_attempts+1, next_attempt_on=?, last_error=? WHERE id=?`,
			status, failedOn, time.Now().Add(delay).Unix(), handleErr.Error(), id)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// DummyHandler has a deliberately measurable SQL effect. The plain INSERT
// makes accidental execution of an already completed event fail visibly.
func DummyHandler(consumer string) SQLHandler {
	return func(ctx context.Context, tx *sql.Tx, item Event) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO dummy_effects
			(consumer,event_id,event_type,payload,created_on) VALUES (?,?,?,?,?)`,
			consumer, item.ID, item.Type, string(item.Payload), item.CreatedOn)
		return err
	}
}
