package outbox

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrWaiting retains a task for an explicitly resumed processing stage.
// Its original body remains durable and Replay can make it pending again.
var ErrWaiting = errors.New("external task waiting")

// ExternalHandler performs idempotent effects outside a SQL transaction.
// nil means complete; ErrWaiting means deferred; ErrPermanent means failed.
// Other errors schedule bounded retries, including uncertain remote outcomes.
type ExternalHandler func(context.Context, Event) error

const externalLease = 30 * time.Second

// RunExternalConsumer shares MQ receipt, quarantine and ACK handling with the
// SQL consumer, but claims and finishes external work in separate transactions.
func RunExternalConsumer(ctx context.Context, db *sql.DB, url, prefix, name string, handler ExternalHandler) error {
	if (name != "feed" && name != "search") || handler == nil {
		return errors.New("external consumer requires feed/search and a handler")
	}
	return runConsumer(ctx, db, url, prefix, name, func(ctx context.Context, id int64) error {
		return processExternal(ctx, db, prefix+"."+name, id, handler)
	})
}

func processExternal(ctx context.Context, db *sql.DB, consumer string, id int64, handler ExternalHandler) error {
	item, attempt, cycle, err := claimExternal(ctx, db, consumer, id)
	if err != nil || attempt == 0 {
		return err
	}
	effectCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	handleErr := handler(effectCtx, item)
	cancel()
	// Shutdown leaves the committed claim recoverable after its lease expires.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	status := "completed"
	var completedOn, failedOn, lastError any
	var nextAttempt int64
	switch {
	case handleErr == nil:
		completedOn = time.Now().Unix()
	case errors.Is(handleErr, ErrWaiting):
		status, lastError = "waiting", handleErr.Error()
	case errors.Is(handleErr, ErrPermanent) || cycle >= maxConsumerAttempts:
		status, failedOn, lastError = "failed", time.Now().Unix(), handleErr.Error()
	default:
		status, lastError = "pending", handleErr.Error()
		nextAttempt = time.Now().Add(time.Second << min(cycle-1, 5)).Unix()
	}
	finishCtx, finishCancel := context.WithTimeout(ctx, operationTimeout)
	defer finishCancel()
	// attempts is a monotonically increasing claim identity. A replay resets only
	// cycle_attempts, so an expired worker cannot finish a newer worker's claim.
	_, err = db.ExecContext(finishCtx, `UPDATE consumption_jobs SET status=?,completed_on=?,failed_on=?,
  last_error=?,next_attempt_on=? WHERE id=? AND consumer=? AND status='pending' AND attempts=?`,
		status, completedOn, failedOn, lastError, nextAttempt, id, consumer, attempt)
	return err
}

func claimExternal(ctx context.Context, db *sql.DB, consumer string, id int64) (Event, uint64, uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, 0, 0, err
	}
	defer tx.Rollback()
	var status, messageID string
	var body []byte
	var attempt uint64
	var cycle uint32
	var nextAttempt int64
	err = tx.QueryRowContext(ctx, `SELECT status,message_id,body,attempts,cycle_attempts,next_attempt_on
  FROM consumption_jobs WHERE id=? AND consumer=? FOR UPDATE`, id, consumer).
		Scan(&status, &messageID, &body, &attempt, &cycle, &nextAttempt)
	if err != nil {
		return Event{}, 0, 0, err
	}
	if status != "pending" || nextAttempt > time.Now().Unix() {
		return Event{}, 0, 0, nil
	}
	if cycle >= maxConsumerAttempts {
		_, err = tx.ExecContext(ctx, `UPDATE consumption_jobs SET status='failed',failed_on=?,
   last_error='external claim expired after final attempt; effect requires reconciliation' WHERE id=?`, time.Now().Unix(), id)
		if err == nil {
			err = tx.Commit()
		}
		return Event{}, 0, 0, err
	}
	item, err := decodeDelivery(consumer, messageID, body)
	if err != nil {
		return Event{}, 0, 0, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE consumption_jobs SET attempts=attempts+1,cycle_attempts=cycle_attempts+1,
  next_attempt_on=?,last_error='external effect in progress; outcome unconfirmed' WHERE id=?`,
		time.Now().Add(externalLease).Unix(), id)
	if err != nil {
		return Event{}, 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return Event{}, 0, 0, err
	}
	return item, attempt + 1, cycle + 1, nil
}
