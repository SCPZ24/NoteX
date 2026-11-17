package outbox

import (
	"context"
	"encoding/json"
	"errors"
)

// TaskPublisher is owned by one goroutine. It confirms each persistent task
// publication and closes uncertain connections before the caller retries.
type TaskPublisher struct{ broker broker }

func NewTaskPublisher(url, prefix string) *TaskPublisher {
	return &TaskPublisher{broker: broker{url: url, prefix: prefix, extraQueues: []string{"feed.plan", "feed.chunk"}}}
}

// NewShardedPublisher declares the fixed topology before publishing.
func NewShardedPublisher(url, prefix string, count int) *TaskPublisher {
	p := NewTaskPublisher(url, prefix)
	for shard := 0; shard < count; shard++ {
		p.broker.extraQueues = append(p.broker.extraQueues, ShardName(shard))
	}
	return p
}

func (p *TaskPublisher) Close() { p.broker.close() }

func (p *TaskPublisher) Publish(ctx context.Context, event Event) error {
	if event.ID <= 0 || event.CreatedOn <= 0 || (event.Type != "feed.plan" && event.Type != "feed.chunk") {
		return errors.New("invalid Feed task publication")
	}
	queues, err := event.destinations(p.broker.prefix)
	if err != nil {
		return err
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err = p.broker.connect(ctx); err == nil {
		err = p.broker.publish(ctx, queues[0], event.ID, body)
	}
	if err != nil {
		p.broker.close()
	}
	return err
}
