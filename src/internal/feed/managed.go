package feed

import (
	"context"
	"errors"
	"sync"

	"github.com/SCPZ24/NoteX/src/internal/control"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
)

// RunManaged reconciles a validated runtime snapshot. Every generation drains
// before lowering SQL/concurrency limits or changing subscription priorities.
// Root/planner and historical mixed-reader chunks retain their stable queues.
func (s *Service) RunManaged(ctx context.Context, url, id string, node *control.Node) error {
	for ctx.Err() == nil {
		snapshot, changed := node.Current()
		cfg := snapshot.Config
		if !cfg.Includes(id) {
			return errors.New("worker is outside the configured instance slots")
		}
		s.shards = cfg.Shards
		s.db.SetMaxOpenConns(cfg.WorkerSQL)
		s.db.SetMaxIdleConns(min(2, cfg.WorkerSQL))
		generation, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- s.runGeneration(generation, url, id, cfg, func() { node.Applied(snapshot.Revision) }) }()
		select {
		case <-ctx.Done():
			cancel()
			return <-done
		case err := <-done:
			cancel()
			return err
		case <-changed:
			cancel()
			if err := <-done; err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) runGeneration(ctx context.Context, url, id string, cfg control.Runtime, applied func()) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	results := make(chan error, cfg.Shards+3)
	// The bound covers all stages in this process, including legacy task recovery.
	slots := make(chan struct{}, cfg.Concurrency)
	bounded := func(handler outbox.ExternalHandler) outbox.ExternalHandler {
		return func(ctx context.Context, event outbox.Event) error {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				return ctx.Err()
			}
			return handler(ctx, event)
		}
	}
	start := func(run func() error) {
		workers.Add(1)
		go func() { defer workers.Done(); results <- run(); cancel() }()
	}
	root := outbox.NewShardedPublisher(url, s.prefix, cfg.Shards)
	defer root.Close()
	planner := outbox.NewShardedPublisher(url, s.prefix, cfg.Shards)
	defer planner.Close()
	start(func() error {
		return outbox.RunExternalConsumer(ctx, s.db, url, s.prefix, "feed", bounded(func(ctx context.Context, event outbox.Event) error { return s.handleRoot(ctx, event, root.Publish) }))
	})
	start(func() error {
		return outbox.RunExternalConsumer(ctx, s.db, url, s.prefix, "feed.plan", bounded(func(ctx context.Context, event outbox.Event) error { return s.plan(ctx, event, planner.Publish) }))
	})
	start(func() error {
		return outbox.RunExternalConsumer(ctx, s.db, url, s.prefix, "feed.chunk", bounded(s.distribute))
	})
	subscribed := make(chan struct{}, cfg.Shards)
	for shard := 0; shard < cfg.Shards; shard++ {
		var once sync.Once
		start(func() error {
			return outbox.RunShard(ctx, s.db, url, s.prefix, shard, cfg.Priority(id, shard), bounded(s.distribute), func() { once.Do(func() { subscribed <- struct{}{} }) })
		})
	}
	workers.Add(1)
	go func() { defer workers.Done(); outbox.RunShardRetries(ctx, s.db, url, s.prefix, cfg.Shards) }()
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < cfg.Shards; i++ {
			select {
			case <-ctx.Done():
				return
			case <-subscribed:
			}
		}
		applied()
	}()
	workers.Wait()
	close(results)
	var errs []error
	for err := range results {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
