package app

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/config"
	"github.com/SCPZ24/NoteX/src/internal/httpapi"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/platform/mysql"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

func startLikes(ctx context.Context, db *sql.DB, store *cache.Store, cfg config.Config, relay, worker bool) func() {
	task, cancel := context.WithCancel(ctx)
	var group sync.WaitGroup
	if relay {
		group.Go(func() { outbox.RunLikeRelay(task, store.Redis, cfg.RabbitMQURL, cfg.QueuePrefix) })
		group.Go(func() {
			service := post.New(db, nil)
			service.UseCache(store)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-task.Done():
					return
				case <-ticker.C:
					if err := service.CoolLikes(task); err != nil && task.Err() == nil {
						slog.Warn("like cooldown retrying", "error", err)
					}
				}
			}
		})
	}
	if worker {
		group.Go(func() { outbox.RunLikeWorker(task, db, store.Redis, cfg.RabbitMQURL, cfg.QueuePrefix) })
	}
	return func() { cancel(); group.Wait() }
}

func runLikeWorker(ctx context.Context, cfg config.Config) error {
	db, err := mysql.OpenWithLimit(ctx, cfg.MySQLDSN, cfg.MySQLMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	store, err := cache.Open(cfg.RedisURL, cfg.QueuePrefix)
	if err != nil {
		return err
	}
	defer store.Redis.Close()
	defer store.Close()
	defer startLikes(ctx, db, store, cfg, false, true)()
	return serveRole(ctx, cfg, httpapi.NewRouter(db.PingContext), nil)
}
