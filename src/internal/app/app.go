package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/config"
	"github.com/SCPZ24/NoteX/src/internal/core"
	"github.com/SCPZ24/NoteX/src/internal/feed"
	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/httpapi"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/platform/mysql"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

// Run starts exactly the configured process role. The empty role preserves
// programmatic callers of the combined-process baseline.
func Run(ctx context.Context, cfg config.Config) error {
	if len(cfg.EtcdEndpoints) > 0 {
		if err := openControl(ctx, &cfg); err != nil {
			return err
		}
		defer cfg.Control.Close()
	}
	switch cfg.Role {
	case "", "all":
		return runCombined(ctx, cfg)
	case "core":
		return runCore(ctx, cfg)
	case "feed-query":
		return runFeedQuery(ctx, cfg)
	case "feed-worker":
		return runFeedWorker(ctx, cfg)
	case "feed-rebuilder":
		return runRebuilder(ctx, cfg)
	case "relay":
		return runRelay(ctx, cfg)
	default:
		return fmt.Errorf("unknown process role %q", cfg.Role)
	}
}

func runCombined(ctx context.Context, cfg config.Config) error {
	db, err := mysql.Open(ctx, cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	defer listener.Close()
	router := httpapi.NewRouter(db.PingContext)
	wake := make(chan struct{}, 1)
	posts := post.New(db, wake)
	var projection *feed.Service
	var budget *cache.Store
	if cfg.FeedMode != "sql" && cfg.RedisURL != "" {
		projection, err = feed.New(db, cfg.RedisURL, cfg.QueuePrefix, cfg.FeedMode, cfg.PullAuthors)
		if err != nil {
			return err
		}
		defer projection.Close()
		budget = projection.Cache()
		posts.UseCache(budget)
	}
	authority := core.New(db, budget)
	authority.ConfigureIndex(cfg.FeedMode, cfg.PullAuthors, cfg.QueuePrefix)
	rpc, err := core.StartLocal(ctx, authority)
	if err != nil {
		return err
	}
	defer rpc.Close()
	var reader httpapi.FeedReader = rpc.Client
	if projection != nil {
		projection.UseCore(rpc.Client)
		reader = projection
	}
	if cfg.RabbitMQURL != "" {
		workerCtx, cancelWorker := context.WithCancel(ctx)
		workerDone := make(chan struct{})
		go func() {
			defer close(workerDone)
			outbox.Run(workerCtx, db, wake, cfg.RabbitMQURL, cfg.QueuePrefix)
		}()
		defer func() {
			cancelWorker()
			<-workerDone
		}()
	}
	if projection != nil {
		workerCtx, cancelWorker := context.WithCancel(ctx)
		workerDone := make(chan struct{})
		go func() {
			defer close(workerDone)
			if err := projection.Run(workerCtx, cfg.RabbitMQURL); err != nil {
				slog.Error("feed consumer stopped", "error", err)
			}
		}()
		defer func() {
			cancelWorker()
			<-workerDone
		}()
	}
	httpapi.RegisterAPI(router, account.New(db), follow.New(db, wake), posts, reader)
	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	defer server.Close()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	slog.Info("HTTP server listening", "address", listener.Addr().String())

	var failure error
	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case err := <-rpc.Done:
		failure = fmt.Errorf("Core RPC server stopped: %v", err)
	case <-ctx.Done():
	}
	// Drain HTTP before stopping its RPC dependency and the shared SQL pool.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return errors.Join(failure, fmt.Errorf("shutdown HTTP: %w", err))
	}
	slog.Info("HTTP server stopped")
	return failure
}
