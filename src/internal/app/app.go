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
	"github.com/SCPZ24/NoteX/src/internal/config"
	"github.com/SCPZ24/NoteX/src/internal/feed"
	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/httpapi"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/platform/mysql"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

func Run(ctx context.Context, cfg config.Config) error {
	db, err := mysql.Open(ctx, cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	router := httpapi.NewRouter(db.PingContext)
	wake := make(chan struct{}, 1)
	posts := post.New(db, wake)
	var reader httpapi.FeedReader = posts
	if cfg.RabbitMQURL != "" {
		workerCtx, cancelWorker := context.WithCancel(ctx)
		workerDone := make(chan struct{})
		go func() {
			defer close(workerDone)
			outbox.Run(workerCtx, db, wake, cfg.RabbitMQURL, cfg.QueuePrefix)
		}()
		// Stop the worker before closing the shared database pool.
		defer func() {
			cancelWorker()
			<-workerDone
		}()
	}
	if cfg.FeedMode != "sql" && cfg.RedisURL != "" {
		projection, err := feed.New(db, cfg.RedisURL, cfg.QueuePrefix, cfg.FeedMode, cfg.PullAuthors)
		if err != nil {
			listener.Close()
			return err
		}
		defer projection.Close()
		reader = projection
		posts.UseCache(projection.Cache())
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

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	slog.Info("HTTP server listening", "address", listener.Addr().String())

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		// Use a fresh context: ctx has already been canceled by the signal.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			server.Close()
			return fmt.Errorf("shutdown HTTP: %w", err)
		}
		slog.Info("HTTP server stopped")
		return nil
	}
}
