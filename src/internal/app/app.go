package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/config"
	"github.com/SCPZ24/NoteX/src/internal/httpapi"
	"github.com/SCPZ24/NoteX/src/internal/platform/mysql"
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
	server := &http.Server{
		Handler:           httpapi.NewRouter(db.PingContext),
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
