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
	"github.com/SCPZ24/NoteX/src/internal/core/corepb"
	"github.com/SCPZ24/NoteX/src/internal/feed"
	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/httpapi"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/platform/mysql"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"google.golang.org/grpc"
)

func runCore(ctx context.Context, cfg config.Config) error {
	db, err := mysql.OpenWithLimit(ctx, cfg.MySQLDSN, cfg.MySQLMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	var store *cache.Store
	if cfg.FeedMode != "sql" {
		store, err = cache.Open(cfg.RedisURL, cfg.QueuePrefix)
		if err != nil {
			return err
		}
		defer store.Redis.Close()
	} else {
		store = cache.New(nil, cfg.QueuePrefix)
	}
	defer store.Close()
	posts := post.New(db, nil)
	if store.Redis != nil {
		posts.UseCache(store)
	}
	authority := core.New(db, store)
	authority.ConfigureIndex(cfg.FeedMode, cfg.PullAuthors, cfg.QueuePrefix)
	rpc, err := core.Start(cfg.CoreRPCAddr, cfg.RPCToken, authority)
	if err != nil {
		return err
	}
	defer rpc.Close()
	router := httpapi.NewRouter(db.PingContext)
	httpapi.RegisterCore(router, account.New(db), follow.New(db, nil), posts)
	return serveRole(ctx, cfg, router, rpc.Done)
}

func connectCore(ctx context.Context, cfg config.Config) (*core.Client, *grpc.ClientConn, error) {
	client, conn, err := core.Dial(ctx, cfg.CoreRPCAddr, cfg.RPCToken)
	if err != nil {
		return nil, nil, err
	}
	if err := deploymentReady(ctx, cfg, client.CheckReady); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return client, conn, nil
}

func deploymentReady(ctx context.Context, cfg config.Config, check func(context.Context) (*corepb.Deployment, error)) error {
	deployment, err := check(ctx)
	if err != nil {
		return err
	}
	if deployment.PolicyId != core.IndexPolicy(cfg.FeedMode, cfg.PullAuthors) || deployment.QueuePrefix != cfg.QueuePrefix {
		return core.ErrPolicyMismatch
	}
	return nil
}

func connectRebuilder(ctx context.Context, cfg config.Config) (*feed.RebuildClient, *grpc.ClientConn, error) {
	client, conn, err := feed.DialRebuilder(ctx, cfg.RebuilderRPCAddr, cfg.RPCToken)
	if err != nil {
		return nil, nil, err
	}
	if err := deploymentReady(ctx, cfg, client.CheckReady); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return client, conn, nil
}

func runFeedQuery(ctx context.Context, cfg config.Config) error {
	client, conn, err := connectCore(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	var reader httpapi.FeedReader = client
	if cfg.FeedMode != "sql" {
		rebuilder, rebuildConn, err := connectRebuilder(ctx, cfg)
		if err != nil {
			return err
		}
		defer rebuildConn.Close()
		projection, err := feed.NewRemote(nil, client, cfg.RedisURL, cfg.QueuePrefix, cfg.FeedMode, cfg.PullAuthors)
		if err != nil {
			return err
		}
		defer projection.Close()
		projection.UseRebuilder(rebuilder)
		reader = projection
	}
	// Redis/rebuilder failures can still use Core's authoritative SQL pagination.
	router := httpapi.NewRouter(func(ctx context.Context) error { return deploymentReady(ctx, cfg, client.CheckReady) })
	httpapi.RegisterFeed(router, reader, client)
	return serveRole(ctx, cfg, router, nil)
}

func runRebuilder(ctx context.Context, cfg config.Config) error {
	client, conn, err := connectCore(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	projection, err := feed.NewRemote(nil, client, cfg.RedisURL, cfg.QueuePrefix, cfg.FeedMode, cfg.PullAuthors)
	if err != nil {
		return err
	}
	defer projection.Close()
	rpc, err := feed.StartRebuilder(cfg.RebuilderRPCAddr, cfg.RPCToken, projection)
	if err != nil {
		return err
	}
	defer rpc.Close()
	router := httpapi.NewRouter(func(ctx context.Context) error {
		if err := deploymentReady(ctx, cfg, client.CheckReady); err != nil {
			return err
		}
		return projection.Cache().Redis.Ping(ctx).Err()
	})
	return serveRole(ctx, cfg, router, rpc.Done)
}

func runFeedWorker(ctx context.Context, cfg config.Config) error {
	client, conn, err := connectCore(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	rebuilder, rebuildConn, err := connectRebuilder(ctx, cfg)
	if err != nil {
		return err
	}
	defer rebuildConn.Close()
	db, err := mysql.OpenWithLimit(ctx, cfg.MySQLDSN, cfg.MySQLMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	projection, err := feed.NewRemote(db, client, cfg.RedisURL, cfg.QueuePrefix, cfg.FeedMode, cfg.PullAuthors)
	if err != nil {
		return err
	}
	defer projection.Close()
	projection.UseRebuilder(rebuilder)
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { defer close(done); done <- projection.Run(workerCtx, cfg.RabbitMQURL) }()
	defer func() { cancel(); <-done }()
	router := httpapi.NewRouter(func(ctx context.Context) error {
		if err := db.PingContext(ctx); err != nil {
			return err
		}
		return deploymentReady(ctx, cfg, client.CheckReady)
	})
	return serveRole(ctx, cfg, router, done)
}

func runRelay(ctx context.Context, cfg config.Config) error {
	db, err := mysql.OpenWithLimit(ctx, cfg.MySQLDSN, cfg.MySQLMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		outbox.Run(workerCtx, db, nil, cfg.RabbitMQURL, cfg.QueuePrefix)
		done <- nil
	}()
	defer func() { cancel(); <-done }()
	return serveRole(ctx, cfg, httpapi.NewRouter(db.PingContext), done)
}

func serveRole(ctx context.Context, cfg config.Config, handler http.Handler, failures <-chan error) error {
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	slog.Info("process listening", "role", cfg.Role, "http", listener.Addr().String())
	var failure error
	select {
	case <-ctx.Done():
	case err := <-failures:
		if ctx.Err() == nil {
			failure = fmt.Errorf("%s background service stopped: %v", cfg.Role, err)
		}
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			failure = err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(failure, server.Shutdown(shutdown))
}
