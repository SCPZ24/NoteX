// Command events inspects/replays durable work and explicitly runs SQL dummy consumers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/SCPZ24/NoteX/src/internal/config"
	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/platform/mysql"
)

func main() {
	if err := run(); err != nil {
		slog.Error("events command failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: events list|replay|dummy [flags]")
	}
	command := os.Args[1]
	if command != "list" && command != "replay" && command != "dummy" {
		return errors.New("command must be list, replay, or dummy")
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	scope := flags.String("scope", "consumer", "consumer or outbox")
	status := flags.String("status", "", "filter pending, waiting, failed, or completed records")
	id := flags.Int64("id", 0, "record ID (required for replay)")
	after := flags.Int64("after", 0, "list records after this ID")
	limit := flags.Int("limit", 20, "list at most 1-100 records")
	reason := flags.String("reason", "", "reason for explicit replay")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if command == "dummy" && (cfg.RabbitMQURL == "" || !strings.HasPrefix(cfg.QueuePrefix, "notex_dummy_")) {
		return errors.New("dummy requires RABBITMQ_URL and an isolated RABBITMQ_QUEUE_PREFIX starting with notex_dummy_")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := mysql.Open(ctx, cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	switch command {
	case "list":
		records, err := outbox.Inspect(ctx, db, *scope, *status, *id, *after, *limit)
		if err != nil {
			return err
		}
		history := []outbox.ReplayRecord{}
		if *id > 0 {
			history, err = outbox.ReplayHistory(ctx, db, *scope, *id)
			if err != nil {
				return err
			}
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(struct {
			Records []outbox.Record       `json:"records"`
			Replays []outbox.ReplayRecord `json:"replays"`
		}{records, history})
	case "replay":
		if err := outbox.Replay(ctx, db, *scope, *id, *reason); err != nil {
			return err
		}
		fmt.Printf("scheduled %s record %d for replay\n", *scope, *id)
	case "dummy":
		var workers sync.WaitGroup
		for _, name := range []string{"feed", "search"} {
			workers.Go(func() {
				consumer := cfg.QueuePrefix + "." + name
				slog.Info("dummy consumer started", "consumer", consumer)
				if err := outbox.RunConsumer(ctx, db, cfg.RabbitMQURL, cfg.QueuePrefix, name, outbox.DummyHandler(consumer)); err != nil {
					slog.Error("dummy consumer stopped", "error", err)
					stop()
				}
			})
		}
		workers.Wait()
	}
	return nil
}
