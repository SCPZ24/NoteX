// Command control publishes explicitly versioned deployment configuration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/control"
	"github.com/SCPZ24/NoteX/src/internal/core"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: control init|status|apply|assign [flags]")
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	prefix := flags.String("prefix", os.Getenv("RABBITMQ_QUEUE_PREFIX"), "deployment namespace")
	file := flags.String("file", "", "runtime JSON for apply")
	expected := flags.Int64("expect", 0, "required current config revision for apply/assign")
	workers := flags.String("workers", "worker-a,worker-b,worker-c", "fixed instance slots at initialization")
	active := flags.String("active", "worker-a,worker-b", "preferred instances for initial placement or assign")
	shards := flags.Int("shards", 4, "fixed shard count at initialization")
	sql := flags.Int("sql", 4, "per-worker SQL ceiling including slot guard connection")
	concurrency := flags.Int("concurrency", 2, "per-worker execution limit at initialization")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *prefix == "" {
		*prefix = "notex"
	}
	if command != "init" && command != "status" && command != "apply" && command != "assign" {
		return errors.New("unknown command")
	}
	client, err := control.Connect(control.Endpoints(os.Getenv("ETCD_ENDPOINTS")))
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := control.Root(*prefix)
	key := root + "/runtime"
	if command == "status" {
		response, err := client.Get(ctx, root+"/", clientv3.WithPrefix())
		if err != nil {
			return err
		}
		type entry struct {
			Key      string          `json:"key"`
			Revision int64           `json:"revision"`
			Value    json.RawMessage `json:"value"`
		}
		var entries []entry
		for _, kv := range response.Kvs {
			value := json.RawMessage(kv.Value)
			if !json.Valid(value) {
				value, _ = json.Marshal(string(value))
			}
			entries = append(entries, entry{string(kv.Key), kv.ModRevision, value})
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(entries)
	}
	var next control.Runtime
	compare := clientv3.Compare(clientv3.Version(key), "=", 0)
	if command == "init" {
		mode := os.Getenv("FEED_MODE")
		if mode != "push" && mode != "hybrid" {
			return errors.New("set FEED_MODE=push or hybrid before initialization")
		}
		var authors []int64
		for _, raw := range split(os.Getenv("FEED_PULL_AUTHORS")) {
			id, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || id <= 0 {
				return errors.New("invalid FEED_PULL_AUTHORS")
			}
			authors = append(authors, id)
		}
		slots := split(*workers)
		next = control.Runtime{QueuePrefix: *prefix, PolicyID: core.IndexPolicy(mode, authors), Shards: *shards, MaxWorkers: len(slots), MaxWorkerSQL: *sql, TotalWorkerSQL: len(slots) * (*sql), MaxConcurrency: *concurrency, Workers: slots, WorkerSQL: *sql, Concurrency: *concurrency}
		if *shards < 1 || *shards > 32 {
			return errors.New("shards must be 1-32")
		}
		if err = assign(&next, split(*active)); err != nil {
			return err
		}
	} else {
		if *expected <= 0 {
			return errors.New("supply -expect with the current runtime revision from control status")
		}
		response, err := client.Get(ctx, key)
		if err != nil {
			return err
		}
		if len(response.Kvs) != 1 || response.Kvs[0].ModRevision != *expected {
			return errors.New("configuration changed or missing; inspect status before retry")
		}
		current, err := control.Parse(response.Kvs[0].Value)
		if err != nil {
			return err
		}
		next = current
		if command == "apply" {
			data, err := os.ReadFile(*file)
			if err != nil {
				return err
			}
			next, err = control.Parse(data)
			if err != nil {
				return err
			}
		} else if err = assign(&next, split(*active)); err != nil {
			return err
		}
		if err = current.Compatible(next); err != nil {
			return err
		}
		// Preference changes require a registered candidate that has initialized
		// subscriptions for the current revision. A lease never substitutes for
		// actual RabbitMQ ownership; /controlz and broker state remain observable.
		for shard, id := range next.Preferred {
			if current.Preferred[shard] == id {
				continue
			}
			registered, err := client.Get(ctx, root+"/services/feed-worker/"+id)
			if err != nil {
				return err
			}
			var record control.Registration
			if len(registered.Kvs) != 1 || json.Unmarshal(registered.Kvs[0].Value, &record) != nil || record.AppliedRevision < *expected {
				return fmt.Errorf("worker %s must register and apply revision %d before reassignment", id, *expected)
			}
		}
		compare = clientv3.Compare(clientv3.ModRevision(key), "=", *expected)
	}
	if err = next.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	response, err := client.Txn(ctx).If(compare).Then(clientv3.OpPut(key, string(data))).Commit()
	if err != nil {
		return err
	}
	if !response.Succeeded {
		return errors.New("configuration CAS failed; inspect current revision")
	}
	fmt.Printf("published runtime revision %d\n%s\n", response.Header.Revision, data)
	return nil
}

func split(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func assign(cfg *control.Runtime, active []string) error {
	if len(active) == 0 {
		return errors.New("at least one preferred instance is required")
	}
	seen := map[string]bool{}
	for _, id := range active {
		if !cfg.Includes(id) || seen[id] {
			return errors.New("active instances must be unique configured slots")
		}
		seen[id] = true
	}
	cfg.Preferred = make([]string, cfg.Shards)
	for shard := range cfg.Preferred {
		cfg.Preferred[shard] = active[shard%len(active)]
	}
	return nil
}
