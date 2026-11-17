package control

import (
	"encoding/json"
	"strings"
	"testing"
)

func validRuntime() Runtime {
	return Runtime{QueuePrefix: "test", PolicyID: strings.Repeat("a", 64), Shards: 4, MaxWorkers: 3, MaxWorkerSQL: 4, TotalWorkerSQL: 12, MaxConcurrency: 2, Workers: []string{"a", "b", "c"}, Preferred: []string{"a", "b", "a", "b"}, WorkerSQL: 4, Concurrency: 2}
}

func TestRuntimeRejectsTopologyChangesAndOverBudget(t *testing.T) {
	initial := validRuntime()
	if err := initial.Validate(); err != nil {
		t.Fatal(err)
	}
	next := validRuntime()
	next.Preferred[2] = "c"
	next.Concurrency = 1
	next.WorkerSQL = 2
	if err := initial.Compatible(next); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Runtime){
		func(c *Runtime) { c.Shards = 5 },
		func(c *Runtime) { c.Workers[2] = "d" },
		func(c *Runtime) { c.TotalWorkerSQL = 11 },
		func(c *Runtime) { c.Concurrency = 3 },
		func(c *Runtime) { c.Preferred[0] = "unknown" },
		func(c *Runtime) { c.Workers[1] = "a" },
	} {
		next = validRuntime()
		change(&next)
		if initial.Compatible(next) == nil {
			t.Fatalf("accepted invalid transition: %+v", next)
		}
	}
}

func TestLastGoodSnapshotSurvivesInvalidUpdate(t *testing.T) {
	node := &Node{snapshot: Snapshot{validRuntime(), 7}, changed: make(chan struct{})}
	_, changed := node.Current()
	node.accept([]byte(`{"unknown":1}`), 8)
	snapshot, _ := node.Current()
	if snapshot.Revision != 7 {
		t.Fatal("invalid update replaced snapshot")
	}
	select {
	case <-changed:
		t.Fatal("invalid update notified consumers")
	default:
	}
	next := validRuntime()
	next.Preferred[2] = "c"
	data, _ := json.Marshal(next)
	node.accept(data, 9)
	snapshot, _ = node.Current()
	if snapshot.Revision != 9 || snapshot.Config.Preferred[2] != "c" {
		t.Fatal("valid update missing")
	}
	select {
	case <-changed:
	default:
		t.Fatal("valid update did not notify consumers")
	}
	node.accept(data, 6)
	snapshot, _ = node.Current()
	if snapshot.Revision != 9 {
		t.Fatal("stale revision applied")
	}
}

func TestParseRejectsTrailingDocument(t *testing.T) {
	data, _ := json.Marshal(validRuntime())
	if _, err := Parse(append(data, []byte(` {}`)...)); err == nil {
		t.Fatal("accepted extra document")
	}
}
