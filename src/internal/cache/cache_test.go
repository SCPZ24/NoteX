package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedWorkSurvivesWaiterCancellation(t *testing.T) {
	s := New(nil, "test")
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	fn := func(ctx context.Context) error {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	first := make(chan error, 1)
	go func() { first <- s.Do(ctx, "key", fn) }()
	<-started
	second := make(chan error, 1)
	go func() { second <- s.Do(context.Background(), "key", fn) }()
	deadline := time.Now().Add(time.Second)
	for s.Stats()["shared_wait"] == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Stats()["shared_wait"] != 1 {
		t.Fatal("second request did not join")
	}
	cancel()
	if !errors.Is(<-first, context.Canceled) {
		t.Fatal("waiter did not cancel")
	}
	close(release)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("queries=%d", calls.Load())
	}
}

func TestAdmissionAndShutdown(t *testing.T) {
	s := New(nil, "test")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Do(context.Background(), string(rune(i)), func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		}(i)
	}
	deadline := time.Now().Add(time.Second)
	for s.Stats()["build_started"] < 64 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Stats()["build_started"] != 64 {
		t.Fatal("tasks not admitted")
	}
	if err := s.Do(context.Background(), "overflow", func(context.Context) error { t.Error("executed rejected task"); return nil }); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	s.Close()
	wg.Wait()
}

func TestSQLBoundAndQueueDeadline(t *testing.T) {
	s := New(nil, "test")
	defer s.Close()
	release := make(chan struct{})
	var active, max atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.SQL(context.Background(), func(context.Context) error {
				n := active.Add(1)
				for old := max.Load(); n > old && !max.CompareAndSwap(old, n); old = max.Load() {
				}
				<-release
				active.Add(-1)
				return nil
			})
		}()
	}
	deadline := time.Now().Add(time.Second)
	for active.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	err := s.SQL(context.Background(), func(context.Context) error { t.Error("over capacity"); return nil })
	if !errors.Is(err, ErrBusy) || time.Since(start) > time.Second {
		t.Fatalf("queue err=%v elapsed=%v", err, time.Since(start))
	}
	close(release)
	wg.Wait()
	if max.Load() != 4 {
		t.Fatal(max.Load())
	}
}
