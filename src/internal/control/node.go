package control

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const timeout = 5 * time.Second
const leaseTTL = 15

type Snapshot struct {
	Config   Runtime
	Revision int64
}

type Registration struct {
	ID              string `json:"id"`
	Role            string `json:"role"`
	RPC             string `json:"rpc,omitempty"`
	HTTP            string `json:"http"`
	Version         string `json:"version"`
	Session         string `json:"session"`
	PolicyID        string `json:"policy_id"`
	Shards          int    `json:"shards"`
	AppliedRevision int64  `json:"applied_revision"`
}

type Node struct {
	client       *clientv3.Client
	root         string
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	snapshot     Snapshot
	changed      chan struct{}
	applied      int64
	lastError    string
	registration Registration
	lease        clientv3.LeaseID
	registered   bool
}

// Connect uses the project's local-only deployment boundary, with bounded RPCs.
func Connect(endpoints []string) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("ETCD_ENDPOINTS is required")
	}
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() || u.Port() == "" || u.User != nil || u.Path != "" {
			return nil, errors.New("etcd endpoints must be http://literal-loopback:port")
		}
	}
	return clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: timeout, MaxCallRecvMsgSize: 1 << 20})
}

func Root(prefix string) string { return "/notex/" + prefix }

// Open requires a validated initial snapshot. A running node retains the last
// good snapshot during disconnects, deletion or malformed configuration updates.
func Open(ctx context.Context, endpoints []string, prefix string) (*Node, error) {
	if !identifier.MatchString(prefix) {
		return nil, errors.New("invalid control namespace")
	}
	client, err := Connect(endpoints)
	if err != nil {
		return nil, err
	}
	startup, cancel := context.WithTimeout(ctx, timeout)
	response, err := client.Get(startup, Root(prefix)+"/runtime")
	cancel()
	if err != nil {
		client.Close()
		return nil, err
	}
	if len(response.Kvs) != 1 {
		client.Close()
		return nil, errors.New("initialize etcd runtime configuration before starting services")
	}
	cfg, err := Parse(response.Kvs[0].Value)
	if err != nil || cfg.QueuePrefix != prefix {
		client.Close()
		return nil, fmt.Errorf("invalid initial runtime configuration: %v", err)
	}
	lifetime, stop := context.WithCancel(context.WithoutCancel(ctx))
	n := &Node{client: client, root: Root(prefix), ctx: lifetime, cancel: stop, changed: make(chan struct{}), snapshot: Snapshot{cfg, response.Kvs[0].ModRevision}}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.watch(response.Header.Revision + 1) }()
	return n, nil
}

// Current returns an independent snapshot and a broadcast channel closed when a
// newer valid configuration replaces it.
func (n *Node) Current() (Snapshot, <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	snapshot := n.snapshot
	snapshot.Config.Workers = slices.Clone(snapshot.Config.Workers)
	snapshot.Config.Preferred = slices.Clone(snapshot.Config.Preferred)
	return snapshot, n.changed
}

func (n *Node) Applied(revision int64) { n.mu.Lock(); n.applied = revision; n.mu.Unlock() }

func (n *Node) Status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	return map[string]any{"config_revision": n.snapshot.Revision, "applied_revision": n.applied, "registered": n.registered, "last_control_error": n.lastError, "instance_id": n.registration.ID}
}

func (n *Node) recordError(err error) {
	n.mu.Lock()
	n.lastError = err.Error()
	n.mu.Unlock()
	slog.Warn("control plane update", "error", err)
}

func (n *Node) accept(data []byte, revision int64) {
	cfg, err := Parse(data)
	if err != nil {
		n.recordError(err)
		return
	}
	n.mu.Lock()
	err = n.snapshot.Config.Compatible(cfg)
	if err == nil && revision > n.snapshot.Revision {
		n.snapshot = Snapshot{cfg, revision}
		if n.registration.Role != "" && n.registration.Role != "feed-worker" {
			n.applied = revision
		}
		n.lastError = ""
		close(n.changed)
		n.changed = make(chan struct{})
	}
	n.mu.Unlock()
	if err != nil {
		n.recordError(err)
	}
}

func (n *Node) watch(revision int64) {
	key := n.root + "/runtime"
	for n.ctx.Err() == nil {
		watch := n.client.Watch(n.ctx, key, clientv3.WithRev(revision), clientv3.WithProgressNotify())
		resnapshot := false
		for response := range watch {
			if err := response.Err(); err != nil {
				n.recordError(err)
				resnapshot = response.CompactRevision > 0
				break
			}
			for _, event := range response.Events {
				if event.Type == clientv3.EventTypePut {
					n.accept(event.Kv.Value, event.Kv.ModRevision)
				} else {
					n.recordError(errors.New("runtime configuration deleted; retaining last valid snapshot"))
				}
			}
			if len(response.Events) > 0 {
				revision = response.Events[len(response.Events)-1].Kv.ModRevision + 1
			} else if response.IsProgressNotify() {
				revision = max(revision, response.Header.Revision+1)
			}
		}
		if n.ctx.Err() != nil {
			return
		}
		if resnapshot {
			for n.ctx.Err() == nil {
				ctx, cancel := context.WithTimeout(n.ctx, timeout)
				response, err := n.client.Get(ctx, key)
				cancel()
				if err == nil {
					if len(response.Kvs) == 1 {
						n.accept(response.Kvs[0].Value, response.Kvs[0].ModRevision)
					}
					revision = response.Header.Revision + 1
					break
				}
				n.recordError(err)
				if !n.pause() {
					return
				}
			}
		}
		if !n.pause() {
			return
		}
	}
}

func (n *Node) pause() bool {
	select {
	case <-n.ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}

// Register publishes only after listeners and role dependencies are initialized.
// The same session renews its lease and re-registers after a lease loss. A
// different live session using the same identity is rejected by an etcd CAS.
func (n *Node) Register(ctx context.Context, record Registration) error {
	if !identifier.MatchString(record.ID) || !identifier.MatchString(record.Role) {
		return errors.New("invalid instance identity")
	}
	n.mu.Lock()
	record.Session = rand.Text()
	record.PolicyID = n.snapshot.Config.PolicyID
	record.Shards = n.snapshot.Config.Shards
	n.registration = record
	n.mu.Unlock()
	keep, err := n.register(ctx)
	if err != nil {
		return err
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.keepRegistered(keep) }()
	return nil
}

func (n *Node) register(ctx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	lease, err := n.client.Grant(ctx, leaseTTL)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	record := n.registration
	record.AppliedRevision = n.applied
	oldLease := n.lease
	n.mu.Unlock()
	data, _ := json.Marshal(record)
	key := n.root + "/services/" + record.Role + "/" + record.ID
	response, err := n.client.Get(ctx, key)
	if err == nil && len(response.Kvs) > 0 {
		var old Registration
		if json.Unmarshal(response.Kvs[0].Value, &old) != nil || old.Session != record.Session {
			err = errors.New("instance ID already registered by a different session")
		}
	}
	compare := clientv3.Compare(clientv3.Version(key), "=", 0)
	if err == nil && len(response.Kvs) > 0 {
		compare = clientv3.Compare(clientv3.LeaseValue(key), "=", int64(oldLease))
	}
	if err == nil {
		var txn *clientv3.TxnResponse
		txn, err = n.client.Txn(ctx).If(compare).Then(clientv3.OpPut(key, string(data), clientv3.WithLease(lease.ID))).Commit()
		if err == nil && !txn.Succeeded {
			err = errors.New("instance registration conflict")
		}
	}
	if err != nil {
		n.client.Revoke(ctx, lease.ID)
		return nil, err
	}
	keep, err := n.client.KeepAlive(n.ctx, lease.ID)
	if err != nil {
		n.client.Revoke(ctx, lease.ID)
		return nil, err
	}
	n.mu.Lock()
	n.lease = lease.ID
	n.registered = true
	n.mu.Unlock()
	return keep, nil
}

func (n *Node) keepRegistered(keep <-chan *clientv3.LeaseKeepAliveResponse) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case response, ok := <-keep:
			if ok && response != nil {
				continue
			}
			n.mu.Lock()
			n.registered = false
			n.mu.Unlock()
			for n.ctx.Err() == nil {
				var err error
				keep, err = n.register(n.ctx)
				if err == nil {
					break
				}
				n.recordError(err)
				if !n.pause() {
					return
				}
			}
		case <-ticker.C:
			n.mu.Lock()
			record := n.registration
			record.AppliedRevision = n.applied
			lease := n.lease
			n.mu.Unlock()
			data, _ := json.Marshal(record)
			key := n.root + "/services/" + record.Role + "/" + record.ID
			ctx, cancel := context.WithTimeout(n.ctx, timeout)
			response, err := n.client.Txn(ctx).If(clientv3.Compare(clientv3.LeaseValue(key), "=", int64(lease))).Then(clientv3.OpPut(key, string(data), clientv3.WithLease(lease))).Commit()
			cancel()
			if err != nil {
				n.recordError(err)
			} else if !response.Succeeded {
				n.recordError(errors.New("registration lease superseded"))
			}
		}
	}
}

// Close runs after the role stops admission and drains, then revokes registration.
func (n *Node) Close() {
	n.cancel()
	n.wg.Wait()
	n.mu.Lock()
	lease := n.lease
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if lease != 0 {
		n.client.Revoke(ctx, lease)
	}
	n.client.Close()
}

func Endpoints(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
