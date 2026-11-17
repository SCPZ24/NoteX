package control

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	transport "github.com/SCPZ24/NoteX/src/internal/platform/rpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

type directory struct {
	node *Node
	role string
}
type directoryWatch struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Resolver supplies a connection-scoped directory. RPCs use gRPC's local
// round_robin addresses; an etcd disconnect retains the last verified snapshot.
func (n *Node) Resolver(role string) resolver.Builder           { return &directory{n, role} }
func (d *directory) Scheme() string                             { return "notex" }
func (w *directoryWatch) ResolveNow(resolver.ResolveNowOptions) {}
func (w *directoryWatch) Close()                                { w.cancel(); <-w.done }

func (d *directory) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(d.node.ctx)
	w := &directoryWatch{cancel: cancel, done: make(chan struct{})}
	go func() { defer close(w.done); d.run(ctx, cc) }()
	return w, nil
}

func (d *directory) run(ctx context.Context, cc resolver.ClientConn) {
	key := d.node.root + "/services/" + d.role + "/"
	entries := map[string][]byte{}
	var revision int64
	refresh := true
	for ctx.Err() == nil {
		if refresh {
			request, cancel := context.WithTimeout(ctx, timeout)
			response, err := d.node.client.Get(request, key, clientv3.WithPrefix())
			cancel()
			if err != nil {
				cc.ReportError(err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			entries = map[string][]byte{}
			for _, kv := range response.Kvs {
				entries[string(kv.Key)] = kv.Value
			}
			revision = response.Header.Revision + 1
			refresh = false
			d.publish(cc, entries)
		}
		watch := d.node.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithProgressNotify())
		for response := range watch {
			if err := response.Err(); err != nil {
				cc.ReportError(err)
				refresh = response.CompactRevision > 0
				break
			}
			for _, event := range response.Events {
				if event.Type == clientv3.EventTypeDelete {
					delete(entries, string(event.Kv.Key))
				} else {
					entries[string(event.Kv.Key)] = event.Kv.Value
				}
			}
			if len(response.Events) > 0 {
				d.publish(cc, entries)
			}
			if len(response.Events) > 0 {
				revision = response.Events[len(response.Events)-1].Kv.ModRevision + 1
			} else if response.IsProgressNotify() {
				revision = max(revision, response.Header.Revision+1)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (d *directory) publish(cc resolver.ClientConn, entries map[string][]byte) {
	snapshot, _ := d.node.Current()
	unique := map[string]bool{}
	for _, data := range entries {
		var record Registration
		if json.Unmarshal(data, &record) != nil || record.Role != d.role || record.PolicyID != snapshot.Config.PolicyID || record.Shards != snapshot.Config.Shards || transport.ValidateAddress(record.RPC) != nil {
			continue
		}
		unique[record.RPC] = true
	}
	addresses := make([]resolver.Address, 0, len(unique))
	for address := range unique {
		addresses = append(addresses, resolver.Address{Addr: address})
	}
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].Addr < addresses[j].Addr })
	if err := cc.UpdateState(resolver.State{Addresses: addresses}); err != nil {
		slog.Warn("RPC directory update", "role", d.role, "error", err)
	}
}
