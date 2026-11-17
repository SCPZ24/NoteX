package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/config"
	"github.com/SCPZ24/NoteX/src/internal/control"
	"github.com/SCPZ24/NoteX/src/internal/core"
	"github.com/gin-gonic/gin"
)

func openControl(ctx context.Context, cfg *config.Config) error {
	node, err := control.Open(ctx, cfg.EtcdEndpoints, cfg.QueuePrefix)
	if err != nil {
		return err
	}
	snapshot, _ := node.Current()
	if snapshot.Config.PolicyID != core.IndexPolicy(cfg.FeedMode, cfg.PullAuthors) {
		node.Close()
		return core.ErrPolicyMismatch
	}
	if cfg.Role == "feed-worker" {
		if !snapshot.Config.Includes(cfg.InstanceID) {
			node.Close()
			return errors.New("INSTANCE_ID must occupy a configured worker slot")
		}
		cfg.MySQLMaxConns = snapshot.Config.WorkerSQL
	}
	cfg.Control = node
	return nil
}

// A MySQL connection-scoped lock prevents two live processes occupying the same
// fixed worker slot, including during an etcd partition. This connection is part
// of the configured SQL pool. Lock loss stops the worker's admission and drain.
func workerSlot(ctx context.Context, db *sql.DB, cfg config.Config) (func(), <-chan error, error) {
	request, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := db.Conn(request)
	if err != nil {
		return nil, nil, err
	}
	name := fmt.Sprintf("%x", sha256.Sum256([]byte(cfg.QueuePrefix+"/"+cfg.InstanceID)))
	var acquired int
	if err = conn.QueryRowContext(request, "SELECT GET_LOCK(?,0)", name).Scan(&acquired); err != nil || acquired != 1 {
		conn.Close()
		return nil, nil, fmt.Errorf("worker slot is occupied or unavailable: %v", err)
	}
	snapshot, _ := cfg.Control.Current()
	_, err = db.ExecContext(request, `INSERT INTO feed_topologies(queue_prefix,shard_count,policy_id) VALUES(?,?,?) ON DUPLICATE KEY UPDATE queue_prefix=queue_prefix`, cfg.QueuePrefix, snapshot.Config.Shards, snapshot.Config.PolicyID)
	var shards int
	var policy string
	if err == nil {
		err = db.QueryRowContext(request, "SELECT shard_count,policy_id FROM feed_topologies WHERE queue_prefix=?", cfg.QueuePrefix).Scan(&shards, &policy)
	}
	if err == nil && (shards != snapshot.Config.Shards || policy != snapshot.Config.PolicyID) {
		err = errors.New("durable Feed topology conflicts with etcd configuration")
	}
	if err != nil {
		conn.ExecContext(request, "SELECT RELEASE_LOCK(?)", name)
		conn.Close()
		return nil, nil, err
	}
	monitor, stop := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	failures := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-monitor.Done():
				return
			case <-ticker.C:
				check, cancel := context.WithTimeout(monitor, 3*time.Second)
				var owns bool
				err := conn.QueryRowContext(check, "SELECT IS_USED_LOCK(?)=CONNECTION_ID()", name).Scan(&owns)
				cancel()
				if err != nil || !owns {
					if monitor.Err() == nil {
						failures <- errors.New("worker slot connection lost")
					}
					return
				}
			}
		}
	}()
	closeSlot := func() {
		stop()
		<-done
		release, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn.ExecContext(release, "SELECT RELEASE_LOCK(?)", name)
		conn.Close()
	}
	return closeSlot, failures, nil
}

func registerControlStatus(router *gin.Engine, cfg config.Config) {
	if cfg.Control == nil {
		return
	}
	router.GET("/controlz", func(c *gin.Context) { c.JSON(http.StatusOK, cfg.Control.Status()) })
}
