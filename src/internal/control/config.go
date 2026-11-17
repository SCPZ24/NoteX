// Package control owns the low-frequency etcd directory and configuration.
package control

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// Runtime is one atomic configuration document. Capacity ceilings and topology
// are frozen at bootstrap, so mixed revisions remain within the same envelope.
type Runtime struct {
	QueuePrefix    string   `json:"queue_prefix"`
	PolicyID       string   `json:"policy_id"`
	Shards         int      `json:"shards"`
	MaxWorkers     int      `json:"max_workers"`
	MaxWorkerSQL   int      `json:"max_worker_sql"`
	TotalWorkerSQL int      `json:"total_worker_sql"`
	MaxConcurrency int      `json:"max_concurrency"`
	Workers        []string `json:"workers"`
	Preferred      []string `json:"preferred"`
	WorkerSQL      int      `json:"worker_sql"`
	Concurrency    int      `json:"concurrency"`
}

func Parse(data []byte) (Runtime, error) {
	var cfg Runtime
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, errors.New("configuration must contain one JSON document")
	}
	return cfg, cfg.Validate()
}

func (c Runtime) Validate() error {
	_, digestErr := hex.DecodeString(c.PolicyID)
	if !identifier.MatchString(c.QueuePrefix) || len(c.PolicyID) != 64 || digestErr != nil {
		return errors.New("invalid deployment identity")
	}
	if c.Shards < 1 || c.Shards > 32 || c.MaxWorkers < 1 || c.MaxWorkers > 8 {
		return errors.New("shards must be 1-32 and max_workers 1-8")
	}
	if c.MaxWorkerSQL < 2 || c.MaxWorkerSQL > 32 || c.TotalWorkerSQL < c.MaxWorkers*c.MaxWorkerSQL || c.TotalWorkerSQL > 256 {
		return errors.New("SQL ceiling requires max_workers * max_worker_sql <= total_worker_sql <= 256")
	}
	if c.WorkerSQL < 2 || c.WorkerSQL > c.MaxWorkerSQL || c.MaxConcurrency < 1 || c.MaxConcurrency > 16 || c.Concurrency < 1 || c.Concurrency > c.MaxConcurrency {
		return errors.New("invalid worker SQL/concurrency budget")
	}
	if len(c.Workers) < 1 || len(c.Workers) > c.MaxWorkers || len(c.Preferred) != c.Shards {
		return errors.New("worker roster or preferred shard assignment has wrong size")
	}
	seen := map[string]bool{}
	for _, id := range c.Workers {
		if !identifier.MatchString(id) || seen[id] {
			return errors.New("invalid or duplicate worker ID")
		}
		seen[id] = true
	}
	for _, id := range c.Preferred {
		if !seen[id] {
			return fmt.Errorf("preferred worker %q is outside roster", id)
		}
	}
	return nil
}

func (c Runtime) Compatible(next Runtime) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if c.QueuePrefix != next.QueuePrefix || c.PolicyID != next.PolicyID || c.Shards != next.Shards || c.MaxWorkers != next.MaxWorkers || c.MaxWorkerSQL != next.MaxWorkerSQL || c.TotalWorkerSQL != next.TotalWorkerSQL || c.MaxConcurrency != next.MaxConcurrency || !slices.Equal(c.Workers, next.Workers) {
		return errors.New("topology and capacity ceilings require a separate deployment migration")
	}
	return nil
}

func (c Runtime) Includes(id string) bool { return slices.Contains(c.Workers, id) }

// Priority lets quorum/SAC perform an acknowledged handover to the preferred
// instance. The other configured instances remain eligible standbys.
func (c Runtime) Priority(id string, shard int) int {
	if c.Preferred[shard] == id {
		return 100
	}
	return 10
}
