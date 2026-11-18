package config

import (
	"errors"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/SCPZ24/NoteX/src/internal/control"
	transport "github.com/SCPZ24/NoteX/src/internal/platform/rpc"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	AdaptiveLikes    bool
	EtcdEndpoints    []string
	InstanceID       string
	Version          string
	Control          *control.Node
	Role             string
	CoreRPCAddr      string
	RebuilderRPCAddr string
	RPCToken         string
	MySQLMaxConns    int
	CoreReadMaxConns int
	HTTPAddr         string
	PprofAddr        string
	MySQLDSN         string
	RabbitMQURL      string
	QueuePrefix      string
	RedisURL         string
	FeedMode         string
	PullAuthors      []int64
}

// Load reads environment variables; it does not automatically load a .env file.
func Load() (Config, error) {
	cfg := Config{
		AdaptiveLikes:    os.Getenv("LIKE_MODE") == "adaptive",
		EtcdEndpoints:    control.Endpoints(os.Getenv("ETCD_ENDPOINTS")),
		InstanceID:       os.Getenv("INSTANCE_ID"),
		Version:          os.Getenv("SERVICE_VERSION"),
		Role:             os.Getenv("NOTEX_ROLE"),
		CoreRPCAddr:      os.Getenv("CORE_RPC_ADDR"),
		RebuilderRPCAddr: os.Getenv("REBUILDER_RPC_ADDR"),
		RPCToken:         os.Getenv("RPC_TOKEN"),
		HTTPAddr:         os.Getenv("HTTP_ADDR"),
		PprofAddr:        os.Getenv("NOTEX_PPROF_ADDR"),
		MySQLDSN:         os.Getenv("MYSQL_DSN"),
		RabbitMQURL:      os.Getenv("RABBITMQ_URL"),
		QueuePrefix:      os.Getenv("RABBITMQ_QUEUE_PREFIX"),
		RedisURL:         os.Getenv("REDIS_URL"),
		FeedMode:         os.Getenv("FEED_MODE"),
	}
	if cfg.Role == "" {
		cfg.Role = "all"
	}
	if mode := os.Getenv("LIKE_MODE"); mode != "" && mode != "sql" && mode != "adaptive" {
		return Config{}, errors.New("LIKE_MODE must be sql or adaptive")
	}
	if cfg.AdaptiveLikes && (cfg.RedisURL == "" || ((cfg.Role == "relay" || cfg.Role == "all" || cfg.Role == "like-worker") && cfg.RabbitMQURL == "")) {
		return Config{}, errors.New("adaptive likes require REDIS_URL and RabbitMQ for background roles")
	}
	if cfg.Role == "like-worker" && (!cfg.AdaptiveLikes || cfg.MySQLDSN == "") {
		return Config{}, errors.New("like-worker requires LIKE_MODE=adaptive and MYSQL_DSN")
	}
	ports := map[string]string{"all": "8080", "core": "8080", "feed-query": "8081", "feed-worker": "8082", "feed-rebuilder": "8083", "relay": "8084"}
	ports["like-worker"] = "8086"
	portDefault, valid := ports[cfg.Role]
	if !valid {
		return Config{}, errors.New("unknown NOTEX_ROLE")
	}
	if cfg.CoreRPCAddr == "" {
		cfg.CoreRPCAddr = "127.0.0.1:9090"
	}
	if cfg.RebuilderRPCAddr == "" {
		cfg.RebuilderRPCAddr = "127.0.0.1:9091"
	}
	if cfg.Role != "all" && cfg.Role != "relay" && cfg.Role != "like-worker" {
		if len(cfg.RPCToken) < 32 {
			return Config{}, errors.New("RPC_TOKEN requires at least 32 characters for split services")
		}
		for _, address := range []string{cfg.CoreRPCAddr, cfg.RebuilderRPCAddr} {
			if err := transport.ValidateAddress(address); err != nil {
				return Config{}, err
			}
		}
	}
	cfg.MySQLMaxConns = 10
	switch cfg.Role {
	case "core":
		cfg.MySQLMaxConns = 32
	case "relay":
		cfg.MySQLMaxConns = 2
	case "feed-worker":
		cfg.MySQLMaxConns = 4
	}
	if value := os.Getenv("MYSQL_MAX_OPEN_CONNS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 100 {
			return Config{}, errors.New("MYSQL_MAX_OPEN_CONNS must be between 1 and 100")
		}
		cfg.MySQLMaxConns = n
	}
	if cfg.Role == "core" {
		cfg.CoreReadMaxConns = 4
		if value := os.Getenv("CORE_READ_MAX_OPEN_CONNS"); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 100 {
				return Config{}, errors.New("CORE_READ_MAX_OPEN_CONNS must be between 1 and 100")
			}
			cfg.CoreReadMaxConns = n
		}
	}
	if cfg.FeedMode == "" {
		cfg.FeedMode = "sql"
	}
	if cfg.FeedMode != "sql" && cfg.FeedMode != "push" && cfg.FeedMode != "hybrid" {
		return Config{}, errors.New("FEED_MODE must be sql, push, or hybrid")
	}
	if cfg.Role != "relay" && cfg.FeedMode != "sql" && cfg.RedisURL == "" {
		return Config{}, errors.New("push/hybrid Feed requires REDIS_URL")
	}
	if value := strings.TrimSpace(os.Getenv("FEED_PULL_AUTHORS")); value != "" {
		for _, raw := range strings.Split(value, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil || id <= 0 {
				return Config{}, errors.New("FEED_PULL_AUTHORS requires positive integer IDs")
			}
			cfg.PullAuthors = append(cfg.PullAuthors, id)
		}
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:" + portDefault
	}
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return Config{}, errors.New("HTTP_ADDR must have the form host:port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return Config{}, errors.New("HTTP_ADDR port must be between 1 and 65535")
	}
	if cfg.MySQLDSN == "" && (cfg.Role == "all" || cfg.Role == "core" || cfg.Role == "feed-worker" || cfg.Role == "relay") {
		return Config{}, errors.New("MYSQL_DSN is required")
	}
	if cfg.QueuePrefix == "" {
		cfg.QueuePrefix = "notex"
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`).MatchString(cfg.QueuePrefix) {
		return Config{}, errors.New("RABBITMQ_QUEUE_PREFIX must contain 1-64 letters, digits, underscores or hyphens")
	}
	if cfg.RabbitMQURL != "" {
		if _, err := amqp.ParseURI(cfg.RabbitMQURL); err != nil {
			return Config{}, errors.New("RABBITMQ_URL is invalid")
		}
	}
	if cfg.Role == "all" && cfg.RedisURL != "" && cfg.RabbitMQURL == "" {
		return Config{}, errors.New("REDIS_URL requires RABBITMQ_URL for the Feed consumer")
	}
	if cfg.Role == "feed-worker" || cfg.Role == "feed-rebuilder" {
		if cfg.FeedMode == "sql" {
			return Config{}, errors.New("Feed workers and rebuilders require push or hybrid mode")
		}
	}
	if (cfg.Role == "feed-worker" || cfg.Role == "relay") && cfg.RabbitMQURL == "" {
		return Config{}, errors.New("this role requires RABBITMQ_URL")
	}
	if len(cfg.EtcdEndpoints) > 0 {
		if cfg.Role == "all" {
			return Config{}, errors.New("etcd governance requires a split process role")
		}
		if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`).MatchString(cfg.InstanceID) {
			return Config{}, errors.New("INSTANCE_ID is required for etcd governance")
		}
		if cfg.Version == "" {
			cfg.Version = "dev"
		}
	}
	return cfg, nil
}
