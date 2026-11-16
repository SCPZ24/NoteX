package config

import (
	"errors"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	HTTPAddr    string
	MySQLDSN    string
	RabbitMQURL string
	QueuePrefix string
	RedisURL    string
	FeedMode    string
	PullAuthors []int64
}

// Load reads environment variables; it does not automatically load a .env file.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    os.Getenv("HTTP_ADDR"),
		MySQLDSN:    os.Getenv("MYSQL_DSN"),
		RabbitMQURL: os.Getenv("RABBITMQ_URL"),
		QueuePrefix: os.Getenv("RABBITMQ_QUEUE_PREFIX"),
		RedisURL:    os.Getenv("REDIS_URL"),
		FeedMode:    os.Getenv("FEED_MODE"),
	}
	if cfg.FeedMode == "" {
		cfg.FeedMode = "sql"
	}
	if cfg.FeedMode != "sql" && cfg.FeedMode != "push" && cfg.FeedMode != "hybrid" {
		return Config{}, errors.New("FEED_MODE must be sql, push, or hybrid")
	}
	if cfg.FeedMode != "sql" && cfg.RedisURL == "" {
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
		cfg.HTTPAddr = "127.0.0.1:8080"
	}
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return Config{}, errors.New("HTTP_ADDR must have the form host:port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return Config{}, errors.New("HTTP_ADDR port must be between 1 and 65535")
	}
	if cfg.MySQLDSN == "" {
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
	if cfg.RedisURL != "" && cfg.RabbitMQURL == "" {
		return Config{}, errors.New("REDIS_URL requires RABBITMQ_URL for the Feed consumer")
	}
	return cfg, nil
}
