package config

import (
	"errors"
	"net"
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr string
	MySQLDSN string
}

// Load reads environment variables; it does not automatically load a .env file.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr: os.Getenv("HTTP_ADDR"),
		MySQLDSN: os.Getenv("MYSQL_DSN"),
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
	return cfg, nil
}
