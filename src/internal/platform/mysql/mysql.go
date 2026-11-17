package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// Open creates one shared connection pool for the application.
// The caller owns the pool and must close it after requests have stopped.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	return OpenWithLimit(ctx, dsn, 10)
}

// OpenWithLimit assigns a connection budget to one independently deployed role.
func OpenWithLimit(ctx context.Context, dsn string, maximum int) (*sql.DB, error) {
	if maximum < 1 {
		return nil, errors.New("MySQL connection budget must be positive")
	}
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		// Do not include the DSN or parser error: they can contain credentials.
		return nil, errors.New("MYSQL_DSN is invalid")
	}
	if cfg.DBName == "" {
		return nil, errors.New("MYSQL_DSN must specify a database")
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second

	connector, err := driver.NewConnector(cfg)
	if err != nil {
		return nil, errors.New("cannot configure MySQL connector")
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maximum)
	db.SetMaxIdleConns(min(5, maximum))
	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetConnMaxIdleTime(time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to MySQL: %w", err)
	}
	return db, nil
}
