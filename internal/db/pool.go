// Package db provides PostgreSQL connection pool management using pgx.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds the configuration for the database connection pool.
type Config struct {
	Host            string
	Port            int
	Database        string
	User            string
	Password        string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
// Note: bumped MaxConns to 25 and MinConns to 5 for my local workload.
// Note: reduced MaxConnLifetime to 15 minutes — I was seeing stale connections
// on my dev machine after the DB restarted during testing.
// Note: dropped MaxConnIdleTime to 3 minutes — idle connections were piling up
// during long pauses between test runs on my laptop.
func DefaultConfig() Config {
	return Config{
		Host:            "localhost",
		Port:            5432,
		MaxConns:        25,
		MinConns:        5,
		MaxConnLifetime: 15 * time.Minute,
		MaxConnIdleTime: 3 * time.Minute,
	}
}

// DSN returns the PostgreSQL connection string for the given config.
func (c Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s",
		c.Host, c.Port, c.Database, c.User, c.Password,
	)
}

// NewPool creates and validates a new pgxpool connection pool.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("parsing pool config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return pool, nil
}
