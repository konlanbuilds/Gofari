package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL           string
	APIAddr               string
	WorkerConcurrency     int
	WorkerPollInterval    time.Duration
	WorkerLeaseDuration   time.Duration
	WorkerLeaseHeartbeat  time.Duration
	WorkerShutdownTimeout time.Duration
	WorkerID              string
	DBMaxConns            int
	DBMinConns            int
}

func Load() (Config, error) {
	config := Config{
		DatabaseURL:           os.Getenv("GOFARI_DATABASE_URL"),
		APIAddr:               get("GOFARI_API_ADDR", "127.0.0.1:8080"),
		WorkerConcurrency:     getInt("GOFARI_WORKER_CONCURRENCY", 4),
		WorkerPollInterval:    getDuration("GOFARI_WORKER_POLL_INTERVAL", time.Second),
		WorkerLeaseDuration:   getDuration("GOFARI_WORKER_LEASE_DURATION", 30*time.Second),
		WorkerLeaseHeartbeat:  getDuration("GOFARI_WORKER_LEASE_HEARTBEAT", 10*time.Second),
		WorkerShutdownTimeout: getDuration("GOFARI_WORKER_SHUTDOWN_TIMEOUT", 20*time.Second),
		WorkerID:              get("GOFARI_WORKER_ID", defaultWorkerID()),
		DBMaxConns:            getInt("GOFARI_DB_MAX_CONNS", 10),
		DBMinConns:            getInt("GOFARI_DB_MIN_CONNS", 1),
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("GOFARI_DATABASE_URL is required")
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 1024 {
		return errors.New("worker concurrency must be between 1 and 1024")
	}
	if c.WorkerPollInterval < 100*time.Millisecond {
		return errors.New("worker poll interval must be at least 100ms")
	}
	if c.WorkerLeaseDuration < 5*time.Second {
		return errors.New("worker lease duration must be at least 5s")
	}
	if c.WorkerLeaseHeartbeat <= 0 || c.WorkerLeaseHeartbeat >= c.WorkerLeaseDuration/2 {
		return errors.New("worker lease heartbeat must be less than half the lease duration")
	}
	if c.WorkerShutdownTimeout <= 0 {
		return errors.New("worker shutdown timeout must be a positive number")
	}
	if c.WorkerID == "" {
		return errors.New("worker ID must not be empty")
	}
	if c.DBMinConns < 0 || c.DBMaxConns < 1 || c.DBMinConns > c.DBMaxConns {
		return errors.New("database connection limits are invalid")
	}
	return nil
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func get(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func getDuration(key string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}
