package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("GOFARI_DATABASE_URL", "postgres://localhost/gofari")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkerConcurrency != 4 || c.WorkerLeaseDuration != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.WorkerLeaseHeartbeat != 10*time.Second || c.DBMaxConns != 10 || c.DBMinConns != 1 {
		t.Fatalf("unexpected extended defaults: %+v", c)
	}
	if c.WorkerID == "" {
		t.Fatal("worker ID is empty")
	}
}

func TestLoadValidation(t *testing.T) {
	t.Setenv("GOFARI_DATABASE_URL", "postgres://localhost/gofari")
	t.Setenv("GOFARI_WORKER_CONCURRENCY", "0")
	if _, err := Load(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestLoadRejectsInvalidHeartbeat(t *testing.T) {
	t.Setenv("GOFARI_DATABASE_URL", "postgres://localhost/gofari")
	t.Setenv("GOFARI_WORKER_LEASE_HEARTBEAT", "20s")
	if _, err := Load(); err == nil {
		t.Fatal("expected heartbeat validation error")
	}
}
