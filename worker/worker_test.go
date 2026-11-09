package worker

import (
	"context"
	"testing"
	"time"

	"github.com/konlanbuilds/Gofari/executor"
	"github.com/konlanbuilds/Gofari/jobs"
	"github.com/konlanbuilds/Gofari/queue/memory"
)

func TestProcessOneExecutesRegisteredTask(t *testing.T) {
	store := memory.New()
	reg := executor.NewRegistry()
	if err := reg.Register("success", &executor.TestSuccess{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(context.Background(), jobs.Job{
		TargetKind: jobs.TargetTask, TargetName: "success", MaxAttempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	w := New(store, reg, "worker", time.Second)
	processed, err := w.ProcessOne(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessOne() = %v, %v", processed, err)
	}
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := New(memory.New(), executor.NewRegistry(), "worker", time.Second)
	if err := w.Run(ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}
}
