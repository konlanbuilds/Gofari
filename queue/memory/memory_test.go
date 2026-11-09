package memory

import (
	"context"
	"testing"
	"time"

	"github.com/konlanbuilds/Gofari/jobs"
)

func TestStoreLifecycle(t *testing.T) {
	store := New()
	enqueued, err := store.Enqueue(context.Background(), jobs.Job{
		TargetKind: jobs.TargetTask, TargetName: "task", MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background(), "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != enqueued.ID || claimed.Status != jobs.StatusRunning {
		t.Fatalf("unexpected claim: %+v", claimed)
	}
	if err := store.AcknowledgeSuccess(context.Background(), claimed.ID, claimed.LeaseToken, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeSuccess(context.Background(), claimed.ID, claimed.LeaseToken, nil); err == nil {
		t.Fatal("expected second acknowledgement to fail")
	}
}

func TestStoreRejectsStaleLeaseToken(t *testing.T) {
	store := New()
	job, err := store.Enqueue(context.Background(), jobs.Job{TargetKind: jobs.TargetTask, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background(), "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeSuccess(context.Background(), job.ID, job.LeaseToken, nil); err == nil {
		t.Fatal("expected stale token to fail")
	}
	if err := store.AcknowledgeSuccess(context.Background(), claimed.ID, claimed.LeaseToken, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCancelQueuedJob(t *testing.T) {
	store := New()
	job, err := store.Enqueue(context.Background(), jobs.Job{TargetKind: jobs.TargetTask})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CancelRequested(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.Claim(context.Background(), "worker", time.Second); err != nil || claimed != nil {
		t.Fatalf("cancelled job was claimable: job=%v err=%v", claimed, err)
	}
}
