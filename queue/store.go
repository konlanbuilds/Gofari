package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/konlanbuilds/Gofari/jobs"
)

type Store interface {
	Enqueue(ctx context.Context, job jobs.Job) (jobs.Job, error)
	Claim(ctx context.Context, workerID string, lease time.Duration) (*jobs.Job, error)
	AcknowledgeSuccess(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, result []byte) error
	AcknowledgeFailure(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, lastErr string, retryAt *time.Time) error
	CancelRequested(ctx context.Context, id uuid.UUID) error
}
