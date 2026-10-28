package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TargetKind string

const (
	TargetTask     TargetKind = "task"
	TargetWorkflow TargetKind = "workflow"
)

type Job struct {
	ID                uuid.UUID
	TargetKind        TargetKind
	TargetName        string
	Payload           json.RawMessage
	PayloadVersion    int
	Status            Status
	IdempotencyKey    string
	Attempts          int
	MaxAttempts       int
	RunAt             time.Time
	LeaseUntil        *time.Time
	LeaseToken        uuid.UUID
	WorkerID          string
	LastError         string
	Result            json.RawMessage
	CancelRequestedAt *time.Time
	TraceParent       string
	CreatedAt         time.Time
	StartedAt         *time.Time
	FinishedAt        *time.Time
	UpdatedAt         time.Time
}
