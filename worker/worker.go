package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/konlanbuilds/Gofari/executor"
	"github.com/konlanbuilds/Gofari/jobs"
	"github.com/konlanbuilds/Gofari/queue"
)

type Worker struct {
	Store    queue.Store
	Registry *executor.Registry
	WorkerID string
	Lease    time.Duration
}

func New(store queue.Store, reg *executor.Registry, workerID string, lease time.Duration) *Worker {
	if lease == 0 {
		lease = 30 * time.Second
	}
	return &Worker{
		Store:    store,
		Registry: reg,
		WorkerID: workerID,
		Lease:    lease,
	}
}

func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, err := w.Store.Claim(ctx, w.WorkerID, w.Lease)
	if err != nil {
		return false, err
	}
	if job == nil {
		return false, nil
	}

	if job.TargetKind == jobs.TargetWorkflow {
		return true, w.Store.AcknowledgeFailure(ctx, job.ID, job.LeaseToken, "workflow not supported in v1", nil)
	}

	if w.Registry == nil {
		return true, w.Store.AcknowledgeFailure(ctx, job.ID, job.LeaseToken, "no executor registry", nil)
	}

	exec, ok := w.Registry.Resolve(job.TargetName)
	if !ok {
		return true, w.Store.AcknowledgeFailure(ctx, job.ID, job.LeaseToken, "unknown task: "+job.TargetName, nil)
	}

	res, execErr := exec.Execute(ctx, *job)
	if execErr != nil {
		return true, w.Store.AcknowledgeFailure(ctx, job.ID, job.LeaseToken, execErr.Error(), nil)
	}

	if res == nil {
		res = json.RawMessage("null")
	}
	if err := w.Store.AcknowledgeSuccess(ctx, job.ID, job.LeaseToken, res); err != nil {
		return true, err
	}
	return true, nil
}

func (w *Worker) Run(ctx context.Context, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		return errors.New("poll interval must be positive")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := w.ProcessOne(ctx); err != nil {
				return err
			}
		}
	}
}
