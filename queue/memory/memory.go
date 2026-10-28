package memory

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/konlanbuilds/Gofari/jobs"
)

type Store struct {
	mu   sync.Mutex
	jobs map[uuid.UUID]jobs.Job
}

func New() *Store {
	return &Store{
		jobs: make(map[uuid.UUID]jobs.Job),
	}
}

func (s *Store) Enqueue(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	if ctx.Err() != nil {
		return jobs.Job{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if job.PayloadVersion == 0 {
		job.PayloadVersion = 1
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = 1
	}
	now := time.Now().UTC()
	if job.RunAt.IsZero() {
		job.RunAt = now
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	job.Status = jobs.StatusQueued
	job.Attempts = 0
	job.LeaseUntil = nil
	job.LeaseToken = uuid.Nil
	job.WorkerID = ""
	job.LastError = ""
	job.Result = nil
	job.FinishedAt = nil
	job.StartedAt = nil
	job.CancelRequestedAt = nil

	s.jobs[job.ID] = job
	return job, nil
}

func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*jobs.Job, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var chosenID uuid.UUID
	var chosenJob jobs.Job
	found := false

	for id, j := range s.jobs {
		if j.Status != jobs.StatusQueued {
			continue
		}
		if j.RunAt.After(now) {
			continue
		}
		if !found {
			chosenID = id
			chosenJob = j
			found = true
			continue
		}

		if j.RunAt.Before(chosenJob.RunAt) ||
			(j.RunAt.Equal(chosenJob.RunAt) && j.CreatedAt.Before(chosenJob.CreatedAt)) ||
			(j.RunAt.Equal(chosenJob.RunAt) && j.CreatedAt.Equal(chosenJob.CreatedAt) && id.String() < chosenID.String()) {
			chosenID = id
			chosenJob = j
		}
	}
	if !found {
		return nil, nil
	}

	claimedJob := s.jobs[chosenID]
	if claimedJob.Status != jobs.StatusQueued {
		return nil, nil
	}
	claimedJob.Attempts++
	if claimedJob.StartedAt == nil {
		sn := time.Now().UTC()
		claimedJob.StartedAt = &sn
	}
	leaseUntil := time.Now().UTC().Add(lease)
	claimedJob.LeaseUntil = &leaseUntil
	claimedJob.LeaseToken = uuid.New()
	claimedJob.WorkerID = workerID
	claimedJob.Status = jobs.StatusRunning
	claimedJob.UpdatedAt = time.Now().UTC()
	s.jobs[claimedJob.ID] = claimedJob
	return &claimedJob, nil
}

func (s *Store) AcknowledgeSuccess(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return errors.New("job not found")
	}
	if j.Status != jobs.StatusRunning {
		return errors.New("job not running")
	}
	if j.LeaseToken != leaseToken {
		return errors.New("lease token mismatch")
	}
	now := time.Now().UTC()
	j.Status = jobs.StatusSucceeded
	if result != nil {
		j.Result = append(json.RawMessage(nil), result...)
	}
	j.FinishedAt = &now
	j.LeaseUntil = nil
	j.LeaseToken = uuid.Nil
	j.WorkerID = ""
	j.UpdatedAt = now
	s.jobs[id] = j
	return nil
}

func (s *Store) AcknowledgeFailure(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, lastErr string, retryAt *time.Time) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return errors.New("job not found")
	}
	if j.Status != jobs.StatusRunning {
		return errors.New("job not running")
	}
	if j.LeaseToken != leaseToken {
		return errors.New("lease token mismatch")
	}
	now := time.Now().UTC()
	j.LastError = lastErr
	if retryAt != nil && j.Attempts < j.MaxAttempts {
		j.Status = jobs.StatusQueued
		j.RunAt = *retryAt
		j.LeaseUntil = nil
		j.LeaseToken = uuid.Nil
		j.WorkerID = ""
		j.UpdatedAt = now
		s.jobs[id] = j
		return nil
	}
	j.Status = jobs.StatusFailed
	j.FinishedAt = &now
	j.LeaseUntil = nil
	j.LeaseToken = uuid.Nil
	j.WorkerID = ""
	j.UpdatedAt = now
	s.jobs[id] = j
	return nil
}

func (s *Store) CancelRequested(ctx context.Context, id uuid.UUID) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return errors.New("job not found")
	}
	now := time.Now().UTC()
	if j.Status == jobs.StatusQueued {
		j.Status = jobs.StatusCancelled
		j.CancelRequestedAt = &now
		j.FinishedAt = &now
		j.UpdatedAt = now
		s.jobs[id] = j
		return nil
	}
	if j.Status == jobs.StatusRunning {
		if j.CancelRequestedAt == nil {
			j.CancelRequestedAt = &now
			j.UpdatedAt = now
			s.jobs[id] = j
		}
		return nil
	}
	return nil
}
