package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/konlanbuilds/Gofari/jobs"
	"github.com/konlanbuilds/Gofari/queue"
)

type Store struct{ Pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) Enqueue(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	if job.Payload == nil {
		job.Payload = json.RawMessage("null")
	}
	if job.PayloadVersion == 0 {
		job.PayloadVersion = 1
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = 5
	}
	var result jobs.Job
	transaction, err := s.Pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	defer transaction.Rollback(ctx)

	err = scanJob(transaction.QueryRow(ctx, `
		INSERT INTO jobs (
			target_kind, target_name, payload, payload_version, idempotency_key,
			max_attempts, run_at, trace_parent
		) VALUES ($1, $2, $3::jsonb, $4, $5, $6, COALESCE($7, now()), $8)
		ON CONFLICT (target_kind, target_name, idempotency_key)
			WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+jobColumns, job.TargetKind, job.TargetName, []byte(job.Payload),
		job.PayloadVersion, nullableString(job.IdempotencyKey), job.MaxAttempts,
		nullableTime(job.RunAt), nullableString(job.TraceParent)), &result)
	if err == nil {
		return result, transaction.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) || job.IdempotencyKey == "" {
		return jobs.Job{}, err
	}

	err = scanJob(transaction.QueryRow(ctx, `SELECT `+jobColumns+`
		FROM jobs WHERE target_kind=$1 AND target_name=$2 AND idempotency_key=$3`,
		job.TargetKind, job.TargetName, job.IdempotencyKey), &result)
	if err != nil {
		return jobs.Job{}, err
	}
	if !sameJSON(result.Payload, job.Payload) || result.PayloadVersion != job.PayloadVersion {
		return jobs.Job{}, queue.ErrConflict
	}
	return result, transaction.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (jobs.Job, error) {
	var job jobs.Job
	err := scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id), &job)
	return job, err
}

func (s *Store) List(ctx context.Context, status jobs.Status, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
		WHERE ($1 = '' OR status = $1) ORDER BY created_at DESC, id DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []jobs.Job
	for rows.Next() {
		var job jobs.Job
		if err := rows.Scan(jobScanArgs(&job)...); err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, rows.Err()
}

func (s *Store) Claim(context.Context, string, time.Duration) (*jobs.Job, error) {
	return nil, errors.New("postgres claim is not implemented yet")
}
func (s *Store) AcknowledgeSuccess(context.Context, uuid.UUID, uuid.UUID, []byte) error {
	return errors.New("postgres acknowledge is not implemented yet")
}
func (s *Store) AcknowledgeFailure(context.Context, uuid.UUID, uuid.UUID, string, *time.Time) error {
	return errors.New("postgres acknowledge is not implemented yet")
}
func (s *Store) CancelRequested(context.Context, uuid.UUID) error {
	return errors.New("postgres cancellation is not implemented yet")
}

const jobColumns = `id, target_kind, target_name, payload, payload_version, status,
	idempotency_key, attempts, max_attempts, run_at, lease_until, lease_token,
	worker_id, last_error, result, cancel_requested_at, trace_parent, created_at,
	started_at, finished_at, updated_at`

func scanJob(row pgx.Row, job *jobs.Job) error { return row.Scan(jobScanArgs(job)...) }

func jobScanArgs(job *jobs.Job) []any {
	return []any{
		&job.ID, &job.TargetKind, &job.TargetName, &job.Payload, &job.PayloadVersion,
		&job.Status, &job.IdempotencyKey, &job.Attempts, &job.MaxAttempts, &job.RunAt,
		&job.LeaseUntil, &job.LeaseToken, &job.WorkerID, &job.LastError, &job.Result,
		&job.CancelRequestedAt, &job.TraceParent, &job.CreatedAt, &job.StartedAt,
		&job.FinishedAt, &job.UpdatedAt,
	}
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func sameJSON(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return string(left) == string(right)
	}
	return reflect.DeepEqual(a, b)
}

var _ queue.Store = (*Store)(nil)
