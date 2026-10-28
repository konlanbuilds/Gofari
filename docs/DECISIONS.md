# Gofari Design Decisions

Status: **active implementation contract**. These decisions resolve ambiguities in
[`PLAN.md`](./PLAN.md) and supersede the plan where the two disagree. 

Decision IDs (`D-*`) are stable references for code comments and pull requests.

---

## 1. Repository and package layout

### D-1.1 Track the Go module

`go.mod` and `go.sum` are build inputs and must be committed. Do not add `*.mod`
or `*.sum` to `.gitignore`; doing so makes a fresh clone unbuildable and breaks
CI.

They are now tracked. To keep them tracked:

```bash
go mod tidy
git add go.mod go.sum
```

`chi` is not currently in `go.mod`: `go mod tidy` removed it because nothing
imports it yet. Re-add it when `internal/httpapi` is wired. An empty `go.sum` is
valid until the first dependency is added.

### D-1.2 Module and Go version

- Module path: `github.com/konlanbuilds/Gofari`
- Go version: `1.27.2` (toolchain currently installed)

### D-1.3 Package layout

Gofari is both a reusable core and a deployable service, so domain and queue
packages are public. Only service wiring lives under `internal/`.

```text
cmd/
  api/                 HTTP service entry point
  worker/              worker service entry point
  migrate/             explicit migration command

jobs/                  public domain: Job, Status, TargetKind, validation errors
queue/                 public storage contract
  memory/              in-memory test double implementing queue.Store
executor/              public executor interface, registry, error classification
worker/                public polling runner and ProcessOne lifecycle
postgres/              public pgx implementation of queue.Store
migrations/            embedded SQL migrations + embed.go
internal/
  config/              environment parsing and validation
  httpapi/             handlers, middleware, JSON errors, request IDs
  observability/       slog, Prometheus, OpenTelemetry setup
```

This replaces the `internal/jobs`, `internal/job`, `internal/queue`,
`internal/executor`, and `internal/worker` placements in the plan. The old
`internal/job/` placeholder has become `jobs/` at the module root; the queue and
executor placeholders moved to `queue/` and `executor/`, and `postgres/` replaces
`internal/database/`.

### D-1.4 Public core, internal service

`jobs`, `queue`, `executor`, `worker`, and `postgres` must not import anything
under `internal/`. Their configuration is passed as explicit structs, not loaded
from the environment. This keeps them usable by another module and testable
without process-level setup.

If reusable-core support is later dropped, the packages can be moved under
`internal/` without changing their contents.

### D-1.5 Dependencies

Keep the dependency set intentional:

| Purpose | Choice |
|---|---|
| HTTP routing | `github.com/go-chi/chi/v5` |
| PostgreSQL | `github.com/jackc/pgx/v5` + `pgxpool` |
| UUIDs | `github.com/google/uuid` |
| Migrations | `github.com/pressly/goose/v3` |
| Metrics | `github.com/prometheus/client_golang` |
| Tracing | OpenTelemetry SDK + OTLP HTTP exporter |
| Tests | standard library `testing`; no assertion framework initially |

No ORM and no SQL code generator initially. SQL stays explicit.

UUID mapping: the domain uses `google/uuid.UUID`; the `postgres` package converts
to/from `pgtype.UUID`. No extra UUID adapter dependency.

### D-1.6 Repository hygiene

- The tracked-but-deleted `internal/handler`, `internal/retry`, and
  `internal/snowflake` placeholders have been removed.
- `internal/worker/` is intentionally absent; the worker lives at `worker/`.
- `.env` stays ignored. `.env.example` is the committed template.

---

## 2. Domain model

### D-2.1 One Job type from Version 1

Use the final field names immediately. Do **not** introduce a temporary `Type`
field for the in-memory version and rename it later.

```go
type Job struct {
    ID                 uuid.UUID
    TargetKind         TargetKind  // "task" (and "workflow" reserved)
    TargetName         string
    Payload            json.RawMessage
    PayloadVersion     int
    Status             Status
    IdempotencyKey     string        // "" means absent
    Attempts           int
    MaxAttempts        int
    RunAt              time.Time
    LeaseUntil         *time.Time
    LeaseToken         uuid.UUID     // uuid.Nil when no lease
    WorkerID           string        // "" when no lease
    LastError          string        // "" when none
    Result             json.RawMessage // nil when none
    CancelRequestedAt  *time.Time
    TraceParent        string        // "" when tracing is off
    CreatedAt          time.Time
    StartedAt          *time.Time
    FinishedAt         *time.Time
    UpdatedAt          time.Time
}
```

Conventions:

- Optional timestamps are pointers so JSON can omit them and the zero value is
  unambiguous.
- Optional strings use `""`; `postgres` maps that to `NULL`.
- `Result == nil` maps to SQL `NULL`.
- `Payload` is always valid JSON; `null` is a valid payload.

### D-2.2 Statuses and target kinds

```go
type Status string
const (
    StatusQueued    Status = "queued"
    StatusRunning   Status = "running"
    StatusSucceeded Status = "succeeded"
    StatusFailed    Status = "failed"
    StatusCancelled Status = "cancelled"
)

type TargetKind string
const (
    TargetTask     TargetKind = "task"
    TargetWorkflow TargetKind = "workflow"
)
```

Terminal statuses: `succeeded`, `failed`, `cancelled`.

### D-2.3 Workflow is reserved, not executable

Version 1 has no workflow table, workflow definition language, or workflow
executor. Storing `target_kind = "workflow"` would only produce a permanently
failing job.

Decision: the database accepts `task` and `workflow` for forward compatibility,
but the Version 1 HTTP API rejects `workflow` with `422`. A workflow feature must
add its own schema and execution semantics; it must not overload the task
registry.

### D-2.4 Payload and payload version

- `Payload` is any valid JSON value. The API rejects invalid JSON and payloads
  larger than the configured limit (default 256 KiB). The `payload` column is
  `NOT NULL`; an omitted request payload is stored as JSONB `null`, not SQL
  `NULL`.
- `PayloadVersion` defaults to `1`, must be `>= 1`, and is passed to the executor.
- Version 1 does not implement central version negotiation. A task that needs
  multiple payload versions handles that inside its executor or a future adapter.
  The field is not a dead column: it is part of the job contract and visible to
  executors.

### D-2.5 Attempts

`Attempts` means **number of claims**, not number of completed executions and not
number of failures.

- Initial value: `0`.
- The claim operation increments it.
- A retry does not increment it; the next claim does.
- A claim is only allowed while `attempts < max_attempts`.
- After a retryable failure, the job is terminal `failed` when
  `attempts >= max_attempts`; otherwise it returns to `queued`.
- Lease-expiry recovery follows the same terminal rule.
- Manual retry resets `attempts` to `0` (see D-5.6).

This removes the plan’s double-increment of attempts on retry.

### D-2.6 State transitions

| From | To | Actor | Guard / effect |
|---|---|---|---|
| — | `queued` | API | Insert; `attempts = 0`; `finished_at = NULL` |
| `queued` | `running` | worker claim | `run_at <= now()`, `attempts < max_attempts`; increment attempts; set lease and `started_at` |
| `queued` | `cancelled` | API | Immediate; set `cancel_requested_at` and `finished_at` |
| `running` | `succeeded` | worker | Lease token + `status = 'running'`; no cancel request; store result; clear lease |
| `running` | `cancelled` | worker | Lease token + `status = 'running'`; cancel request committed before finalization |
| `running` | `queued` | worker | Lease token + `status = 'running'`; retryable, `attempts < max_attempts`; set backoff `run_at` |
| `running` | `failed` | worker | Lease token + `status = 'running'`; permanent or `attempts >= max_attempts` |
| `running` | `cancelled` | reaper | Lease expired and cancel requested |
| `running` | `queued` | reaper | Lease expired, `attempts < max_attempts` |
| `running` | `failed` | reaper | Lease expired, `attempts >= max_attempts` |
| `failed` | `queued` | API manual retry | Reset `attempts = 0`, clear lease/result/error/finish time |
| terminal | — | — | No other transitions; `cancelled` is not retryable in v1 |

### D-2.7 Timing

All scheduling and lease decisions use the database clock (`now()`), never the
worker’s clock:

- `run_at` eligibility
- lease expiry
- retry `run_at` computation (`now() + interval`)
- `created_at`, `updated_at`, `finished_at`

Workers use local time only for context timeouts and for computing a backoff
duration passed to SQL. This avoids clock-skew-induced duplicate execution.

---

## 3. Executor contract

### D-3.1 Interface

```go
package executor

type Executor interface {
    Execute(ctx context.Context, job jobs.Job) (json.RawMessage, error)
}
```

The whole job is passed, not just the payload, so executors can use the job ID for
idempotency and inspect `PayloadVersion`/`TargetName`.

Delivery is at-least-once. Executors must be idempotent, keyed by `job.ID`, because
lease expiry or a lost connection can cause the same job to execute again.
Returning `nil` with a nil error means "success with no result".

### D-3.2 Error classification

Unknown errors are **retryable** by default. Permanent failures must be explicit.

```go
package executor

type Error struct {
    Permanent bool
    Err       error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

func Permanent(err error) error  // marks err permanent
func Retryable(err error) error  // marks err retryable (useful to override)
```

Worker classification:

| Error source | Classification |
|---|---|
| `executor.Error{Permanent: true}` | permanent → `failed` |
| Any other error, including `context.DeadlineExceeded` | retryable |
| `context.Canceled` caused by job cancellation | handled as cancellation, not failure |
| Panic recovered by worker | retryable, stack logged |
| Unknown `TargetName` | permanent |
| Invalid or oversized executor result | permanent |

Task-level payload validation belongs in the executor and must be wrapped with
`executor.Permanent`.

### D-3.3 Timeouts

- One execution timeout per job: `GOFARI_WORKER_EXECUTION_TIMEOUT` (default 5m).
- The worker wraps `Execute` in `context.WithTimeout`.
- Timeout expiry is a retryable failure (unless attempts are exhausted).
- Lease heartbeat continues while the executor runs, so a 5-minute execution is
  valid with a 30-second lease.

### D-3.4 Registry

```go
type Registry struct { /* explicit map */ }

func NewRegistry() *Registry
func (r *Registry) Register(name string, e Executor) error // duplicate name = error
func (r *Registry) Resolve(name string) (Executor, bool)
func (r *Registry) Names() []string
```

- Tasks are registered explicitly at startup. No `init()` registration and no
  dynamic loading from PostgreSQL.
- Duplicate registration fails process startup.
- An empty registry fails worker startup; it is a configuration error.
- Task names match `^[a-z][a-z0-9_.-]{0,127}$`. The plan’s `test_success` is valid.
- The API does **not** validate task names against the registry. API and worker
  may be deployed at different times with different registries. An unknown task
  claimed by a worker becomes a permanent failure.
- Consequence: a job targeting a task that is not yet deployed fails permanently
  on first execution. Deploy workers before producers, or change this decision to
  "unknown task is retryable" later.

### D-3.5 Results

- Results must be valid JSON; otherwise the attempt is a permanent failure.
- Results larger than `GOFARI_JOB_MAX_RESULT_BYTES` (default 1 MiB) are a permanent
  failure.
- Error text stored in `last_error` is truncated to 4 KiB. Payloads and results
  are never logged.

---

## 4. PostgreSQL persistence

### D-4.1 Driver and migrations

- Runtime: `pgx/v5` with `pgxpool`. No `database/sql` in production paths.
- Migrations: `goose/v3` with embedded SQL files under `migrations/`.
- `migrations/embed.go` exposes the embedded filesystem.
- `cmd/migrate` supports `up` and `status`; `down` exists for local development
  only.
- Services never run migrations at startup.
- Readiness checks the embedded migration version against `goose_db_version`
  (configurable, enabled by default).
- Migrations use `database/sql` + `pgx/v5/stdlib` because that is what goose
  expects; this is the only `database/sql` use.

### D-4.2 Schema additions to the plan

Add these columns to `jobs`:

| Column | Type | Notes |
|---|---|---|
| `cancel_requested_at` | `timestamptz NULL` | Cooperative cancellation request |
| `trace_parent` | `text NULL` | W3C `traceparent` from the enqueue trace |

`payload` is `NOT NULL`; omitted request payloads are stored as JSONB `null`.
`payload_version` is `NOT NULL DEFAULT 1`.
`max_attempts` is `NOT NULL DEFAULT 5`.
`run_at` is `NOT NULL DEFAULT now()`.
`lease_token` and `lease_until` are `NULL` outside `running`.
`worker_id` is `NULL` outside `running`.

Do **not** add a `queue` column yet; named queues are deferred.

### D-4.3 Constraints

```sql
CHECK (status IN ('queued','running','succeeded','failed','cancelled'))
CHECK (target_kind IN ('task','workflow'))
CHECK (attempts >= 0)
CHECK (max_attempts BETWEEN 1 AND 100)
CHECK ((status = 'running') =
       (lease_token IS NOT NULL AND lease_until IS NOT NULL AND worker_id IS NOT NULL))
CHECK ((finished_at IS NOT NULL) =
       (status IN ('succeeded','failed','cancelled')))
CHECK (cancel_requested_at IS NULL OR status IN ('running','cancelled'))
CHECK (result IS NULL OR status = 'succeeded')
```

The lease consistency check is deliberate: every transition that leaves `running`
must clear the lease in the same statement.

### D-4.4 Indexes

```sql
CREATE INDEX jobs_claimable_idx
ON jobs (run_at, created_at, id)
WHERE status = 'queued';

CREATE INDEX jobs_expired_lease_idx
ON jobs (lease_until)
WHERE status = 'running';

CREATE UNIQUE INDEX jobs_idempotency_uidx
ON jobs (target_kind, target_name, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_listing_idx
ON jobs (status, created_at DESC, id DESC);

CREATE INDEX jobs_retention_idx
ON jobs (finished_at, id)
WHERE status IN ('succeeded','failed','cancelled');
```

The claimable index includes `id` to match the deterministic claim ordering.

### D-4.5 Claim

One job per claim in Version 1; the SQL accepts a limit for later batching.
Default transaction isolation (Read Committed) is sufficient because row locks
and status guards provide correctness.

```sql
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE status = 'queued'
      AND run_at <= now()
      AND attempts < max_attempts
    ORDER BY run_at, created_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET status        = 'running',
    attempts      = j.attempts + 1,
    started_at    = COALESCE(j.started_at, now()),
    lease_until   = now() + make_interval(secs => $2),
    lease_token   = gen_random_uuid(),
    worker_id     = $1,
    updated_at    = now()
FROM candidate c
WHERE j.id = c.id
RETURNING j.*;
```

- `$2` is the lease duration in seconds (a float), avoiding PostgreSQL interval
  string parsing in Go.
- `lease_token` is generated by PostgreSQL, one per claim.
- `started_at` is the first-ever start and survives retries and manual retries.
- `finished_at` is already `NULL` for queued jobs.

### D-4.6 Heartbeat and cancellation polling

One query per in-flight job performs both lease renewal and cancellation
detection:

```sql
UPDATE jobs
SET lease_until = now() + make_interval(secs => $3),
    updated_at  = now()
WHERE id = $1
  AND lease_token = $2
  AND status = 'running'
RETURNING cancel_requested_at;
```

- No row returned: the worker lost the lease (expired or reclaimed). Cancel the
  local execution and discard its result.
- `cancel_requested_at` non-null: cancel the local context; the executor is
  expected to return promptly.
- Run every `GOFARI_WORKER_LEASE_HEARTBEAT` (default 10s, must be less than half
  the lease duration).

This is poll-based. `LISTEN/NOTIFY` may be added later to reduce cancellation
latency, but polling remains the correct fallback.

### D-4.7 Finalization

Every worker finalization includes `id`, `lease_token`, and
`status = 'running'`. Zero rows returned means a stale worker; log and discard.

Success (cancellation wins if requested before this statement commits):

```sql
UPDATE jobs
SET status = CASE WHEN cancel_requested_at IS NOT NULL
                  THEN 'cancelled' ELSE 'succeeded' END,
    result = CASE WHEN cancel_requested_at IS NULL
                  THEN $3::jsonb ELSE NULL END,
    finished_at = now(),
    lease_until = NULL,
    lease_token = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND lease_token = $2 AND status = 'running'
RETURNING status;
```

Failure/retry:

```sql
UPDATE jobs
SET status = CASE
        WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
        WHEN $3::boolean OR attempts >= max_attempts THEN 'failed'
        ELSE 'queued'
    END,
    last_error = CASE WHEN cancel_requested_at IS NOT NULL
                      THEN NULL ELSE $4 END,
    finished_at = CASE
        WHEN cancel_requested_at IS NOT NULL
          OR $3::boolean
          OR attempts >= max_attempts THEN now()
        ELSE NULL
    END,
    run_at = CASE
        WHEN cancel_requested_at IS NULL
          AND NOT $3::boolean
          AND attempts < max_attempts
        THEN now() + make_interval(secs => $5)
        ELSE run_at
    END,
    lease_until = NULL,
    lease_token = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND lease_token = $2 AND status = 'running'
RETURNING status;
```

- `$3` is `permanent`.
- `$4` is the truncated error message.
- `$5` is the jittered backoff in seconds (a float); ignored for permanent
  failures and cancellations.

### D-4.8 Expired-lease recovery

Each worker runs a reaper every `GOFARI_WORKER_RECOVERY_INTERVAL` (default 10s).
Multiple workers may run it concurrently; row locks and guards make it safe.

```sql
WITH expired AS (
    SELECT id
    FROM jobs
    WHERE status = 'running'
      AND lease_until < now()
    ORDER BY lease_until
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET status = CASE
        WHEN j.cancel_requested_at IS NOT NULL THEN 'cancelled'
        WHEN j.attempts >= j.max_attempts THEN 'failed'
        ELSE 'queued'
    END,
    last_error = CASE
        WHEN j.cancel_requested_at IS NULL
          AND j.attempts >= j.max_attempts
        THEN 'lease expired after final attempt'
        ELSE j.last_error
    END,
    finished_at = CASE
        WHEN j.cancel_requested_at IS NOT NULL
          OR j.attempts >= j.max_attempts THEN now()
        ELSE NULL
    END,
    lease_until = NULL,
    lease_token = NULL,
    worker_id = NULL,
    updated_at = now()
FROM expired e
WHERE j.id = e.id
RETURNING j.id, j.status;
```

- Recovery does not add backoff. The job is immediately eligible again; attempts
  bound poison crash loops.
- A job whose final attempt crashed becomes terminal `failed`.
- A cancellation requested before the worker died becomes terminal `cancelled`.
- Emit `gofari_lease_recoveries_total{outcome="queued|failed|cancelled"}`.

### D-4.9 Backoff

- Base delay: `GOFARI_RETRY_BASE_DELAY` (default 1s).
- Maximum delay: `GOFARI_RETRY_MAX_DELAY` (default 5m).
- Formula: `delay = min(base * 2^(attempts-1), max)`, then full jitter in
  `[0, delay]`.
- `attempts` is the just-completed attempt number (already incremented at claim).
- Clock and random source must be injectable so tests are deterministic.
- Permanent failures do not wait for retries.

### D-4.10 Enqueue and idempotency

```sql
INSERT INTO jobs (
    target_kind, target_name, payload, payload_version,
    status, idempotency_key, attempts, max_attempts, run_at,
    trace_parent, created_at, updated_at
)
VALUES ($1, $2, $3::jsonb, $4, 'queued', $5, 0, $6, COALESCE($7, now()), $8, now(), now())
ON CONFLICT (target_kind, target_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING *;
```

If no row is returned, select the existing job by the key and compare identity.
Run the insert and fallback select in one transaction so the comparison cannot
race with a concurrent delete.

- Same `payload` (JSONB equality) and `payload_version`: return the existing job
  with `created = false`.
- Different payload or version: return `409 Conflict`; do not silently reuse the
  job for different work.
- `run_at` and `max_attempts` are ignored on replay because the existing job wins.

HTTP mapping:

- New job: `201 Created` + `Location: /v1/jobs/{id}`
- Idempotent replay: `200 OK` with the existing job
- Key reused with different payload/version: `409 Conflict`

The key is scoped to `(target_kind, target_name, idempotency_key)`. Retention
cleanup eventually deletes the row, after which the key may be reused; document
this.

### D-4.11 Manual retry

```sql
UPDATE jobs
SET status = 'queued',
    attempts = 0,
    run_at = now(),
    finished_at = NULL,
    last_error = NULL,
    result = NULL,
    lease_until = NULL,
    lease_token = NULL,
    worker_id = NULL,
    cancel_requested_at = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'failed'
RETURNING *;
```

- Only `failed` is retryable. `cancelled` is deliberately not retryable in
  Version 1; enqueue a new job instead.
- `started_at` is retained; it means first-ever start.
- `attempts = 0` gives the job a fresh retry budget.
- Concurrent retries: one succeeds, the other sees `queued` and returns `409`.

### D-4.12 `job_events`

Deferred. Do not add the table in Version 1 or milestone 2. It is an additive
migration later. Queue operation signatures must not depend on event insertion.

### D-4.13 Connection pool

- `pgxpool` with `GOFARI_DB_MAX_CONNS` default `max(10, WORKER_CONCURRENCY+5)` for
  the worker and `10` for the API.
- `GOFARI_DB_MIN_CONNS` default `1`.
- Connection acquisition timeout is bounded; do not let a worker goroutine hang
  forever on the pool.
- Pool exhaustion is logged and surfaced in readiness/metrics.

---

## 5. Cancellation and graceful shutdown

### D-5.1 Cooperative cancellation across processes

The API cannot cancel a Go context inside a worker. Cancellation is therefore
persisted and observed:

- **Queued job:** the API atomically sets `status = 'cancelled'` and
  `cancel_requested_at = now()`. The job can never start.
- **Running job:** the API sets `cancel_requested_at` if not already set and
  returns `202 Accepted`. It does not set the terminal status.
- **Worker:** the heartbeat query (D-4.6) returns `cancel_requested_at`; the
  worker cancels the job context. Executors should return promptly on
  `ctx.Done()`.
- **Finalization:** the success/failure statements give cancellation priority if
  the flag was committed before finalization (D-4.7).
- **Reaper:** if the worker dies after a cancellation request, recovery marks the
  job `cancelled` (D-4.8).
- **Already cancelled:** cancel is idempotent and returns `200`.
- **Terminal `succeeded`/`failed`:** cancel returns `409`.
- **Unknown job:** `404`.

Cancellation always wins over retry. A job that returns an error after a
cancellation request becomes `cancelled`, not `queued`.

### D-5.2 Shutdown sequence

On SIGTERM/SIGINT, the worker:

1. Stops claiming new jobs (closes off the claim loop).
2. Keeps heartbeating in-flight jobs.
3. Waits up to `GOFARI_WORKER_SHUTDOWN_TIMEOUT` (default 20s) for in-flight jobs
   to finish.
4. If the grace period expires, cancels in-flight contexts and attempts to
   finalize them with the lease token.
5. If the database is unreachable, exits anyway; lease expiry recovers the jobs.

Shutdown-induced cancellation is treated as retryable:

- If `attempts < max_attempts`: return the job to `queued`.
- If `attempts >= max_attempts`: mark it `failed` with a "worker shutdown" error.

This means an operator shutdown can consume the final attempt. Keep the shutdown
timeout comfortably below the lease duration and size `max_attempts` accordingly.
A second signal exits immediately; lease recovery handles the rest.

The API shuts down with `http.Server.Shutdown` using
`GOFARI_API_SHUTDOWN_TIMEOUT` (default 15s): stop accepting new requests, wait
for in-flight requests, then close the pool. The worker's health/metrics server
uses the worker shutdown timeout. Both processes close their database pools last.

### D-5.3 Stale workers

Any finalization or heartbeat that returns zero rows means the lease was lost.
The worker must:

- cancel the local context,
- discard the executor result,
- log the stale completion with job ID, worker ID, and lease token,
- not retry the finalization.

This is the lease-token protection the plan describes.

### D-5.4 Polling and concurrency

- The worker maintains a semaphore of `GOFARI_WORKER_CONCURRENCY` in-flight jobs.
- While slots are free, it claims one job at a time in its own short transaction.
- After a job finishes, the worker immediately tries to claim again.
- When no job is eligible, it sleeps `GOFARI_WORKER_POLL_INTERVAL` with jitter
  (for example 0.5–1.5x) so idle workers do not synchronize.
- Claim transactions are never held while an executor runs.
- Worker identity is generated once per process as `hostname-pid-<random>`,
  overridable with `GOFARI_WORKER_ID`, and logged at startup.

---

## 6. HTTP API

### D-6.1 Routes

All routes are versioned under `/v1`.

| Method | Path | Success | Notes |
|---|---|---|---|
| `POST` | `/v1/jobs` | `201` new, `200` replay | Enqueue |
| `GET` | `/v1/jobs/{id}` | `200` | Inspect |
| `GET` | `/v1/jobs` | `200` | List with filters |
| `POST` | `/v1/jobs/{id}/cancel` | `200` queued/cancelled, `202` running | Cancel |
| `POST` | `/v1/jobs/{id}/retry` | `200` | Failed only |
| `GET` | `/healthz` | `200` | Liveness |
| `GET` | `/readyz` | `200` / `503` | DB + migration readiness |
| `GET` | `/metrics` | `200` | Prometheus |

The worker exposes `/healthz`, `/readyz`, and `/metrics` on
`GOFARI_WORKER_HTTP_ADDR` (default `127.0.0.1:8081`), independent of the API.

### D-6.2 Enqueue request

```json
{
  "target_kind": "task",
  "target_name": "test_success",
  "payload": {"message": "hello"},
  "payload_version": 1,
  "max_attempts": 5,
  "run_at": "2026-10-26T12:00:00Z"
}
```

- `Idempotency-Key` is an HTTP header, not a body field. Max 255 bytes; an empty
  value is treated as absent.
- `target_kind` defaults to `task`; `workflow` is rejected with `422`.
- `target_name` is required and validated against `^[a-z][a-z0-9_.-]{0,127}$`.
- `payload` is optional; omitted means JSON `null`.
- `payload_version` defaults to `1`; must be `>= 1`.
- `max_attempts` is optional; defaults to `GOFARI_JOB_DEFAULT_MAX_ATTEMPTS` (5)
  and must be in `1..GOFARI_JOB_MAX_ATTEMPTS_LIMIT` (100); out of range is
  `400`. This prevents clients from requesting unbounded retries.
- `run_at` is optional; omitted or in the past means immediately eligible.
- Unknown fields are rejected (`DisallowUnknownFields`) to catch typos.
- `Content-Type` must be `application/json`; otherwise `415`.

### D-6.3 Responses and errors

- Responses return the complete job object, including status and result.
- Error envelope:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "target_name is required",
    "request_id": "01J..."
  }
}
```

- Error codes: `invalid_request` (400), `unsupported_media_type` (415),
  `payload_too_large` (413), `not_found` (404), `conflict` (409),
  `unsupported_target` (422), `internal` (500), `unavailable` (503).
- Every response includes `X-Request-ID`; accept a client-supplied value or
  generate one.
- Parse/validation failures never leak internal error text or SQL details.

### D-6.4 List and pagination

- Filters: `status`, `target_name`, `limit` (default 50, max 200), `cursor`.
- Order: `created_at DESC, id DESC`.
- Cursor is opaque base64 encoding of `created_at|id`.
- Invalid status or cursor: `400`.

### D-6.5 Readiness

- API: database ping + expected migration version.
- Worker: database ping + expected migration version.
- Worker startup also fails fast if the executor registry is empty.
- Liveness does not touch the database.

### D-6.6 Security boundary

- No authentication in Version 1. Bind to loopback by default:
  API `127.0.0.1:8080`, worker `127.0.0.1:8081`.
- Enforce the body limit at the HTTP layer (default 1 MiB body, 256 KiB payload).
- Set server timeouts: read header 5s, read 15s, write 30s, idle 60s, max header
  bytes 1 MiB.
- Never log payloads or results.
- Do not expose the API beyond a trusted network until authentication is added.
  This is a release gate, not a note.

---

## 7. Configuration

All application settings use the `GOFARI_` prefix and are validated at startup;
invalid configuration fails fast with a clear error. Secrets are redacted when
the effective configuration is logged.

| Variable | Default | Validation / notes |
|---|---|---|
| `GOFARI_DATABASE_URL` | required | PostgreSQL DSN |
| `GOFARI_API_ADDR` | `127.0.0.1:8080` | HTTP listen address |
| `GOFARI_API_SHUTDOWN_TIMEOUT` | `15s` | Graceful HTTP shutdown |
| `GOFARI_WORKER_HTTP_ADDR` | `127.0.0.1:8081` | Health/metrics listen address |
| `GOFARI_WORKER_ID` | `hostname-pid-<random>` | Worker identity in leases/logs |
| `GOFARI_WORKER_CONCURRENCY` | `4` | `1..1024` |
| `GOFARI_WORKER_POLL_INTERVAL` | `1s` | `>= 100ms`, no-job sleep with jitter |
| `GOFARI_WORKER_LEASE_DURATION` | `30s` | `>= 5s` |
| `GOFARI_WORKER_LEASE_HEARTBEAT` | `10s` | `< lease/2` |
| `GOFARI_WORKER_EXECUTION_TIMEOUT` | `5m` | Per attempt |
| `GOFARI_WORKER_RECOVERY_INTERVAL` | `10s` | Reaper cadence |
| `GOFARI_WORKER_RECOVERY_BATCH` | `100` | Max expired leases per sweep |
| `GOFARI_WORKER_SHUTDOWN_TIMEOUT` | `20s` | Grace period |
| `GOFARI_DB_MAX_CONNS` | `10` (API), `max(10, concurrency+5)` (worker) | Pool size |
| `GOFARI_DB_MIN_CONNS` | `1` | Pool size |
| `GOFARI_JOB_MAX_PAYLOAD_BYTES` | `262144` | 256 KiB |
| `GOFARI_JOB_MAX_RESULT_BYTES` | `1048576` | 1 MiB |
| `GOFARI_JOB_DEFAULT_MAX_ATTEMPTS` | `5` | `1..GOFARI_JOB_MAX_ATTEMPTS_LIMIT`; must not exceed the limit |
| `GOFARI_JOB_MAX_ATTEMPTS_LIMIT` | `100` | Hard client cap |
| `GOFARI_RETRY_BASE_DELAY` | `1s` | Backoff |
| `GOFARI_RETRY_MAX_DELAY` | `5m` | Backoff |
| `GOFARI_READY_REQUIRE_MIGRATIONS` | `true` | Readiness gate |
| `GOFARI_RETENTION_ENABLED` | `false` | Enable only with an explicit policy |
| `GOFARI_RETENTION_AGE` | `720h` | 30 days |
| `GOFARI_RETENTION_BATCH` | `1000` | Deletes per sweep |
| `GOFARI_RETENTION_INTERVAL` | `1h` | Cleanup cadence |
| `GOFARI_LOG_LEVEL` | `info` | `debug|info|warn|error` |
| `GOFARI_OTEL_ENABLED` | `false` | Tracing toggle |
| `GOFARI_OTEL_ENDPOINT` | empty | OTLP HTTP endpoint |
| `GOFARI_OTEL_SERVICE_NAME` | `gofari-api` / `gofari-worker` | Per command |

Local development: commands do not auto-load `.env`. Use a `Makefile` target or
`set -a; source .env; set +a` so production behavior is not hidden. `.env.example`
must include `GOFARI_DATABASE_URL` in addition to the `POSTGRES_*` variables used
by Compose. Keep the duplicated password in sync.

---

## 8. Observability

### D-8.1 Logs

Use `log/slog` with JSON output. Every job lifecycle log includes:

- `job_id`
- `target_kind`
- `target_name`
- `attempt` / `max_attempts`
- `worker_id`
- `outcome`
- `duration_ms`
- `error` (truncated, no payload)

API logs include `request_id`, method, route pattern, status, and duration. Do not
log payloads, results, or full DSNs.

### D-8.2 Metrics

Initial metric set:

| Metric | Labels |
|---|---|
| `gofari_jobs_enqueued_total` | `target_kind`, `result` (`new`/`replay`) |
| `gofari_jobs_claimed_total` | `target_kind` |
| `gofari_job_executions_total` | `target_kind`, `outcome` |
| `gofari_job_duration_seconds` | `outcome` |
| `gofari_lease_recoveries_total` | `outcome` |
| `gofari_worker_in_flight` | none |
| `gofari_http_requests_total` | `route`, `method`, `code` |

Do **not** use `job_id`, `target_name`, `worker_id`, or arbitrary client strings
as metric labels. `target_name` is client-controlled and unbounded. Logs and
traces may carry it; metrics may not until an allowlist exists.

### D-8.3 Tracing

- Instrument HTTP, PostgreSQL, queue operations, and execution.
- At enqueue, store the current W3C `traceparent` in `jobs.trace_parent`
  (honoring an incoming client `traceparent` header).
- At claim, extract the stored context and start a `job.execute` span with a
  **link** to the enqueue span, not a child: execution may happen much later.
- Telemetry is best effort. An unavailable collector must not block job
  processing.
- `tracestate` and baggage are deferred; `traceparent` is sufficient for v1.
- An invalid incoming `traceparent` is ignored and a new trace is started.

---

## 9. Testing and CI

### D-9.1 Test layers

- **Unit:** statuses, config validation, executor classification, backoff with
  fake clock/random source, registry, API handlers with a fake store.
- **Contract:** one lifecycle suite run against both `queue/memory` and
  `postgres`. Memory must emulate claims, attempts, tokens, cancellation, and
  retry terminal rules closely enough for the suite to be meaningful.
- **PostgreSQL integration:** build tag `integration`, `TEST_DATABASE_URL`
  required. Migrate to latest, truncate `jobs` between tests. Cover:
  - concurrent claimers: `N` workers claim `M` jobs, no job claimed twice
  - stale lease token cannot finalize
  - expired lease recovery (queued, failed on final attempt, cancelled)
  - cancellation while queued and while running
  - retry exhaustion and manual retry budget reset
  - idempotency: concurrent same key, replay, payload mismatch
  - listing cursor and retention deletion
- **Worker:** success, retryable failure, permanent failure, panic, unknown task,
  timeout, cancellation, stale completion, bounded concurrency, SIGTERM.
- **HTTP:** validation, status codes, idempotency mapping, body limits, readiness,
  request IDs.

The in-memory queue is a test double with real lease/token semantics, not a
production backend. Do not add features to it that PostgreSQL does not have.

Integration tests run serially against one database and truncate `jobs` between
tests. Per-test schemas or parallel databases are deferred.

### D-9.2 CI

GitHub Actions must run:

```bash
gofmt -l .            # fail if any output
go vet ./...
go mod tidy           # then: git diff --exit-code go.mod go.sum
go test ./...
go test -race ./...
go test -race -tags=integration ./...   # with TEST_DATABASE_URL
```

Use a `postgres:17` service container matching `docker-compose.yml`. Integration
tests skip locally when `TEST_DATABASE_URL` is unset; CI must set it.

---

## 10. Operations

### D-10.1 Retention

- Implemented in milestone 7, owned by the worker maintenance loop.
- Disabled by default; enabling destructive cleanup must be explicit.
- Deletes terminal jobs older than `GOFARI_RETENTION_AGE`, in batches:
  `DELETE ... WHERE id IN (SELECT id ... ORDER BY finished_at LIMIT ...)`.
- Multiple workers may run it safely.
- `job_events` (when added) cascades on delete.
- Deleting a job frees its idempotency key; document this to API users.

### D-10.2 Compose and local start

- Keep Compose to PostgreSQL until milestone 6, then add API/worker services.
- Use `${VAR:-default}` interpolation so a missing `.env` does not create a
  database with empty credentials.
- `.env.example` now includes `GOFARI_DATABASE_URL` in addition to the
  `POSTGRES_*` variables used by Compose. Keep the duplicated password in sync.
- README documents: copy env file, `docker compose up -d`, run migrate, start API
  and worker, submit a job, inspect it.

### D-10.3 Authentication

Deferred, but the API must not be internet-exposed without it. Loopback binding
plus a trusted network is the Version 1 boundary. Authentication will be
middleware, not a rewrite of handlers.

### D-10.4 Dead letters

Deferred. Terminal `failed` rows are the dead-letter record in Version 1, with
manual retry available through the API until retention deletes them.

---

## 11. `PLAN.md` alignment (applied)

These changes are now reflected in `PLAN.md`. Use this list as review criteria for
future plan edits.

- Replace the package tree and responsibilities with D-1.3; remove the
  `internal/jobs`/`internal/worker` split and the “reusable core” contradiction.
- State that `attempts` increments only at claim; remove the retry increment.
- Replace the cancellation section with the persisted `cancel_requested_at` +
  heartbeat watchdog design (D-5).
- Move lease renewal from milestone 7 to the lease/claim milestone. It is
  required for correctness, not hardening.
- Move execution timeouts and payload/body limits out of milestone 7. They belong
  with the worker and the first HTTP API version respectively.
- State that Version 1 rejects `workflow` targets at the API.
- Specify idempotency replay (`200`/`201`/`409`) and the partial unique index.
- Add `cancel_requested_at` and `trace_parent` to the schema.
- Replace “malformed payload” with “invalid JSON is rejected at the API;
  task-level payload validation is a permanent executor failure.”
- Clarify that unknown task types fail permanently, so producers must not deploy
  ahead of workers.
- Change log field `type` to `target_name`.
- Change acceptance “at most one active claim” to “at most one valid lease; a
  stale worker cannot finalize, but execution may overlap after lease expiry.”
- Remove workflow-intent language unless a workflow table/definition is added.
  The database accepts the enum value; the product does not execute it.
- Link to this document for the resolved details.
- Rename or map “Version 1–4” to the seven milestones. Versions 1–3 are learning
  stages inside milestone 4; Version 4 spans milestones 2–3.
- Add the missing `internal/worker` or remove it from the tree (D-1.3 removes it).
- Add `GOFARI_DATABASE_URL` to `.env.example` and the Compose/README steps.
- Replace the implied “API → queue service; worker → queue repository” split
  with one `queue.Store` interface; avoid a premature service/repository layer.
- State that Version 1 uses the final `target_kind`/`target_name`/`payload_version`
  fields, not a temporary `type`.
- Note that containers must set `GOFARI_API_ADDR=0.0.0.0:8080` (and the worker
  equivalent) because the local default binds to loopback.

---

## 12. Version 1 implementation slice

After applying D-1.1 and D-1.3, Version 1 should contain only:

```text
jobs/
  job.go
  status.go
  job_test.go

queue/
  store.go            # minimal contract: Enqueue, Claim, Succeed, Fail
  memory/
    memory.go
    memory_test.go

executor/
  executor.go
  errors.go
  registry.go
  test_success.go
  executor_test.go

worker/
  worker.go           # ProcessOne(ctx) (bool, error)
  worker_test.go
```

Behavior:

```text
Given a queued test_success job
When Worker.ProcessOne is called
Then it returns processed=true
And the job status is succeeded
And the result contains the expected value
```

Use final field names now. Keep the memory store mutex-protected. Version 2 adds
deterministic failure executors; Version 3 adds concurrency/race tests. PostgreSQL
arrives at milestone 2 with the contract suite from D-9.1.

`ProcessOne` returns `(processed bool, err error)` so callers can distinguish “no
eligible job” from a processing error.

---

## 13. Product choices in this contract

These are product choices rather than pure correctness fixes. They are reflected
in `PLAN.md` and the code should follow them. Change this file and the plan
together if any need to change.

1. **Public core layout** (D-1.3/D-1.4) rather than keeping everything internal.
2. **Idempotency payload mismatch returns `409`** rather than returning the
   existing job silently (D-4.10).
3. **Workflow targets are rejected with `422`** in Version 1 (D-2.3).
4. **Cancelled jobs cannot be manually retried**; enqueue a new job (D-4.11).
5. **Clients may set `max_attempts`**, capped at 100 (D-6.2).
6. **Goose** for migrations (D-4.1).
7. **Unknown task names fail permanently** (D-3.4).
8. **Shutdown can consume the final attempt** (D-5.2).
