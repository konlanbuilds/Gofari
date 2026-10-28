# Gofari Implementation Plan

Gofari will be both a reusable Go core and a deployable API/worker service. It will
accept generic JSON jobs, persist them in PostgreSQL, provide at-least-once
delivery with lease recovery and retries, and expose a pluggable executor
interface for future Python support.

Resolved implementation details are recorded in
[DECISIONS.md](./DECISIONS.md). If this plan and that document disagree,
`DECISIONS.md` is authoritative.

## Product Scope

Gofari provides:

- public domain and queue packages that another Go module can import;
- an HTTP API for enqueue, inspection, cancellation, and manual retry;
- a worker process that claims jobs and runs registered Go executors;
- an explicit migration command;
- durable state, leases, retries, and recovery in PostgreSQL.

The first release executes single tasks. Workflow execution, Python adapters,
authentication, and dead-letter replay are later milestones.

## Queue Domain Model

Gofari distinguishes reusable work definitions from individual executions:

- **Task**: a registered, reusable unit of work with a name, input contract, and
  executor implementation.
- **Workflow**: a reserved named composition of Tasks. Workflows are represented
  as a target kind for forward compatibility but are not executable in the first
  release.
- **Job**: a persisted request to execute one Task or Workflow. It contains the
  target, generic versioned input, scheduling information, retry policy, and
  lifecycle state.
- **Executor registry**: an explicit in-process mapping from task names to Go
  implementations. Task definitions are not dynamically loaded from PostgreSQL.

The first implementation uses explicit `target_kind` (`task` or `workflow`) and
`target_name` fields from the start, plus a generic versioned `payload`. There is
no temporary `type` field. The database accepts `workflow` for forward
compatibility, but the Version 1 API rejects it until workflow execution exists.

### Queue decisions

- Use one default queue first. Named queues and priority routing are later
  extensions, not initial requirements.
- Support one-time scheduling with `run_at`. Recurring schedules are out of scope
  for the first release.
- Use polling as the baseline delivery mechanism. PostgreSQL notifications may
  reduce idle latency later, but they are not the durable delivery mechanism.
- Cancel cooperatively across processes: queued jobs transition directly to
  `cancelled`. Cancelling a running job records `cancel_requested_at`; the
  worker's heartbeat observes it, cancels the executor context, and finalizes the
  job as `cancelled`. The system never force-kills a process.
- Keep completed, failed, and cancelled jobs according to configurable retention
  and cleanup policy.
- Idempotency keys are optional and scoped to
  `(target_kind, target_name, idempotency_key)`. Replaying a key returns the
  existing job; reusing a key with a different payload or payload version is a
  conflict.

### Workflow scope

Workflows are reserved in the domain model but are not executable in the first
release. There is no workflow definition table, composition language, or workflow
executor yet, so storing a workflow target would only produce a permanently
failing job. The Version 1 API rejects `target_kind = "workflow"` with `422`; the
database permits the value for a future additive migration.

When workflows are implemented, they will run ordered tasks and stop at the
failed step, retrying the Job according to its policy. Durable per-step resume,
compensation tasks, and continuing after a failed step remain deferred.

## Requirements

### Functional

- Accept generic JSON jobs through an HTTP API.
- Persist jobs durably in PostgreSQL.
- Support task targets, payload versions, scheduled execution, and idempotency
  keys.
- Represent both Task and Workflow targets, but reject Workflow targets at the
  Version 1 API.
- Replay idempotent submissions (`200`), create new jobs (`201`), and reject key
  reuse with a different payload/version (`409`).
- Atomically claim jobs across multiple workers.
- Track attempts, leases, failures, and execution timestamps.
- Renew leases while executors run and recover expired leases.
- Retry transient failures with exponential backoff and jitter.
- Mark permanent failures and exhausted retries as terminal.
- Keep retry-exhausted jobs as terminal failed records; defer a separate
  dead-letter queue/table and replay tooling.
- Recover jobs from crashed workers after lease expiry.
- Support inspection, cancellation, and manual retry.
- Provide an executor interface for Python, HTTP, or subprocess adapters later.
- Provide liveness, readiness, and metrics endpoints.
- Support graceful worker shutdown.
- Retain terminal jobs for configurable inspection and cleanup.
- Enforce request body, payload, and result size limits.
- Validate request shape and size at the API, but not task registration: API and
  worker registries may be deployed at different times.

### Non-functional

- At-least-once delivery; executors must be idempotent, keyed by job ID.
- Accepted jobs must survive API, worker, and database restarts.
- At most one valid lease exists per job. A stale worker cannot finalize, but
  execution may overlap after lease expiry, so duplicate execution remains
  possible.
- State transitions must be transactional and guarded by job ID, lease token, and
  current status.
- Configuration must be environment-based.
- Logs must include job ID, target name, attempt, worker, and outcome.
- OpenTelemetry tracing should cover API, database, queue, and execution
  operations, including propagation from enqueue to execution.
- CI must run formatting, tests, race checks, vet, module tidiness checks, and
  PostgreSQL integration tests.

## Runtime Architecture

Gofari has two deployable services that share PostgreSQL:

```text
                         ┌────────────────────┐
                         │      Client        │
                         └─────────┬──────────┘
                                   │ HTTP
                                   ▼
                         ┌────────────────────┐
                         │    API service     │
                         │                    │
                         │ - validate input   │
                         │ - request IDs      │
                         │ - enqueue jobs     │
                         │ - idempotency      │
                         │ - inspect/control  │
                         └─────────┬──────────┘
                                   │ SQL transaction
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                              │
│                                                                 │
│  jobs                                                           │
│  - queued / running / succeeded / failed / cancelled            │
│  - attempts, lease_until, lease_token, worker_id                │
│  - cancel_requested_at, trace_parent                            │
│  - payload, result, errors                                      │
└──────────────┬──────────────────────────────────┬───────────────┘
               │ claim / heartbeat / finalize     │ status queries
               ▼                                  ▼
      ┌────────────────┐                 ┌────────────────────┐
      │ Worker process │                 │ API status/control │
      │                │                 │ endpoints          │
      │ - poll queue   │                 └────────────────────┘
      │ - claim lease  │
      │ - heartbeat    │
      │ - run executor │
      │ - retry/fail   │
      │ - shutdown     │
      └───────┬────────┘
              │ executor interface
              ▼
      ┌────────────────┐
      │ Job executor   │
      │                │
      │ - test adapter │
      │ - future HTTP  │
      │ - future Python│
      └────────────────┘
```

### End-to-end flow

1. A client submits a generic JSON job to the API.
2. The API validates request shape, size, and field constraints, then inserts a
   `queued` job (or returns the existing job for an idempotency key).
3. A worker claims an eligible job in a short transaction using
   `FOR UPDATE SKIP LOCKED`.
4. The claim increments the attempt count and records a lease expiration, lease
   token, and worker identity.
5. The worker resolves the target through the executor registry and runs it.
6. While the executor runs, a heartbeat renews the lease and detects
   cancellation requests.
7. The worker acknowledges success, records a permanent failure, or schedules a
   retry with backoff and jitter.
8. If a worker crashes or loses its connection, lease expiry makes the job
   claimable again.
9. Cancelling a running job records `cancel_requested_at`; the worker observes it
   on its next heartbeat, cancels the executor context, and finalizes the job as
   `cancelled`. Cancellation wins over retry.
10. API status and control endpoints read or update the persisted job state.

### Lease and stale-worker protection

The system guarantees at most one valid lease per job, not at most one execution.
A worker that finishes after its lease has expired cannot overwrite the newer
claim because acknowledgment, failure, retry, and heartbeat updates require both
the job ID and the current lease token and `status = 'running'`.

Workers renew their lease with a heartbeat while an executor runs. The same
heartbeat query returns `cancel_requested_at`, which is how a worker learns about
cancellation requested in another process. If a heartbeat or finalization returns
no row, the worker lost the lease and must discard its result.

At-least-once delivery means a job may execute more than once. Executors must
therefore be idempotent, especially when lease expiry or worker recovery causes
duplicate execution.

### Package boundaries

Gofari is a reusable core plus a deployable service, so domain and queue packages
are public. Service wiring stays under `internal/`.

```text
cmd/
  api/
  worker/
  migrate/

jobs/
queue/
  memory/
executor/
worker/
postgres/
migrations/

internal/
  config/
  httpapi/
  observability/
```

Responsibilities are separated as follows:

- `jobs`: domain types, statuses, errors, and public contracts.
- `queue`: the public `Store` contract for enqueue, claim, acknowledge, retry,
  cancel, and lease recovery.
- `queue/memory`: in-memory test double implementing the same semantics.
- `postgres`: pgx implementation of `queue.Store` and database health checks.
- `executor`: executor interface, registry, error classification, and adapters.
- `worker`: public polling runner, bounded concurrency, panic recovery,
  cooperative cancellation, lease heartbeat, and graceful shutdown.
- `httpapi`: request validation, handlers, consistent errors, and request IDs.
- `config` and `observability`: internal service wiring and instrumentation.
- `cmd/*`: process entry points and configuration wiring.

The key contracts are:

```text
API → queue.Store
worker → queue.Store
worker → executor.Registry
postgres.Store → PostgreSQL
```

There is no separate queue service or repository layer. Milestone 1 defines these
contracts and their state-transition guards before PostgreSQL-specific
persistence is implemented.

## Learning-First Implementation Path

The full architecture is the destination, not the first coding task. Development
should begin with a small in-memory version that proves the core job lifecycle
before introducing PostgreSQL, HTTP, retries, or external services.

Versions 1–3 are learning stages inside Milestone 4 (retries and workers).
Version 4 supplies the persistence exercised by Milestones 2–3. The seven
milestones remain the delivery checklist.

### Version 1: One successful job

Implement only:

```text
in-memory queue
one executor
one worker call
one passing test
```

The first behavior should be:

```text
create job
  → queued
  → claim job
  → running
  → execute test_success
  → succeeded with result
```

Use the final job field names from the start: `id`, `target_kind`, `target_name`,
`payload`, `payload_version`, `status`, `result`, `last_error`, `attempts`, and
`max_attempts`. Version 1 exercises only the success path, but renaming fields
later creates unnecessary churn.

Use `json.RawMessage` for generic JSON payloads. The first worker operation should
be a testable `ProcessOne(ctx)` function that claims and processes one job rather
than a permanent polling loop. It returns `(processed bool, err error)` so callers
can distinguish "no eligible job" from a processing error.

Suggested initial layout:

```text
jobs/
  job.go
  job_test.go

queue/
  store.go
  memory/
    memory.go
    memory_test.go

executor/
  executor.go
  errors.go
  registry.go
  test_success.go

worker/
  worker.go
  worker_test.go
```

The first test should verify behavior:

```text
Given a queued test_success job
When the worker processes it
Then its status is succeeded
And its result contains the expected value
```

### Version 2: Failure states

Add deterministic test executors and verify each transition:

```text
test_permanent_failure  : queued → running → failed
test_retryable_failure  : queued → running → queued
```

Immediate retry is sufficient at this stage. Add backoff and jitter only after
the retry state transition is correct. `attempts` increments on claim, so the next
claim consumes the next attempt; the retry transition itself does not increment
it.

### Version 3: Multiple jobs and concurrency

Process several jobs, then protect shared in-memory state with a mutex. Run the
race detector:

```bash
go test -race ./...
```

Add polling, multiple workers, context cancellation, and graceful shutdown only
after `ProcessOne(ctx)` is understandable and tested.

The in-memory queue is a test double, not a production backend. It must model
claims, attempts, lease tokens, cancellation, and retry terminal rules closely
enough for the shared lifecycle contract tests to be meaningful.

### Version 4: PostgreSQL persistence

Replace the in-memory queue with PostgreSQL while preserving the same queue and
executor behavior. Run the shared lifecycle contract suite against both the memory
store and `postgres.Store`. The database implementation should satisfy the
contracts already exercised by the in-memory tests.

### Deferred concerns

These belong to later milestones and should not block the first working version:

- HTTP API (until Milestone 5)
- OpenTelemetry and metrics (Milestone 6)
- authentication
- workflow execution and durable step resume
- Python execution
- email and webhook providers
- dead-letter queues and replay tooling
- job event audit history
- Docker integration tests
- named queues, priority routing, and complex scheduling
- exactly-once effects

Lease renewal, execution timeouts, and payload limits are not deferred: they are
required for correctness and belong with the worker and first HTTP API
respectively.

## Milestones

### 1. Contracts and foundation

- Define configuration, public Go interfaces, statuses, errors, executor
  interface, and shutdown behavior.
- Record the state-transition guards from [DECISIONS.md](./DECISIONS.md).
- Add API, worker, and migration command entry points.
- Add unit tests for the public contracts.
- Ensure `go.mod` and `go.sum` are tracked and `.gitignore` does not exclude them.

Acceptance: `go test ./...` passes and the public contracts are stable.

### 2. PostgreSQL persistence

- Add embedded migrations and pgx connection pooling.
- Implement job insertion, retrieval, listing, and idempotent replay
  (`200`/`201`/`409`).
- Add the `cancel_requested_at` and `trace_parent` columns, indexes, constraints,
  and database health checks.
- Add an explicit migration command; services never migrate at startup.

Acceptance: a clean PostgreSQL instance can be migrated and store/retrieve jobs
correctly, and concurrent duplicate submissions produce one logical job.

### 3. Queue claiming and recovery

- Implement atomic claiming with `FOR UPDATE SKIP LOCKED`.
- Add leases, lease tokens, and heartbeat lease renewal.
- Implement acknowledgment, failure, cancellation request/finalization, manual
  retry, and expired-lease recovery.
- Test concurrent claimers, stale workers, heartbeat renewal, and late cancel
  requests.

Acceptance: each job has at most one valid lease, a stale worker cannot finalize,
and crashed-worker jobs become claimable again.

### 4. Retries and workers

- Implement retry classification, execution timeouts, and jittered backoff.
- Add bounded worker concurrency, panic recovery, cooperative cancellation, and
  graceful shutdown.
- Add deterministic test executors.

Acceptance: successful, retryable, permanent, panicking, timed-out, and cancelled
jobs reach the expected state.

### 5. HTTP API

- Add enqueue, status, list, cancel, retry, liveness, readiness, and metrics
  routes.
- Reject workflow targets with `422` until workflow execution exists.
- Define consistent JSON errors, validation, request IDs, idempotency semantics,
  body limits, and payload limits.
- Keep authentication as a middleware extension point.

Acceptance: API integration tests cover normal and invalid requests, idempotent
replay, payload mismatch, cancellation, and body/payload limits.

### 6. Observability and operations

- Add structured logs and queue, worker, retry, latency, and API metrics.
- Add configurable OpenTelemetry tracing, including `trace_parent` propagation
  from enqueue to execution.
- Update Compose and `.env.example`.
- Document local and production operation, including loopback vs container bind
  addresses.

Acceptance: a developer can start PostgreSQL, migrate, run API and worker
processes, submit a job, and observe its lifecycle and trace.

### 7. CI and hardening

- Add GitHub Actions for formatting, tests, race checks, vet, module tidiness,
  and PostgreSQL integration tests.
- Add retention cleanup and production security hardening.
- Load-test claiming and lease recovery; document capacity limits.

Acceptance: CI reproduces the documented local verification commands.

## Database Design

### `jobs`

| Column | Type | Purpose |
|---|---|---|
| `id` | `uuid` | Stable job identifier |
| `target_kind` | `text` | `task` or `workflow`; API rejects workflow in v1 |
| `target_name` | `text` | Registered task name; workflow name reserved |
| `payload` | `jsonb NOT NULL` | Generic job input; JSON `null` is allowed |
| `payload_version` | `integer` | Payload compatibility version, default `1` |
| `status` | `text` | `queued`, `running`, `succeeded`, `failed`, `cancelled` |
| `idempotency_key` | `text` | Optional duplicate submission key |
| `attempts` | `integer` | Number of claims; incremented at claim |
| `max_attempts` | `integer` | Per-job retry limit, `1..100` |
| `run_at` | `timestamptz` | Earliest eligible execution time |
| `lease_until` | `timestamptz` | Worker lease expiration; `NULL` unless running |
| `lease_token` | `uuid` | Per-claim token; `NULL` unless running |
| `worker_id` | `text` | Current worker identity; `NULL` unless running |
| `cancel_requested_at` | `timestamptz` | Cooperative cancellation request |
| `last_error` | `text` | Most recent failure |
| `result` | `jsonb` | Optional execution result; only when succeeded |
| `trace_parent` | `text` | W3C trace context from enqueue |
| `created_at` | `timestamptz` | Submission time |
| `started_at` | `timestamptz` | First-ever execution start; survives retries |
| `finished_at` | `timestamptz` | Terminal completion time; `NULL` otherwise |
| `updated_at` | `timestamptz` | Last state change |

The learning-first in-memory version uses these same final names from the start;
there is no temporary `type` field. Workflow values are schema-compatible but not
executable in Version 1.

`gen_random_uuid()` is built into PostgreSQL 17; no extension is required.

### Constraints

- Valid status values: `queued`, `running`, `succeeded`, `failed`, `cancelled`.
- Valid target kinds: `task`, `workflow`.
- `attempts >= 0`.
- `max_attempts BETWEEN 1 AND 100`.
- `(status = 'running') = (lease_token IS NOT NULL AND lease_until IS NOT NULL AND worker_id IS NOT NULL)`.
- `(finished_at IS NOT NULL) = (status IN ('succeeded','failed','cancelled'))`.
- `cancel_requested_at IS NULL OR status IN ('running','cancelled')`.
- `result IS NULL OR status = 'succeeded'`.
- Partial unique `(target_kind, target_name, idempotency_key)` where the key is
  present.

### Indexes

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

### Idempotent enqueue

Enqueue uses `ON CONFLICT (target_kind, target_name, idempotency_key) WHERE
idempotency_key IS NOT NULL DO NOTHING`. If no row is returned, the store selects
the existing job and compares `payload` and `payload_version`:

- same payload and version: return the existing job with `200`;
- different payload or version: `409 Conflict`;
- new job: `201 Created`.

The insert and fallback select run in one transaction.

### Deferred `job_events`

An append-only audit table can record state changes and attempt history later.
Do not add it in Version 1 or Milestone 2. It is an additive migration, and queue
operation signatures must not depend on event insertion.

## State Transitions

```text
queued  -> running      claim; attempts++ and lease assigned
queued  -> cancelled    API cancellation
running -> succeeded    worker success, no cancel request
running -> cancelled    worker observes cancel_requested_at
running -> queued       retryable failure with attempts left
running -> failed       permanent failure or retry exhaustion
running -> cancelled    reaper: expired lease with cancel request
running -> queued       reaper: expired lease with attempts left
running -> failed       reaper: expired lease on final attempt
failed  -> queued       manual retry; attempts reset to 0
```

`attempts` increments only at claim. A retry transition does not increment it.
Cancelled jobs cannot be manually retried in Version 1. Terminal jobs have no
other outgoing transitions.

Every worker finalization and heartbeat includes the job ID and current lease
token and requires `status = 'running'`. This prevents a late worker from
overwriting a newer claim. Cancelling a running job only sets
`cancel_requested_at`; it does not change the status directly.

## Failure Scenarios

| Failure | Expected behavior |
|---|---|
| API crashes after commit | Client may retry; idempotency key prevents duplicate logical jobs |
| API crashes before commit | No job is accepted; client retries |
| Worker crashes during execution | Lease expires and job is queued again |
| Worker loses database connection | Lease expiry enables recovery |
| Worker loses heartbeat/lease | Worker cancels execution, discards the result, and does not finalize |
| Worker finishes after lease expiry | Lease token rejects stale updates |
| Retryable executor error | Increment attempt at claim; schedule a future retry |
| Permanent executor error | Mark job failed immediately |
| Retry limit reached | Mark terminal failed state; defer dead-letter storage and replay |
| Invalid JSON | Rejected by the API with `400` |
| Task-level payload validation fails | Permanent failure; do not retry indefinitely |
| Unknown task | Permanent failure; deploy workers before producers |
| Worker panic | Recover panic and apply retry policy |
| Cancellation while queued | Transition directly to `cancelled`; job never starts |
| Cancellation while running | Set `cancel_requested_at`; heartbeat cancels the context; finalize as `cancelled` |
| Worker dies after cancellation request | Reaper marks the job `cancelled` |
| Long-running job | Heartbeat renews the lease; execution timeout bounds the attempt |
| Worker shutdown | Stop claiming, finish in-flight within grace, then requeue or fail |
| Poison job | Backoff and terminal failure prevent hot loops |
| Database restart | Services reconnect; queued jobs remain durable |
| Duplicate submission | Return the existing job (`200`); different payload/version is `409` |
| Telemetry backend unavailable | Job processing continues; telemetry is best effort |

## Architectural Comparison

| Decision | Selected approach | Alternative and tradeoff |
|---|---|---|
| Queue storage | PostgreSQL | Redis/RabbitMQ/Kafka scale better but add infrastructure and consistency boundaries |
| Service shape | Separate API and worker | One process is simpler but couples failures and scaling |
| Claiming | Polling with optional notifications | `LISTEN/NOTIFY` lowers idle latency but is not durable delivery |
| Execution | Pluggable executor interface | Direct subprocess execution is easy but introduces security and resource risks |
| SQL access | Explicit SQL for state transitions | ORM reduces CRUD but can hide locking behavior |
| Migrations | Explicit command, embedded SQL | Startup migration is convenient locally but risky with multiple instances |
| Cancellation | Persisted `cancel_requested_at` plus heartbeat polling | In-process context cancellation cannot cross API and worker processes |

## Design Review

### Strengths

- PostgreSQL provides durable state and transactional queue transitions.
- Lease tokens plus heartbeats protect against stale-worker races and premature
  reclamation.
- At-least-once delivery is realistic and explicitly documented.
- The executor boundary allows Python support without coupling persistence to
  Python.
- Separate API and worker processes can scale independently.
- Public core packages keep the queue reusable outside the bundled service.
- Integration tests can exercise real concurrency and recovery behavior.

### Risks and mitigations

- PostgreSQL may become a throughput bottleneck. Use short transactions, proper
  indexes, batching, cleanup, and load testing.
- Lease expiry can cause duplicate execution. Require idempotent executors, renew
  leases, and enforce timeouts.
- Large JSON payloads increase storage and contention. Enforce limits and later
  support object-storage references.
- External executors can become code-execution vulnerabilities. Isolate them,
  authenticate submissions, and apply resource limits.
- No initial authentication is safe only for trusted development. Add
  authentication before internet exposure.
- Terminal jobs can grow without bound. Add configurable retention and cleanup
  before production use; add dead-letter replay later.
- Job IDs and client-controlled target names must not become metric labels. Keep
  job IDs in logs/traces and use bounded labels in metrics.
- Unknown tasks fail permanently, so deployment ordering matters: deploy workers
  before producers.
- The memory store and PostgreSQL store can drift. Run the same lifecycle contract
  suite against both.

## Verification

1. Start PostgreSQL with Compose and run the explicit migration command against an
   empty volume.
2. Run `gofmt` checks, `go mod tidy` (no diff), `go test ./...`,
   `go test -race ./...` where supported, and `go vet ./...`.
3. Test concurrent claims, idempotency including payload mismatch, lease recovery,
   heartbeat renewal, lease-token protection, cancellation, and retry exhaustion.
4. Test worker success, retries, permanent errors, invalid payloads, panic recovery,
   execution timeout, cancellation, bounded concurrency, and SIGTERM shutdown.
5. Test HTTP validation, enqueue/status/list/control routes, health/readiness,
   metrics, body/payload limits, and trace/log correlation.
6. Manually run the API-to-worker flow with the test executor and verify persisted
   terminal state.
7. Verify that a workflow target is rejected with `422` and that a running
   cancellation is observed through the heartbeat path.

## Decisions

- Deliverable: reusable public core packages plus deployable API, worker, and
  migration commands.
- Decision record: [DECISIONS.md](./DECISIONS.md) is authoritative when it
  conflicts with this plan.
- Package layout: `jobs`, `queue`, `executor`, `worker`, `postgres`, and
  `migrations` are public; `config`, `httpapi`, and `observability` are internal.
- Job contract: generic externally submitted JSON with `target_kind`,
  `target_name`, and `payload_version`.
- Attempts: `attempts` counts claims; it increments only at claim.
- Delivery: at-least-once; executors must be idempotent.
- Execution: pluggable interface; Python support comes later.
- API: enqueue, inspection, listing, cancellation, manual retry, health,
  readiness, and metrics.
- Idempotency: replay returns `200`, new jobs return `201`, and payload/version
  mismatch returns `409`.
- Authentication: omitted initially for trusted development; required before
  production exposure.
- Storage: pgx/v5 with pgxpool; explicit SQL.
- Migrations: goose with embedded SQL, explicit, not automatic at service startup.
- Observability: structured logs, metrics, and configurable OpenTelemetry tracing
  with `trace_parent` propagation.
- Queue model: one default queue first; named queues and priority routing are
  deferred.
- Scheduling: one-time `run_at` scheduling first; recurring schedules are
  deferred.
- Domain model: Tasks are executable; Workflows are reserved and rejected by the
  Version 1 API.
- Workflow behavior: later workflows execute ordered steps and stop/retry at the
  failed step; durable step resume and compensation are deferred.
- Cancellation: persisted `cancel_requested_at`, heartbeat detection, and
  cooperative context cancellation in the worker.
- Lease renewal: implemented with claims and recovery in Milestone 3, not deferred
  to hardening.
- Retention: configurable cleanup for terminal jobs.
- Dead letters: terminal failed records first; separate dead-letter storage and
  replay tooling later.
- Configuration: `GOFARI_*` environment variables with startup validation.
- Testing: shared lifecycle contract suite against memory and PostgreSQL, plus
  PostgreSQL concurrency tests.
- Out of scope for the first release: exactly-once effects, distributed
  scheduling, authentication implementation, workflow execution, and Python
  execution itself.

## Reference Material

- [Gofari Design Decisions](./DECISIONS.md) records the resolved implementation
  contract, SQL semantics, API details, and configuration defaults.
- [Payload Jobs and Queues](https://payloadcms.com/docs/jobs-queue/jobs) informed
  the Task/Workflow/Job distinction, scheduling, queue selection, cancellation,
  and job inspection concepts.
- [Task Queues - System Design](https://www.geeksforgeeks.org/system-design/task-queues-system-design/)
  informed the producer, durable queue, worker, acknowledgment, and retry model.
- [Dead Letter Queue - System Design](https://www.geeksforgeeks.org/system-design/dead-letter-queue-system-design/)
  informed the decision to make retry exhaustion observable and to defer
  dedicated dead-letter storage and replay tooling.
- [Queues and Task Scheduling](https://medium.com/@bit_by_bit/queues-and-task-scheduling-80917ce58edb)
  was supplied as design reference; it was not directly fetched because the site
  returned HTTP 403 during review.
