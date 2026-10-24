# Gofari Implementation Plan

## Product Scope

Gofari will be both a reusable Go core and a deployable API/worker service. It will accept generic JSON jobs, persist them in PostgreSQL, provide at-least-once delivery with lease recovery and retries, and expose a pluggable executor interface for future Python support.

## Queue Domain Model

Gofari distinguishes reusable work definitions from individual executions:

- **Task**: a registered, reusable unit of work with a name, input contract, and
  executor implementation.
- **Workflow**: a named ordered composition of Tasks. Workflows are part of the
  domain model now, but durable multi-step execution is deferred until the
  single-task lifecycle is proven.
- **Job**: a persisted request to execute one Task or Workflow. It contains the
  target, generic versioned input, scheduling information, retry policy, and
  lifecycle state.
- **Executor registry**: an explicit in-process mapping from task names to Go
  implementations. Task definitions are not dynamically loaded from PostgreSQL.

The first implementation uses the existing generic `type` and `payload` idea as
the task target and input. The model should evolve toward an explicit target kind
(`task` or `workflow`) and target name without requiring every workflow capability
in the first working version.

### Queue decisions

- Use one default queue first. Named queues and priority routing are later
  extensions, not initial requirements.
- Support one-time scheduling with `run_at`. Recurring schedules are out of
  scope for the first release.
- Use polling as the baseline delivery mechanism. PostgreSQL notifications may
  reduce idle latency later, but they are not the durable delivery mechanism.
- Cancel cooperatively: queued jobs should not start after cancellation, and
  running executors should observe context cancellation. The system will not
  attempt unsafe forced termination.
- Keep completed, failed, and cancelled jobs according to configurable retention
  and cleanup policy.

### Workflow scope

The first workflow-capable design records workflow intent but implements the
single-task path first. Later workflow execution will run ordered tasks and stop
at the failed step, retrying the Job according to its policy. Durable per-step
resume, compensation tasks, and continuing after a failed step are deferred until
the basic lifecycle is stable.

## Requirements

### Functional

- Accept generic JSON jobs through an HTTP API.
- Persist jobs durably in PostgreSQL.
- Support job types, payload versions, scheduled execution, and idempotency keys.
- Represent both Task and Workflow targets while implementing single-task
  execution first.
- Atomically claim jobs across multiple workers.
- Track attempts, leases, failures, and execution timestamps.
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

### Non-functional

- At-least-once delivery; executors must be idempotent.
- Accepted jobs must survive API, worker, and database restarts.
- Concurrent workers must not hold the same active lease.
- State transitions must be transactional.
- Configuration must be environment-based.
- Logs must include job ID, type, attempt, worker, and outcome.
- OpenTelemetry tracing should cover API, database, queue, and execution operations.
- CI must run formatting, tests, race checks, vet, and PostgreSQL integration tests.

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
                         │ - inspect/control  │
                         └─────────┬──────────┘
                                   │ SQL transaction
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                              │
│                                                                 │
│  jobs                                                           │
│  - queued / running / succeeded / failed / cancelled            │
│  - attempts, lease_until, lease_token                           │
│  - payload, result, errors                                      │
└──────────────┬──────────────────────────────────┬───────────────┘
               │ claim / acknowledge / retry      │ status queries
               ▼                                  ▼
      ┌────────────────┐                 ┌────────────────────┐
      │ Worker process │                 │ API status/control │
      │                │                 │ endpoints          │
      │ - poll queue   │                 └────────────────────┘
      │ - claim lease  │
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
2. The API validates the request and inserts a `queued` job in PostgreSQL.
3. A worker claims an eligible job in a short transaction using
   `FOR UPDATE SKIP LOCKED`.
4. The claim increments the attempt count and records a lease expiration,
   lease token, and worker identity.
5. The worker resolves the job type through the executor registry and runs it.
6. The worker acknowledges success, records a permanent failure, or schedules a
   retry with backoff and jitter.
7. If a worker crashes or loses its connection, lease expiry makes the job
   claimable again.
8. API status and control endpoints read or update the persisted job state.

### Lease and stale-worker protection

Worker state updates must include both the job ID and lease token. A worker that
finishes after its lease has expired cannot overwrite the newer claim because
acknowledgment, failure, and retry updates require the current lease token.

At-least-once delivery means a job may execute more than once. Executors must
therefore be idempotent, especially when lease expiry or worker recovery causes
duplicate execution.

### Package boundaries

The proposed Go layout is:

```text
cmd/
  api/
  worker/
  migrate/

internal/
  config/
  database/
  jobs/
  queue/
  executor/
  httpapi/
  observability/

migrations/
```

Responsibilities are separated as follows:

- `jobs`: domain types, statuses, errors, and public contracts.
- `queue`: claim, acknowledge, retry, cancel, and lease-recovery operations.
- `database`: PostgreSQL connection management and migrations.
- `executor`: executor interface, registry, and adapters.
- `httpapi`: request validation, handlers, consistent errors, and request IDs.
- `worker`: polling, bounded concurrency, panic recovery, cancellation, and
  graceful shutdown.
- `cmd/*`: process entry points and configuration wiring.

The key contracts are:

```text
API → queue service
worker → queue repository
worker → executor registry
queue repository → PostgreSQL
```

Milestone 1 defines these contracts and their state-transition rules before
PostgreSQL-specific persistence is implemented.

## Learning-First Implementation Path

The full architecture is the destination, not the first coding task. Development
should begin with a small in-memory version that proves the core job lifecycle
before introducing PostgreSQL, HTTP, retries, or external services.

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

The initial job model needs only:

- `id`
- `type`
- generic JSON `payload`
- `status`
- `result`
- `error`

Use `json.RawMessage` for generic JSON payloads. The first worker operation should
be a testable `ProcessOne(ctx)` function that claims and processes one job rather
than a permanent polling loop.

Suggested initial layout:

```text
jobs/
  job.go
  job_test.go

queue/
  memory.go
  memory_test.go

executor/
  executor.go
  test_success.go
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
the retry state transition is correct.

### Version 3: Multiple jobs and concurrency

Process several jobs, then protect shared in-memory state with a mutex. Run the
race detector:

```bash
go test -race ./...
```

Add polling, multiple workers, context cancellation, and graceful shutdown only
after `ProcessOne(ctx)` is understandable and tested.

### Version 4: PostgreSQL persistence

Replace the in-memory queue with PostgreSQL while preserving the same queue and
executor behavior. The database implementation should satisfy the contracts
already exercised by the in-memory tests.

### Deferred concerns

These belong to later milestones and should not block the first working version:

- HTTP API
- OpenTelemetry and metrics
- authentication
- Python execution
- email and webhook providers
- lease renewal
- dead-letter queues
- Docker integration tests
- complex scheduling

## Milestones

### 1. Contracts and foundation

- Define configuration, public Go interfaces, statuses, errors, executor interface, and shutdown behavior.
- Add API, worker, and migration command entry points.
- Add unit tests for the public contracts.

Acceptance: `go test ./...` passes and the public contracts are stable.

### 2. PostgreSQL persistence

- Add migrations and connection pooling.
- Implement job insertion and retrieval.
- Add indexes, idempotency constraints, and database health checks.
- Add an explicit migration command.

Acceptance: a clean PostgreSQL instance can be migrated and store/retrieve jobs correctly.

### 3. Queue claiming and recovery

- Implement atomic claiming with `FOR UPDATE SKIP LOCKED`.
- Add leases and lease tokens.
- Implement acknowledgment, failure, cancellation, manual retry, and expired-lease recovery.
- Test concurrent claimers and late workers.

Acceptance: each job has at most one active claim and crashed-worker jobs become claimable again.

### 4. Retries and workers

- Implement retry classification and backoff.
- Add bounded worker concurrency, panic recovery, cancellation, and graceful shutdown.
- Add a deterministic test executor.

Acceptance: successful, retryable, permanent, panicking, and cancelled jobs reach the expected state.

### 5. HTTP API

- Add enqueue, status, cancel, retry, liveness, readiness, and metrics routes.
- Define consistent JSON errors, validation, and request IDs.
- Keep authentication as a middleware extension point.

Acceptance: API integration tests cover normal and invalid requests.

### 6. Observability and operations

- Add structured logs and queue, worker, retry, latency, and API metrics.
- Add configurable OpenTelemetry tracing.
- Update Compose and `.env.example`.
- Document local and production operation.

Acceptance: a developer can start PostgreSQL, migrate, run API and worker processes, submit a job, and observe its lifecycle.

### 7. CI and hardening

- Add GitHub Actions for formatting, tests, race checks, vet, and PostgreSQL integration tests.
- Add retention cleanup, payload limits, execution timeouts, and lease renewal.

Acceptance: CI reproduces the documented local verification commands.

## Database Design

### `jobs`

| Column | Type | Purpose |
|---|---|---|
| `id` | `uuid` | Stable job identifier |
| `target_kind` | `text` | `task` or `workflow`; single-task execution comes first |
| `target_name` | `text` | Registered task or workflow name |
| `payload` | `jsonb` | Generic job input |
| `payload_version` | `integer` | Payload compatibility version |
| `status` | `text` | `queued`, `running`, `succeeded`, `failed`, `cancelled` |
| `idempotency_key` | `text` | Optional duplicate submission key |
| `attempts` | `integer` | Number of claims/executions |
| `max_attempts` | `integer` | Per-job retry limit |
| `run_at` | `timestamptz` | Earliest eligible execution time |
| `lease_until` | `timestamptz` | Worker lease expiration |
| `lease_token` | `uuid` | Prevents stale worker updates |
| `worker_id` | `text` | Current worker identity |
| `last_error` | `text` | Most recent failure |
| `result` | `jsonb` | Optional execution result |
| `created_at` | `timestamptz` | Submission time |
| `started_at` | `timestamptz` | First execution time |
| `finished_at` | `timestamptz` | Terminal completion time |
| `updated_at` | `timestamptz` | Last state change |

For the learning-first in-memory version, `type` may remain a single field. The
PostgreSQL model should use `target_kind` and `target_name` once workflows are
represented explicitly.

Recommended constraints:

- `attempts >= 0`
- `max_attempts > 0`
- Valid status values
- Unique `(target_kind, target_name, idempotency_key)` when the key is present

Recommended indexes:

```sql
CREATE INDEX jobs_claimable_idx
ON jobs (run_at, created_at)
WHERE status = 'queued';

CREATE INDEX jobs_expired_lease_idx
ON jobs (lease_until)
WHERE status = 'running';

CREATE INDEX jobs_status_created_idx
ON jobs (status, created_at);
```

### Optional `job_events`

An append-only audit table can record state changes and attempt history without overloading the `jobs` row:

- `id` (`bigserial`)
- `job_id` (`uuid`)
- `event_type` (`text`)
- `attempt` (`integer`)
- `metadata` (`jsonb`)
- `created_at` (`timestamptz`)

## State Transitions

```text
queued -> running
running -> succeeded
running -> queued       retryable failure
running -> failed       permanent failure or retry exhaustion
running -> cancelled
running -> queued       expired lease recovery
queued  -> cancelled
failed  -> queued       manual retry
```

Worker updates must include both the job ID and lease token so a late worker cannot overwrite a newer claim.

## Failure Scenarios

| Failure | Expected behavior |
|---|---|
| API crashes after commit | Client may retry; idempotency key prevents duplicate logical jobs |
| API crashes before commit | No job is accepted; client retries |
| Worker crashes during execution | Lease expires and job is queued again |
| Worker loses database connection | Lease expiry enables recovery |
| Worker finishes after lease expiry | Lease token rejects stale updates |
| Retryable executor error | Increment attempt and schedule a future retry |
| Permanent executor error | Mark job failed immediately |
| Retry limit reached | Mark terminal failed state; defer dead-letter storage and replay |
| Malformed payload | Permanent failure; do not retry indefinitely |
| Unknown job type | Permanent failure until an adapter is available |
| Worker panic | Recover panic and apply retry policy |
| Cancellation | Cancel context and persist cancellation or retry according to policy |
| Long-running job | Renew lease or enforce execution timeout |
| Poison job | Backoff and terminal failure prevent hot loops |
| Database restart | Services reconnect; queued jobs remain durable |
| Duplicate submission | Return the existing job for the idempotency key |
| Telemetry backend unavailable | Job processing continues; telemetry is best effort |

## Architectural Comparison

| Decision | Selected approach | Alternative and tradeoff |
|---|---|---|
| Queue storage | PostgreSQL | Redis/RabbitMQ/Kafka scale better but add infrastructure and consistency boundaries |
| Service shape | Separate API and worker | One process is simpler but couples failures and scaling |
| Claiming | Polling with optional notifications | `LISTEN/NOTIFY` lowers idle latency but is not durable delivery |
| Execution | Pluggable executor interface | Direct subprocess execution is easy but introduces security and resource risks |
| SQL access | Explicit SQL for state transitions | ORM reduces CRUD but can hide locking behavior |
| Migrations | Explicit command | Startup migration is convenient locally but risky with multiple instances |

## Design Review

### Strengths

- PostgreSQL provides durable state and transactional queue transitions.
- Lease tokens protect against stale-worker races.
- At-least-once delivery is realistic and explicitly documented.
- The executor boundary allows Python support without coupling persistence to Python.
- Separate API and worker processes can scale independently.
- Integration tests can exercise real concurrency and recovery behavior.

### Risks and mitigations

- PostgreSQL may become a throughput bottleneck. Use short transactions, proper indexes, batching, cleanup, and load testing.
- Lease expiry can cause duplicate execution. Require idempotent executors, renew leases, and enforce timeouts.
- Large JSON payloads increase storage and contention. Enforce limits and later support object-storage references.
- External executors can become code-execution vulnerabilities. Isolate them, authenticate submissions, and apply resource limits.
- No initial authentication is safe only for trusted development. Add authentication before internet exposure.
- Terminal jobs can grow without bound. Add configurable retention and cleanup before production use; add dead-letter replay later.
- Job IDs must not become metric labels. Keep IDs in logs/traces and use bounded labels in metrics.

## Verification

1. Start PostgreSQL with Compose and run the explicit migration command against an empty volume.
2. Run `gofmt` checks, `go test ./...`, `go test -race ./...` where supported, and `go vet ./...`.
3. Test concurrent claims, idempotency, lease recovery, lease-token protection, cancellation, and retry exhaustion.
4. Test worker success, retries, permanent errors, malformed jobs, panic recovery, cancellation, bounded concurrency, and SIGTERM shutdown.
5. Test HTTP validation, enqueue/status/control routes, health/readiness, metrics, and trace/log correlation.
6. Manually run the API-to-worker flow with the test executor and verify persisted terminal state.

## Decisions

- Deliverable: reusable core packages plus deployable API, worker, and migration commands.
- Job contract: generic externally submitted JSON.
- Delivery: at-least-once; executors must be idempotent.
- Execution: pluggable interface; Python support comes later.
- API: enqueue, inspection, cancellation, manual retry, health, readiness, and metrics.
- Authentication: omitted initially for trusted development; required before production exposure.
- Migrations: explicit, not automatic at service startup.
- Observability: structured logs, metrics, and configurable OpenTelemetry tracing.
- Queue model: one default queue first; named queues and priority routing are deferred.
- Scheduling: one-time `run_at` scheduling first; recurring schedules are deferred.
- Domain model: Tasks and Workflows are explicit concepts, but single-task execution is implemented first.
- Workflow behavior: later workflows execute ordered steps and stop/retry at the failed step; durable step resume and compensation are deferred.
- Cancellation: cooperative cancellation through context propagation.
- Retention: configurable cleanup for terminal jobs.
- Dead letters: terminal failed records first; separate dead-letter storage and replay tooling later.
- Out of scope for the first release: exactly-once effects, distributed scheduling, authentication implementation, and Python execution itself.

## Reference Material

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
