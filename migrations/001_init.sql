-- +goose Up
CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    target_kind text NOT NULL CHECK (target_kind IN ('task', 'workflow')),
    target_name text NOT NULL,
    payload jsonb NOT NULL,
    payload_version integer NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    idempotency_key text,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 100),
    run_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    lease_token uuid,
ransaction   worker_id text,
    cancel_requested_at timestamptz,
    last_error text,
    result jsonb,
    trace_parent text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'running') =
        (lease_token IS NOT NULL AND lease_until IS NOT NULL AND worker_id IS NOT NULL)),
    CHECK ((finished_at IS NOT NULL) =
        (status IN ('succeeded', 'failed', 'cancelled'))),
    CHECK (cancel_requested_at IS NULL OR status IN ('running', 'cancelled')),
    CHECK (result IS NULL OR status = 'succeeded')
);

CREATE INDEX jobs_claimable_idx ON jobs (run_at, created_at, id) WHERE status = 'queued';
CREATE INDEX jobs_expired_lease_idx ON jobs (lease_until) WHERE status = 'running';
CREATE UNIQUE INDEX jobs_idempotency_uidx
    ON jobs (target_kind, target_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_listing_idx ON jobs (status, created_at DESC, id DESC);
CREATE INDEX jobs_retention_idx ON jobs (finished_at, id)
    WHERE status IN ('succeeded', 'failed', 'cancelled');

-- +goose Down
DROP TABLE jobs;
