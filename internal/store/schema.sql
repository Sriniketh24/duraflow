-- Duraflow schema: durable workflow engine.
-- Append-only event history + leased activity tasks backed by PostgreSQL.
-- All worker dequeues use SELECT ... FOR UPDATE SKIP LOCKED for safe concurrency.

CREATE TABLE IF NOT EXISTS workflow_runs (
    id              UUID PRIMARY KEY,
    workflow_name   TEXT        NOT NULL,
    input           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running','completed','failed','cancelled')),
    current_step    INTEGER     NOT NULL DEFAULT 0,
    total_steps     INTEGER     NOT NULL DEFAULT 0,
    -- Idempotency: starting a workflow with an existing key returns the existing run.
    idempotency_key TEXT        UNIQUE,
    result          JSONB,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per scheduled activity (or durable timer) execution.
CREATE TABLE IF NOT EXISTS activity_tasks (
    id               UUID PRIMARY KEY,
    run_id           UUID        NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_index       INTEGER     NOT NULL,
    activity_name    TEXT        NOT NULL,
    kind             TEXT        NOT NULL DEFAULT 'activity'
                         CHECK (kind IN ('activity','timer')),
    input            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT        NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','leased','completed','failed','dead','cancelled')),
    attempt          INTEGER     NOT NULL DEFAULT 0,
    max_attempts     INTEGER     NOT NULL DEFAULT 5,
    -- available_at drives both retry backoff and durable timers: a task is only
    -- eligible to be leased once now() >= available_at.
    available_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Visibility timeout: a leased task whose lease_expires_at has passed is
    -- reclaimed by the reaper (covers crashed workers).
    lease_expires_at TIMESTAMPTZ,
    leased_by        TEXT,
    result           JSONB,
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, step_index)
);

-- Hot-path index for the dequeue query (pending + due, ordered by availability).
CREATE INDEX IF NOT EXISTS idx_activity_tasks_dequeue
    ON activity_tasks (available_at)
    WHERE status = 'pending';

-- Reaper index for reclaiming expired leases.
CREATE INDEX IF NOT EXISTS idx_activity_tasks_reap
    ON activity_tasks (lease_expires_at)
    WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS idx_activity_tasks_run ON activity_tasks (run_id);

-- Append-only, replayable event history. Never updated, only inserted.
CREATE TABLE IF NOT EXISTS events (
    id         BIGSERIAL PRIMARY KEY,
    run_id     UUID        NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_id    UUID,
    type       TEXT        NOT NULL,
    payload    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_run ON events (run_id, id);
