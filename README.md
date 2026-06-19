# Duraflow

A **durable workflow engine** in Go, backed by PostgreSQL. Submit a multi-step
workflow and Duraflow runs it to completion exactly as defined — surviving
worker crashes, retrying transient failures with backoff, firing durable timers,
dead-lettering poison tasks, and never losing or double-completing a step.

It is the kind of system that sits under "run this reliably in the background"
at any backend company: a small, honest cousin of Temporal / AWS Step Functions,
built to demonstrate the distributed-systems fundamentals — leased execution,
visibility timeouts, idempotency, at-least-once delivery, and crash recovery.

> Status: built, tested (race detector + crash-injection integration tests),
> benchmarked, and deployed. See [Benchmarks](#benchmarks) and [Live demo](#live-demo).

---

## Why it's interesting

Most "job queue" projects stop at "push a job, pop a job." Duraflow proves the
hard parts that interviews actually probe:

- **Durable execution** — every state transition is an append-only event in
  PostgreSQL, so a run's full history can be replayed and the system can be
  killed at any instant without losing work.
- **Leased workers with visibility timeouts** — tasks are claimed with
  `SELECT … FOR UPDATE SKIP LOCKED`; if a worker dies mid-task, its lease
  expires and a reaper returns the task to the queue.
- **At-least-once + lease fencing** — a reclaimed task may run twice, but a
  stale worker's commit is fenced out by lease ownership, so a step is never
  *advanced* twice. Activities are expected to be idempotent.
- **Retries with exponential backoff + full jitter**, then **dead-lettering**.
- **Durable timers** — a workflow can sleep for a duration that outlives any
  process restart, because the timer is just a row with a future `available_at`.
- **Graceful drain** — on `SIGTERM` the engine stops leasing and lets in-flight
  tasks finish before exiting.

The crash-recovery guarantee isn't asserted in prose — it's enforced by an
integration test that **injects real worker crashes via lease expiry** and
proves every workflow still completes exactly once. See
[`internal/engine/integration_test.go`](internal/engine/integration_test.go)
(`TestChaosZeroLoss`).

---

## Architecture

```
                 ┌─────────────┐      POST /v1/workflows/{name}/runs
   client ──────▶│  HTTP API   │──────────────────────────────────┐
                 │  (net/http) │                                   │
                 └─────────────┘                                   ▼
                        ▲                              ┌───────────────────────┐
        GET /v1/runs…   │                              │      PostgreSQL       │
        /history /dlq   │                              │  workflow_runs        │
                        │                              │  activity_tasks  ◀──┐ │
                 ┌──────┴───────┐   lease (SKIP LOCKED)│  events (history)   │ │
                 │   Engine     │─────────────────────▶│                     │ │
                 │ worker pool  │   advance / retry /  └─────────────────────┘ │
                 │  + reaper    │   dead-letter (txn)                          │
                 └──────┬───────┘                                              │
                        │ reaper reclaims expired leases ──────────────────────┘
                        ▼
              registered activities
        (order pipeline, timers, retries…)
```

- **`internal/workflow`** — domain model: a `Definition` is an ordered list of
  `Step`s (an activity, or a durable timer); a `Registry` holds definitions and
  activity handlers.
- **`internal/store`** — the `Store` interface and its PostgreSQL implementation.
  Every multi-row transition (start run, advance step, retry, dead-letter) is a
  single transaction. The hot-path dequeue is `FOR UPDATE SKIP LOCKED`.
- **`internal/engine`** — the worker pool: lease → execute → advance/retry/DLQ,
  plus the reaper and graceful drain.
- **`internal/api`** — stdlib `net/http` REST API (Go 1.22+ method-pattern mux)
  with CORS, structured logging, panic recovery, and Prometheus `/metrics`.
- **`internal/metrics`** — Prometheus collectors.
- **`cmd/duraflow`** — server entrypoint. **`cmd/bench`** — benchmark harness.
- **`dashboard/`** — a Next.js operations console (runs, history timeline, DLQ
  replay, live stats), deployed to Vercel.

### Delivery-guarantee semantics

| Property | How it's achieved |
|---|---|
| At-least-once execution | A task is only marked complete after the handler returns; a crash before commit leaves it leased, and the reaper requeues it once the lease expires. |
| No double *advance* | `UPDATE … WHERE status='leased' AND leased_by=$worker` — a stale lease's commit affects 0 rows and is discarded. |
| Idempotent starts | `workflow_runs.idempotency_key` is unique; re-starting with the same key returns the existing run. |
| Ordered steps | Step *i*'s successful completion and the scheduling of step *i+1* happen in the same transaction. |
| Crash recovery | Visibility timeout (`lease_expires_at`) + reaper. Recovery time is bounded by the lease duration + reap interval. |
| Poison tasks | After `max_attempts`, a task is dead-lettered and its run is failed; the DLQ supports replay. |

---

## Quickstart

```bash
# 1. Start Postgres + the engine
docker compose up --build          # API on :8080, Postgres on :55432

# …or run against your own Postgres
export DATABASE_URL='postgres://user:pass@localhost:5432/duraflow?sslmode=disable'
make run

# 2. Start a workflow run
curl -XPOST localhost:8080/v1/workflows/order_processing/runs \
  -H 'Content-Type: application/json' \
  -d '{"input":{"order_id":"A1"}}'

# 3. Watch it
curl localhost:8080/v1/runs                 # list
curl localhost:8080/v1/runs/<id>            # run + per-step tasks
curl localhost:8080/v1/runs/<id>/history    # append-only event history
curl localhost:8080/v1/stats                # aggregate counts
curl localhost:8080/metrics                 # Prometheus
```

### Built-in demo workflows

| Workflow | Steps | Demonstrates |
|---|---|---|
| `order_processing` | validate → charge → ship | a normal multi-step pipeline |
| `retry_demo` | fail N times then succeed | retry with exponential backoff |
| `dlq_demo` | always fails | dead-lettering + replay |
| `timer_demo` | noop → 5s timer → noop | durable timers |
| `bench_noop` | single no-op | throughput benchmarking |

---

## API

| Method & path | Purpose |
|---|---|
| `POST /v1/workflows/{name}/runs` | start a run (`{input, idempotency_key?}`, or `Idempotency-Key` header) |
| `GET /v1/workflows` | list registered workflows |
| `GET /v1/runs?limit=N` | recent runs |
| `GET /v1/runs/{id}` | run + its tasks |
| `GET /v1/runs/{id}/history` | append-only event history |
| `POST /v1/runs/{id}/cancel` | cancel a running run |
| `GET /v1/dlq?limit=N` | dead-letter queue |
| `POST /v1/dlq/{taskId}/replay` | requeue a dead task |
| `GET /v1/stats` | aggregate counts |
| `GET /healthz` · `GET /readyz` | liveness · readiness |
| `GET /metrics` | Prometheus |

---

## Testing

```bash
make pg                                   # throwaway Postgres on :55432
DATABASE_URL='postgres://duraflow:duraflow@localhost:55432/duraflow?sslmode=disable' \
  go test ./... -race -count=1 -p 1
make pg-stop
```

- **Unit tests** (no DB): backoff bounds, jitter band, task construction.
- **Integration tests** (real Postgres, skipped unless `DATABASE_URL` is set):
  happy path, idempotent starts, retry-then-succeed, dead-letter, durable timer,
  and `SKIP LOCKED` single-winner leasing.
- **Crash-injection** (`TestChaosZeroLoss`): stalls workers past their lease to
  force real reclamation, then asserts **(1)** every run completes — zero loss,
  **(2)** total executions exceed the run count — re-runs actually happened, and
  **(3)** each run has exactly one `ActivityCompleted` event — no double advance.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `gofmt`, `go vet`,
`go build`, and the full race-enabled suite against a Postgres service container,
plus a dashboard build.

---

## Benchmarks

Measured locally with [`cmd/bench`](cmd/bench) against PostgreSQL. Methodology:
enqueue `-runs` `bench_noop` workflows recording per-call latency, then start the
worker pool and time how long until all runs complete.

<!-- BENCH_RESULTS -->
Single node, PostgreSQL 16, **durable commits** (`synchronous_commit=on`), 10,000
`bench_noop` workflows, 32 workers, pgx pool of 30. Measured on Apple Silicon with
the benchmark process and Postgres on the same Docker network (so the figures
reflect engine cost, not a macOS port-forward or WAN round-trip).

| Metric | Result |
|---|---|
| Enqueue throughput (`StartWorkflow`) | **~3,850 workflows/sec** |
| End-to-end throughput (enqueue → executed → committed) | **~2,200 workflows/sec** |
| Enqueue latency p50 / p95 | **15 ms / 31 ms** |
| Crash recovery | bounded by the visibility timeout + reap interval (1 s + 200 ms in the chaos test); zero job loss verified by `TestChaosZeroLoss` |

Each completed `bench_noop` run performs, durably, a transactional enqueue plus a
lease + execute + transactional advance — several committed round-trips per
workflow. Tail latency spikes under the heaviest worker counts come from the
constrained local VM; production-grade storage does materially better.
<!-- /BENCH_RESULTS -->

Reproduce:

```bash
make pg
DATABASE_URL='postgres://duraflow:duraflow@localhost:55432/duraflow?sslmode=disable' \
  go run ./cmd/bench -runs 5000 -workers 16
```

---

## Live demo

- **API:** `<RAILWAY_URL>` (try `GET /v1/stats`, `GET /healthz`)
- **Dashboard:** `<VERCEL_URL>`

Deployed on Railway (Go service + co-located PostgreSQL, private networking) and
Vercel (dashboard). The Dockerfile is a multi-stage distroless build.

---

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | PostgreSQL DSN |
| `PORT` | `8080` | HTTP listen port |
| `DURAFLOW_WORKERS` | `8` | worker pool size |
| `DURAFLOW_LEASE_SECONDS` | `30` | visibility timeout |

## License

MIT
