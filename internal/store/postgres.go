package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// Postgres is the production github.com/jackc/pgx/v5-backed implementation of
// store.Store. All multi-step state transitions run inside a single
// transaction so the engine can crash at any point without losing or
// duplicating durable state.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres builds a connection pool for dsn, verifies connectivity with a
// ping, and returns the ready-to-use store.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Ping checks connectivity (readiness probe).
func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Close releases the connection pool.
func (p *Postgres) Close() {
	p.pool.Close()
}

// --- row scanning helpers ---------------------------------------------------

// runColumns lists the workflow_runs columns in the exact order scanRun
// expects them, for use in SELECT/RETURNING clauses.
const runColumns = `id, workflow_name, input, status, current_step, total_steps,
	idempotency_key, result, error, created_at, updated_at`

// rowScanner is satisfied by both pgx.Row and pgx.Rows, letting scanRun /
// scanTask / scanEvent be shared between QueryRow and Query call sites.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(row rowScanner) (workflow.Run, error) {
	var r workflow.Run
	var errMsg *string
	if err := row.Scan(
		&r.ID, &r.WorkflowName, &r.Input, &r.Status, &r.CurrentStep, &r.TotalSteps,
		&r.IdempotencyKey, &r.Result, &errMsg, &r.CreatedAt, &r.UpdatedAt,
	); err != nil {
		return workflow.Run{}, err
	}
	if errMsg != nil {
		r.Error = *errMsg
	}
	return r, nil
}

// taskColumns lists the activity_tasks columns in the exact order scanTask
// expects them.
const taskColumns = `id, run_id, step_index, activity_name, kind, input, status,
	attempt, max_attempts, available_at, lease_expires_at, leased_by, result, error,
	created_at, updated_at`

func scanTask(row rowScanner) (workflow.Task, error) {
	var t workflow.Task
	var errMsg *string
	if err := row.Scan(
		&t.ID, &t.RunID, &t.StepIndex, &t.ActivityName, &t.Kind, &t.Input, &t.Status,
		&t.Attempt, &t.MaxAttempts, &t.AvailableAt, &t.LeaseExpires, &t.LeasedBy,
		&t.Result, &errMsg, &t.CreatedAt, &t.UpdatedAt,
	); err != nil {
		return workflow.Task{}, err
	}
	if errMsg != nil {
		t.Error = *errMsg
	}
	return t, nil
}

func scanEvent(row rowScanner) (workflow.Event, error) {
	var e workflow.Event
	if err := row.Scan(&e.ID, &e.RunID, &e.TaskID, &e.Type, &e.Payload, &e.CreatedAt); err != nil {
		return workflow.Event{}, err
	}
	return e, nil
}

// --- StartRun ----------------------------------------------------------------

// StartRun inserts the run + first task + WorkflowStarted/ActivityScheduled
// events atomically. If cmd.IdempotencyKey is set and a run with that key
// already exists, no new work is scheduled and the existing run is returned
// with created=false.
func (p *Postgres) StartRun(ctx context.Context, cmd StartRunCmd) (workflow.Run, bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return workflow.Run{}, false, fmt.Errorf("store: start run: begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once committed

	// Idempotency pre-check: if a key is supplied and already claimed, return
	// the existing run untouched rather than scheduling duplicate work.
	if cmd.IdempotencyKey != nil {
		row := tx.QueryRow(ctx,
			`SELECT `+runColumns+` FROM workflow_runs WHERE idempotency_key = $1`,
			*cmd.IdempotencyKey,
		)
		existing, err := scanRun(row)
		if err == nil {
			// Found a prior run under this key; nothing new to insert.
			if err := tx.Commit(ctx); err != nil {
				return workflow.Run{}, false, fmt.Errorf("store: start run: commit: %w", err)
			}
			return existing, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return workflow.Run{}, false, fmt.Errorf("store: start run: idempotency check: %w", err)
		}
		// ErrNoRows: fall through and create the run below.
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO workflow_runs (id, workflow_name, input, status, current_step, total_steps, idempotency_key)
		VALUES ($1, $2, $3, 'running', 0, $4, $5)
		RETURNING `+runColumns,
		cmd.RunID, cmd.WorkflowName, cmd.Input, cmd.TotalSteps, cmd.IdempotencyKey,
	)
	run, err := scanRun(row)
	if err != nil {
		return workflow.Run{}, false, fmt.Errorf("store: start run: insert run: %w", err)
	}

	if err := insertTask(ctx, tx, cmd.RunID, cmd.First); err != nil {
		return workflow.Run{}, false, fmt.Errorf("store: start run: insert first task: %w", err)
	}

	if err := insertEvent(ctx, tx, cmd.RunID, nil, workflow.EventWorkflowStarted, cmd.Input); err != nil {
		return workflow.Run{}, false, fmt.Errorf("store: start run: WorkflowStarted event: %w", err)
	}
	if err := insertEvent(ctx, tx, cmd.RunID, &cmd.First.ID, workflow.EventActivityScheduled, cmd.First.Input); err != nil {
		return workflow.Run{}, false, fmt.Errorf("store: start run: ActivityScheduled event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return workflow.Run{}, false, fmt.Errorf("store: start run: commit: %w", err)
	}
	return run, true, nil
}

// insertTask inserts one activity_tasks row from a NewTask spec.
func insertTask(ctx context.Context, tx pgx.Tx, runID string, t NewTask) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO activity_tasks (id, run_id, step_index, activity_name, kind, input, status, attempt, max_attempts, available_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $8)`,
		t.ID, runID, t.StepIndex, t.ActivityName, t.Kind, t.Input, t.MaxAttempts, t.AvailableAt,
	)
	return err
}

// insertEvent appends one row to the append-only events log.
func insertEvent(ctx context.Context, tx pgx.Tx, runID string, taskID *string, typ string, payload []byte) error {
	if payload == nil {
		payload = []byte(`{}`)
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO events (run_id, task_id, type, payload) VALUES ($1, $2, $3, $4)`,
		runID, taskID, typ, payload,
	)
	return err
}

// --- LeaseTask -----------------------------------------------------------------

// LeaseTask atomically claims the next eligible pending task using
// SELECT ... FOR UPDATE SKIP LOCKED, bumping attempt and setting the lease
// deadline. Returns (nil, nil) when no task is currently available.
func (p *Postgres) LeaseTask(ctx context.Context, workerID string, lease time.Duration) (*workflow.Task, error) {
	// Bind the lease as double precision seconds and build the interval by
	// multiplication. Casting the parameter to text (the previous approach)
	// made pgx try to encode a float64 into a text-typed param (OID 25), which
	// it cannot do; double precision encodes cleanly and also supports the
	// sub-second leases used to exercise the reaper.
	intervalSeconds := lease.Seconds()

	row := p.pool.QueryRow(ctx, `
		UPDATE activity_tasks SET status='leased', leased_by=$1, lease_expires_at=now()+($2::double precision * interval '1 second'),
			attempt=attempt+1, updated_at=now()
		WHERE id = (
			SELECT id FROM activity_tasks
			WHERE status='pending' AND available_at <= now()
			ORDER BY available_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+taskColumns,
		workerID, intervalSeconds,
	)
	task, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: lease task: %w", err)
	}
	return &task, nil
}

// --- Advance -------------------------------------------------------------------

// Advance applies AdvanceCmd in a single transaction, fenced on the lease
// owner: the completion update only takes effect while the task is still
// leased by cmd.Owner. If the lease was reclaimed and re-leased elsewhere in
// the meantime, the update affects zero rows and this call is a no-op
// (returns nil, nil work performed) rather than an error — this prevents a
// step from advancing twice under a stale worker.
func (p *Postgres) Advance(ctx context.Context, cmd AdvanceCmd) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: advance: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE activity_tasks SET status='completed', result=$1, updated_at=now()
		WHERE id=$2 AND status='leased' AND leased_by=$3`,
		cmd.Result, cmd.TaskID, cmd.Owner,
	)
	if err != nil {
		return fmt.Errorf("store: advance: complete task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Stale lease — another worker already won this task. Nothing to do;
		// commit the empty transaction (rollback would behave identically).
		return tx.Commit(ctx)
	}

	if err := insertEvent(ctx, tx, cmd.RunID, &cmd.TaskID, workflow.EventActivityCompleted, cmd.Result); err != nil {
		return fmt.Errorf("store: advance: ActivityCompleted event: %w", err)
	}

	if cmd.NextStep != nil {
		if err := insertTask(ctx, tx, cmd.RunID, *cmd.NextStep); err != nil {
			return fmt.Errorf("store: advance: insert next task: %w", err)
		}
		if err := insertEvent(ctx, tx, cmd.RunID, &cmd.NextStep.ID, workflow.EventActivityScheduled, cmd.NextStep.Input); err != nil {
			return fmt.Errorf("store: advance: ActivityScheduled event: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE workflow_runs SET current_step=$1, updated_at=now() WHERE id=$2`,
			cmd.NextStep.StepIndex, cmd.RunID,
		); err != nil {
			return fmt.Errorf("store: advance: update current_step: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx,
			`UPDATE workflow_runs SET status='completed', result=$1, updated_at=now() WHERE id=$2`,
			cmd.RunResult, cmd.RunID,
		); err != nil {
			return fmt.Errorf("store: advance: complete run: %w", err)
		}
		if err := insertEvent(ctx, tx, cmd.RunID, nil, workflow.EventWorkflowCompleted, cmd.RunResult); err != nil {
			return fmt.Errorf("store: advance: WorkflowCompleted event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: advance: commit: %w", err)
	}
	return nil
}

// --- Retry ---------------------------------------------------------------------

// Retry reschedules a failed task with backoff. Fenced on the lease owner
// like Advance: a stale lease's retry is silently discarded.
func (p *Postgres) Retry(ctx context.Context, cmd RetryCmd) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: retry: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE activity_tasks
		SET status='pending', available_at=$1, error=$2, leased_by=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE id=$3 AND status='leased' AND leased_by=$4`,
		cmd.AvailableAt, cmd.ErrMsg, cmd.TaskID, cmd.Owner,
	)
	if err != nil {
		return fmt.Errorf("store: retry: update task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	if err := insertEvent(ctx, tx, cmd.RunID, &cmd.TaskID, workflow.EventActivityRetried, []byte(fmt.Sprintf(`{"error":%q}`, cmd.ErrMsg))); err != nil {
		return fmt.Errorf("store: retry: ActivityRetryScheduled event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: retry: commit: %w", err)
	}
	return nil
}

// --- DeadLetter ------------------------------------------------------------------

// DeadLetter moves an exhausted task to the DLQ and fails its run. Fenced on
// the lease owner like Advance/Retry.
func (p *Postgres) DeadLetter(ctx context.Context, cmd DeadLetterCmd) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: dead letter: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE activity_tasks SET status='dead', error=$1, updated_at=now()
		WHERE id=$2 AND status='leased' AND leased_by=$3`,
		cmd.ErrMsg, cmd.TaskID, cmd.Owner,
	)
	if err != nil {
		return fmt.Errorf("store: dead letter: update task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	if err := insertEvent(ctx, tx, cmd.RunID, &cmd.TaskID, workflow.EventDeadLettered, []byte(fmt.Sprintf(`{"error":%q}`, cmd.ErrMsg))); err != nil {
		return fmt.Errorf("store: dead letter: ActivityDeadLettered event: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE workflow_runs SET status='failed', error=$1, updated_at=now() WHERE id=$2`,
		cmd.ErrMsg, cmd.RunID,
	); err != nil {
		return fmt.Errorf("store: dead letter: fail run: %w", err)
	}
	if err := insertEvent(ctx, tx, cmd.RunID, nil, workflow.EventWorkflowFailed, []byte(fmt.Sprintf(`{"error":%q}`, cmd.ErrMsg))); err != nil {
		return fmt.Errorf("store: dead letter: WorkflowFailed event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: dead letter: commit: %w", err)
	}
	return nil
}

// --- ReapExpiredLeases -------------------------------------------------------------

// ReapExpiredLeases returns leased tasks whose visibility timeout has elapsed
// back to pending (covers crashed workers) and returns the reclaimed count.
func (p *Postgres) ReapExpiredLeases(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: reap: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE activity_tasks SET status='pending', leased_by=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
		RETURNING id, run_id`,
	)
	if err != nil {
		return 0, fmt.Errorf("store: reap: update: %w", err)
	}

	type reclaimed struct {
		taskID string
		runID  string
	}
	var ids []reclaimed
	for rows.Next() {
		var rc reclaimed
		if err := rows.Scan(&rc.taskID, &rc.runID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: reap: scan: %w", err)
		}
		ids = append(ids, rc)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: reap: rows: %w", err)
	}

	for _, rc := range ids {
		taskID := rc.taskID
		if err := insertEvent(ctx, tx, rc.runID, &taskID, workflow.EventReclaimed, []byte(`{}`)); err != nil {
			return 0, fmt.Errorf("store: reap: TaskReclaimed event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: reap: commit: %w", err)
	}
	return len(ids), nil
}

// --- reads -----------------------------------------------------------------------

// GetRun returns a run by id (ErrNotFound if missing).
func (p *Postgres) GetRun(ctx context.Context, id string) (workflow.Run, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM workflow_runs WHERE id = $1`, id)
	run, err := scanRun(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workflow.Run{}, ErrNotFound
		}
		return workflow.Run{}, fmt.Errorf("store: get run: %w", err)
	}
	return run, nil
}

// GetTasks returns all tasks for a run, ordered by step index.
func (p *Postgres) GetTasks(ctx context.Context, runID string) ([]workflow.Task, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT `+taskColumns+` FROM activity_tasks WHERE run_id = $1 ORDER BY step_index`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: get tasks: %w", err)
	}
	defer rows.Close()

	var tasks []workflow.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: get tasks: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: get tasks: rows: %w", err)
	}
	return tasks, nil
}

// GetHistory returns the append-only event history for a run, ordered by id
// (insertion order).
func (p *Postgres) GetHistory(ctx context.Context, runID string) ([]workflow.Event, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, run_id, task_id, type, payload, created_at FROM events WHERE run_id = $1 ORDER BY id`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: get history: %w", err)
	}
	defer rows.Close()

	var events []workflow.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("store: get history: scan: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: get history: rows: %w", err)
	}
	return events, nil
}

// ListRuns returns recent runs (most recent first), capped by limit.
func (p *Postgres) ListRuns(ctx context.Context, limit int) ([]workflow.Run, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT `+runColumns+` FROM workflow_runs ORDER BY created_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list runs: %w", err)
	}
	defer rows.Close()

	var runs []workflow.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list runs: scan: %w", err)
		}
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list runs: rows: %w", err)
	}
	return runs, nil
}

// ListDeadLetters returns tasks currently in the DLQ, capped by limit.
func (p *Postgres) ListDeadLetters(ctx context.Context, limit int) ([]workflow.Task, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT `+taskColumns+` FROM activity_tasks WHERE status = 'dead' ORDER BY updated_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: %w", err)
	}
	defer rows.Close()

	var tasks []workflow.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list dead letters: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list dead letters: rows: %w", err)
	}
	return tasks, nil
}

// ReplayDeadLetter resets a dead task to pending/available-now so workers
// pick it up again. ErrNotFound if the task is not currently dead.
func (p *Postgres) ReplayDeadLetter(ctx context.Context, taskID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: replay dead letter: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var runID string
	row := tx.QueryRow(ctx, `
		UPDATE activity_tasks SET status='pending', available_at=now(), error=NULL, attempt=0, updated_at=now()
		WHERE id=$1 AND status='dead'
		RETURNING run_id`,
		taskID,
	)
	if err := row.Scan(&runID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("store: replay dead letter: update: %w", err)
	}

	if err := insertEvent(ctx, tx, runID, &taskID, workflow.EventReplayed, []byte(`{}`)); err != nil {
		return fmt.Errorf("store: replay dead letter: TaskReplayed event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: replay dead letter: commit: %w", err)
	}
	return nil
}

// CancelRun cancels a running run and all of its non-terminal tasks.
// ErrNotFound if the run does not exist or is not currently running.
func (p *Postgres) CancelRun(ctx context.Context, runID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: cancel run: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE workflow_runs SET status='cancelled', updated_at=now() WHERE id=$1 AND status='running'`,
		runID,
	)
	if err != nil {
		return fmt.Errorf("store: cancel run: update run: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Either the run doesn't exist or it already left the running state;
		// disambiguate so callers get ErrNotFound only for the former.
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE id=$1)`, runID).Scan(&exists); err != nil {
			return fmt.Errorf("store: cancel run: existence check: %w", err)
		}
		if !exists {
			return ErrNotFound
		}
		// Run exists but isn't running (already terminal) — treat as a no-op
		// success rather than an error.
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE activity_tasks SET status='cancelled', updated_at=now()
		 WHERE run_id=$1 AND status NOT IN ('completed','failed','dead','cancelled')`,
		runID,
	); err != nil {
		return fmt.Errorf("store: cancel run: update tasks: %w", err)
	}

	if err := insertEvent(ctx, tx, runID, nil, workflow.EventWorkflowCancelled, []byte(`{}`)); err != nil {
		return fmt.Errorf("store: cancel run: WorkflowCancelled event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: cancel run: commit: %w", err)
	}
	return nil
}

// GetStats returns aggregate counts across runs and tasks.
func (p *Postgres) GetStats(ctx context.Context) (Stats, error) {
	var s Stats
	row := p.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM workflow_runs WHERE status='running')   AS runs_running,
			(SELECT count(*) FROM workflow_runs WHERE status='completed') AS runs_completed,
			(SELECT count(*) FROM workflow_runs WHERE status='failed')    AS runs_failed,
			(SELECT count(*) FROM workflow_runs WHERE status='cancelled') AS runs_cancelled,
			(SELECT count(*) FROM activity_tasks WHERE status='pending')  AS tasks_pending,
			(SELECT count(*) FROM activity_tasks WHERE status='leased')   AS tasks_leased,
			(SELECT count(*) FROM activity_tasks WHERE status='dead')     AS tasks_dead
	`)
	if err := row.Scan(
		&s.RunsRunning, &s.RunsCompleted, &s.RunsFailed, &s.RunsCancelled,
		&s.TasksPending, &s.TasksLeased, &s.TasksDead,
	); err != nil {
		return Stats{}, fmt.Errorf("store: get stats: %w", err)
	}
	return s, nil
}
