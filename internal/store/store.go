// Package store defines the persistence contract for Duraflow and its
// PostgreSQL implementation. All multi-row state transitions are performed in a
// single transaction so the engine can crash at any point without losing or
// duplicating durable state.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// ErrNotFound is returned when a requested run or task does not exist.
var ErrNotFound = errors.New("not found")

// NewTask describes an activity task to schedule.
type NewTask struct {
	ID           string
	StepIndex    int
	ActivityName string
	Kind         workflow.TaskKind
	Input        json.RawMessage
	MaxAttempts  int
	AvailableAt  time.Time // when the task becomes eligible (now for activities, future for timers)
}

// StartRunCmd starts a workflow run and schedules its first step atomically.
type StartRunCmd struct {
	RunID          string
	WorkflowName   string
	Input          json.RawMessage
	TotalSteps     int
	IdempotencyKey *string
	First          NewTask
}

// AdvanceCmd is applied when an activity completes successfully. In one
// transaction it marks the task completed, appends the ActivityCompleted event,
// and EITHER schedules the next step OR completes the run.
//
// Owner fences the transition against a stale lease: the update only applies
// while the task is still leased by this worker. If the lease was reclaimed and
// re-leased elsewhere, the stale worker's commit affects zero rows and its
// result is discarded — preventing a step from advancing twice.
type AdvanceCmd struct {
	TaskID    string
	RunID     string
	Owner     string
	Result    json.RawMessage
	NextStep  *NewTask        // nil => run is finished
	RunResult json.RawMessage // set when NextStep is nil
}

// RetryCmd reschedules a failed task with backoff (status -> pending).
type RetryCmd struct {
	TaskID      string
	RunID       string
	Owner       string
	ErrMsg      string
	AvailableAt time.Time
}

// DeadLetterCmd moves a task to the dead-letter state and fails the run.
type DeadLetterCmd struct {
	TaskID string
	RunID  string
	Owner  string
	ErrMsg string
}

// Stats is an aggregate snapshot used by metrics and the dashboard.
type Stats struct {
	RunsRunning   int `json:"runs_running"`
	RunsCompleted int `json:"runs_completed"`
	RunsFailed    int `json:"runs_failed"`
	RunsCancelled int `json:"runs_cancelled"`
	TasksPending  int `json:"tasks_pending"`
	TasksLeased   int `json:"tasks_leased"`
	TasksDead     int `json:"tasks_dead"`
}

// Store is the persistence boundary. The PostgreSQL implementation is the only
// production backend; the interface exists so the engine and API can be tested
// against fakes and so the hot-path SQL is isolated in one place.
type Store interface {
	// Migrate applies the embedded schema (idempotent).
	Migrate(ctx context.Context) error

	// StartRun inserts the run + first task + events atomically. If an
	// idempotency key is supplied and already exists, the existing run is
	// returned with created=false and no new work is scheduled.
	StartRun(ctx context.Context, cmd StartRunCmd) (run workflow.Run, created bool, err error)

	// LeaseTask atomically claims the next eligible task using
	// SELECT ... FOR UPDATE SKIP LOCKED, bumping attempt and setting the lease
	// deadline. Returns (nil, nil) when no task is currently available.
	LeaseTask(ctx context.Context, workerID string, lease time.Duration) (*workflow.Task, error)

	// Advance applies AdvanceCmd in a single transaction.
	Advance(ctx context.Context, cmd AdvanceCmd) error

	// Retry reschedules a failed task with backoff.
	Retry(ctx context.Context, cmd RetryCmd) error

	// DeadLetter moves an exhausted task to the DLQ and fails its run.
	DeadLetter(ctx context.Context, cmd DeadLetterCmd) error

	// ReapExpiredLeases returns leased tasks whose visibility timeout has
	// elapsed back to pending (covers crashed workers). Returns the count.
	ReapExpiredLeases(ctx context.Context) (int, error)

	// GetRun returns a run by id (ErrNotFound if missing).
	GetRun(ctx context.Context, id string) (workflow.Run, error)

	// GetTasks returns all tasks for a run, ordered by step index.
	GetTasks(ctx context.Context, runID string) ([]workflow.Task, error)

	// GetHistory returns the append-only event history for a run.
	GetHistory(ctx context.Context, runID string) ([]workflow.Event, error)

	// ListRuns returns recent runs (most recent first), capped by limit.
	ListRuns(ctx context.Context, limit int) ([]workflow.Run, error)

	// ListDeadLetters returns tasks currently in the DLQ.
	ListDeadLetters(ctx context.Context, limit int) ([]workflow.Task, error)

	// ReplayDeadLetter resets a dead task to pending/available-now so workers
	// pick it up again. ErrNotFound if the task is not dead.
	ReplayDeadLetter(ctx context.Context, taskID string) error

	// CancelRun cancels a running run and its outstanding tasks.
	CancelRun(ctx context.Context, runID string) error

	// GetStats returns aggregate counts.
	GetStats(ctx context.Context) (Stats, error)

	// Ping checks connectivity (readiness probe).
	Ping(ctx context.Context) error

	// Close releases the connection pool.
	Close()
}
