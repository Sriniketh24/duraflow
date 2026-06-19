// Package workflow defines Duraflow's core domain model: workflows are ordered
// sequences of activity (or durable-timer) steps, executed durably with an
// append-only event history backing recovery and replay.
package workflow

import (
	"context"
	"encoding/json"
	"time"
)

// RunStatus is the lifecycle state of a workflow run.
type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// TaskStatus is the lifecycle state of a single activity task.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskLeased    TaskStatus = "leased"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskDead      TaskStatus = "dead" // dead-letter: exhausted retries
	TaskCancelled TaskStatus = "cancelled"
)

// TaskKind distinguishes ordinary activities from durable timers.
type TaskKind string

const (
	KindActivity TaskKind = "activity"
	KindTimer    TaskKind = "timer"
)

// Event types recorded in the append-only history.
const (
	EventWorkflowStarted   = "WorkflowStarted"
	EventActivityScheduled = "ActivityScheduled"
	EventActivityStarted   = "ActivityStarted"
	EventActivityCompleted = "ActivityCompleted"
	EventActivityFailed    = "ActivityFailed"
	EventActivityRetried   = "ActivityRetryScheduled"
	EventTimerStarted      = "TimerStarted"
	EventTimerFired        = "TimerFired"
	EventDeadLettered      = "ActivityDeadLettered"
	EventReclaimed         = "TaskReclaimed"
	EventReplayed          = "TaskReplayed"
	EventWorkflowCompleted = "WorkflowCompleted"
	EventWorkflowFailed    = "WorkflowFailed"
	EventWorkflowCancelled = "WorkflowCancelled"
)

// Run is a workflow execution instance.
type Run struct {
	ID             string          `json:"id"`
	WorkflowName   string          `json:"workflow_name"`
	Input          json.RawMessage `json:"input"`
	Status         RunStatus       `json:"status"`
	CurrentStep    int             `json:"current_step"`
	TotalSteps     int             `json:"total_steps"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          string          `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Task is a single durable unit of work (an activity or timer step).
type Task struct {
	ID           string          `json:"id"`
	RunID        string          `json:"run_id"`
	StepIndex    int             `json:"step_index"`
	ActivityName string          `json:"activity_name"`
	Kind         TaskKind        `json:"kind"`
	Input        json.RawMessage `json:"input"`
	Status       TaskStatus      `json:"status"`
	Attempt      int             `json:"attempt"`
	MaxAttempts  int             `json:"max_attempts"`
	AvailableAt  time.Time       `json:"available_at"`
	LeaseExpires *time.Time      `json:"lease_expires_at,omitempty"`
	LeasedBy     *string         `json:"leased_by,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        string          `json:"error,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// Event is one immutable entry in a run's history.
type Event struct {
	ID        int64           `json:"id"`
	RunID     string          `json:"run_id"`
	TaskID    *string         `json:"task_id,omitempty"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// Step is one node of a workflow definition.
type Step struct {
	// Activity is the registered activity name to execute. Empty for timers.
	Activity string
	// Timer, if > 0, makes this step a durable timer that sleeps the given
	// duration before the workflow advances (survives restarts via Postgres).
	Timer time.Duration
	// MaxAttempts overrides the engine default for this step (0 = use default).
	MaxAttempts int
}

// Definition is a named, ordered list of steps.
type Definition struct {
	Name  string
	Steps []Step
}

// ActivityFunc is a user-supplied activity handler. The returned bytes become
// the task result; a non-nil error triggers retry/backoff (and eventually DLQ).
// Handlers must be idempotent: at-least-once delivery means a handler may run
// more than once for the same task if a worker crashes after executing but
// before committing the result.
type ActivityFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

// Registry holds workflow definitions and activity handlers.
type Registry struct {
	defs       map[string]Definition
	activities map[string]ActivityFunc
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		defs:       make(map[string]Definition),
		activities: make(map[string]ActivityFunc),
	}
}

// RegisterWorkflow adds a workflow definition.
func (r *Registry) RegisterWorkflow(d Definition) { r.defs[d.Name] = d }

// RegisterActivity adds an activity handler by name.
func (r *Registry) RegisterActivity(name string, fn ActivityFunc) { r.activities[name] = fn }

// Workflow looks up a definition by name.
func (r *Registry) Workflow(name string) (Definition, bool) {
	d, ok := r.defs[name]
	return d, ok
}

// Activity looks up a handler by name.
func (r *Registry) Activity(name string) (ActivityFunc, bool) {
	fn, ok := r.activities[name]
	return fn, ok
}

// Workflows returns all registered definition names.
func (r *Registry) Workflows() []string {
	names := make([]string, 0, len(r.defs))
	for n := range r.defs {
		names = append(names, n)
	}
	return names
}
