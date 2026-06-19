// Package engine is Duraflow's execution core: a pool of workers that lease
// activity tasks from PostgreSQL, run the registered handler, and durably
// advance the workflow — with retries, exponential backoff, durable timers,
// dead-lettering, lease reclamation, and graceful drain on shutdown.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Sriniketh24/duraflow/internal/metrics"
	"github.com/Sriniketh24/duraflow/internal/store"
	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// Config tunes the engine. Zero values fall back to sensible defaults.
type Config struct {
	Workers        int           // number of concurrent worker goroutines
	LeaseDuration  time.Duration // visibility timeout for a leased task
	PollInterval   time.Duration // idle poll interval when no work is available
	ReapInterval   time.Duration // how often expired leases are reclaimed
	BackoffBase    time.Duration // base for exponential retry backoff
	BackoffMax     time.Duration // cap for retry backoff
	DefaultRetries int           // default max attempts per task
	WorkerID       string        // stable id prefix (e.g. hostname) for lease ownership
}

func (c *Config) withDefaults() {
	if c.Workers <= 0 {
		c.Workers = 8
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 5 * time.Second
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 500 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 5 * time.Minute
	}
	if c.DefaultRetries <= 0 {
		c.DefaultRetries = 5
	}
	if c.WorkerID == "" {
		c.WorkerID = "worker"
	}
}

// Engine coordinates workers against a Store and Registry.
type Engine struct {
	cfg     Config
	store   store.Store
	reg     *workflow.Registry
	metrics *metrics.Metrics
	log     *slog.Logger

	wg   sync.WaitGroup
	rng  *lockedRand
	stop chan struct{}
	once sync.Once
}

// New constructs an Engine.
func New(st store.Store, reg *workflow.Registry, m *metrics.Metrics, cfg Config, log *slog.Logger) *Engine {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Engine{
		cfg:     cfg,
		store:   st,
		reg:     reg,
		metrics: m,
		log:     log,
		rng:     newLockedRand(),
		stop:    make(chan struct{}),
	}
}

// StartWorkflow creates a run and schedules its first step. If idempotencyKey is
// non-empty and already used, the existing run is returned (created=false).
func (e *Engine) StartWorkflow(ctx context.Context, name string, input json.RawMessage, idempotencyKey string) (workflow.Run, bool, error) {
	def, ok := e.reg.Workflow(name)
	if !ok {
		return workflow.Run{}, false, fmt.Errorf("unknown workflow %q", name)
	}
	if len(def.Steps) == 0 {
		return workflow.Run{}, false, fmt.Errorf("workflow %q has no steps", name)
	}
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	var keyPtr *string
	if idempotencyKey != "" {
		keyPtr = &idempotencyKey
	}
	runID := uuid.NewString()
	first := e.buildTask(def, 0, input)

	cmd := store.StartRunCmd{
		RunID:          runID,
		WorkflowName:   name,
		Input:          input,
		TotalSteps:     len(def.Steps),
		IdempotencyKey: keyPtr,
		First:          first,
	}
	run, created, err := e.store.StartRun(ctx, cmd)
	if err != nil {
		return workflow.Run{}, false, err
	}
	if created && e.metrics != nil {
		e.metrics.RunsStarted.Inc()
	}
	return run, created, nil
}

// buildTask constructs the NewTask for a given step index of a definition.
func (e *Engine) buildTask(def workflow.Definition, step int, input json.RawMessage) store.NewTask {
	s := def.Steps[step]
	maxAttempts := s.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = e.cfg.DefaultRetries
	}
	t := store.NewTask{
		ID:           uuid.NewString(),
		StepIndex:    step,
		Input:        input,
		MaxAttempts:  maxAttempts,
		AvailableAt:  time.Now(),
		Kind:         workflow.KindActivity,
		ActivityName: s.Activity,
	}
	if s.Timer > 0 {
		t.Kind = workflow.KindTimer
		t.ActivityName = "__timer__"
		t.AvailableAt = time.Now().Add(s.Timer)
		t.MaxAttempts = 1 // timers do not retry
	}
	return t
}

// Run starts the worker pool and the reaper, blocking until ctx is cancelled,
// after which it drains in-flight work and returns. Workers stop *leasing* new
// tasks the moment ctx is cancelled but always finish the task in hand, so no
// in-flight job is abandoned mid-commit.
func (e *Engine) Run(ctx context.Context) {
	e.log.Info("engine starting", "workers", e.cfg.Workers, "lease", e.cfg.LeaseDuration.String())
	for i := 0; i < e.cfg.Workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx, fmt.Sprintf("%s-%d", e.cfg.WorkerID, i))
	}
	e.wg.Add(1)
	go e.reaper(ctx)

	<-ctx.Done()
	e.log.Info("engine draining in-flight tasks")
	e.wg.Wait()
	e.log.Info("engine stopped")
}

// worker is the lease-execute-advance loop for a single worker.
func (e *Engine) worker(ctx context.Context, id string) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Lease using a background context so an in-flight lease/commit is not
		// torn down by shutdown; the loop condition above handles stopping.
		start := time.Now()
		task, err := e.store.LeaseTask(context.Background(), id, e.cfg.LeaseDuration)
		if e.metrics != nil {
			e.metrics.LeaseLatency.Observe(time.Since(start).Seconds())
		}
		if err != nil {
			e.log.Warn("lease failed", "worker", id, "err", err)
			if e.sleep(ctx, e.cfg.PollInterval) {
				return
			}
			continue
		}
		if task == nil {
			if e.sleep(ctx, e.jitter(e.cfg.PollInterval)) {
				return
			}
			continue
		}
		if e.metrics != nil {
			e.metrics.TasksLeased.Inc()
		}
		e.execute(id, task)
	}
}

// execute runs one leased task to a durable conclusion (advance / retry / DLQ).
// It uses context.Background() so a task that has begun always runs to a
// committed terminal transition even during shutdown drain.
func (e *Engine) execute(workerID string, task *workflow.Task) {
	ctx := context.Background()

	// Durable timer: nothing to run, just fire and advance.
	if task.Kind == workflow.KindTimer {
		if e.metrics != nil {
			e.metrics.TimersFired.Inc()
		}
		e.advance(ctx, workerID, task, json.RawMessage(`{"fired":true}`))
		return
	}

	fn, ok := e.reg.Activity(task.ActivityName)
	if !ok {
		e.fail(ctx, workerID, task, fmt.Errorf("no activity registered for %q", task.ActivityName))
		return
	}

	execStart := time.Now()
	result, err := safeInvoke(ctx, fn, task.Input)
	if e.metrics != nil {
		e.metrics.ExecDuration.Observe(time.Since(execStart).Seconds())
	}
	if err != nil {
		e.fail(ctx, workerID, task, err)
		return
	}
	e.advance(ctx, workerID, task, result)
}

// advance records success and either schedules the next step or finishes the run.
func (e *Engine) advance(ctx context.Context, workerID string, task *workflow.Task, result json.RawMessage) {
	cmd := store.AdvanceCmd{
		TaskID: task.ID,
		RunID:  task.RunID,
		Owner:  workerID,
		Result: result,
	}
	next, runResult, finished, err := e.nextStep(ctx, task, result)
	if err != nil {
		e.log.Error("compute next step failed", "task", task.ID, "err", err)
		e.fail(ctx, workerID, task, err)
		return
	}
	if finished {
		cmd.RunResult = runResult
	} else {
		cmd.NextStep = &next
	}
	if err := e.store.Advance(ctx, cmd); err != nil {
		e.log.Error("advance failed", "task", task.ID, "err", err)
		return
	}
	if e.metrics != nil {
		e.metrics.TasksCompleted.Inc()
		if finished {
			e.metrics.RunsCompleted.Inc()
		}
	}
}

// nextStep resolves the workflow definition for a task's run and builds the
// next task (or signals completion). It needs the workflow name, which it reads
// from the run.
func (e *Engine) nextStep(ctx context.Context, task *workflow.Task, lastResult json.RawMessage) (store.NewTask, json.RawMessage, bool, error) {
	run, err := e.store.GetRun(ctx, task.RunID)
	if err != nil {
		return store.NewTask{}, nil, false, err
	}
	def, ok := e.reg.Workflow(run.WorkflowName)
	if !ok {
		return store.NewTask{}, nil, false, fmt.Errorf("unknown workflow %q", run.WorkflowName)
	}
	nextIdx := task.StepIndex + 1
	if nextIdx >= len(def.Steps) {
		return store.NewTask{}, lastResult, true, nil // finished
	}
	// Next step's input is the previous step's result (simple data hand-off).
	return e.buildTask(def, nextIdx, lastResult), nil, false, nil
}

// fail applies retry-with-backoff or dead-letters an exhausted task.
func (e *Engine) fail(ctx context.Context, workerID string, task *workflow.Task, cause error) {
	if e.metrics != nil {
		e.metrics.TasksFailed.Inc()
	}
	// task.Attempt was incremented to the current attempt number at lease time.
	if task.Attempt >= task.MaxAttempts {
		dlErr := e.store.DeadLetter(ctx, store.DeadLetterCmd{
			TaskID: task.ID, RunID: task.RunID, Owner: workerID, ErrMsg: cause.Error(),
		})
		if dlErr != nil {
			e.log.Error("dead-letter failed", "task", task.ID, "err", dlErr)
			return
		}
		if e.metrics != nil {
			e.metrics.TasksDead.Inc()
		}
		e.log.Warn("task dead-lettered", "task", task.ID, "attempts", task.Attempt, "cause", cause)
		return
	}
	backoff := e.backoff(task.Attempt)
	rErr := e.store.Retry(ctx, store.RetryCmd{
		TaskID: task.ID, RunID: task.RunID, Owner: workerID,
		ErrMsg: cause.Error(), AvailableAt: time.Now().Add(backoff),
	})
	if rErr != nil {
		e.log.Error("retry schedule failed", "task", task.ID, "err", rErr)
		return
	}
	e.log.Info("task retry scheduled", "task", task.ID, "attempt", task.Attempt, "backoff", backoff.String())
}

// reaper periodically reclaims tasks whose visibility timeout has elapsed.
func (e *Engine) reaper(ctx context.Context) {
	defer e.wg.Done()
	t := time.NewTicker(e.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := e.store.ReapExpiredLeases(context.Background())
			if err != nil {
				e.log.Warn("reap failed", "err", err)
				continue
			}
			if n > 0 && e.metrics != nil {
				e.metrics.TasksReclaimed.Add(float64(n))
			}
			if n > 0 {
				e.log.Info("reclaimed expired leases", "count", n)
			}
		}
	}
}

// backoff returns full-jitter exponential backoff for a given attempt (1-based).
func (e *Engine) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := e.cfg.BackoffBase << (attempt - 1)
	if exp <= 0 || exp > e.cfg.BackoffMax {
		exp = e.cfg.BackoffMax
	}
	// Full jitter: random in [0, exp].
	return e.rng.duration(exp)
}

func (e *Engine) jitter(d time.Duration) time.Duration {
	// +/- 20% jitter to desynchronize idle pollers.
	delta := e.rng.duration(d / 5)
	return d - (d / 10) + delta
}

// sleep waits for d or until ctx is done; returns true if ctx was cancelled.
func (e *Engine) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}

// safeInvoke runs an activity handler, converting panics into errors so one bad
// handler cannot crash a worker.
func safeInvoke(ctx context.Context, fn workflow.ActivityFunc, input json.RawMessage) (out json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("activity panicked: %v", r)
		}
	}()
	return fn(ctx, input)
}

// lockedRand is a concurrency-safe random source for jitter/backoff.
type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newLockedRand() *lockedRand {
	return &lockedRand{r: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (l *lockedRand) duration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return time.Duration(l.r.Int63n(int64(max) + 1))
}
