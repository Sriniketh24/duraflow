package store

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// newTestStore connects to DATABASE_URL, migrates, and truncates all tables
// so every test starts from a clean slate. Skips the test if DATABASE_URL is
// unset (these are integration tests, not unit tests).
func newTestStore(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL")
	}

	ctx := context.Background()
	pg, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	t.Cleanup(pg.Close)

	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `TRUNCATE events, activity_tasks, workflow_runs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pg
}

func newTaskID() string { return uuid.NewString() }
func newRunID() string  { return uuid.NewString() }

func rawJSON(s string) json.RawMessage { return json.RawMessage(s) }

// startedRun is a small helper that starts a run with one pending task and
// returns both, to cut boilerplate across tests that need a leasable task.
func startedRun(t *testing.T, ctx context.Context, pg *Postgres) (workflow.Run, NewTask) {
	t.Helper()
	first := NewTask{
		ID:           newTaskID(),
		StepIndex:    0,
		ActivityName: "do-thing",
		Kind:         workflow.KindActivity,
		Input:        rawJSON(`{"x":1}`),
		MaxAttempts:  5,
		AvailableAt:  time.Now(),
	}
	run, created, err := pg.StartRun(ctx, StartRunCmd{
		RunID:        newRunID(),
		WorkflowName: "test-wf",
		Input:        rawJSON(`{"a":1}`),
		TotalSteps:   2,
		First:        first,
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if !created {
		t.Fatalf("StartRun: expected created=true")
	}
	return run, first
}

func TestStartRun_IdempotencyKey(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)

	key := "idem-" + uuid.NewString()
	first := NewTask{
		ID:           newTaskID(),
		StepIndex:    0,
		ActivityName: "do-thing",
		Kind:         workflow.KindActivity,
		Input:        rawJSON(`{}`),
		MaxAttempts:  5,
		AvailableAt:  time.Now(),
	}
	cmd := StartRunCmd{
		RunID:          newRunID(),
		WorkflowName:   "wf",
		Input:          rawJSON(`{}`),
		TotalSteps:     1,
		IdempotencyKey: &key,
		First:          first,
	}

	run1, created1, err := pg.StartRun(ctx, cmd)
	if err != nil {
		t.Fatalf("StartRun #1: %v", err)
	}
	if !created1 {
		t.Fatalf("StartRun #1: expected created=true")
	}

	// Second call reuses the same key but a different run/task ID — must be
	// rejected and return the first run untouched.
	cmd2 := cmd
	cmd2.RunID = newRunID()
	cmd2.First.ID = newTaskID()
	run2, created2, err := pg.StartRun(ctx, cmd2)
	if err != nil {
		t.Fatalf("StartRun #2: %v", err)
	}
	if created2 {
		t.Fatalf("StartRun #2: expected created=false")
	}
	if run2.ID != run1.ID {
		t.Fatalf("StartRun #2: expected same run ID %s, got %s", run1.ID, run2.ID)
	}

	runs, err := pg.ListRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected exactly 1 run after idempotent retry, got %d", len(runs))
	}

	tasks, err := pg.GetTasks(ctx, run1.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected exactly 1 task after idempotent retry, got %d", len(tasks))
	}
}

func TestLeaseTask_SkipLocked_OnlyOneWinner(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	_, first := startedRun(t, ctx, pg)

	const concurrency = 5
	var wins int32
	var nils int32
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			task, err := pg.LeaseTask(ctx, "worker-"+uuid.NewString(), 30*time.Second)
			if err != nil {
				t.Errorf("LeaseTask: %v", err)
				return
			}
			if task == nil {
				atomic.AddInt32(&nils, 1)
				return
			}
			if task.ID != first.ID {
				t.Errorf("leased unexpected task ID %s", task.ID)
			}
			atomic.AddInt32(&wins, 1)
		}(i)
	}
	wg.Wait()

	if wins != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", wins)
	}
	if nils != concurrency-1 {
		t.Fatalf("expected %d nil leases, got %d", concurrency-1, nils)
	}
}

func TestLeaseTask_NoneAvailable(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)

	task, err := pg.LeaseTask(ctx, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if task != nil {
		t.Fatalf("expected nil task, got %+v", task)
	}
}

func TestAdvance_SchedulesNextStep(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	owner := "worker-1"
	leased, err := pg.LeaseTask(ctx, owner, 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if leased == nil || leased.ID != first.ID {
		t.Fatalf("expected to lease first task, got %+v", leased)
	}

	next := NewTask{
		ID:           newTaskID(),
		StepIndex:    1,
		ActivityName: "step-2",
		Kind:         workflow.KindActivity,
		Input:        rawJSON(`{"y":2}`),
		MaxAttempts:  5,
		AvailableAt:  time.Now(),
	}
	if err := pg.Advance(ctx, AdvanceCmd{
		TaskID:   leased.ID,
		RunID:    run.ID,
		Owner:    owner,
		Result:   rawJSON(`{"ok":true}`),
		NextStep: &next,
	}); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	updatedRun, err := pg.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != workflow.RunRunning {
		t.Fatalf("expected run still running, got %s", updatedRun.Status)
	}
	if updatedRun.CurrentStep != 1 {
		t.Fatalf("expected current_step=1, got %d", updatedRun.CurrentStep)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	if tasks[0].Status != workflow.TaskCompleted {
		t.Fatalf("expected first task completed, got %s", tasks[0].Status)
	}
	if tasks[1].Status != workflow.TaskPending {
		t.Fatalf("expected second task pending, got %s", tasks[1].Status)
	}

	history, err := pg.GetHistory(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	wantTypes := []string{
		workflow.EventWorkflowStarted,
		workflow.EventActivityScheduled,
		workflow.EventActivityCompleted,
		workflow.EventActivityScheduled,
	}
	if len(history) != len(wantTypes) {
		t.Fatalf("expected %d events, got %d: %+v", len(wantTypes), len(history), history)
	}
	for i, want := range wantTypes {
		if history[i].Type != want {
			t.Errorf("event[%d]: expected type %s, got %s", i, want, history[i].Type)
		}
	}
}

func TestAdvance_CompletesRun(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	owner := "worker-1"
	leased, err := pg.LeaseTask(ctx, owner, 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if leased == nil || leased.ID != first.ID {
		t.Fatalf("expected to lease first task")
	}

	if err := pg.Advance(ctx, AdvanceCmd{
		TaskID:    leased.ID,
		RunID:     run.ID,
		Owner:     owner,
		Result:    rawJSON(`{"done":true}`),
		NextStep:  nil,
		RunResult: rawJSON(`{"final":42}`),
	}); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	updatedRun, err := pg.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != workflow.RunCompleted {
		t.Fatalf("expected run completed, got %s", updatedRun.Status)
	}
	// Compare semantically: jsonb reformats (e.g. adds a space after the colon),
	// so an exact byte match would be brittle.
	var gotResult map[string]any
	if err := json.Unmarshal(updatedRun.Result, &gotResult); err != nil || gotResult["final"] != float64(42) {
		t.Fatalf("unexpected run result: %s", updatedRun.Result)
	}

	history, err := pg.GetHistory(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	last := history[len(history)-1]
	if last.Type != workflow.EventWorkflowCompleted {
		t.Fatalf("expected last event WorkflowCompleted, got %s", last.Type)
	}
}

func TestAdvance_StaleLeaseIsNoop(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	if _, err := pg.LeaseTask(ctx, "worker-real-owner", 30*time.Second); err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}

	// Advance using a different (stale) owner — must be a silent no-op.
	err := pg.Advance(ctx, AdvanceCmd{
		TaskID:    first.ID,
		RunID:     run.ID,
		Owner:     "worker-impostor",
		Result:    rawJSON(`{}`),
		RunResult: rawJSON(`{}`),
	})
	if err != nil {
		t.Fatalf("Advance with stale owner should be a no-op, got error: %v", err)
	}

	updatedRun, err := pg.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != workflow.RunRunning {
		t.Fatalf("expected run still running after stale Advance, got %s", updatedRun.Status)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	if tasks[0].Status != workflow.TaskLeased {
		t.Fatalf("expected task to remain leased, got %s", tasks[0].Status)
	}
}

func TestRetry_ReschedulesWithFutureAvailability(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	owner := "worker-1"
	if _, err := pg.LeaseTask(ctx, owner, 30*time.Second); err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}

	future := time.Now().Add(1 * time.Hour).Truncate(time.Millisecond)
	if err := pg.Retry(ctx, RetryCmd{
		TaskID:      first.ID,
		RunID:       run.ID,
		Owner:       owner,
		ErrMsg:      "boom",
		AvailableAt: future,
	}); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	tk := tasks[0]
	if tk.Status != workflow.TaskPending {
		t.Fatalf("expected task pending after retry, got %s", tk.Status)
	}
	if tk.LeasedBy != nil {
		t.Fatalf("expected leased_by cleared, got %v", *tk.LeasedBy)
	}
	if tk.Error != "boom" {
		t.Fatalf("expected error 'boom', got %q", tk.Error)
	}
	if !tk.AvailableAt.Equal(future) {
		t.Fatalf("expected available_at=%v, got %v", future, tk.AvailableAt)
	}

	// Not yet available, so leasing now must return nothing.
	leased, err := pg.LeaseTask(ctx, "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if leased != nil {
		t.Fatalf("expected no leasable task before available_at, got %+v", leased)
	}

	history, err := pg.GetHistory(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	last := history[len(history)-1]
	if last.Type != workflow.EventActivityRetried {
		t.Fatalf("expected last event ActivityRetryScheduled, got %s", last.Type)
	}
}

func TestDeadLetter_AfterExhaustionFailsRun(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	owner := "worker-1"
	if _, err := pg.LeaseTask(ctx, owner, 30*time.Second); err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}

	if err := pg.DeadLetter(ctx, DeadLetterCmd{
		TaskID: first.ID,
		RunID:  run.ID,
		Owner:  owner,
		ErrMsg: "exhausted retries",
	}); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	if tasks[0].Status != workflow.TaskDead {
		t.Fatalf("expected task dead, got %s", tasks[0].Status)
	}

	updatedRun, err := pg.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != workflow.RunFailed {
		t.Fatalf("expected run failed, got %s", updatedRun.Status)
	}
	if updatedRun.Error != "exhausted retries" {
		t.Fatalf("expected run error message, got %q", updatedRun.Error)
	}

	dead, err := pg.ListDeadLetters(ctx, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != first.ID {
		t.Fatalf("expected dead letter list to contain task %s, got %+v", first.ID, dead)
	}
}

func TestReapExpiredLeases_Reclaims(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	// Lease with a duration so short it is already expired by the time we
	// check, simulating a crashed worker.
	if _, err := pg.LeaseTask(ctx, "worker-1", 1*time.Nanosecond); err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	n, err := pg.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 reclaimed lease, got %d", n)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	if tasks[0].Status != workflow.TaskPending {
		t.Fatalf("expected task pending after reap, got %s", tasks[0].Status)
	}
	if tasks[0].LeasedBy != nil {
		t.Fatalf("expected leased_by cleared after reap")
	}

	// And it should be leasable again.
	leased, err := pg.LeaseTask(ctx, "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask after reap: %v", err)
	}
	if leased == nil || leased.ID != first.ID {
		t.Fatalf("expected reclaimed task to be leasable, got %+v", leased)
	}
}

func TestReplayDeadLetter(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, first := startedRun(t, ctx, pg)

	owner := "worker-1"
	if _, err := pg.LeaseTask(ctx, owner, 30*time.Second); err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if err := pg.DeadLetter(ctx, DeadLetterCmd{
		TaskID: first.ID,
		RunID:  run.ID,
		Owner:  owner,
		ErrMsg: "boom",
	}); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	if err := pg.ReplayDeadLetter(ctx, first.ID); err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	tk := tasks[0]
	if tk.Status != workflow.TaskPending {
		t.Fatalf("expected task pending after replay, got %s", tk.Status)
	}
	if tk.Attempt != 0 {
		t.Fatalf("expected attempt reset to 0, got %d", tk.Attempt)
	}
	if tk.Error != "" {
		t.Fatalf("expected error cleared, got %q", tk.Error)
	}

	// Replaying a non-dead (or non-existent) task must return ErrNotFound.
	if err := pg.ReplayDeadLetter(ctx, first.ID); err == nil {
		t.Fatalf("expected ErrNotFound replaying an already-pending task")
	} else if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := pg.ReplayDeadLetter(ctx, newTaskID()); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for unknown task, got %v", err)
	}
}

func TestCancelRun(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)
	run, _ := startedRun(t, ctx, pg)

	if err := pg.CancelRun(ctx, run.ID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}

	updatedRun, err := pg.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if updatedRun.Status != workflow.RunCancelled {
		t.Fatalf("expected run cancelled, got %s", updatedRun.Status)
	}

	tasks, err := pg.GetTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetTasks: %v", err)
	}
	if tasks[0].Status != workflow.TaskCancelled {
		t.Fatalf("expected task cancelled, got %s", tasks[0].Status)
	}

	history, err := pg.GetHistory(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	last := history[len(history)-1]
	if last.Type != workflow.EventWorkflowCancelled {
		t.Fatalf("expected last event WorkflowCancelled, got %s", last.Type)
	}

	// Cancelling an unknown run must return ErrNotFound.
	if err := pg.CancelRun(ctx, newRunID()); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for unknown run, got %v", err)
	}
}

func TestGetStats(t *testing.T) {
	ctx := context.Background()
	pg := newTestStore(t)

	// One run that completes.
	run1, first1 := startedRun(t, ctx, pg)
	leased1, err := pg.LeaseTask(ctx, "w1", 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if leased1 == nil || leased1.ID != first1.ID {
		t.Fatalf("expected to lease first1")
	}
	if err := pg.Advance(ctx, AdvanceCmd{
		TaskID:    leased1.ID,
		RunID:     run1.ID,
		Owner:     "w1",
		Result:    rawJSON(`{}`),
		RunResult: rawJSON(`{}`),
	}); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	// One run that stays running with a pending task.
	startedRun(t, ctx, pg)

	// One run that gets dead-lettered (-> failed). LeaseTask returns the oldest
	// eligible pending task across all runs (not necessarily this run's), so we
	// dead-letter whichever task we actually leased — the fence requires the
	// task to be in the 'leased' state owned by us.
	_, _ = startedRun(t, ctx, pg)
	leased3, err := pg.LeaseTask(ctx, "w3", 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	if leased3 == nil {
		t.Fatalf("expected to lease a task to dead-letter")
	}
	if err := pg.DeadLetter(ctx, DeadLetterCmd{
		TaskID: leased3.ID,
		RunID:  leased3.RunID,
		Owner:  "w3",
		ErrMsg: "boom",
	}); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	// One run that gets cancelled.
	run4, _ := startedRun(t, ctx, pg)
	if err := pg.CancelRun(ctx, run4.ID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}

	stats, err := pg.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.RunsCompleted != 1 {
		t.Errorf("expected 1 completed run, got %d", stats.RunsCompleted)
	}
	if stats.RunsRunning != 1 {
		t.Errorf("expected 1 running run, got %d", stats.RunsRunning)
	}
	if stats.RunsFailed != 1 {
		t.Errorf("expected 1 failed run, got %d", stats.RunsFailed)
	}
	if stats.RunsCancelled != 1 {
		t.Errorf("expected 1 cancelled run, got %d", stats.RunsCancelled)
	}
	if stats.TasksPending != 1 {
		t.Errorf("expected 1 pending task, got %d", stats.TasksPending)
	}
	if stats.TasksDead != 1 {
		t.Errorf("expected 1 dead task, got %d", stats.TasksDead)
	}
}
