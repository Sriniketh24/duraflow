package engine_test

// Integration tests run against a real PostgreSQL instance. They are skipped
// unless DATABASE_URL is set (CI provides a postgres service container; locally
// use `make pg` then DATABASE_URL=... go test ./internal/engine -race).
//
// The centerpiece is TestChaosZeroLoss: it induces real worker "crashes" via
// lease expiry mid-execution and proves every workflow still completes exactly
// once — no lost jobs, no double advance.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sriniketh24/duraflow/internal/demo"
	"github.com/Sriniketh24/duraflow/internal/engine"
	"github.com/Sriniketh24/duraflow/internal/metrics"
	"github.com/Sriniketh24/duraflow/internal/store"
	"github.com/Sriniketh24/duraflow/internal/workflow"

	"github.com/prometheus/client_golang/prometheus"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to run integration tests")
	}
	return dsn
}

func freshStore(t *testing.T, dsn string) store.Store {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	st, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE events, activity_tasks, workflow_runs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	pool.Close()
	t.Cleanup(st.Close)
	return st
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// waitFor polls until cond() is true or the deadline elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestHappyPath(t *testing.T) {
	dsn := dsnOrSkip(t)
	st := freshStore(t, dsn)
	reg := workflow.NewRegistry()
	demo.Register(reg)
	eng := engine.New(st, reg, metrics.New(prometheus.NewRegistry()),
		engine.Config{Workers: 4, WorkerID: "happy"}, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eng.Run(ctx)

	run, _, err := eng.StartWorkflow(ctx, "order_processing", json.RawMessage(`{"order_id":"A1"}`), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, 15*time.Second, func() bool {
		r, err := st.GetRun(ctx, run.ID)
		return err == nil && r.Status == workflow.RunCompleted
	})

	tasks, _ := st.GetTasks(ctx, run.ID)
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
	for _, tk := range tasks {
		if tk.Status != workflow.TaskCompleted {
			t.Fatalf("step %d not completed: %s", tk.StepIndex, tk.Status)
		}
	}
	events, _ := st.GetHistory(ctx, run.ID)
	if len(events) == 0 {
		t.Fatal("expected event history")
	}
}

func TestRetryThenSucceed(t *testing.T) {
	dsn := dsnOrSkip(t)
	st := freshStore(t, dsn)
	reg := workflow.NewRegistry()
	demo.Register(reg)
	// Override backoff to keep the test fast.
	eng := engine.New(st, reg, metrics.New(prometheus.NewRegistry()),
		engine.Config{Workers: 4, WorkerID: "retry", BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond}, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eng.Run(ctx)

	run, _, err := eng.StartWorkflow(ctx, "retry_demo", json.RawMessage(`{"key":"k1","fail_times":3}`), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		r, err := st.GetRun(ctx, run.ID)
		return err == nil && r.Status == workflow.RunCompleted
	})
	tasks, _ := st.GetTasks(ctx, run.ID)
	if tasks[0].Attempt < 4 {
		t.Fatalf("expected >=4 attempts (3 failures + success), got %d", tasks[0].Attempt)
	}
}

func TestDeadLetter(t *testing.T) {
	dsn := dsnOrSkip(t)
	st := freshStore(t, dsn)
	reg := workflow.NewRegistry()
	demo.Register(reg)
	eng := engine.New(st, reg, metrics.New(prometheus.NewRegistry()),
		engine.Config{Workers: 4, WorkerID: "dlq", BackoffBase: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond}, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eng.Run(ctx)

	run, _, err := eng.StartWorkflow(ctx, "dlq_demo", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		r, err := st.GetRun(ctx, run.ID)
		return err == nil && r.Status == workflow.RunFailed
	})
	dl, _ := st.ListDeadLetters(ctx, 10)
	found := false
	for _, tk := range dl {
		if tk.RunID == run.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the exhausted task in the dead-letter queue")
	}
}

func TestDurableTimer(t *testing.T) {
	dsn := dsnOrSkip(t)
	st := freshStore(t, dsn)
	reg := workflow.NewRegistry()
	reg.RegisterActivity("noop", func(_ context.Context, in json.RawMessage) (json.RawMessage, error) {
		if len(in) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return in, nil
	})
	reg.RegisterWorkflow(workflow.Definition{
		Name: "short_timer",
		Steps: []workflow.Step{
			{Activity: "noop"},
			{Timer: 800 * time.Millisecond},
			{Activity: "noop"},
		},
	})
	eng := engine.New(st, reg, metrics.New(prometheus.NewRegistry()),
		engine.Config{Workers: 4, WorkerID: "timer"}, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eng.Run(ctx)

	start := time.Now()
	run, _, err := eng.StartWorkflow(ctx, "short_timer", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, 15*time.Second, func() bool {
		r, err := st.GetRun(ctx, run.ID)
		return err == nil && r.Status == workflow.RunCompleted
	})
	if elapsed := time.Since(start); elapsed < 800*time.Millisecond {
		t.Fatalf("workflow completed in %s, before the durable timer elapsed", elapsed)
	}
}

func TestIdempotentStart(t *testing.T) {
	dsn := dsnOrSkip(t)
	st := freshStore(t, dsn)
	reg := workflow.NewRegistry()
	demo.Register(reg)
	eng := engine.New(st, reg, metrics.New(prometheus.NewRegistry()),
		engine.Config{Workers: 2, WorkerID: "idem"}, quietLogger())
	ctx := context.Background()

	r1, c1, err := eng.StartWorkflow(ctx, "order_processing", json.RawMessage(`{}`), "dedup-key-1")
	if err != nil || !c1 {
		t.Fatalf("first start: created=%v err=%v", c1, err)
	}
	r2, c2, err := eng.StartWorkflow(ctx, "order_processing", json.RawMessage(`{}`), "dedup-key-1")
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	if c2 {
		t.Fatal("second start with same idempotency key should not create a new run")
	}
	if r1.ID != r2.ID {
		t.Fatalf("idempotency returned a different run: %s vs %s", r1.ID, r2.ID)
	}
}

// crashFirst fails-by-stalling on the first execution of each key (sleeping
// past the lease so the reaper reclaims the task mid-flight, simulating a hard
// worker crash), then succeeds on the reclaim. It records total executions so
// the test can prove re-execution actually occurred.
type crashFirst struct {
	seen      sync.Map // key -> struct{}
	execCount atomic.Int64
	lease     time.Duration
}

func (c *crashFirst) fn(_ context.Context, in json.RawMessage) (json.RawMessage, error) {
	c.execCount.Add(1)
	var req struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(in, &req)
	if _, loaded := c.seen.LoadOrStore(req.Key, struct{}{}); !loaded {
		// First time we ever see this key: stall past the lease so this worker's
		// lease expires and the task is reclaimed and re-run elsewhere.
		time.Sleep(c.lease + 500*time.Millisecond)
		// When we wake, the lease is gone; our Advance will be fenced to a no-op.
		return json.RawMessage(`{"stalled":true}`), nil
	}
	return json.RawMessage(`{"recovered":true}`), nil
}

func TestChaosZeroLoss(t *testing.T) {
	dsn := dsnOrSkip(t)
	st := freshStore(t, dsn)

	const n = 40
	lease := 1 * time.Second
	cf := &crashFirst{lease: lease}

	reg := workflow.NewRegistry()
	reg.RegisterActivity("crash_first", cf.fn)
	reg.RegisterWorkflow(workflow.Definition{
		Name:  "chaos",
		Steps: []workflow.Step{{Activity: "crash_first", MaxAttempts: 10}},
	})

	m := metrics.New(prometheus.NewRegistry())
	eng := engine.New(st, reg, m, engine.Config{
		Workers:       12,
		LeaseDuration: lease,
		ReapInterval:  200 * time.Millisecond,
		PollInterval:  20 * time.Millisecond,
		WorkerID:      "chaos",
	}, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runIDs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		input := json.RawMessage(fmt.Sprintf(`{"key":"chaos-%d"}`, i))
		r, _, err := eng.StartWorkflow(ctx, "chaos", input, "")
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		runIDs = append(runIDs, r.ID)
	}

	start := time.Now()
	go eng.Run(ctx)

	// Every workflow must reach completed despite induced crashes.
	waitFor(t, 90*time.Second, func() bool {
		s, err := st.GetStats(ctx)
		return err == nil && s.RunsCompleted >= n
	})
	recovery := time.Since(start)

	// 1) Zero loss: every run completed.
	completed := 0
	for _, id := range runIDs {
		r, err := st.GetRun(ctx, id)
		if err != nil {
			t.Fatalf("get run: %v", err)
		}
		if r.Status != workflow.RunCompleted {
			t.Fatalf("run %s ended in %s, expected completed (LOST JOB)", id, r.Status)
		}
		completed++
	}
	if completed != n {
		t.Fatalf("expected %d completed, got %d", n, completed)
	}

	// 2) Crashes actually happened: total executions exceed n (re-runs occurred).
	if got := cf.execCount.Load(); got <= int64(n) {
		t.Fatalf("expected >%d executions from re-runs, got %d (crash scenario did not trigger)", n, got)
	}

	// 3) No double advance: each run has exactly one ActivityCompleted event,
	//    proving lease fencing discarded the stalled workers' commits.
	for _, id := range runIDs {
		events, err := st.GetHistory(ctx, id)
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		completes := 0
		for _, ev := range events {
			if ev.Type == workflow.EventActivityCompleted {
				completes++
			}
		}
		if completes != 1 {
			t.Fatalf("run %s has %d ActivityCompleted events, expected exactly 1 (double advance!)", id, completes)
		}
	}

	t.Logf("chaos passed: %d/%d runs completed, %d total executions (%.1fx re-run), recovery wall-time %s",
		completed, n, cf.execCount.Load(), float64(cf.execCount.Load())/float64(n), recovery.Round(time.Millisecond))
}
