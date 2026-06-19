package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sriniketh24/duraflow/internal/engine"
	"github.com/Sriniketh24/duraflow/internal/store"
	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// fakeStore is a minimal in-memory implementation of store.Store sufficient
// to exercise the API handlers. Methods not reached by the handlers under
// test return zero values.
type fakeStore struct {
	mu             sync.Mutex
	runs           map[string]workflow.Run
	tasks          map[string][]workflow.Task
	events         map[string][]workflow.Event
	idempotency    map[string]string // key -> run id
	deadLetters    []workflow.Task
	pingErr        error
	cancelledRuns  map[string]bool
	replayedTaskID string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		runs:          make(map[string]workflow.Run),
		tasks:         make(map[string][]workflow.Task),
		events:        make(map[string][]workflow.Event),
		idempotency:   make(map[string]string),
		cancelledRuns: make(map[string]bool),
	}
}

func (f *fakeStore) Migrate(ctx context.Context) error { return nil }

func (f *fakeStore) StartRun(ctx context.Context, cmd store.StartRunCmd) (workflow.Run, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if cmd.IdempotencyKey != nil {
		if existingID, ok := f.idempotency[*cmd.IdempotencyKey]; ok {
			return f.runs[existingID], false, nil
		}
	}

	run := workflow.Run{
		ID:             cmd.RunID,
		WorkflowName:   cmd.WorkflowName,
		Input:          cmd.Input,
		Status:         workflow.RunRunning,
		CurrentStep:    0,
		TotalSteps:     cmd.TotalSteps,
		IdempotencyKey: cmd.IdempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	f.runs[run.ID] = run
	f.tasks[run.ID] = []workflow.Task{
		{
			ID:           cmd.First.ID,
			RunID:        run.ID,
			StepIndex:    cmd.First.StepIndex,
			ActivityName: cmd.First.ActivityName,
			Kind:         cmd.First.Kind,
			Input:        cmd.First.Input,
			Status:       workflow.TaskPending,
			MaxAttempts:  cmd.First.MaxAttempts,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		},
	}
	if cmd.IdempotencyKey != nil {
		f.idempotency[*cmd.IdempotencyKey] = run.ID
	}
	return run, true, nil
}

func (f *fakeStore) LeaseTask(ctx context.Context, workerID string, lease time.Duration) (*workflow.Task, error) {
	return nil, nil
}

func (f *fakeStore) Advance(ctx context.Context, cmd store.AdvanceCmd) error { return nil }

func (f *fakeStore) Retry(ctx context.Context, cmd store.RetryCmd) error { return nil }

func (f *fakeStore) DeadLetter(ctx context.Context, cmd store.DeadLetterCmd) error { return nil }

func (f *fakeStore) ReapExpiredLeases(ctx context.Context) (int, error) { return 0, nil }

func (f *fakeStore) GetRun(ctx context.Context, id string) (workflow.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.runs[id]
	if !ok {
		return workflow.Run{}, store.ErrNotFound
	}
	return run, nil
}

func (f *fakeStore) GetTasks(ctx context.Context, runID string) ([]workflow.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tasks[runID], nil
}

func (f *fakeStore) GetHistory(ctx context.Context, runID string) ([]workflow.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.events[runID], nil
}

func (f *fakeStore) ListRuns(ctx context.Context, limit int) ([]workflow.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]workflow.Run, 0, len(f.runs))
	for _, r := range f.runs {
		out = append(out, r)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeStore) ListDeadLetters(ctx context.Context, limit int) ([]workflow.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.deadLetters
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeStore) ReplayDeadLetter(ctx context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.deadLetters {
		if t.ID == taskID {
			f.replayedTaskID = taskID
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeStore) CancelRun(ctx context.Context, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.runs[runID]; !ok {
		return store.ErrNotFound
	}
	f.cancelledRuns[runID] = true
	return nil
}

func (f *fakeStore) GetStats(ctx context.Context) (store.Stats, error) {
	return store.Stats{RunsRunning: len(f.runs)}, nil
}

func (f *fakeStore) Ping(ctx context.Context) error { return f.pingErr }

func (f *fakeStore) Close() {}

var _ store.Store = (*fakeStore)(nil)

// newTestServer builds a real Engine wired to a fakeStore plus a Registry
// with one trivial demo workflow ("demo_wf" -> activity "demo_activity"),
// and returns the fully-assembled API handler.
func newTestServer(t *testing.T) (http.Handler, *fakeStore) {
	t.Helper()
	fs := newFakeStore()
	reg := workflow.NewRegistry()
	reg.RegisterActivity("demo_activity", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	reg.RegisterWorkflow(workflow.Definition{
		Name:  "demo_wf",
		Steps: []workflow.Step{{Activity: "demo_activity"}},
	})

	eng := engine.New(fs, reg, nil, engine.Config{}, nil)
	handler := NewServer(eng, fs, reg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), nil)
	return handler, fs
}

func TestStartRun_Success(t *testing.T) {
	handler, _ := newTestServer(t)

	body := bytes.NewBufferString(`{"input":{"foo":"bar"}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/demo_wf/runs", body)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var run workflow.Run
	if err := json.Unmarshal(rec.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if run.ID == "" {
		t.Fatal("expected non-empty run id")
	}
	if run.WorkflowName != "demo_wf" {
		t.Fatalf("expected workflow_name demo_wf, got %q", run.WorkflowName)
	}
}

func TestStartRun_UnknownWorkflow(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/nonexistent/runs", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestStartRun_BadJSON(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/demo_wf/runs", bytes.NewBufferString(`{not valid json`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestStartRun_IdempotencyHeader(t *testing.T) {
	handler, _ := newTestServer(t)

	req1 := httptest.NewRequest(http.MethodPost, "/v1/workflows/demo_wf/runs", bytes.NewBufferString(`{}`))
	req1.Header.Set("Idempotency-Key", "my-key")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 on first call, got %d: %s", rec1.Code, rec1.Body.String())
	}
	var run1 workflow.Run
	_ = json.Unmarshal(rec1.Body.Bytes(), &run1)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/workflows/demo_wf/runs", bytes.NewBufferString(`{}`))
	req2.Header.Set("Idempotency-Key", "my-key")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on idempotent replay, got %d: %s", rec2.Code, rec2.Body.String())
	}
	var run2 workflow.Run
	_ = json.Unmarshal(rec2.Body.Bytes(), &run2)
	if run1.ID != run2.ID {
		t.Fatalf("expected same run id, got %q and %q", run1.ID, run2.ID)
	}
}

func TestGetRun(t *testing.T) {
	handler, fs := newTestServer(t)

	runID := uuid.NewString()
	fs.mu.Lock()
	fs.runs[runID] = workflow.Run{ID: runID, WorkflowName: "demo_wf", Status: workflow.RunRunning}
	fs.tasks[runID] = []workflow.Task{{ID: "t1", RunID: runID, StepIndex: 0}}
	fs.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/runs/"+runID, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Run   workflow.Run    `json:"run"`
		Tasks []workflow.Task `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Run.ID != runID {
		t.Fatalf("expected run id %q, got %q", runID, resp.Run.ID)
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(resp.Tasks))
	}
}

func TestGetRun_NotFound(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/runs/does-not-exist", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHealthz(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["status"] != "ok" {
		t.Fatalf("expected status ok, got %q", resp["status"])
	}
}

func TestReadyz(t *testing.T) {
	handler, fs := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	fs.pingErr = errors.New("db down")
	req2 := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec2.Code)
	}
}

func TestReplayDeadLetter_NotFound(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/dlq/unknown-task/replay", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReplayDeadLetter_Success(t *testing.T) {
	handler, fs := newTestServer(t)
	fs.deadLetters = []workflow.Task{{ID: "dead-task-1", Status: workflow.TaskDead}}

	req := httptest.NewRequest(http.MethodPost, "/v1/dlq/dead-task-1/replay", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
	if fs.replayedTaskID != "dead-task-1" {
		t.Fatalf("expected replay to be recorded, got %q", fs.replayedTaskID)
	}
}

func TestCancelRun(t *testing.T) {
	handler, fs := newTestServer(t)
	runID := uuid.NewString()
	fs.mu.Lock()
	fs.runs[runID] = workflow.Run{ID: runID, Status: workflow.RunRunning}
	fs.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v1/runs/"+runID+"/cancel", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestListWorkflows(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Workflows []string `json:"workflows"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Workflows) != 1 || resp.Workflows[0] != "demo_wf" {
		t.Fatalf("expected [demo_wf], got %v", resp.Workflows)
	}
}

func TestStats(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestOptionsPreflight(t *testing.T) {
	handler, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodOptions, "/v1/runs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected CORS header '*', got %q", got)
	}
}
