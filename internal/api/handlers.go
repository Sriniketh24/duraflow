package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Sriniketh24/duraflow/internal/store"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// writeJSON encodes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr writes a JSON error response: {"error": "..."}.
func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// writeStoreErr maps store errors to HTTP status codes, defaulting to 500.
func writeStoreErr(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	writeErr(w, http.StatusInternalServerError, err.Error())
}

// parseLimit reads the "limit" query parameter, defaulting and capping it.
func parseLimit(r *http.Request) int {
	limit := defaultListLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return limit
}

// startRunRequest is the body accepted by POST /v1/workflows/{name}/runs.
type startRunRequest struct {
	Input          json.RawMessage `json:"input"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// handleStartRun starts a new workflow run (or returns the existing one if an
// idempotency key was already used).
func (s *server) handleStartRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var req startRunRequest
	if r.Body != nil {
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
			if !isEmptyBodyErr(err) {
				writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
				return
			}
		}
	}

	idempotencyKey := req.IdempotencyKey
	if h := r.Header.Get("Idempotency-Key"); h != "" {
		idempotencyKey = h
	}

	run, created, err := s.eng.StartWorkflow(r.Context(), name, req.Input, idempotencyKey)
	if err != nil {
		if isUnknownWorkflowErr(err) {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, run)
}

// isEmptyBodyErr reports whether err is just an empty request body (io.EOF),
// which is acceptable — input/idempotency_key are both optional.
func isEmptyBodyErr(err error) bool {
	return err.Error() == "EOF"
}

// isUnknownWorkflowErr reports whether err originates from an unrecognized
// workflow name (engine.StartWorkflow returns a plain fmt.Errorf, not a
// sentinel, so we match on the message it documents).
func isUnknownWorkflowErr(err error) bool {
	msg := err.Error()
	return len(msg) >= 7 && containsUnknown(msg)
}

func containsUnknown(s string) bool {
	const needle = "unknown workflow"
	for i := 0; i+len(needle) <= len(s); i++ {
		if s[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// handleListWorkflows returns all registered workflow names.
func (s *server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"workflows": s.reg.Workflows()})
}

// handleListRuns returns recent runs, most recent first.
func (s *server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)
	runs, err := s.st.ListRuns(r.Context(), limit)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

// handleGetRun returns a single run along with its tasks.
func (s *server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.st.GetRun(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	tasks, err := s.st.GetTasks(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run, "tasks": tasks})
}

// handleGetHistory returns the append-only event history for a run.
func (s *server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	events, err := s.st.GetHistory(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// handleCancelRun cancels a running run.
func (s *server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.st.CancelRun(r.Context(), id); err != nil {
		writeStoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListDeadLetters returns tasks currently in the dead-letter queue.
func (s *server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)
	tasks, err := s.st.ListDeadLetters(r.Context(), limit)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

// handleReplayDeadLetter resets a dead-lettered task to pending.
func (s *server) handleReplayDeadLetter(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskId")
	if err := s.st.ReplayDeadLetter(r.Context(), taskID); err != nil {
		writeStoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStats returns aggregate run/task counts.
func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.st.GetStats(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// handleHealthz is a liveness probe: it never touches the database.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is a readiness probe: it pings the store.
func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.st.Ping(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "not ready: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
