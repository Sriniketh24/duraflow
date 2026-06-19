// Package api implements Duraflow's HTTP API: workflow run management, the
// dead-letter queue, aggregate stats, health/readiness probes, and the
// Prometheus metrics endpoint.
package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/Sriniketh24/duraflow/internal/engine"
	"github.com/Sriniketh24/duraflow/internal/store"
	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// server holds the dependencies shared by all handlers.
type server struct {
	eng *engine.Engine
	st  store.Store
	reg *workflow.Registry
	log *slog.Logger
}

// NewServer builds the Duraflow HTTP API: a *http.ServeMux with all routes
// mounted, wrapped in CORS, logging, and panic-recovery middleware.
func NewServer(eng *engine.Engine, st store.Store, reg *workflow.Registry, promHandler http.Handler, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	s := &server{eng: eng, st: st, reg: reg, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/workflows/{name}/runs", s.handleStartRun)
	mux.HandleFunc("GET /v1/workflows", s.handleListWorkflows)
	mux.HandleFunc("GET /v1/runs", s.handleListRuns)
	mux.HandleFunc("GET /v1/runs/{id}", s.handleGetRun)
	mux.HandleFunc("GET /v1/runs/{id}/history", s.handleGetHistory)
	mux.HandleFunc("POST /v1/runs/{id}/cancel", s.handleCancelRun)
	mux.HandleFunc("GET /v1/dlq", s.handleListDeadLetters)
	mux.HandleFunc("POST /v1/dlq/{taskId}/replay", s.handleReplayDeadLetter)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promHandler)

	return withRecover(withLogging(log, withCORS(mux)))
}

// withCORS sets permissive CORS headers and short-circuits preflight requests.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Idempotency-Key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// statusRecorder captures the status code written by downstream handlers so
// the logging middleware can record it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// withLogging logs method, path, status, and duration for every request.
func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration", time.Since(start).String(),
		)
	})
}

// withRecover converts panics in downstream handlers into a 500 JSON response
// instead of crashing the server.
func withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeErr(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
