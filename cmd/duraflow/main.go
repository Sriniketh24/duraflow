// Command duraflow runs the workflow engine: it applies migrations, starts the
// worker pool, and serves the HTTP API. Engine and HTTP server share a single
// shutdown signal so SIGTERM drains in-flight tasks before exit.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Sriniketh24/duraflow/internal/api"
	"github.com/Sriniketh24/duraflow/internal/demo"
	"github.com/Sriniketh24/duraflow/internal/engine"
	"github.com/Sriniketh24/duraflow/internal/metrics"
	"github.com/Sriniketh24/duraflow/internal/store"
	"github.com/Sriniketh24/duraflow/internal/workflow"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	port := envDefault("PORT", "8080")
	workers := envInt("DURAFLOW_WORKERS", 8)
	leaseSecs := envInt("DURAFLOW_LEASE_SECONDS", 30)

	// Root context cancelled on SIGINT/SIGTERM; this is the drain trigger.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := store.NewPostgres(connectCtx, dsn)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	log.Info("migrations applied")

	reg := workflow.NewRegistry()
	demo.Register(reg)

	promReg := prometheus.NewRegistry()
	m := metrics.New(promReg)
	promHandler := promhttp.HandlerFor(promReg, promhttp.HandlerOpts{})

	eng := engine.New(st, reg, m, engine.Config{
		Workers:       workers,
		LeaseDuration: time.Duration(leaseSecs) * time.Second,
		WorkerID:      hostnameOr("duraflow"),
	}, log)

	// Engine runs until ctx is cancelled, then drains.
	engineDone := make(chan struct{})
	go func() {
		eng.Run(ctx)
		close(engineDone)
	}()

	handler := api.NewServer(eng, st, reg, promHandler, log)
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop() // trigger shutdown
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, draining")

	// Stop accepting HTTP first, then let the engine finish in-flight tasks.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	<-engineDone
	log.Info("clean shutdown complete")
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
