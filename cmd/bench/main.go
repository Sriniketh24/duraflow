// Command bench measures Duraflow against a real PostgreSQL instance:
//   - enqueue (StartWorkflow) latency percentiles
//   - end-to-end throughput (workflows completed per second)
//
// Usage: DATABASE_URL=... go run ./cmd/bench -runs 5000 -workers 16
// It operates on the bench_noop workflow and TRUNCATEs the tables first, so
// point it at a development database, never production data you care about.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sriniketh24/duraflow/internal/demo"
	"github.com/Sriniketh24/duraflow/internal/engine"
	"github.com/Sriniketh24/duraflow/internal/metrics"
	"github.com/Sriniketh24/duraflow/internal/store"
	"github.com/Sriniketh24/duraflow/internal/workflow"

	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	runs := flag.Int("runs", 5000, "number of workflow runs to execute")
	workers := flag.Int("workers", 16, "engine worker goroutines")
	conc := flag.Int("concurrency", 50, "concurrent enqueue goroutines")
	flag.Parse()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required")
		os.Exit(1)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()

	if err := truncate(ctx, dsn); err != nil {
		fmt.Fprintln(os.Stderr, "truncate:", err)
		os.Exit(1)
	}

	st, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}

	reg := workflow.NewRegistry()
	demo.Register(reg)
	m := metrics.New(prometheus.NewRegistry())
	eng := engine.New(st, reg, m, engine.Config{Workers: *workers, WorkerID: "bench"}, log)

	// ---- Phase 1: enqueue latency (engine not yet running) ----
	lat := make([]time.Duration, *runs)
	var idx int64 = -1
	var wg sync.WaitGroup
	enqStart := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&idx, 1)
				if int(i) >= *runs {
					return
				}
				input := json.RawMessage(`{}`)
				t0 := time.Now()
				if _, _, err := eng.StartWorkflow(ctx, "bench_noop", input, ""); err != nil {
					fmt.Fprintln(os.Stderr, "enqueue:", err)
					os.Exit(1)
				}
				lat[i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	enqElapsed := time.Since(enqStart)

	// ---- Phase 2: drain throughput ----
	ectx, cancel := context.WithCancel(ctx)
	drainStart := time.Now()
	go eng.Run(ectx)

	var drainElapsed time.Duration
	for {
		s, err := st.GetStats(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "stats:", err)
			os.Exit(1)
		}
		if s.RunsCompleted >= *runs {
			drainElapsed = time.Since(drainStart)
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()

	// ---- Report ----
	p := percentiles(lat)
	enqThroughput := float64(*runs) / enqElapsed.Seconds()
	drainThroughput := float64(*runs) / drainElapsed.Seconds()

	fmt.Println("====================  Duraflow benchmark  ====================")
	fmt.Printf("runs=%d  workers=%d  enqueue_concurrency=%d\n", *runs, *workers, *conc)
	fmt.Println("--------------------------------------------------------------")
	fmt.Printf("Enqueue (StartWorkflow) latency:  p50=%-8s p95=%-8s p99=%-8s max=%s\n",
		round(p.p50), round(p.p95), round(p.p99), round(p.max))
	fmt.Printf("Enqueue throughput:               %.0f workflows/sec (%s for %d)\n",
		enqThroughput, round(enqElapsed), *runs)
	fmt.Printf("End-to-end drain throughput:      %.0f workflows/sec (%s for %d)\n",
		drainThroughput, round(drainElapsed), *runs)
	fmt.Println("==============================================================")
}

type pcts struct{ p50, p95, p99, max time.Duration }

func percentiles(d []time.Duration) pcts {
	s := make([]time.Duration, len(d))
	copy(s, d)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(q float64) time.Duration {
		if len(s) == 0 {
			return 0
		}
		i := int(q * float64(len(s)-1))
		return s[i]
	}
	return pcts{p50: at(0.50), p95: at(0.95), p99: at(0.99), max: at(1.0)}
}

func round(d time.Duration) time.Duration {
	if d > time.Millisecond {
		return d.Round(time.Microsecond * 10)
	}
	return d.Round(time.Microsecond)
}

func truncate(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Tables may not exist yet on a fresh DB; ignore "does not exist" by creating
	// nothing here — Migrate runs after. Use IF EXISTS-safe truncation.
	_, err = pool.Exec(ctx, `DO $$ BEGIN
		IF to_regclass('public.events') IS NOT NULL THEN
			TRUNCATE events, activity_tasks, workflow_runs RESTART IDENTITY CASCADE;
		END IF;
	END $$;`)
	return err
}
