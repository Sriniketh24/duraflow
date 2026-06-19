// Package metrics holds the Prometheus collectors exposed at /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics bundles all Duraflow collectors.
type Metrics struct {
	TasksLeased    prometheus.Counter
	TasksCompleted prometheus.Counter
	TasksFailed    prometheus.Counter
	TasksDead      prometheus.Counter
	TasksReclaimed prometheus.Counter
	TimersFired    prometheus.Counter
	RunsStarted    prometheus.Counter
	RunsCompleted  prometheus.Counter
	ExecDuration   prometheus.Histogram
	LeaseLatency   prometheus.Histogram
	QueueDepth     prometheus.Gauge
}

// New registers and returns the collectors on the given registry.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		TasksLeased:    f.NewCounter(prometheus.CounterOpts{Name: "duraflow_tasks_leased_total", Help: "Activity tasks leased by workers."}),
		TasksCompleted: f.NewCounter(prometheus.CounterOpts{Name: "duraflow_tasks_completed_total", Help: "Activity tasks completed successfully."}),
		TasksFailed:    f.NewCounter(prometheus.CounterOpts{Name: "duraflow_tasks_failed_total", Help: "Activity executions that returned an error."}),
		TasksDead:      f.NewCounter(prometheus.CounterOpts{Name: "duraflow_tasks_dead_total", Help: "Tasks moved to the dead-letter queue."}),
		TasksReclaimed: f.NewCounter(prometheus.CounterOpts{Name: "duraflow_tasks_reclaimed_total", Help: "Leases reclaimed after visibility timeout (crashed workers)."}),
		TimersFired:    f.NewCounter(prometheus.CounterOpts{Name: "duraflow_timers_fired_total", Help: "Durable timers fired."}),
		RunsStarted:    f.NewCounter(prometheus.CounterOpts{Name: "duraflow_runs_started_total", Help: "Workflow runs started."}),
		RunsCompleted:  f.NewCounter(prometheus.CounterOpts{Name: "duraflow_runs_completed_total", Help: "Workflow runs completed."}),
		ExecDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "duraflow_activity_exec_seconds",
			Help:    "Activity handler execution duration.",
			Buckets: prometheus.DefBuckets,
		}),
		LeaseLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "duraflow_lease_latency_seconds",
			Help:    "Latency of the dequeue (lease) query.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
		QueueDepth: f.NewGauge(prometheus.GaugeOpts{Name: "duraflow_queue_depth", Help: "Pending activity tasks."}),
	}
}
