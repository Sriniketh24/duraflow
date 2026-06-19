package engine

import (
	"testing"
	"time"

	"github.com/Sriniketh24/duraflow/internal/workflow"
)

func newTestEngine() *Engine {
	return New(nil, workflow.NewRegistry(), nil, Config{
		BackoffBase:    100 * time.Millisecond,
		BackoffMax:     2 * time.Second,
		DefaultRetries: 5,
	}, nil)
}

func TestBackoffNeverExceedsMax(t *testing.T) {
	e := newTestEngine()
	for attempt := 1; attempt <= 30; attempt++ {
		for i := 0; i < 100; i++ {
			b := e.backoff(attempt)
			if b < 0 {
				t.Fatalf("attempt %d: negative backoff %v", attempt, b)
			}
			if b > e.cfg.BackoffMax {
				t.Fatalf("attempt %d: backoff %v exceeds max %v", attempt, b, e.cfg.BackoffMax)
			}
		}
	}
}

func TestBackoffUpperBoundGrows(t *testing.T) {
	// With full jitter the value is random in [0, exp]; sample the observed max
	// across many draws and confirm earlier attempts cannot exceed the
	// (uncapped) exponential ceiling of their attempt number.
	e := newTestEngine()
	ceil := func(attempt int) time.Duration {
		exp := e.cfg.BackoffBase << (attempt - 1)
		if exp <= 0 || exp > e.cfg.BackoffMax {
			exp = e.cfg.BackoffMax
		}
		return exp
	}
	for attempt := 1; attempt <= 6; attempt++ {
		c := ceil(attempt)
		for i := 0; i < 200; i++ {
			if b := e.backoff(attempt); b > c {
				t.Fatalf("attempt %d draw %v exceeded its ceiling %v", attempt, b, c)
			}
		}
	}
}

func TestJitterWithinExpectedBand(t *testing.T) {
	e := newTestEngine()
	base := 100 * time.Millisecond
	lo := base - base/10
	hi := base + base/10
	for i := 0; i < 1000; i++ {
		j := e.jitter(base)
		if j < lo-time.Millisecond || j > hi+time.Millisecond {
			t.Fatalf("jitter %v outside band [%v,%v]", j, lo, hi)
		}
	}
}

func TestBuildTaskActivityVsTimer(t *testing.T) {
	e := newTestEngine()
	def := workflow.Definition{
		Name: "wf",
		Steps: []workflow.Step{
			{Activity: "do_thing"},
			{Timer: 5 * time.Second},
		},
	}
	act := e.buildTask(def, 0, []byte(`{}`))
	if act.Kind != workflow.KindActivity || act.ActivityName != "do_thing" {
		t.Fatalf("expected activity task, got %+v", act)
	}
	if act.AvailableAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("activity should be available ~now, got %v", act.AvailableAt)
	}

	tim := e.buildTask(def, 1, []byte(`{}`))
	if tim.Kind != workflow.KindTimer {
		t.Fatalf("expected timer task, got kind %v", tim.Kind)
	}
	if tim.MaxAttempts != 1 {
		t.Fatalf("timers must not retry; max attempts = %d", tim.MaxAttempts)
	}
	if !tim.AvailableAt.After(time.Now().Add(4 * time.Second)) {
		t.Fatalf("timer should be available ~5s out, got %v", tim.AvailableAt)
	}
}

func TestBuildTaskRespectsStepMaxAttempts(t *testing.T) {
	e := newTestEngine()
	def := workflow.Definition{Name: "wf", Steps: []workflow.Step{{Activity: "a", MaxAttempts: 9}}}
	if got := e.buildTask(def, 0, nil).MaxAttempts; got != 9 {
		t.Fatalf("expected per-step max attempts 9, got %d", got)
	}
}
