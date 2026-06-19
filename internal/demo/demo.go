// Package demo registers the built-in workflows and activities that make a
// fresh Duraflow deployment self-demonstrating: an order pipeline, a durable
// timer, a retry-then-succeed flow, a guaranteed dead-letter, and a single-step
// no-op used by the benchmark harness.
package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Sriniketh24/duraflow/internal/workflow"
)

// Register installs all demo workflows and activities into the registry.
func Register(r *workflow.Registry) {
	a := &activities{}

	r.RegisterActivity("noop", a.noop)
	r.RegisterActivity("validate_order", a.validateOrder)
	r.RegisterActivity("charge_payment", a.chargePayment)
	r.RegisterActivity("ship_order", a.shipOrder)
	r.RegisterActivity("fail_n_times", a.failNTimes)
	r.RegisterActivity("always_fail", a.alwaysFail)

	r.RegisterWorkflow(workflow.Definition{
		Name: "order_processing",
		Steps: []workflow.Step{
			{Activity: "validate_order"},
			{Activity: "charge_payment"},
			{Activity: "ship_order"},
		},
	})
	r.RegisterWorkflow(workflow.Definition{
		Name: "timer_demo",
		Steps: []workflow.Step{
			{Activity: "noop"},
			{Timer: 5 * time.Second},
			{Activity: "noop"},
		},
	})
	r.RegisterWorkflow(workflow.Definition{
		Name: "retry_demo",
		Steps: []workflow.Step{
			{Activity: "fail_n_times", MaxAttempts: 6},
		},
	})
	r.RegisterWorkflow(workflow.Definition{
		Name: "dlq_demo",
		Steps: []workflow.Step{
			{Activity: "always_fail", MaxAttempts: 3},
		},
	})
	r.RegisterWorkflow(workflow.Definition{
		Name:  "bench_noop",
		Steps: []workflow.Step{{Activity: "noop"}},
	})
}

type activities struct {
	counters sync.Map // key -> *int32-ish attempt counter for fail_n_times
}

func (a *activities) noop(_ context.Context, in json.RawMessage) (json.RawMessage, error) {
	if len(in) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return in, nil
}

func (a *activities) validateOrder(_ context.Context, in json.RawMessage) (json.RawMessage, error) {
	var m map[string]any
	_ = json.Unmarshal(in, &m)
	return json.RawMessage(`{"validated":true}`), nil
}

func (a *activities) chargePayment(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"charged":true}`), nil
}

func (a *activities) shipOrder(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"shipped":true,"tracking":"DF-DEMO"}`), nil
}

// failNTimes fails the first N attempts (tracked in-memory by key), then
// succeeds — demonstrating retry-with-backoff converging to success.
func (a *activities) failNTimes(_ context.Context, in json.RawMessage) (json.RawMessage, error) {
	var req struct {
		Key       string `json:"key"`
		FailTimes int    `json:"fail_times"`
	}
	_ = json.Unmarshal(in, &req)
	if req.Key == "" {
		req.Key = "default"
	}
	if req.FailTimes == 0 {
		req.FailTimes = 2
	}
	v, _ := a.counters.LoadOrStore(req.Key, new(counter))
	c := v.(*counter)
	n := c.inc()
	if n <= req.FailTimes {
		return nil, fmt.Errorf("transient failure %d/%d for %q", n, req.FailTimes, req.Key)
	}
	return json.RawMessage(fmt.Sprintf(`{"succeeded_on_attempt":%d}`, n)), nil
}

func (a *activities) alwaysFail(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return nil, fmt.Errorf("permanent failure: this activity always fails (dead-letter demo)")
}

type counter struct {
	mu sync.Mutex
	n  int
}

func (c *counter) inc() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return c.n
}
