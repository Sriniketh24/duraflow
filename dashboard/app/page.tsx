'use client';

import { useCallback, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { api, ApiError, Run, Stats } from '@/lib/api';
import { formatDateTime, pillClass, shortId } from '@/lib/format';

const POLL_INTERVAL_MS = 3000;

export default function OverviewPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [runs, setRuns] = useState<Run[] | null>(null);
  const [workflows, setWorkflows] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [selectedWorkflow, setSelectedWorkflow] = useState('');
  const [inputJson, setInputJson] = useState('{}');
  const [idempotencyKey, setIdempotencyKey] = useState('');
  const [formError, setFormError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const loadDashboard = useCallback(async (isInitial = false) => {
    try {
      const [statsRes, runsRes] = await Promise.all([
        api.getStats(),
        api.getRuns(50),
      ]);
      setStats(statsRes);
      setRuns(runsRes.runs);
      setError(null);
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.message
          : 'Unexpected error loading dashboard data.'
      );
    } finally {
      if (isInitial) setLoading(false);
    }
  }, []);

  useEffect(() => {
    api
      .getWorkflows()
      .then((res) => {
        setWorkflows(res.workflows);
        if (res.workflows.length > 0) setSelectedWorkflow(res.workflows[0]);
      })
      .catch(() => {
        // Workflow list failure is non-fatal; the form will simply be empty.
      });
  }, []);

  useEffect(() => {
    loadDashboard(true);
    const interval = setInterval(() => loadDashboard(false), POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [loadDashboard]);

  async function handleStartWorkflow(e: React.FormEvent) {
    e.preventDefault();
    setFormError(null);

    if (!selectedWorkflow) {
      setFormError('Select a workflow to start.');
      return;
    }

    let parsedInput: object;
    try {
      parsedInput = inputJson.trim() ? JSON.parse(inputJson) : {};
    } catch {
      setFormError('Input must be valid JSON.');
      return;
    }

    setSubmitting(true);
    try {
      await api.startWorkflow(
        selectedWorkflow,
        parsedInput,
        idempotencyKey.trim() || undefined
      );
      setIdempotencyKey('');
      await loadDashboard(false);
    } catch (err) {
      setFormError(
        err instanceof ApiError ? err.message : 'Failed to start workflow.'
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <>
      <div className="page-header">
        <h1 className="page-title">Overview</h1>
        <p className="page-subtitle">
          Live status of workflow runs and the task queue.
        </p>
      </div>

      {error && (
        <div className="alert alert-error section">
          {error} Retrying every {POLL_INTERVAL_MS / 1000}s.
        </div>
      )}

      {loading ? (
        <p className="loading-text">Loading dashboard…</p>
      ) : (
        <>
          <div className="stats-grid section">
            <StatCard label="Running" value={stats?.runs_running} variant="running" />
            <StatCard label="Completed" value={stats?.runs_completed} variant="completed" />
            <StatCard label="Failed" value={stats?.runs_failed} variant="failed" />
            <StatCard label="Cancelled" value={stats?.runs_cancelled} />
            <StatCard label="Tasks pending" value={stats?.tasks_pending} variant="pending" />
            <StatCard label="Tasks leased" value={stats?.tasks_leased} variant="running" />
            <StatCard label="Tasks dead" value={stats?.tasks_dead} variant="failed" />
          </div>

          <div className="section card">
            <h2 className="card-title">Start workflow</h2>
            <form onSubmit={handleStartWorkflow}>
              <div className="form-row-inline">
                <div className="form-row">
                  <label htmlFor="workflow">Workflow</label>
                  <select
                    id="workflow"
                    value={selectedWorkflow}
                    onChange={(e) => setSelectedWorkflow(e.target.value)}
                  >
                    {workflows.length === 0 && <option value="">No workflows found</option>}
                    {workflows.map((wf) => (
                      <option key={wf} value={wf}>
                        {wf}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="form-row">
                  <label htmlFor="idempotency-key">Idempotency key (optional)</label>
                  <input
                    id="idempotency-key"
                    type="text"
                    value={idempotencyKey}
                    onChange={(e) => setIdempotencyKey(e.target.value)}
                    placeholder="e.g. order-12345"
                  />
                </div>
              </div>
              <div className="form-row">
                <label htmlFor="input-json">Input (JSON)</label>
                <textarea
                  id="input-json"
                  value={inputJson}
                  onChange={(e) => setInputJson(e.target.value)}
                  spellCheck={false}
                />
                <span className="helper-text">
                  Passed as the run&apos;s input payload to the workflow.
                </span>
              </div>
              {formError && (
                <div className="alert alert-error" style={{ marginBottom: 14 }}>
                  {formError}
                </div>
              )}
              <button type="submit" className="btn btn-primary" disabled={submitting}>
                {submitting ? 'Starting…' : 'Start workflow'}
              </button>
            </form>
          </div>

          <div className="section">
            <div className="toolbar">
              <h2 className="card-title" style={{ margin: 0 }}>
                Recent runs
              </h2>
              <span className="live-dot">Live</span>
            </div>
            <div className="card" style={{ padding: 0 }}>
              <RunsTable runs={runs} />
            </div>
          </div>
        </>
      )}
    </>
  );
}

function StatCard({
  label,
  value,
  variant,
}: {
  label: string;
  value?: number;
  variant?: string;
}) {
  return (
    <div className={`stat-card${variant ? ` ${variant}` : ''}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value ?? '—'}</div>
    </div>
  );
}

function RunsTable({ runs }: { runs: Run[] | null }) {
  if (!runs || runs.length === 0) {
    return <div className="empty-state">No runs yet. Start a workflow above.</div>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Run ID</th>
            <th>Workflow</th>
            <th>Status</th>
            <th>Step</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <RunRow key={run.id} run={run} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RunRow({ run }: { run: Run }) {
  const router = useRouter();
  return (
    <tr className="row-link" onClick={() => router.push(`/runs/${run.id}`)}>
      <td className="mono">{shortId(run.id)}</td>
      <td>{run.workflow_name}</td>
      <td>
        <span className={pillClass(run.status)}>{run.status}</span>
      </td>
      <td className="mono">
        {run.current_step}/{run.total_steps}
      </td>
      <td className="mono">{formatDateTime(run.created_at)}</td>
    </tr>
  );
}
