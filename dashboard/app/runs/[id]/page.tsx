'use client';

import { useCallback, useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { api, ApiError, Event, Run, Task } from '@/lib/api';
import { formatDateTime, pillClass, shortId } from '@/lib/format';

const POLL_INTERVAL_MS = 3000;

export default function RunDetailPage() {
  const params = useParams<{ id: string }>();
  const runId = params.id;

  const [run, setRun] = useState<Run | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [cancelling, setCancelling] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);

  const load = useCallback(
    async (isInitial = false) => {
      try {
        const [detail, history] = await Promise.all([
          api.getRun(runId),
          api.getRunHistory(runId),
        ]);
        setRun(detail.run);
        setTasks(detail.tasks);
        setEvents(history.events);
        setError(null);
      } catch (err) {
        setError(
          err instanceof ApiError
            ? err.message
            : 'Unexpected error loading run details.'
        );
      } finally {
        if (isInitial) setLoading(false);
      }
    },
    [runId]
  );

  useEffect(() => {
    load(true);
    const interval = setInterval(() => load(false), POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [load]);

  async function handleCancel() {
    setCancelling(true);
    setCancelError(null);
    try {
      await api.cancelRun(runId);
      await load(false);
    } catch (err) {
      setCancelError(
        err instanceof ApiError ? err.message : 'Failed to cancel run.'
      );
    } finally {
      setCancelling(false);
    }
  }

  return (
    <>
      <Link href="/" className="back-link">
        &larr; Back to overview
      </Link>

      {loading ? (
        <p className="loading-text">Loading run…</p>
      ) : error ? (
        <div className="alert alert-error">{error}</div>
      ) : run ? (
        <>
          <div className="detail-header">
            <div>
              <h1 className="page-title">
                {run.workflow_name}{' '}
                <span className="mono" style={{ fontWeight: 400, fontSize: 14 }}>
                  {shortId(run.id, 12)}
                </span>
              </h1>
              <p className="page-subtitle">Run details and execution history</p>
            </div>
            {run.status === 'running' && (
              <button
                className="btn btn-danger"
                onClick={handleCancel}
                disabled={cancelling}
              >
                {cancelling ? 'Cancelling…' : 'Cancel run'}
              </button>
            )}
          </div>

          {cancelError && (
            <div className="alert alert-error section">{cancelError}</div>
          )}

          <div className="card section">
            <h2 className="card-title">Summary</h2>
            <div className="summary-grid">
              <SummaryItem label="Status">
                <span className={pillClass(run.status)}>{run.status}</span>
              </SummaryItem>
              <SummaryItem label="Progress">
                {run.current_step}/{run.total_steps}
              </SummaryItem>
              <SummaryItem label="Created">{formatDateTime(run.created_at)}</SummaryItem>
              <SummaryItem label="Updated">{formatDateTime(run.updated_at)}</SummaryItem>
            </div>
            {run.error && (
              <div style={{ marginTop: 16 }}>
                <div className="summary-item-label">Error</div>
                <div className="error-box">{run.error}</div>
              </div>
            )}
          </div>

          <div className="section">
            <h2 className="card-title">Tasks</h2>
            <div className="card" style={{ padding: 0 }}>
              <TasksTable tasks={tasks} />
            </div>
          </div>

          <div className="section">
            <h2 className="card-title">Event history</h2>
            <div className="card">
              <EventTimeline events={events} />
            </div>
          </div>
        </>
      ) : null}
    </>
  );
}

function SummaryItem({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="summary-item-label">{label}</div>
      <div className="summary-item-value">{children}</div>
    </div>
  );
}

function TasksTable({ tasks }: { tasks: Task[] }) {
  if (tasks.length === 0) {
    return <div className="empty-state">No tasks recorded for this run.</div>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Step</th>
            <th>Activity</th>
            <th>Kind</th>
            <th>Status</th>
            <th>Attempt</th>
            <th>Available at</th>
          </tr>
        </thead>
        <tbody>
          {tasks
            .slice()
            .sort((a, b) => a.step_index - b.step_index)
            .map((task) => (
              <tr key={task.id}>
                <td className="mono">{task.step_index}</td>
                <td>{task.activity_name}</td>
                <td className="mono">{task.kind}</td>
                <td>
                  <span className={pillClass(task.status)}>{task.status}</span>
                </td>
                <td className="mono">
                  {task.attempt}/{task.max_attempts}
                </td>
                <td className="mono">{formatDateTime(task.available_at)}</td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  );
}

function EventTimeline({ events }: { events: Event[] }) {
  if (events.length === 0) {
    return <div className="empty-state">No events recorded yet.</div>;
  }

  const sorted = events
    .slice()
    .sort(
      (a, b) =>
        new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
    );

  return (
    <div className="timeline">
      {sorted.map((event) => (
        <div key={event.id} className="timeline-item">
          <div className="timeline-dot" />
          <div className="timeline-type">{event.type}</div>
          <div className="timeline-time">{formatDateTime(event.created_at)}</div>
          {event.payload !== undefined && event.payload !== null && (
            <pre className="timeline-payload">
              {JSON.stringify(event.payload, null, 2)}
            </pre>
          )}
        </div>
      ))}
    </div>
  );
}
