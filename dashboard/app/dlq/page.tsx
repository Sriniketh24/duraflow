'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { api, ApiError, Task } from '@/lib/api';
import { formatDateTime, pillClass, shortId } from '@/lib/format';

const POLL_INTERVAL_MS = 3000;

export default function DlqPage() {
  const [tasks, setTasks] = useState<Task[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [replayingId, setReplayingId] = useState<string | null>(null);
  const [replayError, setReplayError] = useState<string | null>(null);

  const load = useCallback(async (isInitial = false) => {
    try {
      const res = await api.getDlq(50);
      setTasks(res.tasks);
      setError(null);
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.message
          : 'Unexpected error loading the dead-letter queue.'
      );
    } finally {
      if (isInitial) setLoading(false);
    }
  }, []);

  useEffect(() => {
    load(true);
    const interval = setInterval(() => load(false), POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [load]);

  async function handleReplay(taskId: string) {
    setReplayingId(taskId);
    setReplayError(null);
    try {
      await api.replayDlqTask(taskId);
      await load(false);
    } catch (err) {
      setReplayError(
        err instanceof ApiError ? err.message : `Failed to replay task ${taskId}.`
      );
    } finally {
      setReplayingId(null);
    }
  }

  return (
    <>
      <div className="page-header">
        <h1 className="page-title">Dead-letter queue</h1>
        <p className="page-subtitle">
          Tasks that exhausted their retry attempts. Replay to retry execution.
        </p>
      </div>

      {error && <div className="alert alert-error section">{error}</div>}
      {replayError && <div className="alert alert-error section">{replayError}</div>}

      {loading ? (
        <p className="loading-text">Loading dead-letter queue…</p>
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <DlqTable
            tasks={tasks}
            replayingId={replayingId}
            onReplay={handleReplay}
          />
        </div>
      )}
    </>
  );
}

function DlqTable({
  tasks,
  replayingId,
  onReplay,
}: {
  tasks: Task[] | null;
  replayingId: string | null;
  onReplay: (taskId: string) => void;
}) {
  if (!tasks || tasks.length === 0) {
    return <div className="empty-state">No dead-lettered tasks. Everything is healthy.</div>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Task ID</th>
            <th>Run</th>
            <th>Activity</th>
            <th>Step</th>
            <th>Attempt</th>
            <th>Error</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((task) => (
            <tr key={task.id}>
              <td className="mono">{shortId(task.id)}</td>
              <td>
                <Link href={`/runs/${task.run_id}`} className="mono">
                  {shortId(task.run_id)}
                </Link>
              </td>
              <td>{task.activity_name}</td>
              <td className="mono">{task.step_index}</td>
              <td className="mono">
                {task.attempt}/{task.max_attempts}
              </td>
              <td>
                {task.error ? (
                  <span className="mono" title={task.error}>
                    {task.error.length > 40
                      ? `${task.error.slice(0, 40)}…`
                      : task.error}
                  </span>
                ) : (
                  '—'
                )}
              </td>
              <td>
                <button
                  className="btn btn-small btn-primary"
                  onClick={() => onReplay(task.id)}
                  disabled={replayingId === task.id}
                >
                  {replayingId === task.id ? 'Replaying…' : 'Replay'}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
