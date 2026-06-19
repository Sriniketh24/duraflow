// API client for the Duraflow HTTP API.
// Base URL is read from NEXT_PUBLIC_API_URL, falling back to a local dev server.

export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';

export interface Stats {
  runs_running: number;
  runs_completed: number;
  runs_failed: number;
  runs_cancelled: number;
  tasks_pending: number;
  tasks_leased: number;
  tasks_dead: number;
}

export type RunStatus =
  | 'running'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | string;

export interface Run {
  id: string;
  workflow_name: string;
  status: RunStatus;
  current_step: number;
  total_steps: number;
  created_at: string;
  updated_at: string;
  error?: string;
}

export type TaskStatus = 'pending' | 'leased' | 'completed' | 'failed' | 'dead' | string;

export interface Task {
  id: string;
  run_id: string;
  step_index: number;
  activity_name: string;
  kind: string;
  status: TaskStatus;
  attempt: number;
  max_attempts: number;
  available_at: string;
  error?: string;
}

export interface Event {
  id: string;
  run_id: string;
  task_id?: string;
  type: string;
  payload: unknown;
  created_at: string;
}

export interface RunDetail {
  run: Run;
  tasks: Task[];
}

export interface RunHistory {
  events: Event[];
}

export interface WorkflowsResponse {
  workflows: string[];
}

export interface RunsResponse {
  runs: Run[];
}

export interface DlqResponse {
  tasks: Task[];
}

export class ApiError extends Error {
  constructor(message: string, public status?: number) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${API_BASE_URL}${path}`, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers || {}),
      },
      cache: 'no-store',
    });
  } catch (err) {
    throw new ApiError(
      `Could not reach Duraflow API at ${API_BASE_URL}. Is the server running?`
    );
  }

  if (!res.ok) {
    let detail = '';
    try {
      const body = await res.json();
      detail = body?.error || body?.message || '';
    } catch {
      // ignore body parse failures
    }
    throw new ApiError(
      detail || `Request failed with status ${res.status}`,
      res.status
    );
  }

  if (res.status === 204) {
    return undefined as T;
  }

  return res.json() as Promise<T>;
}

export const api = {
  getStats: () => request<Stats>('/v1/stats'),

  getWorkflows: () => request<WorkflowsResponse>('/v1/workflows'),

  getRuns: (limit = 50) => request<RunsResponse>(`/v1/runs?limit=${limit}`),

  getRun: (id: string) => request<RunDetail>(`/v1/runs/${id}`),

  getRunHistory: (id: string) => request<RunHistory>(`/v1/runs/${id}/history`),

  startWorkflow: (
    name: string,
    input: object,
    idempotencyKey?: string
  ) =>
    request<Run>(`/v1/workflows/${encodeURIComponent(name)}/runs`, {
      method: 'POST',
      body: JSON.stringify({
        input,
        ...(idempotencyKey ? { idempotency_key: idempotencyKey } : {}),
      }),
    }),

  cancelRun: (id: string) =>
    request<void>(`/v1/runs/${id}/cancel`, { method: 'POST' }),

  getDlq: (limit = 50) => request<DlqResponse>(`/v1/dlq?limit=${limit}`),

  replayDlqTask: (taskId: string) =>
    request<void>(`/v1/dlq/${taskId}/replay`, { method: 'POST' }),
};
