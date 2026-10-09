// Thin fetch wrapper around the FastAPI backend. Every call here hits a real
// HTTP endpoint — there is no simulated/mock data path in this file or
// anywhere else in the frontend.
const BASE_URL = (import.meta as any).env?.VITE_API_BASE_URL || "http://localhost:8000";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json();
}

export interface PipelineRun {
  run_id: string;
  dag_id: string;
  logical_date: string;
  status: string;
  started_at: string;
  completed_at: string | null;
  duration_seconds: number | null;
  row_counts: Record<string, number> | null;
  incident_count: number;
  auto_fix_count: number;
  escalation_count: number;
}

export interface Incident {
  incident_id: string;
  run_id: string | null;
  dataset: string;
  fault_category: string;
  severity: string;
  action: string;
  status: string;
  confidence: number;
  hypothesis: string;
  remediation_status: string;
  verification_status: string;
  payload: any;
  created_at: string;
  updated_at: string;
}

export interface PipelineSummary {
  total_runs: number;
  healthy_runs: number;
  recovered_runs: number;
  failed_runs: number;
  total_incidents: number;
  total_auto_fixes: number;
  total_escalations: number;
  latest_run: PipelineRun | null;
}

export interface TaskInstance {
  task_id: string;
  state: string;
  start_date: string | null;
  end_date: string | null;
}

export const api = {
  health: () => request<any>("/health"),
  summary: () => request<PipelineSummary>("/api/v1/pipeline/summary"),
  runs: (limit = 20) => request<PipelineRun[]>(`/api/v1/pipeline/runs?limit=${limit}`),
  run: (runId: string) => request<PipelineRun & { quality_results: any[]; aggregated_metrics: any }>(
    `/api/v1/pipeline/runs/${encodeURIComponent(runId)}`
  ),
  airflowState: (runId: string) =>
    request<{ task_instances: TaskInstance[] }>(`/api/v1/pipeline/runs/${encodeURIComponent(runId)}/airflow-state`),
  incidents: (limit = 50) => request<Incident[]>(`/api/v1/incidents?limit=${limit}`),
  profiles: () => request<any[]>("/api/v1/data/profiles"),
  logs: () => request<any[]>("/api/v1/logs"),
  logContent: (filename: string) => request<{ lines: string[]; total_lines: number }>(`/api/v1/logs/${encodeURIComponent(filename)}`),
  triggerRun: (executionDate: string) =>
    request<any>("/api/v1/pipeline/trigger", { method: "POST", body: JSON.stringify({ execution_date: executionDate }) }),
  injectFault: (faultType: string, executionDate: string) =>
    request<any>("/api/v1/simulation/inject-fault", {
      method: "POST",
      body: JSON.stringify({ fault_type: faultType, execution_date: executionDate }),
    }),
};

export default api;
