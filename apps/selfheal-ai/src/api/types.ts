export type HealthStatus = "connected" | "degraded" | "offline";

export interface HealthComponent {
  key: string;
  name: string;
  status: HealthStatus;
  detail: string;
  latency_ms: number | null;
  meta: Record<string, any>;
}

export interface SystemHealth {
  status: HealthStatus;
  checked_at: string;
  components: HealthComponent[];
}

export interface Run {
  run_id: string;
  dag_id: string;
  logical_date: string | null;
  run_type: string | null;
  state: string | null;
  start_date: string | null;
  end_date: string | null;
  duration_s: number | null;
  pipeline_status: string | null;
  row_counts: Record<string, number> | null;
  incident_count: number | null;
  auto_fix_count: number | null;
  escalation_count: number | null;
  is_test: boolean;
}

export interface Task {
  task_id: string;
  state: string | null;
  start_date: string | null;
  end_date: string | null;
  duration_s: number | null;
  try_number: number | null;
  max_tries: number | null;
  queued_when: string | null;
  operator: string | null;
}

export interface Incident {
  incident_id: string;
  run_id: string | null;
  dataset: string | null;
  fault_category: string | null;
  severity: string | null;
  status: string | null;
  created_at: string | null;
  updated_at: string | null;
  detected_at: string | null;
  execution_date: string | null;
  task_id: string | null;
  diagnosis_source: "LOCAL_OLLAMA_AI" | "RULE_BASED";
  ai_mode: string | null;
  model: string | null;
  ai_confidence: number | null;
  engine_confidence: number | null;
  risk: string | null;
  recommended_action: string | null;
  policy_decision: string | null;
  policy_rationale: string | null;
  root_cause: string | null;
  ai_reason: string | null;
  hypothesis: string | null;
  observed: string | null;
  expected: string | null;
  error_message: string | null;
  possible_root_causes: string[];
  blast_radius: any;
  remediation_status: string | null;
  verification_status: string | null;
  remediation: Record<string, any> | null;
  verification: Record<string, any> | null;
  recovery_seconds: number | null;
  evidence: Record<string, any>;
  is_test: boolean;
  ai_calls?: Diagnosis[];
}

export interface Diagnosis {
  incident_id: string;
  model: string | null;
  host: string | null;
  fault_category: string | null;
  request_ts: string | null;
  response_ts: string | null;
  diagnosis_ts: string | null;
  response_time_s: number | null;
  response_chars: number | null;
  root_cause: string | null;
  confidence: number | null;
  risk: string | null;
  ai_action: string | null;
  policy_gate_action: string | null;
  policy_gate_rationale: string | null;
  source: "LOCAL_OLLAMA_AI" | "RULE_BASED_FALLBACK";
  fallback: boolean;
  fallback_reason: string | null;
  last_ts: string | null;
  run_id: string | null;
  incident_status: string | null;
  is_test: boolean;
}

export interface PipelineRunRow {
  run_id: string;
  dag_id: string;
  logical_date: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  duration_seconds: number | null;
  row_counts: Record<string, number> | null;
  incident_count: number;
  auto_fix_count: number;
  escalation_count: number;
}

export interface Dashboard {
  errors: Record<string, string>;
  health?: SystemHealth | null;
  executions?: { total: number; success: number; failed: number; running: number; queued: number };
  recent_executions?: Run[];
  latest_pipeline_run?: PipelineRunRow | null;
  pipeline_totals?: { auto_fix_count: number; escalation_count: number; recovered_runs: number };
  incidents?: { total: number; recovered: number; escalated: number };
  recent_incidents?: Incident[];
  ai?: { avg_response_time_s: number | null; responses_measured: number; last_diagnosis: Diagnosis | null };
}

export interface ActivityEvent {
  ts: string;
  kind: "run" | "incident" | "ai";
  severity: "info" | "success" | "warning" | "error" | "ai";
  title: string;
  detail: string;
  ref: { run_id?: string; incident_id?: string };
}

export interface PipelineNode {
  key: string;
  title: string;
  subtitle: string;
  status: "success" | "failed" | "running" | "pending" | "skipped" | "warning";
  task_name: string;
  started_at: string | null;
  ended_at: string | null;
  duration_s: number | null;
  details: { label: string; value: any }[];
  note: string | null;
  highlight: boolean;
}

export interface PipelineGraph {
  run: Run;
  nodes: PipelineNode[];
  incident_id: string | null;
  tasks: Task[];
}

export interface AIStatus {
  connected: boolean;
  host: string;
  model: string;
  model_installed: boolean;
  models: { name: string; size: number; parameter_size?: string; quantization?: string; modified_at?: string }[];
  latency_ms: number;
  error: string | null;
}

export interface AISummary {
  total_calls: number;
  ollama_diagnoses: number;
  fallbacks: number;
  avg_response_time_s: number | null;
  p95_response_time_s: number | null;
  latest: Diagnosis | null;
  latest_ollama: Diagnosis | null;
}

export interface LogEntry {
  n: number;
  ts: string | null;
  level: string | null;
  source: string | null;
  message: string;
  text: string;
  marker: string | null;
  continuation: boolean;
}

export interface LogPayload {
  source: "file" | "airflow";
  name: string;
  total_lines: number;
  entries: LogEntry[];
}

export interface LogFile {
  filename: string;
  size_bytes: number;
  modified_time: string;
}

export interface Analytics {
  executions_by_day: { date: string; success: number; failed: number; running: number }[];
  run_states: Record<string, number>;
  pipeline_outcomes: Record<string, number>;
  incidents_by_type: { fault_category: string; remediated: number; escalated: number; other: number }[];
  ai_confidence: { ts: string; incident_id: string; confidence: number; fault_category: string; policy_decision: string }[];
  ai_response_times: { ts: string; incident_id: string; response_time_s: number; model: string }[];
  autofix_vs_escalation: { auto_fix: number; escalated: number; by_day: { date: string; auto_fix: number; escalated: number }[] };
  recovery_durations: { ts: string; incident_id: string; run_id: string; seconds: number; diagnosis_source: string }[];
  sources: Record<string, string>;
}

export interface Dag {
  dag_id: string;
  is_paused: boolean;
  is_active: boolean;
  description: string | null;
  tags: string[];
  owners: string[];
}

export interface FaultType {
  id: string;
  label: string;
  expected: "AUTO_FIX" | "ESCALATE";
  effect: string;
}

export interface PlatformConfig {
  dag_id: string;
  ollama_model: string;
  ollama_host: string;
  airflow_public_url: string;
  backend_url: string;
}

/** Existing backend: GET /api/v1/data/profiles (written by ingestion's DataProfiler). */
export interface DataProfile {
  dataset: string;
  timestamp: string;
  row_count: number;
  column_count: number;
  null_percentage_overall: number;
  duplicate_percentage: number;
  primary_key: string | null;
  freshness: { latest_timestamp?: string; oldest_timestamp?: string } | null;
  columns: Record<string, { type: string; null_pct: number; unique_values: number }>;
}

/** Existing backend: GET /api/v1/pipeline/runs/{run_id}. */
export interface BackendRunDetail extends PipelineRunRow {
  quality_results: { id: number; dataset: string; check_name: string; status: string; details: Record<string, any>; created_at: string }[];
  aggregated_metrics: { metrics: Record<string, any>; created_at: string } | null;
}
