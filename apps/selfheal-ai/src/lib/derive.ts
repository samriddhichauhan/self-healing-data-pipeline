/**
 * Pure derivations over real API records. Nothing here creates data: every
 * value is computed from records the backend returned, and anything that
 * cannot be computed returns null (rendered as "N/A").
 */
import type { Incident, PipelineNode, Run, Task } from "../api/types";

const ts = (v?: string | null) => (v ? new Date(v).getTime() : NaN);

/** Count in the last 24h vs the 24h before, from each record's own timestamp. */
export function trend24h<T>(items: T[] | undefined, at: (t: T) => string | null | undefined, now = Date.now()) {
  if (!items) return null;
  const day = 86_400_000;
  let cur = 0;
  let prev = 0;
  for (const it of items) {
    const t = ts(at(it));
    if (Number.isNaN(t)) continue;
    if (t > now - day) cur++;
    else if (t > now - 2 * day) prev++;
  }
  return { current: cur, previous: prev, delta: cur - prev };
}

export const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);

// ---------- incidents ----------
export type IncidentFilter = "all" | "open" | "resolved" | "failed" | "auto-healed" | "escalated";

export function incidentMatches(i: Incident, f: IncidentFilter): boolean {
  switch (f) {
    case "all": return true;
    case "open": return i.status !== "REMEDIATED" && i.status !== "ESCALATED";
    case "resolved": return i.status === "REMEDIATED";
    case "auto-healed": return i.status === "REMEDIATED" && i.policy_decision === "AUTO_FIX" && i.verification_status === "PASSED";
    case "escalated": return i.status === "ESCALATED";
    case "failed": return i.remediation_status === "FAILED" || i.verification_status === "FAILED";
  }
}

export const incidentResolvedAt = (i: Incident) => (i.status === "REMEDIATED" ? i.updated_at : null);

// ---------- pipeline stages ----------
/** The eight stages shown in the UI, derived from the API's 9-node view
 *  (incident detection is folded into the Data Quality stage). */
export type StageStatus = "pending" | "running" | "success" | "failed" | "skipped" | "recovered" | "warning";
export interface Stage {
  key: string;
  label: string;
  status: StageStatus;
  node: PipelineNode;
  extra?: PipelineNode;
}

const map = (s: PipelineNode["status"]): StageStatus => s;

export function toStages(nodes: PipelineNode[], run?: Run | null): Stage[] {
  const n = Object.fromEntries(nodes.map((x) => [x.key, x])) as Record<string, PipelineNode>;
  if (!n.data) return [];
  const det = n.detection;
  const quality = n.quality;
  const incident = det?.status === "warning";
  let qStatus = map(quality.status);
  if (incident && quality.status === "success") qStatus = run?.pipeline_status === "RECOVERED" ? "recovered" : "warning";

  let outcome = map(n.outcome.status);
  if (run?.pipeline_status === "RECOVERED" && n.outcome.status === "success") outcome = "recovered";

  return [
    { key: "data", label: "Data Ingestion", status: map(n.data.status), node: n.data },
    { key: "schema", label: "Schema Validation", status: map(n.schema.status), node: n.schema },
    { key: "quality", label: "Data Quality", status: qStatus, node: quality, extra: det },
    { key: "ai", label: "AI Diagnosis", status: map(n.ai.status), node: n.ai },
    { key: "policy", label: "Policy Gate", status: map(n.policy.status), node: n.policy },
    { key: "remediation", label: "Remediation", status: map(n.remediation.status), node: n.remediation },
    { key: "verification", label: "Verification", status: map(n.verification.status), node: n.verification },
    { key: "outcome", label: "Pipeline Complete", status: outcome, node: n.outcome },
  ];
}

// ---------- runs / tasks ----------
export const TASK_ORDER = [
  "ingest_dimensions", "ingest_orders", "ingest_events", "validate_schema",
  "validate_quality", "transform_data", "load_data", "agent_monitoring",
];

export function runProgress(tasks: Task[]) {
  const done = tasks.filter((t) => ["success", "failed", "upstream_failed", "skipped"].includes(t.state ?? ""));
  const running = tasks.find((t) => t.state === "running" || t.state === "queued" || t.state === "up_for_retry");
  const total = tasks.length || TASK_ORDER.length;
  const remaining = TASK_ORDER.filter((id) => !tasks.some((t) => t.task_id === id && done.includes(t)));
  return { completed: done.length, total, running, remaining };
}

export const isActive = (r?: Run | null) => !!r && (r.state === "running" || r.state === "queued");
