import { useState } from "react";
import { Link } from "react-router-dom";
import { GitBranch, ScrollText } from "lucide-react";
import { useIncident, useTaskLog } from "../api/hooks";
import type { Incident } from "../api/types";
import { incidentResolvedAt } from "../lib/derive";
import { fmtConf, fmtDateTime, fmtDuration } from "../lib/format";
import { LogViewer } from "./LogViewer";
import { Drawer } from "./Overlay";
import { TL_COLOR, Timeline, type TimelineItem } from "./Timeline";
import { Badge, ConfidenceMeter, ErrorState, KV, SkeletonRows, SourceBadge, StatusBadge, Tabs } from "./ui";

type Tab = "overview" | "ai" | "remediation" | "evidence" | "logs";

export function incidentTimeline(i: Incident): TimelineItem[] {
  const items: TimelineItem[] = [];
  if (i.detected_at) items.push({ ts: i.detected_at, title: `Detected · ${i.fault_category}`, meta: `${i.task_id ?? "validate_quality"} · ${i.observed ?? ""}`, color: TL_COLOR.warning });
  for (const c of i.ai_calls ?? []) {
    if (c.request_ts && !c.fallback) items.push({ ts: c.request_ts, title: `Ollama request · ${c.model ?? ""}`, color: TL_COLOR.info });
    if (c.response_ts) items.push({ ts: c.response_ts, title: "Ollama response received", meta: c.response_time_s !== null ? `${c.response_time_s.toFixed(2)} s` : undefined, color: TL_COLOR.ai });
    if (c.diagnosis_ts) items.push({ ts: c.diagnosis_ts, title: `Policy Gate · ${c.policy_gate_action ?? "N/A"}`, meta: `AI confidence ${fmtConf(c.confidence)} · risk ${c.risk ?? "N/A"}`, color: TL_COLOR.policy });
    if (c.fallback) items.push({ ts: c.last_ts, title: "Rule-based fallback used", meta: c.fallback_reason ?? undefined, color: TL_COLOR.warning });
  }
  if (i.status === "REMEDIATED") items.push({ ts: i.updated_at, title: "Remediated and verified", meta: i.recovery_seconds !== null ? `Recovered in ${fmtDuration(i.recovery_seconds)}` : undefined, color: TL_COLOR.success });
  else if (i.status === "ESCALATED") items.push({ ts: i.updated_at, title: "Escalated to human review", color: TL_COLOR.danger });
  return items;
}

function IncidentLogs({ i }: { i: Incident }) {
  const tryNo = Number(i.evidence?.try_number ?? 1) || 1;
  const task = i.task_id ?? "validate_quality";
  const log = useTaskLog(i.run_id, task, tryNo, false);
  if (!i.run_id) return <div className="small muted">This incident is not linked to a DAG run, so there is no task log.</div>;
  if (log.isError) return <ErrorState error={log.error} title="Task log unavailable" />;
  if (!log.data) return <SkeletonRows rows={6} />;
  return (
    <LogViewer compact entries={log.data.entries} exportName={`${i.incident_id}-${task}`}
      header={<Link className="btn btn-sm" to={`/logs?run=${encodeURIComponent(i.run_id)}&task=${task}&try=${tryNo}`}><ScrollText size={12} /> Open in Logs</Link>} />
  );
}

export function IncidentDrawer({ id, onClose }: { id: string; onClose: () => void }) {
  const q = useIncident(id);
  const [tab, setTab] = useState<Tab>("overview");
  const i = q.data;
  const ai = i?.diagnosis_source === "LOCAL_OLLAMA_AI";
  return (
    <Drawer title={id} subtitle={i ? `${i.fault_category} · ${i.dataset ?? "unknown dataset"}` : "Loading…"} onClose={onClose}>
      {q.isError ? <ErrorState error={q.error} /> : !i ? <SkeletonRows rows={12} /> : (
        <>
          <div className="row-wrap" style={{ marginBottom: 12 }}>
            <StatusBadge status={i.status} />
            <SourceBadge source={i.diagnosis_source} />
            {i.policy_decision && <StatusBadge status={i.policy_decision} />}
            {i.is_test && <Badge tone="outline" plain>test data</Badge>}
            {i.run_id && !i.is_test && <Link className="btn btn-sm" to={`/flow?run=${encodeURIComponent(i.run_id)}`}><GitBranch size={12} /> Pipeline run</Link>}
          </div>
          <Tabs value={tab} onChange={setTab} tabs={[
            { value: "overview", label: "Overview" }, { value: "ai", label: "AI diagnosis" }, { value: "remediation", label: "Remediation" },
            { value: "evidence", label: "Evidence" }, { value: "logs", label: "Logs" },
          ]} />
          {tab === "overview" && (
            <>
              <KV items={[
                ["Fault category", i.fault_category], ["Dataset", i.dataset],
                ["Pipeline run", i.run_id ? <span className="mono">{i.run_id}</span> : null], ["Execution date", i.execution_date],
                ["Observed condition", i.observed], ["Expected condition", i.expected], ["Error", i.error_message],
                ["Created", fmtDateTime(i.created_at)], ["Resolved", incidentResolvedAt(i) ? fmtDateTime(incidentResolvedAt(i)) : null],
              ]} />
              <div className="section-title">Timeline</div>
              <Timeline items={incidentTimeline(i)} empty="No timestamps recorded for this incident." />
            </>
          )}
          {tab === "ai" && (
            <KV items={[
              ["Diagnosis source", ai ? "LOCAL_OLLAMA_AI" : "RULE_BASED (fallback)"], ["AI mode", i.ai_mode], ["Model", i.model],
              ["Root cause", i.root_cause], ["AI reasoning", i.ai_reason],
              ["AI confidence", ai ? <ConfidenceMeter value={i.ai_confidence} /> : <span className="muted">Not an AI score (rule-based engine: {fmtConf(i.engine_confidence)})</span>],
              ["Risk", i.risk ? <StatusBadge status={i.risk} /> : null], ["Recommended action", i.recommended_action],
              ["Policy decision", i.policy_decision ? <StatusBadge status={i.policy_decision} /> : null], ["Policy rationale", i.policy_rationale],
              ["Possible causes", i.possible_root_causes?.length ? i.possible_root_causes.join(" · ") : null],
            ]} />
          )}
          {tab === "remediation" && (
            <KV items={[
              ["Remediation", i.remediation_status ? <StatusBadge status={i.remediation_status} /> : null],
              ["Action", i.remediation?.action_type ?? i.remediation?.method],
              ["Rows", i.remediation?.initial_rows !== undefined ? `${i.remediation.initial_rows} → ${i.remediation.cleaned_rows} (${i.remediation.removed_duplicates} removed)` : null],
              ["Rationale", i.remediation?.rationale],
              ["Verification", i.verification_status ? <StatusBadge status={i.verification_status} /> : null],
              ["Verified result", i.verification?.remaining_rows !== undefined ? `${i.verification.remaining_rows} rows · ${i.verification.duplicate_count} duplicates` : null],
              ["Verification notes", i.verification?.verification_notes],
              ["Recovery duration", i.recovery_seconds !== null ? fmtDuration(i.recovery_seconds) : null],
            ]} />
          )}
          {tab === "evidence" && <pre className="json">{JSON.stringify(i.evidence, null, 2)}</pre>}
          {tab === "logs" && <IncidentLogs i={i} />}
        </>
      )}
    </Drawer>
  );
}
