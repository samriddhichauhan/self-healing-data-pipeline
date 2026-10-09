import { useState } from "react";
import { Link } from "react-router-dom";
import { BrainCircuit, Clock } from "lucide-react";
import { useAIDiagnoses, useAIStatus, useAISummary } from "../api/hooks";
import type { Diagnosis } from "../api/types";
import { Card, ConfidenceMeter, EmptyState, ErrorState, KV, PageHeader, SkeletonRows, SourceBadge, StatusBadge } from "../components/ui";
import { fmtConf, fmtDateTime, fmtDuration, fmtRelative, titleCase } from "../lib/format";

function Focus({ d }: { d: Diagnosis }) {
  const ai = d.source === "LOCAL_OLLAMA_AI";
  return (
    <>
      <div className="big-stats" style={{ borderTop: "1px solid var(--divider)", borderBottom: "1px solid var(--divider)", margin: "4px -16px 14px" }}>
        <div className="big-stat"><div className="k">AI confidence</div><div className="v">{d.confidence !== null ? `${Math.round(d.confidence * 100)}%` : "N/A"}</div><div className="d">Policy threshold 85%</div></div>
        <div className="big-stat"><div className="k">Risk</div><div className="v" style={{ fontSize: 20 }}>{d.risk ? <StatusBadge status={d.risk} /> : "N/A"}</div></div>
        <div className="big-stat"><div className="k">Recommended action</div><div className="v mono" style={{ fontSize: 17 }}>{d.ai_action ?? "N/A"}</div><div className="d">from the model</div></div>
        <div className="big-stat"><div className="k">Policy</div><div className="v mono" style={{ fontSize: 17, color: d.policy_gate_action === "AUTO_FIX" ? "var(--success)" : d.policy_gate_action ? "var(--warning)" : undefined }}>{d.policy_gate_action ?? "N/A"}</div><div className="d">authoritative decision</div></div>
        <div className="big-stat"><div className="k">Response time</div><div className="v">{d.response_time_s !== null ? d.response_time_s.toFixed(1) : "N/A"}{d.response_time_s !== null && <span className="small muted"> s</span>}</div><div className="d">Ollama generation</div></div>
      </div>
      <div className="grid g-2" style={{ gap: 20 }}>
        <div>
          <div className="row" style={{ gap: 6, marginBottom: 8 }}><SourceBadge source={d.source} />{d.incident_status && <StatusBadge status={d.incident_status} />}</div>
          <div className="small muted">Root cause</div>
          <div style={{ fontSize: 15, fontWeight: 650, marginTop: 2 }}>{d.root_cause ?? (d.fallback ? "Diagnosed by the rule-based engine" : "N/A")}</div>
          <div style={{ marginTop: 12 }}><ConfidenceMeter value={d.confidence} /></div>
          {d.policy_gate_rationale && <div className="small muted" style={{ marginTop: 10 }}>{d.policy_gate_rationale}</div>}
        </div>
        <KV items={[
          ["Incident", <Link className="mono" to={`/incidents?id=${encodeURIComponent(d.incident_id)}`}>{d.incident_id}</Link>],
          ["Pipeline run", d.run_id ? <Link className="mono" to={`/flow?run=${encodeURIComponent(d.run_id)}`}>{d.run_id}</Link> : null],
          ["Fault", d.fault_category], ["Model", d.model],
          ["Diagnosis source", ai ? "LOCAL_OLLAMA_AI" : "RULE_BASED_FALLBACK"],
          ["Request", fmtDateTime(d.request_ts)], ["Response", fmtDateTime(d.response_ts)],
          ["Fallback", d.fallback ? <span className="text-warning">Yes — {d.fallback_reason}</span> : "No"],
        ]} />
      </div>
    </>
  );
}

export default function AIDiagnosis() {
  const status = useAIStatus();
  const summary = useAISummary();
  const list = useAIDiagnoses(150);
  const [focus, setFocus] = useState<number | null>(null);
  const s = status.data;
  const focused = focus !== null ? list.data?.[focus] : summary.data?.latest_ollama ?? summary.data?.latest ?? null;
  const online = s?.connected && s.model_installed;

  return (
    <>
      <PageHeader title="AI Diagnostics" description="Ollama is the primary diagnosis engine; the rule-based engine answers only when Ollama fails." />
      <div className="hero">
        <div className="hero-icon"><BrainCircuit size={22} /></div>
        <div>
          <div className="hero-title">Ollama AI Diagnostics {s && <StatusBadge status={online ? "connected" : "offline"} label={online ? "Online" : "Offline"} />}</div>
          <div className="small muted">{s?.error ?? "Local LLM diagnosing pipeline incidents inside validate_quality"}</div>
        </div>
        <div className="hero-facts">
          <div><div className="fact-k">Model</div><div className="fact-v mono">{s?.model ?? "—"}</div></div>
          <div><div className="fact-k">Host</div><div className="fact-v mono">{s?.host ?? "—"}</div></div>
          <div><div className="fact-k">Latency</div><div className="fact-v">{s?.connected ? `${s.latency_ms} ms` : "N/A"}</div></div>
          <div><div className="fact-k">Diagnoses</div><div className="fact-v">{summary.data ? `${summary.data.ollama_diagnoses} AI · ${summary.data.fallbacks} fallback` : "—"}</div></div>
        </div>
      </div>

      <Card className="mt" title={focus === null ? "Latest AI diagnosis" : "Selected diagnosis"} subtitle={focused ? <><Clock size={11} style={{ verticalAlign: -1 }} /> {focused.incident_id} · {fmtRelative(focused.last_ts)}</> : undefined}
        actions={focus !== null && <button className="btn btn-sm" onClick={() => setFocus(null)}>Show latest</button>}>
        {summary.isError ? <ErrorState error={summary.error} onRetry={summary.refetch} /> : summary.isLoading ? <SkeletonRows rows={5} />
          : focused ? <Focus d={focused} /> : <EmptyState icon={<BrainCircuit size={18} />} title="Waiting for pipeline data">No AI diagnosis has been recorded for a pipeline incident yet.</EmptyState>}
      </Card>

      <Card className="mt" flush title="Recent AI diagnoses" subtitle="From the adapter's [OLLAMA_*] events. Click a row to inspect it.">
        {list.isError ? <div style={{ padding: 16 }}><ErrorState error={list.error} /></div> : list.isLoading ? <SkeletonRows rows={8} />
          : !list.data?.length ? <EmptyState>No AI calls recorded.</EmptyState> : (
          <div className="table-wrap" style={{ maxHeight: 520 }}>
            <table className="table">
              <thead><tr><th>Incident</th><th>Fault</th><th>Root cause</th><th className="r">Confidence</th><th>Risk</th><th>Action</th><th className="r">Response</th><th>Timestamp</th></tr></thead>
              <tbody>
                {list.data.map((d, idx) => (
                  <tr key={`${d.incident_id}-${idx}`} className={`clickable ${focus === idx ? "selected" : ""}`} onClick={() => setFocus(idx)}>
                    <td className="mono nowrap">{d.incident_id}</td>
                    <td className="nowrap">{d.fault_category ? titleCase(d.fault_category) : "N/A"}</td>
                    <td className="truncate" style={{ maxWidth: 300 }} title={d.root_cause ?? d.fallback_reason ?? ""}>{d.root_cause ?? <span className="text-warning">Fallback: {d.fallback_reason}</span>}</td>
                    <td className="r mono">{fmtConf(d.confidence)}</td>
                    <td>{d.risk ? <StatusBadge status={d.risk} /> : <span className="faint">N/A</span>}</td>
                    <td>{d.policy_gate_action ? <StatusBadge status={d.policy_gate_action} /> : <span className="faint">N/A</span>}</td>
                    <td className="r mono">{fmtDuration(d.response_time_s)}</td>
                    <td className="nowrap muted">{fmtDateTime(d.last_ts, false)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </>
  );
}
