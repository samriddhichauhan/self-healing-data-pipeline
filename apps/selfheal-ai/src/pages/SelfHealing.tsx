import { useNavigate } from "react-router-dom";
import { BrainCircuit, CheckCircle2, Percent, ScanSearch, ShieldCheck, Siren, Timer, TriangleAlert, Wrench } from "lucide-react";
import { useIncidents, useRuns } from "../api/hooks";
import { Card, EmptyState, ErrorState, MetricCard, PageHeader, SkeletonRows, StatusBadge } from "../components/ui";
import { mean } from "../lib/derive";
import { fmtDateTime, fmtDuration, fmtNum } from "../lib/format";

export default function SelfHealing() {
  const incidents = useIncidents();
  const runs = useRuns(300);
  const navigate = useNavigate();
  const all = incidents.data ?? [];
  const aiDiagnosed = all.filter((i) => i.diagnosis_source === "LOCAL_OLLAMA_AI").length;
  const autoFix = all.filter((i) => i.policy_decision === "AUTO_FIX").length;
  const escalateDecisions = all.filter((i) => i.policy_decision === "ESCALATE").length;
  const fixed = all.filter((i) => i.remediation_status === "SUCCESS").length;
  const verified = all.filter((i) => i.verification_status === "PASSED").length;
  const remediated = all.filter((i) => i.status === "REMEDIATED");
  const escalated = all.filter((i) => i.status === "ESCALATED").length;
  const recoveredRuns = (runs.data ?? []).filter((r) => r.pipeline_status === "RECOVERED").length;
  const rate = remediated.length + escalated ? remediated.length / (remediated.length + escalated) : null;
  const avgRecovery = mean(remediated.map((i) => i.recovery_seconds).filter((v): v is number => v !== null));
  const n = (v: number) => (incidents.data ? fmtNum(v) : "—");

  const steps = [
    { k: "Detected", v: n(all.length), d: "incidents raised by validate_quality", icon: Siren },
    { k: "Diagnosed by AI", v: n(aiDiagnosed), d: `${n(all.length - aiDiagnosed)} used the rule-based fallback`, icon: BrainCircuit },
    { k: "Policy evaluation", v: n(autoFix), d: `AUTO_FIX · ${n(escalateDecisions)} ESCALATE`, icon: ShieldCheck },
    { k: "Auto-fix", v: n(fixed), d: "remediations executed successfully", icon: Wrench },
    { k: "Verification", v: n(verified), d: "independently verified clean", icon: ScanSearch },
    { k: "Recovered", v: runs.data ? fmtNum(recoveredRuns) : "—", d: "DAG runs finished RECOVERED", icon: CheckCircle2 },
  ];

  return (
    <>
      <PageHeader title="Self-Healing" description="How incidents move through detection, AI diagnosis, policy evaluation, automatic remediation and verification." />
      {incidents.isError && <ErrorState error={incidents.error} onRetry={incidents.refetch} />}

      <Card flush title="Self-healing lifecycle" subtitle="Counts are computed from recorded incidents and DAG runs">
        {incidents.isLoading ? <SkeletonRows rows={3} /> : (
          <div className="lifecycle" style={{ borderTop: "1px solid var(--divider)", marginTop: 4 }}>
            {steps.map(({ k, v, d, icon: Icon }) => (
              <div className="lc-step" key={k}>
                <div className="lc-icon"><Icon size={15} /></div>
                <div className="lc-k">{k}</div>
                <div className="lc-v">{v}</div>
                <div className="lc-d">{d}</div>
              </div>
            ))}
          </div>
        )}
      </Card>

      <div className="grid g-4 mt">
        <MetricCard label="Auto-fix count" tone="success" icon={<Wrench size={14} />} loading={incidents.isLoading} value={incidents.data ? remediated.length : null} foot="Incidents remediated and recorded" />
        <MetricCard label="Escalation count" tone="warning" icon={<TriangleAlert size={14} />} loading={incidents.isLoading} value={incidents.data ? escalated : null} foot="Sent to human review by policy" />
        <MetricCard label="Recovery rate" icon={<Percent size={14} />} loading={incidents.isLoading} value={rate !== null ? (rate * 100).toFixed(0) : null} unit="%" foot="Auto-fixed ÷ (auto-fixed + escalated)" />
        <MetricCard label="Avg recovery time" tone="violet" icon={<Timer size={14} />} loading={incidents.isLoading} value={avgRecovery !== null ? fmtDuration(avgRecovery) : null} foot="Detection → verified fix" />
      </div>

      <div className="grid g-main mt">
        <Card flush title="Recent recoveries" subtitle="Incidents that were auto-remediated and verified">
          {incidents.isLoading ? <SkeletonRows rows={6} /> : !remediated.length ? <EmptyState>No incident has been auto-remediated yet.</EmptyState> : (
            <div className="table-wrap" style={{ maxHeight: 420 }}>
              <table className="table">
                <thead><tr><th>Incident</th><th>Run</th><th>Detected</th><th className="r">Rows</th><th>Verification</th><th className="r">Recovery</th></tr></thead>
                <tbody>
                  {remediated.map((i) => (
                    <tr key={i.incident_id} className="clickable" onClick={() => navigate(`/incidents?id=${encodeURIComponent(i.incident_id)}`)}>
                      <td className="mono nowrap">{i.incident_id}</td>
                      <td className="mono muted truncate" style={{ maxWidth: 220 }}>{i.run_id ?? "N/A"}</td>
                      <td className="nowrap muted">{fmtDateTime(i.detected_at, false)}</td>
                      <td className="r mono">{i.remediation?.initial_rows !== undefined ? `${i.remediation.initial_rows} → ${i.remediation.cleaned_rows}` : "N/A"}</td>
                      <td><StatusBadge status={i.verification_status} /></td>
                      <td className="r mono">{fmtDuration(i.recovery_seconds)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
        <Card title="Remediation policy" subtitle="As implemented in the pipeline's Policy Gate (read-only)">
          <div className="table-wrap">
            <table className="table">
              <thead><tr><th>Fault category</th><th>Decision</th></tr></thead>
              <tbody>
                <tr><td className="mono">DUPLICATE_INGESTION</td><td><StatusBadge status="AUTO_FIX" /></td></tr>
                {["SCHEMA_DRIFT", "NULL_SPIKE", "REFERENTIAL_BREAK", "VOLUME_ANOMALY_DROP", "STALENESS"].map((c) => (
                  <tr key={c}><td className="mono">{c}</td><td><StatusBadge status="ESCALATE" /></td></tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="small muted" style={{ marginTop: 10 }}>
            AUTO_FIX additionally requires AI confidence ≥ 0.85; below that the gate escalates. Verification re-reads the data independently before the run continues.
          </div>
        </Card>
      </div>
    </>
  );
}
