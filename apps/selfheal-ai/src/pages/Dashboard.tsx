import { Link, useNavigate } from "react-router-dom";
import { Activity, BrainCircuit, CheckCircle2, Clock, Layers, ShieldCheck, Timer, Wind, XCircle } from "lucide-react";
import { useActivity, useAIDiagnoses, useAISummary, useDashboard, useIncidents, useRunPipeline, useRuns } from "../api/hooks";
import type { ActivityEvent } from "../api/types";
import { IncidentTable } from "../components/IncidentTable";
import { StageStepper } from "../components/PipelineFlow";
import { RunTable } from "../components/RunTable";
import { TL_COLOR, Timeline } from "../components/Timeline";
import { Card, ConfidenceMeter, EmptyState, ErrorState, MetricCard, PageHeader, SkeletonRows, SourceBadge, StatusBadge } from "../components/ui";
import { isActive, mean, toStages, trend24h } from "../lib/derive";
import { fmtDuration, fmtNum, fmtRelative } from "../lib/format";

const SEV_COLOR: Record<ActivityEvent["severity"], string> = {
  info: TL_COLOR.info, success: TL_COLOR.success, warning: TL_COLOR.warning, error: TL_COLOR.danger, ai: TL_COLOR.ai,
};

function StatusBanner() {
  const { data } = useDashboard();
  const runs = useRuns(50);
  const comp = (k: string) => data?.health?.components.find((c) => c.key === k);
  const airflow = comp("airflow")?.status === "connected" ? comp("scheduler")?.status : comp("airflow")?.status;
  const ollama = comp("ollama")?.status === "connected" ? comp("model")?.status : comp("ollama")?.status;
  const active = runs.data?.find((r) => isActive(r));
  const latest = data?.latest_pipeline_run;
  const pipeline = active ? "RUNNING" : latest?.status ?? null;
  const cell = (k: string, value: string | null, tone: string, detail: string | undefined, Icon: typeof Wind) => (
    <div className="banner-cell">
      <span className={`banner-icon tone-${tone}`}><Icon size={17} /></span>
      <div style={{ minWidth: 0 }}>
        <div className="banner-k">{k}</div>
        <div className={`banner-v text-${tone}`}><span className={`dot ${tone} ${value && tone !== "danger" ? "pulse" : ""}`} />{value ?? "CHECKING…"}</div>
        {detail && <div className="banner-d">{detail}</div>}
      </div>
    </div>
  );
  const hTone = (s?: string) => (s === "connected" ? "success" : s === "degraded" ? "warning" : s ? "danger" : "neutral");
  const pTone = pipeline === "RUNNING" ? "info" : pipeline === "FAILED" ? "danger" : pipeline ? "success" : "neutral";
  return (
    <div className="banner">
      {cell("Airflow", airflow ? (airflow === "connected" ? "CONNECTED" : airflow.toUpperCase()) : null, hTone(airflow), comp("scheduler")?.detail, Wind)}
      {cell("Ollama", ollama ? (ollama === "connected" ? "ONLINE" : ollama.toUpperCase()) : null, hTone(ollama), comp("model")?.detail, BrainCircuit)}
      {cell("Pipeline", pipeline, pTone, active ? `${active.run_id} in progress` : latest ? `${latest.run_id} · ${fmtRelative(latest.completed_at ?? latest.started_at)}` : "Waiting for pipeline data", Layers)}
    </div>
  );
}

export default function Dashboard() {
  const dash = useDashboard();
  const runs = useRuns(200);
  const incidents = useIncidents();
  const ai = useAISummary();
  const diagnoses = useAIDiagnoses(200);
  const activity = useActivity(14);
  const navigate = useNavigate();
  const latestRun = runs.data?.[0];
  const graph = useRunPipeline(latestRun?.run_id);
  const d = dash.data;
  const loading = dash.isLoading;

  const healed = (incidents.data ?? []).filter((i) => i.status === "REMEDIATED");
  const recovery = mean(healed.map((i) => i.recovery_seconds).filter((v): v is number => v !== null));
  const tRuns = trend24h(runs.data, (r) => r.start_date);
  const tOk = trend24h(runs.data?.filter((r) => r.state === "success"), (r) => r.end_date);
  const tFail = trend24h(runs.data?.filter((r) => r.state === "failed"), (r) => r.end_date);
  const tHeal = trend24h(healed, (i) => i.updated_at);
  const tAi = trend24h(diagnoses.data?.filter((x) => x.source === "LOCAL_OLLAMA_AI"), (x) => x.diagnosis_ts);
  const tr = (t: ReturnType<typeof trend24h>, goodWhenUp = true) => (t ? { delta: t.delta, label: "vs previous 24 h", goodWhenUp } : null);
  const lastDiag = ai.data?.latest_ollama ?? null;

  return (
    <>
      <PageHeader title="Pipeline Overview" description="Monitor your data pipelines, AI diagnostics and automated recovery." />
      {dash.error && <div style={{ marginBottom: 14 }}><ErrorState error={dash.error} onRetry={dash.refetch} /></div>}
      {d?.errors && Object.keys(d.errors).length > 0 && (
        <div className="alert warning" style={{ marginBottom: 14 }}><XCircle size={15} /><div className="alert-body">Some sources are unavailable: {Object.entries(d.errors).map(([k, v]) => `${k} (${v})`).join("; ")}</div></div>
      )}

      <StatusBanner />

      <div className="grid g-6 mt">
        <MetricCard label="Pipeline runs" loading={loading} icon={<Activity size={14} />} value={d?.executions ? fmtNum(d.executions.total) : null}
          trend={tr(tRuns)} status={latestRun ? <StatusBadge status={latestRun.state} /> : undefined} />
        <MetricCard label="Successful runs" tone="success" loading={loading} icon={<CheckCircle2 size={14} />} value={d?.executions ? fmtNum(d.executions.success) : null}
          trend={tr(tOk)} status={d?.executions?.total ? <span className="small strong text-success">{Math.round((d.executions.success / d.executions.total) * 100)}%</span> : undefined} />
        <MetricCard label="Failed runs" tone="danger" loading={loading} icon={<XCircle size={14} />} value={d?.executions ? fmtNum(d.executions.failed) : null}
          trend={tr(tFail, false)} status={d?.executions ? <StatusBadge status={d.executions.failed ? "failed" : "success"} label={d.executions.failed ? "Attention" : "Clear"} /> : undefined} />
        <MetricCard label="Auto-healed incidents" tone="success" loading={incidents.isLoading} icon={<ShieldCheck size={14} />} value={incidents.data ? healed.length : null}
          trend={tr(tHeal)} status={d?.incidents ? <span className="small muted">{d.incidents.escalated} escalated</span> : undefined} />
        <MetricCard label="AI diagnoses" tone="violet" loading={ai.isLoading} icon={<BrainCircuit size={14} />} value={ai.data ? ai.data.ollama_diagnoses : null}
          trend={tr(tAi)} status={ai.data ? <span className="small muted">{ai.data.fallbacks} fallback</span> : undefined} />
        <MetricCard label="Avg recovery time" tone="warning" loading={incidents.isLoading} icon={<Timer size={14} />}
          value={recovery !== null ? fmtDuration(recovery) : null}
          foot={healed.length ? `Detection → verified fix · ${healed.length} incidents` : "No auto-healed incidents yet"} />
      </div>

      <div className="grid g-main mt">
        <Card title="Latest pipeline run" subtitle={latestRun ? <span className="mono">{latestRun.run_id}</span> : "Waiting for pipeline data"}
          actions={latestRun && <Link className="btn btn-sm" to={`/flow?run=${encodeURIComponent(latestRun.run_id)}`}>Open flow</Link>}>
          {!latestRun ? <EmptyState title="Waiting for pipeline data">No DAG runs exist in Airflow yet.</EmptyState> : !graph.data ? <SkeletonRows rows={2} /> : (
            <>
              <div className="row-wrap" style={{ marginBottom: 14 }}>
                <StatusBadge status={latestRun.state} />
                {latestRun.pipeline_status && <StatusBadge status={latestRun.pipeline_status} />}
                <span className="small muted">Duration <strong className="mono" style={{ color: "var(--text)" }}>{fmtDuration(latestRun.duration_s)}</strong></span>
                <span className="small muted">Started <strong style={{ color: "var(--text)" }}>{fmtRelative(latestRun.start_date)}</strong></span>
                {latestRun.row_counts && <span className="small muted">Rows <strong className="mono" style={{ color: "var(--text)" }}>{Object.entries(latestRun.row_counts).map(([k, v]) => `${k} ${v}`).join(" · ")}</strong></span>}
              </div>
              <StageStepper stages={toStages(graph.data.nodes, graph.data.run)} />
            </>
          )}
          <div className="divider" style={{ margin: "16px 0 12px" }} />
          <div className="row between" style={{ marginBottom: 8 }}>
            <span className="card-title">Latest AI diagnosis</span>
            {lastDiag && <Link className="small" to="/ai">AI Diagnostics →</Link>}
          </div>
          {!lastDiag ? <div className="small muted">No Ollama diagnosis recorded for a pipeline incident yet.</div> : (
            <div className="grid g-2" style={{ gap: 12 }}>
              <div>
                <div className="row" style={{ gap: 6, marginBottom: 6 }}><SourceBadge source={lastDiag.source} /><span className="mono small muted">{lastDiag.incident_id}</span></div>
                <div className="strong" style={{ fontSize: 13.5 }}>{lastDiag.root_cause ?? "N/A"}</div>
                <div className="small muted" style={{ marginTop: 4 }}><Clock size={11} style={{ verticalAlign: -1 }} /> {fmtRelative(lastDiag.diagnosis_ts)} · {fmtDuration(lastDiag.response_time_s)} response</div>
              </div>
              <div className="stack" style={{ gap: 8 }}>
                <ConfidenceMeter value={lastDiag.confidence} />
                <div className="row-wrap">{lastDiag.risk && <StatusBadge status={lastDiag.risk} />}{lastDiag.policy_gate_action && <StatusBadge status={lastDiag.policy_gate_action} />}</div>
              </div>
            </div>
          )}
        </Card>
        <Card title="Live activity" subtitle="DAG runs, incidents and AI calls" actions={<span className="badge info plain"><span className="dot pulse" /> Live</span>}>
          {activity.isLoading ? <SkeletonRows rows={7} /> : (
            <div style={{ maxHeight: 360, overflowY: "auto", paddingRight: 4 }}>
              <Timeline newestFirst empty="No pipeline events recorded yet."
                items={(activity.data ?? []).map((e) => ({ ts: e.ts, title: e.title, meta: e.detail, color: SEV_COLOR[e.severity] }))} />
            </div>
          )}
        </Card>
      </div>

      <div className="grid g-2 mt">
        <Card title="Recent pipeline runs" flush actions={<Link className="btn btn-sm" to="/runs">View all</Link>}>
          {runs.isLoading ? <SkeletonRows rows={6} /> : !runs.data?.length ? <EmptyState title="Waiting for pipeline data" /> : (
            <RunTable compact runs={runs.data.slice(0, 7)} onSelect={(r) => navigate(`/runs?run=${encodeURIComponent(r.run_id)}`)} />
          )}
        </Card>
        <Card title="Recent incidents" flush actions={<Link className="btn btn-sm" to="/incidents">View all</Link>}>
          {incidents.isLoading ? <SkeletonRows rows={6} /> : !incidents.data?.length ? <EmptyState>No incidents recorded.</EmptyState> : (
            <IncidentTable compact incidents={incidents.data.slice(0, 7)} onSelect={(i) => navigate(`/incidents?id=${encodeURIComponent(i.incident_id)}`)} />
          )}
        </Card>
      </div>
    </>
  );
}
