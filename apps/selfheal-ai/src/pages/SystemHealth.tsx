import { BrainCircuit, Database, GitBranch, RefreshCw, Server, Wind } from "lucide-react";
import { useAIStatus, useConfig, useHealth, useRuns } from "../api/hooks";
import type { HealthComponent, HealthStatus } from "../api/types";
import { HEALTH_LABEL, HealthCard } from "../components/HealthCard";
import { Card, ErrorState, PageHeader, Skeleton, StatusBadge } from "../components/ui";
import { isActive } from "../lib/derive";
import { fmtDateTime, fmtRelative } from "../lib/format";

const worst = (...xs: (HealthStatus | undefined)[]): HealthStatus | null => {
  const s = xs.filter(Boolean) as HealthStatus[];
  if (!s.length) return null;
  return s.includes("offline") ? "offline" : s.includes("degraded") ? "degraded" : "connected";
};

export default function SystemHealth() {
  const health = useHealth();
  const ai = useAIStatus();
  const config = useConfig();
  const runs = useRuns(20);
  const c = (k: string): HealthComponent | undefined => health.data?.components.find((x) => x.key === k);
  const latest = runs.data?.[0];
  const pipelineStatus: HealthStatus | null = !latest ? null : isActive(latest) ? "connected"
    : latest.pipeline_status === "FAILED" || latest.state === "failed" ? "offline" : latest.pipeline_status ? "connected" : "degraded";
  const checked = health.data?.checked_at;
  const ms = (x?: HealthComponent) => (x?.latency_ms != null ? `${x.latency_ms} ms` : "N/A");

  return (
    <>
      <PageHeader title="System Health" description="Live checks against every component the platform depends on."
        actions={<><span className="small muted">Last checked {checked ? fmtRelative(checked) : "—"}</span>
          <button className="btn" onClick={() => { health.refetch(); ai.refetch(); }}><RefreshCw size={14} className={health.isFetching ? "spin" : ""} /> Check now</button></>} />
      {health.isError && <ErrorState error={health.error} onRetry={health.refetch} />}
      {health.data && (
        <div className={`alert ${health.data.status === "connected" ? "success" : health.data.status === "degraded" ? "warning" : "danger"}`} style={{ marginBottom: 14 }}>
          <Server size={15} />
          <div className="alert-body"><strong>{health.data.status === "connected" ? "All systems operational" : health.data.status === "degraded" ? "Some components are degraded" : "One or more components are offline"}</strong> · {health.data.components.filter((x) => x.status === "connected").length} of {health.data.components.length} checks passing · checked {fmtDateTime(checked)}</div>
        </div>
      )}

      {health.isLoading ? <div className="grid g-3">{Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} height={190} style={{ borderRadius: 10 }} />)}</div> : (
        <div className="grid g-3">
          <HealthCard name="Airflow" icon={<Wind size={16} />} status={worst(c("airflow")?.status, c("scheduler")?.status, c("airflow_db")?.status, c("dag")?.status)}
            detail={c("scheduler")?.detail} rows={[
              ["Webserver", c("airflow") ? <><StatusBadge status={c("airflow")!.status} label={HEALTH_LABEL[c("airflow")!.status]} /> <span className="small muted">{ms(c("airflow"))}</span></> : null],
              ["Scheduler", c("scheduler")?.detail], ["Metadata DB", c("airflow_db")?.detail], ["DAG", c("dag")?.detail],
              ["Endpoint", config.data ? <span className="mono small">{config.data.airflow_public_url}</span> : null],
              ["Version", <span className="faint" title="The platform API does not expose Airflow's /api/v1/version">N/A</span>],
            ]} />
          <HealthCard name="Ollama" icon={<BrainCircuit size={16} />} status={worst(c("ollama")?.status, c("model")?.status)} detail={c("ollama")?.detail} delay={30} rows={[
            ["Response time", ms(c("ollama"))], ["Model", ai.data ? <span className="mono">{ai.data.model}</span> : null], ["Model state", c("model")?.detail],
            ["Endpoint", ai.data ? <span className="mono small">{ai.data.host}</span> : null],
            ["Version", <span className="faint" title="The platform API does not expose Ollama's /api/version">N/A</span>],
          ]} />
          <HealthCard name="Backend API" icon={<Server size={16} />} status={c("api")?.status ?? null} detail={c("api")?.detail} delay={60} rows={[
            ["Response time", ms(c("api"))], ["Endpoint", config.data ? <span className="mono small">{config.data.backend_url}</span> : null],
            ["Version", c("api")?.detail?.match(/\d+\.\d+\.\d+/)?.[0] ?? "N/A"], ["Platform API", <StatusBadge status="connected" label="Online (serving this page)" />],
          ]} />
          <HealthCard name="Database" icon={<Database size={16} />} status={worst(c("pipeline_db")?.status, c("airflow_db")?.status)} detail="PostgreSQL" delay={90} rows={[
            ["Pipeline DB", c("pipeline_db")?.detail], ["Airflow metadata DB", c("airflow_db")?.detail], ["Checked via", "backend /health · Airflow /health"],
          ]} />
          <HealthCard name="Pipeline" icon={<GitBranch size={16} />} status={pipelineStatus} detail={latest ? `Latest run ${fmtRelative(latest.start_date)}` : "Waiting for pipeline data"} delay={120} rows={[
            ["Latest run", latest ? <span className="mono small">{latest.run_id}</span> : null],
            ["Airflow state", latest ? <StatusBadge status={latest.state} /> : null],
            ["Pipeline status", latest?.pipeline_status ? <StatusBadge status={latest.pipeline_status} /> : null],
            ["DAG", c("dag")?.detail],
          ]} />
        </div>
      )}

      <Card className="mt" flush title="All checks" subtitle="Raw component checks performed by the platform API">
        <div className="table-wrap">
          <table className="table">
            <thead><tr><th>Component</th><th>Status</th><th>Detail</th><th className="r">Latency</th></tr></thead>
            <tbody>{(health.data?.components ?? []).map((x) => (
              <tr key={x.key}><td className="strong">{x.name}</td><td><StatusBadge status={x.status} label={HEALTH_LABEL[x.status]} /></td><td className="muted">{x.detail}</td><td className="r mono">{ms(x)}</td></tr>
            ))}</tbody>
          </table>
        </div>
      </Card>
      <div className="small muted" style={{ marginTop: 8 }}>Rules: scheduler heartbeat &gt; 60 s is degraded, &gt; 300 s offline; responses slower than 2 s are degraded; a paused DAG is degraded because new runs would stay queued.</div>
    </>
  );
}
