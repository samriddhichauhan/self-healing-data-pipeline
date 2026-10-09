import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ExternalLink, GitBranch, Play, RefreshCw, Timer, Wind } from "lucide-react";
import { useConfig, useDags, useHealth, useRuns } from "../api/hooks";
import { RunDrawer, RunTable } from "../components/RunTable";
import { TriggerDialog } from "../components/TriggerDialog";
import { Badge, Card, EmptyState, ErrorState, KV, PageHeader, Segmented, SkeletonRows, StatusBadge } from "../components/ui";
import { fmtDateTime, fmtDuration, fmtRelative } from "../lib/format";

const FILTERS = ["all", "success", "failed", "running", "queued"] as const;

export default function PipelineRuns() {
  const runs = useRuns(300);
  const dags = useDags();
  const health = useHealth();
  const config = useConfig();
  const [params, setParams] = useSearchParams();
  const openId = params.get("run");
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>("all");
  const [triggering, setTriggering] = useState(false);

  const rows = useMemo(() => (runs.data ?? []).filter((r) => filter === "all" || r.state === filter), [runs.data, filter]);
  const count = (s: string) => (runs.data ?? []).filter((r) => r.state === s).length;
  const comp = (k: string) => health.data?.components.find((c) => c.key === k);
  const dag = dags.data?.find((d) => d.dag_id === config.data?.dag_id) ?? dags.data?.[0];
  const latest = runs.data?.[0];
  const durations = (runs.data ?? []).map((r) => r.duration_s).filter((v): v is number => v !== null);

  return (
    <>
      <PageHeader title="Pipeline Runs" description="Airflow DAG runs for the self-healing pipeline, with the pipeline's own recorded status."
        actions={<>
          <button className="btn" onClick={() => { runs.refetch(); dags.refetch(); }}><RefreshCw size={14} className={runs.isFetching ? "spin" : ""} /> Refresh</button>
          <button className="btn btn-primary" onClick={() => setTriggering(true)}><Play size={14} /> Trigger DAG</button>
        </>} />

      <div className="grid g-4">
        <Card title={<><Wind size={14} /> Airflow</>}>
          <div className="row between" style={{ marginBottom: 8 }}>
            <StatusBadge status={comp("airflow")?.status === "connected" ? comp("scheduler")?.status : comp("airflow")?.status ?? null} />
            {config.data && <a className="small" href={config.data.airflow_public_url} target="_blank" rel="noreferrer">Open UI <ExternalLink size={11} /></a>}
          </div>
          <div className="small muted">{comp("scheduler")?.detail ?? "Checking scheduler…"}</div>
          <div className="small muted">{comp("airflow_db")?.detail}</div>
        </Card>
        <Card title={<><GitBranch size={14} /> DAG</>}>
          {!dag ? <SkeletonRows rows={2} /> : (
            <>
              <div className="row between" style={{ marginBottom: 6 }}>
                <span className="mono strong">{dag.dag_id}</span>
                <StatusBadge status={dag.is_paused ? "warning" : "success"} label={dag.is_paused ? "Paused" : "Active"} />
              </div>
              <div className="small muted" style={{ marginBottom: 6 }}>{dag.description}</div>
              <div className="row-wrap">{dag.tags.map((t) => <Badge key={t} tone="outline" plain>{t}</Badge>)}<span className="small muted">owner {dag.owners.join(", ")}</span></div>
            </>
          )}
        </Card>
        <Card title="Run summary">
          <KV items={[
            ["Total", runs.data?.length ?? null], ["Success", count("success")], ["Failed", count("failed")],
            ["Running / queued", count("running") + count("queued")],
          ]} />
        </Card>
        <Card title={<><Timer size={14} /> Latest run</>}>
          {!latest ? <div className="small muted">Waiting for pipeline data</div> : (
            <>
              <div className="mono small strong truncate" title={latest.run_id}>{latest.run_id}</div>
              <div className="row-wrap" style={{ margin: "6px 0" }}><StatusBadge status={latest.state} />{latest.pipeline_status && <StatusBadge status={latest.pipeline_status} />}</div>
              <div className="small muted">{fmtRelative(latest.start_date)} · {fmtDuration(latest.duration_s)} · avg {durations.length ? fmtDuration(durations.reduce((a, b) => a + b, 0) / durations.length) : "N/A"}</div>
            </>
          )}
        </Card>
      </div>

      <Card className="mt" flush title="DAG runs" subtitle="Click a run to see its task instances"
        actions={<Segmented value={filter} onChange={setFilter} options={FILTERS.map((f) => ({ value: f, label: `${f === "all" ? "All" : f[0].toUpperCase() + f.slice(1)} ${f === "all" ? runs.data?.length ?? 0 : count(f)}` }))} />}>
        {runs.isError ? <div style={{ padding: "0 16px 14px" }}><ErrorState error={runs.error} onRetry={runs.refetch} /></div>
          : runs.isLoading ? <SkeletonRows rows={8} />
          : !rows.length ? <EmptyState title={runs.data?.length ? "No runs match this filter" : "Waiting for pipeline data"} />
          : <RunTable runs={rows} selected={openId} onSelect={(r) => setParams({ run: r.run_id })} />}
      </Card>
      <div className="small muted" style={{ marginTop: 8 }}>Last refreshed {runs.dataUpdatedAt ? fmtDateTime(new Date(runs.dataUpdatedAt).toISOString()) : "—"} · Source: Airflow REST API joined with pipeline_runs</div>

      {openId && <RunDrawer runId={openId} onClose={() => setParams({})} />}
      {triggering && <TriggerDialog onClose={() => setTriggering(false)} onTriggered={(id) => { setParams({ run: id }); runs.refetch(); }} />}
    </>
  );
}
