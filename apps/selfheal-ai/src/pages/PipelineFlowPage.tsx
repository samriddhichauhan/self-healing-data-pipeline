import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ExternalLink, Info, ScrollText, Siren } from "lucide-react";
import { useConfig, useRunPipeline, useRuns } from "../api/hooks";
import { PipelineFlow, STAGE_LABEL } from "../components/PipelineFlow";
import { TaskTimeline } from "../components/TaskTimeline";
import { Card, EmptyState, ErrorState, KV, PageHeader, Skeleton, StatusBadge } from "../components/ui";
import { isActive, toStages } from "../lib/derive";
import { fmtDateTime, fmtDuration } from "../lib/format";

const LOG_TASK: Record<string, string> = {
  data: "ingest_orders", schema: "validate_schema", quality: "validate_quality", ai: "validate_quality", policy: "validate_quality",
  remediation: "validate_quality", verification: "validate_quality", outcome: "agent_monitoring",
};

export default function PipelineFlowPage() {
  const [params, setParams] = useSearchParams();
  const runs = useRuns(100);
  const runId = params.get("run") ?? runs.data?.[0]?.run_id ?? null;
  const graph = useRunPipeline(runId);
  const config = useConfig();
  const [selected, setSelected] = useState("ai");
  useEffect(() => setSelected("ai"), [runId]);

  const stages = useMemo(() => (graph.data ? toStages(graph.data.nodes, graph.data.run) : []), [graph.data]);
  const stage = stages.find((s) => s.key === selected);
  const run = graph.data?.run;
  const airflowLink = run && config.data ? `${config.data.airflow_public_url}/dags/${run.dag_id}/grid?dag_run_id=${encodeURIComponent(run.run_id)}` : null;
  const details = stage ? [...stage.node.details, ...(stage.extra?.details ?? [])] : [];

  return (
    <>
      <PageHeader title="Pipeline Flow" description="Every stage of one DAG run — from ingestion through AI diagnosis to recovery."
        actions={
          <select className="select" style={{ width: 340 }} value={runId ?? ""} onChange={(e) => setParams({ run: e.target.value })} aria-label="Run">
            {runs.isLoading && <option>Loading runs…</option>}
            {runs.data?.map((r) => <option key={r.run_id} value={r.run_id}>{isActive(r) ? "● " : ""}{r.run_id} · {r.pipeline_status ?? r.state}</option>)}
          </select>
        } />
      {runs.isError && <ErrorState error={runs.error} onRetry={runs.refetch} />}
      {runs.data && !runs.data.length && <Card><EmptyState title="Waiting for pipeline data">No DAG runs found in Airflow.</EmptyState></Card>}

      {runId && (
        <>
          <Card>
            {graph.isError ? <ErrorState error={graph.error} onRetry={graph.refetch} /> : !run ? <Skeleton height={36} /> : (
              <div className="row-wrap between">
                <div className="row-wrap" style={{ gap: 12 }}>
                  <span className="mono strong">{run.run_id}</span>
                  <StatusBadge status={run.state} pulse={isActive(run)} />
                  {run.pipeline_status && <StatusBadge status={run.pipeline_status} />}
                  {graph.data?.incident_id && <Link className="badge warning plain" to={`/incidents?id=${encodeURIComponent(graph.data.incident_id)}`}><Siren size={11} /> {graph.data.incident_id}</Link>}
                </div>
                <div className="row-wrap small muted" style={{ gap: 16 }}>
                  <span>Execution <strong className="mono" style={{ color: "var(--text)" }}>{run.logical_date?.slice(0, 19).replace("T", " ")}</strong></span>
                  <span>Duration <strong style={{ color: "var(--text)" }}>{fmtDuration(run.duration_s)}</strong></span>
                  {airflowLink && <a className="btn btn-sm" href={airflowLink} target="_blank" rel="noreferrer"><ExternalLink size={12} /> Airflow</a>}
                </div>
              </div>
            )}
          </Card>

          <div className="flow-layout mt">
            <Card title="Execution flow" subtitle="Connectors animate only while a stage is running">
              {!graph.data ? <Skeleton height={520} /> : <PipelineFlow stages={stages} selected={selected} onSelect={setSelected} />}
            </Card>
            <div className="flow-detail">
              {stage && (
                <Card title={<>{stage.label} <StatusBadge status={stage.status === "warning" ? "warning" : stage.status} label={STAGE_LABEL[stage.status]} /></>}
                  subtitle={stage.node.subtitle}
                  actions={run && <Link className="btn btn-sm" to={`/logs?run=${encodeURIComponent(run.run_id)}&task=${LOG_TASK[stage.key]}`}><ScrollText size={12} /> Task log</Link>}>
                  <KV items={[
                    ["Task", <span className="mono">{stage.node.task_name}</span>],
                    ["Status", STAGE_LABEL[stage.status]],
                    ["Started / recorded", fmtDateTime(stage.node.started_at)],
                    ["Ended", fmtDateTime(stage.node.ended_at)],
                    ["Duration", stage.node.duration_s !== null ? fmtDuration(stage.node.duration_s) : "Not measured separately"],
                  ]} />
                  <div className="section-title">Details from the pipeline</div>
                  {details.length ? <KV items={details.map((d) => [d.label, String(d.value)])} /> : <div className="small muted">No additional details recorded for this stage.</div>}
                  {(stage.node.note || stage.extra?.note) && (
                    <div className="alert info" style={{ marginTop: 12 }}><Info size={14} /><div className="alert-body">{stage.node.note ?? stage.extra?.note}</div></div>
                  )}
                </Card>
              )}
              <Card className="mt" title="Airflow task timeline">
                {graph.data ? <TaskTimeline tasks={graph.data.tasks} /> : <Skeleton height={140} />}
              </Card>
            </div>
          </div>
        </>
      )}
    </>
  );
}
