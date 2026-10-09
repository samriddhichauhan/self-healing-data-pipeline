import { Link } from "react-router-dom";
import { ExternalLink, GitBranch, ScrollText } from "lucide-react";
import { useBackendRun, useConfig, useRun } from "../api/hooks";
import type { Run } from "../api/types";
import { fmtDateTime, fmtDuration, fmtRelative } from "../lib/format";
import { Drawer } from "./Overlay";
import { TaskTimeline } from "./TaskTimeline";
import { Badge, ErrorState, KV, SkeletonRows, StatusBadge } from "./ui";

export function RunTable({ runs, selected, onSelect, compact = false }: {
  runs: Run[]; selected?: string | null; onSelect: (r: Run) => void; compact?: boolean;
}) {
  return (
    <div className="table-wrap" style={compact ? undefined : { maxHeight: "66vh" }}>
      <table className="table">
        <thead>
          <tr>
            <th>Run ID</th>
            {!compact && <th>DAG</th>}
            {!compact && <th>Execution date</th>}
            {!compact && <th className="hide-md">Started</th>}
            {!compact && <th className="hide-md">Ended</th>}
            <th className="r">Duration</th>
            <th>State</th>
            <th>Pipeline</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.run_id} className={`clickable ${selected === r.run_id ? "selected" : ""}`} onClick={() => onSelect(r)}>
              <td className={`mono nowrap ${compact ? "truncate" : ""}`} style={compact ? { maxWidth: 260 } : undefined} title={r.run_id}>{r.run_id} {r.is_test && <Badge tone="outline" plain>test</Badge>}</td>
              {!compact && <td className="mono muted">{r.dag_id}</td>}
              {!compact && <td className="nowrap">{fmtDateTime(r.logical_date, false)}</td>}
              {!compact && <td className="nowrap muted hide-md" title={fmtDateTime(r.start_date)}>{fmtRelative(r.start_date)}</td>}
              {!compact && <td className="nowrap muted hide-md">{fmtDateTime(r.end_date)}</td>}
              <td className="r mono">{fmtDuration(r.duration_s)}</td>
              <td><StatusBadge status={r.state} /></td>
              <td>{r.pipeline_status ? <StatusBadge status={r.pipeline_status} /> : <span className="faint">N/A</span>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function RunDrawer({ runId, onClose }: { runId: string; onClose: () => void }) {
  const run = useRun(runId);
  const detail = useBackendRun(runId);
  const config = useConfig();
  const airflowUrl = config.data ? `${config.data.airflow_public_url}/dags/${config.data.dag_id}/grid?dag_run_id=${encodeURIComponent(runId)}` : null;
  const r = run.data?.run;
  return (
    <Drawer title={runId} subtitle="DAG run · task instances" onClose={onClose}
      actions={airflowUrl && <a className="btn btn-sm" href={airflowUrl} target="_blank" rel="noreferrer"><ExternalLink size={12} /> Airflow</a>}>
      {run.isError ? <ErrorState error={run.error} /> : !r ? <SkeletonRows rows={10} /> : (
        <>
          <div className="row-wrap" style={{ marginBottom: 14 }}>
            <StatusBadge status={r.state} />
            {r.pipeline_status && <StatusBadge status={r.pipeline_status} />}
            <Link className="btn btn-sm" to={`/flow?run=${encodeURIComponent(runId)}`}><GitBranch size={12} /> Pipeline flow</Link>
            <Link className="btn btn-sm" to={`/logs?run=${encodeURIComponent(runId)}&task=validate_quality`}><ScrollText size={12} /> Logs</Link>
          </div>
          <KV items={[
            ["DAG", <span className="mono">{r.dag_id}</span>],
            ["Execution date", fmtDateTime(r.logical_date)],
            ["Run type", r.run_type],
            ["Started", fmtDateTime(r.start_date)],
            ["Ended", fmtDateTime(r.end_date)],
            ["Duration", fmtDuration(r.duration_s)],
            ["Row counts", r.row_counts ? Object.entries(r.row_counts).map(([k, v]) => `${k} ${v}`).join(" · ") : null],
            ["Incidents", r.incident_count],
            ["Auto-fixed / escalated", r.incident_count !== null ? `${r.auto_fix_count} / ${r.escalation_count}` : null],
          ]} />
          <div className="section-title">Task timeline</div>
          <TaskTimeline tasks={run.data!.tasks} />
          <div className="section-title">Task instances</div>
          <div className="table-wrap">
            <table className="table">
              <thead><tr><th>Task</th><th>State</th><th className="r">Try</th><th>Start</th><th className="r">Duration</th><th /></tr></thead>
              <tbody>
                {run.data!.tasks.map((t) => (
                  <tr key={t.task_id}>
                    <td className="mono">{t.task_id}</td>
                    <td><StatusBadge status={t.state} /></td>
                    <td className="r">{t.try_number ?? "—"}</td>
                    <td className="nowrap muted">{fmtDateTime(t.start_date)}</td>
                    <td className="r mono">{fmtDuration(t.duration_s)}</td>
                    <td><Link className="btn btn-ghost btn-sm btn-icon" title="Task log" to={`/logs?run=${encodeURIComponent(runId)}&task=${t.task_id}&try=${Math.max(1, t.try_number ?? 1)}`}><ScrollText size={13} /></Link></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="section-title">Quality checks recorded by load_data</div>
          {detail.isError ? <div className="small muted">Not recorded for this run (the pipeline writes these only when load_data runs).</div>
            : !detail.data ? <SkeletonRows rows={2} />
            : !detail.data.quality_results.length ? <div className="small muted">No quality results recorded for this run.</div> : (
              <div className="table-wrap">
                <table className="table">
                  <thead><tr><th>Dataset</th><th>Check</th><th>Status</th><th>Details</th></tr></thead>
                  <tbody>
                    {detail.data.quality_results.map((q) => (
                      <tr key={q.id}><td>{q.dataset}</td><td className="mono">{q.check_name}</td><td><StatusBadge status={q.status} /></td><td className="mono muted">{JSON.stringify(q.details)}</td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
        </>
      )}
    </Drawer>
  );
}
