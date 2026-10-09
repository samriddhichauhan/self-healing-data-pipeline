import { Fragment, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ChevronDown, ChevronRight, Database, FileCheck2, Gauge, ShieldAlert } from "lucide-react";
import { useBackendRun, useIncidents, useProfiles, useRun, useRuns } from "../api/hooks";
import type { DataProfile } from "../api/types";
import { IncidentTable } from "../components/IncidentTable";
import { Badge, Card, EmptyState, ErrorState, MetricCard, PageHeader, SkeletonRows, StatusBadge } from "../components/ui";
import { fmtDateTime, fmtNum } from "../lib/format";

/** The profiler stores rounded percentages of row_count; these invert them exactly. */
const dupCount = (p: DataProfile) => Math.round((p.duplicate_percentage / 100) * p.row_count);
const nullCells = (p: DataProfile) => Object.values(p.columns ?? {}).reduce((s, c) => s + Math.round((c.null_pct / 100) * p.row_count), 0);
const batchDate = (p: DataProfile) => p.freshness?.latest_timestamp?.slice(0, 10) ?? null;

export default function DataQuality() {
  const profiles = useProfiles();
  const runs = useRuns(50);
  const latest = runs.data?.[0]?.run_id ?? null;
  const run = useRun(latest);
  const checks = useBackendRun(latest);
  const incidents = useIncidents();
  const navigate = useNavigate();
  const [open, setOpen] = useState<string | null>(null);

  const latestByDataset = useMemo(() => {
    const m = new Map<string, DataProfile>();
    for (const p of profiles.data ?? []) if (!m.has(p.dataset)) m.set(p.dataset, p);
    return [...m.values()];
  }, [profiles.data]);
  const task = (id: string) => run.data?.tasks.find((t) => t.task_id === id);
  const schema = task("validate_schema");
  const quality = task("validate_quality");
  const rowCounts = run.data?.run.row_counts ?? null;
  const qualityIncidents = (incidents.data ?? []).filter((i) => (i.task_id ?? "validate_quality") === "validate_quality");
  const profiled = new Set(latestByDataset.map((p) => p.dataset));
  const dims = Object.entries(rowCounts ?? {}).filter(([k]) => !profiled.has(k));

  return (
    <>
      <PageHeader title="Data Quality" description="Dataset profiles captured at ingestion, contract validation results and quality incidents." />
      {profiles.isError && (
        <div className="alert warning" style={{ marginBottom: 14 }}><ShieldAlert size={15} /><div className="alert-body">
          Dataset profiles are unavailable: {(profiles.error as Error).message}. They are read from the pipeline backend (<code>/api/v1/data/profiles</code>) through the UI server's read-only <code>/backend/</code> proxy.
        </div></div>
      )}

      <div className="grid g-4">
        <MetricCard label="Datasets profiled" icon={<Database size={14} />} loading={profiles.isLoading} value={profiles.data ? latestByDataset.length : null} foot={profiles.data ? `${profiles.data.length} profiles on record` : undefined} />
        <MetricCard label="Schema validation" icon={<FileCheck2 size={14} />} tone="success" loading={run.isLoading}
          value={schema ? <StatusBadge status={schema.state} /> : null} foot={latest ? <span className="mono truncate">{latest}</span> : "Waiting for pipeline data"} />
        <MetricCard label="Quality validation" icon={<Gauge size={14} />} tone="success" loading={run.isLoading}
          value={quality ? <StatusBadge status={quality.state} /> : null} foot={run.data?.incidents.length ? `${run.data.incidents.length} incident(s) in this run` : "No incidents in the latest run"} />
        <MetricCard label="Quality incidents" icon={<ShieldAlert size={14} />} tone="warning" loading={incidents.isLoading}
          value={incidents.data ? qualityIncidents.length : null} foot={incidents.data ? `${qualityIncidents.filter((i) => i.status === "REMEDIATED").length} auto-healed · ${qualityIncidents.filter((i) => i.status === "ESCALATED").length} escalated` : undefined} />
      </div>

      <Card className="mt" flush title="Datasets" subtitle="Latest profile per dataset, captured at ingestion — before any self-healing remediation. Click a row for per-column statistics."
        footer="Duplicate and null counts are derived exactly from the profiler's stored percentages. Dimension tables are not profiled by the pipeline; their row counts come from the latest run.">
        {profiles.isLoading ? <SkeletonRows rows={4} /> : !latestByDataset.length && !dims.length ? <EmptyState title="Waiting for pipeline data" /> : (
          <div className="table-wrap">
            <table className="table">
              <thead><tr><th /><th>Dataset</th><th>Batch date</th><th className="r">Rows</th><th className="r">Columns</th><th className="r">Duplicates</th><th className="r">Null cells</th><th>Primary key</th><th>Schema</th><th>At ingestion</th><th>Profiled</th></tr></thead>
              <tbody>
                {latestByDataset.map((p) => {
                  const dups = dupCount(p);
                  const nulls = nullCells(p);
                  const isOpen = open === p.dataset;
                  return (
                    <Fragment key={p.dataset}>
                      <tr className="clickable" onClick={() => setOpen(isOpen ? null : p.dataset)}>
                        <td style={{ width: 28 }}>{isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}</td>
                        <td className="strong">{p.dataset}</td>
                        <td className="mono">{batchDate(p) ?? "N/A"}</td>
                        <td className="r mono">{fmtNum(p.row_count)}</td>
                        <td className="r mono">{p.column_count}</td>
                        <td className="r mono">{fmtNum(dups)} <span className="cell-sub">({p.duplicate_percentage}%)</span></td>
                        <td className="r mono">{fmtNum(nulls)} <span className="cell-sub">({p.null_percentage_overall}%)</span></td>
                        <td className="mono muted">{p.primary_key ?? "N/A"}</td>
                        <td>{schema ? <StatusBadge status={schema.state} /> : <span className="faint">N/A</span>}</td>
                        <td><StatusBadge status={dups === 0 && nulls === 0 ? "PASS" : "warning"} label={dups === 0 && nulls === 0 ? "CLEAN" : "ISSUES"} /></td>
                        <td className="muted nowrap">{fmtDateTime(p.timestamp, false)}</td>
                      </tr>
                      {isOpen && (
                        <tr>
                          <td />
                          <td colSpan={10} style={{ background: "var(--surface-2)" }}>
                            <table className="table" style={{ background: "transparent" }}>
                              <thead><tr><th>Column</th><th>Type</th><th className="r">Null %</th><th className="r">Null count</th><th className="r">Distinct values</th></tr></thead>
                              <tbody>
                                {Object.entries(p.columns ?? {}).map(([name, c]) => (
                                  <tr key={name}><td className="mono">{name}{name === p.primary_key && <Badge tone="info" plain>PK</Badge>}</td><td className="mono muted">{c.type}</td>
                                    <td className="r mono">{c.null_pct}</td><td className="r mono">{Math.round((c.null_pct / 100) * p.row_count)}</td><td className="r mono">{fmtNum(c.unique_values)}</td></tr>
                                ))}
                              </tbody>
                            </table>
                            {p.freshness && <div className="small muted" style={{ padding: "8px 0 2px" }}>Freshness window: {p.freshness.oldest_timestamp} → {p.freshness.latest_timestamp}</div>}
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
                {dims.map(([name, rows]) => (
                  <tr key={name}>
                    <td /><td className="strong">{name} <Badge tone="outline" plain>dimension</Badge></td><td className="faint">N/A</td>
                    <td className="r mono">{fmtNum(rows)}</td><td className="r faint">N/A</td><td className="r faint">N/A</td><td className="r faint">N/A</td><td className="faint">N/A</td>
                    <td>{schema ? <StatusBadge status={schema.state} /> : <span className="faint">N/A</span>}</td><td className="faint">N/A</td><td className="faint">N/A</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <div className="grid g-2 mt">
        <Card flush title="Checks recorded for the latest run" subtitle={latest ? <span className="mono">{latest}</span> : undefined}>
          {checks.isLoading ? <SkeletonRows rows={3} /> : checks.isError ? <EmptyState>No quality results are recorded for this run (they are written only when load_data runs).</EmptyState>
            : !checks.data?.quality_results.length ? <EmptyState>No quality results recorded.</EmptyState> : (
            <div className="table-wrap">
              <table className="table">
                <thead><tr><th>Dataset</th><th>Check</th><th>Status</th><th>Details</th></tr></thead>
                <tbody>{checks.data.quality_results.map((q) => (
                  <tr key={q.id}><td>{q.dataset}</td><td className="mono">{q.check_name}</td><td><StatusBadge status={q.status} /></td><td className="mono muted">{JSON.stringify(q.details)}</td></tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </Card>
        <Card flush title="Profile history" subtitle="Every profile captured at ingestion">
          {profiles.isLoading ? <SkeletonRows rows={5} /> : !profiles.data?.length ? <EmptyState /> : (
            <div className="table-wrap" style={{ maxHeight: 300 }}>
              <table className="table">
                <thead><tr><th>Dataset</th><th>Batch</th><th className="r">Rows</th><th className="r">Dup %</th><th className="r">Null %</th></tr></thead>
                <tbody>{profiles.data.map((p, i) => (
                  <tr key={`${p.dataset}-${p.timestamp}-${i}`}><td>{p.dataset}</td><td className="mono">{batchDate(p) ?? "N/A"}</td><td className="r mono">{fmtNum(p.row_count)}</td>
                    <td className={`r mono ${p.duplicate_percentage > 0 ? "text-warning" : ""}`}>{p.duplicate_percentage}</td><td className={`r mono ${p.null_percentage_overall > 0 ? "text-warning" : ""}`}>{p.null_percentage_overall}</td></tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </Card>
      </div>

      <Card className="mt" flush title="Recent quality incidents">
        {incidents.isLoading ? <SkeletonRows rows={5} /> : !qualityIncidents.length ? <EmptyState>No quality incidents recorded.</EmptyState> : (
          <IncidentTable compact incidents={qualityIncidents.slice(0, 8)} onSelect={(i) => navigate(`/incidents?id=${encodeURIComponent(i.incident_id)}`)} />
        )}
      </Card>
      {profiles.error && <div style={{ marginTop: 10 }}><ErrorState error={profiles.error} onRetry={profiles.refetch} /></div>}
    </>
  );
}
