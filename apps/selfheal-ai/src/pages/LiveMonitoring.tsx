import { useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { BrainCircuit, Cpu, GitBranch, ListTodo, Loader2, Timer } from "lucide-react";
import { useAIDiagnoses, useAIStatus, useHealth, useRun, useRunPipeline, useRuns, useTaskLog } from "../api/hooks";
import { StageStepper } from "../components/PipelineFlow";
import { TaskTimeline } from "../components/TaskTimeline";
import { TL_COLOR, Timeline, type TimelineItem } from "../components/Timeline";
import { Card, EmptyState, ErrorState, MetricCard, PageHeader, SkeletonRows, StatusBadge } from "../components/ui";
import { isActive, runProgress, toStages, TASK_ORDER } from "../lib/derive";
import { fmtDuration, fmtConf } from "../lib/format";
import { isoTs, useNow } from "../lib/useNow";

export default function LiveMonitoring() {
  const [params, setParams] = useSearchParams();
  const runs = useRuns(60);
  const active = runs.data?.find((r) => isActive(r));
  const runId = params.get("run") ?? active?.run_id ?? runs.data?.[0]?.run_id ?? null;
  const run = useRun(runId);
  const graph = useRunPipeline(runId);
  const ai = useAIStatus();
  const health = useHealth();
  const diagnoses = useAIDiagnoses(300);
  const r = run.data?.run;
  const tasks = run.data?.tasks ?? [];
  const live = isActive(r);
  const now = useNow(live);
  const quality = tasks.find((t) => t.task_id === "validate_quality");
  const qLog = useTaskLog(quality?.start_date ? runId : null, "validate_quality", Math.max(1, quality?.try_number ?? 1), live);
  const progress = runProgress(tasks);
  const elapsed = r?.start_date ? (live ? (now - new Date(r.start_date).getTime()) / 1000 : r.duration_s) : null;

  const events = useMemo<TimelineItem[]>(() => {
    const items: TimelineItem[] = [];
    for (const t of tasks) {
      if (t.start_date) items.push({ ts: t.start_date, title: `${t.task_id} started`, color: TL_COLOR.info });
      if (t.end_date && t.state) items.push({ ts: t.end_date, title: `${t.task_id} ${t.state}`, meta: t.duration_s !== null ? fmtDuration(t.duration_s) : undefined, color: t.state === "success" ? TL_COLOR.success : t.state === "failed" ? TL_COLOR.danger : TL_COLOR.neutral });
    }
    const ids = new Set((run.data?.incidents ?? []).map((i) => i.incident_id));
    for (const i of run.data?.incidents ?? []) {
      if (i.detected_at) items.push({ ts: i.detected_at, title: `Incident detected · ${i.fault_category}`, meta: `${i.incident_id} · ${i.observed ?? ""}`, color: TL_COLOR.warning });
      if (i.status === "REMEDIATED") items.push({ ts: i.updated_at, title: "Remediation complete · verification passed", meta: i.verification ? `${i.verification.remaining_rows} rows · ${i.verification.duplicate_count} duplicates` : undefined, color: TL_COLOR.success });
      if (i.status === "ESCALATED") items.push({ ts: i.updated_at, title: "Escalated to human review", meta: i.incident_id, color: TL_COLOR.danger });
    }
    for (const d of diagnoses.data ?? []) {
      if (!ids.has(d.incident_id)) continue;
      if (d.request_ts && !d.fallback) items.push({ ts: d.request_ts, title: "Ollama request sent", meta: `${d.model ?? ""} · ${d.incident_id}`, color: TL_COLOR.info });
      if (d.response_ts) items.push({ ts: d.response_ts, title: "Ollama response received", meta: d.response_time_s !== null ? `${d.response_time_s.toFixed(2)} s` : undefined, color: TL_COLOR.ai });
      if (d.diagnosis_ts) {
        items.push({ ts: d.diagnosis_ts, title: "AI diagnosis generated", meta: d.root_cause ?? undefined, color: TL_COLOR.ai });
        items.push({ ts: d.diagnosis_ts, title: `Policy Gate: ${d.policy_gate_action ?? "N/A"}`, meta: `Confidence ${fmtConf(d.confidence)} · risk ${d.risk ?? "N/A"}`, color: TL_COLOR.policy });
      }
      if (d.fallback) items.push({ ts: d.last_ts, title: "Rule-based fallback used", meta: d.fallback_reason ?? undefined, color: TL_COLOR.warning });
    }
    for (const e of qLog.data?.entries ?? []) {
      if (e.continuation || !e.ts) continue;
      if (e.marker === "SELF-HEALING SUCCESS") items.push({ ts: isoTs(e.ts), title: "Self-healing succeeded", meta: e.message.replace("[SELF-HEALING SUCCESS] ", ""), color: TL_COLOR.success });
    }
    return items;
  }, [tasks, run.data?.incidents, diagnoses.data, qLog.data]);

  const comp = (k: string) => health.data?.components.find((c) => c.key === k);
  return (
    <>
      <PageHeader title="Live Monitoring" description="Follow a DAG run as it executes. Active runs refresh every 3 seconds."
        actions={
          <select className="select" style={{ width: 320 }} value={runId ?? ""} onChange={(e) => setParams({ run: e.target.value })} aria-label="Run">
            {runs.data?.map((x) => <option key={x.run_id} value={x.run_id}>{isActive(x) ? "● " : ""}{x.run_id}</option>)}
          </select>
        } />
      {runs.data && !runs.data.length && <Card><EmptyState title="Waiting for pipeline data">No DAG runs exist yet. Trigger one from Pipeline Runs.</EmptyState></Card>}
      {run.isError && <ErrorState error={run.error} />}
      {runId && (
        <>
          <div className={`alert ${live ? "info" : r?.state === "failed" ? "danger" : "success"}`} style={{ marginBottom: 14 }}>
            {live ? <Loader2 size={15} className="spin" /> : <GitBranch size={15} />}
            <div className="alert-body">
              {!r ? "Loading run…" : live ? <><strong>{r.run_id}</strong> is {r.state}{progress.running ? <> · running <span className="mono">{progress.running.task_id}</span></> : null}</>
                : <><strong>{r.run_id}</strong> finished {r.state}{r.pipeline_status ? ` · pipeline ${r.pipeline_status}` : ""}. {active ? <>A run is active now: <Link to={`/live?run=${encodeURIComponent(active.run_id)}`}>{active.run_id}</Link></> : "No run is currently active."}</>}
            </div>
            <Link className="btn btn-sm" to={`/flow?run=${encodeURIComponent(runId)}`}>Pipeline flow</Link>
          </div>

          <div className="grid g-4">
            <MetricCard label="Pipeline status" icon={<GitBranch size={14} />} value={r ? <StatusBadge status={r.pipeline_status ?? r.state} pulse={live} /> : null} foot={r ? `Airflow state: ${r.state}` : undefined} loading={!r} />
            <MetricCard label="Current task" icon={<ListTodo size={14} />} tone="violet" loading={!r}
              value={progress.running ? <span className="mono" style={{ fontSize: 15 }}>{progress.running.task_id}</span> : <span style={{ fontSize: 15, color: "var(--muted)" }}>{live ? "Waiting for scheduler" : "None (run finished)"}</span>}
              foot={progress.running?.start_date ? `since ${new Date(progress.running.start_date).toISOString().slice(11, 19)} UTC` : undefined} />
            <MetricCard label={live ? "Elapsed" : "Duration"} icon={<Timer size={14} />} tone="warning" loading={!r} value={elapsed !== null && elapsed !== undefined ? fmtDuration(elapsed) : null} foot={r?.start_date ? `Started ${new Date(r.start_date).toISOString().slice(11, 19)} UTC` : "Not started"} />
            <MetricCard label="Completed tasks" icon={<ListTodo size={14} />} tone="success" loading={!r} value={r ? `${progress.completed} / ${progress.total}` : null}
              foot={<div className={`progress ${live ? "running" : ""}`} style={{ width: "100%", marginTop: 4 }}><span style={{ width: `${(progress.completed / progress.total) * 100}%` }} /></div>} />
          </div>

          <div className="grid g-4 mt">
            <MetricCard label="Remaining tasks" icon={<ListTodo size={14} />} tone="neutral" loading={!r} value={progress.remaining.length}
              foot={progress.remaining.length ? <span className="mono truncate" title={progress.remaining.join(", ")}>{progress.remaining.slice(0, 3).join(", ")}{progress.remaining.length > 3 ? "…" : ""}</span> : "All tasks finished"} />
            <MetricCard label="AI status" icon={<BrainCircuit size={14} />} loading={ai.isLoading}
              value={ai.data ? <StatusBadge status={ai.data.connected && ai.data.model_installed ? "connected" : "offline"} label={ai.data.connected && ai.data.model_installed ? "Ready" : "Unavailable"} /> : null}
              foot={ai.data ? `${ai.data.model} ${ai.data.model_installed ? "installed" : "missing"}` : undefined} />
            <MetricCard label="Ollama status" icon={<Cpu size={14} />} loading={ai.isLoading}
              value={ai.data ? <StatusBadge status={ai.data.connected ? "connected" : "offline"} /> : null} foot={ai.data ? (ai.data.connected ? `${ai.data.latency_ms} ms · ${ai.data.host}` : ai.data.error) : undefined} />
            <MetricCard label="Airflow scheduler" icon={<Timer size={14} />} loading={health.isLoading}
              value={comp("scheduler") ? <StatusBadge status={comp("scheduler")!.status} /> : null} foot={comp("scheduler")?.detail} />
          </div>

          <Card className="mt" title="Stage progress">
            {graph.data ? <StageStepper stages={toStages(graph.data.nodes, graph.data.run)} /> : <SkeletonRows rows={1} />}
          </Card>

          <div className="grid g-main mt">
            <Card title="Live activity timeline" subtitle="Assembled from Airflow task instances, the AI adapter's [OLLAMA_*] events, incidents and the validate_quality task log">
              {!run.data ? <SkeletonRows rows={8} /> : <Timeline items={events} empty="No events recorded for this run yet." />}
            </Card>
            <Card title="Task execution" subtitle={`${TASK_ORDER.length} DAG tasks`}>
              {!run.data ? <SkeletonRows rows={8} /> : <TaskTimeline tasks={tasks} />}
            </Card>
          </div>
        </>
      )}
    </>
  );
}
