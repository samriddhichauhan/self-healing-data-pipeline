import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { AlertTriangle, CheckCircle2, Dices, FlaskConical, Info, Loader2, Siren, XCircle } from "lucide-react";
import { useDags, useRun, useRunPipeline, useTriggerOptions, useTriggerRun } from "../api/hooks";
import { ConfirmationModal } from "../components/Overlay";
import { StageStepper } from "../components/PipelineFlow";
import { defaultLogical, newRunId, RUN_ID_RE } from "../components/TriggerDialog";
import { Badge, Card, ConfidenceMeter, EmptyState, ErrorState, KV, PageHeader, SkeletonRows, SourceBadge, StatusBadge } from "../components/ui";
import { isActive, runProgress, toStages } from "../lib/derive";
import { fmtDuration, utcIso } from "../lib/format";
import { useToast } from "../lib/toast";

function Tracker({ runId, injection }: { runId: string; injection: string | null }) {
  const run = useRun(runId);
  const graph = useRunPipeline(runId);
  const r = run.data?.run;
  const incident = run.data?.incidents[run.data.incidents.length - 1];
  const progress = runProgress(run.data?.tasks ?? []);
  const live = isActive(r);
  const outcome = !r ? null : live ? "running" : r.pipeline_status === "RECOVERED" ? "recovered" : r.state === "failed" || r.pipeline_status === "FAILED" ? "failed" : r.pipeline_status ?? r.state;

  return (
    <Card className="mt" title={<>Test run <span className="mono small muted">{runId}</span></>} subtitle="Updates every 3 seconds while the run is active"
      actions={<><Link className="btn btn-sm" to={`/live?run=${encodeURIComponent(runId)}`}>Live monitor</Link><Link className="btn btn-sm" to={`/flow?run=${encodeURIComponent(runId)}`}>Pipeline flow</Link></>}>
      {run.isError ? <ErrorState error={run.error} /> : !r ? <SkeletonRows rows={6} /> : (
        <>
          {graph.data && <div style={{ marginBottom: 16 }}><StageStepper stages={toStages(graph.data.nodes, graph.data.run)} /></div>}
          <div className="grid g-2" style={{ gap: 20 }}>
            <div>
              <div className="section-title">1 · Injection</div>
              {injection ? <div className="alert info"><FlaskConical size={14} /><div className="alert-body mono small">{injection}</div></div>
                : <div className="small muted">Injected before this run was triggered (output not available after a page reload).</div>}
              <div className="section-title">2 · Pipeline run</div>
              <KV items={[
                ["Airflow state", <StatusBadge status={r.state} pulse={live} />],
                ["Progress", `${progress.completed} / ${progress.total} tasks${progress.running ? ` · running ${progress.running.task_id}` : ""}`],
                ["Duration", fmtDuration(r.duration_s)],
              ]} />
            </div>
            <div>
              <div className="section-title">3 · Resulting incident</div>
              {!incident ? <div className="small muted">{live ? "Waiting for validate_quality…" : "No incident was recorded for this run."}</div> : (
                <KV items={[
                  ["Incident", <Link className="mono" to={`/incidents?id=${encodeURIComponent(incident.incident_id)}`}>{incident.incident_id}</Link>],
                  ["Fault", incident.fault_category], ["Status", <StatusBadge status={incident.status} />],
                ]} />
              )}
              <div className="section-title">4 · AI diagnosis</div>
              {!incident ? <div className="small muted">Not diagnosed yet.</div> : (
                <KV items={[
                  ["Engine", <SourceBadge source={incident.diagnosis_source} />], ["Root cause", incident.root_cause],
                  ["Confidence", incident.diagnosis_source === "LOCAL_OLLAMA_AI" ? <ConfidenceMeter value={incident.ai_confidence} compact /> : "N/A (rule-based)"],
                  ["Policy decision", incident.policy_decision ? <StatusBadge status={incident.policy_decision} /> : null],
                ]} />
              )}
            </div>
          </div>
          <div className="section-title">5 · Recovery result</div>
          {outcome === "running" ? <div className="alert info"><Loader2 size={14} className="spin" /><div className="alert-body">Run in progress. An Ollama diagnosis typically takes 45–80 s; escalated faults fail after two retries (about 5 minutes).</div></div>
            : outcome === "recovered" ? <div className="alert success"><CheckCircle2 size={14} /><div className="alert-body"><strong>RECOVERED.</strong> The fault was auto-remediated and verified; the pipeline completed. {incident?.remediation?.removed_duplicates !== undefined && `${incident.remediation.removed_duplicates} duplicate rows removed.`}</div></div>
            : outcome === "failed" ? <div className="alert danger"><XCircle size={14} /><div className="alert-body"><strong>FAILED — escalated to human review.</strong> The injected staging file stays in place, so later runs for this date keep failing until it is restored (see README “Demonstrate escalation”).</div></div>
            : <div className="alert success"><CheckCircle2 size={14} /><div className="alert-body">Run finished {outcome}.</div></div>}
        </>
      )}
    </Card>
  );
}

export default function FaultInjection() {
  const options = useTriggerOptions();
  const dags = useDags();
  const trigger = useTriggerRun();
  const toast = useToast();
  const [params, setParams] = useSearchParams();
  const tracked = params.get("run");
  const init = useMemo(defaultLogical, []);
  const [fault, setFault] = useState("duplicate_ingestion");
  const [date, setDate] = useState(init.date);
  const [time, setTime] = useState(init.time);
  const [runId, setRunId] = useState(() => newRunId("fault"));
  const [confirming, setConfirming] = useState(false);
  const [injection, setInjection] = useState<string | null>(null);

  const f = options.data?.fault_types.find((x) => x.id === fault);
  const dag = dags.data?.[0];
  const logical = utcIso(date, time);
  const future = new Date(logical).getTime() > Date.now();
  const valid = RUN_ID_RE.test(runId) && !future && !!dag && !dag.is_paused;

  const run = () => trigger.mutate({ dag_id: dag!.dag_id, run_id: runId, logical_date: logical, fault_type: fault }, {
    onSuccess: (res) => {
      setInjection(res.fault_injection?.cli_output ?? null);
      setParams({ run: res.run.dag_run_id });
      setConfirming(false);
      setRunId(newRunId("fault"));
      toast({ kind: "success", title: "Fault injected and run triggered", message: res.run.dag_run_id });
    },
  });

  return (
    <>
      <PageHeader title="Fault Injection" description="Safely simulate pipeline failures to validate AI diagnosis and automated recovery." />
      <div className="grid g-side">
        <Card title="1 · Choose a fault" subtitle="The fault types supported by the existing scripts/fault-injection/inject_fault.py">
          {options.isError ? <ErrorState error={options.error} /> : !options.data ? <SkeletonRows rows={4} /> : (
            <div className="fault-grid">
              {options.data.fault_types.map((x) => (
                <button key={x.id} className={`fault-card ${fault === x.id ? "on" : ""}`} onClick={() => setFault(x.id)}>
                  <div className="row between"><code>{x.id}</code><Badge tone={x.expected === "AUTO_FIX" ? "success" : "warning"}>{x.expected}</Badge></div>
                  <div className="small muted">{x.effect}</div>
                </button>
              ))}
            </div>
          )}
        </Card>
        <Card title="2 · Target and run">
          <div className="stack" style={{ gap: 12 }}>
            <div className="grid g-2" style={{ gap: 10 }}>
              <div className="field"><label htmlFor="f-date">Execution date (UTC)</label><input id="f-date" type="date" className="input" value={date} onChange={(e) => setDate(e.target.value)} /></div>
              <div className="field"><label htmlFor="f-time">Time (UTC)</label><input id="f-time" type="time" step={1} className="input" value={time} onChange={(e) => setTime(e.target.value)} /></div>
            </div>
            <div className="field">
              <label htmlFor="f-run">Run ID</label>
              <div className="row"><input id="f-run" className="input mono" value={runId} onChange={(e) => setRunId(e.target.value.trim())} /><button className="btn btn-icon" onClick={() => setRunId(newRunId("fault"))} title="New run ID"><Dices size={14} /></button></div>
            </div>
            <div className="field">
              <label>Data directory</label>
              <input className="input mono" readOnly value="managed by the pipeline backend (data/)" />
              <span className="hint">The existing injection endpoint uses the backend's own data directory and does not accept another one.</span>
            </div>
            {future && <div className="alert danger"><AlertTriangle size={14} /><div className="alert-body">This time is in the future; Airflow would leave the run queued.</div></div>}
            {f && (
              <div className={`alert ${f.expected === "AUTO_FIX" ? "info" : "warning"}`}>
                <Info size={14} />
                <div className="alert-body">{f.expected === "AUTO_FIX"
                  ? <>Expected: detected by validate_quality → Ollama diagnosis → Policy Gate AUTO_FIX (if confidence ≥ 0.85) → dedupe → verification → <strong>RECOVERED</strong>.</>
                  : <>Expected: detected and <strong>escalated</strong> by policy; the run ends FAILED and staging for <strong>{date}</strong> stays modified until restored.</>}</div>
              </div>
            )}
            <button className="btn btn-primary" disabled={!valid || trigger.isPending} onClick={() => setConfirming(true)}><Siren size={14} /> Inject fault</button>
          </div>
        </Card>
      </div>

      {tracked ? <Tracker runId={tracked} injection={injection} /> : (
        <Card className="mt"><EmptyState icon={<FlaskConical size={18} />} title="No test run yet">Choose a fault and inject it to follow detection, AI diagnosis and recovery here.</EmptyState></Card>
      )}

      {confirming && f && (
        <ConfirmationModal title="Inject this fault?" confirmLabel="Inject fault & run pipeline" danger={f.expected === "ESCALATE"} busy={trigger.isPending}
          error={trigger.isError ? (trigger.error as Error).message : null} onClose={() => { setConfirming(false); trigger.reset(); }} onConfirm={run}>
          <KV items={[
            ["Fault", <code>{f.id}</code>], ["Effect", f.effect], ["Expected outcome", <Badge tone={f.expected === "AUTO_FIX" ? "success" : "warning"}>{f.expected}</Badge>],
            ["Batch date", <span className="mono">{date}</span>], ["Run ID", <span className="mono">{runId}</span>],
          ]} />
          <div className="alert warning"><AlertTriangle size={14} /><div className="alert-body">This rewrites the staged data for <strong>{date}</strong> using the existing injection script, then triggers the DAG. Raw data and the database are not modified directly.</div></div>
        </ConfirmationModal>
      )}
    </>
  );
}
