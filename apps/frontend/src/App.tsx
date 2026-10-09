import { useEffect, useState, useCallback } from "react";
import { Activity, AlertTriangle, CheckCircle2, PlayCircle, RefreshCw, Zap } from "lucide-react";
import api, { type PipelineRun, type Incident, type PipelineSummary, type TaskInstance } from "./api";

const EXECUTION_DATE = "2026-06-01";
const FAULT_TYPES = ["duplicate_ingestion", "schema_drift", "null_spike", "volume_drop", "referential_break", "staleness"];
const TASK_ORDER = [
  "ingest_dimensions", "ingest_orders", "ingest_events",
  "validate_schema", "validate_quality", "transform_data", "load_data", "agent_monitoring",
];

function StatusBadge({ status }: { status: string }) {
  const cls = status === "HEALTHY" || status === "SUCCESS" ? "badge badge-healthy"
    : status === "RECOVERED" ? "badge badge-recovered"
    : status === "FAILED" ? "badge badge-failed"
    : "badge badge-pending";
  return <span className={cls}>{status}</span>;
}

function App() {
  const [summary, setSummary] = useState<PipelineSummary | null>(null);
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [taskInstances, setTaskInstances] = useState<TaskInstance[]>([]);
  const [backendError, setBackendError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [faultType, setFaultType] = useState(FAULT_TYPES[0]);
  const [lastAction, setLastAction] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [s, r, i] = await Promise.all([api.summary(), api.runs(20), api.incidents(20)]);
      setSummary(s);
      setRuns(r);
      setIncidents(i);
      setBackendError(null);
      if (!selectedRunId && r.length > 0) setSelectedRunId(r[0].run_id);
    } catch (e: any) {
      setBackendError(e.message || String(e));
    }
  }, [selectedRunId]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, [refresh]);

  useEffect(() => {
    if (!selectedRunId) return;
    let cancelled = false;
    api.airflowState(selectedRunId)
      .then((res) => !cancelled && setTaskInstances(res.task_instances || []))
      .catch(() => !cancelled && setTaskInstances([]));
    return () => { cancelled = true; };
  }, [selectedRunId, runs]);

  const selectedRun = runs.find((r) => r.run_id === selectedRunId) || summary?.latest_run || null;

  async function handleTrigger() {
    setBusy("trigger");
    setLastAction(null);
    try {
      const res = await api.triggerRun(EXECUTION_DATE);
      setLastAction(`Triggered DAG run ${res.dag_run_id || res.run_id || "(pending)"} for ${EXECUTION_DATE}.`);
      setTimeout(refresh, 2000);
    } catch (e: any) {
      setLastAction(`Trigger failed: ${e.message}`);
    } finally {
      setBusy(null);
    }
  }

  async function handleInjectFault() {
    setBusy("inject");
    setLastAction(null);
    try {
      const res = await api.injectFault(faultType, EXECUTION_DATE);
      setLastAction(`Fault injected (${faultType}): ${res.cli_output}`);
    } catch (e: any) {
      setLastAction(`Fault injection failed: ${e.message}`);
    } finally {
      setBusy(null);
    }
  }

  const taskState = (taskId: string) => taskInstances.find((t) => t.task_id === taskId)?.state || "no_status";

  return (
    <div className="app">
      <header className="app-header">
        <div className="brand"><Activity size={22} /><h1>Self-Healing Data Pipeline</h1></div>
        <div className="header-actions">
          <button className="btn" onClick={refresh} title="Refresh now"><RefreshCw size={16} /> Refresh</button>
        </div>
      </header>

      {backendError && (
        <div className="banner banner-error">
          <AlertTriangle size={16} /> Backend unreachable: {backendError}
        </div>
      )}

      <section className="controls">
        <button className="btn btn-primary" disabled={busy !== null} onClick={handleTrigger}>
          <PlayCircle size={16} /> {busy === "trigger" ? "Triggering…" : `Trigger DAG run (${EXECUTION_DATE})`}
        </button>
        <div className="fault-controls">
          <select value={faultType} onChange={(e) => setFaultType(e.target.value)}>
            {FAULT_TYPES.map((f) => <option key={f} value={f}>{f}</option>)}
          </select>
          <button className="btn btn-danger" disabled={busy !== null} onClick={handleInjectFault}>
            <Zap size={16} /> {busy === "inject" ? "Injecting…" : "Inject fault"}
          </button>
        </div>
        {lastAction && <p className="last-action">{lastAction}</p>}
      </section>

      {summary && (
        <section className="summary-grid">
          <div className="card"><span className="metric">{summary.total_runs}</span><span className="label">Total runs</span></div>
          <div className="card"><span className="metric">{summary.healthy_runs}</span><span className="label">Healthy</span></div>
          <div className="card"><span className="metric">{summary.recovered_runs}</span><span className="label">Recovered</span></div>
          <div className="card"><span className="metric">{summary.failed_runs}</span><span className="label">Failed</span></div>
          <div className="card"><span className="metric">{summary.total_auto_fixes}</span><span className="label">Auto-fixes</span></div>
          <div className="card"><span className="metric">{summary.total_escalations}</span><span className="label">Escalations</span></div>
        </section>
      )}

      <section className="panels">
        <div className="panel">
          <h2>Pipeline Runs</h2>
          <table>
            <thead><tr><th>Run ID</th><th>Date</th><th>Status</th><th>Incidents</th></tr></thead>
            <tbody>
              {runs.map((r) => (
                <tr key={r.run_id} className={r.run_id === selectedRunId ? "selected" : ""} onClick={() => setSelectedRunId(r.run_id)}>
                  <td>{r.run_id}</td>
                  <td>{r.logical_date}</td>
                  <td><StatusBadge status={r.status} /></td>
                  <td>{r.incident_count}</td>
                </tr>
              ))}
              {runs.length === 0 && <tr><td colSpan={4}>No runs yet — trigger one above.</td></tr>}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <h2>Selected Run</h2>
          {selectedRun ? (
            <div className="run-detail">
              <div className="run-detail-header">
                <strong>{selectedRun.run_id}</strong>
                <StatusBadge status={selectedRun.status} />
              </div>
              <p>Logical date: {selectedRun.logical_date}</p>
              <p>Duration: {selectedRun.duration_seconds ? `${selectedRun.duration_seconds.toFixed(1)}s` : "—"}</p>
              {selectedRun.row_counts && (
                <p>Rows: {Object.entries(selectedRun.row_counts).map(([k, v]) => `${k}=${v}`).join(", ")}</p>
              )}
              <h3>Task States (live from Airflow)</h3>
              <ul className="task-list">
                {TASK_ORDER.map((t) => {
                  const state = taskState(t);
                  return (
                    <li key={t} className={`task-item state-${state}`}>
                      {state === "success" ? <CheckCircle2 size={14} /> : state === "failed" ? <AlertTriangle size={14} /> : <span className="dot" />}
                      {t}: {state}
                    </li>
                  );
                })}
              </ul>
            </div>
          ) : <p>No run selected.</p>}
        </div>

        <div className="panel">
          <h2>Incidents</h2>
          <ul className="incident-list">
            {incidents.map((inc) => (
              <li key={inc.incident_id} className="incident-item">
                <div className="incident-item-top">
                  <strong>{inc.incident_id}</strong>
                  <StatusBadge status={inc.status} />
                </div>
                <p>{inc.fault_category} · {inc.dataset} · confidence {(inc.confidence * 100).toFixed(0)}%</p>
                <p className="hypothesis">{inc.hypothesis}</p>
                <p className="meta">
                  Action: {inc.action} · Remediation: {inc.remediation_status} · Verification: {inc.verification_status}
                </p>
              </li>
            ))}
            {incidents.length === 0 && <li>No incidents recorded yet.</li>}
          </ul>
        </div>
      </section>
    </div>
  );
}

export default App;
