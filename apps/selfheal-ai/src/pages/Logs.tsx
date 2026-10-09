import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { RefreshCw } from "lucide-react";
import { useLogFile, useLogFiles, useRuns, useTaskLog } from "../api/hooks";
import { LogViewer } from "../components/LogViewer";
import { ErrorState, PageHeader, Segmented, Skeleton, Switch } from "../components/ui";
import { TASK_ORDER } from "../lib/derive";

export default function Logs() {
  const [params, setParams] = useSearchParams();
  const [source, setSource] = useState<"airflow" | "file">(params.get("file") ? "file" : "airflow");
  const [live, setLive] = useState(true);
  const [paused, setPaused] = useState(false);
  const [tail, setTail] = useState(2000);
  const runs = useRuns(100);
  const files = useLogFiles();
  const runId = params.get("run") ?? runs.data?.[0]?.run_id ?? null;
  const task = params.get("task") ?? "validate_quality";
  const tryNo = Number(params.get("try") ?? 1);
  const file = params.get("file") ?? files.data?.[0]?.filename ?? null;
  const polling = live && !paused;

  const set = (patch: Record<string, string>) => {
    const next = new URLSearchParams(params);
    Object.entries(patch).forEach(([k, v]) => next.set(k, v));
    setParams(next, { replace: true });
  };
  const switchSource = (s: "airflow" | "file") => {
    setSource(s);
    if (s === "airflow" && params.has("file")) { const p = new URLSearchParams(params); p.delete("file"); setParams(p, { replace: true }); }
  };

  const taskLog = useTaskLog(source === "airflow" ? runId : null, task, tryNo, polling);
  const fileLog = useLogFile(source === "file" ? file : null, tail, polling);
  const active = source === "airflow" ? taskLog : fileLog;

  const selector = source === "airflow" ? (
    <>
      <select className="select" style={{ width: 280 }} value={runId ?? ""} onChange={(e) => set({ run: e.target.value })} aria-label="Run">
        {runs.data?.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id}</option>)}
      </select>
      <select className="select" style={{ width: 170 }} value={task} onChange={(e) => set({ task: e.target.value, try: "1" })} aria-label="Task">
        {TASK_ORDER.map((t) => <option key={t}>{t}</option>)}
      </select>
      <select className="select" style={{ width: 86 }} value={tryNo} onChange={(e) => set({ try: e.target.value })} aria-label="Attempt">
        {[1, 2, 3].map((n) => <option key={n} value={n}>Try {n}</option>)}
      </select>
    </>
  ) : (
    <>
      <select className="select" style={{ width: 250 }} value={file ?? ""} onChange={(e) => set({ file: e.target.value })} aria-label="Log file">
        {files.data?.map((f) => <option key={f.filename} value={f.filename}>{f.filename} ({(f.size_bytes / 1024).toFixed(0)} KB)</option>)}
      </select>
      <select className="select" style={{ width: 140 }} value={tail} onChange={(e) => setTail(Number(e.target.value))} aria-label="Lines">
        {[500, 2000, 5000, 20000].map((n) => <option key={n} value={n}>Last {n.toLocaleString()}</option>)}
      </select>
    </>
  );

  return (
    <>
      <PageHeader title="Logs" description={source === "airflow" ? "Airflow task logs, fetched live through the Airflow REST API." : "Pipeline log files written by the pipeline and the Ollama adapter (logs/)."}
        actions={<>
          <Segmented value={source} onChange={switchSource} options={[{ value: "airflow", label: "Airflow task logs" }, { value: "file", label: "Pipeline log files" }]} />
          <Switch checked={live} onChange={setLive} label="Live" />
          <button className="btn" onClick={() => active.refetch()}><RefreshCw size={14} className={active.isFetching ? "spin" : ""} /> Refresh</button>
        </>} />
      {active.isError ? <ErrorState error={active.error} onRetry={active.refetch} />
        : active.isLoading && !active.data ? <Skeleton height={460} style={{ borderRadius: 10 }} />
        : <LogViewer entries={active.data?.entries ?? []} header={selector} paused={live ? paused : undefined} onTogglePause={live ? () => setPaused(!paused) : undefined}
            exportName={active.data?.name ?? "logs"}
            emptyText={source === "airflow" ? "This task produced no log output (or has not run yet)." : "This log file is empty."} />}
      {active.data && <div className="small muted" style={{ marginTop: 8 }}>{active.data.name} · {active.data.total_lines.toLocaleString()} lines · export downloads the currently loaded, filtered lines</div>}
    </>
  );
}
