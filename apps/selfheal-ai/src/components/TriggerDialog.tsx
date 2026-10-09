import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, Dices, Info, Loader2, Play } from "lucide-react";
import { useDags, useTriggerRun } from "../api/hooks";
import { utcIso } from "../lib/format";
import { useToast } from "../lib/toast";
import { Modal } from "./Overlay";

const pad = (n: number) => String(n).padStart(2, "0");
export function newRunId(prefix = "ui") {
  const d = new Date();
  return `${prefix}__${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
}
/** Two minutes ago in UTC — never "in the future" for Airflow. */
export function defaultLogical() {
  const d = new Date(Date.now() - 2 * 60_000);
  return { date: `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`, time: `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}` };
}
export const RUN_ID_RE = /^[A-Za-z0-9_.:+-]{1,200}$/;

/** Confirmation + form for triggering the existing DAG through the platform API. */
export function TriggerDialog({ onClose, onTriggered }: { onClose: () => void; onTriggered?: (runId: string) => void }) {
  const dags = useDags();
  const trigger = useTriggerRun();
  const toast = useToast();
  const init = useMemo(defaultLogical, []);
  const [dagId, setDagId] = useState("");
  const [runId, setRunId] = useState(() => newRunId());
  const [date, setDate] = useState(init.date);
  const [time, setTime] = useState(init.time);

  useEffect(() => { if (!dagId && dags.data?.length) setDagId(dags.data[0].dag_id); }, [dags.data, dagId]);
  const dag = dags.data?.find((d) => d.dag_id === dagId);
  const logical = date && time ? utcIso(date, time) : "";
  const future = logical ? new Date(logical).getTime() > Date.now() : false;
  const valid = !!dagId && RUN_ID_RE.test(runId) && !!logical && !future && !dag?.is_paused;

  const submit = () => trigger.mutate({ dag_id: dagId, run_id: runId, logical_date: logical, fault_type: null }, {
    onSuccess: (res) => {
      toast({ kind: "success", title: "DAG run triggered", message: `${res.run.dag_run_id} is ${res.run.state}. The scheduler picks it up within seconds.` });
      onTriggered?.(res.run.dag_run_id);
      onClose();
    },
    onError: (e) => toast({ kind: "error", title: "Trigger rejected", message: (e as Error).message }),
  });

  return (
    <Modal title="Trigger DAG run" subtitle="Starts the existing Airflow DAG via the Airflow REST API. No configuration is changed." onClose={trigger.isPending ? () => {} : onClose}
      footer={<>
        <button className="btn" onClick={onClose} disabled={trigger.isPending}>Cancel</button>
        <button className="btn btn-primary" disabled={!valid || trigger.isPending} onClick={submit}>
          {trigger.isPending ? <Loader2 size={14} className="spin" /> : <Play size={14} />} Confirm & trigger
        </button>
      </>}>
      <div className="field">
        <label htmlFor="t-dag">DAG</label>
        <select id="t-dag" className="select" value={dagId} onChange={(e) => setDagId(e.target.value)}>
          {dags.isLoading && <option>Loading…</option>}
          {dags.data?.map((d) => <option key={d.dag_id} value={d.dag_id}>{d.dag_id}{d.is_paused ? " (paused)" : ""}</option>)}
        </select>
        {dags.isError && <span className="error-text">Could not load DAGs: {(dags.error as Error).message}</span>}
        {dag?.is_paused && <span className="error-text">This DAG is paused; a new run would stay queued. Unpause it in Airflow first.</span>}
      </div>
      <div className="field">
        <label htmlFor="t-run">Run ID</label>
        <div className="row">
          <input id="t-run" className="input mono" value={runId} onChange={(e) => setRunId(e.target.value.trim())} />
          <button className="btn btn-icon" onClick={() => setRunId(newRunId())} title="Generate a unique run ID"><Dices size={14} /></button>
        </div>
        {!RUN_ID_RE.test(runId) && <span className="error-text">Letters, digits and _ . : + - only.</span>}
      </div>
      <div className="grid g-2" style={{ gap: 10 }}>
        <div className="field"><label htmlFor="t-date">Execution date (UTC)</label><input id="t-date" type="date" className="input" value={date} onChange={(e) => setDate(e.target.value)} /></div>
        <div className="field"><label htmlFor="t-time">Time (UTC)</label><input id="t-time" type="time" step={1} className="input" value={time} onChange={(e) => setTime(e.target.value)} /></div>
      </div>
      {future ? (
        <div className="alert danger"><AlertTriangle size={15} /><div className="alert-body">This logical date is in the future. Airflow would keep the run queued with every task in state None until then.</div></div>
      ) : (
        <div className="alert info"><Info size={15} /><div className="alert-body">Tasks process the raw batch for <strong>{date}</strong>, which must exist (<code>scripts\generate_daily_data.py --date {date}</code>). Each run needs a unique timestamp.</div></div>
      )}
      {trigger.isError && <div className="alert danger"><AlertTriangle size={15} /><div className="alert-body">{(trigger.error as Error).message}</div></div>}
    </Modal>
  );
}
