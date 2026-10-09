import type { Task } from "../api/types";
import { fmtDuration, fmtTime } from "../lib/format";
import { EmptyState, StatusBadge, toneOf } from "./ui";

const BAR: Record<string, string> = {
  success: "var(--success-dot)", danger: "var(--danger-dot)", warning: "var(--warning-dot)",
  info: "var(--info-dot)", neutral: "var(--neutral-dot)", ai: "var(--primary)", violet: "var(--violet)",
};

/** Gantt-style view of real Airflow task start/end times. */
export function TaskTimeline({ tasks }: { tasks: Task[] }) {
  if (!tasks.length) return <EmptyState>No task instances exist for this run yet.</EmptyState>;
  const now = Date.now();
  const times = tasks.flatMap((t) => [t.start_date, t.end_date]).filter(Boolean).map((v) => new Date(v!).getTime());
  const min = times.length ? Math.min(...times) : now;
  const max = Math.max(...times, ...tasks.filter((t) => t.state === "running").map(() => now));
  const span = max - min || 1;
  return (
    <div>
      {tasks.map((t) => {
        const s = t.start_date ? new Date(t.start_date).getTime() : null;
        const e = t.end_date ? new Date(t.end_date).getTime() : t.state === "running" ? now : s;
        return (
          <div className="gantt-row" key={t.task_id}>
            <span className="mono truncate" title={t.task_id}>{t.task_id}</span>
            <div className="gantt-track" title={`${fmtTime(t.start_date)} → ${fmtTime(t.end_date)}`}>
              {s !== null && <span className="gantt-bar" style={{ left: `${((s - min) / span) * 100}%`, width: `${(((e ?? s) - s) / span) * 100}%`, background: BAR[toneOf(t.state)] }} />}
            </div>
            <div style={{ textAlign: "right" }}>
              {t.duration_s !== null ? <span className="small mono">{fmtDuration(t.duration_s)}</span> : <StatusBadge status={t.state ?? "pending"} />}
            </div>
          </div>
        );
      })}
    </div>
  );
}
