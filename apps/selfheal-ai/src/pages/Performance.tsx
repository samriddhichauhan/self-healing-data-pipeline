import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, Tooltip, XAxis, YAxis } from "recharts";
import { Gauge, Hourglass, Timer, Zap } from "lucide-react";
import { useAISummary, useRunDetails, useRuns } from "../api/hooks";
import { ChartCard, useChartTheme } from "../components/charts";
import { Card, EmptyState, MetricCard, PageHeader, SkeletonRows } from "../components/ui";
import { mean, TASK_ORDER } from "../lib/derive";
import { fmtDuration } from "../lib/format";

const SAMPLE = 15;
const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.round(p * (s.length - 1)))]; };

export default function Performance() {
  const runs = useRuns(300);
  const ai = useAISummary();
  const t = useChartTheme();
  const finished = (runs.data ?? []).filter((r) => r.state === "success" || r.state === "failed");
  const sample = finished.slice(0, SAMPLE).map((r) => r.run_id);
  const details = useRunDetails(sample);
  const loadingDetails = details.some((d) => d.isLoading);

  const durations = finished.map((r) => r.duration_s).filter((v): v is number => v !== null);
  const stats = useMemo(() => {
    const byTask = new Map<string, number[]>();
    const queue: number[] = [];
    for (const d of details) {
      for (const task of d.data?.tasks ?? []) {
        if (task.duration_s !== null) byTask.set(task.task_id, [...(byTask.get(task.task_id) ?? []), task.duration_s]);
        if (task.queued_when && task.start_date) queue.push((new Date(task.start_date).getTime() - new Date(task.queued_when).getTime()) / 1000);
      }
    }
    const rows = TASK_ORDER.filter((id) => byTask.has(id)).map((id) => {
      const xs = byTask.get(id)!;
      return { task: id, avg: mean(xs)!, p95: pct(xs, 0.95)!, max: Math.max(...xs), min: Math.min(...xs), n: xs.length };
    });
    return { rows, queue };
  }, [details]);
  const slowest = [...stats.rows].sort((a, b) => b.avg - a.avg)[0];

  return (
    <>
      <PageHeader title="Performance" description={`Run and task latency from Airflow task instances of the last ${SAMPLE} finished runs.`} />
      <div className="grid g-4">
        <MetricCard label="Avg run duration" icon={<Timer size={14} />} loading={runs.isLoading} value={mean(durations) !== null ? fmtDuration(mean(durations)) : null} foot={`${durations.length} finished runs`} />
        <MetricCard label="p95 run duration" tone="warning" icon={<Gauge size={14} />} loading={runs.isLoading} value={pct(durations, 0.95) !== null ? fmtDuration(pct(durations, 0.95)) : null} foot="95th percentile" />
        <MetricCard label="Avg scheduling latency" tone="violet" icon={<Hourglass size={14} />} loading={loadingDetails} value={mean(stats.queue) !== null ? fmtDuration(mean(stats.queue)) : null} foot="Task queued → started" />
        <MetricCard label="Slowest task (avg)" tone="danger" icon={<Zap size={14} />} loading={loadingDetails}
          value={slowest ? <span className="mono" style={{ fontSize: 15 }}>{slowest.task}</span> : null} foot={slowest ? `${fmtDuration(slowest.avg)} average · AI calls avg ${ai.data?.avg_response_time_s != null ? `${ai.data.avg_response_time_s.toFixed(1)} s` : "N/A"}` : undefined} />
      </div>

      <div className="grid g-main mt">
        <ChartCard title="Average task duration" subtitle={`Across ${sample.length} runs`} source="Airflow REST API (taskInstances)" empty={!stats.rows.length} height={290}>
          <BarChart data={stats.rows} layout="vertical" margin={{ top: 4, right: 16, left: 30, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} horizontal={false} />
            <XAxis type="number" {...t.axis} unit="s" /><YAxis type="category" dataKey="task" width={130} {...t.axis} />
            <Tooltip {...t.tooltip} cursor={{ fill: t.cursor }} formatter={(v: any) => [`${Number(v).toFixed(2)} s`, "Average"]} />
            <Bar isAnimationActive={false} dataKey="avg" fill={t.c1} radius={[0, 3, 3, 0]} maxBarSize={18} />
          </BarChart>
        </ChartCard>
        <Card flush title="Task latency breakdown" subtitle="Seconds per task instance">
          {loadingDetails && !stats.rows.length ? <SkeletonRows rows={8} /> : !stats.rows.length ? <EmptyState title="Waiting for pipeline data" /> : (
            <div className="table-wrap">
              <table className="table">
                <thead><tr><th>Task</th><th className="r">Avg</th><th className="r">p95</th><th className="r">Max</th><th className="r">n</th></tr></thead>
                <tbody>{stats.rows.map((r) => (
                  <tr key={r.task}><td className="mono">{r.task}</td><td className="r mono">{fmtDuration(r.avg)}</td><td className="r mono">{fmtDuration(r.p95)}</td><td className="r mono">{fmtDuration(r.max)}</td><td className="r mono muted">{r.n}</td></tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </Card>
      </div>
      <div className="small muted" style={{ marginTop: 8 }}>validate_quality includes the Ollama generation time whenever an incident is diagnosed, which is why it dominates recovered runs.</div>
    </>
  );
}
