import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, Pie, PieChart, ReferenceLine, Tooltip, XAxis, YAxis } from "recharts";
import { useAnalytics, useRuns } from "../api/hooks";
import { ChartCard, useChartTheme } from "../components/charts";
import { ErrorState, PageHeader, Skeleton } from "../components/ui";
import { fmtDateTime, titleCase } from "../lib/format";

const shortTs = (v: string) => fmtDateTime(v, false).replace(/ UTC$/, "");

export default function Analytics() {
  const { data, isLoading, error, refetch } = useAnalytics();
  const runs = useRuns(300);
  const t = useChartTheme();
  if (error) return <><PageHeader title="Analytics" /><ErrorState error={error} onRetry={refetch} /></>;
  if (isLoading || !data) return <><PageHeader title="Analytics" /><div className="grid g-2">{Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} height={300} style={{ borderRadius: 10 }} />)}</div></>;

  const states = Object.entries(data.run_states).map(([name, value]) => ({ name, value }));
  const stateColor: Record<string, string> = { success: t.ok, failed: t.bad, running: t.c1, queued: t.c3 };
  const fixSplit = [{ name: "Auto-fixed", value: data.autofix_vs_escalation.auto_fix }, { name: "Escalated", value: data.autofix_vs_escalation.escalated }].filter((d) => d.value > 0);
  const confidence = data.ai_confidence.map((d) => ({ ...d, label: shortTs(d.ts) }));
  const responses = data.ai_response_times.map((d) => ({ ...d, label: shortTs(d.ts) }));
  const avgResp = responses.length ? responses.reduce((s, d) => s + d.response_time_s, 0) / responses.length : null;
  const recovery = data.recovery_durations.map((d) => ({ ...d, label: d.incident_id }));
  const byType = data.incidents_by_type.map((d) => ({ ...d, name: titleCase(d.fault_category) }));
  const durations = (runs.data ?? []).filter((r) => r.duration_s !== null && r.start_date).slice().reverse()
    .map((r) => ({ label: shortTs(r.start_date!), run: r.run_id, seconds: r.duration_s, status: r.pipeline_status ?? r.state }));
  const tooltipLabel = (key: string) => (_: unknown, p: any) => p?.[0]?.payload?.[key] ?? "";

  return (
    <>
      <PageHeader title="Analytics" description="Every chart is computed from recorded pipeline runs, incidents and AI events. Metrics without records show an empty state." />
      <div className="grid g-2">
        <ChartCard title="Pipeline success vs failure" subtitle="Airflow DAG run states" source={data.sources.executions} empty={!states.length}>
          <PieChart>
            <Pie isAnimationActive={false} data={states} dataKey="value" nameKey="name" innerRadius="58%" outerRadius="82%" paddingAngle={2} stroke="none">
              {states.map((d) => <Cell key={d.name} fill={stateColor[d.name] ?? t.muted} />)}
            </Pie>
            <Tooltip {...t.tooltip} /><Legend {...t.legend} verticalAlign="bottom" />
          </PieChart>
        </ChartCard>

        <ChartCard title="Self-healed incidents over time" subtitle="Resolved incidents per day" source={data.sources.incidents} empty={!data.autofix_vs_escalation.by_day.length}>
          <BarChart data={data.autofix_vs_escalation.by_day} margin={{ top: 8, right: 8, left: -18, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} vertical={false} />
            <XAxis dataKey="date" {...t.axis} /><YAxis allowDecimals={false} {...t.axis} />
            <Tooltip {...t.tooltip} cursor={{ fill: t.cursor }} /><Legend {...t.legend} />
            <Bar isAnimationActive={false} dataKey="auto_fix" name="Auto-healed" stackId="a" fill={t.ok} maxBarSize={44} />
            <Bar isAnimationActive={false} dataKey="escalated" name="Escalated" stackId="a" fill={t.warn} radius={[3, 3, 0, 0]} maxBarSize={44} />
          </BarChart>
        </ChartCard>

        <ChartCard title="AI confidence over time" subtitle="Ollama confidence per diagnosed incident · dashed line = Policy Gate threshold 0.85" source={data.sources.ai_confidence} empty={!confidence.length}>
          <LineChart data={confidence} margin={{ top: 8, right: 12, left: -18, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} vertical={false} />
            <XAxis dataKey="label" {...t.axis} minTickGap={28} /><YAxis domain={[0.5, 1]} {...t.axis} />
            <Tooltip {...t.tooltip} formatter={(v: any) => [Number(v).toFixed(2), "Confidence"]} labelFormatter={tooltipLabel("incident_id")} />
            <ReferenceLine y={0.85} stroke={t.c2} strokeDasharray="5 4" />
            <Line isAnimationActive={false} type="linear" dataKey="confidence" stroke={t.c1} strokeWidth={2} dot={{ r: 4, fill: t.surface, strokeWidth: 2 }} />
          </LineChart>
        </ChartCard>

        <ChartCard title="Ollama response time" subtitle={avgResp !== null ? `Seconds per call · average ${avgResp.toFixed(1)} s` : "Seconds per call"} source={data.sources.ai_response_times} empty={!responses.length}>
          <LineChart data={responses} margin={{ top: 8, right: 12, left: -14, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} vertical={false} />
            <XAxis dataKey="label" {...t.axis} minTickGap={28} /><YAxis {...t.axis} unit="s" />
            <Tooltip {...t.tooltip} formatter={(v: any) => [`${Number(v).toFixed(2)} s`, "Response time"]} labelFormatter={tooltipLabel("incident_id")} />
            {avgResp !== null && <ReferenceLine y={avgResp} stroke={t.muted} strokeDasharray="4 4" />}
            <Line isAnimationActive={false} type="linear" dataKey="response_time_s" stroke={t.c2} strokeWidth={2} dot={{ r: 3.5, fill: t.surface, strokeWidth: 2 }} />
          </LineChart>
        </ChartCard>

        <ChartCard title="Incidents by fault category" subtitle="How each category was resolved" source={data.sources.incidents} empty={!byType.length}>
          <BarChart data={byType} layout="vertical" margin={{ top: 4, right: 12, left: 20, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} horizontal={false} />
            <XAxis type="number" allowDecimals={false} {...t.axis} /><YAxis type="category" dataKey="name" width={140} {...t.axis} />
            <Tooltip {...t.tooltip} cursor={{ fill: t.cursor }} /><Legend {...t.legend} />
            <Bar isAnimationActive={false} dataKey="remediated" name="Auto-healed" stackId="a" fill={t.ok} />
            <Bar isAnimationActive={false} dataKey="escalated" name="Escalated" stackId="a" fill={t.warn} />
            <Bar isAnimationActive={false} dataKey="other" name="Other" stackId="a" fill={t.muted} radius={[0, 3, 3, 0]} />
          </BarChart>
        </ChartCard>

        <ChartCard title="Auto-fix vs escalation" subtitle="All resolved incidents" source={data.sources.incidents} empty={!fixSplit.length}>
          <PieChart>
            <Pie isAnimationActive={false} data={fixSplit} dataKey="value" nameKey="name" innerRadius="58%" outerRadius="82%" paddingAngle={2} stroke="none">
              {fixSplit.map((d) => <Cell key={d.name} fill={d.name === "Auto-fixed" ? t.ok : t.warn} />)}
            </Pie>
            <Tooltip {...t.tooltip} /><Legend {...t.legend} verticalAlign="bottom" />
          </PieChart>
        </ChartCard>

        <ChartCard title="Pipeline execution duration" subtitle="Wall-clock duration of each DAG run" source="Airflow REST API (dagRuns)" empty={!durations.length}>
          <BarChart data={durations} margin={{ top: 8, right: 8, left: -14, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} vertical={false} />
            <XAxis dataKey="label" {...t.axis} minTickGap={28} /><YAxis {...t.axis} unit="s" />
            <Tooltip {...t.tooltip} cursor={{ fill: t.cursor }} formatter={(v: any, _n: any, p: any) => [`${Number(v).toFixed(1)} s`, p.payload.status]} labelFormatter={tooltipLabel("run")} />
            <Bar isAnimationActive={false} dataKey="seconds" radius={[3, 3, 0, 0]} maxBarSize={30}>
              {durations.map((d) => <Cell key={d.run} fill={d.status === "RECOVERED" ? t.c1 : d.status === "FAILED" || d.status === "failed" ? t.bad : t.ok} />)}
            </Bar>
          </BarChart>
        </ChartCard>

        <ChartCard title="Recovery time" subtitle="Detection → verified fix, per auto-healed incident (blue = diagnosed by Ollama)" source={data.sources.recovery} empty={!recovery.length}>
          <BarChart data={recovery} margin={{ top: 8, right: 8, left: -14, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} vertical={false} />
            <XAxis dataKey="label" {...t.axis} tick={false} /><YAxis {...t.axis} unit="s" />
            <Tooltip {...t.tooltip} cursor={{ fill: t.cursor }} formatter={(v: any, _n: any, p: any) => [`${Number(v).toFixed(1)} s`, p.payload.diagnosis_source === "LOCAL_OLLAMA_AI" ? "Ollama diagnosis" : "Rule-based diagnosis"]} />
            <Bar isAnimationActive={false} dataKey="seconds" radius={[3, 3, 0, 0]} maxBarSize={32}>
              {recovery.map((r) => <Cell key={r.incident_id} fill={r.diagnosis_source === "LOCAL_OLLAMA_AI" ? t.c1 : t.muted} />)}
            </Bar>
          </BarChart>
        </ChartCard>
      </div>
    </>
  );
}
