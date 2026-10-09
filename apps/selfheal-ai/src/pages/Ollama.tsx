import { CartesianGrid, Line, LineChart, ReferenceLine, Tooltip, XAxis, YAxis } from "recharts";
import { Activity, Cpu, LifeBuoy, Plug, Timer } from "lucide-react";
import { useAIDiagnoses, useAIStatus, useAISummary, useAnalytics } from "../api/hooks";
import { ChartCard, useChartTheme } from "../components/charts";
import { Badge, Card, EmptyState, MetricCard, PageHeader, SkeletonRows, StatusBadge } from "../components/ui";
import { fmtDateTime } from "../lib/format";

export default function Ollama() {
  const status = useAIStatus();
  const summary = useAISummary();
  const diagnoses = useAIDiagnoses(300);
  const analytics = useAnalytics();
  const t = useChartTheme();
  const s = status.data;
  const fallbacks = (diagnoses.data ?? []).filter((d) => d.fallback);
  const series = (analytics.data?.ai_response_times ?? []).map((r) => ({ ...r, label: fmtDateTime(r.ts, false).replace(/ UTC$/, "") }));
  const avg = series.length ? series.reduce((a, b) => a + b.response_time_s, 0) / series.length : null;

  return (
    <>
      <PageHeader title="Ollama" description="The local LLM server used for incident diagnosis. Status is checked live through /api/tags." />
      <div className="grid g-4">
        <MetricCard label="Server" tone={s?.connected ? "success" : "danger"} icon={<Plug size={14} />} loading={status.isLoading}
          value={s ? <StatusBadge status={s.connected ? "connected" : "offline"} /> : null} foot={s ? <span className="mono truncate">{s.host}</span> : undefined} />
        <MetricCard label="Availability check" icon={<Activity size={14} />} loading={status.isLoading} value={s?.connected ? s.latency_ms : null} unit="ms" foot="GET /api/tags round trip" />
        <MetricCard label="Pipeline model" tone={s?.model_installed ? "violet" : "danger"} icon={<Cpu size={14} />} loading={status.isLoading}
          value={s ? <span className="mono" style={{ fontSize: 16 }}>{s.model}</span> : null} foot={s ? (s.model_installed ? "Installed" : "Not installed") : undefined} />
        <MetricCard label="Generation time" tone="warning" icon={<Timer size={14} />} loading={summary.isLoading}
          value={summary.data?.avg_response_time_s != null ? summary.data.avg_response_time_s.toFixed(1) : null} unit="s"
          foot={summary.data?.p95_response_time_s != null ? `avg · p95 ${summary.data.p95_response_time_s.toFixed(1)} s` : "No measured responses"} />
      </div>

      <div className="grid g-main mt">
        <ChartCard title="Response time per diagnosis" subtitle={avg !== null ? `Average ${avg.toFixed(1)} s across ${series.length} pipeline calls` : undefined}
          source="[OLLAMA_RESPONSE] events in logs/ollama_ai_agent.log" empty={!series.length} emptyText="No Ollama responses have been recorded for pipeline incidents.">
          <LineChart data={series} margin={{ top: 8, right: 12, left: -14, bottom: 0 }}>
            <CartesianGrid stroke={t.grid} vertical={false} />
            <XAxis dataKey="label" {...t.axis} minTickGap={28} />
            <YAxis {...t.axis} unit="s" />
            <Tooltip {...t.tooltip} formatter={(v: any) => [`${Number(v).toFixed(2)} s`, "Response time"]} labelFormatter={(_, p: any) => p?.[0]?.payload?.incident_id ?? ""} />
            {avg !== null && <ReferenceLine y={avg} stroke={t.muted} strokeDasharray="4 4" />}
            <Line isAnimationActive={false} type="linear" dataKey="response_time_s" stroke={t.c1} strokeWidth={2} dot={{ r: 3.5, fill: t.surface, strokeWidth: 2 }} />
          </LineChart>
        </ChartCard>
        <Card title="Call outcomes" subtitle="AI calls made for pipeline incidents">
          {summary.isLoading ? <SkeletonRows rows={4} /> : !summary.data ? null : (
            <div className="stack" style={{ gap: 12 }}>
              {[["Ollama diagnoses", summary.data.ollama_diagnoses, "var(--primary)"], ["Rule-based fallbacks", summary.data.fallbacks, "var(--warning-dot)"]].map(([k, v, c]) => (
                <div key={k as string}>
                  <div className="row between small"><span>{k}</span><span className="mono strong">{v as number}</span></div>
                  <div className="progress" style={{ marginTop: 5 }}><span style={{ width: `${summary.data!.total_calls ? ((v as number) / summary.data!.total_calls) * 100 : 0}%`, background: c as string }} /></div>
                </div>
              ))}
              <div className="small muted">{summary.data.total_calls} calls in total. Fallbacks happen when Ollama is unreachable, times out, or returns invalid JSON.</div>
            </div>
          )}
        </Card>
      </div>

      <div className="grid g-2 mt">
        <Card flush title="Installed models" subtitle={s ? `Reported by ${s.host}/api/tags` : undefined}>
          {!s?.models.length ? <EmptyState>{s?.connected === false ? "Ollama is offline." : "No models reported."}</EmptyState> : (
            <div className="table-wrap">
              <table className="table">
                <thead><tr><th>Model</th><th>Parameters</th><th>Quantization</th><th className="r">Size</th><th /></tr></thead>
                <tbody>{s.models.map((m) => (
                  <tr key={m.name}><td className="mono">{m.name}</td><td>{m.parameter_size ?? "N/A"}</td><td>{m.quantization ?? "N/A"}</td>
                    <td className="r mono">{m.size ? `${(m.size / 1e9).toFixed(2)} GB` : "N/A"}</td><td>{m.name === s.model && <Badge tone="ai" plain>Pipeline model</Badge>}</td></tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </Card>
        <Card flush title={<><LifeBuoy size={14} /> Fallback history</>} subtitle="Pipeline incidents diagnosed by the rule-based engine instead of Ollama">
          {diagnoses.isLoading ? <SkeletonRows rows={5} /> : !fallbacks.length ? <EmptyState>No fallbacks recorded.</EmptyState> : (
            <div className="table-wrap" style={{ maxHeight: 320 }}>
              <table className="table">
                <thead><tr><th>Time</th><th>Incident</th><th>Reason</th></tr></thead>
                <tbody>{fallbacks.map((d, i) => (
                  <tr key={`${d.incident_id}-${i}`}><td className="nowrap muted">{fmtDateTime(d.last_ts, false)}</td><td className="mono">{d.incident_id}</td><td className="small">{d.fallback_reason}</td></tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </Card>
      </div>
    </>
  );
}
