import { useMemo, type ReactElement, type ReactNode } from "react";
import { ResponsiveContainer } from "recharts";
import { useSettings } from "../lib/settings";
import { Card, EmptyState } from "./ui";

/** Chart palette read from the active theme's CSS tokens. */
export function useChartTheme() {
  const { resolvedTheme } = useSettings();
  return useMemo(() => {
    const cs = getComputedStyle(document.documentElement);
    const v = (n: string) => cs.getPropertyValue(n).trim();
    const t = {
      grid: v("--chart-grid"), axis: v("--chart-axis"), c1: v("--chart-1"), c2: v("--chart-2"), c3: v("--chart-3"),
      ok: v("--chart-ok"), bad: v("--chart-bad"), warn: v("--chart-warn"), muted: v("--chart-muted"),
      text: v("--text"), surface: v("--bg-elevated"), border: v("--border"), cursor: v("--hover"),
    };
    return {
      ...t,
      axis: { stroke: t.axis, fontSize: 11, tickLine: false, axisLine: { stroke: t.grid } } as const,
      tooltip: {
        contentStyle: { background: t.surface, borderRadius: 8, border: `1px solid ${t.border}`, boxShadow: "0 10px 28px rgba(0,0,0,.18)", fontSize: 12, color: t.text },
        labelStyle: { fontWeight: 650, color: t.text },
        itemStyle: { color: t.text },
      },
      legend: { iconType: "circle" as const, wrapperStyle: { fontSize: 11.5, color: t.axis } },
    };
  }, [resolvedTheme]);
}

export function ChartCard({ title, subtitle, source, empty, emptyText, height = 250, children, className, actions }: {
  title: string; subtitle?: ReactNode; source?: string; empty: boolean; emptyText?: string; height?: number;
  children: ReactElement; className?: string; actions?: ReactNode;
}) {
  return (
    <Card title={title} subtitle={subtitle} className={className} actions={actions} footer={source ? <>Source · {source}</> : undefined}>
      {empty ? (
        <div style={{ height, display: "grid", placeItems: "center" }}>
          <EmptyState title="No data available">{emptyText ?? "Nothing has been recorded for this metric yet."}</EmptyState>
        </div>
      ) : (
        <div style={{ height }}>
          <ResponsiveContainer width="100%" height="100%">{children}</ResponsiveContainer>
        </div>
      )}
    </Card>
  );
}
