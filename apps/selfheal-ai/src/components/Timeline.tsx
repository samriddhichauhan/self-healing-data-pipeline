import type { ReactNode } from "react";
import { fmtTime } from "../lib/format";
import { EmptyState } from "./ui";

export interface TimelineItem { ts: string | null; title: ReactNode; meta?: ReactNode; color: string }

export const TL_COLOR = {
  info: "var(--info-dot)", success: "var(--success-dot)", warning: "var(--warning-dot)", danger: "var(--danger-dot)",
  ai: "var(--primary)", policy: "var(--violet)", neutral: "var(--neutral-dot)",
};

/** Chronological event list. Items without a real timestamp are dropped. */
export function Timeline({ items, empty = "No events recorded yet.", newestFirst = false }: { items: TimelineItem[]; empty?: string; newestFirst?: boolean }) {
  const sorted = items.filter((i) => i.ts).sort((a, b) => (new Date(a.ts!).getTime() - new Date(b.ts!).getTime()) * (newestFirst ? -1 : 1));
  if (!sorted.length) return <EmptyState>{empty}</EmptyState>;
  return (
    <ul className="tl">
      {sorted.map((it, i) => (
        <li key={i} style={{ animationDelay: `${Math.min(i, 12) * 20}ms` }}>
          <span className="tl-time">{fmtTime(it.ts)}</span>
          <span className="tl-rail"><span className="tl-dot" style={{ background: it.color }} /></span>
          <div style={{ minWidth: 0 }}>
            <div className="tl-title">{it.title}</div>
            {it.meta && <div className="tl-meta">{it.meta}</div>}
          </div>
        </li>
      ))}
    </ul>
  );
}
