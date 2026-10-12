import type { CSSProperties, ReactNode } from "react";
import { AlertTriangle, ArrowDownRight, ArrowUpRight, Inbox, Loader2, Minus, RefreshCw } from "lucide-react";

export type Tone = "success" | "warning" | "danger" | "info" | "neutral" | "ai" | "violet";

/** One status vocabulary for every source: Airflow task/run states, pipeline
 *  statuses, incident statuses, health states and pipeline-stage states. */
const TONES: Record<string, Tone> = {
  success: "success", healthy: "success", recovered: "success", remediated: "success", connected: "success", online: "success",
  passed: "success", pass: "success", auto_fix: "success", low: "success", auto_healed: "success", resolved: "success",
  running: "info", queued: "info", scheduled: "info", restarting: "info", pending_approval: "info", deferred: "info", open: "info",
  warning: "warning", degraded: "warning", escalated: "warning", escalate: "warning", up_for_retry: "warning",
  up_for_reschedule: "warning", medium: "warning", fallback: "warning", rule_based: "warning", rule_based_fallback: "warning",
  failed: "danger", offline: "danger", upstream_failed: "danger", high: "danger", critical: "danger", error: "danger", fail: "danger",
  conflict: "danger", rolled_back: "danger", rollback_failed: "danger",
  not_run: "neutral", skipped: "neutral",
  local_ollama_ai: "ai",
};

export function toneOf(status?: string | null): Tone {
  return TONES[(status ?? "").toLowerCase()] ?? "neutral";
}

const LABELS: Record<string, string> = {
  connected: "Online", upstream_failed: "Upstream failed", up_for_retry: "Up for retry", auto_fix: "AUTO_FIX",
  local_ollama_ai: "LOCAL_OLLAMA_AI", rule_based: "RULE_BASED", rule_based_fallback: "RULE_BASED_FALLBACK", escalate: "ESCALATE",
  not_run: "SKIPPED",
};

export function StatusBadge({ status, label, pulse }: { status?: string | null; label?: string; pulse?: boolean }) {
  if (!status) return <span className="badge neutral">—</span>;
  const tone = toneOf(status);
  const text = label ?? LABELS[status.toLowerCase()] ?? status.replace(/_/g, " ");
  return (
    <span className={`badge ${tone}`}>
      {tone !== "ai" && <span className={`dot ${pulse || tone === "info" ? "pulse" : ""}`} />}
      {text}
    </span>
  );
}

export function Badge({ tone = "neutral", children, plain = false, title }: { tone?: Tone | "outline"; children: ReactNode; plain?: boolean; title?: string }) {
  return <span className={`badge ${tone} ${plain ? "plain" : ""}`} title={title}>{children}</span>;
}

export function Dot({ tone, pulse }: { tone: Tone; pulse?: boolean }) {
  return <span className={`dot ${tone === "ai" ? "info" : tone} ${pulse ? "pulse" : ""}`} />;
}

export function Card({ title, subtitle, actions, children, footer, className = "", flush = false, style, bodyStyle }: {
  title?: ReactNode; subtitle?: ReactNode; actions?: ReactNode; children?: ReactNode; footer?: ReactNode;
  className?: string; flush?: boolean; style?: CSSProperties; bodyStyle?: CSSProperties;
}) {
  return (
    <section className={`card ${className}`} style={style}>
      {(title || actions) && (
        <div className="card-head">
          <div style={{ minWidth: 0 }}>
            {title && <div className="card-title">{title}</div>}
            {subtitle && <div className="card-sub">{subtitle}</div>}
          </div>
          {actions && <div className="row" style={{ flex: "none" }}>{actions}</div>}
        </div>
      )}
      <div className={`card-body ${flush ? "flush" : ""}`} style={bodyStyle}>{children}</div>
      {footer && <div className="card-foot">{footer}</div>}
    </section>
  );
}

export interface TrendInfo { delta: number; label: string; goodWhenUp?: boolean }

export function MetricCard({ label, value, unit, icon, tone = "info", foot, trend, status, loading }: {
  label: string; value: ReactNode; unit?: string; icon: ReactNode; tone?: Tone; foot?: ReactNode;
  trend?: TrendInfo | null; status?: ReactNode; loading?: boolean;
}) {
  const na = value === null || value === undefined || value === "N/A";
  const TrendIcon = trend ? (trend.delta > 0 ? ArrowUpRight : trend.delta < 0 ? ArrowDownRight : Minus) : null;
  const good = trend ? (trend.delta === 0 ? "flat" : (trend.delta > 0) === (trend.goodWhenUp ?? true) ? "good" : "bad") : "flat";
  return (
    <section className="card metric">
      <div className="metric-head">
        <span className="metric-label">{label}</span>
        <span className={`metric-icon ${tone === "info" || tone === "ai" ? "" : tone}`}>{icon}</span>
      </div>
      {loading ? <Skeleton height={28} width="55%" style={{ marginTop: 4 }} /> : (
        <div className={`metric-value ${na ? "na" : ""}`}>{na ? "N/A" : value}{!na && unit && <span className="unit">{unit}</span>}</div>
      )}
      <div className="metric-foot">
        {trend && TrendIcon && <span className={`trend ${good}`}><TrendIcon size={13} />{trend.delta > 0 ? "+" : ""}{trend.delta}</span>}
        {trend && <span>{trend.label}</span>}
        {!trend && foot}
        {status && <span style={{ marginLeft: "auto" }}>{status}</span>}
      </div>
    </section>
  );
}

export function PageHeader({ title, description, actions, eyebrow }: { title: string; description?: ReactNode; actions?: ReactNode; eyebrow?: string }) {
  return (
    <div className="page-head">
      <div style={{ minWidth: 0 }}>
        {eyebrow && <div className="small strong" style={{ color: "var(--primary)", letterSpacing: "0.06em", textTransform: "uppercase", fontSize: 11, marginBottom: 3 }}>{eyebrow}</div>}
        <h1>{title}</h1>
        {description && <p>{description}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </div>
  );
}

export function EmptyState({ title = "No data available", children, icon }: { title?: string; children?: ReactNode; icon?: ReactNode }) {
  return (
    <div className="empty">
      <div className="empty-icon">{icon ?? <Inbox size={18} />}</div>
      <strong>{title}</strong>
      {children && <div className="small" style={{ maxWidth: 420 }}>{children}</div>}
    </div>
  );
}

export function LoadingState({ label = "Loading…", height = 160 }: { label?: string; height?: number }) {
  return (
    <div className="empty" style={{ minHeight: height }}>
      <Loader2 size={18} className="spin" />
      <span className="small">{label}</span>
    </div>
  );
}

export function ErrorState({ error, onRetry, title }: { error: unknown; onRetry?: () => void; title?: string }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="alert danger" role="alert">
      <AlertTriangle size={15} />
      <div className="alert-body">{title && <strong>{title}: </strong>}{msg}</div>
      {onRetry && <button className="btn btn-sm" onClick={onRetry}><RefreshCw size={12} /> Retry</button>}
    </div>
  );
}

export function Skeleton({ height = 14, width = "100%", style }: { height?: number; width?: number | string; style?: CSSProperties }) {
  return <div className="skeleton" style={{ height, width, ...style }} />;
}

export function SkeletonRows({ rows = 5 }: { rows?: number }) {
  return (
    <div className="stack" style={{ gap: 10, padding: "6px 16px 14px" }}>
      {Array.from({ length: rows }).map((_, i) => <Skeleton key={i} height={16} width={`${94 - i * 7}%`} />)}
    </div>
  );
}

export function KV({ items }: { items: [ReactNode, ReactNode][] }) {
  return (
    <dl className="kv">
      {items.map(([k, v], i) => (
        <div key={i} style={{ display: "contents" }}>
          <dt>{k}</dt>
          <dd>{v === null || v === undefined || v === "" ? <span className="faint">N/A</span> : v}</dd>
        </div>
      ))}
    </dl>
  );
}

export function Switch({ checked, onChange, label }: { checked: boolean; onChange: (v: boolean) => void; label: ReactNode }) {
  return (
    <label className="switch">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      <span className="track" />
      {label}
    </label>
  );
}

export function Segmented<T extends string | number>({ value, options, onChange }: {
  value: T; options: { value: T; label: ReactNode }[]; onChange: (v: T) => void;
}) {
  return (
    <div className="segmented" role="tablist">
      {options.map((o) => (
        <button key={String(o.value)} role="tab" aria-selected={o.value === value} className={o.value === value ? "on" : ""} onClick={() => onChange(o.value)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Tabs<T extends string>({ value, tabs, onChange }: { value: T; tabs: { value: T; label: ReactNode }[]; onChange: (v: T) => void }) {
  return (
    <div className="tabs" role="tablist">
      {tabs.map((t) => (
        <button key={t.value} role="tab" aria-selected={t.value === value} className={t.value === value ? "on" : ""} onClick={() => onChange(t.value)}>{t.label}</button>
      ))}
    </div>
  );
}

export function ConfidenceMeter({ value, threshold = 0.85, compact = false }: { value?: number | null; threshold?: number; compact?: boolean }) {
  if (value === null || value === undefined) return <span className="faint">N/A</span>;
  const below = value < threshold;
  return (
    <div className="row" style={{ gap: 8, minWidth: compact ? 100 : 150 }}>
      <div className="meter" style={{ flex: 1 }} title={`Policy Gate threshold ${threshold}`}>
        <span style={{ width: `${Math.max(0, Math.min(1, value)) * 100}%`, background: below ? "var(--warning-dot)" : undefined }} />
        <i className="threshold" style={{ left: `${threshold * 100}%` }} />
      </div>
      <span className="mono strong">{value.toFixed(2)}</span>
    </div>
  );
}

export function SourceBadge({ source }: { source?: string | null }) {
  return source === "LOCAL_OLLAMA_AI"
    ? <Badge tone="ai">Ollama AI</Badge>
    : <Badge tone="warning">Rule-based</Badge>;
}
