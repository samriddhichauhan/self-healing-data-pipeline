import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ArrowDownToLine, Check, ChevronDown, ChevronRight, Copy, Download, FilterX, Pause, Play, Search, WrapText } from "lucide-react";
import type { LogEntry } from "../api/types";
import { groupEntries, markerLabel, severityOf, type Severity } from "../lib/logs";
import "../styles/logs.css";

const MARKERS = [
  { id: "OLLAMA_REQUEST", cls: "m-req" },
  { id: "OLLAMA_RESPONSE", cls: "m-res" },
  { id: "OLLAMA_DIAGNOSIS", cls: "m-diag" },
  { id: "POLICY_GATE", cls: "m-policy" },
  { id: "SELF-HEALING SUCCESS", cls: "m-heal" },
  { id: "OLLAMA_FALLBACK", cls: "m-fallback" },
  { id: "OLLAMA_ERROR_FALLBACK", cls: "m-fallback" },
  { id: "ERROR", cls: "m-error" },
] as const;
const MARKER_CLS: Record<string, string> = Object.fromEntries(MARKERS.map((m) => [m.id, m.cls]));
const SEVERITIES: Severity[] = ["INFO", "SUCCESS", "WARNING", "ERROR"];

function highlight(text: string, q: string): ReactNode {
  if (!q) return text;
  const out: ReactNode[] = [];
  const lower = text.toLowerCase();
  const needle = q.toLowerCase();
  let i = 0;
  let hit = lower.indexOf(needle);
  while (hit !== -1) {
    out.push(text.slice(i, hit), <mark key={hit}>{text.slice(hit, hit + q.length)}</mark>);
    i = hit + q.length;
    hit = lower.indexOf(needle, i);
  }
  out.push(text.slice(i));
  return out;
}

export function LogViewer({ entries, header, emptyText = "No log lines.", paused, onTogglePause, exportName = "selfheal-logs", compact = false }: {
  entries: LogEntry[]; header?: ReactNode; emptyText?: string; paused?: boolean; onTogglePause?: () => void; exportName?: string; compact?: boolean;
}) {
  const [query, setQuery] = useState("");
  const [sevs, setSevs] = useState<Severity[]>([]);
  const [markers, setMarkers] = useState<string[]>([]);
  const [component, setComponent] = useState("");
  const [wrap, setWrap] = useState(true);
  const [follow, setFollow] = useState(!compact);
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [expandAll, setExpandAll] = useState(true);
  const scroller = useRef<HTMLDivElement>(null);

  const groups = useMemo(() => groupEntries(entries), [entries]);
  const components = useMemo(() => [...new Set(entries.filter((e) => !e.continuation && e.source).map((e) => e.source!))].sort(), [entries]);

  const visible = useMemo(() => groups.filter(({ head, children }) => {
    const text = [head.text, ...children.map((c) => c.text)].join("\n").toLowerCase();
    const label = markerLabel(head);
    return (!query || text.includes(query.toLowerCase()))
      && (!sevs.length || sevs.includes(severityOf(head)))
      && (!markers.length || (label !== null && markers.includes(label)) || children.some((c) => c.marker === "POLICY_GATE" && markers.includes("POLICY_GATE")))
      && (!component || head.source === component);
  }), [groups, query, sevs, markers, component]);

  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const { head, children } of groups) {
      const l = markerLabel(head);
      if (l) c[l] = (c[l] ?? 0) + 1;
      if (l !== "POLICY_GATE" && children.some((x) => x.marker === "POLICY_GATE")) c.POLICY_GATE = (c.POLICY_GATE ?? 0) + 1;
    }
    return c;
  }, [groups]);
  const sevCounts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const { head } of groups) { const s = severityOf(head); c[s] = (c[s] ?? 0) + 1; }
    return c;
  }, [groups]);

  useEffect(() => {
    if (follow && !paused && scroller.current) scroller.current.scrollTop = scroller.current.scrollHeight;
  }, [visible, follow, paused]);

  const toggle = <T,>(list: T[], set: (v: T[]) => void, v: T) => set(list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);
  const lines = () => visible.flatMap((g) => [g.head.text, ...g.children.map((c) => c.text)]);
  const copy = async () => {
    try { await navigator.clipboard.writeText(lines().join("\n")); setCopied(true); setTimeout(() => setCopied(false), 1500); } catch { /* clipboard blocked */ }
  };
  const download = () => {
    const blob = new Blob([lines().join("\n") + "\n"], { type: "text/plain" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `${exportName.replace(/[^\w.-]+/g, "_")}.log`;
    a.click();
    URL.revokeObjectURL(a.href);
  };
  const filtered = !!(query || sevs.length || markers.length || component);
  const clear = () => { setQuery(""); setSevs([]); setMarkers([]); setComponent(""); };
  const isOpen = (n: number) => (expandAll ? !expanded.has(n) : expanded.has(n));
  const flip = (n: number) => setExpanded((s) => { const x = new Set(s); if (x.has(n)) x.delete(n); else x.add(n); return x; });

  return (
    <div className="logview">
      <div className="log-toolbar">
        {header}
        <div className="search" style={{ flex: "1 1 200px", minWidth: 160 }}>
          <Search size={14} />
          <input className="input" placeholder="Search logs…" value={query} onChange={(e) => setQuery(e.target.value)} />
        </div>
        {components.length > 1 && (
          <select className="select" style={{ width: 190 }} value={component} onChange={(e) => setComponent(e.target.value)} aria-label="Component filter">
            <option value="">All components</option>
            {components.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        )}
        <div className="row" style={{ marginLeft: "auto", gap: 6 }}>
          {onTogglePause && (
            <button className={`btn btn-sm ${paused ? "btn-primary" : ""}`} onClick={onTogglePause} title={paused ? "Resume live updates" : "Pause live updates"}>
              {paused ? <Play size={13} /> : <Pause size={13} />} <span className="hide-sm">{paused ? "Resume" : "Pause"}</span>
            </button>
          )}
          <button className="btn btn-sm btn-icon" onClick={() => { setExpandAll(!expandAll); setExpanded(new Set()); }} title={expandAll ? "Collapse all entries" : "Expand all entries"}>
            {expandAll ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </button>
          <button className={`btn btn-sm btn-icon ${wrap ? "" : "btn-ghost"}`} onClick={() => setWrap(!wrap)} title="Wrap lines"><WrapText size={14} /></button>
          <button className={`btn btn-sm ${follow ? "" : "btn-ghost"}`} onClick={() => setFollow(!follow)} title="Auto-scroll to newest"><ArrowDownToLine size={13} /> <span className="hide-sm">Auto-scroll</span></button>
          <button className="btn btn-sm btn-icon" onClick={copy} title="Copy visible lines">{copied ? <Check size={14} /> : <Copy size={14} />}</button>
          <button className="btn btn-sm btn-icon" onClick={download} title="Export visible lines (.log)"><Download size={14} /></button>
        </div>
      </div>
      <div className="log-filters">
        {SEVERITIES.map((s) => (
          <button key={s} className={`marker-chip sev-chip s-${s.toLowerCase()} ${sevs.includes(s) ? "on" : ""}`} onClick={() => toggle(sevs, setSevs, s)}>
            {s}<span className="count">{sevCounts[s] ?? 0}</span>
          </button>
        ))}
        <span style={{ width: 1, alignSelf: "stretch", background: "var(--border)", margin: "0 4px" }} />
        {MARKERS.map((m) => (
          <button key={m.id} className={`marker-chip ${m.cls} ${markers.includes(m.id) ? "on" : ""}`} onClick={() => toggle(markers, setMarkers, m.id)}>
            {m.id}<span className="count">{counts[m.id] ?? 0}</span>
          </button>
        ))}
        {filtered && <button className="btn btn-ghost btn-sm" onClick={clear}><FilterX size={13} /> Clear filters</button>}
        <span className="log-count">{visible.length.toLocaleString()} / {groups.length.toLocaleString()} entries</span>
      </div>
      <div className={`terminal ${wrap ? "wrap" : ""} ${compact ? "compact" : ""}`} ref={scroller}>
        {paused && <div className="term-paused">Live updates paused — showing a frozen snapshot.</div>}
        {!visible.length ? <div className="term-empty">{groups.length ? "No entries match the current filters." : emptyText}</div> : visible.map(({ head, children }) => {
          const label = markerLabel(head);
          const sev = severityOf(head);
          const open = isOpen(head.n);
          return (
            <div key={head.n}>
              <div className={`ln ${label ? MARKER_CLS[label] ?? "" : ""} ${children.length ? "expandable" : ""}`} onClick={() => children.length && flip(head.n)}>
                <span className="ln-exp">{children.length ? (open ? <ChevronDown size={12} /> : <ChevronRight size={12} />) : null}</span>
                <span className="ln-no">{head.n}</span>
                <span className="ln-ts" title={head.ts ?? ""}>{head.ts ?? ""}</span>
                <span className={`ln-sev ${sev.toLowerCase()}`}>{sev}</span>
                <span className="ln-msg">
                  {label && <span className="ln-tag">{label}</span>}
                  {highlight(head.message, query)}
                  {children.length > 0 && !open && <span className="ln-more">+{children.length} lines</span>}
                </span>
              </div>
              {open && children.length > 0 && (
                <div className="ln-children">
                  {children.map((c) => (
                    <div key={c.n} className={`ln-child ${c.marker === "POLICY_GATE" ? "m-policy" : ""}`}>
                      <span /><span className="ln-no">{c.n}</span><span /><span />
                      <span className="ln-msg" style={c.marker === "POLICY_GATE" ? { color: "#ddd6fe" } : undefined}>{highlight(c.text, query)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
