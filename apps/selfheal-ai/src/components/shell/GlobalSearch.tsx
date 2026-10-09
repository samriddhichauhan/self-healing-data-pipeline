import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ListChecks, Search, Siren } from "lucide-react";
import { useIncidents, useRuns } from "../../api/hooks";
import { ALL_NAV } from "./nav";

interface Hit { group: string; label: string; sub?: string; to: string; icon: typeof Search }

/** Searches pages plus the runs and incidents already loaded from the API. */
export function GlobalSearch() {
  const [q, setQ] = useState("");
  const [open, setOpen] = useState(false);
  const [idx, setIdx] = useState(0);
  const ref = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const runs = useRuns(200);
  const incidents = useIncidents();

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") { e.preventDefault(); ref.current?.focus(); setOpen(true); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const hits = useMemo<Hit[]>(() => {
    const s = q.trim().toLowerCase();
    const pages = ALL_NAV.filter((n) => !s || `${n.label} ${n.keywords ?? ""}`.toLowerCase().includes(s))
      .map((n) => ({ group: "Pages", label: n.label, sub: n.section, to: n.to, icon: n.icon }));
    if (!s) return pages.slice(0, 8);
    const r = (runs.data ?? []).filter((x) => x.run_id.toLowerCase().includes(s)).slice(0, 6)
      .map((x) => ({ group: "Pipeline runs", label: x.run_id, sub: x.pipeline_status ?? x.state ?? "", to: `/flow?run=${encodeURIComponent(x.run_id)}`, icon: ListChecks }));
    const i = (incidents.data ?? []).filter((x) => `${x.incident_id} ${x.fault_category} ${x.run_id ?? ""}`.toLowerCase().includes(s)).slice(0, 6)
      .map((x) => ({ group: "Incidents", label: x.incident_id, sub: `${x.fault_category} · ${x.status}`, to: `/incidents?id=${encodeURIComponent(x.incident_id)}`, icon: Siren }));
    return [...pages.slice(0, 5), ...r, ...i];
  }, [q, runs.data, incidents.data]);

  const go = (h?: Hit) => {
    if (!h) return;
    navigate(h.to);
    setOpen(false);
    setQ("");
    ref.current?.blur();
  };

  let lastGroup = "";
  return (
    <div className="gsearch hide-sm">
      <div className="search">
        <Search size={14} />
        <input ref={ref} className="input" placeholder="Search pages, runs, incidents…" value={q}
          onChange={(e) => { setQ(e.target.value); setIdx(0); setOpen(true); }}
          onFocus={() => setOpen(true)} onBlur={() => setTimeout(() => setOpen(false), 150)}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") { e.preventDefault(); setIdx((i) => Math.min(i + 1, hits.length - 1)); }
            else if (e.key === "ArrowUp") { e.preventDefault(); setIdx((i) => Math.max(i - 1, 0)); }
            else if (e.key === "Enter") go(hits[idx]);
            else if (e.key === "Escape") { setOpen(false); ref.current?.blur(); }
          }}
          aria-label="Global search" />
      </div>
      {!open && <span className="kbd">Ctrl K</span>}
      {open && (
        <div className="popover" style={{ left: 0, right: "auto" }}>
          {!hits.length ? <div className="gs-item muted">No matches in pages, runs or incidents.</div> : hits.map((h, i) => {
            const header = h.group !== lastGroup ? <div className="gs-group">{h.group}</div> : null;
            lastGroup = h.group;
            const Icon = h.icon;
            return (
              <div key={`${h.group}-${h.label}`}>
                {header}
                <div className={`gs-item ${i === idx ? "active" : ""}`} onMouseDown={() => go(h)} onMouseEnter={() => setIdx(i)}>
                  <Icon size={14} /> <span className={h.group === "Pages" ? "" : "mono"} style={{ fontSize: h.group === "Pages" ? undefined : 12 }}>{h.label}</span>
                  {h.sub && <span className="sub">{h.sub}</span>}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
