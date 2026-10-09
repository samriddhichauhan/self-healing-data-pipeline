import { NavLink } from "react-router-dom";
import { PanelLeftClose, PanelLeftOpen, ShieldCheck } from "lucide-react";
import { useHealth, useIncidents, useRuns } from "../../api/hooks";
import { useSettings } from "../../lib/settings";
import { isActive } from "../../lib/derive";
import { Dot } from "../ui";
import { NAV } from "./nav";

function useBadges() {
  const runs = useRuns(50);
  const incidents = useIncidents();
  const live = runs.data?.some((r) => isActive(r)) ?? false;
  const since = Date.now() - 86_400_000;
  const escalated24h = (incidents.data ?? []).filter((i) => i.status === "ESCALATED" && i.created_at && new Date(i.created_at).getTime() > since).length;
  return { live, escalated24h };
}

export function Sidebar({ open }: { open: boolean }) {
  const { settings, update } = useSettings();
  const collapsed = settings.sidebarCollapsed;
  const health = useHealth();
  const { live, escalated24h } = useBadges();
  const comp = (k: string) => health.data?.components.find((c) => c.key === k);
  const airflow = comp("airflow")?.status === "connected" ? comp("scheduler")?.status : comp("airflow")?.status;
  const ollama = comp("ollama")?.status === "connected" ? comp("model")?.status : comp("ollama")?.status;
  const label = (s?: string, on = "Connected") => (!s ? "Checking…" : s === "connected" ? on : s === "degraded" ? "Degraded" : "Offline");
  const tone = (s?: string) => (s === "connected" ? "success" : s === "degraded" ? "warning" : s ? "danger" : "neutral") as "success";

  return (
    <aside className={`sidebar ${collapsed ? "collapsed" : ""} ${open ? "open" : ""}`} aria-label="Main navigation">
      <div className="brand">
        <div className="brand-mark"><ShieldCheck size={17} /></div>
        <div className="brand-text">
          <div className="brand-name">SelfHeal <b>AI</b></div>
          <div className="brand-sub">AI Data Reliability Platform</div>
        </div>
      </div>
      <nav className="nav">
        {NAV.map((g) => (
          <div key={g.section}>
            <div className="nav-section">{g.section}</div>
            {g.items.map(({ to, label: text, icon: Icon, end }) => {
              const badge = to === "/live" && live ? { cls: "live", text: "LIVE" }
                : to === "/incidents" && escalated24h ? { cls: "danger", text: String(escalated24h) } : null;
              return (
                <NavLink key={to} to={to} end={end} title={collapsed ? text : undefined}
                  className={({ isActive: a }) => `nav-item ${a ? "active" : ""}`}>
                  <Icon size={16} />
                  <span className="nav-label">{text}</span>
                  {badge && <span className={`nav-badge ${badge.cls}`} title={to === "/incidents" ? "Escalated in the last 24 h" : "A DAG run is in progress"}>{badge.text}</span>}
                  {badge && <span className="nav-badge-dot" style={badge.cls === "live" ? { background: "var(--info-dot)" } : undefined} />}
                </NavLink>
              );
            })}
          </div>
        ))}
      </nav>
      <div className="side-foot">
        <div className="foot-row" title={`Airflow: ${label(airflow)}`}>
          <Dot tone={tone(airflow)} pulse={airflow === "connected"} />
          <span className="foot-text">Airflow <strong>{label(airflow)}</strong></span>
        </div>
        <div className="foot-row" title={`Ollama: ${label(ollama, "Online")}`}>
          <Dot tone={tone(ollama)} pulse={ollama === "connected"} />
          <span className="foot-text">Ollama <strong>{label(ollama, "Online")}</strong></span>
        </div>
        <div className="foot-row between" style={{ marginTop: 2 }}>
          <span className="foot-text faint mono" style={{ fontSize: 11 }}>v1.0</span>
          <button className="btn btn-ghost btn-sm btn-icon collapse-btn" onClick={() => update({ sidebarCollapsed: !collapsed })}
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"} aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}>
            {collapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
          </button>
        </div>
      </div>
    </aside>
  );
}
