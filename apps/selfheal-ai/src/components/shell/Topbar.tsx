import { useEffect, useRef, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle, Bell, BrainCircuit, CheckCircle2, ChevronRight, CircleDot, ExternalLink, Laptop, Menu, Moon,
  RefreshCw, Settings as SettingsIcon, Sun, User, XCircle,
} from "lucide-react";
import { useActivity, useConfig } from "../../api/hooks";
import { fmtRelative } from "../../lib/format";
import { useSettings, type ThemePref } from "../../lib/settings";
import { GlobalSearch } from "./GlobalSearch";
import type { NavItem } from "./nav";

function usePopover() {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => ref.current && !ref.current.contains(e.target as Node) && setOpen(false);
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);
  return { open, setOpen, ref };
}

function Popover({ button, children, width = 240 }: { button: (toggle: () => void) => ReactNode; children: (close: () => void) => ReactNode; width?: number }) {
  const { open, setOpen, ref } = usePopover();
  return (
    <div ref={ref} style={{ position: "relative" }}>
      {button(() => setOpen(!open))}
      {open && <div className="popover" style={{ width }}>{children(() => setOpen(false))}</div>}
    </div>
  );
}

const THEMES: { value: ThemePref; label: string; icon: typeof Sun }[] = [
  { value: "light", label: "Light", icon: Sun }, { value: "dark", label: "Dark", icon: Moon }, { value: "system", label: "System", icon: Laptop },
];

export function ThemeToggle() {
  const { settings, resolvedTheme, update } = useSettings();
  const Icon = resolvedTheme === "dark" ? Moon : Sun;
  return (
    <Popover width={180} button={(t) => <button className="btn btn-icon" onClick={t} title="Theme" aria-label="Theme"><Icon size={15} /></button>}>
      {(close) => (
        <div style={{ padding: "4px 0" }}>
          {THEMES.map(({ value, label, icon: I }) => (
            <button key={value} className={`menu-item ${settings.theme === value ? "on" : ""}`} onClick={() => { update({ theme: value }); close(); }}>
              <I size={14} /> {label}{value === "system" && <span className="faint small" style={{ marginLeft: "auto" }}>{resolvedTheme}</span>}
            </button>
          ))}
        </div>
      )}
    </Popover>
  );
}

const SEV_ICON = { error: XCircle, warning: AlertTriangle, success: CheckCircle2, ai: BrainCircuit, info: CircleDot };
const SEV_CLS = { error: "text-danger", warning: "text-warning", success: "text-success", ai: "text-info", info: "text-neutral" };

/** Notifications are the real activity feed; unread = newer than last opened. */
function Notifications() {
  const { settings, update } = useSettings();
  const activity = useActivity(25);
  const items = (activity.data ?? []).filter((e) => e.severity !== "info");
  const seen = settings.notificationsSeenAt ? new Date(settings.notificationsSeenAt).getTime() : 0;
  const unread = items.filter((e) => new Date(e.ts).getTime() > seen).length;
  return (
    <Popover width={360} button={(t) => (
      <button className="btn btn-icon icon-btn" onClick={() => { t(); }} title="Notifications" aria-label="Notifications">
        <Bell size={15} />{unread > 0 && <span className="count">{unread > 9 ? "9+" : unread}</span>}
      </button>
    )}>
      {(close) => (
        <div>
          <div className="menu-head row between">
            <strong style={{ fontSize: 13 }}>Notifications</strong>
            <button className="btn btn-ghost btn-sm" onClick={() => update({ notificationsSeenAt: new Date().toISOString() })}>Mark all read</button>
          </div>
          <div style={{ maxHeight: 380, overflowY: "auto" }}>
            {!items.length ? <div className="empty small">No pipeline events yet.</div> : items.map((e, i) => {
              const Icon = SEV_ICON[e.severity] ?? CircleDot;
              const href = e.ref.incident_id ? `/incidents?id=${encodeURIComponent(e.ref.incident_id)}` : e.ref.run_id ? `/flow?run=${encodeURIComponent(e.ref.run_id)}` : "/";
              const isNew = new Date(e.ts).getTime() > seen;
              return (
                <Link key={`${e.ts}-${i}`} to={href} className="menu-item" onClick={close} style={{ alignItems: "flex-start", background: isNew ? "var(--primary-soft)" : undefined }}>
                  <Icon size={14} className={SEV_CLS[e.severity]} style={{ marginTop: 2, flex: "none" }} />
                  <span style={{ minWidth: 0, flex: 1 }}>
                    <span style={{ display: "block", fontWeight: 600, color: "var(--text)" }}>{e.title}</span>
                    <span className="small muted truncate" style={{ display: "block" }}>{e.detail}</span>
                  </span>
                  <span className="small faint nowrap">{fmtRelative(e.ts)}</span>
                </Link>
              );
            })}
          </div>
        </div>
      )}
    </Popover>
  );
}

function ProfileMenu() {
  const config = useConfig();
  return (
    <Popover width={250} button={(t) => (
      <button className="btn btn-icon" onClick={t} title="Session" aria-label="Session menu" style={{ borderRadius: "50%" }}><User size={15} /></button>
    )}>
      {(close) => (
        <div>
          <div className="menu-head">
            <div className="strong" style={{ fontSize: 13 }}>Local session</div>
            <div className="small muted">No authentication is configured for this platform.</div>
          </div>
          <div style={{ padding: "4px 0" }}>
            <Link to="/settings" className="menu-item" onClick={close}><SettingsIcon size={14} /> Settings</Link>
            {config.data && <a className="menu-item" href={config.data.airflow_public_url} target="_blank" rel="noreferrer" onClick={close}><ExternalLink size={14} /> Airflow UI</a>}
            <a className="menu-item" href="http://localhost:8001/docs" target="_blank" rel="noreferrer" onClick={close}><ExternalLink size={14} /> Platform API docs</a>
            <a className="menu-item" href="http://localhost:8000/docs" target="_blank" rel="noreferrer" onClick={close}><ExternalLink size={14} /> Pipeline API docs</a>
          </div>
        </div>
      )}
    </Popover>
  );
}

function LiveIndicator() {
  const { settings } = useSettings();
  const fetching = useIsFetching();
  const on = settings.refreshSeconds > 0;
  return (
    <Link to="/settings" className="badge plain hide-md" style={{ height: 26, padding: "0 9px", textDecoration: "none", background: on ? "var(--success-soft)" : "var(--neutral-soft)", color: on ? "var(--success)" : "var(--neutral)", borderColor: on ? "var(--success-line)" : "var(--neutral-line)" }}
      title={on ? `Auto-refresh every ${settings.refreshSeconds}s` : "Auto-refresh off"}>
      <span className={`dot ${on ? "pulse" : ""}`} /> {on ? (fetching ? "Syncing" : "Live") : "Paused"}
    </Link>
  );
}

export function Topbar({ item, onMenu }: { item: NavItem; onMenu: () => void }) {
  const qc = useQueryClient();
  const fetching = useIsFetching();
  return (
    <header className="topbar">
      <button className="btn btn-ghost btn-icon menu-btn" onClick={onMenu} aria-label="Open navigation"><Menu size={17} /></button>
      <div className="crumbs">
        <div className="crumb-path">SelfHeal AI <ChevronRight size={11} /> {item.section}</div>
        <div className="crumb-title">{item.label}</div>
      </div>
      <div className="top-spacer" />
      <GlobalSearch />
      <LiveIndicator />
      <button className="btn btn-icon" title="Refresh all data" aria-label="Refresh" onClick={() => qc.invalidateQueries()}>
        <RefreshCw size={15} className={fetching ? "spin" : ""} />
      </button>
      <Notifications />
      <ThemeToggle />
      <ProfileMenu />
    </header>
  );
}
