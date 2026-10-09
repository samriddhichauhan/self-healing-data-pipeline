import { getSettings } from "./settings";

const toDate = (v?: string | null): Date | null => {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
};

function parts() {
  const utc = getSettings().timeZone === "utc";
  const opts: Intl.DateTimeFormatOptions = { timeZone: utc ? "UTC" : undefined, hour12: false };
  return { utc, opts };
}

export function fmtDateTime(v?: string | null, withSeconds = true): string {
  const d = toDate(v);
  if (!d) return "—";
  const { utc, opts } = parts();
  const s = d.toLocaleString("en-GB", {
    ...opts, year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit",
    ...(withSeconds ? { second: "2-digit" } : {}),
  });
  return utc ? `${s} UTC` : s;
}

export function fmtTime(v?: string | null): string {
  const d = toDate(v);
  if (!d) return "—";
  const { opts } = parts();
  return d.toLocaleTimeString("en-GB", { ...opts, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function fmtDate(v?: string | null): string {
  const d = toDate(v);
  if (!d) return "—";
  const { opts } = parts();
  return d.toLocaleDateString("en-GB", { ...opts, year: "numeric", month: "short", day: "2-digit" });
}

export function fmtRelative(v?: string | null): string {
  const d = toDate(v);
  if (!d) return "—";
  const s = Math.round((Date.now() - d.getTime()) / 1000);
  if (s < 0) return "in the future";
  if (s < 45) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 36) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

export function fmtDuration(seconds?: number | null): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) return "—";
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  const total = Math.round(seconds);
  const m = Math.floor(total / 60);
  const s = total % 60;
  if (m < 60) return `${m}m ${s}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

export const fmtNum = (v?: number | null) => (v === null || v === undefined ? "—" : v.toLocaleString("en-US"));
export const fmtConf = (v?: number | null) => (v === null || v === undefined ? "—" : v.toFixed(2));
export const titleCase = (s?: string | null) =>
  !s ? "—" : s.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());

/** "2026-09-27T06:30:00Z" for the trigger form, from date + time inputs (UTC). */
export function utcIso(date: string, time: string) {
  return `${date}T${time.length === 5 ? `${time}:00` : time}Z`;
}
