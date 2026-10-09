import type { LogEntry } from "../api/types";

export type Severity = "INFO" | "SUCCESS" | "WARNING" | "ERROR";

/** Severity shown in the viewer, derived from the parsed level and marker. */
export function severityOf(e: LogEntry): Severity {
  const lvl = (e.level ?? "").toUpperCase();
  if (lvl === "ERROR" || lvl === "CRITICAL" || e.marker === "ERROR") return "ERROR";
  if (lvl === "WARNING" || e.marker === "FALLBACK") return "WARNING";
  if (e.marker === "SELF-HEALING SUCCESS" || /Marking task as SUCCESS|validations passed|verified|Successfully staged/i.test(e.message)) return "SUCCESS";
  return "INFO";
}

/** Exact marker label, distinguishing the two fallback kinds. */
export function markerLabel(e: LogEntry): string | null {
  if (!e.marker) return null;
  if (e.marker === "FALLBACK") return e.message.includes("OLLAMA_ERROR_FALLBACK") ? "OLLAMA_ERROR_FALLBACK" : "OLLAMA_FALLBACK";
  return e.marker;
}

export interface LogGroup { head: LogEntry; children: LogEntry[] }

/** Groups continuation lines (e.g. "  - key: value") under their header line. */
export function groupEntries(entries: LogEntry[]): LogGroup[] {
  const groups: LogGroup[] = [];
  for (const e of entries) {
    if (e.continuation && groups.length) groups[groups.length - 1].children.push(e);
    else groups.push({ head: e, children: [] });
  }
  return groups;
}
