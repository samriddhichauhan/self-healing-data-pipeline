import { Fragment } from "react";
import {
  BrainCircuit, CheckCircle2, Database, FileCheck2, Gauge, Loader2, ScanSearch, ShieldCheck, Wrench,
} from "lucide-react";
import type { Stage, StageStatus } from "../lib/derive";
import { fmtDuration, fmtTime } from "../lib/format";
import { StatusBadge } from "./ui";
import "../styles/flow.css";

const ICONS: Record<string, typeof Database> = {
  data: Database, schema: FileCheck2, quality: Gauge, ai: BrainCircuit, policy: ShieldCheck,
  remediation: Wrench, verification: ScanSearch, outcome: CheckCircle2,
};

export const STAGE_LABEL: Record<StageStatus, string> = {
  pending: "PENDING", running: "RUNNING", success: "SUCCESS", failed: "FAILED", skipped: "SKIPPED", recovered: "RECOVERED", warning: "ATTENTION",
};

function connectorClass(a: StageStatus, b: StageStatus) {
  if (b === "running") return "active";        // animate only into a running stage
  if (b === "failed") return "failed";
  if ((a === "success" || a === "recovered" || a === "warning") && b !== "pending" && b !== "skipped") return "done";
  return "";
}

function aiTag(s: Stage) {
  if (s.key !== "ai") return null;
  if (s.status === "success") return <span className="ai-tag">LOCAL_OLLAMA_AI</span>;
  if (s.status === "warning") return <span className="ai-tag fallback">RULE_BASED_FALLBACK</span>;
  return <span className="ai-tag idle">NOT INVOKED</span>;
}

export function PipelineFlow({ stages, selected, onSelect }: { stages: Stage[]; selected?: string | null; onSelect: (k: string) => void }) {
  return (
    <div className="flow" role="list">
      {stages.map((s, i) => {
        const Icon = s.status === "running" ? Loader2 : ICONS[s.key] ?? Database;
        const next = stages[i + 1];
        const when = s.node.started_at ?? s.node.ended_at;
        return (
          <Fragment key={s.key}>
            <button role="listitem" onClick={() => onSelect(s.key)} style={{ animationDelay: `${i * 30}ms` }}
              className={`fnode st-${s.status} ${s.key === "ai" ? "ai" : ""} ${selected === s.key ? "selected" : ""}`}>
              <span className="fnode-icon"><Icon size={16} className={s.status === "running" ? "spin" : ""} /></span>
              <span style={{ minWidth: 0 }}>
                <span className="fnode-title">{s.label} {aiTag(s)}</span>
                <span className="fnode-meta">
                  <span className="mono truncate" style={{ maxWidth: 260 }} title={s.node.task_name}>{s.node.task_name}</span>
                  <span>Last execution {fmtTime(when)}</span>
                </span>
              </span>
              <span className="fnode-right">
                <StatusBadge status={s.status === "warning" ? "warning" : s.status} label={STAGE_LABEL[s.status]} />
                <span className="fnode-dur">{s.node.duration_s !== null ? fmtDuration(s.node.duration_s) : "—"}</span>
              </span>
            </button>
            {next && (
              <div className={`fconn ${connectorClass(s.status, next.status)}`} aria-hidden>
                <svg viewBox="0 0 12 22"><path d="M6 1 V21" /></svg>
              </div>
            )}
          </Fragment>
        );
      })}
    </div>
  );
}

/** Compact horizontal progress used on the dashboard and live monitor. */
export function StageStepper({ stages }: { stages: Stage[] }) {
  return (
    <div className="stepper">
      {stages.map((s) => (
        <div key={s.key} className={`step st-${s.status}`} title={`${s.label}: ${STAGE_LABEL[s.status]}`}>
          <div className="step-bar" />
          <div className="step-label">{s.label}</div>
        </div>
      ))}
    </div>
  );
}
