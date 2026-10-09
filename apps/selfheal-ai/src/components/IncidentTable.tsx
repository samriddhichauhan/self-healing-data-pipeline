import type { Incident } from "../api/types";
import { incidentResolvedAt } from "../lib/derive";
import { fmtConf, fmtDateTime, titleCase } from "../lib/format";
import { Badge, SourceBadge, StatusBadge } from "./ui";

export function IncidentTable({ incidents, selected, onSelect, compact = false }: {
  incidents: Incident[]; selected?: string | null; onSelect: (i: Incident) => void; compact?: boolean;
}) {
  return (
    <div className="table-wrap" style={compact ? undefined : { maxHeight: "66vh" }}>
      <table className="table">
        <thead>
          <tr>
            <th>Incident ID</th><th>Fault type</th>{!compact && <th className="hide-md">Pipeline</th>}<th>Status</th>
            <th>AI diagnosis</th><th className="r">Confidence</th>{!compact && <th>Action</th>}{!compact && <th>Created</th>}{!compact && <th className="hide-md">Resolved</th>}
          </tr>
        </thead>
        <tbody>
          {incidents.map((i) => {
            const resolved = incidentResolvedAt(i);
            return (
              <tr key={i.incident_id} className={`clickable ${selected === i.incident_id ? "selected" : ""}`} onClick={() => onSelect(i)}>
                <td className="mono nowrap">{i.incident_id} {i.is_test && <Badge tone="outline" plain>test</Badge>}</td>
                <td className="nowrap">{titleCase(i.fault_category)}</td>
                {!compact && <td className="mono muted hide-md truncate" style={{ maxWidth: 230 }} title={i.run_id ?? ""}>{i.run_id ?? "N/A"}</td>}
                <td><StatusBadge status={i.status} /></td>
                <td><SourceBadge source={i.diagnosis_source} /></td>
                <td className="r mono">{i.diagnosis_source === "LOCAL_OLLAMA_AI" ? fmtConf(i.ai_confidence) : <span className="faint" title="Rule-based diagnoses have no AI confidence">N/A</span>}</td>
                {!compact && <td>{i.policy_decision ? <StatusBadge status={i.policy_decision} /> : <span className="faint">N/A</span>}</td>}
                {!compact && <td className="nowrap muted">{fmtDateTime(i.created_at, false)}</td>}
                {!compact && <td className="nowrap muted hide-md">{resolved ? fmtDateTime(resolved, false) : "—"}</td>}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
