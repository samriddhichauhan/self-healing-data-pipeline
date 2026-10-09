import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Search } from "lucide-react";
import { useIncidents } from "../api/hooks";
import { IncidentDrawer } from "../components/IncidentDrawer";
import { IncidentTable } from "../components/IncidentTable";
import { Card, EmptyState, ErrorState, PageHeader, SkeletonRows } from "../components/ui";
import { incidentMatches, type IncidentFilter } from "../lib/derive";
import { titleCase } from "../lib/format";

const FILTERS: { value: IncidentFilter; label: string }[] = [
  { value: "all", label: "All" }, { value: "open", label: "Open" }, { value: "resolved", label: "Resolved" },
  { value: "failed", label: "Failed" }, { value: "auto-healed", label: "Auto-Healed" }, { value: "escalated", label: "Escalated" },
];

export default function Incidents() {
  const { data, isLoading, error, refetch } = useIncidents();
  const [params, setParams] = useSearchParams();
  const openId = params.get("id");
  const [filter, setFilter] = useState<IncidentFilter>("all");
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("");

  const categories = useMemo(() => [...new Set((data ?? []).map((i) => i.fault_category).filter(Boolean) as string[])].sort(), [data]);
  const rows = useMemo(() => (data ?? []).filter((i) =>
    incidentMatches(i, filter) && (!category || i.fault_category === category) &&
    (!query || [i.incident_id, i.run_id, i.dataset, i.root_cause, i.fault_category].some((v) => v?.toLowerCase().includes(query.toLowerCase())))
  ), [data, filter, category, query]);

  return (
    <>
      <PageHeader title="Incidents" description="Data-quality incidents detected by the pipeline, with AI diagnosis, policy decision and recovery outcome." />
      <Card flush>
        <div className="row-wrap" style={{ padding: "2px 16px 12px", gap: 6 }}>
          {FILTERS.map((f) => (
            <button key={f.value} className={`chip ${filter === f.value ? "on" : ""}`} onClick={() => setFilter(f.value)}>
              {f.label} <span className="n">{(data ?? []).filter((i) => incidentMatches(i, f.value)).length}</span>
            </button>
          ))}
          <div style={{ flex: 1 }} />
          <select className="select" style={{ width: 200 }} value={category} onChange={(e) => setCategory(e.target.value)} aria-label="Fault type">
            <option value="">All fault types</option>
            {categories.map((c) => <option key={c} value={c}>{titleCase(c)}</option>)}
          </select>
          <div className="search" style={{ width: 260 }}>
            <Search size={14} />
            <input className="input" placeholder="Search ID, run, dataset, cause…" value={query} onChange={(e) => setQuery(e.target.value)} />
          </div>
        </div>
        {error ? <div style={{ padding: "0 16px 14px" }}><ErrorState error={error} onRetry={refetch} /></div>
          : isLoading ? <SkeletonRows rows={10} />
          : !rows.length ? <EmptyState title={data?.length ? "No incidents match these filters" : "No incidents recorded"} />
          : <IncidentTable incidents={rows} selected={openId} onSelect={(i) => setParams({ id: i.incident_id })} />}
      </Card>
      <div className="small muted" style={{ marginTop: 8 }}>
        Open = not yet remediated or escalated · Resolved = remediated and recorded · Auto-healed = AUTO_FIX with verification PASSED · Failed = remediation or verification failed.
      </div>
      {openId && <IncidentDrawer id={openId} onClose={() => setParams({})} />}
    </>
  );
}
