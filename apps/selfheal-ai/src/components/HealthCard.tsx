import type { ReactNode } from "react";
import type { HealthStatus } from "../api/types";
import { Card, KV } from "./ui";

export const HEALTH_LABEL: Record<HealthStatus, string> = { connected: "Online", degraded: "Degraded", offline: "Offline" };
const TONE: Record<HealthStatus, string> = { connected: "success", degraded: "warning", offline: "danger" };

export function HealthCard({ name, status, icon, detail, rows, delay = 0 }: {
  name: string; status: HealthStatus | null; icon: ReactNode; detail?: ReactNode; rows: [ReactNode, ReactNode][]; delay?: number;
}) {
  const tone = status ? TONE[status] : "neutral";
  return (
    <Card style={{ animationDelay: `${delay}ms` }}>
      <div className="row between" style={{ marginBottom: 10 }}>
        <div className="row" style={{ gap: 10, minWidth: 0 }}>
          <span className={`banner-icon tone-${tone}`}>{icon}</span>
          <div style={{ minWidth: 0 }}>
            <div className="strong">{name}</div>
            {detail && <div className="small muted truncate">{detail}</div>}
          </div>
        </div>
        <span className={`badge ${tone}`}><span className={`dot ${status === "connected" ? "pulse" : ""}`} /> {status ? HEALTH_LABEL[status] : "Unknown"}</span>
      </div>
      <KV items={rows} />
    </Card>
  );
}
