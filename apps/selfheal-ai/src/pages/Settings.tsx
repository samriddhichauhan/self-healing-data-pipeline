import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Info, Laptop, Moon, RotateCcw, Sun } from "lucide-react";
import { useConfig } from "../api/hooks";
import { Card, KV, PageHeader, Segmented, Switch } from "../components/ui";
import { DEFAULT_SETTINGS, useSettings } from "../lib/settings";
import { useToast } from "../lib/toast";

export default function Settings() {
  const { settings, resolvedTheme, update, reset } = useSettings();
  const config = useConfig();
  const qc = useQueryClient();
  const toast = useToast();
  const [apiBase, setApiBase] = useState(settings.apiBaseUrl);

  return (
    <>
      <PageHeader title="Settings" description="Preferences for this browser only. Pipeline, Airflow, Ollama and Docker configuration cannot be changed here."
        actions={<button className="btn" onClick={() => { reset(); setApiBase(DEFAULT_SETTINGS.apiBaseUrl); qc.invalidateQueries(); toast({ kind: "info", title: "Settings reset to defaults" }); }}><RotateCcw size={14} /> Reset to defaults</button>} />
      <div className="grid g-2">
        <Card title="Appearance">
          <div className="stack">
            <div className="field">
              <label>Theme</label>
              <Segmented value={settings.theme} onChange={(v) => update({ theme: v })} options={[
                { value: "light", label: <><Sun size={13} /> Light</> }, { value: "dark", label: <><Moon size={13} /> Dark</> }, { value: "system", label: <><Laptop size={13} /> System</> },
              ]} />
              <span className="hint">Currently {resolvedTheme}. “System” follows your operating system and updates automatically.</span>
            </div>
            <div className="field">
              <label>Sidebar</label>
              <Switch checked={settings.sidebarCollapsed} onChange={(v) => update({ sidebarCollapsed: v })} label="Collapse to icons" />
            </div>
            <div className="field">
              <label>Time zone</label>
              <Segmented value={settings.timeZone} onChange={(v) => update({ timeZone: v })} options={[{ value: "utc", label: "UTC (Airflow)" }, { value: "local", label: "Browser local" }]} />
              <span className="hint">Airflow logical dates are UTC; UTC keeps timestamps aligned with the DAG.</span>
            </div>
          </div>
        </Card>
        <Card title="Data & refresh">
          <div className="stack">
            <div className="field">
              <label>Auto-refresh</label>
              <Segmented value={settings.refreshSeconds} onChange={(v) => update({ refreshSeconds: v })} options={[0, 5, 10, 30, 60].map((s) => ({ value: s, label: s ? `${s}s` : "Off" }))} />
              <span className="hint">Active DAG runs always refresh every 3 s.</span>
            </div>
            <div className="field">
              <label>Test data</label>
              <Switch checked={settings.showTestData} onChange={(v) => update({ showTestData: v })} label="Include pytest, CLI and manual test records" />
              <span className="hint">Off: metrics count only real DAG runs and their incidents and AI calls.</span>
            </div>
            <div className="field">
              <label htmlFor="api-base">API base URL</label>
              <div className="row">
                <input id="api-base" className="input mono" placeholder="(same origin: /api and /backend)" value={apiBase} onChange={(e) => setApiBase(e.target.value)} />
                <button className="btn btn-primary" disabled={apiBase.trim() === settings.apiBaseUrl} onClick={() => { update({ apiBaseUrl: apiBase.trim() }); qc.invalidateQueries(); }}>Apply</button>
              </div>
              <span className="hint">Leave empty to use this server's built-in proxies.</span>
            </div>
          </div>
        </Card>
      </div>
      <Card className="mt" title="Connected services" subtitle="Read-only · from the platform API environment">
        <KV items={[
          ["DAG", config.data ? <span className="mono">{config.data.dag_id}</span> : null],
          ["Ollama model", config.data ? <span className="mono">{config.data.ollama_model}</span> : null],
          ["Ollama host", config.data ? <span className="mono">{config.data.ollama_host}</span> : null],
          ["Airflow UI", config.data ? <a href={config.data.airflow_public_url} target="_blank" rel="noreferrer">{config.data.airflow_public_url}</a> : null],
          ["Pipeline API", config.data ? <span className="mono">{config.data.backend_url}</span> : null],
          ["Version", "SelfHeal AI v1.0"],
        ]} />
        <div className="alert info" style={{ marginTop: 12 }}><Info size={14} /><div className="alert-body">Infrastructure settings live in <code>docker-compose.yml</code>, <code>docker-compose.platform.yml</code> and <code>.env</code>, and are intentionally not editable from the UI.</div></div>
      </Card>
    </>
  );
}
