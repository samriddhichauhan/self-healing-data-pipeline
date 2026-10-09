import {
  Activity, BarChart3, BrainCircuit, Cpu, FlaskConical, Gauge, GitBranch, HeartPulse, LayoutDashboard,
  ListChecks, Radio, ScrollText, Settings, ShieldCheck, Siren, Table2,
} from "lucide-react";

export interface NavItem { to: string; label: string; icon: typeof Activity; section: string; end?: boolean; keywords?: string }

export const NAV: { section: string; items: NavItem[] }[] = [
  { section: "Overview", items: [
    { to: "/", label: "Dashboard", icon: LayoutDashboard, section: "Overview", end: true, keywords: "home kpi overview" },
  ] },
  { section: "Pipelines", items: [
    { to: "/runs", label: "Pipeline Runs", icon: ListChecks, section: "Pipelines", keywords: "airflow dag runs trigger executions" },
    { to: "/live", label: "Live Monitoring", icon: Radio, section: "Pipelines", keywords: "live running current task" },
    { to: "/flow", label: "Pipeline Flow", icon: GitBranch, section: "Pipelines", keywords: "stages flow graph" },
  ] },
  { section: "Operations", items: [
    { to: "/incidents", label: "Incidents", icon: Siren, section: "Operations", keywords: "faults alerts" },
    { to: "/self-healing", label: "Self-Healing", icon: ShieldCheck, section: "Operations", keywords: "remediation recovery auto-fix" },
    { to: "/data-quality", label: "Data Quality", icon: Table2, section: "Operations", keywords: "datasets profiles nulls duplicates schema" },
    { to: "/logs", label: "Logs", icon: ScrollText, section: "Operations", keywords: "task logs terminal" },
  ] },
  { section: "AI", items: [
    { to: "/ai", label: "AI Diagnostics", icon: BrainCircuit, section: "AI", keywords: "diagnosis root cause confidence" },
    { to: "/ollama", label: "Ollama", icon: Cpu, section: "AI", keywords: "model llm llama" },
  ] },
  { section: "Analytics", items: [
    { to: "/analytics", label: "Analytics", icon: BarChart3, section: "Analytics", keywords: "charts trends" },
    { to: "/performance", label: "Performance", icon: Gauge, section: "Analytics", keywords: "duration latency tasks" },
  ] },
  { section: "Tools", items: [
    { to: "/fault-injection", label: "Fault Injection", icon: FlaskConical, section: "Tools", keywords: "simulate test chaos" },
  ] },
  { section: "System", items: [
    { to: "/health", label: "System Health", icon: HeartPulse, section: "System", keywords: "status uptime" },
    { to: "/settings", label: "Settings", icon: Settings, section: "System", keywords: "preferences theme" },
  ] },
];

export const ALL_NAV = NAV.flatMap((g) => g.items);
export const findNav = (pathname: string) =>
  ALL_NAV.find((n) => (n.end ? pathname === n.to : pathname === n.to || pathname.startsWith(`${n.to}/`))) ?? ALL_NAV[0];
