import { lazy, Suspense } from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AppShell } from "./components/shell/AppShell";
import { LoadingState } from "./components/ui";
import { SettingsProvider } from "./lib/settings";
import { ToastProvider } from "./lib/toast";
import Dashboard from "./pages/Dashboard";

const PipelineRuns = lazy(() => import("./pages/PipelineRuns"));
const LiveMonitoring = lazy(() => import("./pages/LiveMonitoring"));
const PipelineFlowPage = lazy(() => import("./pages/PipelineFlowPage"));
const Incidents = lazy(() => import("./pages/Incidents"));
const SelfHealing = lazy(() => import("./pages/SelfHealing"));
const DataQuality = lazy(() => import("./pages/DataQuality"));
const Logs = lazy(() => import("./pages/Logs"));
const AIDiagnosis = lazy(() => import("./pages/AIDiagnosis"));
const Ollama = lazy(() => import("./pages/Ollama"));
const Analytics = lazy(() => import("./pages/Analytics"));
const Performance = lazy(() => import("./pages/Performance"));
const FaultInjection = lazy(() => import("./pages/FaultInjection"));
const SystemHealth = lazy(() => import("./pages/SystemHealth"));
const Settings = lazy(() => import("./pages/Settings"));

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: true, staleTime: 2_000 } },
});

export default function App() {
  return (
    <SettingsProvider>
      <QueryClientProvider client={queryClient}>
        <ToastProvider>
          <BrowserRouter>
            <Routes>
              <Route element={<AppShell />}>
                <Route index element={<Dashboard />} />
                {[
                  ["runs", PipelineRuns], ["live", LiveMonitoring], ["flow", PipelineFlowPage], ["incidents", Incidents],
                  ["self-healing", SelfHealing], ["data-quality", DataQuality], ["logs", Logs], ["ai", AIDiagnosis],
                  ["ollama", Ollama], ["analytics", Analytics], ["performance", Performance], ["fault-injection", FaultInjection],
                  ["health", SystemHealth], ["settings", Settings],
                ].map(([path, Page]) => {
                  const C = Page as React.ComponentType;
                  return <Route key={path as string} path={path as string} element={<Suspense fallback={<LoadingState label="Loading page…" height={320} />}><C /></Suspense>} />;
                })}
                {/* v1 paths */}
                <Route path="pipeline" element={<Navigate to="/flow" replace />} />
                <Route path="executions" element={<Navigate to="/runs" replace />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Route>
            </Routes>
          </BrowserRouter>
        </ToastProvider>
      </QueryClientProvider>
    </SettingsProvider>
  );
}
