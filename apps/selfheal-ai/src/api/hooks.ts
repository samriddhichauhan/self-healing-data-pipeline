import { keepPreviousData, useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { useSettings } from "../lib/settings";
import { api, backend } from "./client";
import type {
  ActivityEvent, AISummary, AIStatus, Analytics, BackendRunDetail, Dag, Dashboard, DataProfile, Diagnosis, FaultType, Incident, LogFile,
  LogPayload, PipelineGraph, PlatformConfig, Run, SystemHealth, Task,
} from "./types";

/** Polling interval from settings (0 disables auto-refresh). */
function usePoll(multiplier = 1) {
  const { settings } = useSettings();
  return settings.refreshSeconds > 0 ? settings.refreshSeconds * 1000 * multiplier : false;
}

function useTestFlag() {
  return useSettings().settings.showTestData;
}

const q = (include: boolean) => (include ? "include_test=true" : "include_test=false");

export const useConfig = () =>
  useQuery({ queryKey: ["config"], queryFn: () => api<PlatformConfig>("/api/config"), staleTime: 60_000 });

export function useHealth() {
  return useQuery({ queryKey: ["health"], queryFn: () => api<SystemHealth>("/api/system/health"), refetchInterval: usePoll() });
}

export function useDashboard() {
  const t = useTestFlag();
  return useQuery({ queryKey: ["dashboard", t], queryFn: () => api<Dashboard>(`/api/dashboard?${q(t)}`), refetchInterval: usePoll(), placeholderData: keepPreviousData });
}

export function useActivity(limit = 30) {
  const t = useTestFlag();
  return useQuery({ queryKey: ["activity", t, limit], queryFn: () => api<ActivityEvent[]>(`/api/activity?limit=${limit}&${q(t)}`), refetchInterval: usePoll(), placeholderData: keepPreviousData });
}

export function useRuns(limit = 200) {
  const t = useTestFlag();
  return useQuery({ queryKey: ["runs", t, limit], queryFn: () => api<Run[]>(`/api/runs?limit=${limit}&${q(t)}`), refetchInterval: usePoll(), placeholderData: keepPreviousData });
}

/** While a run is queued/running, poll every 3 s regardless of the refresh setting. */
const ACTIVE = new Set(["running", "queued"]);
function useRunPoll() {
  const poll = usePoll();
  return (query: { state: { data?: { run?: Run } } }) => (ACTIVE.has(query.state.data?.run?.state ?? "") ? 3000 : poll);
}

export function useRun(runId?: string | null) {
  const interval = useRunPoll();
  return useQuery({
    queryKey: ["run", runId], enabled: !!runId, refetchInterval: interval,
    queryFn: () => api<{ run: Run; tasks: Task[]; incidents: Incident[] }>(`/api/runs/${encodeURIComponent(runId!)}`),
  });
}

export function useRunPipeline(runId?: string | null) {
  const interval = useRunPoll();
  return useQuery({
    queryKey: ["pipeline", runId], enabled: !!runId, refetchInterval: interval, placeholderData: keepPreviousData,
    queryFn: () => api<PipelineGraph>(`/api/runs/${encodeURIComponent(runId!)}/pipeline`),
  });
}

export function useIncidents() {
  const t = useTestFlag();
  return useQuery({ queryKey: ["incidents", t], queryFn: () => api<Incident[]>(`/api/incidents?${q(t)}`), refetchInterval: usePoll(), placeholderData: keepPreviousData });
}

export function useIncident(id?: string | null) {
  return useQuery({ queryKey: ["incident", id], enabled: !!id, queryFn: () => api<Incident>(`/api/incidents/${encodeURIComponent(id!)}`) });
}

export function useAIStatus() {
  return useQuery({ queryKey: ["ai-status"], queryFn: () => api<AIStatus>("/api/ai/status"), refetchInterval: usePoll() });
}

export function useAISummary() {
  const t = useTestFlag();
  return useQuery({ queryKey: ["ai-summary", t], queryFn: () => api<AISummary>(`/api/ai/summary?${q(t)}`), refetchInterval: usePoll(), placeholderData: keepPreviousData });
}

export function useAIDiagnoses(limit = 100) {
  const t = useTestFlag();
  return useQuery({ queryKey: ["ai-diagnoses", t, limit], queryFn: () => api<Diagnosis[]>(`/api/ai/diagnoses?limit=${limit}&${q(t)}`), refetchInterval: usePoll(), placeholderData: keepPreviousData });
}

export function useAnalytics() {
  const t = useTestFlag();
  return useQuery({ queryKey: ["analytics", t], queryFn: () => api<Analytics>(`/api/analytics?${q(t)}`), refetchInterval: usePoll(3), placeholderData: keepPreviousData });
}

export const useLogFiles = () => useQuery({ queryKey: ["log-files"], queryFn: () => api<LogFile[]>("/api/logs/files") });

export function useLogFile(name: string | null, tail: number, live: boolean) {
  const poll = usePoll();
  return useQuery({
    queryKey: ["log-file", name, tail], enabled: !!name, refetchInterval: live ? poll : false, placeholderData: keepPreviousData,
    queryFn: () => api<LogPayload>(`/api/logs/files/${encodeURIComponent(name!)}?tail=${tail}`),
  });
}

export function useTaskLog(runId: string | null, taskId: string | null, tryNumber: number, live: boolean) {
  const poll = usePoll();
  return useQuery({
    queryKey: ["task-log", runId, taskId, tryNumber], enabled: !!runId && !!taskId, refetchInterval: live ? poll : false,
    placeholderData: keepPreviousData, retry: false,
    queryFn: () => api<LogPayload>(`/api/logs/task?run_id=${encodeURIComponent(runId!)}&task_id=${encodeURIComponent(taskId!)}&try_number=${tryNumber}`),
  });
}

export const useDags = () => useQuery({ queryKey: ["dags"], queryFn: () => api<Dag[]>("/api/dags") });

export const useTriggerOptions = () =>
  useQuery({ queryKey: ["trigger-options"], queryFn: () => api<{ fault_types: FaultType[]; server_time_utc: string }>("/api/trigger/options") });

export interface TriggerInput {
  dag_id: string;
  run_id: string;
  logical_date: string;
  fault_type: string | null;
}

export function useTriggerRun() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: TriggerInput) =>
      api<{ run: any; fault_injection: any }>("/api/trigger", { method: "POST", body: JSON.stringify(body) }),
    onSuccess: () => {
      for (const k of ["runs", "dashboard", "activity"]) qc.invalidateQueries({ queryKey: [k] });
    },
  });
}

/** Several runs' task instances at once (Performance page). */
export function useRunDetails(runIds: string[]) {
  return useQueries({
    queries: runIds.map((id) => ({
      queryKey: ["run", id],
      queryFn: () => api<{ run: Run; tasks: Task[]; incidents: Incident[] }>(`/api/runs/${encodeURIComponent(id)}`),
      staleTime: 30_000,
    })),
  });
}

// ---------- existing backend, read-only ----------
export function useProfiles() {
  return useQuery({ queryKey: ["profiles"], queryFn: () => backend<DataProfile[]>("/api/v1/data/profiles"), refetchInterval: usePoll(3), retry: false });
}

export function useBackendRun(runId?: string | null) {
  return useQuery({
    queryKey: ["backend-run", runId], enabled: !!runId, retry: false,
    queryFn: () => backend<BackendRunDetail>(`/api/v1/pipeline/runs/${encodeURIComponent(runId!)}`),
  });
}
