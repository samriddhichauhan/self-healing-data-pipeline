import { createContext, useContext, useEffect, useState, type ReactNode } from "react";

// Frontend-only preferences, stored in this browser. They never change any
// pipeline, Airflow, Ollama or Docker configuration.
export type ThemePref = "light" | "dark" | "system";

export interface Settings {
  theme: ThemePref;
  refreshSeconds: number; // 0 = manual refresh only
  timeZone: "utc" | "local";
  showTestData: boolean;
  sidebarCollapsed: boolean;
  apiBaseUrl: string; // "" = same origin (/api via nginx or the Vite proxy)
  notificationsSeenAt: string | null;
}

export const DEFAULT_SETTINGS: Settings = {
  theme: "system",
  refreshSeconds: 10,
  timeZone: "utc",
  showTestData: false,
  sidebarCollapsed: false,
  apiBaseUrl: "",
  notificationsSeenAt: null,
};

export const SETTINGS_KEY = "selfheal-ai.settings";

function load(): Settings {
  try {
    const raw = window.localStorage.getItem(SETTINGS_KEY);
    return raw ? { ...DEFAULT_SETTINGS, ...JSON.parse(raw) } : DEFAULT_SETTINGS;
  } catch {
    return DEFAULT_SETTINGS;
  }
}

function save(s: Settings) {
  try {
    window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(s));
  } catch {
    /* storage unavailable (private mode) — settings stay in memory */
  }
}

let current: Settings = load();
export const getSettings = () => current;

const systemDark = () => window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
export const resolveTheme = (pref: ThemePref): "light" | "dark" => (pref === "system" ? (systemDark() ? "dark" : "light") : pref);

interface Ctx {
  settings: Settings;
  resolvedTheme: "light" | "dark";
  update: (patch: Partial<Settings>) => void;
  reset: () => void;
}
const SettingsContext = createContext<Ctx | null>(null);

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<Settings>(current);
  const [resolvedTheme, setResolved] = useState(resolveTheme(current.theme));

  useEffect(() => {
    const apply = () => {
      const t = resolveTheme(settings.theme);
      document.documentElement.dataset.theme = t;
      setResolved(t);
    };
    apply();
    const mq = window.matchMedia?.("(prefers-color-scheme: dark)");
    mq?.addEventListener("change", apply);
    return () => mq?.removeEventListener("change", apply);
  }, [settings.theme]);

  const commit = (s: Settings) => {
    current = s;
    save(s);
    setSettings(s);
  };
  return (
    <SettingsContext.Provider value={{
      settings, resolvedTheme,
      update: (patch) => commit({ ...current, ...patch }),
      reset: () => commit({ ...DEFAULT_SETTINGS, notificationsSeenAt: current.notificationsSeenAt }),
    }}>
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings(): Ctx {
  const ctx = useContext(SettingsContext);
  if (!ctx) throw new Error("useSettings must be used inside SettingsProvider");
  return ctx;
}
