import { createContext, useCallback, useContext, useState, type ReactNode } from "react";
import { AlertTriangle, CheckCircle2, Info, X, XCircle } from "lucide-react";

type Kind = "success" | "error" | "warning" | "info";
interface Toast { id: number; kind: Kind; title: string; message?: string }

const ToastCtx = createContext<(t: Omit<Toast, "id">) => void>(() => {});
export const useToast = () => useContext(ToastCtx);

const ICON = { success: CheckCircle2, error: XCircle, warning: AlertTriangle, info: Info };
const TONE = { success: "text-success", error: "text-danger", warning: "text-warning", info: "text-info" };

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const dismiss = (id: number) => setToasts((ts) => ts.filter((t) => t.id !== id));
  const push = useCallback((t: Omit<Toast, "id">) => {
    const id = Date.now() + Math.random();
    setToasts((ts) => [...ts.slice(-3), { ...t, id }]);
    setTimeout(() => dismiss(id), t.kind === "error" ? 9000 : 5000);
  }, []);
  return (
    <ToastCtx.Provider value={push}>
      {children}
      <div className="toasts" role="status" aria-live="polite">
        {toasts.map((t) => {
          const Icon = ICON[t.kind];
          return (
            <div key={t.id} className={`toast ${t.kind}`}>
              <Icon size={16} className={TONE[t.kind]} style={{ flex: "none", marginTop: 1 }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div className="toast-title">{t.title}</div>
                {t.message && <div className="toast-msg">{t.message}</div>}
              </div>
              <button className="btn btn-ghost btn-sm btn-icon" onClick={() => dismiss(t.id)} aria-label="Dismiss"><X size={13} /></button>
            </div>
          );
        })}
      </div>
    </ToastCtx.Provider>
  );
}
