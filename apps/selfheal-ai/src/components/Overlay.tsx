import { useEffect, type ReactNode } from "react";
import { AlertTriangle, Loader2, X } from "lucide-react";

function useEscape(onClose: () => void) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    document.addEventListener("keydown", onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prev;
    };
  }, [onClose]);
}

export function Drawer({ title, subtitle, onClose, children, actions }: {
  title: ReactNode; subtitle?: ReactNode; onClose: () => void; children: ReactNode; actions?: ReactNode;
}) {
  useEscape(onClose);
  return (
    <>
      <div className="overlay" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-modal="true">
        <div className="drawer-head">
          <div style={{ minWidth: 0 }}>
            <h3 style={{ fontSize: 15 }} className="truncate">{title}</h3>
            {subtitle && <div className="small muted" style={{ marginTop: 3 }}>{subtitle}</div>}
          </div>
          <div className="row" style={{ flex: "none" }}>
            {actions}
            <button className="btn btn-ghost btn-icon btn-sm" onClick={onClose} aria-label="Close"><X size={15} /></button>
          </div>
        </div>
        <div className="drawer-body">{children}</div>
      </aside>
    </>
  );
}

export function Modal({ title, subtitle, onClose, children, footer, wide }: {
  title: ReactNode; subtitle?: ReactNode; onClose: () => void; children: ReactNode; footer?: ReactNode; wide?: boolean;
}) {
  useEscape(onClose);
  return (
    <>
      <div className="overlay" />
      <div className="modal-wrap" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
        <div className="modal" role="dialog" aria-modal="true" style={wide ? { width: "min(680px, 100%)" } : undefined}>
          <div className="modal-head">
            <div>
              <h3 style={{ fontSize: 16 }}>{title}</h3>
              {subtitle && <div className="small muted" style={{ marginTop: 3 }}>{subtitle}</div>}
            </div>
            <button className="btn btn-ghost btn-icon btn-sm" onClick={onClose} aria-label="Close"><X size={15} /></button>
          </div>
          <div className="modal-body">{children}</div>
          {footer && <div className="modal-foot">{footer}</div>}
        </div>
      </div>
    </>
  );
}

export function ConfirmationModal({ title, children, confirmLabel, onConfirm, onClose, busy, danger, error }: {
  title: string; children: ReactNode; confirmLabel: string; onConfirm: () => void; onClose: () => void;
  busy?: boolean; danger?: boolean; error?: string | null;
}) {
  return (
    <Modal title={title} onClose={busy ? () => {} : onClose}
      footer={<>
        <button className="btn" onClick={onClose} disabled={busy}>Cancel</button>
        <button className={`btn ${danger ? "btn-danger" : "btn-primary"}`} onClick={onConfirm} disabled={busy}>
          {busy && <Loader2 size={14} className="spin" />} {confirmLabel}
        </button>
      </>}>
      {children}
      {error && <div className="alert danger"><AlertTriangle size={15} /><div className="alert-body">{error}</div></div>}
    </Modal>
  );
}
