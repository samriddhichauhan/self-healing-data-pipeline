import { getSettings } from "../lib/settings";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(url, { headers: { "Content-Type": "application/json" }, ...init });
  } catch (e) {
    throw new ApiError(0, `API unreachable (${(e as Error).message})`);
  }
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail ?? body);
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, detail);
  }
  return res.json() as Promise<T>;
}

const base = () => getSettings().apiBaseUrl.replace(/\/$/, "");

/** SelfHeal platform API (/api/*). No mock or sample data path exists. */
export const api = <T>(path: string, init?: RequestInit) => request<T>(`${base()}${path}`, init);

/** Existing pipeline backend, read-only, via the UI server's GET-only /backend/ proxy. */
export const backend = <T>(path: string) => request<T>(`${base()}/backend${path}`);
