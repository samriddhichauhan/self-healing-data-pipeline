import { useEffect, useState } from "react";

/** Current time, re-rendering every `ms` while `active`. */
export function useNow(active: boolean, ms = 1000) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!active) return;
    const id = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(id);
  }, [active, ms]);
  return now;
}

/** Airflow task logs use "+0000" offsets; make them ISO-8601 parseable. */
export const isoTs = (v?: string | null) => (v ? v.replace(/([+-]\d{2})(\d{2})$/, "$1:$2") : null);
