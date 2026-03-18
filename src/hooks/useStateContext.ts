/**
 * Shared hook for fetching universal state context.
 * Caches responses in module-level Map to avoid re-fetching.
 */
import { useState, useEffect } from "react";
import { getAuthHeaders, API_BASE } from "../lib/api";
import type { StateContextData } from "../types";

const _cache = new Map<string, { data: StateContextData; ts: number }>();
const CACHE_TTL = 600_000; // 10 minutes (client-side)

export function useStateContext(stateCode: string | null | undefined): {
  data: StateContextData | null;
  loading: boolean;
} {
  const [data, setData] = useState<StateContextData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!stateCode) { setData(null); return; }

    const sc = stateCode.toUpperCase();
    const cached = _cache.get(sc);
    if (cached && Date.now() - cached.ts < CACHE_TTL) {
      setData(cached.data);
      return;
    }

    setLoading(true);
    (async () => {
      try {
        const hdrs = await getAuthHeaders();
        const res = await fetch(`${API_BASE}/api/state-context/${sc}`, { headers: hdrs });
        if (res.ok) {
          const d = await res.json() as StateContextData;
          _cache.set(sc, { data: d, ts: Date.now() });
          setData(d);
        }
      } catch { /* graceful null */ }
      setLoading(false);
    })();
  }, [stateCode]);

  return { data, loading };
}
