import { useCallback, useEffect, useRef, useState } from "react";

interface UseApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

/**
 * Fetch data from an async function. Automatically refetches
 * when deps change. Supports polling via interval.
 *
 * Request sequencing: every invocation carries a monotonically
 * increasing generation id. When a fetch resolves, we only commit its
 * result to state if the generation still matches the latest one the
 * hook kicked off. Without this, rapid dep changes (e.g. a status
 * filter in the NotificationsCenter page) could let an older fetch
 * arrive last and stomp on a newer one, showing stale rows.
 */
export function useApi<T>(
  fetcher: () => Promise<T>,
  deps: unknown[] = [],
  options?: { pollInterval?: number },
): UseApiResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const mountedRef = useRef(true);
  const dataRef = useRef<T | null>(null);
  const fetchGenRef = useRef(0);

  const fetchData = useCallback(async () => {
    const gen = ++fetchGenRef.current;
    try {
      setLoading((prev) => (dataRef.current === null ? true : prev));
      const result = await fetcher();
      if (!mountedRef.current || gen !== fetchGenRef.current) return;
      dataRef.current = result;
      setData(result);
      setError(null);
    } catch (err) {
      if (!mountedRef.current || gen !== fetchGenRef.current) return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (mountedRef.current && gen === fetchGenRef.current) setLoading(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    mountedRef.current = true;
    fetchData();
    return () => {
      mountedRef.current = false;
    };
  }, [fetchData]);

  useEffect(() => {
    if (!options?.pollInterval) return;
    const id = setInterval(fetchData, options.pollInterval);
    return () => clearInterval(id);
  }, [fetchData, options?.pollInterval]);

  return { data, loading, error, refetch: fetchData };
}
