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

  const fetchData = useCallback(async () => {
    try {
      setLoading((prev) => (dataRef.current === null ? true : prev));
      const result = await fetcher();
      if (mountedRef.current) {
        dataRef.current = result;
        setData(result);
        setError(null);
      }
    } catch (err) {
      if (mountedRef.current) {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      if (mountedRef.current) setLoading(false);
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
