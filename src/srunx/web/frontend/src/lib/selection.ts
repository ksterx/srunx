// Client-side selection of the cluster and project the UI operates on.
//
// The server has no "current cluster": every /api/* request names its target
// through `?cluster=<name>&project=<name>`. The selection lives here
// (persisted in localStorage) and `installSelectionFetch` appends it to every
// API request that does not already specify those params.

import { useSyncExternalStore } from "react";
import type { ClusterInfo, ProjectInfo } from "./types.ts";

const CLUSTER_KEY = "srunx.selection.cluster";
const PROJECT_KEY = "srunx.selection.project";

export type Selection = {
  cluster: string | null;
  project: string | null;
  clusters: ClusterInfo[];
  defaultCluster: string | null;
  projects: ProjectInfo[];
  /** True once /api/clusters and /api/projects have been loaded. */
  ready: boolean;
};

function readStored(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStored(key: string, value: string | null): void {
  try {
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  } catch {
    /* ignore storage failures (private mode etc.) */
  }
}

let state: Selection = {
  cluster: readStored(CLUSTER_KEY),
  project: readStored(PROJECT_KEY),
  clusters: [],
  defaultCluster: null,
  projects: [],
  ready: false,
};

const listeners = new Set<() => void>();

function setState(patch: Partial<Selection>): void {
  state = { ...state, ...patch };
  writeStored(CLUSTER_KEY, state.cluster);
  writeStored(PROJECT_KEY, state.project);
  listeners.forEach((l) => l());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getSelection(): Selection {
  return state;
}

export function useSelection(): Selection {
  return useSyncExternalStore(subscribe, getSelection);
}

export function selectCluster(name: string | null): void {
  if (name !== state.cluster) setState({ cluster: name });
}

export function selectProject(name: string | null): void {
  if (name === state.project) return;
  // Follow the project's pinned cluster when it has one.
  const pinned = state.projects.find((p) => p.name === name)?.cluster;
  const cluster =
    pinned && state.clusters.some((c) => c.name === pinned)
      ? pinned
      : state.cluster;
  setState({ project: name, cluster });
}

/**
 * (Re)load clusters and projects and repair a stale selection. The empty
 * `cluster=`/`project=` parameters stop the interceptor from attaching the
 * stored selection, which may be stale (the server rejects an unknown
 * project with 400 before these handlers run).
 */
export async function refreshSelection(): Promise<void> {
  const [clusterRes, projectRes] = await Promise.all([
    fetch("/api/clusters?cluster=&project=")
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null),
    fetch("/api/projects?cluster=&project=")
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null),
  ]);
  const clusters: ClusterInfo[] = clusterRes?.clusters ?? [];
  const defaultCluster: string | null = clusterRes?.default ?? null;
  const projects: ProjectInfo[] = Array.isArray(projectRes) ? projectRes : [];

  let project = state.project;
  if (!project || !projects.some((p) => p.name === project))
    project = projects[0]?.name ?? null;

  let cluster = state.cluster;
  if (!cluster || !clusters.some((c) => c.name === cluster)) {
    const pinned = projects.find((p) => p.name === project)?.cluster ?? null;
    const candidates = [pinned, defaultCluster, clusters[0]?.name ?? null];
    cluster =
      candidates.find((c) => c && clusters.some((x) => x.name === c)) ?? null;
  }

  setState({
    clusters,
    defaultCluster,
    projects,
    cluster,
    project,
    ready: true,
  });
}

function isApiRequest(url: string): boolean {
  return url.startsWith("/api/");
}

function withSelection(url: string): string {
  const { cluster, project } = state;
  if (!cluster && !project) return url;
  const hashIdx = url.indexOf("#");
  const base = hashIdx >= 0 ? url.slice(0, hashIdx) : url;
  const hash = hashIdx >= 0 ? url.slice(hashIdx) : "";
  const qIdx = base.indexOf("?");
  const path = qIdx >= 0 ? base.slice(0, qIdx) : base;
  const params = new URLSearchParams(qIdx >= 0 ? base.slice(qIdx + 1) : "");
  if (cluster && !params.has("cluster")) params.set("cluster", cluster);
  if (project && !params.has("project")) params.set("project", project);
  const qs = params.toString();
  return `${path}${qs ? `?${qs}` : ""}${hash}`;
}

/**
 * Wrap window.fetch so /api/* requests carry the selected cluster and project
 * as query params (unless the URL already sets them). Idempotent; composes
 * with the auth interceptor regardless of installation order.
 */
export function installSelectionFetch(): void {
  const w = window as typeof window & { __srunxSelectionInstalled?: boolean };
  if (w.__srunxSelectionInstalled) return;
  w.__srunxSelectionInstalled = true;

  const original = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit) => {
    if (typeof input === "string" && isApiRequest(input)) {
      return original(withSelection(input), init);
    }
    if (input instanceof URL || input instanceof Request) {
      const raw = input instanceof Request ? input.url : input.toString();
      const url = new URL(raw, window.location.origin);
      if (url.origin === window.location.origin && isApiRequest(url.pathname)) {
        const rewritten = withSelection(url.pathname + url.search + url.hash);
        const next = new URL(rewritten, window.location.origin);
        return original(
          input instanceof Request ? new Request(next, input) : next,
          init,
        );
      }
    }
    return original(input, init);
  };
}
