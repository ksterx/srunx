/**
 * Shared BFS-based DAG layer computation.
 *
 * Used by both DAGView (read-only workflow visualization) and
 * use-workflow-builder (editable workflow builder).
 */

type HasDeps = {
  name: string;
  depends_on?: string[];
};

/**
 * Assign each job to a layer via BFS from root nodes (jobs with no dependencies).
 * Jobs that depend on others are placed in layers after their dependencies.
 *
 * Returns a Map<jobName, layerIndex> and the grouped layers.
 */
export function computeDAGLayers<T extends HasDeps>(
  jobs: T[],
): { layers: Map<string, number>; groups: Map<number, string[]> } {
  const layers = new Map<string, number>();
  const roots = jobs.filter((j) => !j.depends_on || j.depends_on.length === 0);

  const queue = roots.map((j) => ({ name: j.name, layer: 0 }));
  while (queue.length > 0) {
    const { name, layer } = queue.shift()!;
    const existing = layers.get(name);
    if (existing !== undefined && existing >= layer) continue;
    layers.set(name, layer);

    for (const job of jobs) {
      if (job.depends_on?.some((d) => d === name || d.includes(`:${name}`))) {
        queue.push({ name: job.name, layer: layer + 1 });
      }
    }
  }

  // Assign missing jobs (disconnected) to layer 0
  for (const job of jobs) {
    if (!layers.has(job.name)) layers.set(job.name, 0);
  }

  // Group by layer
  const groups = new Map<number, string[]>();
  for (const [name, layer] of layers) {
    const group = groups.get(layer) ?? [];
    group.push(name);
    groups.set(layer, group);
  }

  return { layers, groups };
}
