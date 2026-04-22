import { expect, test } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

/**
 * Phase 5b: sweep detail UX — status filter, column sort, per-cell cancel,
 * progress bar + ETA.
 *
 * The backend fixture is a running sweep with a deliberately mixed cell
 * set so each feature has something meaningful to exercise:
 *   - 1 pending, 1 running, 2 completed, 1 failed, 1 cancelled
 */

const SWEEP_ID = 555;
const SWEEP_STARTED_AT = new Date(Date.now() - 120_000).toISOString();

type CellStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

type Cell = {
  id: number;
  workflow_name: string;
  status: CellStatus;
  started_at: string | null;
  completed_at: string | null;
  args: Record<string, unknown>;
  error: string | null;
  triggered_by: "web" | "mcp" | "cli";
};

const INITIAL_CELLS: Cell[] = [
  {
    id: 701,
    workflow_name: "ml-pipeline",
    status: "completed",
    started_at: new Date(Date.now() - 100_000).toISOString(),
    completed_at: new Date(Date.now() - 70_000).toISOString(),
    args: { lr: "0.001", seed: 1 },
    error: null,
    triggered_by: "web",
  },
  {
    id: 702,
    workflow_name: "ml-pipeline",
    status: "completed",
    started_at: new Date(Date.now() - 95_000).toISOString(),
    completed_at: new Date(Date.now() - 30_000).toISOString(),
    args: { lr: "0.01", seed: 1 },
    error: null,
    triggered_by: "web",
  },
  {
    id: 703,
    workflow_name: "ml-pipeline",
    status: "failed",
    started_at: new Date(Date.now() - 90_000).toISOString(),
    completed_at: new Date(Date.now() - 40_000).toISOString(),
    args: { lr: "0.1", seed: 1 },
    error: "diverged at step 3",
    triggered_by: "web",
  },
  {
    id: 704,
    workflow_name: "ml-pipeline",
    status: "running",
    started_at: new Date(Date.now() - 30_000).toISOString(),
    completed_at: null,
    args: { lr: "0.001", seed: 2 },
    error: null,
    triggered_by: "web",
  },
  {
    id: 705,
    workflow_name: "ml-pipeline",
    status: "cancelled",
    started_at: new Date(Date.now() - 80_000).toISOString(),
    completed_at: new Date(Date.now() - 50_000).toISOString(),
    args: { lr: "0.01", seed: 2 },
    error: null,
    triggered_by: "web",
  },
  {
    id: 706,
    workflow_name: "ml-pipeline",
    status: "pending",
    started_at: null,
    completed_at: null,
    args: { lr: "0.1", seed: 2 },
    error: null,
    triggered_by: "web",
  },
];

function buildSweep(cells: Cell[]) {
  const count = (s: CellStatus) => cells.filter((c) => c.status === s).length;
  return {
    id: SWEEP_ID,
    name: "ml-pipeline",
    workflow_yaml_path: null,
    status: "running" as const,
    matrix: { lr: ["0.001", "0.01", "0.1"], seed: [1, 2] },
    args: null,
    fail_fast: false,
    max_parallel: 2,
    cell_count: cells.length,
    cells_pending: count("pending"),
    cells_running: count("running"),
    cells_completed: count("completed"),
    cells_failed: count("failed"),
    cells_cancelled: count("cancelled"),
    submission_source: "web" as const,
    started_at: SWEEP_STARTED_AT,
    completed_at: null,
    cancel_requested_at: null,
    error: null,
  };
}

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Sweep Detail UX (Phase 5b)", () => {
  test("status filter narrows cells to the failed subset only", async ({
    page,
  }) => {
    const cells: Cell[] = JSON.parse(JSON.stringify(INITIAL_CELLS));

    await page.route(`**/api/sweep_runs/${SWEEP_ID}`, (route) =>
      route.fulfill({ json: buildSweep(cells) }),
    );
    await page.route(`**/api/sweep_runs/${SWEEP_ID}/cells`, (route) =>
      route.fulfill({ json: cells }),
    );

    await page.goto(`/sweep_runs/${SWEEP_ID}`);

    // All cells visible initially.
    await expect(page.getByTestId("sweep-cell-row-701")).toBeVisible();
    await expect(page.getByTestId("sweep-cell-row-703")).toBeVisible();
    await expect(page.getByTestId("sweep-cell-row-705")).toBeVisible();

    // Apply "failed" filter → only cell 703 remains.
    await page.getByTestId("sweep-cell-status-filter").selectOption("failed");

    await expect(page.getByTestId("sweep-cell-row-703")).toBeVisible();
    await expect(page.getByTestId("sweep-cell-row-701")).not.toBeVisible();
    await expect(page.getByTestId("sweep-cell-row-702")).not.toBeVisible();
    await expect(page.getByTestId("sweep-cell-row-705")).not.toBeVisible();
  });

  test("sort by started toggles ascending → descending", async ({ page }) => {
    const cells: Cell[] = JSON.parse(JSON.stringify(INITIAL_CELLS));

    await page.route(`**/api/sweep_runs/${SWEEP_ID}`, (route) =>
      route.fulfill({ json: buildSweep(cells) }),
    );
    await page.route(`**/api/sweep_runs/${SWEEP_ID}/cells`, (route) =>
      route.fulfill({ json: cells }),
    );

    await page.goto(`/sweep_runs/${SWEEP_ID}`);

    // Default: # index order == insertion order (cells 701..706).
    const firstRowInitial = page
      .locator('tr[data-testid^="sweep-cell-row-"]')
      .first();
    await expect(firstRowInitial).toHaveAttribute(
      "data-testid",
      "sweep-cell-row-701",
    );

    // Sort by Started asc → oldest-started cell first. 706 has null
    // started_at (pending) and its Date.parse falls back to 0, so it
    // should sort to the top in ascending.
    await page.getByTestId("sort-started").click();

    const firstRowAsc = page
      .locator('tr[data-testid^="sweep-cell-row-"]')
      .first();
    await expect(firstRowAsc).toHaveAttribute(
      "data-testid",
      "sweep-cell-row-706",
    );

    // Toggle: second click on the same header flips to descending.
    // 704 (most recent started) should be first.
    await page.getByTestId("sort-started").click();

    const firstRowDesc = page
      .locator('tr[data-testid^="sweep-cell-row-"]')
      .first();
    await expect(firstRowDesc).toHaveAttribute(
      "data-testid",
      "sweep-cell-row-704",
    );
  });

  test("per-cell cancel button posts to workflow runs cancel", async ({
    page,
  }) => {
    const cells: Cell[] = JSON.parse(JSON.stringify(INITIAL_CELLS));
    const cancelRequests: string[] = [];

    await page.route(`**/api/sweep_runs/${SWEEP_ID}`, (route) =>
      route.fulfill({ json: buildSweep(cells) }),
    );
    await page.route(`**/api/sweep_runs/${SWEEP_ID}/cells`, (route) =>
      route.fulfill({ json: cells }),
    );
    await page.route("**/api/workflows/runs/*/cancel", (route) => {
      const match = route
        .request()
        .url()
        .match(/\/runs\/(\d+)\/cancel/);
      if (match) {
        cancelRequests.push(match[1]);
        // Flip the matching cell to cancelled so the reload sees the
        // action took effect.
        const cancelledId = Number(match[1]);
        const idx = cells.findIndex((c) => c.id === cancelledId);
        if (idx >= 0) {
          cells[idx].status = "cancelled";
          cells[idx].completed_at = new Date().toISOString();
        }
      }
      return route.fulfill({
        json: { status: "cancelled", run_id: match?.[1] ?? "0" },
      });
    });

    await page.goto(`/sweep_runs/${SWEEP_ID}`);

    // Cancel button shown for running cell 704, not shown for terminal 701.
    await expect(page.getByTestId("sweep-cell-cancel-704")).toBeVisible();
    await expect(page.getByTestId("sweep-cell-cancel-701")).toHaveCount(0);

    await page.getByTestId("sweep-cell-cancel-704").click();

    // Expect the cancel endpoint to be hit for the active cell only.
    await expect.poll(() => cancelRequests).toContain("704");
  });

  test("progress bar + ETA render and draining indicator appears on draining sweeps", async ({
    page,
  }) => {
    const cells: Cell[] = JSON.parse(JSON.stringify(INITIAL_CELLS));

    await page.route(`**/api/sweep_runs/${SWEEP_ID}`, (route) =>
      route.fulfill({ json: buildSweep(cells) }),
    );
    await page.route(`**/api/sweep_runs/${SWEEP_ID}/cells`, (route) =>
      route.fulfill({ json: cells }),
    );

    await page.goto(`/sweep_runs/${SWEEP_ID}`);

    await expect(page.getByTestId("sweep-progress-bar")).toBeVisible();
    // ETA label only shown while the sweep is active and at least one
    // cell has terminated (we have 2 completed / 1 failed / 1 cancelled).
    await expect(page.getByTestId("sweep-eta")).toBeVisible();
    await expect(page.getByTestId("sweep-draining-indicator")).toHaveCount(0);

    // Switch the sweep to draining and re-load to trigger the indicator.
    const drainingSweep = {
      ...buildSweep(cells),
      status: "draining" as const,
    };
    await page.route(`**/api/sweep_runs/${SWEEP_ID}`, (route) =>
      route.fulfill({ json: drainingSweep }),
    );
    await page.reload();

    await expect(page.getByTestId("sweep-draining-indicator")).toBeVisible();
  });
});
