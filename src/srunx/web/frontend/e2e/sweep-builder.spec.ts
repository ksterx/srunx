import { expect, test } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

/**
 * Phase I smoke test for the parameter-sweep UX.
 *
 * Covers the Phase J handoff scenario end-to-end using Playwright route
 * interception as a fake backend:
 *   1. Open the workflow Run dialog
 *   2. Add an ``lr`` arg and flip it to list mode
 *   3. Verify the cell_count preview reflects the 3 comma-separated values
 *   4. Submit → the request carries ``sweep.matrix`` and lands on the
 *      sweeps list page with the newly created row in ``pending``
 *   5. Click into the detail page → 3 cell rows rendered, one per axis
 *      value
 */

type RecordedRun = {
  body: unknown;
};

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Sweep Run Dialog", () => {
  test("list-mode args drive sweep submission + detail drilldown", async ({
    page,
  }) => {
    // --- Fake backend state for this test only ---------------------------
    const recorded: RecordedRun = { body: null };
    const matrixValues = ["0.001", "0.01", "0.1"];

    const sweepRow = {
      id: 42,
      name: "ml-pipeline",
      workflow_yaml_path: null,
      status: "pending" as const,
      matrix: { lr: matrixValues },
      args: null,
      fail_fast: false,
      max_parallel: 4,
      cell_count: matrixValues.length,
      cells_pending: matrixValues.length,
      cells_running: 0,
      cells_completed: 0,
      cells_failed: 0,
      cells_cancelled: 0,
      submission_source: "web" as const,
      started_at: new Date().toISOString(),
      completed_at: null,
      cancel_requested_at: null,
      error: null,
    };

    const cells = matrixValues.map((lr, i) => ({
      id: 100 + i,
      workflow_name: "ml-pipeline",
      status: "pending" as const,
      started_at: new Date().toISOString(),
      completed_at: null,
      args: { lr },
      error: null,
      triggered_by: "web",
    }));

    let sweepCreated = false;

    // Intercept the workflow run POST and return the sweep envelope. The
    // backend returns ``{sweep_run_id, status, cell_count}`` with 202
    // when the request body contains ``sweep``.
    await page.route("**/api/workflows/ml-pipeline/run*", (route) => {
      if (route.request().method() !== "POST") return route.continue();
      const body = route.request().postDataJSON();
      recorded.body = body;
      sweepCreated = true;
      return route.fulfill({
        status: 202,
        json: {
          sweep_run_id: sweepRow.id,
          status: "pending",
          cell_count: sweepRow.cell_count,
        },
      });
    });

    // Once the sweep is "created", subsequent fetches from the pages
    // see it. Before creation the list is empty (default mock).
    await page.route("**/api/sweep_runs", (route) => {
      if (route.request().method() !== "GET") return route.continue();
      return route.fulfill({ json: sweepCreated ? [sweepRow] : [] });
    });
    await page.route(/\/api\/sweep_runs\/\d+$/, (route) => {
      if (route.request().method() !== "GET") return route.continue();
      return route.fulfill({ json: sweepRow });
    });
    await page.route(/\/api\/sweep_runs\/\d+\/cells$/, (route) => {
      if (route.request().method() !== "GET") return route.continue();
      return route.fulfill({ json: cells });
    });

    // --- Open the workflow detail page ---------------------------------
    await page.goto("/workflows/ml-pipeline?mount=ml-project");
    await expect(
      page.getByRole("heading", { name: "ml-pipeline" }),
    ).toBeVisible();

    // --- Open the Run dialog --------------------------------------------
    await page.getByRole("button", { name: /Run Workflow/ }).click();
    await expect(page.getByText(/^Run:/)).toBeVisible();

    // --- Add a new arg row "lr" and switch to list mode ----------------
    await page.getByTestId("add-arg-button").click();

    const nameInput = page.getByRole("textbox", { name: "arg name" }).last();
    await nameInput.fill("lr");

    // After typing a name the row testid stabilizes on the key; flip to
    // list mode via the per-row toggle.
    await page.getByTestId("arg-mode-list-lr").click();

    // The value input's aria-label switches in list mode.
    const valueInput = page.getByRole("textbox", {
      name: /arg values \(comma-separated\)/,
    });
    await valueInput.fill(matrixValues.join(","));

    // --- Cell count preview shows 3 ------------------------------------
    const preview = page.getByTestId("sweep-preview");
    await expect(preview).toBeVisible();
    await expect(preview).toContainText("3");
    await expect(preview).toContainText("lr[3]");

    // --- Submit the sweep ----------------------------------------------
    // The action button relabels to "Run Sweep (3)" when in sweep mode.
    await page.getByRole("button", { name: /Run Sweep \(3\)/ }).click();

    // Should navigate to the detail page for this sweep.
    await expect(page).toHaveURL(/\/sweep_runs\/42$/, { timeout: 5000 });

    // Verify the recorded request body carried the sweep payload.
    await expect
      .poll(() => {
        const body = recorded.body as {
          sweep?: { matrix?: Record<string, unknown[]> };
        } | null;
        return body?.sweep?.matrix?.lr ?? null;
      })
      .toEqual(matrixValues);

    // --- Detail page renders 3 cell rows -------------------------------
    for (const cell of cells) {
      await expect(page.getByTestId(`sweep-cell-row-${cell.id}`)).toBeVisible();
    }

    // Navigate back to the list page and confirm the new row appears.
    await page.goto("/sweep_runs");
    await expect(page.getByTestId(`sweep-row-${sweepRow.id}`)).toBeVisible();
  });
});
