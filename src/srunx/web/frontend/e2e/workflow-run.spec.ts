import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Workflow Run & Lifecycle", () => {
  test("Run Workflow button is visible on detail page", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline");

    await expect(
      page.getByRole("button", { name: /Run Workflow/ }),
    ).toBeVisible();
  });

  test("clicking Run Workflow triggers execution", async ({ page }) => {
    let runCalled = false;
    await page.route("**/api/workflows/ml-pipeline/run", (route) => {
      runCalled = true;
      return route.fulfill({
        status: 202,
        json: {
          id: "run-001",
          workflow_name: "ml-pipeline",
          started_at: new Date().toISOString(),
          completed_at: null,
          status: "running",
          job_ids: {
            preprocess: "10001",
            train: "10002",
            evaluate: "10003",
          },
          job_statuses: {
            preprocess: "RUNNING",
            train: "PENDING",
            evaluate: "PENDING",
          },
          error: null,
        },
      });
    });

    await page.goto("/workflows/ml-pipeline");
    await page.getByRole("button", { name: /Run Workflow/ }).click();

    await expect(() => expect(runCalled).toBe(true)).toPass({ timeout: 3000 });
  });

  test("Run Workflow button becomes disabled after clicking", async ({
    page,
  }) => {
    await page.route("**/api/workflows/ml-pipeline/run", (route) => {
      return route.fulfill({
        status: 202,
        json: {
          id: "run-001",
          workflow_name: "ml-pipeline",
          started_at: new Date().toISOString(),
          completed_at: null,
          status: "running",
          job_ids: {},
          job_statuses: {},
          error: null,
        },
      });
    });

    await page.goto("/workflows/ml-pipeline");
    const runBtn = page.getByRole("button", { name: /Run Workflow/ });
    await runBtn.click();

    /* After triggering a run, the button should be disabled */
    await expect(runBtn).toBeDisabled({ timeout: 5000 });
  });

  test("cancel button appears during active run", async ({ page }) => {
    await page.route("**/api/workflows/ml-pipeline/run", (route) => {
      return route.fulfill({
        status: 202,
        json: {
          id: "run-001",
          workflow_name: "ml-pipeline",
          started_at: new Date().toISOString(),
          completed_at: null,
          status: "running",
          job_ids: {},
          job_statuses: {},
          error: null,
        },
      });
    });

    await page.goto("/workflows/ml-pipeline");

    /* Cancel button should not be visible before a run starts */
    await expect(
      page.getByRole("button", { name: /Cancel/ }),
    ).not.toBeVisible();

    await page.getByRole("button", { name: /Run Workflow/ }).click();

    /* Cancel button should appear after run starts */
    await expect(page.getByRole("button", { name: /Cancel/ })).toBeVisible({
      timeout: 3000,
    });
  });

  test("clicking Cancel sets run to cancelled", async ({ page }) => {
    await page.route("**/api/workflows/ml-pipeline/run", (route) => {
      return route.fulfill({
        status: 202,
        json: {
          id: "run-001",
          workflow_name: "ml-pipeline",
          started_at: new Date().toISOString(),
          completed_at: null,
          status: "running",
          job_ids: {},
          job_statuses: {},
          error: null,
        },
      });
    });

    await page.goto("/workflows/ml-pipeline");
    await page.getByRole("button", { name: /Run Workflow/ }).click();

    /* Wait for Cancel button to appear */
    const cancelBtn = page.getByRole("button", { name: /Cancel/ });
    await expect(cancelBtn).toBeVisible({ timeout: 3000 });

    await cancelBtn.click();

    /* After cancellation, the status should update to Cancelled */
    await expect(page.getByText("Cancelled")).toBeVisible({ timeout: 3000 });
  });

  test("delete workflow navigates back to list", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline");

    /* Override confirm dialog to accept */
    page.on("dialog", (dialog) => dialog.accept());

    const deleteBtn = page.getByTitle("Delete workflow");
    await expect(deleteBtn).toBeVisible();
    await deleteBtn.click();

    await expect(page).toHaveURL(/\/workflows$/, { timeout: 3000 });
  });

  test("delete workflow is cancelled when dialog is dismissed", async ({
    page,
  }) => {
    await page.goto("/workflows/ml-pipeline");

    /* Override confirm dialog to dismiss */
    page.on("dialog", (dialog) => dialog.dismiss());

    await page.getByTitle("Delete workflow").click();

    /* Should remain on the detail page */
    await expect(page).toHaveURL(/\/workflows\/ml-pipeline$/);
    await expect(
      page.getByRole("heading", { name: "ml-pipeline" }),
    ).toBeVisible();
  });

  test("Edit link navigates to edit page", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline");

    const editLink = page.getByRole("link", { name: /Edit/i });
    await expect(editLink).toBeVisible();
    await editLink.click();

    await expect(page).toHaveURL(/\/workflows\/ml-pipeline\/edit$/);
  });

  test("Run Workflow button is disabled during active run", async ({
    page,
  }) => {
    await page.route("**/api/workflows/ml-pipeline/run", (route) => {
      return route.fulfill({
        status: 202,
        json: {
          id: "run-001",
          workflow_name: "ml-pipeline",
          started_at: new Date().toISOString(),
          completed_at: null,
          status: "running",
          job_ids: {},
          job_statuses: {},
          error: null,
        },
      });
    });

    await page.goto("/workflows/ml-pipeline");
    await page.getByRole("button", { name: /Run Workflow/ }).click();

    /* Button should be disabled while a run is active */
    await expect(
      page.getByRole("button", { name: /Run Workflow/ }),
    ).toBeDisabled({ timeout: 3000 });
  });
});
