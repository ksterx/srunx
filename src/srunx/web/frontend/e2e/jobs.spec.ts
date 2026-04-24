import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";
import { MOCK_JOBS } from "./fixtures/mock-data.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Jobs", () => {
  test("displays job list table with all jobs", async ({ page }) => {
    await page.goto("/jobs");

    const table = page.locator("table");
    await expect(
      table.getByRole("cell", { name: "train-resnet" }),
    ).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "preprocess-data" }),
    ).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "evaluate-model" }),
    ).toBeVisible();
    await expect(table.getByRole("cell", { name: "failed-job" })).toBeVisible();
  });

  test("shows correct job count", async ({ page }) => {
    await page.goto("/jobs");

    await expect(page.getByText("4 jobs")).toBeVisible();
  });

  test("search filter narrows results", async ({ page }) => {
    await page.goto("/jobs");

    const searchInput = page.getByPlaceholder("Search jobs...");
    await searchInput.fill("train");

    /* Only train-resnet should remain visible */
    await expect(
      page.locator("table").getByRole("cell", { name: "train-resnet" }),
    ).toBeVisible();
    await expect(page.getByText("1 jobs")).toBeVisible();
  });

  test("status filter works", async ({ page }) => {
    await page.goto("/jobs");

    const select = page.locator("select");
    await select.selectOption("FAILED");

    /* Only the failed job should show */
    await expect(
      page.locator("table").getByRole("cell", { name: "failed-job" }),
    ).toBeVisible();
    await expect(page.getByText("1 jobs")).toBeVisible();
  });

  test("cancel button sends DELETE request", async ({ page }) => {
    await page.goto("/jobs");

    let deleteCalled = false;
    await page.route("**/api/jobs/10001", (route) => {
      if (route.request().method() === "DELETE") {
        deleteCalled = true;
        return route.fulfill({ status: 204 });
      }
      return route.continue();
    });

    // Target the 10001 (RUNNING) row explicitly — ``.first()`` would
    // depend on the table's sort order, which changed when the page
    // started grouping active-before-terminal then desc by job_id.
    const row = page.locator("tr").filter({ hasText: "10001" });
    await row.locator("button[title='Cancel Job']").click();

    expect(deleteCalled).toBe(true);
  });

  test("log link navigates to log viewer", async ({ page }) => {
    await page.goto("/jobs");

    const logLink = page.locator("a[title='View Logs']").first();
    await logLink.click();

    await expect(page).toHaveURL(/\/jobs\/\d+\/logs$/);
  });

  test("handles non-standard SLURM statuses without crashing", async ({
    page,
  }) => {
    /* Override jobs mock with non-standard SLURM statuses */
    await page.route("**/api/jobs", async (route) => {
      if (route.request().method() !== "GET") return route.continue();
      return route.fulfill({
        json: [
          {
            ...MOCK_JOBS[0],
            job_id: 20001,
            name: "completing-job",
            status: "COMPLETING",
          },
          {
            ...MOCK_JOBS[0],
            job_id: 20002,
            name: "nodefail-job",
            status: "NODE_FAIL",
          },
          {
            ...MOCK_JOBS[0],
            job_id: 20003,
            name: "preempted-job",
            status: "PREEMPTED",
          },
          {
            ...MOCK_JOBS[0],
            job_id: 20004,
            name: "suspended-job",
            status: "SUSPENDED",
          },
          {
            ...MOCK_JOBS[0],
            job_id: 20005,
            name: "unknown-status-job",
            status: "CONFIGURING",
          },
        ],
      });
    });

    await page.goto("/jobs");

    /* Page must NOT show the ErrorBoundary "Something went wrong" */
    await expect(page.getByText("Something went wrong")).not.toBeVisible();

    /* All jobs should render */
    await expect(page.getByText("5 jobs")).toBeVisible();
    const table = page.locator("table");
    await expect(
      table.getByRole("cell", { name: "completing-job" }),
    ).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "nodefail-job" }),
    ).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "preempted-job" }),
    ).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "suspended-job" }),
    ).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "unknown-status-job" }),
    ).toBeVisible();

    /* Status badges should render (CONFIGURING falls back to Unknown) */
    await expect(table.getByText("Completing", { exact: true })).toBeVisible();
    await expect(table.getByText("Node Fail", { exact: true })).toBeVisible();
    await expect(table.getByText("Preempted", { exact: true })).toBeVisible();
    await expect(table.getByText("Suspended", { exact: true })).toBeVisible();
    await expect(table.getByText("Unknown", { exact: true })).toBeVisible();
  });

  test("handles job status changing during polling without crashing", async ({
    page,
  }) => {
    let callCount = 0;
    await page.route("**/api/jobs", async (route) => {
      if (route.request().method() !== "GET") return route.continue();
      callCount++;
      if (callCount === 1) {
        /* First poll: job is RUNNING */
        return route.fulfill({
          json: [
            {
              ...MOCK_JOBS[0],
              job_id: 30001,
              name: "changing-job",
              status: "RUNNING",
            },
          ],
        });
      }
      /* Subsequent polls: job transitions to COMPLETING then COMPLETED */
      return route.fulfill({
        json: [
          {
            ...MOCK_JOBS[0],
            job_id: 30001,
            name: "changing-job",
            status: "COMPLETED",
          },
        ],
      });
    });

    await page.goto("/jobs");

    /* First render: Running */
    await expect(page.locator("table").getByText("Running")).toBeVisible();

    /* Wait for poll to update status — allow extra time for CI environments */
    await expect(page.locator("table").getByText("Completed")).toBeVisible({
      timeout: 30000,
    });

    /* Must not have crashed */
    await expect(page.getByText("Something went wrong")).not.toBeVisible();
  });

  test("handles undefined/null status gracefully", async ({ page }) => {
    await page.route("**/api/jobs", async (route) => {
      if (route.request().method() !== "GET") return route.continue();
      return route.fulfill({
        json: [
          {
            ...MOCK_JOBS[0],
            job_id: 40001,
            name: "null-status-job",
            status: null,
          },
          {
            ...MOCK_JOBS[0],
            job_id: 40002,
            name: "empty-status-job",
            status: "",
          },
        ],
      });
    });

    await page.goto("/jobs");

    /* Page must NOT crash */
    await expect(page.getByText("Something went wrong")).not.toBeVisible();
    await expect(page.getByText("2 jobs")).toBeVisible();
  });
});
