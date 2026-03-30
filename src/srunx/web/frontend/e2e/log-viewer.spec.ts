import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Log Viewer", () => {
  test("displays stdout log lines", async ({ page }) => {
    await page.goto("/jobs/10001/logs");

    /* Should show stdout tab content by default */
    await expect(page.getByText("Epoch 1/10")).toBeVisible({ timeout: 10000 });
    await expect(page.getByText("Epoch 2/10")).toBeVisible();
    await expect(page.getByText("Epoch 3/10")).toBeVisible();
  });

  test("switches between stdout and stderr tabs", async ({ page }) => {
    await page.goto("/jobs/10001/logs");

    /* Initially stdout */
    await expect(page.getByText("Epoch 1/10")).toBeVisible({ timeout: 10000 });

    /* Click stderr tab */
    await page.getByText("stderr").click();

    /* Should show stderr content */
    await expect(
      page.getByText("WARNING: GPU memory usage high"),
    ).toBeVisible();
    await expect(page.getByText("Checkpoint saved")).toBeVisible();
  });

  test("shows line count in tab badges", async ({ page }) => {
    await page.goto("/jobs/10001/logs");

    /* stdout has 3 lines — the badge inside the stdout button */
    const stdoutBtn = page.getByRole("button", { name: /stdout/i });
    await expect(stdoutBtn).toBeVisible({ timeout: 10000 });
    await expect(stdoutBtn.locator("span").last()).toHaveText("3");

    /* stderr has 2 lines */
    const stderrBtn = page.getByRole("button", { name: /stderr/i });
    await expect(stderrBtn.locator("span").last()).toHaveText("2");
  });

  test("shows job name and status in header", async ({ page }) => {
    await page.goto("/jobs/10001/logs");

    await expect(page.getByText("Job 10001")).toBeVisible({ timeout: 10000 });
    await expect(page.getByText("train-resnet")).toBeVisible();
    await expect(page.getByText("Running")).toBeVisible();
  });

  test("shows error for invalid job ID", async ({ page }) => {
    await page.goto("/jobs/99999/logs");

    await expect(page.getByText("Failed to load job")).toBeVisible({
      timeout: 10000,
    });
  });

  test("back button navigates to jobs list", async ({ page }) => {
    await page.goto("/jobs/10001/logs");

    /* Wait for page to load */
    await expect(page.getByText("Job 10001")).toBeVisible({ timeout: 10000 });

    /* Click the back arrow link (the one without text, not the sidebar link) */
    await page.locator("a[href='/jobs']").last().click();
    await expect(page).toHaveURL(/\/jobs$/);
  });
});
