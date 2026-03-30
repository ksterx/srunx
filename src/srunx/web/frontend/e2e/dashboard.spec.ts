import { test, expect } from "@playwright/test";
import { setupMockRoutes, setupErrorRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Dashboard", () => {
  test("displays four metric cards", async ({ page }) => {
    await page.goto("/");

    await expect(
      page.locator(".metric-label", { hasText: "Active Jobs" }),
    ).toBeVisible();
    await expect(
      page.locator(".metric-label", { hasText: "Failed" }),
    ).toBeVisible();
    await expect(
      page.locator(".metric-label", { hasText: "Completed (Total)" }),
    ).toBeVisible();
    await expect(
      page.locator(".metric-label", { hasText: "GPUs Available" }),
    ).toBeVisible();
  });

  test("shows active jobs count from API data", async ({ page }) => {
    await page.goto("/");

    /* We have 2 active jobs (RUNNING + PENDING) in mock data */
    const activeCard = page
      .locator(".panel")
      .filter({ hasText: "Active Jobs" })
      .first();
    await expect(activeCard.locator(".metric-value")).toHaveText("2");
  });

  test("displays active jobs table", async ({ page }) => {
    await page.goto("/");

    const table = page.locator("table").first();
    await expect(table).toBeVisible();
    await expect(
      table.getByRole("cell", { name: "train-resnet" }),
    ).toBeVisible();
  });

  test("displays GPU resources section", async ({ page }) => {
    await page.goto("/");

    await expect(page.getByText("GPU Resources")).toBeVisible();
    /* The "gpu" partition gauge should be present */
    await expect(
      page.locator(".metric-label").filter({ hasText: /^gpu$/ }),
    ).toBeVisible();
  });

  test("shows error banner on API failure", async ({ page }) => {
    /* Override with error routes */
    await setupErrorRoutes(page);
    await page.goto("/");

    /* Error banner should appear with status text */
    await expect(page.getByText(/500/)).toBeVisible({ timeout: 10000 });
  });
});
