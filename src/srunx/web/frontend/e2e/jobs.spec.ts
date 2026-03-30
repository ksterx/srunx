import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

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

    const cancelBtn = page.locator("button[title='Cancel Job']").first();
    await cancelBtn.click();

    expect(deleteCalled).toBe(true);
  });

  test("log link navigates to log viewer", async ({ page }) => {
    await page.goto("/jobs");

    const logLink = page.locator("a[title='View Logs']").first();
    await logLink.click();

    await expect(page).toHaveURL(/\/jobs\/\d+\/logs$/);
  });
});
