import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Navigation & Layout", () => {
  test("sidebar links navigate to correct pages", async ({ page }) => {
    await page.goto("/");

    /* Dashboard is the default route */
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();

    /* Navigate to Jobs */
    await page.getByRole("link", { name: "Jobs" }).click();
    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    await expect(page).toHaveURL(/\/jobs$/);

    /* Navigate to Workflows */
    await page.getByRole("link", { name: "Workflows" }).click();
    await expect(
      page.getByRole("heading", { name: "Workflows" }),
    ).toBeVisible();
    await expect(page).toHaveURL(/\/workflows$/);

    /* Navigate to Resources */
    await page.getByRole("link", { name: "Resources" }).click();
    await expect(
      page.getByRole("heading", { name: "Resources" }),
    ).toBeVisible();
    await expect(page).toHaveURL(/\/resources$/);

    /* Navigate back to Dashboard */
    await page.getByRole("link", { name: "Dashboard" }).click();
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
    await expect(page).toHaveURL(/\/$/);
  });

  test("sidebar collapse and expand works", async ({ page }) => {
    await page.goto("/");

    /* The sidebar should show "srunx" brand text */
    await expect(page.getByText("srunx")).toBeVisible();

    /* Click collapse button (chevron) */
    const collapseBtn = page.locator("aside button").last();
    await collapseBtn.click();

    /* Brand text should be hidden after collapse */
    await expect(page.getByText("srunx")).toBeHidden();

    /* Click expand button */
    await collapseBtn.click();

    /* Brand text should reappear */
    await expect(page.getByText("srunx")).toBeVisible();
  });

  test("page titles display correctly on each page", async ({ page }) => {
    const pages = [
      { path: "/", title: "Dashboard" },
      { path: "/jobs", title: "Jobs" },
      { path: "/workflows", title: "Workflows" },
      { path: "/resources", title: "Resources" },
    ];

    for (const p of pages) {
      await page.goto(p.path);
      await expect(page.getByRole("heading", { name: p.title })).toBeVisible();
    }
  });
});
