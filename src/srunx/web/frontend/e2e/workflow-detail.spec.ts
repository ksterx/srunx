import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Workflow Detail", () => {
  test("DAG view renders with React Flow canvas", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline?mount=ml-project");

    const reactFlow = page.locator(".react-flow");
    await expect(reactFlow).toBeVisible({ timeout: 10000 });
  });

  test("shows workflow name and job count in header", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline?mount=ml-project");

    await expect(
      page.getByRole("heading", { name: "ml-pipeline" }),
    ).toBeVisible();
    await expect(page.getByText("3 jobs")).toBeVisible();
  });

  test("DAG/list view toggle works", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline?mount=ml-project");

    /* Default is DAG view */
    await expect(page.locator(".react-flow")).toBeVisible({ timeout: 10000 });

    /* Switch to list view */
    await page.getByRole("button", { name: /list/i }).click();

    /* Table should now be visible with job names in the name column */
    const table = page.locator("table");
    await expect(table).toBeVisible();

    const nameColumn = table.locator("tbody td:first-child");
    await expect(nameColumn.getByText("preprocess")).toBeVisible();
    await expect(nameColumn.getByText("train")).toBeVisible();
    await expect(nameColumn.getByText("evaluate")).toBeVisible();
  });

  test("clicking a job in list view opens detail sidebar", async ({ page }) => {
    await page.goto("/workflows/ml-pipeline?mount=ml-project");

    /* Switch to list view */
    await page.getByRole("button", { name: /list/i }).click();

    /* Click on the "train" row */
    await page
      .locator("table tbody tr")
      .filter({ hasText: "train" })
      .first()
      .click();

    /* Sidebar should appear with job details */
    const sidebar = page.locator(".panel").last();
    await expect(sidebar.getByText("Command")).toBeVisible();
  });

  test("shows error state for non-existent workflow", async ({ page }) => {
    await page.goto("/workflows/nonexistent?mount=ml-project");

    await expect(page.getByText("Failed to load workflow")).toBeVisible({
      timeout: 10000,
    });
  });
});
