import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Resources", () => {
  test("displays partition cards", async ({ page }) => {
    await page.goto("/resources");

    /* Partition names in card headers */
    const panels = page.locator(".panel-header");
    await expect(panels.getByText("gpu", { exact: true })).toBeVisible();
    await expect(panels.getByText("cpu", { exact: true })).toBeVisible();
  });

  test("GPU partition shows correct utilization percentage", async ({
    page,
  }) => {
    await page.goto("/resources");

    /* gpu partition: 63% utilization */
    const gpuCard = page
      .locator(".panel")
      .filter({ has: page.locator(".panel-header", { hasText: "gpu" }) })
      .first();
    await expect(gpuCard.getByText("63%")).toBeVisible();
  });

  test("shows AVAILABLE and FULL status labels", async ({ page }) => {
    await page.goto("/resources");

    /* These labels appear in the panel headers with exact text */
    const headers = page.locator(".panel-header");
    await expect(headers.getByText("AVAILABLE", { exact: true })).toBeVisible();
    await expect(headers.getByText("FULL", { exact: true })).toBeVisible();
  });

  test("shows node statistics in GPU partition", async ({ page }) => {
    await page.goto("/resources");

    /* gpu partition has Total Nodes, Idle, Down sections */
    const gpuCard = page
      .locator(".panel")
      .filter({ has: page.locator(".panel-header", { hasText: "gpu" }) })
      .first();
    await expect(gpuCard.getByText("Total Nodes")).toBeVisible();
    await expect(gpuCard.getByText("Idle")).toBeVisible();
  });

  test("shows error banner on API failure", async ({ page }) => {
    await page.route("**/api/resources*", (route) => {
      return route.fulfill({
        status: 500,
        json: { detail: "sinfo command failed" },
      });
    });

    await page.goto("/resources");

    await expect(page.getByText(/500/)).toBeVisible({ timeout: 10000 });
  });
});
