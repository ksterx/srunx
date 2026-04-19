import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Notifications Center", () => {
  test("sidebar exposes Notifications nav entry", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("link", { name: "Notifications" }),
    ).toBeVisible();
  });

  test("page renders summary cards from mocked data", async ({ page }) => {
    await page.goto("/notifications");

    await expect(
      page.getByRole("heading", { name: /Notifications/i }),
    ).toBeVisible();

    // Stat card headings (section headings use the exact text as <h3>)
    await expect(
      page.getByRole("heading", { name: /Open watches/i }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: /Subscriptions/i }),
    ).toBeVisible();
    await expect(page.getByText(/Stuck pending/i)).toBeVisible();
  });

  test("recent deliveries table shows mocked entries + status chips", async ({
    page,
  }) => {
    await page.goto("/notifications");

    // Two deliveries in the fixture: one delivered, one pending.
    await expect(page.getByText("delivered").first()).toBeVisible();
    await expect(page.getByText("pending").first()).toBeVisible();
  });

  test("status filter restricts the recent deliveries table", async ({
    page,
  }) => {
    await page.goto("/notifications");

    const panel = page.locator(".panel", { hasText: "Recent deliveries" });
    const rows = panel.locator("tbody tr");

    // Two rows before filtering.
    await expect(rows).toHaveCount(2);

    // Click the "delivered" filter pill.
    await panel.getByRole("button", { name: "delivered", exact: true }).click();

    // One row remains (the delivered one).
    await expect(rows).toHaveCount(1);
  });

  test("watches + subscriptions tables render", async ({ page }) => {
    await page.goto("/notifications");

    const watchesPanel = page.locator(".panel", {
      hasText: "Open watches",
    });
    await expect(watchesPanel.getByText("job:10001")).toBeVisible();
    await expect(watchesPanel.locator("tbody tr")).toHaveCount(1);

    const subsPanel = page.locator(".panel", {
      hasText: /^Subscriptions/,
    });
    await expect(
      subsPanel.getByText("slack_webhook:primary").first(),
    ).toBeVisible();
  });
});
