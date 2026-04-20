/**
 * E2E coverage for Settings → Notifications endpoint CRUD (P3-10 #J).
 *
 * The existing ``notifications.spec.ts`` covers the
 * ``NotificationsCenter`` *dashboard* — read-only view of the outbox,
 * open watches, subscriptions, and the deliveries filter.
 *
 * This spec exercises the *writable* surface on the Settings tab:
 *
 * 1. Initial render — existing endpoint row visible.
 * 2. Add-endpoint form — validation + happy-path create.
 * 3. Toggle disable/enable — button text + visible status flip.
 * 4. Delete endpoint — row disappears after the confirm dialog.
 *
 * Mocks are in-memory via ``setupMockRoutes``; each test gets a
 * fresh copy of the fixture state.
 */

import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

async function openNotificationsTab(page: import("@playwright/test").Page) {
  await page.goto("/settings");
  // Settings tabs render as buttons with the label text.
  await page.getByRole("button", { name: /Notifications/i }).click();
  await expect(
    page.getByRole("heading", { name: /Notification Endpoints/i }),
  ).toBeVisible();
}

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Settings → Notifications", () => {
  test("existing endpoint is listed", async ({ page }) => {
    await openNotificationsTab(page);

    // Fixture seeds a ``primary`` slack_webhook endpoint.
    await expect(page.getByText("primary")).toBeVisible();
    await expect(page.getByText("slack_webhook").first()).toBeVisible();
    await expect(page.getByText("Enabled")).toBeVisible();
  });

  test("webhook URL validation gates the Create button", async ({ page }) => {
    await openNotificationsTab(page);
    await page.getByRole("button", { name: /Add endpoint/i }).click();

    // Name alone isn't enough — URL must match the Slack pattern.
    const nameInput = page.locator("input[placeholder='e.g. team-alerts']");
    const urlInput = page.locator(
      "input[placeholder='https://hooks.slack.com/services/...']",
    );
    await nameInput.fill("team-alerts");
    await urlInput.fill("not-a-slack-url");
    await expect(page.getByRole("button", { name: /^Create$/ })).toBeDisabled();

    // Inline validation message surfaces.
    await expect(
      page.getByText(/Must be a valid Slack webhook URL/i),
    ).toBeVisible();

    // Valid URL unlocks the button.
    await urlInput.fill("https://hooks.slack.com/services/T00/B00/XYZ123");
    await expect(page.getByRole("button", { name: /^Create$/ })).toBeEnabled();
  });

  test("creates an endpoint and shows the success banner", async ({ page }) => {
    await openNotificationsTab(page);
    await page.getByRole("button", { name: /Add endpoint/i }).click();

    const nameInput = page.locator("input[placeholder='e.g. team-alerts']");
    const urlInput = page.locator(
      "input[placeholder='https://hooks.slack.com/services/...']",
    );
    await nameInput.fill("team-alerts");
    await urlInput.fill("https://hooks.slack.com/services/T00/B00/XYZ123");
    await page.getByRole("button", { name: /^Create$/ }).click();

    // New row appears in the table + success banner fires with the name.
    // The name text appears in two places (success banner + table cell),
    // so scope each assertion to its specific landmark to avoid strict-
    // mode locator collisions.
    await expect(page.getByRole("cell", { name: "team-alerts" })).toBeVisible();
    await expect(
      page.getByText(/Endpoint "team-alerts" created/i),
    ).toBeVisible();
  });

  test("toggle flips the endpoint status", async ({ page }) => {
    await openNotificationsTab(page);

    // Baseline: seeded endpoint is enabled — "Disable" button is shown.
    await expect(page.getByRole("button", { name: "Disable" })).toBeVisible();
    await page.getByRole("button", { name: "Disable" }).click();

    // After click: button label swaps to Enable, status chip says Disabled.
    // ``Disabled`` text appears in both the success banner and the status
    // chip, so scope to the chip's exact match.
    await expect(page.getByRole("button", { name: "Enable" })).toBeVisible();
    await expect(page.getByText("Disabled", { exact: true })).toBeVisible();
    await expect(page.getByText(/Endpoint "primary" disabled/i)).toBeVisible();
  });

  test("deletes an endpoint after confirm", async ({ page }) => {
    await openNotificationsTab(page);

    // Auto-accept the window.confirm prompt the row fires.
    page.on("dialog", (d) => d.accept());

    await page.getByRole("button", { name: "Delete" }).click();

    // Row disappears + empty-state banner shows; success banner fires.
    await expect(
      page.getByText("No endpoints configured yet.", { exact: false }),
    ).toBeVisible();
    await expect(page.getByText(/primary.*deleted/i)).toBeVisible();
  });
});
