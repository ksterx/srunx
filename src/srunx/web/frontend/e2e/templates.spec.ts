import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Templates", () => {
  test("displays template list with built-in and user templates", async ({
    page,
  }) => {
    await page.goto("/templates");

    await expect(
      page.getByRole("heading", { name: "Job Templates" }),
    ).toBeVisible();

    /* Both templates should be listed */
    await expect(page.getByText("base")).toBeVisible();
    await expect(page.getByText("gpu-single")).toBeVisible();

    /* User-defined template shows "custom" badge */
    await expect(page.getByText("custom")).toBeVisible();
  });

  test("New Template button opens create dialog", async ({ page }) => {
    await page.goto("/templates");

    await page.getByRole("button", { name: "New Template" }).click();

    /* Dialog should appear */
    await expect(
      page.getByRole("heading", { name: "New Template" }),
    ).toBeVisible();

    /* Name field should be visible */
    await expect(page.getByPlaceholder("my-template")).toBeVisible();

    /* Content textarea should be visible */
    await expect(page.locator("textarea")).toBeVisible();
  });

  test("create template submits POST and refreshes list", async ({ page }) => {
    await page.goto("/templates");

    let createCalled = false;
    await page.route("**/api/templates", async (route) => {
      if (route.request().method() === "POST") {
        createCalled = true;
        const body = route.request().postDataJSON();
        return route.fulfill({
          status: 201,
          json: {
            name: body.name,
            description: body.description,
            use_case: body.use_case,
          },
        });
      }
      return route.fallback();
    });

    await page.getByRole("button", { name: "New Template" }).click();
    await page.getByPlaceholder("my-template").fill("test-template");
    await page.getByPlaceholder("What this template does").fill("Test desc");
    await page
      .getByPlaceholder("e.g. Single GPU training jobs")
      .fill("Testing");

    await page.getByRole("button", { name: "Create" }).click();

    /* Should have called the API */
    expect(createCalled).toBe(true);

    /* Success message should appear */
    await expect(
      page.getByText('Template "test-template" created'),
    ).toBeVisible();
  });

  test("edit button opens edit dialog for user-defined template", async ({
    page,
  }) => {
    await page.goto("/templates");

    /* Click edit on the user-defined template (gpu-single) */
    const gpuCard = page.locator(".panel", { hasText: "gpu-single" });
    await gpuCard
      .locator("button")
      .filter({ has: page.locator("svg") })
      .first()
      .click();

    /* Dialog should show "Edit: gpu-single" */
    await expect(
      page.getByRole("heading", { name: "Edit: gpu-single" }),
    ).toBeVisible();
  });

  test("delete button sends DELETE request for user-defined template", async ({
    page,
  }) => {
    await page.goto("/templates");

    let deleteCalled = false;
    await page.route("**/api/templates/gpu-single", async (route) => {
      if (route.request().method() === "DELETE") {
        deleteCalled = true;
        return route.fulfill({ status: 204 });
      }
      return route.fallback();
    });

    /* Click delete on the user-defined template */
    const gpuCard = page.locator(".panel", { hasText: "gpu-single" });
    const deleteBtn = gpuCard
      .locator("button")
      .filter({ has: page.locator("svg") })
      .last();
    await deleteBtn.click();

    expect(deleteCalled).toBe(true);
    await expect(page.getByText('Template "gpu-single" deleted')).toBeVisible();
  });

  test("built-in template does not show edit/delete buttons", async ({
    page,
  }) => {
    await page.goto("/templates");

    const baseCard = page.locator(".panel", { hasText: "base" }).first();

    /* base card should NOT have Pencil or Trash2 buttons in header */
    await expect(baseCard.locator(".panel-header button")).toHaveCount(0);
  });

  test("clicking a template card expands detail section", async ({ page }) => {
    await page.goto("/templates");

    /* Click on base template */
    await page.locator(".panel", { hasText: "base" }).first().click();

    /* Detail panel with template source toggle should appear */
    await expect(
      page.getByRole("button", { name: "Template Source" }),
    ).toBeVisible();

    /* Command input should be visible */
    await expect(
      page.getByPlaceholder("python train.py --epochs 10"),
    ).toBeVisible();
  });

  test("cancel button in create dialog closes it", async ({ page }) => {
    await page.goto("/templates");

    await page.getByRole("button", { name: "New Template" }).click();
    await expect(
      page.getByRole("heading", { name: "New Template" }),
    ).toBeVisible();

    await page.getByRole("button", { name: "Cancel" }).click();

    /* Dialog should be closed */
    await expect(
      page.getByRole("heading", { name: "New Template" }),
    ).not.toBeVisible();
  });
});
