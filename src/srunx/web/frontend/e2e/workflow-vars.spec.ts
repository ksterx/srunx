import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Workflow Variables (args & exports)", () => {
  test("args button is visible in builder toolbar", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    const argsBtn = page.getByTitle("Workflow variables (args)");
    await expect(argsBtn).toBeVisible();
  });

  test("clicking args button toggles the args editor panel", async ({
    page,
  }) => {
    await page.goto("/workflows/new?mount=ml-project");

    const argsBtn = page.getByTitle("Workflow variables (args)");
    await argsBtn.click();

    // Panel should appear with "Workflow Variables" title and Add button
    await expect(page.getByText("Workflow Variables")).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Add Variable" }),
    ).toBeVisible();

    // Click again to hide
    await argsBtn.click();
    await expect(page.getByText("Workflow Variables")).not.toBeVisible();
  });

  test("adding and filling args entries works", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByTitle("Workflow variables (args)").click();

    // Click "Add Variable" to add a row
    await page.getByRole("button", { name: "Add Variable" }).click();

    // Should see key and value inputs
    const keyInput = page.getByPlaceholder("variable").first();
    const valueInput = page.getByPlaceholder("/path/or/value").first();
    await expect(keyInput).toBeVisible();
    await expect(valueInput).toBeVisible();

    // Fill them in
    await keyInput.fill("base_dir");
    await valueInput.fill("/data/experiments");

    await expect(keyInput).toHaveValue("base_dir");
    await expect(valueInput).toHaveValue("/data/experiments");
  });

  test("exports section is visible in job property panel", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    // Scroll the Add Export button into view
    const addExportBtn = page.getByRole("button", { name: "Add Export" });
    await addExportBtn.scrollIntoViewIfNeeded();
    await expect(addExportBtn).toBeVisible();
  });

  test("adding exports creates key-value row", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    const addExportBtn = page.getByRole("button", { name: "Add Export" });
    await addExportBtn.scrollIntoViewIfNeeded();
    await addExportBtn.click();

    // Should see key and value inputs for export
    const keyInput = page.getByPlaceholder("name").first();
    const valueInput = page
      .locator('input[placeholder="/path/or/value"]')
      .first();
    await expect(keyInput).toBeVisible();
    await expect(valueInput).toBeVisible();

    await keyInput.fill("model_path");
    await valueInput.fill("/data/model.pt");

    await expect(keyInput).toHaveValue("model_path");
    await expect(valueInput).toHaveValue("/data/model.pt");
  });

  test("help text mentions deps.<job>.<key> syntax", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    const depsText = page.getByText(/deps\.job_1\./);
    await depsText.scrollIntoViewIfNeeded();
    await expect(depsText).toBeVisible();
  });

  test("args editor shows hint about variable_name syntax", async ({
    page,
  }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByTitle("Workflow variables (args)").click();

    await expect(page.getByText("variable_name")).toBeVisible();
  });
});
