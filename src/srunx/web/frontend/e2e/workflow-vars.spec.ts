import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Workflow Variables (args & outputs)", () => {
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

    // Args editor should appear with the placeholder
    const textarea = page.getByPlaceholder("base_dir=/data/experiments");
    await expect(textarea).toBeVisible();

    // Click again to hide
    await argsBtn.click();
    await expect(textarea).not.toBeVisible();
  });

  test("args can be typed into the editor", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    // Open args editor
    await page.getByTitle("Workflow variables (args)").click();

    const textarea = page.getByPlaceholder("base_dir=/data/experiments");
    await textarea.fill("lr=0.001\nbatch_size=32");
    await expect(textarea).toHaveValue("lr=0.001\nbatch_size=32");
  });

  test("outputs section is visible in job property panel", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    // Add a job and click it
    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    // Scroll the Outputs section into view and check
    const outputsTextarea = page.getByPlaceholder(
      "model_path=/data/models/best.pt",
    );
    await outputsTextarea.scrollIntoViewIfNeeded();
    await expect(outputsTextarea).toBeVisible();
  });

  test("outputs can be typed into the panel", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    const outputsTextarea = page.getByPlaceholder(
      "model_path=/data/models/best.pt",
    );
    await outputsTextarea.scrollIntoViewIfNeeded();
    await outputsTextarea.fill("checkpoint=/data/ckpt.pt");
    await expect(outputsTextarea).toHaveValue("checkpoint=/data/ckpt.pt");
  });

  test("help text mentions $SRUNX_OUTPUTS", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    const srunxText = page.getByText("$SRUNX_OUTPUTS");
    await srunxText.scrollIntoViewIfNeeded();
    await expect(srunxText).toBeVisible();
  });

  test("args editor shows hint about {{ var_name }} syntax", async ({
    page,
  }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByTitle("Workflow variables (args)").click();

    await expect(page.getByText("{{ var_name }}")).toBeVisible();
  });
});
