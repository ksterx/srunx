import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Workflows", () => {
  test("displays workflow cards", async ({ page }) => {
    await page.goto("/workflows");

    await expect(page.getByText("ml-pipeline")).toBeVisible();
    await expect(page.getByText("data-pipeline")).toBeVisible();
  });

  test("cards show job count, dependency count, and GPU count", async ({
    page,
  }) => {
    await page.goto("/workflows");

    /* ml-pipeline: 3 jobs, 2 deps, 5 GPUs (4+1) */
    const mlCard = page
      .locator(".panel")
      .filter({ hasText: "ml-pipeline" })
      .first();
    await expect(mlCard.getByText("3")).toBeVisible(); /* jobs */
    await expect(mlCard.getByText("2")).toBeVisible(); /* deps */
    await expect(mlCard.getByText("5")).toBeVisible(); /* GPUs */
  });

  test("View DAG link navigates to workflow detail", async ({ page }) => {
    await page.goto("/workflows");

    const viewBtn = page.getByRole("link", { name: "View DAG" }).first();
    await viewBtn.click();

    await expect(page).toHaveURL(/\/workflows\/ml-pipeline/);
  });

  test("Upload YAML button opens file dialog", async ({ page }) => {
    await page.goto("/workflows");

    /* The hidden file input should exist */
    const fileInput = page.locator("input[type='file']");
    await expect(fileInput).toBeAttached();
    await expect(fileInput).toHaveAttribute("accept", ".yaml,.yml");

    /* Clicking the upload button should trigger the file input */
    const uploadBtn = page.getByRole("button", { name: "Upload YAML" });
    await expect(uploadBtn).toBeVisible();

    /* Upload a file via the file chooser */
    const [fileChooser] = await Promise.all([
      page.waitForEvent("filechooser"),
      uploadBtn.click(),
    ]);

    let uploadCalled = false;
    await page.route("**/api/workflows/upload", (route) => {
      uploadCalled = true;
      return route.fulfill({ json: { name: "test-wf", jobs: [] } });
    });

    await fileChooser.setFiles({
      name: "test.yaml",
      mimeType: "text/yaml",
      buffer: Buffer.from("name: test-wf\njobs: []"),
    });

    /* Wait for upload to complete */
    await page.waitForTimeout(500);
    expect(uploadCalled).toBe(true);
  });

  test("shows job name chips on cards", async ({ page }) => {
    await page.goto("/workflows");

    const mlCard = page
      .locator(".panel")
      .filter({ hasText: "ml-pipeline" })
      .first();
    await expect(mlCard.getByText("preprocess")).toBeVisible();
    await expect(mlCard.getByText("train")).toBeVisible();
    await expect(mlCard.getByText("evaluate")).toBeVisible();
  });
});
