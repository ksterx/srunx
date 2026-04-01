import { test, expect } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

test.beforeEach(async ({ page }) => {
  await setupMockRoutes(page);
});

test.describe("Workflow Builder", () => {
  test("navigates to builder from workflows page via New Workflow button", async ({
    page,
  }) => {
    await page.goto("/workflows");

    const newBtn = page.getByRole("link", { name: "New Workflow" });
    await expect(newBtn).toBeVisible();
    await newBtn.click();

    await expect(page).toHaveURL(/\/workflows\/new/);
  });

  test("displays empty builder canvas with toolbar", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    /* Toolbar elements */
    await expect(page.getByPlaceholder("workflow-name")).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Job" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Save Workflow/ }),
    ).toBeVisible();

    /* Back link */
    await expect(page.getByTitle("Back to workflows")).toBeVisible();
  });

  test("Add Job creates a new node on the canvas", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    const addBtn = page.getByRole("button", { name: "Add Job" });
    await addBtn.click();

    /* The node should show the default name "job_1" */
    await expect(page.getByText("job_1")).toBeVisible();
    /* Should show "draft" badge */
    await expect(page.getByText("draft")).toBeVisible();
  });

  test("adding multiple jobs creates multiple nodes", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    const addBtn = page.getByRole("button", { name: "Add Job" });
    await addBtn.click();
    await addBtn.click();
    await addBtn.click();

    await expect(page.getByText("job_1")).toBeVisible();
    await expect(page.getByText("job_2")).toBeVisible();
    await expect(page.getByText("job_3")).toBeVisible();
  });

  test("clicking a node opens the property panel", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    /* Add a job */
    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();

    /* Click the node in the ReactFlow canvas */
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    /* Property panel should show Name and Command inputs */
    await expect(page.getByPlaceholder("job_name")).toBeVisible();
    await expect(
      page.getByPlaceholder("python train.py --epochs 100"),
    ).toBeVisible();
  });

  test("editing job name in panel updates the node", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();

    /* Click the node */
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    /* Wait for panel to appear */
    const nameInput = page.getByPlaceholder("job_name");
    await expect(nameInput).toBeVisible();

    /* Clear and type new name */
    await nameInput.clear();
    await nameInput.fill("preprocess");

    /* The node and panel header should reflect the change */
    await expect(
      page.getByRole("heading", { name: "preprocess" }),
    ).toBeVisible();
  });

  test("save shows error when workflow name is empty", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();

    /* Try to save without a workflow name */
    await page.getByRole("button", { name: /Save Workflow/ }).click();

    /* Should show workflow name error */
    await expect(page.getByText("Workflow name is required")).toBeVisible();
  });

  test("save shows validation errors for empty command", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    /* Add a job (default has empty command) */
    await page.getByRole("button", { name: "Add Job" }).click();

    /* Set a workflow name */
    await page.getByPlaceholder("workflow-name").fill("test-wf");

    /* Try to save */
    await page.getByRole("button", { name: /Save Workflow/ }).click();

    /* Should show command error */
    await expect(page.getByText(/empty command/i)).toBeVisible();
  });

  test("successful save redirects to workflow detail", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    /* Set workflow name */
    await page.getByPlaceholder("workflow-name").fill("my-pipeline");

    /* Add a job */
    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();

    /* Click the node to open panel */
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    /* Fill in command */
    const commandInput = page.getByPlaceholder("python train.py --epochs 100");
    await expect(commandInput).toBeVisible();
    await commandInput.fill("python train.py");

    /* Close panel by clicking pane */
    await page
      .locator(".react-flow__pane")
      .click({ position: { x: 10, y: 10 } });

    /* Save */
    await page.getByRole("button", { name: /Save Workflow/ }).click();

    /* Should redirect to the workflow detail page */
    await expect(page).toHaveURL(/\/workflows\/my-pipeline/, {
      timeout: 5000,
    });
  });

  test("save with invalid name shows error", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    /* Set invalid workflow name with spaces */
    await page.getByPlaceholder("workflow-name").fill("my pipeline");

    /* Add a job with command */
    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();

    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    const commandInput = page.getByPlaceholder("python train.py --epochs 100");
    await expect(commandInput).toBeVisible();
    await commandInput.fill("python train.py");

    await page
      .locator(".react-flow__pane")
      .click({ position: { x: 10, y: 10 } });

    await page.getByRole("button", { name: /Save Workflow/ }).click();

    /* Should show name validation error */
    await expect(
      page.getByText(/letters, numbers, underscores, and hyphens/i),
    ).toBeVisible();
  });

  test("close button on property panel hides it", async ({ page }) => {
    await page.goto("/workflows/new?mount=ml-project");

    await page.getByRole("button", { name: "Add Job" }).click();
    await expect(page.getByText("job_1")).toBeVisible();

    /* Click node to open panel */
    await page.locator("[data-id]").filter({ hasText: "job_1" }).click();

    /* Panel should be visible */
    const nameInput = page.getByPlaceholder("job_name");
    await expect(nameInput).toBeVisible();

    /* Close the panel */
    await page.getByTitle("Close panel").click();

    /* Panel should be hidden */
    await expect(nameInput).not.toBeVisible();
  });
});
