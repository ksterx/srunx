/**
 * Regenerate the README / docs screenshots from the live UI with mocked APIs.
 *
 * The filename intentionally omits the `.spec.ts` suffix so Playwright's
 * default test glob (`**\/*.spec.ts`) does not pick it up — that keeps
 * `npx playwright test` (used in CI) free of capture overhead. Run it
 * explicitly by passing the path:
 *
 *   npx playwright test e2e/capture-screenshots.ts --project=chromium
 *
 * Outputs land in repo `docs/assets/images/`. The mkdocs build copies them
 * to `public/assets/images/`, so we don't write there directly.
 */
import { test, type Page } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

// playwright runs from src/srunx/web/frontend/, so repo root is 4 levels up.
const REPO_ROOT = path.resolve(process.cwd(), "../../../..");
const DOCS_OUT = path.join(REPO_ROOT, "docs/assets/images");

async function save(page: Page, name: string) {
  fs.mkdirSync(DOCS_OUT, { recursive: true });
  const out = path.join(DOCS_OUT, name);
  await page.screenshot({ path: out, type: "png" });
  console.log(`wrote ${path.relative(REPO_ROOT, out)}`);
}

// Settle: wait for the sidebar's framer-motion width animation to complete +
// any data-driven repaints to flush. 400ms is comfortably above the 200ms
// transition without making the spec slow.
async function settle(page: Page) {
  await page.waitForLoadState("networkidle");
  await page.waitForTimeout(400);
}

test.describe("UI screenshots", () => {
  // Viewport sizing rationale: the docs render images at ~800px wide
  // (e.g. README's `<img width="800">`). Capturing at 1280x800 means the
  // README downscales by ~1.6×, leaving body text legible (~10-11px on
  // screen). 1920x1200 was unreadable when downscaled by 2.4×.

  test("dashboard, jobs, resources @ 1280x800", async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await setupMockRoutes(page);

    await page.goto("/");
    await settle(page);
    await save(page, "ui-dashboard.png");

    await page.goto("/jobs");
    await settle(page);
    await save(page, "ui-jobs.png");

    await page.goto("/resources");
    await settle(page);
    await save(page, "ui-resources.png");
  });

  test("workflow DAG @ 1280x800", async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await setupMockRoutes(page);

    await page.goto("/workflows/ml-pipeline?mount=ml-project");
    await settle(page);
    // Give react-flow's layout a beat after data lands.
    await page.waitForTimeout(600);
    await save(page, "ui-workflow-dag.png");
  });

  test("explorer @ 1280x800", async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await setupMockRoutes(page);

    // Use the demo's richer browse fixture so the tree shows realistic content.
    const SCRIPT = `#!/bin/bash\n#SBATCH --job-name=train_bert\n#SBATCH --partition=gpu\n#SBATCH --gres=gpu:1\nsource activate ml_env\npython train.py\n`;
    const ROOT_ENTRIES = [
      { name: "configs", type: "directory" },
      { name: "datasets", type: "directory" },
      { name: "docs", type: "directory" },
      { name: "models", type: "directory" },
      { name: "scripts", type: "directory" },
      { name: "templates", type: "directory" },
      { name: "tests", type: "directory" },
      { name: "config.yaml", type: "file", size: 412 },
      { name: "main.py", type: "file", size: 2014 },
      { name: "pyproject.toml", type: "file", size: 890 },
      { name: "README.md", type: "file", size: 1820 },
      { name: "run_experiment.sh", type: "file", size: 310 },
      { name: "train_bert.sh", type: "file", size: SCRIPT.length },
      { name: "uv.lock", type: "file", size: 52100 },
    ];
    await page.route("**/api/files/browse*", (route) =>
      route.fulfill({ json: { entries: ROOT_ENTRIES } }),
    );

    await page.goto("/explorer");
    await settle(page);
    // FileExplorer auto-opens the first mount on load and fetches its
    // entries. Just wait for the listing to appear before snapping.
    await page
      .locator("text=train_bert.sh")
      .waitFor({ state: "visible", timeout: 5000 });
    await page.waitForTimeout(300);
    await save(page, "ui-explorer.png");
  });

  test("notifications center variants @ 1200x740", async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 740 });
    await setupMockRoutes(page);

    // Populated (the default mock state).
    await page.goto("/notifications");
    await settle(page);
    await save(page, "notifications-center-populated.png");

    // Filter applied — click the "delivered" filter pill.
    await page
      .locator(".panel", { hasText: "Recent deliveries" })
      .getByRole("button", { name: "delivered", exact: true })
      .click();
    await page.waitForTimeout(250);
    await save(page, "notifications-center-filter-delivered.png");

    // Empty — register higher-priority overrides that short-circuit the
    // default mocks and return empty arrays. (Playwright matches the
    // most-recently-added route first.) The shape mirrors the real backend:
    // /api/deliveries returns a bare array (filtered server-side); watches
    // and subscriptions also return bare arrays.
    await page.route("**/api/deliveries*", (r) => r.fulfill({ json: [] }));
    await page.route("**/api/deliveries/stuck*", (r) =>
      r.fulfill({ json: { count: 0, older_than_sec: 300 } }),
    );
    await page.route("**/api/watches*", (r) => r.fulfill({ json: [] }));
    await page.route("**/api/subscriptions*", (r) => r.fulfill({ json: [] }));
    await page.reload();
    await settle(page);
    await save(page, "notifications-center-empty.png");
  });
});
