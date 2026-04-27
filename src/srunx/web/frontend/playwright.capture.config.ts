/**
 * Alternate Playwright config used only for regenerating UI screenshots.
 *
 * The main `playwright.config.ts` matches `*.spec.ts` (the default
 * Playwright glob), so `e2e/capture-screenshots.ts` is invisible to the
 * regular CI run. This config narrows `testMatch` to exactly that one
 * file. Run it with:
 *
 *   npx playwright test --config=playwright.capture.config.ts
 *
 * NOTE: this intentionally does NOT `import` the base config — Playwright
 * walks the spec tree from any imported `*.config.ts` and would re-trigger
 * test discovery from the wrong context. Duplicating the few fields we
 * need keeps that hazard out of the picture.
 */
import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testMatch: ["**/capture-screenshots.ts"],
  // Captures share the same output directory; serial run keeps that simple.
  workers: 1,
  fullyParallel: false,
  retries: 0,
  reporter: "list",
  use: {
    baseURL: "http://localhost:4173",
    trace: "off",
    screenshot: "off",
  },
  webServer: {
    command: "npm run preview",
    url: "http://localhost:4173",
    reuseExistingServer: true,
    timeout: 10000,
  },
  projects: [
    {
      name: "chromium",
      use: { browserName: "chromium" },
    },
  ],
});
