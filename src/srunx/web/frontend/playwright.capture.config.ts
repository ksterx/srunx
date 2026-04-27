/**
 * Alternate Playwright config used only for regenerating UI screenshots.
 *
 * The main `playwright.config.ts` matches `*.spec.ts` (as set by the
 * default Playwright glob), so `e2e/capture-screenshots.ts` is invisible
 * to the regular CI run. This config overrides `testMatch` to include
 * exactly that one file.
 *
 *   npx playwright test --config=playwright.capture.config.ts
 */
import { defineConfig } from "@playwright/test";
import baseConfig from "./playwright.config.ts";

export default defineConfig({
  ...baseConfig,
  testMatch: ["**/capture-screenshots.ts"],
  // Captures touch the same shared output directory; keep them serial.
  workers: 1,
  fullyParallel: false,
  retries: 0,
  reporter: "list",
});
