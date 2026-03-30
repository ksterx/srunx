import type { Page } from "@playwright/test";
import {
  MOCK_JOBS,
  MOCK_WORKFLOWS,
  MOCK_RESOURCES,
  MOCK_HISTORY,
  MOCK_STATS,
  MOCK_LOGS,
} from "./mock-data.ts";

/**
 * Set up all API mock routes on a Playwright Page.
 * Call this in beforeEach() for every test.
 */
export async function setupMockRoutes(page: Page) {
  /* ── Jobs ──────────────────────────────────── */
  await page.route("**/api/jobs", (route) => {
    if (route.request().method() === "GET") {
      return route.fulfill({ json: MOCK_JOBS });
    }
    if (route.request().method() === "POST") {
      return route.fulfill({ json: MOCK_JOBS[0] });
    }
    return route.continue();
  });

  /* GET /api/jobs/:id/logs — must be registered before the catch-all */
  await page.route("**/api/jobs/*/logs", (route) => {
    return route.fulfill({ json: MOCK_LOGS });
  });

  await page.route("**/api/jobs/*", (route) => {
    const url = route.request().url();

    /* DELETE /api/jobs/:id */
    if (route.request().method() === "DELETE") {
      return route.fulfill({ status: 204 });
    }

    /* GET /api/jobs/:id */
    const match = url.match(/\/api\/jobs\/(\d+)/);
    if (match) {
      const id = Number(match[1]);
      const job = MOCK_JOBS.find((j) => j.job_id === id);
      if (job) {
        return route.fulfill({ json: job });
      }
      return route.fulfill({
        status: 404,
        json: { detail: `Job ${id} not found`, code: "job_not_found" },
      });
    }

    return route.continue();
  });

  /* ── Workflows ─────────────────────────────── */
  await page.route("**/api/workflows/runs*", (route) => {
    return route.fulfill({ json: [] });
  });

  await page.route("**/api/workflows/validate", (route) => {
    return route.fulfill({ json: { valid: true } });
  });

  await page.route("**/api/workflows/upload", (route) => {
    return route.fulfill({ json: MOCK_WORKFLOWS[0] });
  });

  await page.route("**/api/workflows/create", (route) => {
    if (route.request().method() === "POST") {
      const body = route.request().postDataJSON();
      const name = body?.name ?? "test-workflow";
      return route.fulfill({
        json: {
          name,
          jobs: (body?.jobs ?? []).map(
            (j: {
              name: string;
              command: string[];
              depends_on?: string[];
            }) => ({
              name: j.name,
              status: "UNKNOWN",
              depends_on: j.depends_on ?? [],
              command: j.command,
              resources: {},
            }),
          ),
        },
      });
    }
    return route.continue();
  });

  await page.route("**/api/workflows/**/run", (route) => {
    return route.fulfill({
      json: {
        id: "run-001",
        workflow_name: "ml-pipeline",
        started_at: new Date().toISOString(),
        status: "running",
        job_statuses: {},
      },
    });
  });

  await page.route("**/api/workflows", (route) => {
    return route.fulfill({ json: MOCK_WORKFLOWS });
  });

  await page.route("**/api/workflows/*", (route) => {
    const url = route.request().url();
    const segment = decodeURIComponent(url.split("/api/workflows/")[1]);

    /* Let create endpoint through to its own mock */
    if (segment === "create" && route.request().method() === "POST") {
      return route.fallback();
    }

    const wf = MOCK_WORKFLOWS.find((w) => w.name === segment);
    if (wf) {
      return route.fulfill({ json: wf });
    }
    return route.fulfill({
      status: 404,
      json: {
        detail: `Workflow ${segment} not found`,
        code: "workflow_not_found",
      },
    });
  });

  /* ── Resources ─────────────────────────────── */
  await page.route("**/api/resources*", (route) => {
    return route.fulfill({ json: MOCK_RESOURCES });
  });

  /* ── History ───────────────────────────────── */
  await page.route("**/api/history/stats*", (route) => {
    return route.fulfill({ json: MOCK_STATS });
  });

  await page.route("**/api/history*", (route) => {
    return route.fulfill({ json: MOCK_HISTORY });
  });
}

/**
 * Set up routes that return errors for testing error states.
 */
export async function setupErrorRoutes(page: Page) {
  await page.route("**/api/**", (route) => {
    return route.fulfill({
      status: 500,
      json: { detail: "Internal server error", code: "server_error" },
    });
  });
}
