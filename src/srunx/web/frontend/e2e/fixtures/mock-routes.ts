import type { Page } from "@playwright/test";
import {
  MOCK_JOBS,
  MOCK_WORKFLOWS,
  MOCK_RESOURCES,
  MOCK_HISTORY,
  MOCK_STATS,
  MOCK_LOGS,
  MOCK_TEMPLATES,
  MOCK_TEMPLATE_DETAIL,
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

  /* ── Mounts (needed by workflows page) ─────── */
  await page.route("**/api/files/mounts/config*", (route) => {
    return route.fulfill({
      json: [
        {
          name: "ml-project",
          local: "/home/user/ml-project",
          remote: "/home/user/ml-project",
        },
      ],
    });
  });
  await page.route("**/api/files/mounts*", (route) => {
    return route.fulfill({
      json: [{ name: "ml-project", remote: "/home/user/ml-project" }],
    });
  });

  /* ── Workflows ─────────────────────────────── */
  await page.route("**/api/workflows/runs/*/cancel", (route) => {
    if (route.request().method() === "POST") {
      return route.fulfill({
        json: { status: "cancelled", run_id: "run-001" },
      });
    }
    return route.continue();
  });

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
    // Fresh runs stay ``pending`` until ``ActiveWatchPoller`` observes
    // a child job in RUNNING (P1-1). The mock used to return ``running``
    // eagerly, which drifted from the real backend contract after the
    // poller-owned-lifecycle fix landed.
    return route.fulfill({
      json: {
        id: "run-001",
        workflow_name: "ml-pipeline",
        started_at: new Date().toISOString(),
        status: "pending",
        job_statuses: {},
      },
    });
  });

  await page.route("**/api/workflows?*", (route) => {
    return route.fulfill({ json: MOCK_WORKFLOWS });
  });
  await page.route("**/api/workflows", (route) => {
    return route.fulfill({ json: MOCK_WORKFLOWS });
  });

  await page.route("**/api/workflows/*", (route) => {
    const url = route.request().url();
    const rawSegment = url.split("/api/workflows/")[1];
    const segment = decodeURIComponent(rawSegment.split("?")[0]);

    /* Let create endpoint through to its own mock */
    if (segment === "create" && route.request().method() === "POST") {
      return route.fallback();
    }

    /* DELETE /api/workflows/:name */
    if (route.request().method() === "DELETE") {
      return route.fulfill({
        json: { status: "deleted", name: segment },
      });
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

  /* ── Templates ─────────────────────────────── */
  await page.route("**/api/templates", (route) => {
    if (route.request().method() === "GET") {
      return route.fulfill({ json: MOCK_TEMPLATES });
    }
    if (route.request().method() === "POST") {
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
    return route.continue();
  });

  await page.route("**/api/templates/*", (route) => {
    if (route.request().method() === "GET") {
      return route.fulfill({ json: MOCK_TEMPLATE_DETAIL });
    }
    if (route.request().method() === "PUT") {
      const body = route.request().postDataJSON();
      return route.fulfill({
        json: {
          ...MOCK_TEMPLATE_DETAIL,
          ...body,
        },
      });
    }
    if (route.request().method() === "DELETE") {
      return route.fulfill({ status: 204 });
    }
    return route.continue();
  });

  /* ── Notifications (endpoints / watches / subscriptions / deliveries) ── */
  await setupNotificationMocks(page);

  /* ── Config (needed for Settings page + submit-dialog notify logic) ── */
  // P3-10: Settings.tsx and FileExplorer both hit ``/api/config`` and
  // ``/api/config/paths`` on mount. Without these mocks the Settings
  // page stays in its loading skeleton forever and no tab buttons
  // appear. Individual tests can still override these routes with
  // their own handlers if they need richer behaviour.
  await page.route("**/api/config", (route) => {
    if (route.request().method() === "PATCH") {
      return route.fulfill({ status: 200, json: { ok: true } });
    }
    return route.fulfill({
      json: {
        resources: {
          nodes: 1,
          gpus_per_node: 0,
          ntasks_per_node: 1,
          cpus_per_task: 1,
          memory_per_node: null,
          time_limit: null,
          nodelist: null,
          partition: null,
        },
        environment: {
          conda: null,
          venv: null,
          container: null,
          env_vars: {},
        },
        notifications: {
          slack_webhook_url: null,
          default_endpoint_name: null,
          default_preset: "terminal",
        },
        log_dir: "logs",
        work_dir: null,
      },
    });
  });
  await page.route("**/api/config/paths", (route) => {
    return route.fulfill({ json: [] });
  });
  await page.route("**/api/config/env", (route) => {
    return route.fulfill({ json: { items: [] } });
  });
  await page.route("**/api/config/projects", (route) => {
    return route.fulfill({ json: [] });
  });
  await page.route("**/api/config/ssh/status*", (route) => {
    return route.fulfill({
      json: { available: false, profile: null, hostname: null },
    });
  });
  await page.route("**/api/config/ssh/profiles*", (route) => {
    return route.fulfill({ json: { profiles: {}, current: null } });
  });
}

/**
 * Mock the notification domain routes with an in-memory store so the
 * NotificationsCenter page and Settings → Notifications tab render
 * against realistic data during E2E runs.
 */
export async function setupNotificationMocks(page: Page) {
  // Per-page in-memory fixture. Not shared across tests — each test gets
  // a fresh copy via setupMockRoutes/setupNotificationMocks.
  const state = {
    endpoints: [
      {
        id: 1,
        kind: "slack_webhook",
        name: "primary",
        config: {
          webhook_url: "https://hooks.slack.com/services/A/B/C",
        },
        created_at: "2026-04-10T00:00:00Z",
        disabled_at: null as string | null,
      },
    ],
    watches: [
      {
        id: 1,
        kind: "job",
        target_ref: "job:10001",
        filter: null as Record<string, unknown> | null,
        created_at: "2026-04-18T12:00:00Z",
        closed_at: null as string | null,
      },
    ],
    subscriptions: [
      {
        id: 1,
        watch_id: 1,
        endpoint_id: 1,
        preset: "terminal",
        created_at: "2026-04-18T12:00:00Z",
      },
    ],
    deliveries: [
      {
        id: 1,
        event_id: 1,
        subscription_id: 1,
        endpoint_id: 1,
        idempotency_key: "job:10001:COMPLETED",
        status: "delivered",
        attempt_count: 1,
        next_attempt_at: "2026-04-18T12:01:00Z",
        leased_until: null,
        worker_id: "delivery-123",
        last_error: null,
        delivered_at: "2026-04-18T12:01:30Z",
        created_at: "2026-04-18T12:01:00Z",
      },
      {
        id: 2,
        event_id: 2,
        subscription_id: 1,
        endpoint_id: 1,
        idempotency_key: "job:10002:FAILED",
        status: "pending",
        attempt_count: 0,
        next_attempt_at: "2026-04-18T12:05:00Z",
        leased_until: null,
        worker_id: null,
        last_error: null,
        delivered_at: null,
        created_at: "2026-04-18T12:05:00Z",
      },
    ],
  };

  // Matches both ``/api/endpoints`` (POST) and ``/api/endpoints?include_disabled=true``
  // (GET with query string). ``/api/endpoints/123`` is handled by the
  // next route below because ``*`` in Playwright globs doesn't cross
  // path separators.
  await page.route("**/api/endpoints?*", (route) => {
    if (route.request().method() === "GET") {
      return route.fulfill({ json: state.endpoints });
    }
    return route.continue();
  });
  await page.route("**/api/endpoints", (route) => {
    if (route.request().method() === "GET") {
      return route.fulfill({ json: state.endpoints });
    }
    if (route.request().method() === "POST") {
      const body = route.request().postDataJSON();
      const newEndpoint = {
        id: state.endpoints.length + 1,
        kind: body.kind,
        name: body.name,
        config: body.config,
        created_at: new Date().toISOString(),
        disabled_at: null,
      };
      state.endpoints.push(newEndpoint);
      return route.fulfill({ status: 201, json: newEndpoint });
    }
    return route.continue();
  });

  // Also matches ``/api/endpoints/<id>/disable`` and ``/enable`` — the
  // single-segment ``*`` doesn't cross ``/``, so we need ``**`` to
  // cover both ``/api/endpoints/1`` and ``/api/endpoints/1/enable``.
  await page.route("**/api/endpoints/**", (route) => {
    const method = route.request().method();
    const url = route.request().url();
    const idMatch = url.match(/\/api\/endpoints\/(\d+)/);
    if (!idMatch) return route.continue();
    const id = Number(idMatch[1]);
    const endpoint = state.endpoints.find((e) => e.id === id);
    if (!endpoint) {
      return route.fulfill({ status: 404, json: { detail: "not found" } });
    }
    if (method === "DELETE") {
      state.endpoints = state.endpoints.filter((e) => e.id !== id);
      return route.fulfill({ status: 204 });
    }
    if (url.endsWith("/disable") && method === "POST") {
      endpoint.disabled_at = new Date().toISOString();
      return route.fulfill({ json: endpoint });
    }
    if (url.endsWith("/enable") && method === "POST") {
      endpoint.disabled_at = null;
      return route.fulfill({ json: endpoint });
    }
    return route.continue();
  });

  await page.route("**/api/watches*", (route) => {
    const url = new URL(route.request().url());
    const openOnly = url.searchParams.get("open") !== "false";
    const rows = openOnly
      ? state.watches.filter((w) => w.closed_at === null)
      : state.watches;
    return route.fulfill({ json: rows });
  });

  await page.route("**/api/subscriptions*", (route) => {
    if (route.request().method() !== "GET") return route.continue();
    const url = new URL(route.request().url());
    const watchId = url.searchParams.get("watch_id");
    const endpointId = url.searchParams.get("endpoint_id");
    const limit = url.searchParams.get("limit");

    // R9: honour scoped filters and limit so tests fail if the
    // frontend ever stops sending them or the router's contract drifts.
    let rows = state.subscriptions;
    if (watchId !== null) {
      rows = rows.filter((s) => s.watch_id === Number(watchId));
    }
    if (endpointId !== null) {
      rows = rows.filter((s) => s.endpoint_id === Number(endpointId));
    }
    if (limit !== null) {
      const n = Number(limit);
      if (!Number.isFinite(n) || n < 1 || n > 500) {
        return route.fulfill({ status: 422, json: { detail: "bad limit" } });
      }
      rows = rows.slice(0, n);
    }
    return route.fulfill({ json: rows });
  });

  await page.route("**/api/deliveries/stuck*", (route) => {
    const pending = state.deliveries.filter((d) => d.status === "pending");
    return route.fulfill({
      json: { count: pending.length, older_than_sec: 300 },
    });
  });

  await page.route("**/api/deliveries*", (route) => {
    if (route.request().method() !== "GET") return route.continue();
    const url = new URL(route.request().url());
    const status = url.searchParams.get("status");
    const subscriptionId = url.searchParams.get("subscription_id");
    const limit = url.searchParams.get("limit");

    // R9: enforce contracts. ``limit`` is validated to [1, 500] on
    // the backend; surface that here so a regression in either side
    // surfaces in E2E. ``subscription_id`` switches to the scoped
    // list-by-subscription code path.
    if (limit !== null) {
      const n = Number(limit);
      if (!Number.isFinite(n) || n < 1 || n > 500) {
        return route.fulfill({ status: 422, json: { detail: "bad limit" } });
      }
    }

    let rows = state.deliveries;
    if (subscriptionId !== null) {
      rows = rows.filter((d) => d.subscription_id === Number(subscriptionId));
    }
    if (status) {
      rows = rows.filter((d) => d.status === status);
    }
    if (limit !== null) {
      rows = rows.slice(0, Number(limit));
    }
    return route.fulfill({ json: rows });
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
