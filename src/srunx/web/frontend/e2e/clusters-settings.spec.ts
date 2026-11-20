/**
 * E2E coverage for cluster / project selection and the cluster-aware
 * Settings tabs (Clusters, General, Project).
 *
 * A small in-memory backend stands in for the FastAPI server so the tests
 * can assert on what the UI actually sends: the ``?cluster=`` /
 * ``?project=`` parameters on every request, the PATCH bodies written to
 * the config files (with their version tokens), and the 409 handling.
 */

import { test, expect, type Page, type Request } from "@playwright/test";
import { setupMockRoutes } from "./fixtures/mock-routes.ts";

type Json = Record<string, unknown>;

type Backend = {
  clusters: Json[];
  defaultCluster: string | null;
  userFile: Json;
  projectFile: Json;
  userVersion: number;
  projectVersion: number;
  patches: Json[];
  requests: string[];
  conflictNext: boolean;
};

function newBackend(): Backend {
  return {
    clusters: [
      { name: "local", transport: "local", port: 22 },
      { name: "gpu", transport: "ssh", host: "gpu-login", port: 22 },
    ],
    defaultCluster: "local",
    userFile: {
      default_cluster: "local",
      clusters: {
        gpu: { defaults: { resources: { partition: "a100" } } },
      },
    },
    projectFile: { name: "proj", cluster: "gpu", clusters: { gpu: {} } },
    userVersion: 1,
    projectVersion: 1,
    patches: [],
    requests: [],
    conflictNext: false,
  };
}

function setIn(obj: Json, dotted: string, value: unknown) {
  const parts = dotted.split(".");
  let node = obj;
  for (const p of parts.slice(0, -1)) {
    if (typeof node[p] !== "object" || node[p] === null) node[p] = {};
    node = node[p] as Json;
  }
  node[parts[parts.length - 1]] = value;
}

function deleteIn(obj: Json, dotted: string) {
  const parts = dotted.split(".");
  let node: Json | undefined = obj;
  for (const p of parts.slice(0, -1)) {
    node = node?.[p] as Json | undefined;
  }
  if (node) delete node[parts[parts.length - 1]];
}

function effective(b: Backend, cluster: string | null) {
  const partition =
    cluster === "gpu"
      ? (
          (
            (b.userFile.clusters as Json)?.gpu as Json | undefined
          )?.defaults as Json | undefined
        )?.resources
      : undefined;
  const p = (partition as Json | undefined)?.partition ?? null;
  return {
    cluster,
    project: "proj",
    project_root: "/work/proj",
    values: {
      resources: {
        nodes: 1,
        gpus_per_node: 0,
        ntasks_per_node: 1,
        cpus_per_task: 1,
        memory_per_node: null,
        time_limit: null,
        nodelist: null,
        partition: p,
      },
      environment: { conda: null, venv: null, container: null, env_vars: {} },
      notifications: { default_endpoint_name: null, default_preset: "terminal" },
      sync: {
        auto: true,
        lock_timeout_seconds: 120,
        warn_dirty: true,
        require_clean: false,
        owner_check: true,
        verify_remote_hash: false,
      },
      log_dir: "logs",
      work_dir: null,
    },
    sources: p
      ? { "resources.partition": "/home/u/.config/srunx/config.toml [clusters.gpu]" }
      : {},
  };
}

async function installBackend(page: Page, b: Backend) {
  page.on("request", (req: Request) => {
    const url = new URL(req.url());
    if (url.pathname.startsWith("/api/")) b.requests.push(url.pathname + url.search);
  });

  await page.route(/\/api\/clusters(\?.*)?$/, async (route) => {
    const req = route.request();
    if (req.method() === "POST") {
      const body = req.postDataJSON() as Json;
      b.clusters.push({ port: 22, ...body });
      if (body.make_default) b.defaultCluster = body.name as string;
      return route.fulfill({ json: body });
    }
    return route.fulfill({
      json: { default: b.defaultCluster, clusters: b.clusters },
    });
  });
  await page.route(/\/api\/clusters\/[^/?]+\/test(\?.*)?$/, (route) =>
    route.fulfill({
      json: { ok: true, transport: "ssh", detail: "connected", home: "/home/u" },
    }),
  );
  await page.route(/\/api\/clusters\/[^/?]+(\?.*)?$/, async (route) => {
    const req = route.request();
    const name = decodeURIComponent(
      new URL(req.url()).pathname.split("/").pop() ?? "",
    );
    if (req.method() === "DELETE") {
      b.clusters = b.clusters.filter((c) => c.name !== name);
      return route.fulfill({ json: { ok: true } });
    }
    return route.fallback();
  });
  await page.route(/\/api\/projects(\?.*)?$/, (route) =>
    route.fulfill({
      json: [
        {
          name: "proj",
          root: "/work/proj",
          config_path: "/work/proj/.srunx/config.toml",
          cluster: "gpu",
        },
      ],
    }),
  );
  await page.route(/\/api\/projects\/proj(\?.*)?$/, (route) =>
    route.fulfill({
      json: {
        name: "proj",
        root: "/work/proj",
        config_path: "/work/proj/.srunx/config.toml",
        cluster: "gpu",
        mounts: {
          gpu: [
            {
              name: "proj",
              local: "/work/proj",
              remote:
                ((b.projectFile.clusters as Json).gpu as Json).remote_dir ??
                "/home/u/.local/share/srunx/workspaces/proj-1234",
              sync: true,
              exclude: [],
            },
          ],
        },
      },
    }),
  );
  await page.route(/\/api\/config\/paths(\?.*)?$/, (route) =>
    route.fulfill({
      json: [
        { path: "/etc/srunx/config.toml", exists: false, source: "system" },
        { path: "/home/u/.config/srunx/config.toml", exists: true, source: "user" },
      ],
    }),
  );
  await page.route(/\/api\/config\/env(\?.*)?$/, (route) =>
    route.fulfill({ json: [] }),
  );
  await page.route(/\/api\/config(\?.*)?$/, (route) => {
    const cluster = new URL(route.request().url()).searchParams.get("cluster");
    return route.fulfill({ json: effective(b, cluster) });
  });
  await page.route(/\/api\/config\/file(\?.*)?$/, async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    if (req.method() === "PATCH") {
      const body = req.postDataJSON() as Json;
      b.patches.push(body);
      const scope = body.scope as string;
      if (b.conflictNext) {
        b.conflictNext = false;
        return route.fulfill({
          status: 409,
          json: { detail: "config.toml was modified by someone else" },
        });
      }
      const file = scope === "user" ? b.userFile : b.projectFile;
      for (const [k, v] of Object.entries((body.set as Json) ?? {})) setIn(file, k, v);
      for (const k of (body.delete as string[]) ?? []) deleteIn(file, k);
      if (scope === "user") b.userVersion += 1;
      else b.projectVersion += 1;
    }
    const scope =
      req.method() === "PATCH"
        ? ((req.postDataJSON() as Json).scope as string)
        : (url.searchParams.get("scope") ?? "user");
    const user = scope === "user";
    return route.fulfill({
      json: {
        scope,
        path: user
          ? "/home/u/.config/srunx/config.toml"
          : "/work/proj/.srunx/config.toml",
        exists: true,
        version: `v${user ? b.userVersion : b.projectVersion}`,
        data: user ? b.userFile : b.projectFile,
      },
    });
  });
}

let backend: Backend;

/** Wait until the General tab has loaded both the effective config and the file. */
async function waitForGeneral(page: Page) {
  await expect(
    page.getByText("/home/u/.config/srunx/config.toml [clusters.gpu]").first(),
  ).toBeVisible();
  await expect(page.getByText(/\[defaults\]/)).toBeVisible();
}

test.beforeEach(async ({ page }) => {
  // Start every test from an empty selection (once per test, not on
  // every navigation).
  await page.addInitScript(() => {
    if (!sessionStorage.getItem("e2e-cleared")) {
      localStorage.clear();
      sessionStorage.setItem("e2e-cleared", "1");
    }
  });
  await setupMockRoutes(page);
  backend = newBackend();
  await installBackend(page, backend);
});

test.describe("cluster / project selection", () => {
  test("defaults to the project's cluster and tags API requests", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.getByTestId("cluster-switcher")).toContainText("gpu");
    await expect(page.getByTestId("project-switcher")).toContainText("proj");
    await expect
      .poll(() =>
        backend.requests.some(
          (r) =>
            r.startsWith("/api/jobs") &&
            r.includes("cluster=gpu") &&
            r.includes("project=proj"),
        ),
      )
      .toBe(true);
    // Inventory requests never carry a (possibly stale) selection.
    expect(
      backend.requests.filter((r) => r.startsWith("/api/clusters")),
    ).toContain("/api/clusters?cluster=&project=");
  });

  test("switching cluster changes the parameter on later requests", async ({
    page,
  }) => {
    await page.goto("/");
    await page.getByTestId("cluster-switcher").click();
    await page
      .locator('[data-selection-popover="cluster"]')
      .getByText("local", { exact: true })
      .click();
    await expect(page.getByTestId("cluster-switcher")).toContainText("local");
    const before = backend.requests.length;
    // Client-side navigation (a reload would re-run the test's
    // localStorage reset).
    await page.getByRole("link", { name: "Jobs" }).click();
    await expect
      .poll(() =>
        backend.requests
          .slice(before)
          .some((r) => r.startsWith("/api/jobs") && r.includes("cluster=local")),
      )
      .toBe(true);
  });

  test("a stale stored project is repaired instead of breaking the UI", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      localStorage.setItem("srunx.selection.cluster", "gone");
      localStorage.setItem("srunx.selection.project", "gone");
    });
    await page.goto("/");
    await expect(page.getByTestId("project-switcher")).toContainText("proj");
    await expect(page.getByTestId("cluster-switcher")).toContainText("gpu");
    expect(backend.requests.some((r) => r.includes("project=gone"))).toBe(false);
  });
});

test.describe("Settings → Clusters", () => {
  test("lists clusters and adds a new one", async ({ page }) => {
    await page.goto("/settings?tab=clusters");
    await expect(page.getByText("gpu-login")).toBeVisible();
    await page.getByRole("button", { name: /Add Cluster/ }).click();
    await page.getByLabel("Name", { exact: true }).fill("hr");
    await page.getByLabel("Host alias").fill("hr-login");
    await page.getByRole("button", { name: "Save" }).click();
    await expect.poll(() => backend.clusters.map((c) => c.name)).toContain("hr");
    const hr = backend.clusters.find((c) => c.name === "hr");
    expect(hr).toMatchObject({ transport: "ssh", host: "hr-login" });
  });

  test("tests a connection", async ({ page }) => {
    await page.goto("/settings?tab=clusters");
    await page.getByTitle("Test connection").nth(1).click();
    await expect(page.getByText(/OK — connected/)).toBeVisible();
  });
});

test.describe("Settings → General", () => {
  test("shows effective values with their source file", async ({ page }) => {
    await page.goto("/settings?tab=general");
    await expect(
      page.getByText("/home/u/.config/srunx/config.toml [clusters.gpu]").first(),
    ).toBeVisible();
  });

  test("saves at the selected scope with the version token", async ({ page }) => {
    await page.goto("/settings?tab=general");
    await waitForGeneral(page);
    await page.getByLabel("Config scope").selectOption({ index: 1 });
    await expect(page.getByText(/\[clusters\.gpu\.defaults\]/)).toBeVisible();
    await page.getByLabel("Time Limit").fill("04:00:00");
    await page.getByRole("button", { name: "Save" }).click();
    await expect.poll(() => backend.patches.length).toBe(1);
    const patch = backend.patches[0];
    expect(patch.scope).toBe("user");
    expect(patch.version).toBe("v1");
    expect(patch.set).toEqual({
      "clusters.gpu.defaults.resources.time_limit": "04:00:00",
    });
  });

  test("a version conflict reloads instead of overwriting", async ({ page }) => {
    await page.goto("/settings?tab=general");
    await waitForGeneral(page);
    backend.conflictNext = true;
    await page.getByLabel("Partition").fill("h100");
    await page.getByRole("button", { name: "Save" }).click();
    await expect(page.getByText(/changed on disk since it was loaded/)).toBeVisible();
    expect(backend.userVersion).toBe(1);
  });
});

test.describe("Settings → Project", () => {
  test("edits the per-cluster remote directory", async ({ page }) => {
    await page.goto("/settings?tab=project");
    await expect(page.getByLabel("Project cluster")).toHaveValue("gpu");
    const panel = page.getByTestId("project-cluster-gpu");
    await panel.getByLabel("Remote directory (gpu)").fill("/scratch/proj");
    await panel.getByRole("button", { name: "Save" }).click();
    await expect.poll(() => backend.patches.length).toBe(1);
    expect(backend.patches[0]).toMatchObject({
      scope: "project",
      set: { "clusters.gpu.remote_dir": "/scratch/proj" },
    });
    await expect(panel.getByText("/scratch/proj").first()).toBeVisible();
  });
});

test("Settings → Project: a version conflict is reported", async ({ page }) => {
  await page.goto("/settings?tab=project");
  const panel = page.getByTestId("project-cluster-gpu");
  await expect(panel.getByLabel("Remote directory (gpu)")).toBeVisible();
  backend.conflictNext = true;
  await panel.getByLabel("Remote directory (gpu)").fill("/scratch/proj");
  await panel.getByRole("button", { name: "Save" }).click();
  await expect(
    page.getByText(/project config changed on disk since it was loaded/),
  ).toBeVisible();
  expect(backend.projectVersion).toBe(1);
});
