/**
 * Recording spec for the README sbatch-submission demo GIF.
 *
 * All API calls are mocked so nothing real is hit. An injected cursor +
 * spotlight overlay runs on top of the live UI, so the demo reflects the
 * actual look and feel of the web app instead of a stylised mock.
 *
 * Run via scripts/record_demo.sh — the resulting .webm is converted to GIF
 * by that script.
 */
import { test, expect, type Page } from "@playwright/test";

const VIEWPORT = { width: 1280, height: 800 };

test.use({
  viewport: VIEWPORT,
  video: { mode: "on", size: VIEWPORT },
});

const SCRIPT_CONTENT = `#!/bin/bash
#SBATCH --job-name=train_bert
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --time=04:00:00
#SBATCH --mem=32G
#SBATCH --output=logs/%j.out

source activate ml_env

python train.py \\
  --model bert-base-uncased \\
  --epochs 10 \\
  --batch-size 32 \\
  --lr 2e-5
`;

type BrowseEntry = {
  name: string;
  type: "directory" | "file" | "symlink";
  size?: number;
  accessible?: boolean;
  target_kind?: "directory" | "file";
};

const ROOT_ENTRIES: BrowseEntry[] = [
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
  { name: "train_bert.sh", type: "file", size: SCRIPT_CONTENT.length },
  { name: "uv.lock", type: "file", size: 52100 },
];

async function setupDemoRoutes(page: Page) {
  await page.route("**/api/files/mounts/config*", (route) =>
    route.fulfill({
      json: [
        {
          name: "ml-project",
          local: "/home/demo/ml-project",
          remote: "/home/demo/ml-project",
        },
      ],
    }),
  );

  await page.route("**/api/files/mounts*", (route) =>
    route.fulfill({
      json: [{ name: "ml-project", remote: "/home/demo/ml-project" }],
    }),
  );

  await page.route("**/api/files/browse*", (route) =>
    route.fulfill({ json: { entries: ROOT_ENTRIES } }),
  );

  await page.route("**/api/files/read*", (route) =>
    route.fulfill({ json: { content: SCRIPT_CONTENT } }),
  );

  await page.route("**/api/files/sync*", (route) =>
    route.fulfill({ json: { synced: 0, deleted: 0 } }),
  );

  await page.route("**/api/config*", (route) =>
    route.fulfill({
      json: { notifications: { slack_webhook_url: null } },
    }),
  );

  await page.route("**/api/jobs", (route) => {
    if (route.request().method() === "POST") {
      return route.fulfill({
        json: { job_id: 847291, name: "train_bert", status: "PENDING" },
      });
    }
    return route.fulfill({ json: [] });
  });

  await page.route("**/api/resources*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/history*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/workflows*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/templates*", (route) => route.fulfill({ json: [] }));
}

/**
 * Injects a synthetic cursor + a "spotlight" overlay that dims everything
 * outside a focused rectangle. Both are driven from the test via
 * page.evaluate calls.
 */
async function installOverlay(page: Page) {
  await page.addStyleTag({
    content: `
      #__demo_cursor, #__demo_spotlight {
        position: fixed;
        pointer-events: none;
        z-index: 2147483647;
      }
      #__demo_cursor {
        width: 22px; height: 22px;
        left: 0; top: 0;
        transform: translate(-4px, -2px);
        transition: transform 0.05s linear;
      }
      #__demo_cursor svg { filter: drop-shadow(0 2px 3px rgba(0,0,0,0.5)); }
      #__demo_cursor.clicking { transform: translate(-4px, -2px) scale(0.88); }
      #__demo_click_ring {
        position: fixed;
        pointer-events: none;
        z-index: 2147483646;
        width: 40px; height: 40px;
        border-radius: 50%;
        border: 2px solid rgba(129,140,248,0.9);
        opacity: 0;
        transform: translate(-50%, -50%) scale(0.4);
      }
      #__demo_click_ring.go {
        animation: demo-ring 0.5s ease-out forwards;
      }
      @keyframes demo-ring {
        0%   { opacity: 0.9; transform: translate(-50%,-50%) scale(0.4); }
        100% { opacity: 0;   transform: translate(-50%,-50%) scale(1.6); }
      }
      #__demo_spotlight {
        left: 0; top: 0; right: 0; bottom: 0;
        background: rgba(4,6,14,0.55);
        opacity: 0;
        transition: opacity 0.25s ease, clip-path 0.35s ease;
        clip-path: polygon(0 0, 0 0, 0 0, 0 0);
      }
      #__demo_spotlight.active { opacity: 1; }
    `,
  });

  await page.evaluate(() => {
    const cursor = document.createElement("div");
    cursor.id = "__demo_cursor";
    cursor.innerHTML = `
      <svg width="22" height="22" viewBox="0 0 22 22" xmlns="http://www.w3.org/2000/svg">
        <path d="M3 2 L3 17 L7.5 13 L10 19 L13 17.8 L10.5 11.8 L16 11.5 Z"
              fill="white" stroke="#111" stroke-width="1.2" stroke-linejoin="round"/>
      </svg>`;
    document.body.appendChild(cursor);

    const ring = document.createElement("div");
    ring.id = "__demo_click_ring";
    document.body.appendChild(ring);

    const spot = document.createElement("div");
    spot.id = "__demo_spotlight";
    document.body.appendChild(spot);
  });
}

async function moveCursor(
  page: Page,
  x: number,
  y: number,
  duration = 600,
): Promise<void> {
  await page.evaluate(
    async ({ x, y, duration }) => {
      const el = document.getElementById("__demo_cursor");
      if (!el) return;
      const start = performance.now();
      const m = el.style.transform.match(
        /translate\((-?[\d.]+)px,\s*(-?[\d.]+)px\)/,
      );
      const sx = m ? parseFloat(m[1]) : 0;
      const sy = m ? parseFloat(m[2]) : 0;
      const ease = (t: number) => t * t * (3 - 2 * t);
      return new Promise<void>((resolve) => {
        const step = (now: number) => {
          const t = Math.min(1, (now - start) / duration);
          const e = ease(t);
          const cx = sx + (x - sx) * e;
          const cy = sy + (y - sy) * e;
          el.style.transform = `translate(${cx}px, ${cy}px)`;
          if (t < 1) requestAnimationFrame(step);
          else resolve();
        };
        requestAnimationFrame(step);
      });
    },
    { x, y, duration },
  );
}

async function clickBurst(page: Page, x: number, y: number) {
  await page.evaluate(
    ({ x, y }) => {
      const cursor = document.getElementById("__demo_cursor");
      const ring = document.getElementById("__demo_click_ring");
      if (!cursor || !ring) return;
      cursor.classList.add("clicking");
      ring.style.left = `${x}px`;
      ring.style.top = `${y}px`;
      ring.classList.remove("go");
      void ring.offsetWidth;
      ring.classList.add("go");
      setTimeout(() => cursor.classList.remove("clicking"), 120);
    },
    { x, y },
  );
}

async function focusOn(page: Page, selector: string | null, padding = 14) {
  if (!selector) {
    await page.evaluate(() => {
      document.getElementById("__demo_spotlight")?.classList.remove("active");
    });
    return;
  }
  const box = await page.locator(selector).first().boundingBox();
  if (!box) return;
  await page.evaluate(
    ({ box, padding }) => {
      const spot = document.getElementById("__demo_spotlight");
      if (!spot) return;
      const x0 = Math.max(0, box.x - padding);
      const y0 = Math.max(0, box.y - padding);
      const x1 = box.x + box.width + padding;
      const y1 = box.y + box.height + padding;
      spot.style.clipPath = `polygon(
        0 0, 100% 0, 100% 100%, 0 100%, 0 0,
        ${x0}px ${y0}px,
        ${x0}px ${y1}px,
        ${x1}px ${y1}px,
        ${x1}px ${y0}px,
        ${x0}px ${y0}px
      )`;
      spot.classList.add("active");
    },
    { box, padding },
  );
}

async function moveToElement(page: Page, selector: string, duration = 600) {
  const box = await page.locator(selector).first().boundingBox();
  if (!box) throw new Error(`no box for ${selector}`);
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  await moveCursor(page, x, y, duration);
  return { x, y };
}

async function clickAt(page: Page, selector: string) {
  const box = await page.locator(selector).first().boundingBox();
  if (!box) throw new Error(`no box for ${selector}`);
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  await clickBurst(page, x, y);
  await page.locator(selector).first().click();
  return { x, y };
}

test("record sbatch submission demo", async ({ page }) => {
  await setupDemoRoutes(page);
  await page.goto("/");
  await installOverlay(page);

  // Start cursor off-screen-ish near bottom-right.
  await moveCursor(page, VIEWPORT.width - 60, VIEWPORT.height - 60, 0);
  await page.waitForTimeout(500);

  // 1. Click Explorer in sidebar (NavLink → anchor)
  const explorerToggle = 'aside a:has-text("Explorer")';
  await focusOn(page, explorerToggle);
  await moveToElement(page, explorerToggle, 800);
  await page.waitForTimeout(250);
  await clickAt(page, explorerToggle);
  await page.waitForTimeout(400);
  await focusOn(page, null);

  // Wait for Explorer to populate
  await expect(page.locator("text=ml-project").first()).toBeVisible({
    timeout: 5000,
  });
  await page.waitForTimeout(300);

  // 2. Expand mount if needed — click the mount header
  const mountHeader = 'button:has-text("ml-project")';
  if (await page.locator(mountHeader).first().isVisible()) {
    const entriesCount = await page.locator('text="train_bert.sh"').count();
    if (entriesCount === 0) {
      await moveToElement(page, mountHeader, 600);
      await clickAt(page, mountHeader);
      await page.waitForTimeout(500);
    }
  }

  // 3. Hover & select the script file (left-click opens preview)
  const scriptRow = 'text="train_bert.sh"';
  await expect(page.locator(scriptRow).first()).toBeVisible();
  await focusOn(page, scriptRow, 8);
  await moveToElement(page, scriptRow, 800);
  await page.waitForTimeout(300);
  await clickAt(page, scriptRow);
  await page.waitForTimeout(600);
  await focusOn(page, null);

  // Wait for file viewer content
  await expect(page.locator("text=SBATCH --job-name=train_bert")).toBeVisible({
    timeout: 5000,
  });
  await page.waitForTimeout(500);

  // 4. Click the "submit as sbatch" play button on the train_bert.sh row
  const submitBtn =
    'div:has(> span:text-is("train_bert.sh")) > button[title="Submit as sbatch"]';
  await focusOn(page, submitBtn, 10);
  await moveToElement(page, submitBtn, 700);
  await page.waitForTimeout(300);
  await clickAt(page, submitBtn);
  await page.waitForTimeout(500);
  await focusOn(page, null);

  // 5. Dialog appears — focus it
  const dialog = 'div:has(> div > h3:has-text("Submit Job"))';
  await expect(page.locator('h3:has-text("Submit Job")')).toBeVisible();
  await focusOn(page, dialog, 16);
  await page.waitForTimeout(700);

  // 6. Move cursor to Submit button and click
  const finalSubmit = 'button.btn-primary:has-text("Submit")';
  await moveToElement(page, finalSubmit, 900);
  await page.waitForTimeout(350);
  await clickAt(page, finalSubmit);
  await page.waitForTimeout(300);

  // 7. Wait for success state
  await expect(page.locator("text=Job submitted")).toBeVisible({
    timeout: 5000,
  });
  await page.waitForTimeout(1400);
  await focusOn(page, null);
  await page.waitForTimeout(600);
});
