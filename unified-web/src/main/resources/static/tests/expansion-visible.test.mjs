/**
 * Row-expansion visibility — the regression guard for the "thin white bar".
 *
 * <h2>The bug this locks down</h2>
 *
 * The void fix bounded every Details table card with a max-height, so a SHORT list
 * stays compact and a LONG one scrolls inside the card. That same cap applied to the
 * panel a row-click opens, because the panel lives inside the card: an expanded row
 * was cut to a sliver of the card — a thin empty strip instead of the access-details /
 * usage rows, even though the API had returned rich data and the renderer had run.
 *
 * The fix drops the cap ONLY while a row is open
 * (`:has(.to-detail-row.open) { max-height: none }`), so the panel sizes to its
 * content and the Details pane — already the scroller — reveals all of it. The
 * compact short-list behaviour is untouched, because the exemption does not match
 * until a row is expanded.
 *
 * <h2>Two layers</h2>
 *
 * 1. Static + markup — the stylesheet must keep BOTH the cap (void fix) and the
 *    exemption (expansions), and the real renderers must emit the panel and its rows.
 *    This runs everywhere, with no dependencies.
 * 2. Live measurement — when a browser is available, the real stylesheets are applied
 *    to the real DOM shape, a row is opened, and the panel's height is measured: it
 *    must exceed a threshold and must not be clipped by the container. The test also
 *    re-applies the old cap and proves the panel WOULD be clipped — so a regression
 *    that re-adds the clamp fails the measurement rather than passing silently.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
import { readdirSync } from 'node:fs';
import { ok, includes, excludes } from './lib/assert.mjs';
import { installDom, esc } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

/* ───────────────────────── 1. stylesheet guards ───────────────────────── */

export async function test_expansion_cap_is_lifted_while_a_row_is_open() {
  const estate = await read('parser/css/estate.css');
  const tableOps = await read('parser/css/table-ops.css');

  // The void fix's caps MUST stay — the exemption is additive, not a removal.
  includes(estate, 'max-height: calc(100vh - 230px);',
    'the Details container cap (the void fix) was removed — short lists will stretch again');
  includes(tableOps, '#viewDetails .dt-subview.fit-subview .to-container { flex: none; max-height: 60vh; }',
    'the fit-subview container cap (the void fix) was removed');

  // …and both containers must drop the cap when a row-expansion is open.
  includes(estate, '#viewDetails .dt-subview .to-container:has(.to-detail-row.open)',
    'an open expansion inside a Details container is not exempted from the cap — the usage panel will be clipped');
  includes(estate, 'max-height: none;',
    'the Details container exemption must set max-height: none');
  includes(tableOps, '#viewDetails .dt-subview.fit-subview .to-container:has(.to-detail-row.open)',
    'an open expansion inside a fit-subview container is not exempted from the cap');
  includes(tableOps, 'max-height: none;',
    'the fit-subview container exemption must set max-height: none');

  // The exemption must key on the OPEN state, never on the row class alone, or the
  // cap would never apply and the void fix would be undone.
  excludes(estate, '.to-container:has(.to-detail-row) {',
    'the exemption must require .open, not merely a detail row');
  includes(estate, ':has(.to-detail-row.open)',
    'the exemption must match only an OPEN detail row');
}

/* ───────────────────────── 2. the rows actually render ───────────────────────── */

/** Loads the real shared modules into a PA namespace, like the estate-facets tests do. */
async function loadShared() {
  const h = installDom();
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  if (!globalThis.Esc) new Function(await read('shared/escape.js'))();
  PA.esc = (s) => Esc.html(s);
  PA.escJs = (s) => Esc.js(s);
  PA.escAttr = (s) => Esc.attr(s);
  new Function(await read('shared/usage-table.js'))();
  new Function(await read('shared/access-details.js'))();
  return h;
}

export async function test_usage_table_renders_its_rows_inline() {
  await loadShared();
  const rows = [
    { nodeName: 'ERP_USP_RUN_CYCLE', operations: { SELECT: [41, 79] }, nodeId: 'S$P1' },
    { nodeName: 'ERP_USP_INIT_MRP_RUN', operations: { UPDATE: [12] }, nodeId: 'S$P2' }
  ];
  const html = PA.usageTable.render('ut-exp', rows, {
    title: 'Used By', countLabel: 'usages',
    name: (r) => r.nodeName,
    badges: (r) => Object.keys(r.operations)
  });
  includes(html, 'class="ut-panel"', 'the usage panel wrapper is missing');
  includes(html, 'id="ut-exp-tbody"', 'the usage table body is missing');
  // The first page is rendered inline, so the panel is populated before mount runs.
  includes(html, 'ut-row', 'no usage row was rendered into the panel');
  const rendered = (html.match(/class="ut-row/g) || []).length;
  ok(rendered >= rows.length,
    `expected at least ${rows.length} rendered usage rows, got ${rendered} — the panel would be an empty strip`);
  includes(html, 'ERP_USP_RUN_CYCLE', 'the accessed procedure name did not render');
}

export async function test_table_row_expansion_emits_the_access_details_panel() {
  await loadShared();
  const PA = globalThis.PA;
  new Function(await read('parser/js/tableOps.js'))();
  ok(PA.tableOps && PA.tableOps._renderDetail, 'tableOps._renderDetail must load');

  const table = {
    tableName: 'erp_product', tableSchema: 'erp',
    usedBy: [
      { nodeId: 'S$P1', nodeName: 'ERP_USP_RUN_CYCLE', operations: { SELECT: [41, 79] } },
      { nodeId: 'S$P2', nodeName: 'ERP_USP_INIT_MRP_RUN', operations: { UPDATE: [12] } }
    ],
    triggers: []
  };
  const html = PA.tableOps._renderDetail(table, 0, esc);
  includes(html, 'class="to-detail"', 'the expansion wrapper is missing');
  includes(html, 'Access Details (3)', 'the access-details panel did not render its rows count');
  includes(html, 'id="ad-p-0"', 'the access-details table is missing');
  includes(html, 'id="ad-p-0-tbody"', 'the access-details table body is missing — nothing for mount to fill');
  // A table with no triggers must not fabricate a trigger usage panel.
  excludes(html, 'ut-panel', 'a trigger usage panel was emitted for a table with no triggers');
}

/* ───────────────────────── 3. live measurement (optional browser) ───────────────────────── */

/** Finds Playwright if it is installed, so the measurement can run; otherwise null (test skips). */
async function loadPlaywright() {
  const candidates = ['playwright'];
  try {
    const npx = path.join(process.env.HOME || '', '.npm', '_npx');
    for (const dir of readdirSync(npx)) {
      candidates.push(pathToFileURL(path.join(npx, dir, 'node_modules', 'playwright', 'index.mjs')).href);
    }
  } catch { /* no npx cache — fine */ }
  for (const spec of candidates) {
    try { return await import(spec); } catch { /* try next */ }
  }
  return null;
}

export async function test_open_expansion_is_not_clipped_by_the_container() {
  const pw = await loadPlaywright();
  if (!pw) return;   // no browser here — the static guards above are the floor

  const css = (await Promise.all([
    'parser/css/variables.css', 'parser/css/base.css', 'parser/css/join-summary.css',
    'parser/css/right-panel.css', 'parser/css/table-ops.css', 'parser/css/ui-polish.css',
    'parser/css/estate.css'
  ].map(read))).join('\n');

  const html = `<!doctype html><html><head><style>${css}</style></head><body style="margin:0">
    <div class="right-content" style="position:relative;height:100vh">
      <div class="right-view active" id="viewDetails">
        <div class="dt-subview active" id="dv">
          <div class="to-container" id="c">
            <table class="to-table"><thead><tr><th>Table</th></tr></thead><tbody>
              <tr class="to-row"><td>erp_product</td></tr>
              <tr class="to-detail-row open" id="d" style="display:table-row"><td colspan="20">
                <div class="to-detail" id="panel" style="height:600px">usage rows here</div>
              </td></tr>
            </tbody></table>
          </div>
        </div>
      </div>
    </div></body></html>`;

  let browser;
  try {
    browser = await pw.chromium.launch();
  } catch {
    return;   // browser binaries unavailable — skip the live layer
  }
  try {
    const page = await browser.newPage({ viewport: { width: 1200, height: 500 } });
    await page.setContent(html, { waitUntil: 'load' });
    const measure = () => page.evaluate(() => {
      const c = document.getElementById('c');
      const panel = document.getElementById('panel');
      const cr = c.getBoundingClientRect(), pr = panel.getBoundingClientRect();
      return {
        containerMaxHeight: getComputedStyle(c).maxHeight,
        containerHeight: Math.round(cr.height),
        panelHeight: Math.round(pr.height),
        visible: Math.round(Math.max(0, Math.min(pr.bottom, cr.bottom) - Math.max(pr.top, cr.top)))
      };
    });

    const fixed = await measure();
    ok(fixed.containerMaxHeight === 'none',
      `with a row open the container must not cap its height, got max-height: ${fixed.containerMaxHeight}`);
    ok(fixed.panelHeight >= 600,
      `the expansion must size to its content (600px), got ${fixed.panelHeight}px`);
    ok(fixed.visible >= 600,
      `the expanded panel is clipped to ${fixed.visible}px of ${fixed.panelHeight}px — this is the thin white bar`);

    // Prove the guard bites: remove the exemption rules (the pre-fix stylesheet) and
    // confirm the panel WOULD be cut to the container's cap.
    await page.evaluate(() => {
      for (const sheet of document.styleSheets) {
        let rules;
        try { rules = sheet.cssRules; } catch { continue; }
        for (let i = rules.length - 1; i >= 0; i--) {
          const sel = rules[i].selectorText;
          if (sel && sel.includes(':has(.to-detail-row.open)')) sheet.deleteRule(i);
        }
      }
    });
    const clipped = await measure();
    ok(clipped.visible < fixed.visible,
      'the regression check failed to reproduce the clip — the measurement cannot detect the bug');
  } finally {
    await browser.close();
  }
}
