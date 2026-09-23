/**
 * UI-024 (§160) — `leftPanel.js`'s `_renderLeftProcedures` used to call `PA.nodeIndex.all()`
 * unconditionally on every analysis load, regardless of procedure count, before
 * `PA._LP_SERVER_FILTER_THRESHOLD` (MID-002, §39) was ever consulted — the same "whole array
 * resident just to paint the first screen" defect the filter path itself had before MID-002, one
 * call earlier in the sequence. Fixed: at or above the threshold, the initial render fetches a
 * server page (the same `PA.api.searchProcedures('', ...)` route the filter path already uses for
 * an empty query) instead of touching `PA.nodeIndex` at all. Below the threshold, behavior is
 * unchanged — the array path this file's own header comment was measured against.
 *
 * No real corpus at threshold scale exists in this repo (52 procedures on the real analysis run
 * this session), so this is calibrated by stubbing `PA.analysisData.totalNodes` on both sides of
 * the threshold rather than needing 1,000+ real procedures.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function stubEl() {
  return {
    innerHTML: '',
    style: {},
    value: '',
    querySelector: () => null,
    insertAdjacentHTML: function () {} // _lpRenderBatch's "show more" append — content not inspected here
  };
}

function stubDom() {
  const els = {};
  return {
    getElementById: (id) => {
      if (!els[id]) els[id] = stubEl();
      return els[id];
    },
    querySelector: () => null,
    querySelectorAll: () => []
  };
}

async function loadLeftPanel(totalNodes, searchProceduresImpl) {
  globalThis.window = globalThis;
  globalThis.document = stubDom();
  globalThis.PA = globalThis.PA || {};
  const nodeIndexCalls = { all: 0, count: 0 };
  globalThis.PA.nodeIndex = {
    all: () => { nodeIndexCalls.all++; return []; },
    count: () => { nodeIndexCalls.count++; return 0; },
    ensureLoaded: async () => []
  };
  globalThis.PA.analysisData = { totalNodes, _packageAnalysis: false };
  globalThis.PA.escAttr = (s) => s;
  globalThis.PA.escJs = (s) => s;
  globalThis.PA.esc = (s) => s;
  globalThis.PA.getSchemaColor = () => ({ bg: '#fff', fg: '#000' });
  const searchCalls = [];
  globalThis.PA.api = {
    searchProcedures: async (q, limit) => { searchCalls.push([q, limit]); return searchProceduresImpl ? searchProceduresImpl(q, limit) : []; }
  };
  const src = await read('parser/js/leftPanel.js');
  new Function(src)();
  return { nodeIndexCalls, searchCalls };
}

/**
 * Runs `fn` with `setTimeout` replaced by an immediate (synchronous-as-possible) call, so a test
 * never has to race a real 200ms debounce — and, critically, can never leave a real timer pending
 * past the test's own return, which is exactly what crashed a *different*, later-loaded test file
 * during this test's own development (a stray timer fired after control had moved on, against
 * globals a different file had since replaced). Restores the real `setTimeout` unconditionally.
 */
async function withImmediateTimers(fn) {
  const real = globalThis.setTimeout;
  globalThis.setTimeout = (cb) => { cb(); return 0; };
  try {
    return await fn();
  } finally {
    globalThis.setTimeout = real;
  }
}

export async function test_below_threshold_still_uses_the_array_path_unchanged() {
  const { nodeIndexCalls, searchCalls } = await loadLeftPanel(52);
  const panel = document.getElementById('lpProcedures');
  panel.innerHTML = ''; // present so the guard at the top of _renderLeftProcedures passes
  globalThis.PA.nodeIndex.count = () => 52;
  globalThis.PA.nodeIndex.all = () => { nodeIndexCalls.all++; return [{ nodeId: 'A', name: 'A', schema: 'S', depth: 0 }]; };

  await PA._renderLeftProcedures();

  ok(nodeIndexCalls.all >= 1, 'below the threshold, PA.nodeIndex.all() must still be called (unchanged behavior)');
  equal(searchCalls.length, 0, 'below the threshold, no server fetch should happen for the initial render');
  // No debounced timer is started on this path, but clear defensively so no test in this process
  // can ever leak one across files regardless of how this branch evolves.
  if (globalThis.PA._lpFilterTimer) clearTimeout(globalThis.PA._lpFilterTimer);
}

export async function test_at_or_above_threshold_never_touches_nodeIndex() {
  const { nodeIndexCalls, searchCalls } = await loadLeftPanel(1000, () => [{ nodeId: 'B', name: 'B', schema: 'S' }]);
  document.getElementById('lpProcedures').innerHTML = '';

  await withImmediateTimers(async () => {
    PA._renderLeftProcedures();
    // The debounce callback itself does `await PA.api.searchProcedures(...)` — one microtask hop
    // after the (now-immediate) timer fires, so a single tick is needed for it to actually run.
    await Promise.resolve();
    await Promise.resolve();
  });

  equal(nodeIndexCalls.all, 0, 'at or above the threshold, PA.nodeIndex.all() must never be called by the initial render');
  ok(searchCalls.length >= 1, 'at or above the threshold, the initial render must fetch a server page instead');
  equal(searchCalls[0][0], '', 'the initial render\'s own server fetch must be an empty-query first page, not a real search term');
}

/**
 * UI-024 (§165) — the below-threshold path now triggers `ensureLoaded()` itself (`app.js`'s own
 * eager fetch was removed), so this must genuinely wait for it before reading `all()`/`count()`,
 * not just call it and ignore the result. A stub where `all()`/`count()` only return real data
 * once `ensureLoaded()`'s own promise has resolved is what actually exercises the ordering —
 * unlike the other tests' stubs above, which return fixed values regardless and would not have
 * caught a missing `await` (confirmed: removing the `await` in front of `ensureLoaded()` left
 * every other test in this file passing unchanged, which is why this test exists).
 */
export async function test_below_threshold_genuinely_awaits_ensureLoaded_before_reading_the_index() {
  globalThis.window = globalThis;
  globalThis.document = stubDom();
  globalThis.PA = globalThis.PA || {};
  let loaded = false;
  globalThis.PA.nodeIndex = {
    ensureLoaded: () => new Promise((resolve) => {
      setTimeout(() => { loaded = true; resolve([{ nodeId: 'A', name: 'A', schema: 'S', depth: 0 }]); }, 0);
    }),
    all: () => (loaded ? [{ nodeId: 'A', name: 'A', schema: 'S', depth: 0 }] : []),
    count: () => (loaded ? 1 : 0)
  };
  globalThis.PA.analysisData = { totalNodes: 1, _packageAnalysis: false };
  globalThis.PA.escAttr = (s) => s;
  globalThis.PA.escJs = (s) => s;
  globalThis.PA.esc = (s) => s;
  globalThis.PA.getSchemaColor = () => ({ bg: '#fff', fg: '#000' });
  globalThis.PA.api = { searchProcedures: async () => [] };
  new Function(await read('parser/js/leftPanel.js'))();
  document.getElementById('lpProcedures').innerHTML = '';

  await PA._renderLeftProcedures();

  ok(loaded, 'ensureLoaded() must have actually resolved by the time _renderLeftProcedures finishes');
  const panel = document.getElementById('lpProcedures');
  ok(!panel.innerHTML.includes('No procedures found'),
    'must not render the empty state — that would mean count() ran before ensureLoaded() resolved');
}

export async function test_filterLeft_threshold_check_uses_total_nodes_not_the_now_empty_array() {
  // Calibrates the companion fix in PA.filterLeft: its own threshold branch used to read
  // PA._lp.proc.all.length, which is now deliberately 0 above the threshold — reading it post-fix
  // would wrongly route back to the array path. Confirmed by breaking it back and watching this
  // fail, then restoring, during development; pinned here as a regression guard.
  const src = await read('parser/js/leftPanel.js');
  ok(!src.includes('if (PA._lp.proc.all.length < PA._LP_SERVER_FILTER_THRESHOLD)'),
    'PA.filterLeft must not branch on PA._lp.proc.all.length any more — that array is deliberately empty above the threshold');
  ok(src.includes('if (totalKnown < PA._LP_SERVER_FILTER_THRESHOLD)'),
    'PA.filterLeft must branch on the known total node count instead');
}
