/**
 * UI-024 (§163) — the remaining `PA.nodeIndex` consumers beyond §161/§162's `getScopedNodes`/
 * `_buildNodeIdMap` conversion: `callGraphViz.js` (3 sites), `sourceView.js` (5 entry points),
 * `tableDetail.js`'s `open()`, `joinSummary.js`'s `_resolveSourceFile`, `triggerDetail.js`'s
 * `_loadSource`. Each now awaits `PA.nodeIndex.ensureLoaded()` — either directly at the one lookup,
 * or once at a covering async entry point above a synchronous render pass (the same pattern
 * `callGraphViz._buildGraph` established), confirmed here by source inspection since these files
 * have too many external dependencies (canvas, DOM, other PA.* modules) to execute directly in this
 * harness the way `node-index.test.mjs` exercises `ensureLoaded()` itself.
 *
 * `callGraph.js`'s `_getNodeMetrics` (called from the recursive, synchronous `renderNode` tree
 * builder) is deliberately NOT touched — converting it would need `renderNode` itself to become
 * async at every recursion level, the same class of blocker `complexity.js`'s own formulas were
 * left client-side for (§98). This is why `app.js`'s eager `PA.loadAnalysis` fetch still cannot be
 * removed: `callGraph.js` is the one real remaining holdout forcing full `PA.nodeIndex` residency.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function bodyOf(src, marker, endMarker) {
  const start = src.indexOf(marker);
  ok(start >= 0, `could not find ${marker}`);
  const searchFrom = start + marker.length;
  const end = endMarker ? src.indexOf(endMarker, searchFrom) : src.length;
  ok(end > start, `endMarker "${endMarker}" not found after "${marker}"`);
  return src.slice(start, end);
}

export async function test_callGraphViz_buildGraph_awaits_ensureLoaded_once_before_the_loop() {
  const src = await read('parser/js/callGraphViz.js');
  const body = bodyOf(src, '_buildGraph: async function', 'function ensureNode');
  includes(body, 'await PA.nodeIndex.ensureLoaded()', '_buildGraph must preload once before enriching any node');
}

export async function test_callGraphViz_showNodeDetail_is_the_click_path_not_the_hover_path() {
  const src = await read('parser/js/callGraphViz.js');
  // The hover path (_onMouseMove) must never touch PA.nodeIndex — only the geometric _findNodeAt.
  const hoverBody = bodyOf(src, '_onMouseMove: function', '_onMouseUp:');
  excludes(hoverBody, 'PA.nodeIndex', 'the hover path must stay purely geometric, no index lookup');
  // UPDATED for UI-039 (§245): the click path defers _showNodeDetail into the single-click
  // debounce timer (the old synchronous relayout moved the clicked node out from under a real
  // double-click's second half), so _onMouseUp no longer `await`s the call in its own body —
  // the call runs from the timer callback. What must still hold: the click path reaches
  // _showNodeDetail at all, and the panel preloads before its own lookup.
  const clickBody = bodyOf(src, '_onMouseUp: async function', '_onWheel:');
  includes(clickBody, 'PA.callGraphViz._showNodeDetail(', 'the click path must open the detail panel (via the debounce timer since §245)');
  const detailBody = bodyOf(src, '_showNodeDetail: async function', '_closeNodeDetail:');
  includes(detailBody, 'await PA.nodeIndex.ensureLoaded()', '_showNodeDetail must preload before its own lookup');
}

export async function test_sourceView_entry_points_all_preload_before_any_synchronous_helper_runs() {
  const src = await read('parser/js/sourceView.js');
  for (const [marker, endMarker] of [
    ['async _loadSidebar(procId)', 'var searchIds ='],
    ['async openFromProc(procId)', 'if (node && node.sourceFile)'],
    ['async openModalAtLine(sourceFile, lineNumber)', 'const _tok ='],
    ['async openModal(procId)', 'let sf = (node && node.sourceFile)']
  ]) {
    const body = bodyOf(src, marker, endMarker);
    includes(body, 'await PA.nodeIndex.ensureLoaded()', `${marker} must preload before its synchronous nodeIndex helpers run`);
  }
}

export async function test_tableDetail_open_preloads_before_any_tab_renders() {
  const src = await read('parser/js/tableDetail.js');
  const body = bodyOf(src, 'async open(tableName, schema, initialTab)', 'var gen = ++PA.tableDetail._openGen');
  includes(body, 'await PA.nodeIndex.ensureLoaded()', 'open() must preload before any per-row render loop can run');
}

export async function test_joinSummary_resolveSourceFile_is_async_and_awaited_by_both_callers() {
  const src = await read('parser/js/joinSummary.js');
  const fnBody = bodyOf(src, '_resolveSourceFile: async function', '// ====================');
  includes(fnBody, 'await PA.nodeIndex.ensureLoaded()', '_resolveSourceFile must preload before its own lookup');
  const callSites = [...src.matchAll(/await PA\.joinSummary\._resolveSourceFile\(/g)];
  ok(callSites.length === 2, `expected 2 awaited call sites, found ${callSites.length}`);
}

export async function test_triggerDetail_loadSource_preloads_before_its_lookup() {
  const src = await read('parser/js/triggerDetail.js');
  const body = bodyOf(src, 'async _loadSource(trigger, modal)', 'if (node && node.detailFile)');
  includes(body, 'await PA.nodeIndex.ensureLoaded()', '_loadSource must preload before PA.nodeIndex.byId');
}

export async function test_callGraph_render_hot_path_deliberately_stays_synchronous() {
  const src = await read('parser/js/callGraph.js');
  const body = bodyOf(src, '_getNodeMetrics: function', 'return PA.callTrace._indexDataCache[key]');
  excludes(body, 'await', '_getNodeMetrics must stay synchronous — converting it would cascade into the recursive renderNode tree builder');
}
