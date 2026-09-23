/**
 * UI-028 — `plsql/js/tableOps.js` converted the same way as `joins.js`/`cursors.js` (§51):
 * server-paged for the plsql live-crawl backend. MID-021 (§110) later gave the parser backend its
 * own paged, `directProcId`/`nodeIds`-capable endpoint too — `tablesPageSource` now dispatches by
 * backend itself, so `load()` no longer branches at all (see
 * `plsql-tableops-parser-backend.test.mjs` for that conversion's own tests, and
 * `plsql-tableops-parser-shape-bug.test.mjs` for the real shape bug §109 found and fixed along the
 * way).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_load_always_uses_the_paged_source() {
  const src = code(await read('plsql/js/tableOps.js'));
  const start = src.indexOf('async load()');
  const end = src.indexOf('_initOpPills()', start);
  const body = src.slice(start, end);
  includes(body, 'PA.api.tablesPageSource', 'load() must always set a page source now');
  includes(body, 'PA.tableOps._src = PA.api.tablesPageSource', '_src must never be left null');
}

export async function test_the_grid_accepts_either_the_page_source_or_the_array() {
  const src = code(await read('plsql/js/tableOps.js'));
  includes(src, "PA.tf.init('to', PA.tableOps._src || PA.tableOps.data",
    'the grid must be initialised from whichever of _src/data is populated');
  includes(src, 'serverSorts:', 'the src path needs its own declared sortable columns');
}

export async function test_pill_and_checkbox_filters_travel_with_the_request_on_the_src_path() {
  const src = code(await read('plsql/js/tableOps.js'));
  const at = src.indexOf('applyFilters() {');
  includes(src, 'applyFilters() {', 'PA.tableOps.applyFilters definition was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, "src.setParam('types'", 'operation pills must be sent to the server, not filtered client-side');
  includes(body, "src.setParam('cf'", 'the type/external/trigger filters must be sent as column filters');
}

export async function test_scope_travels_as_directProcId_or_nodeIds_on_the_src_path() {
  const src = code(await read('plsql/js/tableOps.js'));
  includes(src, "src.setParam('directProcId'", '"Direct" scope must be sent as a request parameter');
  includes(src, "src.setParam('nodeIds'", '"Full Flow" scope must be sent as a request parameter');
}

export async function test_html_onchange_handlers_were_repointed_at_the_dual_mode_wrapper() {
  const html = await read('plsql/index.html');
  excludes(html, `onchange="PA.tf.filter('to')"`,
    'a filter control still calls PA.tf.filter directly, bypassing the src-path parameter push');
  includes(html, 'onchange="PA.tableOps.applyFilters()"',
    'the type/external/trigger controls must call the dual-mode wrapper');
}
