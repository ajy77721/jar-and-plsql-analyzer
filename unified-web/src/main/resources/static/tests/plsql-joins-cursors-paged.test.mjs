/**
 * UI-028 — `plsql/js/joins.js` and `cursors.js` converted the same way as `tableOps.js` (§51):
 * server-paged for the plsql live-crawl backend. MID-021 (§107) later gave the parser backend its
 * own paged, `directProcId`/`nodeIds`-capable endpoint too — `joinsPageSource`/`cursorsPageSource`
 * now dispatch by backend themselves, so `load()` no longer branches at all (see
 * `plsql-joins-cursors-parser-backend.test.mjs` for that conversion's own tests).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_joins_load_always_uses_the_paged_source() {
  const src = code(await read('plsql/js/joins.js'));
  const start = src.indexOf('async load()');
  const end = src.indexOf('applyScope()', start);
  const body = src.slice(start, end);
  includes(body, 'PA.api.joinsPageSource', 'joins.js: load() must always set a page source now');
  includes(body, 'PA.joins._src = PA.api.joinsPageSource', 'joins.js: _src must never be left null');
}

export async function test_joins_scope_travels_as_request_params() {
  const src = code(await read('plsql/js/joins.js'));
  includes(src, "src.setParam('directProcId'", 'joins.js: "Direct" scope must be a request parameter');
  includes(src, "src.setParam('nodeIds'", 'joins.js: "Full Flow" scope must be a request parameter');
  includes(src, "PA.tf._restart('join')",
    'joins.js: the scoped src path must restart from page 1, not keep stale pages');
}

export async function test_joins_grid_accepts_either_src_or_array() {
  const src = code(await read('plsql/js/joins.js'));
  includes(src, "PA.tf.init('join', PA.joins._src || PA.joins.data",
    'joins.js: the grid must be initialised from whichever of _src/data is populated');
  includes(src, 'serverSorts:', 'joins.js: the src path needs its own declared sortable columns');
}

export async function test_joins_type_pills_travel_as_types_on_the_src_path() {
  const src = code(await read('plsql/js/joins.js'));
  const at = src.indexOf('applyFilters() {');
  includes(src, 'applyFilters() {', 'PA.joins.applyFilters definition was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, "src.setParam('types'", 'join-type pills must be sent to the server');
}

export async function test_cursors_load_always_uses_the_paged_source() {
  const src = code(await read('plsql/js/cursors.js'));
  const start = src.indexOf('async load()');
  const end = src.indexOf('applyScope()', start);
  const body = src.slice(start, end);
  includes(body, 'PA.api.cursorsPageSource', 'cursors.js: load() must always set a page source now');
  includes(body, 'PA.cursors._src = PA.api.cursorsPageSource', 'cursors.js: _src must never be left null');
}

export async function test_cursors_scope_travels_as_request_params() {
  const src = code(await read('plsql/js/cursors.js'));
  includes(src, "src.setParam('directProcId'", 'cursors.js: "Direct" scope must be a request parameter');
  includes(src, "src.setParam('nodeIds'", 'cursors.js: "Full Flow" scope must be a request parameter');
  includes(src, "PA.tf._restart('cur')",
    'cursors.js: the scoped src path must restart from page 1, not keep stale pages');
}

export async function test_cursors_grid_accepts_either_src_or_array() {
  const src = code(await read('plsql/js/cursors.js'));
  includes(src, "PA.tf.init('cur', PA.cursors._src || PA.cursors.data",
    'cursors.js: the grid must be initialised from whichever of _src/data is populated');
  includes(src, 'serverSorts:', 'cursors.js: the src path needs its own declared sortable columns');
}

export async function test_cursors_type_pills_and_operation_dropdown_map_to_different_params() {
  const src = code(await read('plsql/js/cursors.js'));
  const at = src.indexOf('applyFilters() {');
  includes(src, 'applyFilters() {', 'PA.cursors.applyFilters definition was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, "src.setParam('cf'", 'cursorType pills must be sent as a column filter (cf=0:...)');
  includes(body, "src.setParam('types'", 'the operation dropdown must be sent as types');
}
