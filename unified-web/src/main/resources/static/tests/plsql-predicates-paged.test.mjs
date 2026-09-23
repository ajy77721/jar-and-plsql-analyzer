/**
 * UI-028 — `plsql/js/predicates.js` converted (§53). Unlike `tableOps.js`/`joins.js`/`cursors.js`,
 * its rows were never a direct pass-through — `_buildPredicateData` is a real aggregation, ported
 * server-side to `PlsqlPredicatesFacet`. The client no longer builds it at all on the src path.
 *
 * MID-021 (predicates.js's own paged-conversion project): the parser backend got the same
 * conversion too — `predicatesPageSource` now dispatches by backend itself, so `load()` no longer
 * branches at all (see `plsql-predicates-parser-backend.test.mjs` for that conversion's own tests).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_load_always_uses_the_paged_source() {
  const src = code(await read('plsql/js/predicates.js'));
  const start = src.indexOf('async load()');
  const end = src.indexOf('_initOpPills()', start);
  const body = src.slice(start, end);
  includes(body, 'PA.api.predicatesPageSource', 'load() must always set a page source now');
  includes(body, 'PA.predicates._src = PA.api.predicatesPageSource', '_src must never be left null');
}

export async function test_scope_travels_as_request_params() {
  const src = code(await read('plsql/js/predicates.js'));
  includes(src, "src.setParam('directProcId'", '"Direct" scope must be a request parameter');
  includes(src, "src.setParam('nodeIds'", '"Full Flow" scope must be a request parameter');
  includes(src, "PA.tf._restart('pred')",
    'the scoped src path must restart from page 1, not keep stale pages');
}

export async function test_grid_accepts_either_src_or_array() {
  const src = code(await read('plsql/js/predicates.js'));
  includes(src, "PA.tf.init('pred', PA.predicates._src || PA.predicates.data",
    'the grid must be initialised from whichever of _src/data is populated');
  includes(src, 'serverSorts:', 'the src path needs its own declared sortable columns');
}

export async function test_pill_and_dropdown_filters_combine_as_an_intersection() {
  const src = code(await read('plsql/js/predicates.js'));
  const at = src.indexOf('applyFilters() {');
  includes(src, 'applyFilters() {', 'PA.predicates.applyFilters definition was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, "src.setParam('types'", 'the combined filter must be sent to the server');
  includes(body, 'ops.has(opFilter)',
    'the pills and the dropdown filter the same field and must be combined, not one silently dropped');
}
