/**
 * MID-003 / FE-011 (tables) — the Table Operations grid is server-paged, and every control that
 * narrows it travels with the request.
 *
 * ## Why the whole grid had to convert at once
 *
 * Search, sort, the operation pills, the column filters and the Direct/Subtree/SubFlow scope are
 * five separate narrowing controls. With one page in memory, any one of them left in the browser
 * filters the fifty rows on screen and reports the result as the whole answer — the control appears
 * to work, and disagrees with the header count sitting next to it. That is why the endpoint and its
 * consumers moved together rather than one registry row at a time.
 *
 * ## What is asserted
 *
 * Source-level, because the defect is structural: a client-side predicate produces identical markup
 * to a server-side one and differs only in which rows it was allowed to see.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

/** Source with comments removed — the fix's own explanation names the pattern it removed. */
const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_the_grid_is_initialised_from_a_page_source() {
  const src = code(await read('parser/js/tableOps.js'));
  includes(src, "PA.tf.init('to', PA.tableOps._src",
    'the grid is still initialised from a materialised array');
  excludes(src, "PA.tf.init('to', PA.tableOps.data",
    'the grid still initialises from PA.tableOps.data');
}

export async function test_no_control_filters_in_the_browser() {
  const src = code(await read('parser/js/tableOps.js'));
  // Each of these would narrow the page rather than the result set.
  excludes(src, 'searchFn:', 'search is still a client-side predicate');
  excludes(src, 'extraFilter:', 'the operation pills still filter client-side');
  excludes(src, 'sortKeys:', 'sorting is still done in the browser over one page');
  includes(src, 'serverSorts:', 'the grid declares no server-side sorts');
}

export async function test_scope_travels_with_the_request() {
  const src = code(await read('parser/js/tableOps.js'));
  includes(src, "src.setParam('scope'", 'scope is not sent to the server');
  includes(src, "src.setParam('procId'", 'the scoping procedure is not sent to the server');
  excludes(src, '_applyScopeData: function',
    'the client-side scope filter survived; it scopes one page and reports it as the whole answer');
}

export async function test_the_operation_pills_become_a_request_parameter() {
  const src = code(await read('parser/js/tableOps.js'));
  includes(src, "src.setParam('types'", 'the operation pills are not sent to the server');
  // Absent, not empty: `types=` would request the empty set rather than "no filter".
  includes(src, "ops.size ? [...ops].join(',') : ''",
    'an empty pill selection must clear the parameter, not send an empty one');
}

export async function test_counts_come_from_the_summary_not_from_loaded_rows() {
  const src = code(await read('parser/js/tableOps.js'));
  includes(src, 'PA.tableOps._summary', 'counts are not taken from the summary endpoint');
  excludes(src, '(PA.tableOps.data || []).length',
    'the header still counts rows in memory, so it would read "50 tables" at any corpus size');
}

export async function test_column_filter_values_come_from_the_summary() {
  // The browser holds one page and cannot enumerate the distinct values of a column it has not
  // seen. A popover built from loaded rows offers a subset and silently hides the rest.
  const src = code(await read('parser/js/tableOps.js'));
  includes(src, 'PA.tableOps._summary.columnValues',
    'column filter values are not read from the summary');
  includes(src, '_colDefs(), PA.tableOps._summary.columnValues);',
    'the summary values are not passed to initColFilters');
}

export async function test_the_row_shape_is_adapted_at_the_boundary() {
  // The endpoint serves the artifact's field names; the grid was written against a projection of
  // them. Adapting one page of fifty is cheaper and more reversible than renaming fields across
  // the row renderer, the sort keys, the column filters and the detail panel at once.
  const api = code(await read('parser/js/api.js'));
  includes(api, 'tablesPageSource', 'there is no adapting page source for the tables grid');
  includes(api, 'tableName: t.name', 'the row adapter does not map the artifact field names');

  const env = code(await read('shared/page-envelope.js'));
  includes(env, 'typeof mapRow === \'function\'',
    'the shared page source cannot adapt rows, so every caller must match its endpoint exactly');
}

export async function test_the_detail_panel_still_has_a_working_fallback() {
  // tableOps.data is now permanently empty. tableDetail treats it as a warm cache and falls back
  // to a fetch. UI-001 corrected what that fallback actually was: `getTableOperations()` is not
  // "one table's worth of work" — it flat-maps every table's every access line to find one row by
  // name. The fallback now asks for that one table directly (see access-details-paged.test.mjs).
  const src = await read('parser/js/tableDetail.js');
  includes(src, 'PA.tableOps && PA.tableOps.data ? PA.tableOps.data : []',
    'the detail panel no longer reads the cache');
  includes(src, 'await PA.api.getTableDetailByName(tableName)',
    'the detail panel has no scoped fallback for when the cache is empty, which is now always');
}
