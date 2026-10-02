/**
 * The reusable usage-table component: page math, the filter, and — the case that matters most —
 * that database-sourced SQL text cannot inject.
 *
 * The module is loaded through `new Function` like the other frontend tests, with the real
 * `shared/escape.js` behind `PA.esc`/`PA.escJs`, so the escaping under test is the escaping the app
 * actually runs.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function loadUsageTable() {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  new Function(await read('shared/escape.js'))();
  PA.esc = function (s) { return Esc.html(s); };
  PA.escJs = function (s) { return Esc.js(s); };
  new Function(await read('shared/usage-table.js'))();
  return PA.usageTable;
}

/* ------------------------------ pagination math ------------------------------ */

export async function test_pagination_math_pages_and_bounds() {
  const U = await loadUsageTable();
  equal(U.PAGE, 25, 'the default page size must be 25');
  const p = U.paginate(142, 25, 0);
  equal(p.pages, 6, '142 rows at 25/page is 6 pages');
  equal(p.start, 0, 'page 0 starts at 0');
  equal(p.end, 25, 'page 0 ends at 25');
  const last = U.paginate(142, 25, 5);
  equal(last.start, 125, 'the last page starts at 125');
  equal(last.end, 142, 'the last page ends at the total, not at 150');
}

export async function test_pagination_math_clamps_a_page_past_the_end() {
  const U = await loadUsageTable();
  const p = U.paginate(10, 25, 9);
  equal(p.page, 0, 'a page past the end clamps to the last (only) page');
  equal(p.pages, 1, 'ten rows is one page');
  equal(p.end, 10, 'the slice is bounded by the total');
}

export async function test_pagination_math_handles_empty_and_bad_sizes() {
  const U = await loadUsageTable();
  const empty = U.paginate(0, 25, 0);
  equal(empty.pages, 1, 'an empty list still has one (empty) page');
  equal(empty.end, 0, 'nothing is sliced from an empty list');
  const bad = U.paginate(30, 0, 0);
  equal(bad.size, 25, 'a zero page size falls back to the default rather than dividing by zero');
}

/* ---------------------------------- filter ---------------------------------- */

const ROWS = [
  { nodeName: 'PKG_A.PROC_READ', operations: { SELECT: [12] } },
  { nodeName: 'PKG_B.PROC_WRITE', operations: { UPDATE: [4], INSERT: [9] } },
  { nodeName: 'PKG_C.PROC_DEL', operations: { DELETE: [30] } }
];

export async function test_filter_matches_name_line_and_badge() {
  const U = await loadUsageTable();
  equal(U.filterRows(ROWS, 'proc_read', {}).length, 1, 'name match is case-insensitive');
  equal(U.filterRows(ROWS, 'update', {}).length, 1, 'an operation badge is searchable');
  equal(U.filterRows(ROWS, 'L30', {}).length, 1, 'the line number is searchable');
  equal(U.filterRows(ROWS, '', {}).length, 3, 'an empty query keeps every row');
  equal(U.filterRows(ROWS, 'nothing-here', {}).length, 0, 'a miss returns nothing');
}

export async function test_filter_matches_a_custom_snippet_and_search_text() {
  const U = await loadUsageTable();
  const rows = [{ name: 'V', snippet: 'SELECT x FROM base_t' }];
  equal(U.filterRows(rows, 'base_t', {}).length, 1, 'the SQL snippet is searchable');
  equal(U.filterRows(rows, 'zzz', { search: function () { return 'zzz-extra'; } }).length, 1,
    'a caller-supplied search field is included');
}

/* --------------------------- snippet escaping (XSS) --------------------------- */

const HOSTILE = "SELECT * FROM t WHERE a = '<script>alert(1)</script>' AND b = \"x\"";

export async function test_snippet_is_escaped_so_hostile_sql_cannot_inject() {
  const U = await loadUsageTable();
  const esc = (v) => Esc.html(v);
  const out = U.snippetHtml(HOSTILE, esc, 500);
  excludes(out, '<script', 'raw SQL reached the DOM — this is the injection case');
  includes(out, '&lt;script', 'the SQL must still be readable, just inert');
  excludes(out, '<img', 'no markup may survive');
}

export async function test_row_renderer_escapes_name_snippet_and_badges() {
  const U = await loadUsageTable();
  const render = U.makeRowRenderer('ut-test', {
    name: function (r) { return r.name; },
    snippet: function (r) { return r.snippet; },
    badges: function () { return ['<b>X</b>']; }
  });
  const html = render({ name: HOSTILE, snippet: HOSTILE, lineNumber: 7 }, 0, (v) => Esc.html(v));
  excludes(html, '<script', 'a raw snippet reached the DOM');
  excludes(html, '<b>X</b>', 'a raw badge label reached the DOM');
  includes(html, '&lt;script', 'the snippet must render escaped');
  includes(html, '>L7<', 'the line number must be shown');
  includes(html, 'ut-snippet', 'the snippet must use the snippet cell');
  includes(html, 'est-op-badge', 'badges must use the shared badge styling');
}

export async function test_row_renderer_truncates_a_long_snippet() {
  const U = await loadUsageTable();
  const long = 'SELECT ' + 'a,'.repeat(400) + ' FROM t';
  const render = U.makeRowRenderer('ut-test', {});
  const html = render({ name: 'P', snippet: long, lineNumber: 1 }, 0, (v) => Esc.html(v));
  includes(html, '…', 'a long snippet must be truncated with an ellipsis');
  ok(html.length < long.length, 'the truncated snippet must be shorter than the raw text');
}

export async function test_expandable_row_wires_toggle_detail() {
  const U = await loadUsageTable();
  const render = U.makeRowRenderer('ut-exp', { full: function () { return 'detail'; } });
  const html = render({ name: 'P', lineNumber: 3 }, 4, (v) => Esc.html(v));
  includes(html, "PA.tf.toggleDetail('ut-exp',4)", 'an expandable row must toggle its detail row');
  includes(html, 'ut-expandable', 'an expandable row must be marked');
}

/* ------------------------------ panel markup ------------------------------ */

export async function test_optional_procedure_column_renders_second_and_is_searchable() {
  const U = await loadUsageTable();
  const opts = { name: (r) => r.variable, procedure: (r) => r.procedureName,
                 procedureLink: (r) => "PA.showProcedure('" + r.procedureId + "')" };
  const row = U.makeRowRenderer('ut-p', opts)({ variable: 'G_X', procedureName: 'P1', procedureId: 'ID1', lineNumber: 3 }, 0, (v) => Esc.html(v));
  includes(row, 'ut-proc', 'the procedure cell is missing');
  includes(row, "PA.showProcedure('ID1')", 'the procedure link is missing');
  ok(row.indexOf('G_X') < row.indexOf('P1'), 'the variable name must lead and the procedure follow');
  const html = U.render('ut-p', [{ variable: 'G_X', procedureName: 'P1' }], opts);
  includes(html, '>Procedure<', 'the procedure column header is missing');
  equal(U.filterRows([{ variable: 'G_X', procedureName: 'P1' }], 'p1', opts).length, 1,
    'the procedure name must be searchable');
}

export async function test_render_emits_count_filter_pager_and_columns() {
  const U = await loadUsageTable();
  const html = U.render('ut-x', ROWS, { title: 'Used By', countLabel: 'usages' });
  includes(html, 'Used By', 'the title is missing');
  includes(html, 'ut-x-count', 'the count header element is missing');
  includes(html, 'ut-x-q', 'the text filter input is missing');
  includes(html, 'ut-x-tbody', 'the table body is missing');
  includes(html, 'ut-x-pager', 'the pager container is missing');
  includes(html, '>Name<', 'the name column is missing');
  includes(html, '>Line<', 'the line column is missing');
  includes(html, '>SQL<', 'the snippet column is missing');
  includes(html, '>Kind<', 'the badges column is missing');
  includes(html, 'pagination-bar', 'the shared pager styling must be used');
}
