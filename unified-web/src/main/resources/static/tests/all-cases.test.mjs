/**
 * The per-procedure "All cases (N)" panel and the estate usage tables that share the component.
 *
 * The node-doc group builder is pure (`PA.allCases._groups`), so it is asserted directly; the estate
 * detail renderers are asserted through their real output, including that a hostile object name in a
 * usage row is escaped.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');
const esc = (v) => String(v == null ? '' : v)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

async function load(rel) {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  if (!globalThis.Esc) new Function(await read('shared/escape.js'))();
  PA.esc = function (s) { return Esc.html(s); };
  PA.escJs = function (s) { return Esc.js(s); };
  if (!PA.usageTable) new Function(await read('shared/usage-table.js'))();
  new Function(await read(rel))();
  return globalThis.PA;
}

const NODE = {
  nodeId: 'S$PKG$P1', name: 'P1', sourceFile: 'pkg.pkb',
  cursors: [{ name: 'C1', line: 10, forLoop: true, query: 'SELECT a FROM t WHERE x = 1',
              tables: ['T'], stmtOperations: [{ operation: 'FOR_LOOP', line: 10 }] }],
  dynamicSql: [{ type: 'EXECUTE_IMMEDIATE', line: 20, sqlExpression: "EXECUTE IMMEDIATE 'SELECT 1'", usingVariables: ['V'] }],
  tables: [{ schema: 'S', name: 'T', objectType: 'TABLE', operations: { SELECT: [12], UPDATE: [14] },
             joins: [{ joinType: 'INNER', leftTable: 'T', leftTableAlias: 't', joinedTable: 'U', joinedTableAlias: 'u', condition: 't.id=u.id', line: 13 }] },
           { schema: 'S', name: 'V', objectType: 'VIEW', operations: { SELECT: [30] } }],
  externalPackageVarRefDetails: [{ packageName: 'PKG', variableName: 'G_X', line: 40, accessType: 'WRITE' }],
  calls: [{ objectName: 'P2', target: 'S$PKG$P2', packageName: 'PKG', type: 'INTERNAL', lines: [50], count: 1 }],
  sequences: [{ schema: 'S', name: 'SEQ1', operations: { NEXTVAL: [60] } }]
};

export async function test_all_cases_groups_every_applicable_case() {
  const PA = await load('parser/js/allCases.js');
  const titles = PA.allCases._groups(NODE).map((g) => g.title);
  for (const t of ['Cursors', 'Dynamic SQL', 'Table operations', 'Joins', 'Globals', 'Calls', 'Sequences', 'Views referenced']) {
    ok(titles.includes(t), `missing case group: ${t}`);
  }
}

export async function test_all_cases_are_applicable_only() {
  const PA = await load('parser/js/allCases.js');
  const empty = PA.allCases._groups({ nodeId: 'X', tables: [] });
  equal(empty.length, 0, 'a procedure with no captured cases must render no groups');
  const titles = PA.allCases._groups(NODE).map((g) => g.title);
  excludes(titles.join(','), 'Exception', 'a group with no data must not be fabricated');
}

export async function test_all_cases_rows_carry_lines_and_snippets() {
  const PA = await load('parser/js/allCases.js');
  const groups = PA.allCases._groups(NODE);
  const dyn = groups.find((g) => g.title === 'Dynamic SQL');
  equal(dyn.rows.length, 1, 'the dynamic SQL case is missing');
  equal(dyn.opts.line(dyn.rows[0]), 20, 'the dynamic SQL line is missing');
  includes(String(dyn.opts.snippet(dyn.rows[0])), 'SELECT 1', 'the dynamic SQL snippet is missing');

  const globals = groups.find((g) => g.title === 'Globals');
  equal(globals.opts.name(globals.rows[0]), 'G_X', 'the globals row must lead with the variable name');
  equal(globals.opts.nameClass, 'est-var-name', 'the globals variable name must be the prominent first cell');
}

export async function test_all_cases_escaping_a_hostile_snippet() {
  const PA = await load('parser/js/allCases.js');
  const hostile = Object.assign({}, NODE, {
    dynamicSql: [{ type: 'EXECUTE_IMMEDIATE', line: 1, sqlExpression: "BEGIN <script>alert(1)</script> END;" }]
  });
  const g = PA.allCases._groups(hostile).find((x) => x.title === 'Dynamic SQL');
  const html = PA.usageTable.makeRowRenderer(g.id, g.opts)(g.rows[0], 0, esc);
  excludes(html, '<script', 'a hostile snippet reached the DOM through the All-cases panel');
  includes(html, '&lt;script', 'the snippet must render escaped');
}

/* --------------------------- estate usage tables --------------------------- */

const VIEW = {
  name: 'V', schema: 'S', objectType: 'VIEW', definition: 'SELECT 1',
  columns: [], baseTables: [], columnCount: 0, baseTableCount: 0, usedByCount: 1, chain: null,
  usedBy: [{ nodeId: 'S$P1', nodeName: '<script>bad</script>', nodeType: 'PROCEDURE', operations: { SELECT: [5] } }]
};

export async function test_estate_view_usedby_renders_a_paged_filtered_usage_table() {
  const PA = await load('parser/js/estateViews.js');
  const html = PA.estateViews._detailHtml(VIEW, esc, 2);
  includes(html, 'ut-panel', 'the used-by list must render as the shared usage table');
  includes(html, 'ut-vw-usedby-2', 'the table id must be unique per detail row');
  includes(html, 'ut-vw-usedby-2-q', 'the usage table must carry a text filter');
  includes(html, 'ut-vw-usedby-2-pager', 'the usage table must carry a pager');
  includes(html, 'pagination-bar', 'the shared pager styling must be used');
  excludes(html, '<script', 'a hostile procedure name reached the DOM');
  includes(html, '&lt;script', 'the hostile name must render escaped');
  includes(html, '>L5<', 'the line number must be shown');
}

export async function test_estate_dynamic_sql_shows_its_snippet() {
  const PA = await load('parser/js/estateViews.js');
  const html = PA.estateViews._detailHtml({
    name: 'V', schema: 'S', objectType: 'VIEW', definition: 'SELECT 1',
    columns: [], baseTables: [], columnCount: 0, baseTableCount: 0, usedByCount: 0, chain: null,
    dynamicSqlRefs: [{ procedureId: 'S$P1', procedureName: 'P1', lineNumber: 7, type: 'EXECUTE_IMMEDIATE', sqlExpression: 'SELECT * FROM V' }]
  }, esc, 0);
  includes(html, 'ut-vw-dynsql-0', 'the dynamic SQL references must use the shared table');
  includes(html, 'SELECT * FROM V', 'the dynamic SQL snippet is missing');
  includes(html, 'ut-snippet', 'the snippet must render in the snippet cell');
}

export async function test_globals_usage_leads_with_the_variable_name() {
  const PA = await load('parser/js/estateGlobals.js');
  const html = PA.estateGlobals._detailHtml({
    package: 'PKG', variableCount: 1, readCount: 1, writeCount: 0,
    variables: [{ name: 'G_X', dataType: 'NUMBER', defaultValue: null, constant: false, declared: true }],
    usedBy: [{ variable: 'G_X', accessType: 'READ', procedureName: 'P.READER', procedureId: 'ID1', lineNumber: 3, sourceFile: 'f.sql' }]
  }, esc, 0);
  includes(html, 'est-var-name', 'the variable name must be the prominent first cell');
  ok(html.indexOf('G_X') !== -1 && html.indexOf('G_X') < html.indexOf('P.READER'),
    'the variable name must appear before the referencing procedure');
}

/* ------------------------------- wiring ------------------------------- */

export async function test_all_cases_is_wired_into_the_page() {
  const html = await read('parser/index.html');
  includes(html, 'usage-table.js', 'the shared usage-table component is not included');
  includes(html, 'allCases.js', 'the All-cases module is not included');
  const header = await read('parser/js/detailHeader.js');
  includes(header, 'PA.allCases.render(detail)', 'the procedure detail view must render the All-cases panel');
}
