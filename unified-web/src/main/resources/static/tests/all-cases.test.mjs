/**
 * The per-procedure case counts that now live in the Details sub-tab labels, and the estate usage
 * tables that share the component.
 *
 * The retired "All cases (N)" accordion panel used to render these counts; the panel is gone and
 * the counts moved into the sub-tab labels ("Cursors (43)", …). The node-doc group builder
 * (`PA.allCases._groups`) and the label count map (`PA.allCases.counts`) are pure, so they are
 * asserted directly; the estate detail renderers are asserted through their real output, including
 * that a hostile object name in a usage row is escaped.
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
  sequences: [{ schema: 'S', name: 'SEQ1', operations: { NEXTVAL: [60] } }],
  statementSummary: { SELECT: 3, INSERT: 1 },
  transactionScope: { statements: [{ type: 'COMMIT', line: 70 }, { type: 'ROLLBACK', line: 72 }] },
  exceptionHandlers: [{ name: 'WHEN_OTHERS', line: 80 }]
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

/* ------------------- every estate table carries its own filter ------------------- */

export async function test_every_estate_table_has_a_text_filter() {
  const PA = await load('parser/js/estateViews.js');
  const viewHtml = PA.estateViews._detailHtml({
    name: 'V', schema: 'S', objectType: 'VIEW', definition: 'SELECT 1', columns: [],
    baseTables: [], columnCount: 0, baseTableCount: 0, usedByCount: 1, chain: null,
    usedBy: [{ nodeId: 'S$P1', nodeName: 'P1', operations: { SELECT: [1] } }],
    joins: [{ joinType: 'INNER', leftTable: 'A', joinedTable: 'B', condition: 'a=b', line: 1 }],
    indexes: [{ name: 'IX', unique: true, columns: ['X'] }],
    baseTableIndexes: [{ table: 'A', tableSchema: 'S', name: 'IXA', unique: false, columns: ['ID'] }],
    constraints: [{ name: 'CK', type: 'C', condition: 'X>0' }],
    triggerBoundTables: [{ table: 'A', schema: 'S', triggers: [{ name: 'T', timing: 'AFTER', event: 'INSERT', status: 'ENABLED' }] }],
    usedByViews: [{ name: 'V2', schema: 'S', operation: 'SELECT' }],
    usedByTriggers: [{ name: 'T', schema: 'S', timing: 'AFTER', event: 'INSERT', operation: 'SELECT' }],
    dynamicSqlRefs: [{ procedureId: 'S$P1', procedureName: 'P1', lineNumber: 7, type: 'EXECUTE_IMMEDIATE', sqlExpression: 'SELECT 1' }]
  }, esc, 0);
  for (const id of ['ut-vw-usedby-0', 'ut-vw-usedbyviews-0', 'ut-vw-usedbytriggers-0', 'ut-vw-dynsql-0',
                    'ut-vw-triggerbound-0', 'ut-vw-joins-0', 'ut-vw-indexes-0', 'ut-vw-btindexes-0', 'ut-vw-constraints-0']) {
    includes(viewHtml, id + '-q', `table ${id} must render a text filter`);
  }

  const T = await load('parser/js/estateTriggers.js');
  const trigHtml = T.estateTriggers._detailHtml({
    name: 'T', table: 'A', event: 'INSERT', timing: 'AFTER',
    touches: [{ name: 'A', schema: 'S', operation: 'INSERT' }], touchesCount: 1, chain: null,
    bodyCalls: [{ objectName: 'P', type: 'INTERNAL', lines: [1], count: 1 }],
    willFire: [{ nodeId: 'S$P1', nodeName: 'P1', matchingOperations: { INSERT: [1] } }],
    firedBy: [{ nodeId: 'S$P2', nodeName: 'P2', operations: { UPDATE: [2] } }]
  }, esc, 0);
  for (const id of ['ut-tg-bodycalls-0', 'ut-tg-touches-0', 'ut-tg-willfire-0', 'ut-tg-firedby-0']) {
    includes(trigHtml, id + '-q', `table ${id} must render a text filter`);
  }
  // willFire and firedBy are distinct tables with distinct ids (they used to collide).
  equal(trigHtml.split('ut-tg-willfire-0-q').length - 1, 1, 'the will-fire table filter must appear once');
  equal(trigHtml.split('ut-tg-firedby-0-q').length - 1, 1, 'the any-DML table filter must appear once');

  const G = await load('parser/js/estateGlobals.js');
  const globHtml = G.estateGlobals._detailHtml({
    package: 'PKG', variableCount: 1, readCount: 1, writeCount: 1,
    variables: [{ name: 'G_X', dataType: 'NUMBER', defaultValue: '0', constant: false, declared: true, readCount: 1, writeCount: 1 }],
    usedBy: [{ variable: 'G_X', accessType: 'READ', procedureName: 'P1', procedureId: 'S$P1', lineNumber: 3, sourceFile: 'f.sql' }],
    byProcedure: [{ procedureId: 'S$P1', procedureName: 'P1', sourceFile: 'f.sql',
                    reads: [{ variable: 'G_X', lineNumber: 3 }], writes: [], readCount: 1, writeCount: 0 }]
  }, esc, 0);
  for (const id of ['ut-gl-vars-0', 'ut-gl-usedby-0', 'ut-gl-footprint-0']) {
    includes(globHtml, id + '-q', `table ${id} must render a text filter`);
  }
}

export async function test_globals_rows_show_both_variable_and_procedure() {
  const G = await load('parser/js/estateGlobals.js');
  const html = G.estateGlobals._detailHtml({
    package: 'PKG', variableCount: 1, readCount: 1, writeCount: 0,
    variables: [{ name: 'G_X', dataType: 'NUMBER', defaultValue: null, constant: false, declared: true }],
    usedBy: [{ variable: 'G_X', accessType: 'READ', procedureName: 'P.READER', procedureId: 'ID1', lineNumber: 3, sourceFile: 'f.sql' }],
    byProcedure: [{ procedureId: 'S$P1', procedureName: 'P1', sourceFile: 'f.sql',
                    reads: [{ variable: 'G_X', lineNumber: 10 }], writes: [], readCount: 1, writeCount: 0 }]
  }, esc, 0);
  includes(html, 'ut-proc', 'the procedure must be its own row-level column');
  includes(html, "PA.showProcedure('ID1')", 'the procedure name must be clickable in the used-by row');
  includes(html, "PA.showProcedure('S$P1')", 'the procedure name must be clickable in the footprint row');
  ok(html.indexOf('G_X') !== -1 && html.indexOf('G_X') < html.indexOf('P.READER'),
    'the variable name must precede the procedure in the used-by row');
  ok(html.indexOf('G_X') !== -1 && html.indexOf('G_X') < html.indexOf('P1'),
    'the variable name must precede the procedure in the footprint row');
}

/* ------------------------- label counts (the moved panel) ------------------------- */

export async function test_all_cases_counts_map_to_the_details_sub_tabs() {
  const PA = await load('parser/js/allCases.js');
  const c = PA.allCases.counts(NODE);
  equal(c.cursors, 1, 'the Cursors count must be the node doc cursor count');
  equal(c.dynamic, 1, 'the Dynamic SQL count must be the node doc dynamicSql count');
  equal(c.joins, 1, 'the Join Summary count must be the node doc join count');
  equal(c.globals, 1, 'the Globals count must be the node doc external package var count');
  equal(c.sequences, 1, 'the Sequences count must be the node doc sequence count');
  equal(c.views, 1, 'the Views count must be the node doc view count');
  equal(c.statements, 4, 'the Statements count must total the statementSummary');
  equal(c.transactions, 2, 'the Transactions count must come from transactionScope');
  equal(c.exHandlers, 1, 'the Exceptions count must be the node doc exceptionHandlers length');
  equal(c.triggers, 0, 'Triggers fired is async — it starts at 0, never a guess');
}

export async function test_all_cases_counts_are_applicable_only() {
  const PA = await load('parser/js/allCases.js');
  const c = PA.allCases.counts({ nodeId: 'X', tables: [] });
  for (const k of Object.keys(c)) equal(c[k], 0, `a node with no cases must count 0 for ${k}`);
}

/* ------------------------------- wiring ------------------------------- */

export async function test_all_cases_panel_is_retired() {
  const html = await read('parser/index.html');
  includes(html, 'usage-table.js', 'the shared usage-table component is not included');
  includes(html, 'allCases.js', 'the counts helper module is not included');
  // The accordion panel (its host element and title) must be gone from the page and the code.
  excludes(html, 'allCasesSection', 'the All-cases accordion host must be removed');
  const mod = await read('parser/js/allCases.js');
  excludes(mod, 'PA.allCases.render', 'the All-cases panel renderer must be retired');
  excludes(mod, 'ac-wrap', 'the All-cases panel styling must be retired');
  const header = await read('parser/js/detailHeader.js');
  excludes(header, 'PA.allCases.render(detail)', 'the detail view must not render the retired panel');
}

export async function test_counts_are_wired_into_the_sub_tab_labels() {
  const header = await read('parser/js/detailHeader.js');
  includes(header, 'PA.detailsTab.updateLabels(detail)',
    'loading a node detail must refresh the Details sub-tab label counts');
  const app = await read('parser/js/app.js');
  includes(app, 'updateLabels: function', 'detailsTab must expose the label-count updater');
  includes(app, 'PA.allCases.counts', 'the labels must be built from the node-doc counts helper');
  includes(app, "data-dtview", 'the updater must target the Details sub-tab buttons');
  // The "Cursors (43)" suffix format and the async triggers count.
  includes(app, "' (' + n + ')'", 'the count must render as a "(N)" suffix on the label');
  includes(app, 'PA.allCases.triggerCount', 'the async Triggers-fired count must patch its label');
  // Opened from the Details tab, not only on detail load.
  includes(app, 'PA.detailsTab.updateLabels(PA.currentDetail)',
    'opening the Details tab must compute the label counts');
}
