/**
 * Estate facets — views/MVs + chains, triggers, package globals, and the JAR Database Calls tab.
 *
 * Two things are checked here that a source-only test cannot: the chain is rendered as LEVELS (the
 * whole point of the visualiser), and DB-sourced text — a view definition, a trigger body, an
 * object name — is escaped, never emitted raw. Both are called directly against the real module
 * files loaded through `new Function`, the same technique escaping.test.mjs uses.
 *
 * The rest are wiring assertions: each screen must ask the API for the facet it claims to show,
 * and the JAR tab must read the availability header rather than treating "no facet" as "no calls".
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

/**
 * Load one of the real screen files into a shared PA namespace, with the real escaper and the real
 * usage-table component behind it — the estate detail lists are rendered through PA.usageTable now,
 * so the escaping under test is the escaping the app actually runs.
 */
async function loadParser(rel) {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  if (!globalThis.Esc) new Function(await read('shared/escape.js'))();
  PA.esc = function (s) { return Esc.html(s); };
  PA.escJs = function (s) { return Esc.js(s); };
  if (!PA.usageTable) new Function(await read('shared/usage-table.js'))();
  new Function(await read(rel))();
  return globalThis.PA;
}

export async function test_view_chain_renders_every_level_as_a_step() {
  const PA = await loadParser('parser/js/estateViews.js');
  const chain = {
    view: 'A', kind: 'VIEW',
    levels: [['A'], ['B'], ['T']],
    nodes: [{ name: 'A', schema: 'S', objectType: 'VIEW', kind: 'VIEW' },
            { name: 'B', schema: 'S', objectType: 'VIEW', kind: 'VIEW' }],
    edges: [{ from: 'A', to: 'B', via: 'SELECT' }, { from: 'B', to: 'T', via: 'SELECT' }],
    baseTables: ['T'], cyclic: false, truncated: false
  };
  const html = PA.estateViews._chainHtml(chain, esc);
  includes(html, 'est-chain-step', 'a level was not rendered as a step');
  includes(html, '>L0<', 'level 0 (the view itself) is missing');
  includes(html, '>L1<', 'level 1 is missing');
  includes(html, '>L2<', 'the terminal base-table level is missing');
  includes(html, 'est-node-table', 'the terminal base table is not styled as a table');
  includes(html, 'est-node-view', 'the intermediate view is not styled as a view');
  includes(html, 'est-chain-via', 'the operation linking two levels is not shown');
}

export async function test_view_chain_escapes_a_hostile_object_name() {
  const PA = await loadParser('parser/js/estateViews.js');
  const chain = {
    levels: [['<img src=x onerror=alert(1)>']], nodes: [], edges: [], baseTables: [],
    cyclic: false, truncated: false
  };
  const html = PA.estateViews._chainHtml(chain, esc);
  excludes(html, '<img', 'a raw object name reached the DOM — this is the injection case');
  includes(html, '&lt;img', 'the name must still be readable, just inert');
}

export async function test_view_definition_is_rendered_escaped_inside_pre() {
  const PA = await loadParser('parser/js/estateViews.js');
  const d = {
    name: 'V', schema: 'S', objectType: 'VIEW', isMaterialized: false,
    definition: 'SELECT a FROM t WHERE x < 2 AND y = \'q\'',
    columns: [], baseTables: [], usedBy: [],
    columnCount: 0, baseTableCount: 0, usedByCount: 0, chain: null
  };
  const html = PA.estateViews._detailHtml(d, esc);
  includes(html, '<pre class="est-sql">', 'the definition must render in a <pre>');
  excludes(html, 'x < 2', 'raw SQL reached the DOM unescaped');
  includes(html, 'x &lt; 2', 'the SQL must be escaped, not dropped');
}

export async function test_trigger_chain_and_body_are_escaped() {
  const PA = await loadParser('parser/js/estateTriggers.js');
  const chain = {
    trigger: 'T1', table: 'TAB',
    levels: [['T1'], ['T2']],
    nodes: [{ name: 'T1', table: 'TAB', event: 'INSERT' }, { name: 'T2', table: 'TAB', event: 'UPDATE' }],
    edges: [{ from: 'T1', to: 'T2', via: 'TAB' }], touchedTables: ['TAB'], cyclic: false, truncated: false
  };
  const html = PA.estateTriggers._chainHtml(chain, esc);
  includes(html, 'est-node-trigger', 'trigger nodes must be styled as triggers');
  includes(html, '>L1<', 'the second level is missing');

  const bodyHtml = PA.estateTriggers._detailHtml({
    name: 'T1', body: 'BEGIN <script>alert(1)</script> END;', touches: [],
    touchesCount: 0, firedBy: [], chain: null
  }, esc);
  excludes(bodyHtml, '<script', 'a raw trigger body reached the DOM');
  includes(bodyHtml, '&lt;script', 'the body must be escaped, not dropped');
}

export async function test_globals_detail_groups_reads_and_writes() {
  const PA = await loadParser('parser/js/estateGlobals.js');
  const html = PA.estateGlobals._detailHtml({
    package: 'PKG', variableCount: 1, readCount: 1, writeCount: 1,
    variables: [{ name: 'V', dataType: 'NUMBER', defaultValue: null, constant: false, declared: true }],
    usedBy: [
      { variable: 'V', accessType: 'READ', procedureName: 'P.READER', procedureId: 'ID1', lineNumber: 3, sourceFile: 'f.sql' },
      { variable: 'V', accessType: 'WRITE', procedureName: 'P.WRITER', procedureId: 'ID2', lineNumber: 9, sourceFile: 'f.sql' }
    ]
  }, esc);
  includes(html, 'est-var-name', 'variables must be grouped by name');
  includes(html, '>READ<', 'a read must be labelled');
  includes(html, '>WRITE<', 'a write must be labelled');
  includes(html, 'P.READER', 'the reading procedure is missing');
  includes(html, 'P.WRITER', 'the writing procedure is missing');
}

export async function test_parser_api_uses_the_estate_endpoints() {
  const api = await read('parser/js/api.js');
  includes(api, "facetPageSource('views'", 'views must be fetched from the paged /views facet');
  includes(api, "facetPageSource('triggers'", 'triggers must be fetched from the paged /triggers facet');
  includes(api, "facetPageSource('globals'", 'globals must be fetched from the paged /globals facet');
  includes(api, '/views/${encodeURIComponent(view)}', 'the view detail path is wrong');
  includes(api, '/triggers/${encodeURIComponent(trigger)}', 'the trigger detail path is wrong');
  includes(api, '/globals/${encodeURIComponent(pkg)}', 'the global detail path is wrong');

  includes(await read('parser/js/estateViews.js'), 'PA.api.getViewDetail',
    'the views screen must fetch the view detail (which carries the chain)');
  includes(await read('parser/js/estateTriggers.js'), 'PA.api.getTriggerDetail',
    'the triggers screen must fetch the trigger detail (which carries the chain)');
  includes(await read('parser/js/estateGlobals.js'), 'PA.api.getGlobalDetail',
    'the globals screen must fetch the package detail (which carries usedBy)');
}

export async function test_jar_db_calls_reads_the_availability_header() {
  const api = await read('jar/js/api.js');
  includes(api, 'X-Analyzer-Db-Calls-Available',
    'the JAR db-calls API must read the availability header');
  includes(api, '/db-calls/summary', 'the summary endpoint path is wrong');
  includes(api, '/db-calls', 'the list endpoint path is wrong');

  const screen = await read('jar/js/db-calls.js');
  includes(screen, 'probe.available === false',
    'an unavailable facet must be detected, not rendered as an empty list');
  includes(screen, 'Re-analyze this JAR',
    'the unavailable state must say a re-analysis is needed');
}

export async function test_view_detail_renders_enrichment_sections_and_cross_links() {
  const PA = await loadParser('parser/js/estateViews.js');
  const d = {
    name: 'V', schema: 'S', objectType: 'VIEW', isMaterialized: false,
    definition: 'SELECT 1', columns: [], baseTables: [{ name: 'BASE_A', schema: 'S', operation: 'SELECT' }],
    columnCount: 0, baseTableCount: 1, usedByCount: 0, chain: null,
    joins: [{ joinType: 'JOIN', leftTable: 'BASE_A', leftTableAlias: 'a', joinedTable: 'OTHER', joinedTableAlias: 'o', condition: 'a.id=o.id', line: 2 }],
    joinCount: 1,
    indexes: [{ name: 'IX_V', unique: true, columns: ['X'] }],
    indexCount: 1,
    baseTableIndexes: [{ table: 'BASE_A', tableSchema: 'S', name: 'IX_BASE_A', unique: false, columns: ['ID'] }],
    constraints: [{ name: 'CK_X', type: 'C', condition: 'X>0' }],
    triggerBoundTables: [{ table: 'BASE_A', schema: 'S', triggers: [{ name: 'TRG_BASE_A', timing: 'AFTER', event: 'INSERT', status: 'ENABLED' }] }],
    usedByViews: [{ name: 'V2', schema: 'S', objectType: 'VIEW', operation: 'SELECT' }],
    usedByTriggers: [{ name: 'TRG1', schema: 'S', table: 'T', timing: 'AFTER', event: 'INSERT', operation: 'SELECT' }],
    dynamicSqlRefs: [{ procedureId: 'S$P1', procedureName: 'P1', lineNumber: 7, type: 'EXECUTE_IMMEDIATE', sqlExpression: "SELECT * FROM V" }]
  };
  const html = PA.estateViews._detailHtml(d, esc);
  includes(html, 'Joins (1)', 'joins section missing');
  includes(html, 'a.id=o.id', 'join condition missing');
  includes(html, 'IX_BASE_A', 'base-table index missing');
  includes(html, 'CK_X', 'constraint missing');
  includes(html, 'Triggers on base tables', 'trigger-bound section missing');
  includes(html, 'Used by other views', 'reverse view edge missing');
  includes(html, 'Referenced by triggers', 'reverse trigger edge missing');
  includes(html, 'Dynamic SQL references', 'dynamic SQL section missing');
  // Cross-estate links use the established router calls.
  includes(html, "PA.estateTriggers.showTrigger('TRG_BASE_A')", 'trigger link missing');
  includes(html, "PA.estateViews.showView('V2')", 'view link missing');
  includes(html, "PA.tableDetail.open('BASE_A','S')", 'base-table link missing');
  includes(html, "PA.showProcedure('S$P1')", 'dynamic-SQL procedure link missing');
}

export async function test_view_chain_nodes_are_clickable() {
  const PA = await loadParser('parser/js/estateViews.js');
  const chain = {
    view: 'A', kind: 'VIEW', levels: [['A'], ['B'], ['T']],
    nodes: [{ name: 'A', schema: 'S', objectType: 'VIEW', kind: 'VIEW' },
            { name: 'B', schema: 'S', objectType: 'VIEW', kind: 'VIEW' }],
    edges: [], baseTables: ['T'], cyclic: false, truncated: false
  };
  const html = PA.estateViews._chainHtml(chain, esc);
  includes(html, "PA.estateViews.showView('A')", 'a chain view node is not a clickable view link');
  includes(html, "PA.tableDetail.open('T'", 'a chain base-table node is not a clickable table link');
}

export async function test_trigger_detail_renders_inverse_map_flags_and_body_calls() {
  const PA = await loadParser('parser/js/estateTriggers.js');
  const html = PA.estateTriggers._detailHtml({
    name: 'TRG1', table: 'T1', tableSchema: 'S', event: 'INSERT', timing: 'AFTER',
    eventList: ['INSERT'], insteadOf: false, dbDiscovered: false,
    touches: [], touchesCount: 0, chain: null,
    bodyCalls: [{ objectName: 'P_HELPER', type: 'EXTERNAL', lines: [3], count: 1 }],
    willFire: [{ nodeId: 'S$P1', nodeName: 'P1', matchingOperations: { INSERT: [4] }, firesOnEvent: true }],
    firedBy: [{ nodeId: 'S$P1', nodeName: 'P1', operations: { INSERT: [4] }, firesOnEvent: true },
              { nodeId: 'S$P2', nodeName: 'P2', operations: { UPDATE: [9] }, firesOnEvent: false }]
  }, esc);
  includes(html, 'Will fire this trigger (1)', 'the event-matched inverse map is missing');
  includes(html, 'Writes the table — any DML (2)', 'the any-DML list is missing');
  includes(html, 'Calls from the body (1)', 'trigger body calls are missing');
  includes(html, "PA.showProcedure('P_HELPER')", 'the body call is not a clickable procedure');

  const insteadHtml = PA.estateTriggers._detailHtml({
    name: 'TRG_I', table: 'V', event: 'DELETE', timing: 'INSTEAD OF',
    insteadOf: true, compound: false, dbDiscovered: true,
    touches: [], touchesCount: 0, chain: null, firedBy: [], willFire: []
  }, esc);
  includes(insteadHtml, 'INSTEAD OF', 'the INSTEAD OF flag is not surfaced');
  includes(insteadHtml, 'CATALOG ONLY', 'a catalog-only trigger is not labelled');
}

export async function test_globals_detail_renders_per_procedure_footprint() {
  const PA = await loadParser('parser/js/estateGlobals.js');
  const html = PA.estateGlobals._detailHtml({
    package: 'PKG_A', variableCount: 1, readCount: 1, writeCount: 1,
    variables: [{ name: 'G_X', dataType: 'NUMBER', defaultValue: '0', constant: false, declared: true, readCount: 1, writeCount: 1 }],
    usedBy: [],
    byProcedure: [{ procedureId: 'S$P1', procedureName: 'P1', sourceFile: 'f.sql',
                    reads: [{ variable: 'G_X', lineNumber: 10 }], writes: [{ variable: 'G_X', lineNumber: 12 }],
                    readCount: 1, writeCount: 1 }]
  }, esc);
  includes(html, 'Per-procedure footprint (1)', 'the per-procedure footprint is missing');
  includes(html, "PA.showProcedure('S$P1')", 'the procedure footprint is not a clickable link');
  includes(html, 'G_X', 'the variable is missing from the footprint');
}

export async function test_estate_modules_expose_cross_link_router() {
  const views = await read('parser/js/estateViews.js');
  includes(views, 'showView: function', 'estateViews must expose a showView cross-link target');
  includes(views, "PA.estateTab.switchSub('views')", 'showView must switch to the views sub-tab');
  includes(views, "PA.switchRightTab('details')", 'showView must open the Details tab (estate sections live there now)');
  excludes(views, "PA.switchRightTab('estate')", 'showView must not target the removed top-level Estate tab');
  const triggers = await read('parser/js/estateTriggers.js');
  includes(triggers, 'showTrigger: function', 'estateTriggers must expose a showTrigger cross-link target');
  includes(triggers, "PA.estateTab.switchSub('triggers')", 'showTrigger must switch to the triggers sub-tab');
  includes(triggers, "PA.switchRightTab('details')", 'showTrigger must open the Details tab');
  const globals = await read('parser/js/estateGlobals.js');
  includes(globals, 'showPackage: function', 'estateGlobals must expose a showPackage cross-link target');
  includes(globals, "PA.estateTab.switchSub('globals')", 'showPackage must switch to the globals sub-tab');
  includes(globals, "PA.switchRightTab('details')", 'showPackage must open the Details tab');
}

export async function test_estate_sections_live_inside_the_details_tab() {
  const html = await read('parser/index.html');
  // The Estate top-level tab must be gone.
  excludes(html, 'data-view="estate"', 'the top-level Estate tab button must be removed');
  excludes(html, "PA.switchRightTab('estate')", 'no control should target a top-level Estate tab');
  excludes(html, 'id="viewEstate"', 'the standalone Estate view container must be removed');
  // Its four sections are now Details sub-tabs (data-dtview, handled by PA.detailsTab).
  includes(html, 'data-dtview="views"', 'the Views & MVs sub-tab is not in the Details tab');
  includes(html, 'data-dtview="triggers"', 'the Triggers sub-tab is not in the Details tab');
  includes(html, 'data-dtview="globals"', 'the Globals sub-tab is not in the Details tab');
  includes(html, 'data-dtview="jobs"', 'the Scheduled sub-tab is not in the Details tab');
  includes(html, "PA.detailsTab.switchSub('views')", 'the Views sub-tab is not wired to detailsTab');
  includes(html, "PA.detailsTab.switchSub('jobs')", 'the Scheduled sub-tab is not wired to detailsTab');
  // …and their sub-view containers moved with them.
  includes(html, 'id="estViewViews"', 'the views sub-view is missing');
  includes(html, 'id="estViewTriggers"', 'the triggers sub-view is missing');
  includes(html, 'id="estViewGlobals"', 'the globals sub-view is missing');
  includes(html, 'id="estViewJobs"', 'the scheduled-jobs sub-view is missing');

  includes(html, 'estateViews.js', 'the views module is not included');
  includes(html, 'estateTriggers.js', 'the triggers module is not included');
  includes(html, 'estateGlobals.js', 'the globals module is not included');
  includes(html, 'estate.css', 'the estate stylesheet is not included');

  const jarHtml = await read('jar/index.html');
  includes(jarHtml, 'data-tab="dbcalls"', 'the Database Calls tab button is missing');
  includes(jarHtml, 'id="tab-dbcalls"', 'the Database Calls tab content is missing');
  includes(jarHtml, 'db-calls.js', 'the Database Calls module is not included');
}

export async function test_details_tab_lazily_loads_the_estate_sections() {
  const app = await read('parser/js/app.js');
  // The four estate facets are lazy-loaded by detailsTab on first open, like the other sections.
  includes(app, "sub === 'views' && !PA._detailsSubLoaded.views", 'views is not lazily loaded by detailsTab');
  includes(app, "sub === 'triggers' && !PA._detailsSubLoaded.triggers", 'triggers is not lazily loaded by detailsTab');
  includes(app, "sub === 'globals' && !PA._detailsSubLoaded.globals", 'globals is not lazily loaded by detailsTab');
  includes(app, "sub === 'jobs' && !PA._detailsSubLoaded.jobs", 'jobs is not lazily loaded by detailsTab');
  includes(app, 'views: false, triggers: false, globals: false, jobs: false', 'the estate load guards are not part of _detailsSubLoaded');
}
