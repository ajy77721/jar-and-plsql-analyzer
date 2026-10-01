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

/** Load one of the real screen files into a shared PA namespace. */
async function loadParser(rel) {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
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

export async function test_estate_screens_are_wired_into_navigation() {
  const html = await read('parser/index.html');
  includes(html, "PA.switchRightTab('estate')", 'the Estate tab button is not wired');
  includes(html, 'id="viewEstate"', 'the Estate view container is missing');
  includes(html, 'estateViews.js', 'the views module is not included');
  includes(html, 'estateTriggers.js', 'the triggers module is not included');
  includes(html, 'estateGlobals.js', 'the globals module is not included');
  includes(html, 'estate.css', 'the estate stylesheet is not included');

  const jarHtml = await read('jar/index.html');
  includes(jarHtml, 'data-tab="dbcalls"', 'the Database Calls tab button is missing');
  includes(jarHtml, 'id="tab-dbcalls"', 'the Database Calls tab content is missing');
  includes(jarHtml, 'db-calls.js', 'the Database Calls module is not included');
}
