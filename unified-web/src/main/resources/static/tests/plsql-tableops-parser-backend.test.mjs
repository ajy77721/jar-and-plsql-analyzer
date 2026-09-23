/**
 * MID-021 (§110) — `plsql/js/tableOps.js`'s parser-backend fallback used to fetch the whole
 * unpaged array (`PA.api.getTableOperations`) and — until §109 fixed the underlying shape bug —
 * handed the renderer an unusable object besides. Converted to the same paged-source path already
 * used for the live-crawl backend and for `joins.js`/`cursors.js` (§108): `tablesPageSource` now
 * dispatches by `_parserName()` itself, mapping each page through `PA.api._adaptParserTableRow` so
 * the renderer always sees `accessDetails[]`/`accessCount`. `_applyScopeData`'s existing
 * `if (src) {...}` branch (already sending `directProcId`/`nodeIds` — written ahead of the server
 * support landing) needed no changes at all.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_load_no_longer_branches_on_backend() {
  const src = code(await read('plsql/js/tableOps.js'));
  const start = src.indexOf('async load()');
  const end = src.indexOf('_initOpPills()', start);
  const body = src.slice(start, end);
  excludes(body, 'PA.api._parserName && PA.api._parserName()',
    'tableOps.js\'s load() must not branch on backend anymore — the page source itself dispatches');
  excludes(body, '_src = null', 'tableOps.js must always set a page source now, never null');
}

export async function test_api_tables_page_source_dispatches_by_backend() {
  const src = code(await read('plsql/js/api.js'));
  includes(src, 'tablesPageSource(params) {\n        const n = this._parserName();',
    'tablesPageSource must dispatch by backend');
  includes(src, '/api/parser/analyses/${encodeURIComponent(n)}/tables/page`',
    'the parser-backend tables path must hit the new directProcId/nodeIds-capable paged endpoint');
  includes(src, 'PA.api._adaptParserTableRow',
    'the parser-backend page source must map each row through the shape adapter');
}

function stubTf() {
  return { _state: {}, _restart() {}, setData() {}, init() {} };
}

async function loadTableOps(parserName) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, querySelectorAll: () => [] };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.tf = stubTf();
  globalThis.PA.context = { procId: 'ERP$P1', callTreeNodeIds: new Set(['ERP$P1', 'ERP$P2']) };
  const setParamCalls = [];
  const fakeSrc = { setParam: (k, v) => setParamCalls.push([k, v]) };
  globalThis.PA.api = {
    _parserName: () => parserName,
    getTableOperations: async () => { throw new Error('must not be called — the whole point of this fix'); },
    tablesPageSource: () => fakeSrc
  };
  new Function(await read('plsql/js/tableOps.js'))();
  const tableOps = globalThis.PA.tableOps;
  tableOps._initOpPills = () => {};
  tableOps._initTable = () => {};
  tableOps._updateScopeToggle = () => {};
  tableOps._updateCounts = () => {};
  return { tableOps, setParamCalls };
}

export async function test_load_never_fetches_the_whole_array_for_the_parser_backend_either() {
  const { tableOps } = await loadTableOps('some-analysis');
  await tableOps.load();
  equal(tableOps.data.length, 0, 'PA.tableOps.data must stay empty — the fetch that used to populate it is gone');
  ok(tableOps._src, 'a page source must always be set now, for either backend');
}

export async function test_scope_params_flow_through_unchanged_for_the_parser_backend() {
  const { tableOps, setParamCalls } = await loadTableOps('some-analysis');
  await tableOps.load();
  tableOps.scopeMode = 'scoped';
  tableOps._applyScopeData();
  const directProcIdCall = setParamCalls.find(([k]) => k === 'directProcId');
  ok(directProcIdCall, '_applyScopeData must still call src.setParam for directProcId, unchanged');
  equal(directProcIdCall[1], 'ERP$P1');
}
