/**
 * MID-021 (predicates.js's own paged-conversion project) — `plsql/js/predicates.js`'s parser-backend
 * fallback used to fetch the whole tables array (`PA.api.getTableOperations`) and aggregate it
 * client-side via `_buildPredicateData`. Converted to the same paged-source path already used for
 * the live-crawl backend and for `tableOps.js`/`joins.js`/`cursors.js`: `predicatesPageSource` now
 * dispatches by `_parserName()` itself, hitting `ParserAnalysisController.getPredicates` (which
 * aggregates server-side via `PlsqlPredicatesFacet.build`, the same class the live-crawl backend
 * uses). `_applyScopeData`'s existing `if (src) {...}` branch needed no changes at all.
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
  const src = code(await read('plsql/js/predicates.js'));
  const start = src.indexOf('async load()');
  const end = src.indexOf('_initOpPills()', start);
  const body = src.slice(start, end);
  excludes(body, 'PA.api._parserName && PA.api._parserName()',
    'predicates.js\'s load() must not branch on backend anymore — the page source itself dispatches');
  excludes(body, '_src = null', 'predicates.js must always set a page source now, never null');
}

export async function test_api_predicates_page_source_dispatches_by_backend() {
  const src = code(await read('plsql/js/api.js'));
  includes(src, 'predicatesPageSource(params) {\n        const n = this._parserName();',
    'predicatesPageSource must dispatch by backend');
  includes(src, '/api/parser/analyses/${encodeURIComponent(n)}/predicates`',
    'the parser-backend predicates path must hit the new directProcId/nodeIds-capable endpoint');
}

function stubTf() {
  return { _state: {}, _restart() {}, setData() {}, init() {} };
}

async function loadPredicates(parserName) {
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
    predicatesPageSource: () => fakeSrc
  };
  new Function(await read('plsql/js/predicates.js'))();
  const predicates = globalThis.PA.predicates;
  predicates._initOpPills = () => {};
  predicates._initTable = () => {};
  predicates._updateScopeToggle = () => {};
  predicates._updateCounts = () => {};
  return { predicates, setParamCalls };
}

export async function test_load_never_fetches_the_whole_tables_array_for_the_parser_backend_either() {
  const { predicates } = await loadPredicates('some-analysis');
  await predicates.load();
  equal(predicates.data.length, 0, 'PA.predicates.data must stay empty — the fetch that used to populate it is gone');
  ok(predicates._src, 'a page source must always be set now, for either backend');
}

export async function test_scope_params_flow_through_unchanged_for_the_parser_backend() {
  const { predicates, setParamCalls } = await loadPredicates('some-analysis');
  await predicates.load();
  predicates.scopeMode = 'scoped';
  predicates._applyScopeData();
  const directProcIdCall = setParamCalls.find(([k]) => k === 'directProcId');
  ok(directProcIdCall, '_applyScopeData must still call src.setParam for directProcId, unchanged');
  equal(directProcIdCall[1], 'ERP$P1');
}
