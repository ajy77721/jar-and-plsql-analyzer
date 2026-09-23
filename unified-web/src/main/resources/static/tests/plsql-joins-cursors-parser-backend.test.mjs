/**
 * MID-021 (§107) — `plsql/js/joins.js`/`cursors.js`'s parser-backend fallback used to fetch the
 * whole unpaged array (`PA.api.getJoinOperations`/`getCursorOperations`) and filter it client-side.
 * Converted to the same paged-source path already used for the live-crawl backend:
 * `joinsPageSource`/`cursorsPageSource` now dispatch by `_parserName()` themselves, and
 * `_applyScopeData`'s existing `if (src) {...}` branch (already sending `directProcId`/`nodeIds`)
 * needed no changes at all — it was already backend-agnostic once `_src` pointed at the right URL.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_load_no_longer_branches_on_backend_for_joins_and_cursors() {
  for (const file of ['plsql/js/joins.js', 'plsql/js/cursors.js']) {
    const src = code(await read(file));
    const start = src.indexOf('async load()');
    const end = src.indexOf('applyScope()', start);
    const body = src.slice(start, end);
    excludes(body, 'PA.api._parserName && PA.api._parserName()',
      file + '\'s load() must not branch on backend anymore — the page source itself dispatches');
    excludes(body, '_src = null', file + ' must always set a page source now, never null');
  }
}

export async function test_api_joins_and_cursors_page_sources_dispatch_by_backend() {
  const src = code(await read('plsql/js/api.js'));
  includes(src, "joinsPageSource(params) {\n        const n = this._parserName();", 'joinsPageSource must dispatch by backend');
  includes(src, "cursorsPageSource(params) {\n        const n = this._parserName();", 'cursorsPageSource must dispatch by backend');
  includes(src, '/api/parser/analyses/${encodeURIComponent(n)}/joins`', 'the parser-backend joins path must hit the new directProcId/nodeIds-capable endpoint');
  includes(src, '/api/parser/analyses/${encodeURIComponent(n)}/cursors`', 'the parser-backend cursors path must hit the new directProcId/nodeIds-capable endpoint');
}

function stubTf() {
  const calls = { setParam: [], restart: [] };
  return {
    calls,
    _state: {},
    _restart(id) { calls.restart.push(id); },
    setData() {}, init() {}
  };
}

async function loadJoins(parserName) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, querySelectorAll: () => [] };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.tf = stubTf();
  globalThis.PA.context = { procId: 'ERP$P1', callTreeNodeIds: new Set(['ERP$P1', 'ERP$P2']) };
  const setParamCalls = [];
  const fakeSrc = { setParam: (k, v) => setParamCalls.push([k, v]) };
  globalThis.PA.api = {
    _parserName: () => parserName,
    getJoinOperations: async () => { throw new Error('must not be called — the whole point of this fix'); },
    joinsPageSource: () => fakeSrc
  };
  new Function(await read('plsql/js/joins.js'))();
  const joins = globalThis.PA.joins;
  joins._initTypePills = () => {};
  joins._initTable = () => {};
  joins._updateScopeToggle = () => {};
  joins._updateCounts = () => {};
  return { joins, setParamCalls };
}

export async function test_load_never_fetches_the_whole_array_for_the_parser_backend_either() {
  const { joins } = await loadJoins('some-analysis');
  await joins.load();
  equal(joins.data.length, 0, 'PA.joins.data must stay empty — the fetch that used to populate it is gone');
  ok(joins._src, 'a page source must always be set now, for either backend');
}

export async function test_scope_params_flow_through_unchanged_for_the_parser_backend() {
  const { joins, setParamCalls } = await loadJoins('some-analysis');
  await joins.load();
  joins.scopeMode = 'scoped';
  joins._applyScopeData();
  const directProcIdCall = setParamCalls.find(([k]) => k === 'directProcId');
  ok(directProcIdCall, '_applyScopeData must still call src.setParam for directProcId, unchanged');
  equal(directProcIdCall[1], 'ERP$P1');
}
