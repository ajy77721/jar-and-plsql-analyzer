/**
 * UI-024 — `parser/js/statements.js`'s statement-type summary, converted from two
 * `PA.nodeIndex.all()` scans (`_aggregate`/`_aggregateScoped`) to fetching
 * `StatementsSummaryService`'s server-side aggregate (`PA.api.getStatementsSummary`).
 * `PA.statements.data` must stay the full/unscoped total (used by the type pills); only the table's
 * own row data is swapped per scope, exactly as the old code did with its two local functions.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_no_nodeindex_scan_remains() {
  const src = code(await read('parser/js/statements.js'));
  excludes(src, 'PA.nodeIndex.all()', 'statements.js must not scan the whole analysis client-side anymore');
  includes(src, "PA.api.getStatementsSummary('full', null)", 'load() must fetch the unscoped total from the server');
  includes(src, 'PA.api.getStatementsSummary(mode, PA._currentScopeId())', 'applyScope() must fetch the scoped total from the server');
}

function stubTf() {
  return {
    _states: {},
    state(id) { return this._states[id] || (this._states[id] = { colFilters: {}, filtered: [] }); },
    init(id, data) { this._states[id] = this._states[id] || { colFilters: {}, filtered: [] }; this._states[id].data = data; },
    setData(id, data) { this.state(id).data = data; },
    filter() {}, sort() {}, initColFilters() {}, _updateSortIndicators() {}
  };
}

async function loadStatements(summaryByCall) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => ({ innerHTML: '', style: {} }), querySelectorAll: () => [] };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.tf = stubTf();
  globalThis.PA.esc = (s) => s;
  globalThis.PA.escAttr = (s) => s;
  globalThis.PA.escJs = (s) => s;
  globalThis.PA.getSchemaColor = () => ({ bg: '#000', fg: '#fff' });
  globalThis.PA._scope = 'full';
  globalThis.PA._currentScopeId = () => 'ERP$P1';
  globalThis.PA.context = { procId: null };
  let call = 0;
  globalThis.PA.api = {
    getStatementsSummary: async (scope, procId) => summaryByCall(scope, procId, call++)
  };
  new Function(await read('parser/js/statements.js'))();
  return globalThis.PA.statements;
}

export async function test_load_fetches_the_full_unscoped_total_once() {
  const calls = [];
  const statements = await loadStatements((scope, procId) => {
    calls.push([scope, procId]);
    return [{ type: 'SELECT', totalCount: 5, procCount: 2, procs: [] }];
  });
  await statements.load();
  equal(calls[0][0], 'full');
  equal(calls[0][1], null);
  equal(statements.data.length, 1);
  equal(statements.data[0].type, 'SELECT');
}

export async function test_apply_scope_fetches_scoped_data_but_leaves_the_full_total_alone() {
  const statements = await loadStatements((scope) =>
    scope === 'full'
      ? [{ type: 'SELECT', totalCount: 5, procCount: 2, procs: [] }]
      : [{ type: 'SELECT', totalCount: 2, procCount: 1, procs: [] }]
  );
  await statements.load();
  equal(statements.data[0].totalCount, 5, 'the full total must still be 5 after load');

  PA.context.procId = 'ERP$P1';
  PA._scope = 'direct';
  await statements.applyScope();
  const scopedState = PA.tf.state('stmt');
  equal(scopedState.data[0].totalCount, 2, 'the table\'s own row data must reflect the scoped fetch');
  equal(statements.data[0].totalCount, 5, 'PA.statements.data itself (used by the type pills) must remain the unscoped total');
}

export async function test_full_scope_uses_the_already_loaded_data_without_a_second_fetch() {
  let fetchCount = 0;
  const statements = await loadStatements(() => {
    fetchCount++;
    return [{ type: 'SELECT', totalCount: 5, procCount: 2, procs: [] }];
  });
  await statements.load();
  const countAfterLoad = fetchCount;
  PA.context.procId = 'ERP$P1';
  PA._scope = 'full';
  await statements.applyScope();
  equal(fetchCount, countAfterLoad, 'scope=full must reuse PA.statements.data, not fetch again');
}
