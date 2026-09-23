/**
 * UI-024/refs.js bug fix — the "Calls" tab used to list every procedure in the whole analysis
 * (`PA.nodeIndex.all()`, a directory) instead of the current procedure's actual calls/callers.
 * Confirmed as a bug (not intended behavior) by the user; fixed by fetching this one node's own
 * call tree/caller tree instead. Real execution, against fixtures shaped exactly like the parser
 * backend's real call-tree children (`id`/`name`/`schemaName`/`packageName`/`callType`/
 * `callLineNumber`/`sourceFile`), the same shape `callGraph.js`/`sourceView.js` already read.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_load_no_longer_reads_the_whole_node_index() {
  const src = code(await read('parser/js/refs.js'));
  excludes(src, 'PA.nodeIndex.all()', 'refs.js must not scan the whole analysis for a per-procedure calls tab');
  includes(src, 'PA.api.getCallTree(procId, 1)', 'must fetch this node\'s own call tree instead');
  includes(src, 'PA.api.getCallerTree(procId)', 'must fetch this node\'s own caller tree instead');
}

/**
 * The depth argument is not an optimisation, it is the correctness fix.
 *
 * <p>This view reads only the root's DIRECT children, but it used to request the whole subtree.
 * The server caps a call tree at MAX_CALL_TREE_NODES (2000) and spends that budget depth-first, so
 * on a wide/deep root the cap is reached while descending the FIRST few children and the root's
 * own remaining direct children are never built at all. Measured against the loaded demo
 * analyses, References listed 13 of ERP_USP_RUN_CYCLE's 18 callees on Postgres and 4 of 19 on
 * Oracle — with no truncation marker, because the cut happened deep in a subtree this view never
 * displays. Asking for one level can't hit the cap, so the list is exact.
 */
export async function test_calls_tab_asks_for_one_level_so_the_node_budget_cannot_eat_its_siblings() {
  const src = code(await read('parser/js/refs.js'));
  excludes(src, 'PA.api.getCallTree(procId);',
    'an unbounded subtree fetch lets the server node budget silently drop direct callees');
}

function child(id, name, schema, callType, line) {
  return { id, name, schemaName: schema, packageName: '', callType, callLineNumber: line, sourceFile: schema + '.sql' };
}

function stubTf() {
  return {
    _states: {},
    state(id) { return this._states[id] || (this._states[id] = { colFilters: {}, filtered: [] }); },
    init(id) { this._states[id] = this._states[id] || { colFilters: {}, filtered: [] }; },
    filter() {}, sort() {}, initColFilters() {}, _updateSortIndicators() {}
  };
}

async function loadRefs({ callTree, callerTree } = {}) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => ({ innerHTML: '', style: {} }), querySelectorAll: () => [] };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.tf = stubTf();
  globalThis.PA.getSchemaColor = () => ({ bg: '#000', fg: '#fff' });
  globalThis.PA.api = {
    getCallTree: async () => callTree,
    getCallerTree: async () => callerTree
  };
  new Function(await read('parser/js/refs.js'))();
  return globalThis.PA.refs;
}

export async function test_calls_tab_shows_only_this_procedures_direct_calls_not_the_whole_analysis() {
  const refs = await loadRefs({
    callTree: {
      id: 'ERP$P1', name: 'P1', children: [
        child('ERP$P2', 'P2', 'ERP', 'INTERNAL', 10),
        child('BILLING$P3', 'P3', 'BILLING', 'EXTERNAL', 20)
      ]
    },
    callerTree: { id: 'ERP$P1', name: 'P1', children: [] }
  });
  await refs.load('ERP$P1');
  await new Promise(function(r) { setTimeout(r, 5); }); // flush _buildTable's own setTimeout(...,0) before the next test file runs
  equal(refs.callsItems.length, 2, 'only this procedure\'s two direct calls, not every procedure in the analysis');
  const names = refs.callsItems.map(it => it.node.name).sort();
  equal(JSON.stringify(names), JSON.stringify(['P2', 'P3']));
}

export async function test_callers_tab_is_populated_from_the_caller_tree_not_the_calls_tree() {
  const refs = await loadRefs({
    callTree: { id: 'ERP$P1', name: 'P1', children: [] },
    callerTree: { id: 'ERP$P1', name: 'P1', children: [child('ERP$P0', 'P0', 'ERP', 'INTERNAL', 5)] }
  });
  await refs.load('ERP$P1');
  await new Promise(function(r) { setTimeout(r, 5); }); // flush _buildTable's own setTimeout(...,0) before the next test file runs
  equal(refs.callsItems.length, 0);
  equal(refs.callersItems.length, 1);
  equal(refs.callersItems[0].node.name, 'P0');
}

export async function test_a_failed_call_tree_fetch_leaves_calls_empty_rather_than_throwing() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => ({ innerHTML: '', style: {} }), querySelectorAll: () => [] };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.tf = stubTf();
  globalThis.PA.getSchemaColor = () => ({ bg: '#000', fg: '#fff' });
  globalThis.PA.api = {
    getCallTree: async () => { throw new Error('network error'); },
    getCallerTree: async () => ({ id: 'ERP$P1', children: [] })
  };
  new Function(await read('parser/js/refs.js'))();
  const refs = globalThis.PA.refs;
  await refs.load('ERP$P1');
  await new Promise(function(r) { setTimeout(r, 5); }); // flush _buildTable's own setTimeout(...,0) before the next test file runs
  ok(refs._loaded, 'a failed calls-tree fetch must not abort the whole panel load');
  equal(refs.callsItems.length, 0);
}

export async function test_duplicate_call_sites_to_the_same_target_are_merged_with_all_their_lines() {
  const refs = await loadRefs({
    callTree: {
      id: 'ERP$P1', children: [
        child('ERP$P2', 'P2', 'ERP', 'INTERNAL', 10),
        child('ERP$P2', 'P2', 'ERP', 'INTERNAL', 25)
      ]
    },
    callerTree: { id: 'ERP$P1', children: [] }
  });
  await refs.load('ERP$P1');
  await new Promise(function(r) { setTimeout(r, 5); }); // flush _buildTable's own setTimeout(...,0) before the next test file runs
  equal(refs.callsItems.length, 1, 'two calls to the same target must be one row');
  equal(refs.callsItems[0].count, 2);
  equal(JSON.stringify(refs.callsItems[0].lines.sort()), JSON.stringify([10, 25]));
}
