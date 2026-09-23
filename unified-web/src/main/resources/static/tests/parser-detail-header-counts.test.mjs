/**
 * UI-024 — `parser/js/detailHeader.js`'s seven scope-derived stat-counting functions
 * (`_dhUniqueCallCount`/`_dhUniqueCallCountByType`/`_dhFullCallCountByType`/`_dhFullTriggerCount`/
 * `_dhFullCursorCount`/`_dhUniqueTriggerCount`/`_dhUniqueCursorCount`), each its own
 * `PA.nodeIndex.all()` scan, collapsed into one `PA.api.getDetailHeaderCounts` fetch per render.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_the_seven_scanning_functions_are_gone() {
  const src = code(await read('parser/js/detailHeader.js'));
  excludes(src, 'PA.nodeIndex.all()', 'no function in this file should scan the whole analysis anymore');
  for (const fn of ['_dhUniqueCallCount', '_dhUniqueCallCountByType', '_dhFullCallCountByType',
                     '_dhFullTriggerCount', '_dhFullCursorCount', '_dhUniqueTriggerCount', '_dhUniqueCursorCount']) {
    excludes(src, 'PA.' + fn + ' = function', fn + ' must be deleted, not just emptied');
  }
  includes(src, 'PA.api.getDetailHeaderCounts', '_dhComputeStats must fetch the server aggregate instead');
}

async function loadDetailHeader(countsByCall) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  const calls = [];
  globalThis.PA.api = {
    getDetailHeaderCounts: async (scope, procId) => {
      calls.push([scope, procId]);
      return countsByCall(scope, procId);
    }
  };
  globalThis.PA._currentScopeId = () => 'ERP$P1';
  globalThis.PA._isPackageRoot = () => false;
  globalThis.PA.onScopeChange = () => {};
  globalThis.PA.analysisData = { totalLinesOfCode: 1000, totalTables: 20, totalNodes: 50 };
  new Function(await read('parser/js/detailHeader.js'))();
  globalThis.PA._calls = calls;
  return globalThis.PA;
}

function counts(intCalls, extCalls, triggers, cursors) {
  return { intCalls, extCalls, totalCalls: intCalls + extCalls, triggers, cursors };
}

export async function test_direct_mode_needs_no_fetch_at_all() {
  const PA = await loadDetailHeader(() => counts(0, 0, 0, 0));
  PA._scope = 'direct';
  PA.currentDetail = { linesOfCode: 12, counts: { internalCalls: 3, externalCalls: 1, cursors: 2 }, objectType: 'PROCEDURE' };
  const stats = await PA._dhComputeStats();
  equal(PA._calls.length, 0, 'direct mode reads detail.counts directly, no server round trip needed');
  equal(stats.intCalls, 3);
  equal(stats.totalCalls, 4);
}

export async function test_subtree_mode_fetches_scope_subtree_for_the_current_node() {
  const PA = await loadDetailHeader((scope, procId) => {
    equal(scope, 'subtree');
    equal(procId, 'ERP$P1');
    return counts(5, 2, 1, 3);
  });
  PA._scope = 'subtree_or_whatever_falls_through';
  PA.currentDetail = { linesOfCode: 12, counts: {} };
  const stats = await PA._dhComputeStats();
  equal(stats.intCalls, 5);
  equal(stats.triggers, 1);
}

export async function test_subflow_mode_fetches_scope_subflow() {
  const PA = await loadDetailHeader((scope) => {
    equal(scope, 'subflow');
    return counts(9, 1, 0, 2);
  });
  PA._scope = 'subflow';
  PA.currentDetail = { linesOfCode: 12, counts: {} };
  const stats = await PA._dhComputeStats();
  equal(stats.mode, 'subflow');
  equal(stats.totalCalls, 10);
}

export async function test_full_mode_fetches_scope_full_with_no_procid() {
  const PA = await loadDetailHeader((scope, procId) => {
    equal(scope, 'full');
    equal(procId, null);
    return counts(20, 5, 3, 8);
  });
  PA._scope = 'full';
  PA.currentDetail = { linesOfCode: 12, counts: {} };
  const stats = await PA._dhComputeStats();
  equal(stats.totalCalls, 25);
  equal(stats.procs, 50, 'procs comes from PA.analysisData.totalNodes, unaffected by this fix');
}

export async function test_package_root_also_uses_scope_full() {
  const PA = await loadDetailHeader((scope, procId) => {
    equal(scope, 'full');
    equal(procId, null);
    return counts(20, 5, 3, 8);
  });
  PA._isPackageRoot = () => true;
  PA.currentDetail = { linesOfCode: 12, counts: {} };
  const stats = await PA._dhComputeStats();
  equal(stats.mode, 'pkg_root');
  equal(stats.totalCalls, 25);
}
