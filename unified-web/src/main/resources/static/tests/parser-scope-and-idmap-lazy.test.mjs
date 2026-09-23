/**
 * UI-024 (§162) — `PA.getScopedNodes`/`PA._buildNodeIdMap` converted to the lazy `PA.nodeIndex
 * .ensureLoaded()` pattern §161 built but declined to apply to these two, on the assumption they
 * had dozens of synchronous callers. Traced for real this time (`grep -rn`, not re-reading the old
 * estimate): `getScopedNodes` has exactly one real caller (`export.js`'s own `_gather`, already
 * `async`); `_buildNodeIdMap`'s only real callers are `app.js`'s `PA.showProcedure`/`PA
 * .loadNodeDetail` (both already `async`) and `PA._collectTreeNodeIds` (which now always receives
 * a pre-resolved `idMap` instead of building one internally, so it stays synchronous). No cascade
 * beyond those four real call sites.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const NODES = [
  { nodeId: 'PKG$A', name: 'PROC_A', detailFile: 'nodes/PKG$A.json' },
  { nodeId: 'PKG$B', name: 'PROC_B', detailFile: 'nodes/PKG_B_PROC_B.json' },
];

async function loadScopeLazy(fetchImpl) {
  installDom();
  globalThis.PA = {
    analysisData: { name: 'A1' },
    api: { getProcedures: fetchImpl },
    context: { procId: 'PKG$A', callTreeRootId: 'PKG$A', callTreeNodeIds: new Set() },
    nodeIndex: null,
  };
  new Function(await read('parser/js/nodeIndex.js'))();
  new Function(await read('parser/js/scope.js'))();
  // scope.js itself sets PA._scope = 'direct' on load — override after, not before.
  globalThis.PA._scope = 'full';
  return globalThis.PA;
}

export async function test_getScopedNodes_awaits_ensureLoaded_before_reading() {
  const PA = await loadScopeLazy(async () => NODES);
  ok(!Array.isArray(PA.analysisData.nodes), 'nodes must not be resident before the call');
  const scoped = await PA.getScopedNodes();
  equal(scoped.length, NODES.length, 'full scope must return every node once the lazy fetch resolves');
  ok(Array.isArray(PA.analysisData.nodes), 'analysisData.nodes must be populated as a side effect');
}

/**
 * Calibrated by reverting `getScopedNodes` to its old synchronous body (`PA.nodeIndex.all()`
 * instead of `await PA.nodeIndex.ensureLoaded()`) and confirming the exact expected failure — a
 * call made before anything else populated the array returns 0 nodes instead of the real 2 — before
 * restoring the fix.
 */
export async function test_getScopedNodes_actually_waits_rather_than_returning_empty() {
  const PA = await loadScopeLazy(async () => NODES);
  const scoped = await PA.getScopedNodes();
  equal(scoped.length, 2, 'must resolve the real nodes, not an empty array read before the fetch completed');
}

async function loadUtilsLazy(fetchImpl) {
  installDom();
  globalThis.PA = {
    analysisData: { name: 'A1' },
    api: { getProcedures: fetchImpl },
    nodeIndex: null,
  };
  new Function(await read('parser/js/nodeIndex.js'))();
  new Function((await read('parser/js/utils.js')).match(
    /PA\._buildNodeIdMap = async function[\s\S]*?\n};/)[0])();
  new Function((await read('parser/js/utils.js')).match(
    /PA\._collectTreeNodeIds = function[\s\S]*?\n};/)[0])();
  return globalThis.PA;
}

export async function test_buildNodeIdMap_awaits_ensureLoaded_before_reading() {
  const PA = await loadUtilsLazy(async () => NODES);
  ok(!Array.isArray(PA.analysisData.nodes), 'nodes must not be resident before the call');
  const map = await PA._buildNodeIdMap();
  equal(map['PKG$B'], 'PKG_B_PROC_B', 'the dollar-id mapping must be built from the lazily-fetched nodes');
}

export async function test_collectTreeNodeIds_uses_the_explicitly_passed_idMap_not_a_stale_default() {
  const PA = await loadUtilsLazy(async () => NODES);
  const idMap = await PA._buildNodeIdMap();
  const tree = { id: 'PKG$B', children: [] };
  const set = PA._collectTreeNodeIds(tree, undefined, idMap);
  ok(set.has('PKG$B'), 'the raw id must be in the set');
  ok(set.has('PKG_B_PROC_B'), 'the dollar-format equivalent from the resolved idMap must also be in the set');
}

/**
 * Calibrated by reverting `_collectTreeNodeIds` to its old body (an internal
 * `if (idMap === undefined) idMap = PA._buildNodeIdMap()` fallback) with `_buildNodeIdMap` async —
 * confirmed the exact expected failure: `idMap` becomes a Promise object, `idMap[rawId]` is always
 * `undefined`, so no dollar-id equivalent is ever added — a plausible-looking wrong answer, not a
 * crash. Restored to the version that requires callers to pass a pre-resolved map.
 */
export async function test_collectTreeNodeIds_warns_rather_than_silently_treating_a_promise_as_a_map() {
  const PA = await loadUtilsLazy(async () => NODES);
  const tree = { id: 'PKG$B', children: [] };
  const originalWarn = console.warn;
  let warned = false;
  console.warn = () => { warned = true; };
  try {
    const set = PA._collectTreeNodeIds(tree); // no idMap passed — the caller-bug path
    ok(!set.has('PKG_B_PROC_B'), 'without a resolved idMap, no dollar-id equivalent can be added');
    ok(warned, 'the caller-bug path must warn loudly instead of silently degrading');
  } finally {
    console.warn = originalWarn;
  }
}
