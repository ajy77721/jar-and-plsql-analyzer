/**
 * Real bug: the page-level Direct/Subtree/SubFlow/Full toggle (`scope.js`) sits in the shared
 * detail header above every right-tab, including the Graph tab (`callGraphViz.js`), and
 * `app.js`'s `onScopeChange` does call `PA.callGraphViz.applyScope()` when it fires — but
 * `applyScope` used to ignore `PA._scope` entirely and just re-run `_applyVisibility()` with
 * whatever expand/collapse state was already set. So clicking Subtree/SubFlow/Full on the Graph
 * tab visibly did nothing, next to every other scope-aware tab (joins/sequences/tableDetail/...)
 * that does filter — read by the user as the toggle being "disabled" / producing an empty view.
 *
 * Fixed by scoping `applyScope` off `PA.context.callTreeNodeIds` / `PA._currentScopeId()` the same
 * way those other tabs do: direct = only the current node, subtree = its descendants excluding
 * itself, subflow = itself + descendants, full = everything loaded.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function loadCallGraphViz() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, querySelectorAll: () => [], addEventListener: () => {} };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.nodeIndex = {
    _byId: new Map(),
    byId(id) { return this._byId.get(id) || null; },
    ensureLoaded() { return Promise.resolve([]); }
  };
  new Function(await read('parser/js/callGraphViz.js'))();
  return globalThis.PA.callGraphViz;
}

function mockScope(currentId, treeIds, isPkgRoot) {
  globalThis.PA.context = { callTreeRootId: currentId, callTreeNodeIds: treeIds };
  globalThis.PA._currentScopeId = () => currentId;
  globalThis.PA._isPackageRoot = () => !!isPkgRoot;
}

const EDGES = [
  { fromNodeId: 'ERP$ROOT', toNodeId: 'ERP$CHILD1' },
  { fromNodeId: 'ERP$ROOT', toNodeId: 'ERP$CHILD2' },
  { fromNodeId: 'ERP$CHILD1', toNodeId: 'ERP$GRANDCHILD' }
];

export async function test_direct_scope_shows_only_the_current_node() {
  const viz = await loadCallGraphViz();
  await viz._buildGraph({ edges: EDGES });
  mockScope('ERP$ROOT', new Set(['ERP$ROOT', 'ERP$CHILD1', 'ERP$CHILD2', 'ERP$GRANDCHILD']));
  globalThis.PA._scope = 'direct';
  viz.applyScope();
  equal(viz._nodes.map(n => n.id).sort().join(','), 'ERP$ROOT', 'direct scope must isolate the current node only');
}

export async function test_subtree_scope_excludes_the_current_node_itself() {
  const viz = await loadCallGraphViz();
  await viz._buildGraph({ edges: EDGES });
  mockScope('ERP$ROOT', new Set(['ERP$ROOT', 'ERP$CHILD1', 'ERP$CHILD2', 'ERP$GRANDCHILD']));
  globalThis.PA._scope = 'subtree';
  viz.applyScope();
  const ids = viz._nodes.map(n => n.id).sort();
  equal(ids.join(','), 'ERP$CHILD1,ERP$CHILD2,ERP$GRANDCHILD', 'subtree scope must be descendants only, not the root');
}

export async function test_subflow_scope_includes_current_node_and_descendants() {
  const viz = await loadCallGraphViz();
  await viz._buildGraph({ edges: EDGES });
  mockScope('ERP$ROOT', new Set(['ERP$ROOT', 'ERP$CHILD1', 'ERP$CHILD2', 'ERP$GRANDCHILD']));
  globalThis.PA._scope = 'subflow';
  viz.applyScope();
  const ids = viz._nodes.map(n => n.id).sort();
  equal(ids.join(','), 'ERP$CHILD1,ERP$CHILD2,ERP$GRANDCHILD,ERP$ROOT', 'subflow scope must be the root plus its whole subtree');
}

export async function test_full_scope_shows_every_loaded_node() {
  const viz = await loadCallGraphViz();
  await viz._buildGraph({ edges: EDGES });
  mockScope('ERP$CHILD1', new Set(['ERP$CHILD1', 'ERP$GRANDCHILD']));
  globalThis.PA._scope = 'full';
  viz.applyScope();
  equal(viz._nodes.length, 4, 'full scope must show every node the graph loaded, regardless of the current node');
}

export async function test_package_root_context_always_behaves_as_full() {
  const viz = await loadCallGraphViz();
  await viz._buildGraph({ edges: EDGES });
  mockScope('__PKG_ROOT__', new Set(['ERP$CHILD1']), true);
  globalThis.PA._scope = 'direct';
  viz.applyScope();
  equal(viz._nodes.length, 4, 'package-root context has no single current node, so it must not collapse to one');
}

export async function test_applying_scope_before_toggle_previously_was_a_noop() {
  // Regression guard on the fix itself: applyScope's body must actually branch on PA._scope now.
  const src = await read('parser/js/callGraphViz.js');
  const start = src.indexOf('applyScope: function()');
  const body = src.slice(start, src.indexOf('_buildChildMap:', start));
  ok(body.includes('PA._scope'), 'applyScope must read PA._scope, not just re-run _applyVisibility() unchanged');
}
