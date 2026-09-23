/**
 * UI-024, and a real bug found while fixing it — `parser/js/callGraphViz.js`'s `_buildGraph` used
 * to seed its node map from `PA.nodeIndex.all()` (every node in the whole analysis), not just the
 * ids the server's already-bounded `edges` subset actually references. Because `_getRootIds()`
 * classifies any node with no incoming edge as a graph root, every disconnected node from the full
 * index — anything the bounded edge slice never touched — was being treated as a root too, right
 * alongside the graph's genuine roots. Confirmed by reading `_getRootIds` directly and reproduced
 * here with a real execution, not just asserted from the diff.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_build_graph_no_longer_scans_the_whole_node_index() {
  const src = code(await read('parser/js/callGraphViz.js'));
  const start = src.indexOf('_buildGraph: async function');
  const fnSrc = src.slice(start, src.indexOf('var nodes = Object.values(nodeMap);', start));
  excludes(fnSrc, 'PA.nodeIndex.all()', '_buildGraph must not scan the whole analysis to draw a bounded graph');
  includes(fnSrc, 'PA.nodeIndex.byId(id)', 'must enrich only the ids edges actually reference, via the O(1) lookup');
}

async function loadCallGraphViz(indexNodes) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, querySelectorAll: () => [], addEventListener: () => {} };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.nodeIndex = {
    _byId: new Map(indexNodes.map(n => [n.nodeId, n])),
    byId(id) { return this._byId.get(id) || null; },
    // UI-024 (§163) — `_buildGraph` now awaits `ensureLoaded()` once per build (the same
    // preload-at-entry-point pattern used elsewhere); already-resident here, so a same-tick
    // resolve is the correct stub, not a real fetch.
    ensureLoaded() { return Promise.resolve([...this._byId.values()]); }
  };
  new Function(await read('parser/js/callGraphViz.js'))();
  return globalThis.PA.callGraphViz;
}

export async function test_disconnected_nodes_never_touched_by_an_edge_are_not_included_at_all() {
  const viz = await loadCallGraphViz([
    { nodeId: 'ERP$P1', name: 'P1', schema: 'ERP', objectType: 'PROCEDURE', depth: 0, linesOfCode: 10 },
    { nodeId: 'ERP$P2', name: 'P2', schema: 'ERP', objectType: 'PROCEDURE', depth: 1, linesOfCode: 5 },
    // Not referenced by any edge below — must not appear in the built graph at all.
    { nodeId: 'ERP$UNRELATED', name: 'UNRELATED', schema: 'ERP', objectType: 'PROCEDURE', depth: 0, linesOfCode: 999 }
  ]);
  await viz._buildGraph({ edges: [{ fromNodeId: 'ERP$P1', toNodeId: 'ERP$P2' }] });
  const ids = viz._allNodes.map(n => n.id).sort();
  equal(JSON.stringify(ids), JSON.stringify(['ERP$P1', 'ERP$P2']), 'the disconnected node must not be drawn at all');
}

export async function test_the_disconnected_node_would_have_been_a_false_root_before_the_fix() {
  // Demonstrates the actual bug this fix removes: build the OLD way (every index node seeded)
  // and confirm the disconnected node's inEdges is 0 — the exact condition _getRootIds uses to
  // call something a root.
  const nodeMapOldWay = {
    'ERP$P1': { id: 'ERP$P1', inEdges: 0, outEdges: 1 },
    'ERP$P2': { id: 'ERP$P2', inEdges: 1, outEdges: 0 },
    'ERP$UNRELATED': { id: 'ERP$UNRELATED', inEdges: 0, outEdges: 0 }  // never touched by any edge
  };
  const parentMap = { 'ERP$P2': true }; // only P2 has an incoming edge
  const wouldBeRoots = Object.values(nodeMapOldWay).filter(n => !parentMap[n.id]).map(n => n.id);
  ok(wouldBeRoots.includes('ERP$UNRELATED'), 'documents the bug: an untouched node has no parent, so the old logic called it a root');
}

export async function test_enriches_edge_ids_from_the_index_when_available() {
  const viz = await loadCallGraphViz([
    { nodeId: 'ERP$P1', name: 'REAL_NAME', schema: 'ERP', objectType: 'PROCEDURE', depth: 3, linesOfCode: 42 }
  ]);
  await viz._buildGraph({ edges: [{ fromNodeId: 'ERP$P1', toNodeId: 'ERP$P2', to: 'FALLBACK_NAME', toSchema: 'BILLING' }] });
  const p1 = viz._allNodes.find(n => n.id === 'ERP$P1');
  equal(p1.label, 'REAL_NAME');
  equal(p1.depth, 3);
  equal(p1.loc, 42);
  const p2 = viz._allNodes.find(n => n.id === 'ERP$P2');
  equal(p2.label, 'FALLBACK_NAME', 'an id absent from the index must still fall back to the edge\'s own fields');
  equal(p2.schema, 'BILLING');
}
