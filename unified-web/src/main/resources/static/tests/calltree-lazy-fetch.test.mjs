/**
 * UI-019 (§142) — `parser/js/callGraph.js`'s per-level lazy fetch. Ordinary browsing now fetches
 * `_fetchDepth()` levels at a time instead of the whole tree; `toggle()` fetches a node's own next
 * slice on demand when it was cut server-side (`moreChildren`); `expandAll()`/`search()` fall back
 * to a full fetch first, since both need the whole tree in memory (UI-016/UI-018's own established
 * reasoning), unless the tree already has nothing left to fetch (`_hasUnfetchedChildren`).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function sandbox(getCallTreeImpl) {
  const dom = installDom();
  globalThis.PA = {
    esc: (s) => String(s == null ? '' : s),
    escAttr: (s) => String(s == null ? '' : s),
    escJs: (s) => String(s == null ? '' : s),
    showProcedure: () => {},
    // UI-024 (§164): render() now awaits ensureLoaded() once before the walk begins.
    nodeIndex: { count: () => 0, resolve: () => null, ensureLoaded: () => Promise.resolve([]) },
    api: { getCallTree: getCallTreeImpl || (async () => { throw new Error('must not be called'); }) },
  };
  globalThis.MdbColor = { get: () => '#000', alpha: () => '#000' };
  globalThis.requestAnimationFrame = () => 0;
  globalThis.confirm = () => false;
  return dom;
}

async function loadTrace() {
  new Function(await read('parser/js/callGraph.js'))();
  return globalThis.PA.callTrace;
}

export async function test_a_depth_cut_node_still_shows_an_expand_toggle() {
  sandbox();
  const trace = await loadTrace();
  const node = { id: 'n', name: 'n', callType: 'STATIC', children: [], moreChildren: true, childCount: 7 };
  trace._nodeMap = {};
  trace.stepCounter = 0;
  const html = trace.renderNode(node, 0, null);
  ok(!html.includes('visibility:hidden'), 'a node with real, unfetched children must show a live toggle, not a hidden placeholder');
  ok(html.includes('(7)'), 'the child count badge must come from childCount when children[] is empty');
}

export async function test_a_genuinely_childless_node_shows_no_toggle() {
  sandbox();
  const trace = await loadTrace();
  const node = { id: 'n', name: 'n', callType: 'STATIC', children: [] };
  trace._nodeMap = {};
  trace.stepCounter = 0;
  const html = trace.renderNode(node, 0, null);
  ok(html.includes('visibility:hidden'), 'a node with no children at all, and no moreChildren, must not offer to expand');
}

export async function test_hasUnfetchedChildren_finds_a_cut_node_anywhere_in_the_tree() {
  sandbox();
  const trace = await loadTrace();
  const deep = { id: 'deep', children: [], moreChildren: true };
  const root = { id: 'root', children: [{ id: 'mid', children: [deep] }] };
  ok(trace._hasUnfetchedChildren(root), 'a moreChildren node three levels down must still be found');
}

export async function test_hasUnfetchedChildren_is_false_for_a_complete_tree() {
  sandbox();
  const trace = await loadTrace();
  const root = { id: 'root', children: [{ id: 'mid', children: [{ id: 'leaf', children: [] }] }] };
  equal(trace._hasUnfetchedChildren(root), false, 'a tree with no moreChildren anywhere is already complete');
}

export async function test_hasUnfetchedChildren_does_not_follow_a_circular_node() {
  sandbox();
  const trace = await loadTrace();
  const cyc = { id: 'a', children: [] };
  cyc.children.push({ id: 'b', circular: true, moreChildren: true, children: [cyc] });
  // moreChildren on the circular node itself is still found (it's the node being checked, not
  // something followed into) — the rule mirrors _countNodes: don't recurse THROUGH a circular
  // node's own children, but the node itself is still inspected.
  ok(trace._hasUnfetchedChildren(cyc), 'the circular node itself still reports its own moreChildren');
}

export async function test_ensureFullyLoaded_skips_the_network_when_nothing_is_missing() {
  let called = false;
  sandbox(async () => { called = true; return null; });
  const trace = await loadTrace();
  trace._root = { id: 'root', children: [{ id: 'leaf', children: [] }] };
  trace._fullyLoaded = false;
  await trace._ensureFullyLoaded();
  equal(called, false, 'a tree with nothing left to fetch must not trigger a network call');
  equal(trace._fullyLoaded, true, 'it should still be marked fully loaded once confirmed complete');
}

export async function test_ensureFullyLoaded_fetches_the_whole_tree_when_something_is_missing() {
  let requestedId = null;
  const fullTree = { id: 'root', children: [{ id: 'mid', children: [{ id: 'leaf', children: [] }] }] };
  const dom = sandbox(async (id) => { requestedId = id; return fullTree; });
  const trace = await loadTrace();
  dom.add('ctContainer'); dom.add('exploreSearch'); dom.add('breadcrumb'); dom.add('btnAnalysisRoot');
  trace._root = { id: 'root', children: [{ id: 'mid', children: [], moreChildren: true, childCount: 1 }] };
  trace._fullyLoaded = false;

  await trace._ensureFullyLoaded();

  equal(requestedId, 'root', 'must re-fetch rooted at the current tree\'s own id');
  equal(trace._fullyLoaded, true);
  equal(trace._root.id, 'root');
  equal(trace._root.children[0].children.length, 1, 'the fetched full tree must replace _root');
}

export async function test_fetchNextSlice_splices_in_the_new_children_and_updates_moreChildren() {
  const sub = { id: 'mid', children: [{ id: 'g1', children: [] }, { id: 'g2', children: [], moreChildren: true, childCount: 3 }] };
  sandbox(async () => sub);
  const trace = await loadTrace();
  const node = { id: 'mid', children: [], moreChildren: true, childCount: 2 };

  await trace._fetchNextSlice(node);

  equal(node.children.length, 2, 'the fetched children must replace the empty placeholder array');
  equal(node.moreChildren, undefined, 'a node whose own fetch fully resolved its immediate children must drop moreChildren');
  ok(node.children[1].moreChildren, 'a grandchild the slice itself could not fully resolve keeps its own moreChildren');
}

export async function test_api_getCallTree_sends_maxDepth_only_when_given() {
  const src = await read('parser/js/api.js');
  ok(src.includes('async getCallTree(nodeId, maxDepth)'), 'getCallTree must accept an optional maxDepth');
  ok(src.includes('maxDepth=${encodeURIComponent(maxDepth)}'), 'maxDepth must travel as a query parameter');
  ok(src.includes('maxDepth !== undefined && maxDepth !== null'),
    'omitting maxDepth must leave the URL exactly as every existing caller already relies on');
}

export async function test_load_and_drillDown_no_longer_fetch_the_whole_tree_up_front() {
  const src = await read('parser/js/callGraph.js');
  ok(src.includes('PA.api.getCallTree(procId, PA.callTrace._fetchDepth())'),
    'load()/makeRoot() must fetch a bounded depth, not the whole tree, by default');
  ok(src.includes('PA.api.getCallTree(item.id, PA.callTrace._fetchDepth())'),
    'goTo() must do the same for a breadcrumb jump');
}
