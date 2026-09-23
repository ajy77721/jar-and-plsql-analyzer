/**
 * UI-021 — the JAR trace tree has a node ceiling and says when it uses it.
 *
 * <h2>What was wrong</h2>
 *
 * Two unbounded walks over the same tree:
 *
 *   - `_renderTraceNode` recursed the whole tree into one HTML string. The comment in `showTrace`
 *     measures that render at **120 MB**, which is why repeat clicks were made a no-op — the
 *     clicking was a symptom, and refusing the second click left the first one unbounded.
 *   - `_flattenTree` guarded `depth > 50` and left breadth alone. A tree 50 deep is small; a tree
 *     three deep with a hundred thousand children per level is not, and one measured endpoint
 *     expands to 300,167,238 nodes. This is the same defect `_flattenOps` had (FE-018): capping
 *     the dimension that was not the problem.
 *
 * <h2>Why the total is asserted, not just the cap</h2>
 *
 * A capped tree and a small tree render identically. "Showing 2,000 calls" cannot be told apart
 * from "this endpoint has 2,000 calls", so the notice has to carry the real total — and so does
 * the node-nav fallback, whose `totalNodes` would otherwise report the size of the slice.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

/** Wide rather than deep, because breadth is the dimension the old depth guard missed. */
const BREADTH = 5000;

function sandbox() {
  globalThis.JA = {
    utils: { escapeHtml: (s) => String(s == null ? '' : s) },
    toast: { warn() {}, info() {}, loading: null },
    mountOverlay() {},
    app: { currentJarId: '' },
  };
  globalThis.PA = { esc: (s) => String(s == null ? '' : s) };
  return globalThis.JA;
}

async function loadSummary() {
  // All three files, in order. `JA.summary` is assembled across the `summary-*` scripts, and
  // `_renderTraceNode` reaches for two siblings that are not in the file under test:
  // `_jarToProject` (summary-helpers.js) and `_dispatchBadge` (summary-explorer.js). Loading only
  // summary-trace.js failed with "this._jarToProject is not a function" and then
  // "this._dispatchBadge is not a function" — each reads like a product fault and is a missing
  // collaborator. Loaded rather than stubbed, so the rows counted below are the rows the product
  // emits; a stub returning '' could hide a row that stopped being rendered at all.
  new Function(await read('jar/js/summary-helpers.js'))();
  new Function(await read('jar/js/summary-explorer.js'))();
  new Function(await read('jar/js/summary-trace.js'))();
  return globalThis.JA.summary;
}

/** A shallow, very wide tree: depth 2, `BREADTH` leaves. */
function wideTree(breadth) {
  return {
    className: 'com.x.Root', simpleClassName: 'Root', methodName: 'handle',
    children: Array.from({ length: breadth }, (_, i) => ({
      className: 'com.x.C' + i, simpleClassName: 'C' + i, methodName: 'm' + i, children: [],
    })),
  };
}

export async function test_the_render_stops_at_the_ceiling() {
  const JA = sandbox();
  const summary = await loadSummary();
  ok(summary, 'summary-trace.js did not define JA.summary');
  ok(Number.isFinite(summary.MAX_TRACE_NODES), 'the trace tree has no finite node ceiling');

  const esc = JA.utils.escapeHtml;
  const budget = { left: summary.MAX_TRACE_NODES };
  const html = summary._renderTraceNode(wideTree(BREADTH), 'jar', esc, 0, budget);

  equal(budget.left, 0, 'the budget was not consumed, so the ceiling is not being applied');
  // One row per node, so row count is a proxy for nodes rendered. Asserted as "fewer than the
  // tree", not an exact number: the row markup may gain elements without this becoming wrong.
  const rows = (html.match(/sum-trace-node/g) || []).length;
  ok(rows <= summary.MAX_TRACE_NODES,
    `rendered ${rows} rows, ceiling is ${summary.MAX_TRACE_NODES}`);
  ok(rows < BREADTH, `rendered ${rows} of ${BREADTH} nodes — the ceiling did not bite`);
}

export async function test_a_wide_tree_is_bounded_not_just_a_deep_one() {
  // The specific regression: a depth guard that passes while breadth runs away.
  const JA = sandbox();
  const summary = await loadSummary();
  const list = [];
  summary._flattenTree(wideTree(BREADTH), list, 0);

  ok(list.length <= summary.MAX_TRACE_NODES,
    `flattened ${list.length} nodes, ceiling is ${summary.MAX_TRACE_NODES}`);
  ok(list.truncated, 'the flattener hit its ceiling without recording that it did');
}

export async function test_a_small_tree_is_not_truncated() {
  // A cap that fires on everything is a cap nobody trusts. The honest direction of calibration.
  const JA = sandbox();
  const summary = await loadSummary();
  const list = [];
  summary._flattenTree(wideTree(3), list, 0);

  equal(list.length, 4, 'a four-node tree did not flatten to four rows');
  ok(!list.truncated, 'a tree well under the ceiling was reported as truncated');
}

export async function test_the_whole_tree_can_still_be_counted() {
  const summary = await loadSummary();
  ok(typeof summary._countTreeNodes === 'function', 'there is no whole-tree count');
  equal(summary._countTreeNodes(wideTree(BREADTH)), BREADTH + 1,
    'the count is wrong, so the truncation notice would understate the tree');
  equal(summary._countTreeNodes(null), 0, 'an absent tree is not handled');

  // `recursive` nodes are counted and not followed — the renderer's rule. Without it a recursive
  // trace would not terminate here.
  const cyc = { className: 'a', children: [] };
  cyc.children.push({ className: 'b', recursive: true, children: [cyc] });
  equal(summary._countTreeNodes(cyc), 2, 'a recursive node was followed');
}

export async function test_the_user_is_told_the_tree_was_cut() {
  // Emitted from showTrace, which needs an endpoint report and an overlay, so this is asserted on
  // the source rather than by reconstructing one. The ceiling and the notice are asserted as a
  // pair everywhere in this suite: a silent cap turns a slow correct answer into a fast wrong one.
  const src = await read('jar/js/summary-trace.js');
  includes(src, 'cg-truncated', 'the trace view emits no truncation notice');
  includes(src, '_countTreeNodes(r.endpoint.callTree)',
    'the notice does not report the real total, so it cannot say what was cut');
  includes(src, 'budget.left <= 0', 'the notice is not tied to the budget actually running out');
}

export async function test_the_node_nav_reports_the_trees_size_not_the_slices() {
  const src = await read('jar/js/summary-trace.js');
  includes(src, 'flatNodes.truncated',
    'the node-nav fallback ignores whether the flatten was truncated, so totalNodes would '
    + 'report the size of the slice');
}

export async function test_both_jar_tree_views_stop_at_the_same_size() {
  // They sit beside each other. Different ceilings would be a difference a reader has to discover.
  const trace = await read('jar/js/summary-trace.js');
  const graph = await read('jar/js/callGraph.js');
  const a = trace.match(/MAX_TRACE_NODES:\s*(\d+)/);
  const b = graph.match(/MAX_FLATTENED_OPS:\s*(\d+)/);
  ok(a && b, 'one of the two views lost its named ceiling');
  equal(Number(a[1]), Number(b[1]),
    `the trace tree stops at ${a[1]} and the operation list at ${b[1]}`);
  atLeast(Number(a[1]), 100, 'too small a ceiling makes the view useless');
}
