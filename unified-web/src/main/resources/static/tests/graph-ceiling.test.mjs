/**
 * FE-018 — every call-graph renderer has a hard node ceiling and says when it uses it.
 *
 * <h2>What was wrong</h2>
 *
 * Three renderers walked whatever the analysis produced:
 *
 *   - `parser/js/callGraphViz.js` ran an all-pairs repulsion step for 200 frames. That is ~20,000
 *     operations at the 15 nodes anyone tested with, and **6.4 billion** at 8,000 — not slow, but
 *     a hung tab.
 *   - `jar/js/callGraph.js` capped *indentation* at a depth limit while `_flattenOps` kept
 *     recursing. Breadth is where the size is: one measured endpoint expands to 300,167,238 nodes.
 *   - `plsql/js/app.js` had a "show whole flow" button that set the budget to
 *     `Number.MAX_SAFE_INTEGER`. A ceiling a button can remove is not a ceiling.
 *
 * <h2>Why the notice is asserted, not just the cap</h2>
 *
 * A capped graph and a small graph look identical on screen. Without a notice the reader cannot
 * tell "this system has 300 procedures" from "this system has 8,000 and you are seeing 300" — the
 * cap turns a slow correct answer into a fast wrong one. So each assertion here comes in a pair:
 * the ceiling holds, and the user is told it held.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal, includes, excludes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

/** The size the definition of done names. Deliberately far above every ceiling under test. */
const BIG = 8000;
/** The definition of done's budget for one render. */
const BUDGET_MS = 3000;

/** A fresh global for one file, so tests cannot leak state into each other. */
function sandbox() {
  const dom = installDom();
  globalThis.PA = { esc: (s) => String(s == null ? '' : s) };
  globalThis.JA = { utils: { escapeHtml: (s) => String(s == null ? '' : s) } };
  globalThis.MdbColor = { get: () => '#000', alpha: () => '#000' };
  globalThis.requestAnimationFrame = () => 0;
  return dom;
}

async function load(rel) {
  // The real file, not a copy: a ceiling asserted against a transcription is a ceiling that can
  // drift out of the product without any test noticing.
  new Function(await read(rel))();
}

// ---------------------------------------------------------------------------------------------
// parser — force-directed layout
// ---------------------------------------------------------------------------------------------

export async function test_parser_layout_keeps_only_the_most_connected_nodes() {
  sandbox();
  await load('parser/js/callGraphViz.js');
  const viz = globalThis.PA.callGraphViz;

  ok(Number.isFinite(viz.MAX_SIMULATED_NODES), 'the parser layout has no finite node ceiling');

  // Degree is the selection criterion, so the fixture must have a definite answer: node 0 is
  // wired to everything and must survive any correct truncation.
  const nodes = Array.from({ length: BIG }, (_, i) => ({ id: 'n' + i, depth: 1 }));
  const edges = [];
  for (let i = 1; i < BIG; i++) edges.push({ from: 'n0', to: 'n' + i });

  const started = Date.now();
  const kept = viz._topByDegree(nodes, edges, viz.MAX_SIMULATED_NODES);
  const elapsed = Date.now() - started;

  equal(kept.length, viz.MAX_SIMULATED_NODES, 'truncation did not reduce to the ceiling');
  ok(kept.some((n) => n.id === 'n0'), 'the most-connected node was dropped');
  ok(elapsed < BUDGET_MS, `selecting from ${BIG} nodes took ${elapsed}ms`);
}

export async function test_parser_layout_tells_the_user_it_truncated() {
  const dom = sandbox();
  await load('parser/js/callGraphViz.js');
  const viz = globalThis.PA.callGraphViz;

  const container = dom.add('graphContainer');
  const canvas = dom.add('graphCanvas');
  container.appendChild(canvas);
  viz._canvas = canvas;

  // Driven through `_simulate`, not by calling the notice function directly. Calling it directly
  // proves the function works and nothing more: when the single call site was deleted as a
  // calibration check, the direct version still passed while the product showed no notice at all.
  // `requestAnimationFrame` is a no-op here, so the layout frames never run — the notice is
  // rendered in the synchronous preamble, which is exactly the part under test.
  viz._nodes = Array.from({ length: BIG }, (_, i) => ({ id: 'n' + i, depth: 1, x: 0, y: 0 }));
  viz._edges = Array.from({ length: BIG - 1 }, (_, i) => ({ from: 'n0', to: 'n' + (i + 1) }));
  viz._simulate();

  const notice = document.getElementById('cgTruncatedNotice');
  ok(notice, 'nothing told the user the graph was truncated');
  includes(notice.className, 'cg-truncated', 'the notice is not styled as one');
  includes(notice.textContent, String(BIG), 'the notice does not say how many nodes exist');
  includes(notice.textContent, String(viz.MAX_SIMULATED_NODES),
    'the notice does not say how many are shown');

  // And it must disappear again, or every later graph inherits a stale warning. Also through
  // `_simulate`, for the same reason.
  viz._nodes = Array.from({ length: 10 }, (_, i) => ({ id: 's' + i, depth: 1, x: 0, y: 0 }));
  viz._edges = [];
  viz._simulate();
  ok(!document.getElementById('cgTruncatedNotice'),
    'the notice survived a graph that was not truncated');
}

// ---------------------------------------------------------------------------------------------
// jar — flattened operation list
// ---------------------------------------------------------------------------------------------

export async function test_jar_flatten_stops_at_a_node_count() {
  sandbox();
  await load('jar/js/callGraph.js');
  const cg = globalThis.JA.callGraph;

  ok(Number.isFinite(cg.MAX_FLATTENED_OPS), 'the jar flattener has no finite node ceiling');

  // A wide tree, because breadth is what the old depth cap failed to bound. One root with BIG
  // children, each holding an operation.
  const root = {
    name: 'root',
    operations: [{ type: 'SELECT', target: 't' }],
    children: Array.from({ length: BIG }, (_, i) => ({
      name: 'c' + i,
      operations: [{ type: 'SELECT', target: 't' + i }],
      children: [],
    })),
  };

  const result = [];
  const started = Date.now();
  cg._flattenOps(root, 0, result);
  const elapsed = Date.now() - started;

  ok(result.length <= cg.MAX_FLATTENED_OPS,
    `flattened ${result.length} operations, ceiling is ${cg.MAX_FLATTENED_OPS}`);
  ok(result.truncated, 'the flattener hit its ceiling without recording that it did');
  ok(elapsed < BUDGET_MS, `flattening ${BIG} nodes took ${elapsed}ms`);
}

export async function test_jar_reports_truncation_in_the_rendered_html() {
  const src = await read('jar/js/callGraph.js');
  // The notice is emitted from the same place the operations are, so assert on the source rather
  // than reconstructing an endpoint document the renderer would accept.
  includes(src, 'operations.truncated', 'the renderer never checks whether it truncated');
  includes(src, 'cg-truncated', 'the renderer emits no truncation notice');
}

// ---------------------------------------------------------------------------------------------
// plsql — whole-flow expansion
// ---------------------------------------------------------------------------------------------

export async function test_plsql_show_whole_flow_cannot_lift_the_ceiling() {
  const src = await read('plsql/js/app.js');

  includes(src, '_FLOW_NODE_CEILING', 'the plsql flow view has no named ceiling');

  // Comments first, and anchored to the assignment rather than to the name. Anchoring to the name
  // caught the `onclick` handler 20 lines earlier and swept in the doc comment that *explains* the
  // removal of MAX_SAFE_INTEGER — the test failed on the description of its own fix.
  const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  const whole = code.slice(code.indexOf('PA._showWholeFlow = function'));
  const body = whole.slice(0, whole.indexOf('\n};'));
  ok(!/MAX_SAFE_INTEGER/.test(body),
    '_showWholeFlow still sets an effectively unlimited node budget');
  includes(body, '_FLOW_NODE_CEILING', '_showWholeFlow does not respect the ceiling');
}

// ---------------------------------------------------------------------------------------------
// MID-004: the transfer is bounded too, not only the render
// ---------------------------------------------------------------------------------------------

export async function test_the_graph_is_fetched_bounded_not_whole() {
  // FE-018 capped what is drawn. Until MID-004 the client still fetched every edge to draw it:
  // 847 today, 35,776 at 832 procedures in the load proof, ~344,000 at the stated target — to
  // render 300 nodes. A cap on the render alone leaves the transfer unbounded.
  const viz = await read('parser/js/callGraphViz.js');
  includes(viz, 'PA.api.getCallGraphTop(',
    'the visualisation still fetches the whole call graph');
  excludes(viz.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, ''),
    'PA.api.getCallGraph()',
    'the visualisation still calls the unbounded endpoint');

  const api = await read('parser/js/api.js');
  includes(api, 'call-graph/top', 'there is no bounded call-graph endpoint on the client');
  includes(api, 'async getCallGraph()',
    'the unbounded call is gone — export.js needs it, since an export of part of a graph is wrong');
}

export async function test_the_notice_reports_the_servers_total_not_the_arrival_count() {
  // When the server slices before sending, the client never sees the real node count. Reporting
  // what arrived would say "showing 300 of 300" on a graph of 8,000 — a notice that is technically
  // present and tells the reader nothing.
  const viz = await read('parser/js/callGraphViz.js');
  includes(viz, '_serverTotalNodes',
    'the client does not carry the total the server reported');
  includes(viz, 'data.truncated',
    'the client ignores the server\'s own truncation flag');
}

export async function test_the_local_cap_is_kept_as_a_second_line_of_defence() {
  // The server bounds the response now, but a stale deploy or a direct call to the old route can
  // still deliver an unbounded one. The renderer must not depend on someone else having been
  // careful.
  const viz = await read('parser/js/callGraphViz.js');
  includes(viz, 'nodes.length > PA.callGraphViz.MAX_SIMULATED_NODES',
    'the client-side cap was removed once the server started slicing');
}

// ---------------------------------------------------------------------------------------------
// The property that must hold for all three
// ---------------------------------------------------------------------------------------------

export async function test_every_renderer_has_a_ceiling_and_a_notice() {
  // Stated once as a list so that adding a fourth renderer without a ceiling is a failing test
  // rather than an oversight nobody sees.
  const renderers = [
    { file: 'parser/js/callGraphViz.js', ceiling: 'MAX_SIMULATED_NODES' },
    { file: 'jar/js/callGraph.js', ceiling: 'MAX_FLATTENED_OPS' },
    { file: 'plsql/js/app.js', ceiling: '_FLOW_NODE_CEILING' },
  ];
  atLeast(renderers.length, 3, 'a renderer went missing from this list');
  for (const r of renderers) {
    const src = await read(r.file);
    includes(src, r.ceiling, `${r.file} lost its node ceiling`);
    ok(/cg-truncated|showMore|truncat/i.test(src),
      `${r.file} caps its output without telling the user`);
  }
}
