/**
 * Explore > Trace and Explore > Hierarchy render the SAME call tree from the SAME fetch
 * (`PA.showProcedure` calls `PA.callTrace.render(treeData)` and `PA.trace.renderFromTree(treeData)`
 * on one `getCallTree` result). Two things a reader compares between them must therefore agree:
 * the INTERNAL/EXTERNAL badge on a row, and whether the tree was cut short.
 *
 * <h2>What was wrong</h2>
 *
 * 1. Trace recomputed the badge instead of showing the server's: "schema differs from the ROOT's
 *    schema => EXTERNAL". But internal/external describes a call relative to ITS OWN CALLER, not
 *    to whatever node you happen to have rooted the view at. Real Oracle data shows the split
 *    directly — ANALYZER_REF$USP_REF_LOG -> ANALYZER_REF$REF_AUDIT is one schema calling itself,
 *    which the server and Hierarchy both label INTERNAL, while Trace (root schema ANALYZER)
 *    labelled it EXTERNAL. Same edge, same screen, two answers.
 *
 * 2. The server cuts trees at MAX_CALL_TREE_NODES and marks the cut nodes `childrenTruncated` /
 *    `omittedChildCount`. Hierarchy renders a "+N not shown" badge for those. Trace rendered
 *    nothing, so a cut tree just looked shorter than it is, and its "N calls" badge read like a
 *    complete count. Both loaded demo roots are cut (Postgres +39, Oracle +220).
 *
 * 3. The badge counted the ROOT row — the procedure you selected — as one of the calls it makes.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const strip = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

/** Pull the PA.trace object literal out of parser/js/app.js and evaluate just that. */
async function loadTrace() {
  const src = await read('parser/js/app.js');
  const start = src.indexOf('PA.trace = {');
  ok(start > 0, 'PA.trace not found in parser/js/app.js');
  const end = src.indexOf('\n};', start);
  const body = src.slice(start, end + 3);

  const els = {};
  const el = (id) => (els[id] = els[id] || { textContent: '', title: '', innerHTML: '', style: {}, value: '',
    classList: { toggle() {}, add() {}, remove() {} }, querySelector: () => null,
    insertAdjacentHTML(_, h) { this.innerHTML += h; } });

  globalThis.window = globalThis;
  globalThis.document = {
    getElementById: (id) => el(id),
    querySelectorAll: () => []
  };
  const PA = globalThis.PA = globalThis.PA || {};
  PA.esc = (s) => String(s == null ? '' : s);
  PA.escAttr = PA.esc;
  PA.escJs = PA.esc;
  PA.getSchemaColor = () => ({ bg: '#000', fg: '#fff' });
  new Function('PA', body)(PA);
  return { trace: PA.trace, el };
}

/** The exact shape the server returns, with the real Oracle cross-schema case in it. */
function oracleLikeTree() {
  return {
    id: 'ANALYZER$ERP_USP_RUN_CYCLE', name: 'ERP_USP_RUN_CYCLE', schemaName: 'ANALYZER',
    callType: 'ROOT', callLineNumber: 0, sourceFile: 'a.sql',
    children: [
      { id: 'ANALYZER$ERP_USP_XSCHEMA_TOUCH', name: 'ERP_USP_XSCHEMA_TOUCH', schemaName: 'ANALYZER',
        callType: 'INTERNAL', callLineNumber: 5, sourceFile: 'b.sql',
        children: [
          // cross-schema: ANALYZER -> ANALYZER_REF. Server says EXTERNAL; both views must agree.
          { id: 'ANALYZER_REF$USP_REF_LOG', name: 'USP_REF_LOG', schemaName: 'ANALYZER_REF',
            callType: 'EXTERNAL', callLineNumber: 9, sourceFile: 'c.sql',
            children: [
              // ANALYZER_REF calling ANALYZER_REF — a schema calling itself. Server says INTERNAL.
              { id: 'ANALYZER_REF$REF_AUDIT', name: 'REF_AUDIT', schemaName: 'ANALYZER_REF',
                callType: 'INTERNAL', callLineNumber: 7, sourceFile: 'd.sql', children: [] }
            ] }
        ] }
    ]
  };
}

function rowsFor(trace) {
  return trace.flatNodes.map((n) => ({ id: n.id, type: trace._effType(n), depth: n._depth }));
}

export async function test_trace_shows_the_servers_call_type_not_one_derived_from_the_root_schema() {
  const { trace } = await loadTrace();
  trace.renderFromTree(oracleLikeTree());
  const byId = Object.fromEntries(rowsFor(trace).map((r) => [r.id, r.type]));

  equal(byId['ANALYZER_REF$REF_AUDIT'], 'INTERNAL',
    'ANALYZER_REF calling ANALYZER_REF is one schema calling itself — Hierarchy and the server '
    + 'both say INTERNAL, so Trace must not relabel it EXTERNAL just because the ROOT is ANALYZER');
  equal(byId['ANALYZER_REF$USP_REF_LOG'], 'EXTERNAL', 'a genuine cross-schema call is still EXTERNAL');
  equal(byId['ANALYZER$ERP_USP_XSCHEMA_TOUCH'], 'INTERNAL', 'a same-schema call is still INTERNAL');
}

export async function test_every_trace_row_badge_equals_the_servers_callType_for_that_node() {
  const { trace } = await loadTrace();
  const tree = oracleLikeTree();
  trace.renderFromTree(tree);
  // Hierarchy renders `node.callType` verbatim (callGraph.js). So must Trace, for every row.
  for (const n of trace.flatNodes) {
    if (n.circular) continue;
    equal(trace._effType(n), n.callType || '',
      `row ${n.id} shows a different call type than the server sent for it`);
  }
}

export async function test_hierarchy_renders_the_servers_callType_verbatim() {
  // The other half of the agreement: if Hierarchy ever starts deriving its own, this test fails
  // and the one above stops proving anything.
  const src = strip(await read('parser/js/callGraph.js'));
  includes(src, 'const callType = node.callType', 'Hierarchy must read the server callType');
  excludes(src, '_rootSchema', 'Hierarchy must not derive a call type from the root schema either');
}

export async function test_trace_no_longer_derives_a_call_type_from_the_root_schema() {
  const src = strip(await read('parser/js/app.js'));
  const start = src.indexOf('PA.trace = {');
  const body = src.slice(start, src.indexOf('\n};', start));
  excludes(body, '_rootSchema',
    'the root-schema reclassification is what made Trace disagree with Hierarchy');
}

export async function test_a_truncated_tree_says_so_in_the_trace_count() {
  const { trace, el } = await loadTrace();
  const tree = oracleLikeTree();
  tree.children[0].childrenTruncated = true;
  tree.children[0].omittedChildCount = 220;
  trace.renderFromTree(tree);

  equal(trace._omitted, 220, 'the omitted-call total was not collected while flattening');
  includes(el('traceCount').textContent, '+220 not shown',
    'a cut tree renders a count that reads like a complete one');
  includes(el('traceCount').title, 'not expanded', 'no explanation of what the number means');
}

export async function test_a_complete_tree_makes_no_truncation_claim() {
  const { trace, el } = await loadTrace();
  trace.renderFromTree(oracleLikeTree());
  equal(trace._omitted, 0, 'nothing was truncated');
  excludes(el('traceCount').textContent, 'not shown',
    'a complete trace must not suggest calls are missing');
}

export async function test_the_cut_row_itself_carries_the_badge_like_hierarchy_does() {
  const { trace } = await loadTrace();
  const tree = oracleLikeTree();
  tree.children[0].childrenTruncated = true;
  tree.children[0].omittedChildCount = 7;
  trace.renderFromTree(tree);
  const cut = trace.flatNodes.find((n) => n.id === 'ANALYZER$ERP_USP_XSCHEMA_TOUCH');
  const html = trace._renderRow(cut, 0, 3);
  includes(html, '+7 not shown', 'the row where the tree was cut gives no sign of it');
}

export async function test_the_root_is_not_counted_as_a_call_it_makes() {
  const { trace, el } = await loadTrace();
  trace.renderFromTree(oracleLikeTree());
  // 4 rows in the fixture: the ROOT + 3 real calls.
  equal(trace.flatNodes.length, 4, 'fixture changed');
  includes(el('traceCount').textContent, '3 calls',
    'the selected procedure is not one of the calls it makes');
}

export async function test_unique_mode_collapses_repeats_without_inventing_or_losing_any() {
  const { trace, el } = await loadTrace();
  // One callee reached from two distinct call sites — the server sends one child per call SITE.
  trace.renderFromTree({
    id: 'ERP$P1', name: 'P1', schemaName: 'ERP', callType: 'ROOT', sourceFile: 'a.sql',
    children: [
      { id: 'ERP$P2', name: 'P2', schemaName: 'ERP', callType: 'INTERNAL', callLineNumber: 10, children: [] },
      { id: 'ERP$P2', name: 'P2', schemaName: 'ERP', callType: 'INTERNAL', callLineNumber: 20, children: [] },
      { id: 'ERP$P3', name: 'P3', schemaName: 'ERP', callType: 'INTERNAL', callLineNumber: 30, children: [] }
    ]
  });
  includes(el('traceCount').textContent, '3 calls', 'full mode lists one row per call site');

  trace.toggleUnique();
  const u = trace._uniqueNodes();
  // flatNodes carries the ROOT row too, so unique rows = ROOT + P2 + P3.
  const calls = u.filter((n) => (n.callType || '') !== 'ROOT');
  equal(calls.length, 2, 'unique mode must collapse the two P2 call sites into one row');
  equal(calls.find((n) => n.id === 'ERP$P2')._count, 2, 'the collapsed row must say it happened twice');
  equal(calls.reduce((s, n) => s + n._count, 0), 3,
    'the call counts must still add back up to every call site — no call may be swallowed');
  includes(el('traceCount').textContent, '2 unique calls',
    'the unique-mode badge must count collapsed rows, and still not count the root');
}
