/**
 * Hierarchy's collapsed ("Unique") rows — `parser/js/callGraph.js`.
 *
 * <p>Real data drives this: ERP_USP_RUN_CYCLE calls ERP_USP_WRITE_AUDIT_TRAIL from 31 distinct
 * lines, and the uncollapsed tree spent 31 sibling rows saying the same thing. Hierarchy answers
 * "what is the SHAPE of the call structure", so it collapses by default — one row per distinct
 * callee with a &times;N badge. Trace is deliberately the opposite (the complete, execution-order
 * record of every call site, Unique opt-in there) and is not touched by any of this.
 *
 * <p>What is asserted is the part that would silently rot: that collapsing keeps every call-site
 * line number reachable rather than dropping it, that the toggle restores the full per-call-site
 * view exactly, and that the search walk sees the same children the renderer draws — a match on a
 * row that collapsing folded away could never be expanded to.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function loadTrace() {
  installDom();
  globalThis.PA = {
    esc: (s) => String(s == null ? '' : s),
    escAttr: (s) => String(s == null ? '' : s),
    escJs: (s) => String(s == null ? '' : s),
    showProcedure: () => {},
    nodeIndex: { count: () => 0, resolve: () => null, ensureLoaded: () => Promise.resolve([]) },
    api: { _analysisName: 'x', getCallTree: async () => null },
  };
  globalThis.MdbColor = { get: () => '#000', alpha: () => '#000' };
  globalThis.requestAnimationFrame = () => 0;
  globalThis.confirm = () => true;
  new Function(await readFile(path.join(STATIC, 'parser/js/callGraph.js'), 'utf8'))();
  return globalThis.PA.callTrace;
}

/** A parent calling the same child from three lines, plus one distinct child — the real shape. */
function tree() {
  const dup = (line, kids) => ({
    id: 'ERP$AUDIT', name: 'AUDIT', schemaName: 'ERP', callType: 'INTERNAL',
    startLine: 900, callLineNumber: line, children: kids || [],
  });
  return {
    id: 'ERP$ROOT', name: 'ROOT', schemaName: 'ERP', sourceFile: 'ERP.ROOT', startLine: 1,
    children: [
      dup(34), dup(61, [{ id: 'ERP$DEEP', name: 'DEEP', schemaName: 'ERP', startLine: 5 }]), dup(6),
      { id: 'ERP$OTHER', name: 'OTHER', schemaName: 'ERP', callType: 'INTERNAL', startLine: 10, callLineNumber: 12 },
    ],
  };
}

const rowCount = (html) => (html.match(/class="ct-row"/g) || []).length;

function render(trace, data) {
  trace._nodeMap = {}; trace.stepCounter = 0; trace._root = data;
  return trace.renderNode(data, 0, null);
}

export async function test_hierarchy_collapses_repeated_callees_by_default() {
  const trace = await loadTrace();
  equal(trace._unique, true, 'Hierarchy collapses by default — that is the whole point of the view');
  const html = render(trace, tree());
  equal(rowCount(html), 3, 'root + one row per distinct callee, not one row per call site');
  ok(html.includes('&times;3'), 'the collapsed row must say how many call sites it stands for');
}

export async function test_collapsing_keeps_every_call_site_line_number() {
  const trace = await loadTrace();
  const html = render(trace, tree());
  const chips = html.match(/<div class="ct-callsites"[\s\S]*?<\/div>/)[0];
  const lines = [...chips.matchAll(/@(\d+)/g)].map((m) => Number(m[1]));
  // Sorted, and complete: collapsing must not be able to lose a call site.
  equal(lines.join(','), '6,34,61', 'every call-site line stays reachable under the collapsed row');
  ok(chips.includes('style="display:none'), 'the chips start hidden — the row is meant to be clean');
  ok(html.includes('&times;3'), 'and the badge is what reveals them');
}

export async function test_collapsing_never_hides_a_subtree_that_exists() {
  const trace = await loadTrace();
  const data = tree();
  const html = render(trace, data);
  // Only ONE of the three occurrences carries a child; the representative must be that one, or
  // collapsing would amount to deleting a branch of the call graph.
  ok(html.includes('ERP$DEEP') || html.includes('ct-children-'),
     'the collapsed AUDIT row must still carry the subtree its one expandable occurrence had');
  const audit = trace._childrenFor(data).find((n) => n.id === 'ERP$AUDIT');
  equal(audit._dupCount, 3, 'three call sites folded into one row');
  equal((audit.children || []).length, 1, 'the representative is the occurrence that has the subtree');
}

export async function test_toggling_back_restores_every_individual_call_site() {
  const trace = await loadTrace();
  const data = tree();
  equal(rowCount(render(trace, data)), 3, 'collapsed to start with');

  trace._unique = false; trace._resetGrouping(data);
  const all = render(trace, data);
  equal(rowCount(all), 5, 'root + all four call sites, exactly as before this feature existed');
  ok(!all.includes('ct-dup-badge'), 'no count badge when every call site has its own row');
  ok(all.includes('@34') && all.includes('@61') && all.includes('@6'), 'each row keeps its own @line');

  trace._unique = true; trace._resetGrouping(data);
  const back = render(trace, data);
  equal(rowCount(back), 3, 'toggling back collapses again — no stale grouping left over');
  const ids = [...back.matchAll(/id="ct-(\d+)"/g)].map((m) => m[1]);
  equal(ids.length, new Set(ids).size, 'every rendered row keeps a distinct step id across a toggle');
}

export async function test_search_walks_the_children_the_renderer_draws() {
  const trace = await loadTrace();
  const data = tree();
  render(trace, data);
  const found = trace._findMatches('audit', 200);
  equal(found.paths.length, 1, 'one collapsed row means one match, not one match per call site');
  // The matched node must be the object that was actually rendered, or _expandPath cannot reach it.
  ok(found.paths[0][found.paths[0].length - 1]._step !== undefined,
     'the match is the rendered representative, so its path can be expanded');
}
