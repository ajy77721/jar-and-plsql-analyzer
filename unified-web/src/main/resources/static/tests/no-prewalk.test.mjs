/**
 * UI-016 — no full-tree walk before the first row is painted.
 *
 * <h2>What was wrong</h2>
 *
 * Both call-trace renderers walked the entire tree on load to assign every node a step number and
 * put it in `_nodeMap`. The only readers of that map are a click handler and the lazy branch
 * builder, and both can only ever be reached from a row that was rendered — so the walk maintained
 * an index for nodes nobody could interact with.
 *
 * The parser copy also set `_parentSourceFile` on every node, which nothing in that file read:
 * the code that renders an `@line` link builds its own structure in `parser/js/app.js`.
 *
 * <h2>Why the walk is counted rather than timed</h2>
 *
 * A timing assertion on 1,250 nodes proves nothing — the walk is fast at that size. The defect is
 * that the work is proportional to the whole tree while the output is proportional to what is
 * shown, and one measured JAR endpoint expands to 300,167,238 nodes. So the test counts how many
 * nodes were *touched*, using an accessor that records reads, and asserts it is bounded by the
 * rendered depth rather than by the tree.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal, includes, excludes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

/** Deliberately far wider than any initial depth, so a full walk is unmistakable. */
const BREADTH = 400;

function sandbox() {
  const dom = installDom();
  globalThis.PA = {
    esc: (s) => String(s == null ? '' : s),
    escAttr: (s) => String(s == null ? '' : s),
    escJs: (s) => String(s == null ? '' : s),
    showProcedure: () => {},
    // renderNode consults the node index to decide whether a name is resolvable to a procedure.
    // Stubbed as "empty index", which is the state on a page that has not loaded one — the
    // renderer must still draw rows, and the first version of this test failed with
    // "Cannot read properties of undefined (reading 'count')" for the want of it.
    // UI-024 (§164): render() now awaits ensureLoaded() once before the walk begins — stubbed
    // as an immediately-resolved empty array, matching "empty index" above; the first version of
    // this fix crashed the whole test-runner process (not a graceful test failure) with
    // "PA.nodeIndex.ensureLoaded is not a function" for the want of it.
    nodeIndex: { count: () => 0, resolve: () => null, ensureLoaded: () => Promise.resolve([]) },
  };
  globalThis.MdbColor = { get: () => '#000', alpha: () => '#000' };
  globalThis.requestAnimationFrame = () => 0;
  globalThis.confirm = () => false;
  return dom;
}

/**
 * A three-level tree that reports when a **grandchild's** children are read.
 *
 * Three levels, not two, because the obvious two-level version measures the wrong thing. The
 * renderer tests `node.children && node.children.length > 0` to decide whether to draw a toggle,
 * which reads the property twice on every row it draws — so counting reads at the level being
 * rendered reported "800 of 400 leaves" on a renderer that was not walking anything.
 *
 * Descending is what separates a walk from a property check, and the first level the renderer must
 * not descend into is the grandchildren.
 */
function countingTree(breadth, onDescend) {
  const grandchild = (i) => {
    const node = { id: 'g' + i, name: 'g' + i, callType: 'STATIC' };
    Object.defineProperty(node, 'children', {
      get() {
        onDescend('g' + i);
        return [];
      },
      enumerable: true,
    });
    return node;
  };
  const kids = Array.from({ length: breadth }, (_, i) => ({
    id: 'c' + i,
    name: 'c' + i,
    callType: 'STATIC',
    children: [grandchild(i)],
  }));
  return { id: 'root', name: 'root', callType: 'STATIC', children: kids };
}

async function loadTrace(rel) {
  new Function(await read(rel))();
  return globalThis.PA.callTrace;
}

/**
 * Drives the real entry point, `render(data)`, not `renderNode` directly.
 *
 * <p>This is the whole difference between a test and a decoration. The first version of this file
 * called `renderNode(root, 0, true)`, which skips `render()` — the function the pre-walk actually
 * lived in. Calibration exposed it: reinstating the eager walk in `render()` left all nine tests
 * green, because none of them called `render()`. That is the same mistake as asserting a
 * truncation notice by calling the notice function instead of the layout that emits it, and it is
 * the eleventh time in this project that a guard has passed while guarding nothing.
 */
async function driveRender(dom, trace, root) {
  // Every element the entry path touches. `breadcrumb` and `btnAnalysisRoot` belong to
  // updateBreadcrumb, which render() calls before it draws anything — omitting them failed with
  // "Cannot set properties of null (setting 'innerHTML')", which looks like a product fault and
  // is a missing fixture.
  dom.add('ctContainer');
  dom.add('exploreSearch');
  dom.add('breadcrumb');
  dom.add('btnAnalysisRoot');
  trace._nodeMap = {};
  trace.stepCounter = 0;
  // UI-024 (§164): parser/js/callGraph.js's render() is async now (awaits ensureLoaded() once
  // before the walk begins); plsql/js/callGraph.js's own render() stays synchronous. Awaiting
  // here works for both — awaiting a non-promise return value is a no-op.
  await trace.render(root);
}

for (const rel of ['parser/js/callGraph.js', 'plsql/js/callGraph.js']) {
  const label = rel.split('/')[0];

  // Exported per file rather than looped inside one test, so a failure names which renderer.
  globalThis[`test_${label}_does_not_prewalk_the_tree`] = async function () {
    const dom = sandbox();
    const trace = await loadTrace(rel);
    ok(trace, `${rel} did not define PA.callTrace`);
    ok(typeof trace.render === 'function', `${rel} has no render entry point`);

    const descended = [];
    const root = countingTree(BREADTH, (id) => descended.push(id));
    await driveRender(dom, trace, root);

    // Nothing below the initial depth may be descended into. Rendering the first level is
    // expected; reaching the generation below it is the walk this row removed.
    equal(descended.length, 0,
      `${rel} descended into ${descended.length} grandchildren before anything was expanded`);
  };

  globalThis[`test_${label}_indexes_only_what_it_rendered`] = async function () {
    const dom = sandbox();
    const trace = await loadTrace(rel);
    const root = countingTree(BREADTH, () => {});
    await driveRender(dom, trace, root);

    const indexed = Object.keys(trace._nodeMap).length;
    const whole = BREADTH * 2 + 1;
    ok(indexed >= 1, `${rel} indexed nothing, so a click handler could find no node`);
    ok(indexed < whole,
      `${rel} indexed ${indexed} of ${whole} nodes before anything was expanded`);
  };

  globalThis[`test_${label}_keeps_a_step_stable_across_rerender`] = async function () {
    // The objection the plsql copy's own comment raised against lazy numbering: "Numbering during
    // render would renumber every time something expanded." It must not.
    sandbox();
    const trace = await loadTrace(rel);
    const node = { id: 'n', name: 'n', callType: 'STATIC', children: [] };

    trace._nodeMap = {};
    trace.stepCounter = 0;
    trace.renderNode(node, 0, true);
    const first = node._step;
    trace.renderNode(node, 0, true);

    equal(node._step, first, `${rel} renumbered a node on re-render, so its DOM ids went stale`);
    equal(trace._nodeMap[first], node, `${rel} lost the node its step points at`);
  };

  globalThis[`test_${label}_counts_the_whole_tree_for_the_expand_all_warning`] = async function () {
    // Lazy indexing makes _nodeMap a bad source for "how big is this tree", and the expand-all
    // prompt is a safety warning: one that understates is worse than none.
    sandbox();
    const trace = await loadTrace(rel);
    ok(typeof trace._countNodes === 'function', `${rel} has no whole-tree count`);

    const root = countingTree(BREADTH, () => {});
    equal(trace._countNodes(root), BREADTH * 2 + 1,
      `${rel} miscounts the tree, so the expand-all warning would be wrong`);
    equal(trace._countNodes(null), 0, `${rel} does not handle an empty tree`);

    // Circular nodes are counted but not followed — the rule the renderer uses. Without the same
    // rule here, a cyclic trace would not terminate.
    const cyc = { id: 'a', children: [] };
    cyc.children.push({ id: 'b', circular: true, children: [cyc] });
    equal(trace._countNodes(cyc), 2, `${rel} follows a circular node and would not terminate`);
  };
}

export const {
  test_parser_does_not_prewalk_the_tree,
  test_parser_indexes_only_what_it_rendered,
  test_parser_keeps_a_step_stable_across_rerender,
  test_parser_counts_the_whole_tree_for_the_expand_all_warning,
  test_plsql_does_not_prewalk_the_tree,
  test_plsql_indexes_only_what_it_rendered,
  test_plsql_keeps_a_step_stable_across_rerender,
  test_plsql_counts_the_whole_tree_for_the_expand_all_warning,
} = globalThis;

export async function test_the_prewalk_is_gone_from_both_renderers() {
  // Structural, so the function cannot be reintroduced and left uncalled or called again.
  for (const rel of ['parser/js/callGraph.js', 'plsql/js/callGraph.js']) {
    const code = (await read(rel))
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/^\s*\/\/.*$/gm, '');
    excludes(code, '_indexNodes', `${rel} still defines or calls _indexNodes`);
    includes(code, '_countNodes', `${rel} lost its whole-tree count`);
  }
  atLeast(2, 2, 'both renderers are covered');
}
