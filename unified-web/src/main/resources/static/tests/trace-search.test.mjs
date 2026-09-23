/**
 * UI-018 — searching the Explore tree does not materialise it.
 *
 * <h2>What was wrong</h2>
 *
 * `search(query)` called `expandAll()` to put every row in the document, then hid the rows that did
 * not match. So a question about a handful of nodes rendered the whole tree — and the search box is
 * wired to `oninput`, so it did that per keystroke.
 *
 * <p>UI-016 made it worse before this fixed it: giving `expandAll` an accurate whole-tree count
 * meant its confirm prompt started firing correctly, and `search` calls `expandAll`, so **typing
 * one character in the box popped a modal** on any trace over 400 nodes. A fix to one function
 * surfacing as a defect in another is the argument for the caller not reaching for
 * "materialise everything" in the first place.
 *
 * <h2>What replaced it</h2>
 *
 * The match is found in the tree data, and only the ancestor paths of matches are rendered. Results
 * are unchanged: the search compares the same two strings the rendered row shows — the qualified
 * name in `.ct-name` and the owner schema in `.ct-schema-badge`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const BREADTH = 900;

function sandbox() {
  const dom = installDom();
  let confirmed = 0;
  globalThis.PA = {
    esc: (s) => String(s == null ? '' : s),
    escAttr: (s) => String(s == null ? '' : s),
    escJs: (s) => String(s == null ? '' : s),
    showProcedure: () => {},
    // UI-024 (§164): render() now awaits ensureLoaded() once before the walk begins.
    nodeIndex: { count: () => 0, resolve: () => null, ensureLoaded: () => Promise.resolve([]) },
  };
  globalThis.MdbColor = { get: () => '#000', alpha: () => '#000' };
  globalThis.requestAnimationFrame = () => 0;
  // Counted, not just stubbed: a search that prompts the user is the defect under test.
  globalThis.confirm = () => {
    confirmed++;
    return false;
  };
  globalThis.window = globalThis;
  return { dom, confirms: () => confirmed };
}

async function loadTrace() {
  new Function(await read('parser/js/callGraph.js'))();
  return globalThis.PA.callTrace;
}

/** Root, `BREADTH` children, one grandchild each; exactly one grandchild is the needle. */
function haystack(breadth) {
  const kids = Array.from({ length: breadth }, (_, i) => ({
    id: 'c' + i, name: 'C' + i, schemaName: 'APP', packageName: 'PKG',
    children: [{
      id: 'g' + i,
      name: i === breadth - 1 ? 'NEEDLE_PROC' : 'G' + i,
      schemaName: 'APP', packageName: 'PKG', children: [],
    }],
  }));
  return { id: 'root', name: 'ROOT', schemaName: 'APP', packageName: 'PKG', children: kids };
}

async function driveRender(dom, trace, root) {
  dom.add('ctContainer');
  dom.add('exploreSearch');
  dom.add('breadcrumb');
  dom.add('btnAnalysisRoot');
  trace._nodeMap = {};
  trace.stepCounter = 0;
  // UI-024 (§164): render() is async now — awaiting works for both this and the plsql fork's
  // still-synchronous render().
  await trace.render(root);
}

export async function test_searching_does_not_prompt_the_user() {
  // The regression UI-016 introduced into this path, and the most user-visible part of the defect:
  // a confirm dialog per keystroke.
  const { dom, confirms } = sandbox();
  const trace = await loadTrace();
  await driveRender(dom, trace, haystack(BREADTH));

  await trace.search('NEEDLE');
  equal(confirms(), 0,
    'searching prompted the user — search is calling expandAll(), which asks before rendering '
    + 'a large tree, and the box fires on every keystroke');
}

export async function test_searching_renders_only_the_matching_paths() {
  // Asserted as "expands a bounded number of ancestor chains", not by counting DOM rows. The test
  // DOM stores innerHTML as a string rather than building retrievable elements, so
  // _renderChildrenLazy finds no container and nothing is added to _nodeMap — a limit of the
  // harness, not of the product. Counting rows here would have measured the harness.
  const { dom } = sandbox();
  const trace = await loadTrace();
  const root = haystack(BREADTH);
  await driveRender(dom, trace, root);

  const found = trace._findMatches('needle', trace.MAX_SEARCH_HITS);
  const whole = BREADTH * 2 + 1;

  // One match, so at most one ancestor chain gets expanded: root -> child -> the match.
  const toExpand = found.paths.reduce((n, p) => n + (p.length - 1), 0);
  ok(toExpand > 0, 'no ancestor chain would be expanded, so the match cannot be on screen');
  ok(toExpand < whole / 10,
    `the search would expand ${toExpand} of ${whole} nodes — that is materialising the tree`);

  // And search() must not throw when the DOM cannot satisfy it, because the product runs where it
  // can and this path has to be exercised end to end somewhere.
  await trace.search('NEEDLE');
}

export async function test_the_walk_itself_is_bounded_not_only_the_matches() {
  // Capping matches bounds rendering. A query that matches nothing still walks — and while someone
  // is typing, "matches nothing" is the common case and the whole tree is the worst case.
  const { dom } = sandbox();
  const trace = await loadTrace();
  ok(Number.isFinite(trace.MAX_SEARCH_SCAN), 'the search walk has no finite ceiling');

  // A tree larger than the scan cap, with nothing that matches.
  const big = { id: 'r', name: 'R', schemaName: 'S', children: [] };
  for (let i = 0; i < trace.MAX_SEARCH_SCAN + 500; i++) {
    big.children.push({ id: 'n' + i, name: 'N' + i, schemaName: 'S', children: [] });
  }
  await driveRender(dom, trace, big);

  const found = trace._findMatches('zzz_no_match_anywhere', trace.MAX_SEARCH_HITS);
  equal(found.paths.length, 0, 'the probe query should match nothing');
  ok(found.scanned <= trace.MAX_SEARCH_SCAN,
    `the walk examined ${found.scanned} nodes, ceiling is ${trace.MAX_SEARCH_SCAN}`);
  ok(found.truncated, 'the walk hit its ceiling without recording that it did');
}

export async function test_the_match_is_found_at_all() {
  // Guards the guard above: "rendered very little" is trivially satisfied by finding nothing.
  const { dom } = sandbox();
  const trace = await loadTrace();
  const root = haystack(BREADTH);
  await driveRender(dom, trace, root);

  const found = trace._findMatches('needle', trace.MAX_SEARCH_HITS);
  equal(found.paths.length, 1, 'the needle was not found exactly once');
  const leaf = found.paths[0][found.paths[0].length - 1];
  equal(leaf.name, 'NEEDLE_PROC', 'the wrong node matched');
  equal(found.paths[0].length, 3, 'the path to the match is not root -> child -> grandchild');
}

export async function test_the_search_matches_the_schema_badge_too() {
  // The old DOM search read `.ct-name` and `.ct-schema-badge`. Both must still match, or the fix
  // narrows what the box finds without anyone noticing.
  const { dom } = sandbox();
  const trace = await loadTrace();
  await driveRender(dom, trace, haystack(20));

  ok(trace._findMatches('app', trace.MAX_SEARCH_HITS).paths.length > 0,
    'a schema-only match no longer matches, so the search is narrower than the one it replaced');
  ok(trace._findMatches('pkg', trace.MAX_SEARCH_HITS).paths.length > 0,
    'a package match no longer matches — the rendered name includes the package');
}

export async function test_the_result_list_is_capped_and_says_so() {
  const { dom } = sandbox();
  const trace = await loadTrace();
  ok(Number.isFinite(trace.MAX_SEARCH_HITS), 'the search has no finite match ceiling');

  // Every node matches, so the cap must engage.
  await driveRender(dom, trace, haystack(BREADTH));
  const found = trace._findMatches('app', trace.MAX_SEARCH_HITS);
  equal(found.paths.length, trace.MAX_SEARCH_HITS, 'the match list was not capped');
  ok(found.truncated, 'the search hit its ceiling without recording that it did');

  await trace.search('APP');
  const notice = document.getElementById('ctSearchNotice');
  ok(notice, 'nothing told the user the match list was cut');
  includes(notice.className, 'cg-truncated', 'the notice is not styled as one');
  includes(notice.textContent, String(trace.MAX_SEARCH_HITS),
    'the notice does not say how many matches are shown');
}

export async function test_a_complete_result_list_gets_no_notice() {
  // The honest direction: a notice on every search is a notice nobody reads.
  const { dom } = sandbox();
  const trace = await loadTrace();
  await driveRender(dom, trace, haystack(20));

  await trace.search('NEEDLE');
  ok(!document.getElementById('ctSearchNotice'),
    'a search well under the ceiling was reported as truncated');
}

export async function test_clearing_the_box_removes_the_notice() {
  const { dom } = sandbox();
  const trace = await loadTrace();
  await driveRender(dom, trace, haystack(BREADTH));

  await trace.search('APP');
  ok(document.getElementById('ctSearchNotice'), 'precondition: the notice should be present');
  await trace.search('');
  ok(!document.getElementById('ctSearchNotice'),
    'the notice survived an empty query, so every later search inherits a stale warning');
}

export async function test_search_no_longer_calls_expand_all() {
  // Structural, so the call cannot come back quietly. Comments stripped: the fix's own explanation
  // names the function it removed, and an earlier gate in this suite failed on exactly that.
  const code = (await read('parser/js/callGraph.js'))
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/^\s*\/\/.*$/gm, '');
  const from = code.indexOf('search(query) {');
  ok(from > 0, 'search(query) is gone');
  const body = code.slice(from, code.indexOf('\n    },', from));
  excludes(body, 'expandAll', 'search still materialises the tree via expandAll');
  includes(body, '_findMatches', 'search does not look in the tree data');
}
