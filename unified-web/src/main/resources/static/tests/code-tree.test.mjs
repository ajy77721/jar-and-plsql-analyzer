/**
 * FE-017 — the JAR Code Structure tree renders a window, in linear time.
 *
 * <h2>Two independent defects</h2>
 *
 * <b>O(n²).</b> Three render loops recovered a class's position with `_classes.indexOf(cls)` — a
 * linear scan per row. Measured on this fixture at 80,000 classes: **954 ms** with the scan,
 * **155 ms** without. It was already at the 1 s cliff on a machine with nothing else running.
 *
 * <b>The whole corpus in one write.</b> Class *bodies* were lazy but every class *header* was
 * emitted up front: a single **52.4 MB** `innerHTML` assignment and ~80,000 tree rows. Collapsing
 * a package did not help, because collapsed markup is still in the DOM. Packages now fill on first
 * expand, and page 200 rows at a time — 0.22 MB and 400 rows for the same input.
 *
 * <h2>What is deliberately not asserted</h2>
 *
 * Not layout, paint, or scroll behaviour — the harness is a DOM stub, not a browser (see README).
 * What it can prove is the two things that actually broke: how much markup is produced, and how
 * that number grows with the corpus.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom, makeElement } from './lib/dom.mjs';
import { assertNodeBudget, countTags } from './lib/budget.mjs';
import { ok, equal, includes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/** The corpus size FE-017 names. */
const BIG = 80000;
/** The budget FE-017 names for that corpus. */
const BUDGET_MS = 1000;

function makeClasses(n) {
  // Spread over 400 packages, so the tree has realistic branching rather than one giant folder.
  return Array.from({ length: n }, (_, i) => ({
    fullyQualifiedName: `com.acme.pkg${i % 400}.C${i}`,
    simpleName: `C${i}`,
    packageName: `com.acme.pkg${i % 400}`,
    stereotype: 'SERVICE',
    annotations: [],
    fields: [],
    methods: [{ name: 'm' }],
  }));
}

async function loadTree() {
  const dom = installDom();
  globalThis.JA = {
    utils: {
      escapeHtml: (s) => String(s ?? ''),
      stereotypeColor: () => '#000',
      stereotypeBadgeClass: () => 'b',
      formatNumber: (n) => String(n),
    },
    summary: {},
    api: {},
  };
  new Function(await readFile(path.join(STATIC, 'jar/js/codeTree.js'), 'utf8'))();
  return dom;
}

/** Renders the package view and returns the markup plus how long it took. */
function renderPackages(dom, classes) {
  const tree = globalThis.JA.codeTree;
  tree._classes = classes;
  tree._classIndex = null;
  tree._pendingPackages = null;
  tree._writtenPackages = null;
  tree._pendingSeq = 0;
  const container = dom.add('class-tree');
  const started = performance.now();
  tree._renderPackageView(container);
  return { html: container.innerHTML, ms: performance.now() - started, tree, container };
}

export async function test_eighty_thousand_classes_stay_inside_the_dom_budget() {
  const dom = await loadTree();
  const { html, ms } = renderPackages(dom, makeClasses(BIG));

  // One row per package, not per class. The budget is set from the package count with headroom,
  // so a regression that reintroduces per-class rows overshoots it by two orders of magnitude
  // rather than by a margin someone would be tempted to raise.
  assertNodeBudget(html, 'div', 4000, BIG, 'the code tree');
  ok(ms < BUDGET_MS, `rendering ${BIG} classes took ${ms.toFixed(0)}ms`);
}

export async function test_the_render_does_not_grow_with_the_corpus() {
  // The property, not a threshold: 16× the classes must not mean materially more markup, because
  // what is rendered is one row per package and the package count is fixed in this fixture.
  const dom = await loadTree();
  const small = renderPackages(dom, makeClasses(5000)).html.length;
  const big = renderPackages(dom, makeClasses(BIG)).html.length;
  ok(big < small * 1.5,
    `markup grew from ${small} to ${big} bytes for 16x the classes; the render tracks the corpus`);
}

export async function test_a_small_jar_still_opens_expanded() {
  // The lazy path must not reach the sizes it was not written for. A 119-class JAR showing empty
  // folders would be a worse product in exchange for a scale problem it does not have.
  const dom = await loadTree();
  const tree = globalThis.JA.codeTree;
  const { html } = renderPackages(dom, makeClasses(119));
  ok(119 <= tree.EAGER_CLASS_LIMIT, 'the fixture is not actually below the eager limit');
  includes(html, 'tree-toggle expanded', 'a small tree no longer renders expanded');
  atLeast(countTags(html, 'div'), 119, 'a small tree did not render its class rows');
}

export async function test_expanding_a_package_fills_it_once_and_pages_the_rest() {
  const dom = await loadTree();
  const tree = globalThis.JA.codeTree;
  // One package far larger than the page size, to exercise both the fill and the paging.
  const classes = Array.from({ length: 1000 }, (_, i) => ({
    fullyQualifiedName: `com.acme.one.C${i}`, simpleName: `C${i}`,
    packageName: 'com.acme.one', stereotype: 'SERVICE',
    annotations: [], fields: [], methods: [],
  }));
  renderPackages(dom, classes);

  // The toggle and its sibling, as the inline onclick would supply them.
  const children = makeElement();
  children.dataset.pkgKey = '1';
  const toggle = makeElement();
  toggle.nextElementSibling = children;

  tree.togglePkg(toggle);
  const first = countTags(children.innerHTML, 'div');
  atLeast(first, tree.MAX_CLASSES_PER_PACKAGE, 'expanding the package rendered no class rows');
  ok(first < 1000, `expanding rendered ${first} rows; the per-package page size is not applied`);
  includes(children.innerHTML, 'show', 'no control offers the remaining classes');
  includes(children.innerHTML, '1000', 'the control does not say how many classes exist');

  // Paging forward shows strictly more, and eventually all of them.
  const more = makeElement();
  more.parentNode = children;
  more.dataset.next = String(tree.MAX_CLASSES_PER_PACKAGE * 2);
  tree.showMoreClasses(more);
  atLeast(countTags(children.innerHTML, 'div'), first + 1, 'show-more revealed no further classes');
}

export async function test_no_render_loop_scans_the_class_array() {
  // Asserted against the source, because the cost is invisible in output: an O(n²) render produces
  // exactly the same markup as an O(n) one and only differs in how long it takes.
  for (const rel of ['jar/js/codeTree.js', 'jar/js/codeTreeViews.js']) {
    const src = await readFile(path.join(STATIC, rel), 'utf8');
    const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
    equal((code.match(/_classes\.indexOf\(/g) || []).length, 0,
      `${rel} scans _classes with indexOf inside a render loop`);
  }
  const src = await readFile(path.join(STATIC, 'jar/js/codeTree.js'), 'utf8');
  includes(src, 'classIndexOf', 'the constant-time index lookup is gone');
}

export async function test_the_index_map_agrees_with_the_scan_it_replaced() {
  // Equivalence, not just speed. A faster lookup that returns a different index would open the
  // wrong class on expand — silently, and only on the corpus nobody tests with.
  const dom = await loadTree();
  const tree = globalThis.JA.codeTree;
  const classes = makeClasses(500);
  tree._classes = classes;
  tree._classIndex = null;
  for (let i = 0; i < classes.length; i += 37) {
    equal(tree.classIndexOf(classes[i]), classes.indexOf(classes[i]),
      `the index map disagrees with indexOf at ${i}`);
  }
  equal(tree.classIndexOf({ simpleName: 'not-in-the-tree' }), -1,
    'an unknown class must resolve to -1, as indexOf did');
}
