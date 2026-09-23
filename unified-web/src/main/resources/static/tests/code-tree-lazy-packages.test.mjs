/**
 * UI-003 (§134) — above `EAGER_CLASS_LIMIT`, the Code Structure tab's default (package) view no
 * longer fetches every class to render its own top level. §118 found the tree itself has nothing
 * to aggregate (Project/Visual view and the JAR-source filter genuinely need every class, by
 * design), but the *package list* — name, source JAR, class count — is a real aggregate, and this
 * is what lets the tree stop paying for the whole corpus on every load.
 *
 * At/under `EAGER_CLASS_LIMIT` nothing here changes at all — see `code-tree.test.mjs` for that
 * (unmodified) path's own tests.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom, makeElement } from './lib/dom.mjs';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function classRow(pkg, name, jar) {
  return {
    fullyQualifiedName: pkg + '.' + name, simpleName: name, packageName: pkg,
    sourceJar: jar || '', stereotype: 'SERVICE', annotations: [], fields: [], methods: [],
  };
}

/** A page-source stub matching PA.pageSource's own {items, page} contract. */
function pageSourceOf(items, calls) {
  let served = false;
  return {
    fetchPage: async (limit) => {
      if (calls) calls.push(limit);
      if (served) return { items: [], page: { hasMore: false } };
      served = true;
      return { items, page: { hasMore: false, total: items.length, totalIsExact: true } };
    },
  };
}

async function loadTree({ summary, fetchClassTreeCalls, classTreePagesImpl } = {}) {
  const dom = installDom();
  // The three background loaders (resources/bundled-jars/jar-dep-map) are orthogonal to this
  // section and already swallow their own failures into a console.warn — stubbed here only to
  // keep test output free of unrelated noise, not because their behavior is under test.
  globalThis.fetch = async () => ({ ok: false, status: 404 });
  globalThis.JA = {
    utils: {
      escapeHtml: (s) => String(s ?? ''),
      escapeAttr: (s) => String(s ?? ''),
      stereotypeColor: () => '#000',
      stereotypeBadgeClass: () => 'b',
      formatNumber: (n) => String(n),
      stereotypeCategory: () => 'other',
    },
    summary: {},
    app: { currentJarId: 'jar1', _currentVersion: undefined },
    api: {
      MAIN_JAR_SENTINEL: '__MAIN_JAR__',
      getClassPackages: async () => summary,
      classTreePages: classTreePagesImpl || (() => pageSourceOf([])),
      getClassTree: async () => { throw new Error('must not fetch the whole tree in the lazy path'); },
    },
    codeTreeViews: undefined,
    codeTreePanel: undefined,
  };
  // codeTree._fetchPackageClasses gates its lazy-paging path on `window.PA && PA.pageSource` —
  // the jar page defines both via shared/page-envelope.js at runtime (index.html loads it before
  // js/api.js). The harness never loaded it, so canPage was false here and every expansion fell
  // into the whole-tree fallback this file's lazy path exists to avoid. Provide the minimal
  // contract the product sees: PA.pageSource exists; the pager itself is the test's own
  // classTreePagesImpl stub.
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.pageSource = globalThis.PA.pageSource || function () { return {}; };
  new Function(await readFile(path.join(STATIC, 'jar/js/codeTree.js'), 'utf8'))();
  new Function(await readFile(path.join(STATIC, 'jar/js/codeTreeViews.js'), 'utf8'))();
  dom.add('class-tree');
  return { dom, tree: globalThis.JA.codeTree };
}

export async function test_a_large_jar_fetches_the_package_summary_not_every_class() {
  const summary = Array.from({ length: 600 }, (_, i) => ({
    packageName: 'com.acme.pkg' + i, sourceJar: '', classCount: 1,
  }));
  const { tree } = await loadTree({ summary });

  await tree.loadAndRender();

  equal(tree._classes.length, 0, 'no classes should be resident yet — only the summary was fetched');
  ok(tree._packageSummary, 'the package summary must be cached for later expand/filter use');
  equal(tree._packageSummary.length, 600);
}

export async function test_the_package_list_renders_one_row_per_package_with_its_own_count() {
  const summary = [
    { packageName: 'com.acme.service', sourceJar: '', classCount: 42 },
    { packageName: 'com.acme.web', sourceJar: '', classCount: 7 },
  ];
  const { tree } = await loadTree({ summary });
  tree.EAGER_CLASS_LIMIT = 10; // deliberately below this fixture's 49 classes, to force lazy mode
  await tree.loadAndRender();

  const html = document.getElementById('class-tree').innerHTML;
  includes(html, 'com.acme.service', 'the package name must be rendered');
  includes(html, '>42<', 'the package\'s own class count must be rendered');
  includes(html, 'com.acme.web');
  includes(html, '>7<');
}

export async function test_expanding_a_package_fetches_only_that_packages_own_classes() {
  const summary = [
    { packageName: 'com.acme.service', sourceJar: '', classCount: 2 },
    { packageName: 'com.acme.web', sourceJar: '', classCount: 1 },
  ];
  const cfSeen = [];
  const classTreePagesImpl = (id, version, q, cf) => {
    cfSeen.push(cf);
    if (cf && cf[0] === '0:com.acme.service') {
      return pageSourceOf([
        classRow('com.acme.service', 'OrderService', ''),
        classRow('com.acme.service', 'PaymentService', ''),
      ]);
    }
    return pageSourceOf([]);
  };
  const { tree } = await loadTree({ summary, classTreePagesImpl });
  tree.EAGER_CLASS_LIMIT = 1; // deliberately below this fixture's 3 classes, to force lazy mode
  await tree.loadAndRender();

  // The shared DOM stub's querySelector is a stub returning null by design (see lib/dom.mjs's own
  // comment) — a real tree/sibling pair is built by hand instead, the same pattern
  // code-tree.test.mjs's own togglePkg test already established. `com.acme.service` is the first
  // summary row, so `_pendingSeq` is 1 by the time it is rendered.
  equal(tree._pendingPackages.get('p1').pkg, 'com.acme.service',
    'the fixture\'s own first package must be key p1 — otherwise this test is not exercising it');
  const children = makeElement();
  children.dataset.pkgKey = '1';
  const toggle = makeElement();
  toggle.nextElementSibling = children;
  await tree.togglePkg(toggle);

  equal(tree._classes.length, 2, 'only the expanded package\'s 2 classes should now be resident');
  ok(cfSeen.some(cf => cf && cf[0] === '0:com.acme.service' && cf[1] === '1:__MAIN_JAR__'),
    'the fetch must filter by packageName and the main-JAR sentinel, not the empty string');
  includes(children.innerHTML, 'OrderService', 'the expanded package\'s classes must now be rendered');
}

export async function test_switching_to_project_view_fetches_the_full_class_list_once() {
  const summary = [{ packageName: 'com.acme.service', sourceJar: '', classCount: 2 }];
  let fetchCount = 0;
  const { tree } = await loadTree({ summary });
  tree.EAGER_CLASS_LIMIT = 1; // deliberately below this fixture's 2 classes, to force lazy mode
  tree._fetchAllClassPages = async () => {
    fetchCount++;
    return [classRow('com.acme.service', 'A', ''), classRow('com.acme.service', 'B', '')];
  };
  JA.codeTreeViews.renderProjectView = () => {};
  await tree.loadAndRender();
  equal(tree._classesFullyLoaded, false, 'package view alone must not have triggered a full fetch');

  await tree.setViewMode('project');
  equal(fetchCount, 1, 'switching to Project view must fetch the full class list exactly once');
  equal(tree._classesFullyLoaded, true);

  await tree.setViewMode('package');
  await tree.setViewMode('project');
  equal(fetchCount, 1, 'switching back to Project view again must reuse the cached full list');
}

/**
 * A direct, single-purpose test of `_ensureFullClassesLoaded`'s own cache guard, independent of
 * `setViewMode`'s own (redundant, defense-in-depth) mode check — calibrated by disabling this
 * method's cache check specifically and confirming this test, not the one above, is what catches
 * it (the one above is masked by `setViewMode`'s own guard for this exact call sequence).
 */
export async function test_ensureFullClassesLoaded_itself_only_fetches_once() {
  const summary = [{ packageName: 'com.acme.service', sourceJar: '', classCount: 2 }];
  let fetchCount = 0;
  const { tree } = await loadTree({ summary });
  tree.EAGER_CLASS_LIMIT = 1;
  tree._fetchAllClassPages = async () => {
    fetchCount++;
    return [classRow('com.acme.service', 'A', '')];
  };
  await tree.loadAndRender();

  await tree._ensureFullClassesLoaded();
  await tree._ensureFullClassesLoaded();
  equal(fetchCount, 1, '_ensureFullClassesLoaded must not fetch a second time once already loaded');
}

/**
 * UI-003 (§134) — the pre-existing bug found while building this section: cross-tab navigation to
 * a class in a still-collapsed package failed silently before this section too, for any JAR over
 * `EAGER_CLASS_LIMIT`, independent of the lazy-package-view work itself (see `findAndHighlight`'s
 * own comment). `_resolveAndExpandPackage` is the fix; tested directly since the shared DOM stub's
 * `querySelector` always returns null by design (see lib/dom.mjs) and cannot exercise a real
 * class-node search — the package-node lookup below is monkey-patched in for this one test only.
 */
export async function test_resolveAndExpandPackage_finds_a_class_via_the_server_when_not_resident() {
  const summary = [
    { packageName: 'com.acme.service', sourceJar: '', classCount: 1 },
    { packageName: 'com.acme.web', sourceJar: '', classCount: 1 },
  ];
  const classTreePagesImpl = (id, version, q) => {
    equal(q, 'OrderService', 'must search by the class name asked for');
    return pageSourceOf([classRow('com.acme.service', 'OrderService', '')]);
  };
  const { tree } = await loadTree({ summary, classTreePagesImpl });
  tree.EAGER_CLASS_LIMIT = 0;
  await tree.loadAndRender();

  const children = makeElement();
  children.classList._set = new Set(['collapsed']);
  children.classList.contains = (c) => children.classList._set.has(c);
  const toggle = makeElement();
  const pkgNode = makeElement();
  pkgNode.querySelector = (sel) => (sel.includes('tree-toggle') ? toggle : children);
  document.querySelector = (sel) => (sel.includes('data-pkg-name') ? pkgNode : null);

  const opened = await tree._resolveAndExpandPackage('OrderService');
  ok(opened, 'a class not yet resident must still resolve to its package and open it');
}

export async function test_resolveAndExpandPackage_returns_false_for_an_unknown_class() {
  const summary = [{ packageName: 'com.acme.service', sourceJar: '', classCount: 1 }];
  const classTreePagesImpl = () => pageSourceOf([]);
  const { tree } = await loadTree({ summary, classTreePagesImpl });
  tree.EAGER_CLASS_LIMIT = 0;
  await tree.loadAndRender();

  const opened = await tree._resolveAndExpandPackage('NoSuchClass');
  ok(!opened, 'an unresolvable class must not be reported as opened');
}

export async function test_a_small_jar_is_completely_unaffected() {
  // Below EAGER_CLASS_LIMIT: the summary endpoint may not even be consulted meaningfully — the
  // old eager fetch-and-render path must run exactly as it always has.
  const summary = [{ packageName: 'com.acme.service', sourceJar: '', classCount: 3 }];
  const { tree } = await loadTree({ summary });
  tree._fetchAllClassPages = async () => [
    classRow('com.acme.service', 'A', ''), classRow('com.acme.service', 'B', ''),
    classRow('com.acme.service', 'C', ''),
  ];
  await tree.loadAndRender();

  equal(tree._classes.length, 3, 'a small JAR must still eagerly hold every class, unchanged');
  equal(tree._classesFullyLoaded, true);
  const html = document.getElementById('class-tree').innerHTML;
  includes(html, 'tree-toggle expanded', 'a small JAR must still open fully expanded');
}
