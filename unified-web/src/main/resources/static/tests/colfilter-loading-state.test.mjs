/**
 * UX fix — a column that WILL get a filter icon once its async fetch (facet summary, whole-table
 * lookup, …) resolves must show something the instant the table renders, not silence.
 *
 * Without this, the gap between "table rendered" and "initColFilters ran" — every per-screen call
 * site that fetches server values before attaching filters (plsql/js/tableOps.js,
 * plsql/js/joins.js, parser/js/joins.js, plsql/js/search.js, parser/js/tableOps.js,
 * parser/js/cursors.js) — showed a header with nothing where a `.cf-icon` was about to appear. On
 * a slow connection that reads as "the filter isn't there", which is exactly what was reported.
 *
 * `PA.tf.pendingColFilters(id, columns)` renders a pulsing placeholder synchronously; the real
 * `initColFilters` call removes it and attaches the real, interactive icon; `colFiltersFailed`
 * replaces it with a distinct "unavailable" state instead of leaving it pulsing forever.
 *
 * This loads the real `shared/tableFramework.js` (same technique as
 * `tests/lib/tf-golden.mjs`/`tableframework-golden.test.mjs`) against a small hand-built DOM that
 * supports what these three methods actually touch: `<th>` elements found via `querySelectorAll`,
 * `.cf-icon` lookups, and header-relative appendChild.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SHARED = path.join(STATIC, 'shared/tableFramework.js');

/** A `<th>`/`<span>` stub good enough for the cf-icon methods under test. */
function makeEl(tag) {
  const el = {
    tagName: tag.toUpperCase(),
    style: {},
    dataset: {},
    className: '',
    title: '',
    children: [],
    _classes: new Set(),
    classList: {
      add(...cs) { cs.forEach(c => el._classes.add(c)); },
      remove(...cs) { cs.forEach(c => el._classes.delete(c)); },
      toggle(c, on) { on ? el._classes.add(c) : el._classes.delete(c); },
      contains(c) { return el._classes.has(c); },
    },
    appendChild(c) { c.parentElement = el; el.children.push(c); return c; },
    remove() {
      if (!el.parentElement) return;
      const i = el.parentElement.children.indexOf(el);
      if (i >= 0) el.parentElement.children.splice(i, 1);
      el.parentElement = null;
    },
    querySelector(sel) { return findAll(el, sel)[0] || null; },
    querySelectorAll(sel) { return findAll(el, sel); },
    closest(sel) {
      let cur = el;
      while (cur) { if (matches(cur, sel)) return cur; cur = cur.parentElement; }
      return null;
    },
  };
  Object.defineProperty(el, 'className', {
    get() { return [...el._classes].join(' '); },
    set(v) { el._classes = new Set(String(v).split(/\s+/).filter(Boolean)); },
  });
  return el;
}

function matches(el, sel) {
  if (sel === 'table') return el.tagName === 'TABLE';
  if (sel.startsWith('.')) {
    // Compound class selectors, e.g. ".cf-icon.cf-pending" — every class must be present.
    return sel.slice(1).split('.').every(c => el._classes.has(c));
  }
  if (sel === 'th') return el.tagName === 'TH';
  if (sel === 'th[data-sort-col]') return el.tagName === 'TH' && el.dataset.sortCol != null;
  return false;
}

function findAll(root, sel) {
  const out = [];
  const parts = sel.split(',').map(s => s.trim());
  (function walk(node) {
    for (const c of node.children) {
      if (parts.some(p => matches(c, p))) out.push(c);
      walk(c);
    }
  })(root);
  return out;
}

/** Builds a minimal `#id-tbody` inside a `<table>` with the given number of `<th>` columns. */
function buildTable(doc, id, nCols) {
  const table = makeEl('table');
  const thead = makeEl('thead');
  const tr = makeEl('tr');
  for (let i = 0; i < nCols; i++) {
    const th = makeEl('th');
    th.dataset.sortCol = String(i);
    tr.appendChild(th);
  }
  thead.appendChild(tr);
  table.appendChild(thead);
  const tbody = makeEl('tbody');
  tbody.id = id + '-tbody';
  table.appendChild(tbody);
  doc.els.set(tbody.id, tbody);
  return { table, tbody };
}

async function loadFramework() {
  const els = new Map();
  const doc = {
    els,
    getElementById: (id) => els.get(id) || null,
    createElement: (tag) => makeEl(tag),
    addEventListener() {},
    removeEventListener() {},
    body: { appendChild() {}, insertAdjacentHTML() {} },
  };
  globalThis.document = doc;
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.esc = (v) => String(v == null ? '' : v);
  globalThis.PA.escAttr = (v) => String(v == null ? '' : v);

  const src = await readFile(SHARED, 'utf8');
  new Function(src)();
  return { tf: globalThis.PA.tf, doc };
}

export async function test_placeholder_appears_synchronously_before_any_data_arrives() {
  const { tf, doc } = await loadFramework();
  const { table } = buildTable(doc, 'to', 2);

  tf.init('to', [{ a: 1 }], 50, () => '<tr></tr>', {});
  tf.render('to');

  // No fetch has resolved yet — this is the synchronous call a screen makes right after its table
  // is in the DOM, before its async facet/summary fetch even starts.
  tf.pendingColFilters('to', { 0: { label: 'Schema' }, 1: { label: 'Table' } });

  const icons = table.querySelectorAll('.cf-icon');
  equal(icons.length, 2, 'a placeholder icon must appear for every column that will get a filter');
  ok(icons.every(i => i.classList.contains('cf-pending')), 'the placeholder must be visually distinct (cf-pending)');
  ok(icons.every(i => /loading/i.test(i.title)), 'the placeholder must say it is loading, not silently sit there');
}

export async function test_real_icon_replaces_the_placeholder_once_data_resolves() {
  const { tf, doc } = await loadFramework();
  const { table } = buildTable(doc, 'to', 2);

  tf.init('to', [{ a: 1 }], 50, () => '<tr></tr>', {});
  tf.render('to');
  tf.pendingColFilters('to', { 0: { label: 'Schema' }, 1: { label: 'Table' } });
  equal(table.querySelectorAll('.cf-icon.cf-pending').length, 2, 'placeholder did not render');

  // Simulates the async fetch resolving and the screen making its real call.
  tf.initColFilters('to', {
    0: { label: 'Schema', valueFn: (t) => 'S' },
    1: { label: 'Table', valueFn: (t) => 'T' },
  });

  equal(table.querySelectorAll('.cf-icon.cf-pending').length, 0, 'the placeholder must be gone once the real icon attaches');
  equal(table.querySelectorAll('.cf-icon').length, 2, 'the real filter icon must be in its place');
}

export async function test_failed_fetch_shows_an_unavailable_state_not_silence() {
  const { tf, doc } = await loadFramework();
  const { table } = buildTable(doc, 'to', 2);

  tf.init('to', [{ a: 1 }], 50, () => '<tr></tr>', {});
  tf.render('to');
  tf.pendingColFilters('to', { 0: { label: 'Schema' }, 1: { label: 'Table' } });

  tf.colFiltersFailed('to', { 0: { label: 'Schema' }, 1: { label: 'Table' } }, 'network error');

  const icons = table.querySelectorAll('.cf-icon');
  equal(icons.length, 2, 'the header must still show an icon, not go back to nothing');
  ok(icons.every(i => i.classList.contains('cf-error')), 'a failed fetch must render the distinct error state');
  ok(icons.every(i => i.classList.contains('cf-pending') === false), 'the error state must not still be pulsing as if loading');
  ok(icons.every(i => /unavailable/i.test(i.title) && /network error/i.test(i.title)),
    'the tooltip must explain the failure, not just look muted');
}

export async function test_a_synchronous_caller_that_never_uses_pending_is_unaffected() {
  // Existing call sites that already have data at init time never call `pendingColFilters` at all
  // — this proves that path still attaches the real icon exactly as before.
  const { tf, doc } = await loadFramework();
  const { table } = buildTable(doc, 'to', 1);

  tf.init('to', [{ a: 1 }], 50, () => '<tr></tr>', {});
  tf.render('to');
  tf.initColFilters('to', { 0: { label: 'Only', valueFn: (t) => 'V' } });

  const icons = table.querySelectorAll('.cf-icon');
  equal(icons.length, 1, 'the ordinary synchronous path must still attach exactly one icon');
  ok(!icons[0].classList.contains('cf-pending') && !icons[0].classList.contains('cf-error'),
    'a synchronously-attached icon must be the real, active one');
}
