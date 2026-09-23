/**
 * shared/resizable-panel.js — the drag math and the localStorage persistence.
 *
 * The DOM stub in tests/lib/dom.mjs is deliberately too thin for this: its
 * querySelector always returns null, which is fine for the rendering-only
 * modules it was built for, but this module's entire public surface is
 * "find the panel via a selector, then track document-level mouse events on
 * it" — so this file brings its own tiny fake DOM instead of stretching the
 * shared one to fit a shape it wasn't designed for.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SOURCE = await readFile(path.join(STATIC, 'shared/resizable-panel.js'), 'utf8');

/** A fake localStorage — same get/set/remove surface as the real one. */
function makeLocalStorage() {
  const store = new Map();
  return {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
    _store: store,
  };
}

/** Just enough element to exercise install(): classList, dataset, style, rect, one-selector querySelector. */
function makeFakeElement(matches) {
  const listeners = {};
  const el = {
    tagName: 'DIV',
    style: {},
    dataset: {},
    parentNode: null,
    children: [],
    matches,
    classList: {
      _s: new Set(),
      add(c) { this._s.add(c); },
      remove(c) { this._s.delete(c); },
      contains(c) { return this._s.has(c); },
    },
    setAttribute() {},
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
    removeEventListener(type, fn) {
      if (!listeners[type]) return;
      listeners[type] = listeners[type].filter((f) => f !== fn);
    },
    _fire(type, evt) { (listeners[type] || []).forEach((fn) => fn(evt)); },
    _rectWidth: 280,
    getBoundingClientRect() { return { width: parseFloat(this.style.width) || this._rectWidth }; },
    insertBefore(child, ref) {
      const at = ref ? this.children.indexOf(ref) : -1;
      at < 0 ? this.children.push(child) : this.children.splice(at, 0, child);
      child.parentNode = this;
      return child;
    },
    nextSibling: null,
  };
  return el;
}

/** Loads the module against a single panel (`.sidebar`) with no other autoInit targets present. */
function loadModule({ savedWidth } = {}) {
  const localStorage = makeLocalStorage();
  if (savedWidth != null) localStorage.setItem('mdb-panel-width:jar:sidebar', String(savedWidth));

  const panel = makeFakeElement((sel) => sel === '.sidebar');
  const parent = makeFakeElement(() => false);
  parent.insertBefore(panel, null);
  panel.parentNode = parent;

  const documentListeners = {};
  const bodyListeners = {};
  const body = {
    classList: { _s: new Set(), add(c) { this._s.add(c); }, remove(c) { this._s.delete(c); }, contains(c) { return this._s.has(c); } },
  };

  const doc = {
    readyState: 'complete',
    querySelector(sel) {
      if (sel === '.sidebar') return panel;
      return null; // no .left-panel / .code-split-left / .sidebar-toggle-btn on this fake page
    },
    createElement() { return makeFakeElement(() => false); },
    addEventListener(type, fn) { (documentListeners[type] = documentListeners[type] || []).push(fn); },
    removeEventListener(type, fn) {
      if (!documentListeners[type]) return;
      documentListeners[type] = documentListeners[type].filter((f) => f !== fn);
    },
    body,
  };

  const win = {
    location: { pathname: '/jar/index.html' },
    innerWidth: 1200,
    document: doc,
    localStorage,
  };
  win.window = win;

  const fn = new Function('window', 'document', 'localStorage', SOURCE + '\n//# sourceURL=resizable-panel.js');
  fn(win, doc, localStorage);

  return { panel, doc, localStorage, documentListeners, MdbResizablePanel: win.MdbResizablePanel };
}

export function test_clamp_stays_within_bounds() {
  const { MdbResizablePanel } = loadModule();
  equal(MdbResizablePanel.clamp(100, 180, 600), 180, 'below min clamps up to min');
  equal(MdbResizablePanel.clamp(900, 180, 600), 600, 'above max clamps down to max');
  equal(MdbResizablePanel.clamp(300, 180, 600), 300, 'within bounds passes through unchanged');
}

export function test_key_format_matches_documented_shape() {
  const { MdbResizablePanel } = loadModule();
  equal(MdbResizablePanel.keyFor('jar', 'sidebar'), 'mdb-panel-width:jar:sidebar');
}

export function test_install_inserts_one_handle_after_the_panel() {
  const { panel } = loadModule();
  equal(panel.parentNode.children.length, 2, 'panel + handle');
  equal(panel.parentNode.children[1].tagName, 'DIV');
}

export function test_drag_moves_width_by_exact_mouse_delta_and_clamps() {
  const { panel, doc, documentListeners } = loadModule();
  const handle = panel.parentNode.children[1];

  panel._rectWidth = 280; // starting width before any drag
  handle._fire('mousedown', { clientX: 500, preventDefault() {} });

  // Drag +50px: 280 -> 330, well inside [180, 600 (=1200*0.5)].
  documentListeners.mousemove.forEach((fn) => fn({ clientX: 550, preventDefault() {} }));
  equal(panel.style.width, '330px', 'width tracks the exact drag delta');
  equal(panel.style.minWidth, '330px', 'min-width is kept in lockstep so CSS cannot override it');

  // Drag far past the 600px max (1200 viewport * 0.5) — must clamp, not follow.
  documentListeners.mousemove.forEach((fn) => fn({ clientX: 5000, preventDefault() {} }));
  equal(panel.style.width, '600px', 'clamped to the 50%-viewport max');

  // Drag far below the 180px min — must clamp, not follow.
  documentListeners.mousemove.forEach((fn) => fn({ clientX: -5000, preventDefault() {} }));
  equal(panel.style.width, '180px', 'clamped to the 180px min');

  documentListeners.mouseup.forEach((fn) => fn());
}

export function test_mouseup_persists_the_final_width_to_localstorage() {
  const { panel, documentListeners, localStorage } = loadModule();
  const handle = panel.parentNode.children[1];

  panel._rectWidth = 280;
  handle._fire('mousedown', { clientX: 500, preventDefault() {} });
  documentListeners.mousemove.forEach((fn) => fn({ clientX: 550, preventDefault() {} }));
  documentListeners.mouseup.forEach((fn) => fn());

  equal(localStorage.getItem('mdb-panel-width:jar:sidebar'), '330');
}

export function test_saved_width_is_restored_on_load() {
  const { panel } = loadModule({ savedWidth: 340 });
  equal(panel.style.width, '340px');
  equal(panel.style.minWidth, '340px');
}

export function test_out_of_range_saved_width_is_clamped_on_restore() {
  const { panel } = loadModule({ savedWidth: 50 });
  equal(panel.style.width, '180px', 'a stale/tampered value below min is clamped, not applied verbatim');
}

export function test_mousedown_on_collapsed_panel_does_not_start_a_drag() {
  const { panel, documentListeners } = loadModule();
  panel.classList.add('sidebar-collapsed');
  const handle = panel.parentNode.children[1];

  handle._fire('mousedown', { clientX: 500, preventDefault() {} });

  ok(!documentListeners.mousemove || documentListeners.mousemove.length === 0,
    'no mousemove listener attached — collapsed panels are not draggable');
}
