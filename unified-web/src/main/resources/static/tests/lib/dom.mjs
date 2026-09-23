/**
 * A DOM stub big enough to run this codebase's rendering functions.
 *
 * <p>Not a browser and not trying to be. The rendering code here builds HTML strings and assigns
 * them to `innerHTML`, so what a test needs is somewhere for those strings to land and a handful of
 * lookups that do not throw. Anything a real browser would do beyond that — layout, CSS, event
 * dispatch — is out of scope and documented as such in the README.
 */

export function makeElement(id = '') {
  return {
    id,
    tagName: 'DIV',
    value: '',
    textContent: '',
    innerHTML: '',
    title: '',
    className: '',
    style: {},
    // `dataset` is how the pager remembers it has been moved. Omitting it made the parser
    // framework throw mid-render, and the resulting empty output looked like the two forks
    // disagreeing — a harness gap masquerading as a product difference.
    dataset: {},
    attributes: {},
    children: [],
    // Insertion is modelled because the code under test creates a notice, inserts it, and then
    // expects a later call to find it by id. A stub where createElement alone made an element
    // findable would pass even if the insert were dropped.
    parentNode: null,
    classList: {
      _s: new Set(),
      add(c) { this._s.add(c); },
      remove(c) { this._s.delete(c); },
      toggle(c, on) { on === undefined ? (this._s.has(c) ? this._s.delete(c) : this._s.add(c)) : (on ? this._s.add(c) : this._s.delete(c)); },
      contains(c) { return this._s.has(c); },
    },
    getAttribute(k) { return k === 'type' ? (this.attributes.type || null) : (this.attributes[k] ?? null); },
    setAttribute(k, v) { this.attributes[k] = v; },
    removeAttribute(k) { delete this.attributes[k]; if (k === 'title') this.title = ''; },
    appendChild(c) { this.children.push(c); c.parentNode = this; if (this._doc) this._doc._attach(c); return c; },
    insertBefore(c, ref) {
      const at = ref ? this.children.indexOf(ref) : -1;
      at < 0 ? this.children.push(c) : this.children.splice(at, 0, c);
      c.parentNode = this;
      if (this._doc) this._doc._attach(c);
      return c;
    },
    remove() {
      if (!this.parentNode) return;
      const at = this.parentNode.children.indexOf(this);
      if (at >= 0) this.parentNode.children.splice(at, 1);
      if (this.parentNode._doc) this.parentNode._doc._detach(this);
      this.parentNode = null;
    },
    addEventListener() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
    // `PA.tf._updateSortIndicators` walks up from the tbody to find its table. There is no real
    // tree here, so the honest answer is "not found" — the caller already handles that, and a
    // stub that threw would fail a test for the harness's shortcoming rather than the code's.
    closest() { return null; },
  };
}

/** Installs globals. Returns a handle for registering elements and reading what was rendered. */
export function installDom() {
  const els = new Map();
  const doc = {
    getElementById: (id) => els.get(id) || null,
    querySelector: () => null,
    querySelectorAll: () => [],
    createElement: (tag) => {
      const e = makeElement();
      e.tagName = String(tag || 'div').toUpperCase();
      e._doc = doc;
      return e;
    },
    // An element becomes findable when it enters the document, not when it is constructed.
    _attach(e) { e._doc = doc; if (e.id) els.set(e.id, e); },
    _detach(e) { if (e.id && els.get(e.id) === e) els.delete(e.id); },
    head: { appendChild() {} },
    body: { appendChild() {} },
    addEventListener() {},
    readyState: 'complete',
  };
  globalThis.document = doc;
  globalThis.window = globalThis;
  return {
    els,
    add(id) { const e = makeElement(id); e._doc = doc; els.set(id, e); return e; },
    get(id) { return els.get(id); },
  };
}

/**
 * The escaper `PA.tf` passes to a row renderer as its third argument (tableFramework.js:406).
 *
 * <p>Mirrors `shared/escape.js` exactly, apostrophe included. An earlier version omitted `'`, and
 * a golden captured through it disagreed with what the app actually renders — a test asserting
 * markup the product never produces is worse than no test, because it fails for the wrong reason
 * and gets "fixed" by loosening the assertion.
 */
export const esc = (v) => String(v == null ? '' : v)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

/**
 * A stand-in for the paging table framework that captures the row renderer.
 *
 * <p>Capturing it is the whole point: the renderer is where the markup a user sees is produced, and
 * it is handed to `PA.tf.init` rather than exported, so a test cannot reach it any other way.
 */
export function installTableFramework(target) {
  const captured = { rowFn: null, items: null };
  const tf = new Proxy({
    _state: {},
    init(id, items, size, rowFn) { captured.rowFn = rowFn; captured.items = items; this._state[id] = { items }; },
    state: () => ({}),
  }, { get: (t, k) => (k in t ? t[k] : () => {}) });
  target.tf = tf;
  return captured;
}
