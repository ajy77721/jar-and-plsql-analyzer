window.PA = window.PA || {};

/*
 * Resizable columns + horizontal scrolling for the parser's tables.
 *
 * ## Why one module, hooked onto the shared renderer
 *
 * Every grid on this page is drawn by `PA.tf` (`shared/tableFramework.js`) — References, Table
 * Ops, Sequences, Cursors, Joins, Statements, the table-detail tabs, and a dozen more. They all
 * build their <thead> in their own `_initTable` and then hand the row renderer to `PA.tf.init`,
 * so the one place every grid passes through at render time is `PA.tf.render`/`PA.tf.init`. This
 * file wraps those two methods and enhances whatever table they just filled in — one wiring point
 * for all of them, rather than a copy of the drag code in every panel.
 *
 * The Explore → Trace view is not a `PA.tf` grid: it renders rows as a CSS grid of divs
 * (`PA.trace` in app.js). It gets the same feature through `enhanceGrid`, which drives the same
 * drag code from a header row built by the trace renderer.
 *
 * ## What "resize" means here
 *
 * A 7px grip sits on the right edge of every header cell. Dragging it sets that column's width;
 * double-clicking resets the column to the width it had when the table first appeared. Widths are
 * persisted per table id under `mdb-col-width:<page>:<tableId>` — the same shape
 * `shared/resizable-panel.js` already uses for panel widths (`mdb-panel-width:<page>:<panelId>`),
 * so the two read the same way.
 *
 * The table gets `table-layout: fixed` with a <colgroup> of explicit widths, plus a `min-width`
 * equal to their sum. That last part is the point: with fixed layout a column can never be
 * squeezed below its width to make room for a long proc chain in a neighbour — once the columns
 * together exceed the container the wrapper (`.tbl-scroll`, `overflow-x: auto`) scrolls instead.
 */
PA.tblResize = {
    /** Hard floor for a dragged column, so a column can never be dragged to nothing. */
    MIN_COL: 56,

    STORAGE_PREFIX: 'mdb-col-width:',

    _page: function() {
        var seg = window.location.pathname.split('/').filter(Boolean)[0];
        return seg || 'root';
    },

    _key: function(id) { return PA.tblResize.STORAGE_PREFIX + PA.tblResize._page() + ':' + id; },

    _read: function(id) {
        try {
            var raw = localStorage.getItem(PA.tblResize._key(id));
            if (!raw) return null;
            var arr = JSON.parse(raw);
            return Array.isArray(arr) ? arr : null;
        } catch (e) { return null; }
    },

    _save: function(id, widths) {
        try { localStorage.setItem(PA.tblResize._key(id), JSON.stringify(widths)); } catch (e) { /* private mode */ }
    },

    /** Called after every `PA.tf` init/render. No-ops when the table isn't in the DOM yet. */
    enhanceById: function(id) {
        var tbody = document.getElementById(id + '-tbody');
        var table = tbody && tbody.closest ? tbody.closest('table') : null;
        if (table) PA.tblResize.enhanceTable(table, id);
    },

    /** Ensures `table` sits inside a horizontal-scroll wrapper. Idempotent. */
    _wrap: function(table) {
        var parent = table.parentNode;
        if (parent && parent.classList && parent.classList.contains('tbl-scroll')) return;
        var wrap = document.createElement('div');
        wrap.className = 'tbl-scroll';
        parent.insertBefore(wrap, table);
        wrap.appendChild(table);
    },

    /**
     * Builds the <colgroup>/grips for a real `<table>`, or returns without touching it when it
     * cannot be measured (a table rendered while its tab is hidden has zero-width headers, and
     * inventing a width for every column would be worse than waiting). The next render retries.
     */
    enhanceTable: function(table, id) {
        if (!table || !table.querySelectorAll || table._tblResize) return;
        var ths = table.querySelectorAll('thead th');
        if (!ths || ths.length === 0) return;

        var saved = PA.tblResize._read(id);
        var defaults = [];
        var total = 0;
        for (var i = 0; i < ths.length; i++) {
            var measured = (ths[i].getBoundingClientRect ? ths[i].getBoundingClientRect().width : 0) || 0;
            var w = (saved && saved[i] > 0) ? saved[i] : (measured > 0 ? Math.round(measured) : 0);
            defaults.push(w);
            total += w;
        }
        if (total <= 0) return;   // hidden/unmeasurable — retry on the next render

        PA.tblResize._wrap(table);
        table.classList.add('tbl-fixed');
        table.style.tableLayout = 'fixed';
        table.style.minWidth = total + 'px';

        var cg = table.querySelector('colgroup');
        if (!cg) {
            cg = document.createElement('colgroup');
            table.insertBefore(cg, table.firstChild);
        }
        cg.innerHTML = '';
        var cols = [];
        for (var j = 0; j < defaults.length; j++) {
            var col = document.createElement('col');
            col.style.width = Math.max(PA.tblResize.MIN_COL, defaults[j]) + 'px';
            cg.appendChild(col);
            cols.push(col);
        }

        var state = { id: id, cols: cols, defaults: defaults };
        table._tblResize = state;
        PA.tblResize._syncMin(table);

        for (var k = 0; k < ths.length; k++) {
            (function(idx, th) {
                PA.tblResize._grip(th, idx,
                    function() { return parseFloat(state.cols[idx].style.width) || state.defaults[idx]; },
                    function(w) { state.cols[idx].style.width = w + 'px'; PA.tblResize._syncMin(table); },
                    function() { PA.tblResize._persistTable(table); },
                    function() {
                        state.cols[idx].style.width = Math.max(PA.tblResize.MIN_COL, state.defaults[idx]) + 'px';
                        PA.tblResize._syncMin(table);
                        PA.tblResize._persistTable(table);
                    },
                    PA.tblResize.MIN_COL);
            })(k, ths[k]);
        }
    },

    _syncMin: function(table) {
        var state = table._tblResize;
        if (!state) return;
        var sum = 0;
        for (var i = 0; i < state.cols.length; i++) sum += parseFloat(state.cols[i].style.width) || 0;
        table.style.minWidth = sum + 'px';
    },

    _persistTable: function(table) {
        var state = table._tblResize;
        if (!state) return;
        var out = [];
        for (var i = 0; i < state.cols.length; i++) out.push(Math.round(parseFloat(state.cols[i].style.width) || 0));
        PA.tblResize._save(state.id, out);
    },

    /**
     * The trace's column set. A header row is built by `PA.trace._render` with these var names /
     * defaults / floors on each cell; this reads them back and wires the same drag behaviour.
     */
    enhanceGrid: function(head, id) {
        if (!head || !head.querySelectorAll) return;
        var grid = head.parentNode;
        if (!grid || !grid.style) return;
        var cells = head.querySelectorAll('[data-col]');
        if (!cells.length) return;

        var saved = PA.tblResize._read(id);
        var cols = [];
        for (var i = 0; i < cells.length; i++) {
            var c = cells[i];
            var idx = parseInt(c.getAttribute('data-col'), 10);
            var def = parseInt(c.getAttribute('data-default'), 10) || 160;
            var min = parseInt(c.getAttribute('data-min'), 10) || PA.tblResize.MIN_COL;
            var varName = c.getAttribute('data-var');
            if (!varName) continue;
            var w = (saved && saved[idx] > 0) ? saved[idx] : def;
            cols[idx] = { cell: c, varName: varName, def: def, min: min, w: w };
            grid.style.setProperty(varName, w + 'px');
        }
        if (!cols.length) return;

        for (var j = 0; j < cols.length; j++) {
            if (!cols[j]) continue;
            (function(col, cellIdx) {
                PA.tblResize._grip(col.cell, cellIdx,
                    function() { return col.w; },
                    function(w) { col.w = w; grid.style.setProperty(col.varName, w + 'px'); },
                    function() { PA.tblResize._persistGrid(id, cols); },
                    function() {
                        col.w = col.def;
                        grid.style.setProperty(col.varName, col.def + 'px');
                        PA.tblResize._persistGrid(id, cols);
                    },
                    col.min);
            })(cols[j], j);
        }
    },

    _persistGrid: function(id, cols) {
        var out = [];
        for (var i = 0; i < cols.length; i++) out.push(cols[i] ? Math.round(cols[i].w) : 0);
        PA.tblResize._save(id, out);
    },

    /**
     * Attaches one grip to `host`.
     *
     * getStart()  -> current width in px
     * apply(w)    -> set the column to w px
     * commit()    -> persist after a drag ends
     * reset()     -> restore this column's default width
     */
    _grip: function(host, colIdx, getStart, apply, commit, reset, min) {
        if (host.querySelector('.tbl-grip')) return;
        host.style.position = 'relative';
        var grip = document.createElement('span');
        grip.className = 'tbl-grip';
        grip.setAttribute('data-col', colIdx);
        grip.title = 'Drag to resize column · double-click to reset';
        host.appendChild(grip);

        // The header cell carries the sort click (onclick) — none of this may reach it.
        grip.addEventListener('mousedown', function(e) {
            e.preventDefault();
            e.stopPropagation();
            grip.classList.add('active');
            PA.tblResize._drag(e, getStart(), min, apply, function(moved) {
                grip.classList.remove('active');
                if (moved) commit();
            });
        });
        grip.addEventListener('click', function(e) { e.stopPropagation(); });
        grip.addEventListener('dblclick', function(e) {
            e.preventDefault();
            e.stopPropagation();
            reset();
        });
    },

    _drag: function(e, startW, min, apply, done) {
        var startX = e.clientX;
        var floor = min || PA.tblResize.MIN_COL;
        var moved = false;
        document.body.classList.add('tbl-resizing');

        function onMove(ev) {
            var w = Math.max(floor, Math.round(startW + (ev.clientX - startX)));
            apply(w);
            moved = true;
        }
        function onUp() {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.classList.remove('tbl-resizing');
            done(moved);
        }
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    }
};

/**
 * Hook the shared table framework. `PA.tf.init` draws a table's first page and `PA.tf.render`
 * redraws it on every filter/sort/page — the latter is where a table rebuilt by a panel's own
 * `_initTable` (a fresh element each `load()`) gets picked up again.
 *
 * Wrapping at load time rather than inside the framework keeps this parser-only: `shared/
 * tableFramework.js` is also loaded by the plsql and jar pages, and this feature is a parser
 * decision. If they want it, they load this file too.
 */
(function() {
    'use strict';
    function hook() {
        if (!window.PA || !PA.tf || PA.tf._tblResizeHooked) return;
        PA.tf._tblResizeHooked = true;
        var origInit = PA.tf.init;
        var origRender = PA.tf.render;
        if (typeof origInit === 'function') {
            PA.tf.init = function(id) {
                var out = origInit.apply(this, arguments);
                PA.tblResize.enhanceById(id);
                return out;
            };
        }
        if (typeof origRender === 'function') {
            PA.tf.render = function(id) {
                var out = origRender.apply(this, arguments);
                PA.tblResize.enhanceById(id);
                return out;
            };
        }
    }
    hook();
    // tableFramework.js is loaded before this file, but keep the same safety the page's other
    // late-bound modules use: if load order ever changes, wire up on DOMContentLoaded instead of
    // silently never applying the feature.
    if (!PA.tf && document.addEventListener) document.addEventListener('DOMContentLoaded', hook);
})();
