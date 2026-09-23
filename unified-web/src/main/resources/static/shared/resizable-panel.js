/* ==========================================================================
   Draggable resize handle for split-panel layouts — TASK-REGISTRY §282.

   One shared mechanism instead of three one-off ones, wired onto every real
   left/right split found in the app:
     - jar:            <aside class="sidebar"> next to <main class="content">
     - parser/plsql:   <div class="left-panel"> next to <div class="right-panel">
                       (both inside .analysis-layout)
     - jar code view:  <div class="code-split-left"> next to .code-split-right
                       (inside .code-split-container)

   Each handle is a thin sibling inserted between the two flex children. A
   drag changes the *left* panel's inline width/min-width (the right side is
   flex:1 and simply fills whatever space is left — same assumption
   sidebar-toggle.js already relies on for its own collapse mechanic).

   Persistence mirrors shared/sidebar-toggle.js's own localStorage pattern
   exactly: one prefixed key per page + panel, read on load, written on
   mouseup, wrapped in try/catch for private-mode/storage-disabled browsers.
   Key shape: `mdb-panel-width:<page>:<panelId>`.

   Interaction with sidebar-toggle.js's collapse feature: a handle sits right
   after the panel regardless of collapsed state (so it stays visible/
   clickable even at width:0). Mousedown on the handle while the panel is
   collapsed does not start a drag — it clicks the existing
   `.sidebar-toggle-btn` to re-expand instead, since dragging a 0-width panel
   has no sensible meaning. Once expanded, the previously-applied inline
   width (set on load from localStorage) is still in effect, so the panel
   reappears at the user's last chosen size rather than snapping back to the
   CSS default.
   ========================================================================== */
(function () {
    'use strict';

    var STORAGE_PREFIX = 'mdb-panel-width:';

    function keyFor(page, panelId) {
        return STORAGE_PREFIX + page + ':' + panelId;
    }

    function clamp(value, min, max) {
        if (max < min) max = min;
        return Math.min(max, Math.max(min, value));
    }

    function currentPage() {
        var seg = window.location.pathname.split('/').filter(Boolean)[0];
        return seg || 'root';
    }

    function readSaved(page, panelId) {
        try {
            var raw = localStorage.getItem(keyFor(page, panelId));
            if (raw == null) return null;
            var n = parseFloat(raw);
            return isNaN(n) ? null : n;
        } catch (e) { return null; }
    }

    function save(page, panelId, width) {
        try { localStorage.setItem(keyFor(page, panelId), String(Math.round(width))); } catch (e) { /* ignore */ }
    }

    /**
     * Wires a resize handle onto the boundary right after `opts.panelSelector`.
     *
     * opts:
     *   panelSelector  CSS selector for the panel being resized (required)
     *   panelId        stable id used in the localStorage key (required)
     *   page           defaults to the first path segment (jar/parser/plsql)
     *   min            minimum width in px (default 180)
     *   max            function returning the current max width in px
     *                  (default: 50% of viewport width)
     *
     * Returns a handle descriptor, or null if the panel isn't on this page or
     * a handle is already installed for it.
     */
    function install(opts) {
        var panel = document.querySelector(opts.panelSelector);
        if (!panel || !panel.parentNode) return null;
        if (panel.dataset.mdbResizableInstalled) return null;
        panel.dataset.mdbResizableInstalled = '1';

        var page = opts.page || currentPage();
        var panelId = opts.panelId;
        var min = typeof opts.min === 'number' ? opts.min : 180;
        var maxFn = typeof opts.max === 'function' ? opts.max : function () { return window.innerWidth * 0.5; };

        // Tracked independently of getBoundingClientRect(): the panel (jar's
        // .sidebar, parser/plsql's .left-panel) already carries a CSS
        // `transition: width 0.25s ease` for the sidebar-collapse feature,
        // which also fires on *any* inline style.width change, ours included.
        // Reading the rect right after setting style.width — as at
        // mouseup, immediately after the last mousemove — can catch the box
        // mid-transition and report a stale, in-between value instead of the
        // width just applied. The class below suspends that transition only
        // while an actual drag is in progress; lastWidth is the source of
        // truth regardless.
        var lastWidth = null;

        function applyWidth(w) {
            var clamped = clamp(w, min, maxFn());
            panel.style.width = clamped + 'px';
            panel.style.minWidth = clamped + 'px';
            lastWidth = clamped;
            return clamped;
        }

        var saved = readSaved(page, panelId);
        if (saved != null) applyWidth(saved);

        var handle = document.createElement('div');
        handle.className = 'mdb-resize-handle';
        handle.setAttribute('role', 'separator');
        handle.setAttribute('aria-orientation', 'vertical');
        handle.setAttribute('aria-label', 'Resize panel');
        panel.parentNode.insertBefore(handle, panel.nextSibling);

        var dragging = false;
        var startX = 0;
        var startWidth = 0;

        function onMouseMove(e) {
            if (!dragging) return;
            applyWidth(startWidth + (e.clientX - startX));
            e.preventDefault();
        }

        function onMouseUp() {
            if (!dragging) return;
            dragging = false;
            handle.classList.remove('mdb-resize-active');
            panel.classList.remove('mdb-resize-dragging');
            document.body.classList.remove('mdb-resizing');
            save(page, panelId, lastWidth != null ? lastWidth : panel.getBoundingClientRect().width);
            document.removeEventListener('mousemove', onMouseMove);
            document.removeEventListener('mouseup', onMouseUp);
        }

        function onMouseDown(e) {
            if (panel.classList.contains('sidebar-collapsed')) {
                var btn = document.querySelector('.sidebar-toggle-btn');
                if (btn) btn.click();
                return;
            }
            dragging = true;
            startX = e.clientX;
            startWidth = lastWidth != null ? lastWidth : panel.getBoundingClientRect().width;
            handle.classList.add('mdb-resize-active');
            panel.classList.add('mdb-resize-dragging');
            document.body.classList.add('mdb-resizing');
            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp);
            e.preventDefault();
        }

        handle.addEventListener('mousedown', onMouseDown);

        return { panel: panel, handle: handle, applyWidth: applyWidth };
    }

    // Exposed for reuse and for unit tests (see tests/resizable-panel.test.mjs) —
    // matches the window.MdbXxx public-API pattern already used by
    // shared/theme-toggle.js (MdbTheme) and shared/inline-fab.js (MdbInlineFab).
    window.MdbResizablePanel = {
        clamp: clamp,
        keyFor: keyFor,
        install: install
    };

    function autoInit() {
        // jar: <aside class="sidebar"> | <main class="content">
        install({ panelSelector: '.sidebar', panelId: 'sidebar', min: 180 });
        // parser/plsql: .left-panel | .right-panel (inside .analysis-layout)
        install({ panelSelector: '.left-panel', panelId: 'left-panel', min: 180 });
        // jar code structure view: .code-split-left | .code-split-right
        install({
            panelSelector: '.code-split-left',
            panelId: 'code-split-left',
            min: 220,
            max: function () { return window.innerWidth * 0.6; }
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', autoInit);
    } else {
        autoInit();
    }
})();
