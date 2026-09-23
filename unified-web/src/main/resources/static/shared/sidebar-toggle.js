/* ==========================================================================
   Sidebar collapse/expand toggle — TASK-REGISTRY §251.

   §250 found no sidebar-wide collapse control existed anywhere in this app
   (only per-node tree-row collapse inside codeTree.js, a different feature)
   and left three files' comments ("sidebar-toggle, help, theme, chatbot")
   anticipating this control as the first slot in the shared bottom-left
   action cluster. This file builds that control for real.

   Sidebar element differs by page:
     - jar:            <aside class="sidebar">
     - parser/plsql:    <div class="left-panel">
   Both sit as the first flex child next to a flex:1 main-content sibling
   (.content on jar, .right-panel on parser/plsql), so collapsing the
   sidebar's width to 0 is enough for the sibling to fill the freed space —
   no separate "expand main content" class/logic needed.

   State persists across reloads via localStorage, reusing the exact pattern
   theme-toggle.js already uses in this app (try/catch around
   localStorage get/set, one shared key). Key is per-page ('jar'/'parser'/
   'plsql', taken from the first path segment) since collapsing the sidebar
   on one page has no bearing on the other two.
   ========================================================================== */
(function () {
    'use strict';

    var STORAGE_PREFIX = 'mdb-sidebar-collapsed:';

    function pageKey() {
        var seg = window.location.pathname.split('/').filter(Boolean)[0];
        return STORAGE_PREFIX + (seg || 'root');
    }

    function findSidebar() {
        return document.querySelector('.sidebar, .left-panel');
    }

    function readSaved() {
        try { return localStorage.getItem(pageKey()) === '1'; } catch (e) { return false; }
    }

    function save(collapsed) {
        try { localStorage.setItem(pageKey(), collapsed ? '1' : '0'); } catch (e) { /* ignore */ }
    }

    var ICON_COLLAPSE = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><line x1="10" y1="4" x2="10" y2="20"/><path d="M7 9l-2 3 2 3"/></svg>';
    var ICON_EXPAND = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><line x1="10" y1="4" x2="10" y2="20"/><path d="M6 9l2 3-2 3"/></svg>';

    function label(btn, collapsed) {
        btn.innerHTML = collapsed ? ICON_EXPAND : ICON_COLLAPSE;
        btn.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
        btn.setAttribute('title', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
        btn.setAttribute('aria-pressed', String(collapsed));
    }

    function apply(sidebar, collapsed) {
        sidebar.classList.toggle('sidebar-collapsed', collapsed);
    }

    function inject() {
        var sidebar = findSidebar();
        if (!sidebar) return; // page has no sidebar-shaped element (e.g. a bare list page)
        if (document.querySelector('.sidebar-toggle-btn')) return;

        var collapsed = readSaved();
        apply(sidebar, collapsed);

        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'sidebar-toggle-btn help-btn';
        label(btn, collapsed);
        btn.addEventListener('click', function () {
            collapsed = !sidebar.classList.contains('sidebar-collapsed');
            apply(sidebar, collapsed);
            save(collapsed);
            label(btn, collapsed);
        });
        document.body.appendChild(btn);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', inject);
    } else {
        inject();
    }
})();
