window.PA = window.PA || {};

/**
 * UI-024 (§131) — was a full node-detail modal (`show`/`navigate`/`focusNode`/`_findNodeData`/
 * `_deriveCallersCallees`/`_findGraphNode`/`_buildList`/`_getTypeLabel`), confirmed dead code: its
 * only entry point, `show()`, was never called from outside this file (§91 found this first;
 * re-confirmed here by a full closure trace — `navigate`/`focusNode` are only reachable from HTML
 * `show()` itself emits, and nothing outside this file calls `show`/`navigate`/`focusNode` either).
 * Trimmed to the one real external dependency, `close()` (`callGraphFullscreen.js:40`), removing a
 * `PA.nodeIndex.all()` scan that never actually ran.
 */
PA.callGraphModal = {
    close: function() {
        var overlay = document.getElementById('cgModalOverlay');
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(overlay);
        if (overlay) overlay.style.display = 'none';
    }
};
