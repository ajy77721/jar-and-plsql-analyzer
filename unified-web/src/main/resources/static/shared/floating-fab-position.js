/**
 * TASK-REGISTRY §275 — dynamic vertical placement for the shared top-right FAB cluster
 * (.global-export-fab in shared/mongodb-theme.css, .log-fab in shared/log-fab.css).
 *
 * Previously each page hardcoded a pixel `top` guess (parser/css/ui-polish.css: 54px,
 * jar/style.css: 120px, plsql/css/style.css: 40px) tuned against one procedure's header
 * content. Both the topbar action row and, on parser/plsql, the `.detail-header`
 * badge row (built by js/detailHeader.js) are full-width with zero free space at the
 * right edge, and their height varies with the loaded procedure's name/path/line-range
 * length — so any static guess is wrong for some procedures. This measures the real
 * DOM at runtime instead and drives placement through the `--fab-top-offset` CSS custom
 * property, which the fab rules read via `top: var(--fab-top-offset, <fallback>)`.
 *
 * Hooks: DOMContentLoaded, debounced window resize, and a MutationObserver on whichever
 * header containers exist on the page (covers route/procedure changes on all three apps —
 * parser/jar/plsql — none of which fire a dedicated route-change custom event; the
 * existing pattern in jar/js/app.js is a plain `popstate` listener plus direct DOM
 * mutation from render calls, so observing the DOM directly is the only hook that works
 * uniformly across all three SPAs without inventing a new lifecycle event).
 *
 * Defensive: if no candidate header element is found (e.g. home screen, before an
 * analysis is loaded), the CSS variable is left unset and the shared CSS fallback value
 * applies — this script never throws and never forces a value when it has nothing
 * reliable to measure.
 */
(function () {
    'use strict';

    // Candidate selectors for the "content that sits directly under the fab cluster" on
    // each of the three pages. Order doesn't matter — we take the max `bottom` of whatever
    // is actually present and visible.
    var CANDIDATE_SELECTORS = [
        '.topbar',              // parser, plsql
        '.content-top-bar',     // jar
        '.detail-header',       // parser, plsql (procedure badge row)
        '.analysis-header',     // jar (procedure title/stats row)
        '.sum-topbar'           // jar Summary tab's own inline export bar (js/summary.js)
    ];

    var MIN_OFFSET = 48; // never let the fab cluster sit above this, even on a bare/empty screen

    function isVisible(el) {
        if (!el) return false;
        var rect = el.getBoundingClientRect();
        if (rect.width === 0 && rect.height === 0) return false;
        var style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') return false;
        return true;
    }

    function measureAndApply() {
        try {
            var maxBottom = 0;
            var found = false;
            for (var i = 0; i < CANDIDATE_SELECTORS.length; i++) {
                var els = document.querySelectorAll(CANDIDATE_SELECTORS[i]);
                for (var j = 0; j < els.length; j++) {
                    var el = els[j];
                    if (!isVisible(el)) continue;
                    var rect = el.getBoundingClientRect();
                    if (rect.bottom > maxBottom) maxBottom = rect.bottom;
                    found = true;
                }
            }
            if (!found) return; // leave CSS fallback in effect
            var offset = Math.max(maxBottom + 8, MIN_OFFSET);
            document.documentElement.style.setProperty('--fab-top-offset', offset + 'px');
        } catch (e) {
            // Never let a measurement failure break the page — CSS fallback stays in effect.
        }
    }

    function debounce(fn, wait) {
        var t = null;
        return function () {
            if (t) clearTimeout(t);
            t = setTimeout(fn, wait);
        };
    }

    var debouncedMeasure = debounce(measureAndApply, 120);

    document.addEventListener('DOMContentLoaded', measureAndApply);
    window.addEventListener('resize', debouncedMeasure);

    // Re-measure whenever any candidate header container's content changes (procedure
    // switch, tab switch, async stat re-render, etc). One observer on document.body
    // covers containers that don't exist yet at script-load time (e.g. #detailHeader is
    // `display:none` until an analysis loads, and JAR's #analysis is inserted/shown async).
    if (window.MutationObserver) {
        var observer = new MutationObserver(debouncedMeasure);
        var startObserving = function () {
            if (document.body) {
                observer.observe(document.body, {
                    childList: true,
                    subtree: true,
                    attributes: true,
                    attributeFilter: ['style', 'class']
                });
                measureAndApply();
            }
        };
        if (document.body) startObserving();
        else document.addEventListener('DOMContentLoaded', startObserving);
    }
})();
