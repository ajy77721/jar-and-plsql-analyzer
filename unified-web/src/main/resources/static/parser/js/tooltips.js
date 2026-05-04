/**
 * PA.tooltips — Tooltip module for data-tip attributes.
 * Uses event delegation on document for mouseenter/mouseleave.
 * Reuses a single positioned tooltip div for dynamic content.
 */
window.PA = window.PA || {};

PA.tooltips = {
    _el: null,
    _timer: null,
    _DELAY: 350,

    /** Initialize tooltip system — called on DOMContentLoaded and after dynamic renders */
    init: function() {
        PA.tooltips._attachStaticTips();
    },

    /** Attach data-tip to static elements in index.html */
    _attachStaticTips: function() {
        var tips = [
            // Home screen
            { sel: '#homeObject', tip: 'Enter package or procedure name' },
            { sel: '#btnStartAnalysis', tip: 'Start static analysis' },
            { sel: '#historySearch', tip: 'Search analysis history' },

            // Topbar
            { sel: '#globalSearch', tip: 'Search across procedures' },
            { sel: '.btn-back', tip: 'Return to home screen' },
            { sel: '#claudeVerifyBtn', tip: 'Run Claude verification' },

            // Left panel
            { sel: '#leftFilter', tip: 'Filter by name or schema' },
            { sel: '.ltab[data-tab="procedures"]', tip: 'View call flow tree' },
            { sel: '.ltab[data-tab="tables"]', tip: 'View table list' },

            // Right tabs
            { sel: '.rtab[data-view="callTrace"]', tip: 'Explore call tree' },
            { sel: '.rtab[data-view="trace"]', tip: 'Flat call trace' },
            { sel: '.rtab[data-view="refs"]', tip: 'Procedure references' },
            { sel: '.rtab[data-view="tableOps"]', tip: 'Table operations' },
            { sel: '.rtab[data-view="joins"]', tip: 'JOIN relationships' },
            { sel: '.rtab[data-view="cursors"]', tip: 'Cursor usage' },
            { sel: '.rtab[data-view="sequences"]', tip: 'Sequence operations' },
            { sel: '.rtab[data-view="statements"]', tip: 'Statement analysis' },
            { sel: '.rtab[data-view="exHandlers"]', tip: 'Exception handlers' },
            { sel: '.rtab[data-view="summary"]', tip: 'Analysis summary' },
            { sel: '.rtab[data-view="callGraphViz"]', tip: 'Visual call graph' },
            { sel: '.rtab[data-view="source"]', tip: 'Source code viewer' },

            // Explore toolbar
            { sel: '#exploreSearch', tip: 'Filter procedures by name' },

            // Source modal buttons
            { sel: '#cmBack', tip: 'Navigate back' },
            { sel: '#cmFwd', tip: 'Navigate forward' },
            { sel: '#cmFullscreenBtn', tip: 'Toggle fullscreen' },

            // Export
            { sel: '#expFmtJson', tip: 'Export as JSON' },
            { sel: '#expFmtCsv', tip: 'Export as CSV' }
        ];

        for (var i = 0; i < tips.length; i++) {
            var item = tips[i];
            var els = document.querySelectorAll(item.sel);
            for (var j = 0; j < els.length; j++) {
                if (!els[j].hasAttribute('data-tip')) {
                    els[j].setAttribute('data-tip', item.tip);
                }
            }
        }
    },

    /** Create the reusable tooltip div (lazy) */
    _ensureEl: function() {
        if (PA.tooltips._el) return PA.tooltips._el;
        var el = document.createElement('div');
        el.className = 'pa-tooltip-div';
        el.style.cssText = 'position:fixed;padding:4px 8px;background:var(--sidebar-bg,#1e1e2e);color:#f1f5f9;' +
            'font-size:11px;font-family:var(--font-sans);font-weight:500;line-height:1.3;white-space:nowrap;' +
            'border-radius:var(--radius-sm,4px);box-shadow:0 2px 8px rgba(0,0,0,0.25);pointer-events:none;' +
            'opacity:0;transition:opacity 0.15s ease;z-index:99999;display:none';
        document.body.appendChild(el);
        PA.tooltips._el = el;
        return el;
    },

    /** Show tooltip near target element */
    _show: function(target) {
        var tip = target.getAttribute('data-tip');
        if (!tip) return;

        var el = PA.tooltips._ensureEl();
        el.textContent = tip;
        el.style.display = 'block';
        el.style.opacity = '0';

        // Position above the target
        var rect = target.getBoundingClientRect();
        var tipRect = el.getBoundingClientRect();
        var left = rect.left + (rect.width - tipRect.width) / 2;
        var top = rect.top - tipRect.height - 6;

        // Clamp to viewport
        if (left < 4) left = 4;
        if (left + tipRect.width > window.innerWidth - 4) left = window.innerWidth - tipRect.width - 4;
        if (top < 4) {
            top = rect.bottom + 6; // show below if no room above
        }

        el.style.left = left + 'px';
        el.style.top = top + 'px';
        el.style.opacity = '1';
    },

    /** Hide tooltip */
    _hide: function() {
        if (PA.tooltips._el) {
            PA.tooltips._el.style.opacity = '0';
            PA.tooltips._el.style.display = 'none';
        }
    }
};

// Event delegation for [data-tip] elements
document.addEventListener('mouseenter', function(e) {
    var target = e.target.closest('[data-tip]');
    if (!target) return;
    clearTimeout(PA.tooltips._timer);
    PA.tooltips._timer = setTimeout(function() {
        PA.tooltips._show(target);
    }, PA.tooltips._DELAY);
}, true);

document.addEventListener('mouseleave', function(e) {
    var target = e.target.closest('[data-tip]');
    if (!target) return;
    clearTimeout(PA.tooltips._timer);
    PA.tooltips._hide();
}, true);

document.addEventListener('DOMContentLoaded', function() {
    PA.tooltips.init();
});
