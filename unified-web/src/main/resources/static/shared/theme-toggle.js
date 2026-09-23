/* ==========================================================================
   Theme switcher — light/dark for the MongoDB theme.

   Sets data-theme on <html>. Persists the user's choice in localStorage under
   a single key shared by every page, so switching in the parser UI carries
   over to the jar/plsql/war pages.

   Load in <head> WITHOUT defer so the attribute is set before first paint —
   otherwise dark users get a white flash. The toggle button itself is injected
   on DOMContentLoaded.
   ========================================================================== */
(function () {
    'use strict';

    var STORAGE_KEY = 'mdb-theme';
    var root = document.documentElement;

    function preferred() {
        try {
            var saved = localStorage.getItem(STORAGE_KEY);
            if (saved === 'light' || saved === 'dark') return saved;
        } catch (e) { /* private mode / storage disabled */ }
        return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
            ? 'dark' : 'light';
    }

    function apply(theme) {
        root.setAttribute('data-theme', theme);
        root.style.colorScheme = theme;
        try { localStorage.setItem(STORAGE_KEY, theme); } catch (e) { /* ignore */ }
        document.dispatchEvent(new CustomEvent('themechange', { detail: { theme: theme } }));
    }

    // Run immediately — before paint.
    apply(preferred());

    window.MdbTheme = {
        get: function () { return root.getAttribute('data-theme') || 'light'; },
        set: apply,
        toggle: function () { apply(this.get() === 'dark' ? 'light' : 'dark'); }
    };

    // Follow the OS only while the user has not made an explicit choice.
    if (window.matchMedia) {
        var mq = window.matchMedia('(prefers-color-scheme: dark)');
        var onChange = function (e) {
            var saved = null;
            try { saved = localStorage.getItem(STORAGE_KEY); } catch (err) { /* ignore */ }
            if (!saved) apply(e.matches ? 'dark' : 'light');
        };
        if (mq.addEventListener) mq.addEventListener('change', onChange);
        else if (mq.addListener) mq.addListener(onChange);
    }

    var SUN = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>';
    var MOON = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>';

    function label(btn) {
        var dark = window.MdbTheme.get() === 'dark';
        btn.innerHTML = dark ? SUN : MOON;
        btn.setAttribute('aria-label', dark ? 'Switch to light theme' : 'Switch to dark theme');
        btn.setAttribute('title', dark ? 'Light theme' : 'Dark theme');
        btn.setAttribute('aria-pressed', String(dark));
    }

    function inject() {
        if (document.querySelector('.mdb-theme-toggle')) return;

        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'mdb-theme-toggle';
        label(btn);
        btn.addEventListener('click', function () {
            window.MdbTheme.toggle();
            label(btn);
        });
        document.addEventListener('themechange', function () { label(btn); });

        // Always float in the shared bottom-left action cluster (with help/chat/sidebar
        // toggle) instead of docking into a page's top bar — docking into .topbar-actions
        // used to put this control top-right on parser/plsql while jar/war floated it
        // bottom-right, an inconsistent layout across pages. One fixed position now applies
        // everywhere.
        btn.classList.add('mdb-theme-toggle--floating');
        document.body.appendChild(btn);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', inject);
    } else {
        inject();
    }
})();
