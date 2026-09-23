/*
 * In-flight guards: one place for the two patterns this UI kept getting wrong.
 *
 * 1. `UIBusy.begin(el)` — an action button must not stay clickable while its request is in the
 *    air. 77 action buttons existed against 50 `.disabled` toggles (TASKS-UI-AND-PLSQL §B4), so a
 *    double click sent the request twice; and the toggles that did exist were often re-enabled only
 *    on the success path, leaving a button dead forever after one failure. `begin` returns a
 *    release function to be called from `finally`, so success and error settle identically.
 *
 * 2. `UIBusy.seq(owner, key)` — a fetch that writes shared state must ignore its own stale
 *    responses. The established pattern is `PA.sourceModal._reqSeq` / `_isCurrent` in
 *    `parser/js/sourceView.js`; this is that pattern with the trap removed.
 *
 *    THE TRAP: the counter must be initialised. `++undefined` is `NaN`, and `NaN !== NaN`, so an
 *    uninitialised token makes the guard reject *every* response and the panel never updates at
 *    all. That was hit for real. `seq` therefore starts from 0 whenever the current value is not a
 *    number, and hands back a closure rather than a bare number so no caller has to remember how
 *    to compare it.
 *
 * Loaded on every page, before anything else, so both are available to inline `onclick` handlers.
 * No dependencies; plain ES5.
 */
(function () {
    'use strict';

    var BUSY_FLAG = '__uiBusy';

    /**
     * Marks a control busy for the duration of an action.
     *
     * @param el    the control that started the action. May be null/undefined — a handler invoked
     *              from code rather than a click still gets a working release function, so callers
     *              never have to branch.
     * @param label optional text to show while busy; the original text is restored on release.
     * @return a release function, or **null** if `el` is already busy — in which case the caller
     *         must return without starting a second request. Release is idempotent.
     */
    function begin(el, label) {
        if (!el) return function () { /* nothing to re-enable */ };
        if (el[BUSY_FLAG]) return null;

        el[BUSY_FLAG] = true;
        var wasDisabled = !!el.disabled;
        var hadAriaBusy = el.getAttribute ? el.getAttribute('aria-busy') : null;
        var text = null;

        el.disabled = true;
        if (el.classList) el.classList.add('is-busy');
        if (el.setAttribute) el.setAttribute('aria-busy', 'true');
        if (label != null && 'textContent' in el) {
            text = el.textContent;
            el.textContent = label;
        }

        var released = false;
        return function () {
            if (released) return;
            released = true;
            el[BUSY_FLAG] = false;
            el.disabled = wasDisabled;
            if (el.classList) el.classList.remove('is-busy');
            if (el.setAttribute) {
                if (hadAriaBusy == null) el.removeAttribute('aria-busy');
                else el.setAttribute('aria-busy', hadAriaBusy);
            }
            // Only restore text if the button is still the one we changed; a re-render replaces the
            // node, and writing to a detached element is harmless but pointless.
            if (text != null && 'textContent' in el) el.textContent = text;
        };
    }

    /**
     * Wraps a whole action: disables `el`, runs `fn`, and releases however `fn` settles.
     *
     * Returns whatever `fn` returns (or undefined when the control was already busy), so an async
     * caller can still await the outcome.
     */
    function run(el, fn, label) {
        var done = begin(el, label);
        if (!done) return undefined;          // already in flight — drop the duplicate click
        var out;
        try {
            out = fn();
        } catch (e) {
            done();
            throw e;
        }
        if (out && typeof out.then === 'function') {
            return out.then(
                function (v) { done(); return v; },
                function (e) { done(); throw e; }
            );
        }
        done();
        return out;
    }

    /**
     * Issues a stale-response token for `owner[key]` and returns `isCurrent()`.
     *
     * Usage:
     * <pre>
     *   var isCurrent = UIBusy.seq(PA.somePanel, '_reqSeq');
     *   var data = await fetchIt();
     *   if (!isCurrent()) return;            // a newer request superseded this one
     *   panel.innerHTML = render(data);
     * </pre>
     *
     * The counter is read defensively so a fresh owner (`undefined`) starts at 1 rather than `NaN`.
     */
    function seq(owner, key) {
        var k = key || '_reqSeq';
        var current = owner[k];
        var mine = (typeof current === 'number' && isFinite(current) ? current : 0) + 1;
        owner[k] = mine;
        return function isCurrent() { return owner[k] === mine; };
    }

    window.UIBusy = { begin: begin, run: run, seq: seq };
})();

/* ─────────────────────────────────────────────────────────────────────────────
 * Search-query normalisation
 *
 * Every search box in this app compares a typed query against page text. A stray
 * leading or trailing space then produces "no results" for a term that is plainly
 * present, and nothing on screen explains why — the user sees an empty list and
 * concludes the data is missing.
 *
 * Two rules, and they are deliberately different:
 *
 *   - the NEEDLE (what the user typed) is trimmed and case-folded;
 *   - the HAYSTACK (page text, dataset attributes) is only case-folded.
 *
 * Trimming the haystack would be wrong: interior spacing is part of the content
 * being matched, and `"a  b".trim()` does not remove it anyway, so it would cost
 * work and buy nothing.
 *
 * `SearchQuery.flag` exists for the case where trimming alone is not honest —
 * a query of only whitespace matches everything once trimmed, which is right, but
 * the user should be able to see that their input was not what they thought.
 * ──────────────────────────────────────────────────────────────────────────── */
window.SearchQuery = window.SearchQuery || {

    /**
     * Normalises what the user typed: trims, then lower-cases.
     * @param {string|null|undefined} raw value straight off the input
     * @returns {string} always a string, never null
     */
    norm: function (raw) {
        return String(raw == null ? '' : raw).trim().toLowerCase();
    },

    /** As {@link norm} but folding upward — the PL/SQL pages compare against upper-cased identifiers. */
    normUpper: function (raw) {
        return String(raw == null ? '' : raw).trim().toUpperCase();
    },

    /** Trim only, preserving case — for case-sensitive searches such as code search. */
    trim: function (raw) {
        return String(raw == null ? '' : raw).trim();
    },

    /** True when the raw input carried leading or trailing whitespace that was ignored. */
    hadPadding: function (raw) {
        var s = String(raw == null ? '' : raw);
        return s.length > 0 && s !== s.trim();
    },

    /**
     * Marks an input whose value had surrounding whitespace, so a user who pasted
     * `" ORDERS "` can see why it behaved as `ORDERS` rather than wondering.
     * Purely advisory: the search has already been normalised by the time this runs.
     */
    markPadding: function (inputEl, raw) {
        if (!inputEl) return;
        var padded = SearchQuery.hadPadding(raw !== undefined ? raw : inputEl.value);
        inputEl.classList.toggle('search-had-padding', padded);
        if (padded) {
            inputEl.title = 'Leading/trailing spaces ignored';
        } else if (inputEl.title === 'Leading/trailing spaces ignored') {
            inputEl.removeAttribute('title');
        }
    },

    /**
     * Clears the search/filter boxes inside `root`, so a panel cannot reopen showing a query it is
     * no longer applying.
     *
     * <h3>The defect this fixes</h3>
     * Closing a modal drops the table's filter state but leaves the DOM alone. Reopening it renders
     * an unfiltered list *with the old text still sitting in the search box* — so the screen shows
     * a filter that is not in effect. A reader then trusts a full list as if it were filtered, or a
     * filtered-looking box as if it explained the rows. Either way the box and the rows disagree,
     * and nothing says which one is lying.
     *
     * <h3>Why this is not "clear every input"</h3>
     * It deliberately matches only inputs that play a <b>search</b> role: `type="search"`, or a
     * class/placeholder that says search or filter. The chat panels on both pages hold a
     * half-written <em>message</em> in an ordinary text input, and wiping that on a close would
     * destroy the user's own words — a far worse bug than the one being fixed. `SKIP` names those
     * explicitly as well, so the two protections are independent.
     *
     * @param root element to clear within; defaults to the document.
     * @return the number of inputs cleared, so a caller or test can tell it did something.
     */
    resetWithin: function (root) {
        var scope = root || document;
        if (!scope.querySelectorAll) return 0;
        var SKIP = /chat|message|prompt|comment|note/i;
        var LOOKS_LIKE_SEARCH = /search|filter|-cf-|\bq\b/i;
        var cleared = 0;
        var nodes = scope.querySelectorAll('input');
        for (var i = 0; i < nodes.length; i++) {
            var el = nodes[i];
            var type = (el.getAttribute('type') || 'text').toLowerCase();
            if (type !== 'text' && type !== 'search') continue;
            var ident = (el.className || '') + ' ' + (el.id || '') + ' '
                      + (el.getAttribute('placeholder') || '');
            if (SKIP.test(ident)) continue;
            if (type !== 'search' && !LOOKS_LIKE_SEARCH.test(ident)) continue;
            if (!el.value) continue;
            el.value = '';
            el.classList.remove('search-had-padding');
            if (el.title === 'Leading/trailing spaces ignored') el.removeAttribute('title');
            cleared++;
        }
        return cleared;
    }
};

/* Global whitespace indicator.
 *
 * Trimming alone is silent: the search now works, but a user who pasted `" ORDERS "` from a
 * spreadsheet still has no idea their clipboard carried spaces, and will paste it again into the
 * next box. One delegated listener marks every text input on the page, so this holds for search
 * boxes added later without anyone having to remember to wire it up.
 *
 * Advisory only — nothing here changes what a search matches. */
(function () {
    var STYLE_ID = 'search-padding-style';
    function installStyle() {
        if (document.getElementById(STYLE_ID)) return;
        var st = document.createElement('style');
        st.id = STYLE_ID;
        st.textContent =
            '.search-had-padding{' +
            'outline:1px dashed var(--warn, #d08700);' +
            'outline-offset:1px;' +
            'background-image:linear-gradient(90deg,rgba(208,135,0,.18) 0 .55em,transparent .55em),' +
            'linear-gradient(270deg,rgba(208,135,0,.18) 0 .55em,transparent .55em);' +
            'background-repeat:no-repeat;}';
        document.head.appendChild(st);
    }

    function onInput(e) {
        var t = e.target;
        if (!t || t.tagName !== 'INPUT') return;
        var type = (t.getAttribute('type') || 'text').toLowerCase();
        if (type !== 'text' && type !== 'search') return;
        SearchQuery.markPadding(t, t.value);
    }

    function start() {
        installStyle();
        // Capture phase, so the mark lands even where a handler stops propagation.
        document.addEventListener('input', onInput, true);
        document.addEventListener('change', onInput, true);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', start);
    } else {
        start();
    }
})();
