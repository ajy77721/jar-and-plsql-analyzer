window.PA = window.PA || {};

/**
 * PA.definitionModal — one modal that shows a DB-sourced DEFINITION (a view's SQL, a trigger's
 * body + WHEN clause) without opening the row's whole detail panel.
 *
 * <h2>Why this exists</h2>
 *
 * The Views & MVs and Triggers screens each bury their definition inside the row's expand panel —
 * the user reading the LIST wants the SQL on one click, the way the Tables tab's own "def" link
 * already behaves. Both screens now carry a "def" link on every row and both open THIS modal;
 * one modal, not two copies that drift (the reason AccessDetails/usage-table exist as shared
 * modules too).
 *
 * <h2>Contract</h2>
 *
 * `open({ title, subtitle, sections })` — `sections` is an ordered list of
 * `{ title, pre }` (escaped SQL text shown in a `<pre>`) and/or `{ title, html }` (caller-built).
 * The markup is built dynamically and appended to <body> with the same `.src-modal-overlay` /
 * `.src-modal` classes every other modal here uses, so theming and the modal z-index stack
 * (PA.modalStack) come for free. `esc()` and `close()` are the only API.
 *
 * <h2>Escaping</h2>
 *
 * Callers pass definition SQL — database-sourced DATA. Anything handed as `pre` is escaped HERE
 * before it reaches the DOM (same rule as shared/escape.js); `html` sections are caller-built and
 * expected to escape their own data.
 */
PA.definitionModal = (function () {
    function esc(v) {
        return (window.PA && PA.esc) ? PA.esc(v) : String(v == null ? '' : v);
    }

    function close(event) {
        if (event && typeof event.stopPropagation === 'function') {
            // Overlay click closes; clicks INSIDE the card bubble up with the card's own
            // stopPropagation already applied, so this only fires for the backdrop.
            if (event.target && event.target.id !== 'definitionModalOverlay') return;
        }
        var el = document.getElementById('definitionModalOverlay');
        if (el) el.remove();
    }

    function open(opts) {
        opts = opts || {};
        close();
        var z = (PA.modalStack && PA.modalStack.nextZ) ? PA.modalStack.nextZ() : 1100;

        var overlay = document.createElement('div');
        overlay.className = 'src-modal-overlay';
        overlay.id = 'definitionModalOverlay';
        overlay.style.display = 'flex';
        overlay.style.zIndex = String(z);
        overlay.onclick = function (event) { close(event); };

        var h = '<div class="src-modal" onclick="event.stopPropagation()" '
             +  'style="width:min(900px,90vw);max-height:85vh;display:flex;flex-direction:column">';
        h += '<div class="src-modal-header">';
        h += '<span class="src-modal-title">' + esc(opts.title || 'Definition') + '</span>';
        h += '<div class="src-modal-actions">'
           + '<button class="btn btn-sm" onclick="PA.definitionModal.close()" title="Close (Esc)">&times;</button>'
           + '</div>';
        h += '</div>';
        h += '<div class="def-modal-body" style="padding:10px 14px;overflow-y:auto">';
        if (opts.subtitle) h += '<div class="est-meta" style="margin-bottom:6px">' + opts.subtitle + '</div>';
        var sections = opts.sections || [];
        for (var i = 0; i < sections.length; i++) {
            var s = sections[i];
            h += '<div class="est-section">';
            if (s.title) h += '<div class="est-section-title">' + esc(s.title) + '</div>';
            if (s.pre != null && s.pre !== '') h += '<pre class="est-sql' + (s.preClass ? ' ' + s.preClass : '') + '"'
                + (s.preId ? ' id="' + esc(s.preId) + '"' : '') + '>' + esc(s.pre) + '</pre>';
            else if (s.html != null) h += s.html;
            h += '</div>';
        }
        h += '</div></div>';

        overlay.innerHTML = h;
        document.body.appendChild(overlay);

        // Esc closes, same as the other modals on this page.
        this._escHandler = function (e) {
            if (e.key === 'Escape') { PA.definitionModal.close(); document.removeEventListener('keydown', PA.definitionModal._escHandler); }
        };
        document.addEventListener('keydown', this._escHandler);
        return overlay;
    }

    /**
     * The pure half of {@link highlight}: wraps every case-insensitive occurrence of `term` in
     * `<mark>`, re-escaping everything. Exposed for the test suite (the DOM stub cannot parse
     * rendered HTML back into elements, so the DOM half is thin glue over this).
     *
     * @returns {string} HTML — or `''` when the term does not occur (the caller then leaves the
     *                   pre alone rather than rebuilding it for nothing).
     */
    function highlightText(text, term) {
        text = String(text == null ? '' : text);
        term = String(term == null ? '' : term);
        if (!term) return '';
        var lower = text.toLowerCase(), want = term.toLowerCase();
        var out = '', idx = 0, found = false;
        while (idx <= text.length) {
            var at = lower.indexOf(want, idx);
            if (at === -1) { out += esc(text.slice(idx)); break; }
            found = true;
            out += esc(text.slice(idx, at)) + '<mark>' + esc(text.slice(at, at + want.length)) + '</mark>';
            idx = at + want.length;
        }
        return found ? out : '';
    }

    /**
     * Highlights every case-insensitive occurrence of `term` inside the `<pre>` `preId` (or the
     * first `.est-sql` under `scopeEl` when no id is given) and scrolls the first hit into view.
     *
     * The pre's TEXT is the source of truth (escaped rendering never changes textContent), so the
     * rebuild is: read textContent → re-escape → wrap matches in <mark>. A `<mark>` needs no CSS
     * of its own — the browser's default yellow-on-transparent reads correctly in both themes.
     */
    function highlight(preId, term, scopeEl) {
        var pre = (preId && document.getElementById(preId))
            || ((scopeEl || document).querySelector('.est-sql'));
        if (!pre || !term) return;
        var html = highlightText(pre.textContent, term);
        if (!html) return;
        pre.innerHTML = html;
        var mark = pre.querySelector('mark');
        if (mark && mark.scrollIntoView) mark.scrollIntoView({ block: 'center' });
    }

    /**
     * Marks line `lineNo` (1-based) of the identified `<pre>` and scrolls it into view — the
     * click target for a "Line" value that refers INTO the definition itself (a join's line in a
     * view's SQL, a call's line in a trigger body). Rebuilds exactly like {@link highlight}:
     * textContent is the truth, every line re-escaped, only the target line wrapped in <mark>.
     */
    function highlightLine(preId, lineNo) {
        var pre = preId && document.getElementById(preId);
        if (!pre) return;
        var lines = (pre.textContent || '').split('\n');
        var n = parseInt(lineNo, 10);
        if (!n || n < 1 || n > lines.length) return;
        var out = [];
        for (var i = 0; i < lines.length; i++) {
            out.push(i === n - 1 ? '<mark>' + esc(lines[i]) + '</mark>' : esc(lines[i]));
        }
        pre.innerHTML = out.join('\n');
        var mark = pre.querySelector('mark');
        if (mark && mark.scrollIntoView) mark.scrollIntoView({ block: 'center' });
    }

    return { open: open, close: close, highlight: highlight, highlightText: highlightText,
             highlightLine: highlightLine };
})();
