/**
 * PA.sourceView — Source code viewer with PL/SQL syntax highlighting.
 * Adapted from plsql-jar-analyzer for plsql-parser.
 * Supports: full file view, focused proc view, clickable line numbers, copy, chunked rendering.
 */
window.PA = window.PA || {};

const PLSQL_KEYWORDS = new Set([
    'CREATE', 'OR', 'REPLACE', 'PACKAGE', 'BODY', 'PROCEDURE', 'FUNCTION',
    'IS', 'AS', 'BEGIN', 'END', 'DECLARE', 'EXCEPTION', 'WHEN', 'THEN',
    'ELSE', 'ELSIF', 'IF', 'LOOP', 'WHILE', 'FOR', 'IN', 'OUT', 'NOCOPY',
    'RETURN', 'RETURNING', 'INTO', 'BULK', 'COLLECT', 'FORALL',
    'CURSOR', 'OPEN', 'FETCH', 'CLOSE', 'EXIT',
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'FROM', 'WHERE',
    'AND', 'NOT', 'NULL', 'SET', 'VALUES', 'JOIN', 'LEFT',
    'RIGHT', 'INNER', 'OUTER', 'ON', 'GROUP', 'BY', 'ORDER', 'HAVING',
    'UNION', 'ALL', 'EXISTS', 'BETWEEN', 'LIKE', 'DISTINCT',
    'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'PRAGMA', 'AUTONOMOUS_TRANSACTION',
    'RAISE', 'RAISE_APPLICATION_ERROR', 'EXECUTE', 'IMMEDIATE',
    'TYPE', 'SUBTYPE', 'RECORD', 'TABLE', 'INDEX', 'OF', 'REF',
    'VARCHAR2', 'NUMBER', 'INTEGER', 'BOOLEAN', 'DATE', 'TIMESTAMP',
    'CLOB', 'BLOB', 'ROWTYPE', 'NOTFOUND', 'FOUND', 'ROWCOUNT',
    'NO_DATA_FOUND', 'TOO_MANY_ROWS', 'OTHERS', 'SQLERRM', 'SQLCODE',
    'TRUE', 'FALSE', 'DEFAULT', 'CONSTANT', 'TRIGGER', 'BEFORE', 'AFTER',
    'INSTEAD', 'EACH', 'ROW', 'STATEMENT', 'USING', 'CONTINUE', 'GOTO',
    'PIPE', 'CASE', 'LIMIT', 'SAVE', 'EXCEPTIONS', 'EDITIONABLE', 'NONEDITIONABLE',
    // SQL Server (T-SQL), MySQL and DB2 keywords
    'TOP', 'OFFSET', 'IDENTITY', 'ISNULL', 'IIF', 'TRY', 'CATCH', 'THROW',
    'NVARCHAR', 'VARCHAR', 'NCHAR', 'BIT', 'DATETIME', 'DATETIME2', 'BIGINT',
    'TINYINT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'GETDATE', 'ISNUMERIC',
    'DELIMITER', 'ENGINE', 'AUTO_INCREMENT', 'UNSIGNED', 'FETCH', 'FIRST', 'ROWS', 'ONLY'
]);

PA.sourceModal = {
    _content: '',
    _history: [],
    _historyFwd: [],
    _searchMatches: [],
    _searchIdx: -1,

    /**
     * Blank the modal synchronously and claim a request slot.
     *
     * Both open paths await a fetch before touching the DOM, so without this the modal keeps
     * displaying the PREVIOUS file (and the previous sidebar) until the new response lands —
     * which reads as "it shows old data for a moment, then refreshes".
     *
     * The returned token guards against out-of-order responses: open A, then quickly open B,
     * and A's slower response would otherwise land last and overwrite B. Callers must re-check
     * `_isCurrent(token)` after every await before writing to the DOM.
     */
    _beginLoad(titleText) {
        var token = ++PA.sourceModal._reqSeq;
        var title = document.getElementById('srcModalTitle');
        var body = document.getElementById('srcModalBody');
        if (title) title.textContent = titleText || 'Loading\u2026';
        if (body) body.innerHTML = '<div class="empty-msg">Loading\u2026</div>';
        PA.sourceModal._content = '';
        PA.sourceModal.closeSearch();
        PA.sourceModal._clearSidebar();
        return token;
    },

    _isCurrent(token) { return token === PA.sourceModal._reqSeq; },

    /** Empties every sidebar panel so no panel can show the previous procedure's data. */
    _clearSidebar() {
        var ids = ['cmParams', 'cmVars', 'cmTables', 'cmCursors', 'cmCalls', 'cmCalledBy',
                   'cmPkgVars', 'cmTxn', 'cmClaude'];
        for (var i = 0; i < ids.length; i++) {
            var el = document.getElementById(ids[i]);
            if (el) el.innerHTML = '';
        }
    },

    _reqSeq: 0,

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(document.getElementById('srcModal'));
        document.getElementById('srcModal').style.display = 'none';
        // Reset the window-state classes/button-enablement so the NEXT open always
        // starts from a clean "normal" state, rather than carrying over whatever
        // fullscreen/minimized state this session left the panel in.
        var closedPanel = document.querySelector('#srcModal .src-modal');
        if (closedPanel) {
            closedPanel.classList.remove('cm-fullscreen', 'cm-minimized');
            PA.sourceModal._updateWindowCtrlButtons(closedPanel);
        }
        PA.sourceModal.closeSearch();
        // Invalidate any in-flight load and drop the content, so reopening cannot flash the
        // previously viewed file before the new one arrives.
        PA.sourceModal._reqSeq++;
        var body = document.getElementById('srcModalBody');
        if (body) body.innerHTML = '';
        PA.sourceModal._content = '';
        PA.sourceModal._clearSidebar();
    },
    copy() {
        if (PA.sourceModal._content) {
            navigator.clipboard.writeText(PA.sourceModal._content).then(function() { PA.toast('Source copied', 'success'); });
        }
    },

    back() {
        if (PA.sourceModal._history.length <= 1) return;
        var current = PA.sourceModal._history.pop();
        PA.sourceModal._historyFwd.push(current);
        var prev = PA.sourceModal._history[PA.sourceModal._history.length - 1];
        PA.sourceModal._history.pop();
        PA.sourceView.openModal(prev.procId);
    },
    forward() {
        if (!PA.sourceModal._historyFwd.length) return;
        var next = PA.sourceModal._historyFwd.pop();
        PA.sourceView.openModal(next.procId);
    },
    _updateNav() {
        var backBtn = document.getElementById('cmBack');
        var fwdBtn = document.getElementById('cmFwd');
        if (backBtn) backBtn.disabled = PA.sourceModal._history.length <= 1;
        if (fwdBtn) fwdBtn.disabled = PA.sourceModal._historyFwd.length === 0;
    },

    _renderBreadcrumb() {
        var el = document.getElementById('cmBreadcrumb');
        if (!el) return;
        var history = PA.sourceModal._history;
        if (history.length <= 1) { el.style.display = 'none'; return; }
        el.style.display = '';
        var h = '';
        for (var i = 0; i < history.length; i++) {
            if (i > 0) h += '<span class="cm-bc-sep">&#9656;</span>';
            var entry = history[i];
            var node = PA.sourceView._findNodeInfo(entry.procId);
            var label = (node && node.name) || entry.procId || '?';
            var isLast = (i === history.length - 1);
            if (isLast) {
                h += '<span class="cm-bc-item active" data-tip="Current location">' + PA.esc(label) + '</span>';
            } else {
                h += '<span class="cm-bc-item" data-tip="Jump to this point" onclick="PA.sourceModal._jumpBreadcrumb(' + i + ')">' + PA.esc(label) + '</span>';
            }
        }
        el.innerHTML = h;
    },

    _jumpBreadcrumb(idx) {
        var history = PA.sourceModal._history;
        if (idx < 0 || idx >= history.length - 1) return;
        // Pop everything after idx, push them to forward stack
        var target = history[idx];
        var removed = history.splice(idx + 1);
        // The last removed item is the current; push all to forward
        for (var i = removed.length - 1; i >= 0; i--) {
            PA.sourceModal._historyFwd.push(removed[i]);
        }
        // Pop the target too (openModal will re-push it)
        history.pop();
        PA.sourceView.openModal(target.procId);
    },

    toggleFullscreen() {
        // Scope to the source modal: a bare '.src-modal' selector matched the FIRST such
        // panel in the DOM (the hidden table-detail modal), so fullscreen did nothing.
        var panel = document.querySelector('#srcModal .src-modal');
        if (!panel) return;
        // Mutual exclusion: while minimized, the Fullscreen button is disabled (see
        // _updateWindowCtrlButtons) so a real click can't reach here — this guard is
        // defense-in-depth against programmatic/keyboard invocation, so minimized+
        // fullscreen can never be combined into the broken "still 340x56 but tagged
        // fullscreen" state a prior bug report described.
        if (panel.classList.contains('cm-minimized')) return;
        panel.classList.toggle('cm-fullscreen');
        PA.sourceModal._updateWindowCtrlButtons(panel);
        // The code body is virtualized: it only renders enough lines to fill the
        // scroll container's clientHeight, computed at open time. Growing to fullscreen
        // enlarges the container but leaves the rendered window sized to the OLD height,
        // so the code area looks unchanged. Let the new layout settle, then fire a
        // scroll event so the virtualizer recomputes against the new height and fills it.
        var body = document.getElementById('srcModalBody');
        if (body) {
            requestAnimationFrame(function() {
                requestAnimationFrame(function() {
                    body.dispatchEvent(new Event('scroll'));
                });
            });
        }
    },

    // Minimize the modal to a small title bar (bottom-right); click again to restore.
    toggleMinimize() {
        var panel = document.querySelector('#srcModal .src-modal');
        if (!panel) return;
        // Mutual exclusion: while fullscreen, the Minimize button is disabled (see
        // _updateWindowCtrlButtons); this guard covers programmatic calls the same way
        // toggleFullscreen's guard does above.
        if (panel.classList.contains('cm-fullscreen')) return;
        panel.classList.toggle('cm-minimized');
        PA.sourceModal._updateWindowCtrlButtons(panel);
    },

    // Keep the Minimize/Fullscreen buttons correctly reflecting the panel's current
    // window state: whichever of the two states you're currently IN has its OWN
    // toggle button left enabled (so clicking it again restores you to normal), but
    // the OTHER control is disabled — you cannot jump straight from minimized to
    // fullscreen (or vice versa) without going back through normal/"medium" first.
    // Reuses the same disabled-button look already used for cmBack/cmFwd
    // (`.cm-nav-btns .btn:disabled`) and pagination's own prev/next buttons elsewhere
    // in this codebase, rather than inventing a new disabled style.
    _updateWindowCtrlButtons(panel) {
        var minBtn = document.getElementById('cmMinimizeBtn');
        var fsBtn = document.getElementById('cmFullscreenBtn');
        var isFullscreen = panel.classList.contains('cm-fullscreen');
        var isMinimized = panel.classList.contains('cm-minimized');
        if (minBtn) minBtn.disabled = isFullscreen;
        if (fsBtn) fsBtn.disabled = isMinimized;
    },

    toggleSearch() {
        var bar = document.getElementById('cmSearchBar');
        if (bar.style.display === 'none') {
            bar.style.display = '';
            var input = document.getElementById('cmSearchInput');
            input.focus();
            var sel = window.getSelection ? window.getSelection().toString().trim() : '';
            if (sel && sel.length >= 2 && sel.length <= 150) {
                input.value = sel;
                input.select();
                PA.sourceModal.search(sel);
            }
        } else {
            PA.sourceModal.closeSearch();
        }
    },
    closeSearch() {
        var bar = document.getElementById('cmSearchBar');
        if (bar) bar.style.display = 'none';
        PA.sourceModal._clearSearchHighlights();
        PA.sourceModal._searchMatches = [];
        PA.sourceModal._searchIdx = -1;
        PA.sourceModal._searchTotalCount = 0;
        var input = document.getElementById('cmSearchInput');
        if (input) input.value = '';
        var count = document.getElementById('cmSearchCount');
        if (count) count.textContent = '0';
    },
    search(query) {
        PA.sourceModal._clearSearchHighlights();
        PA.sourceModal._searchMatches = [];
        PA.sourceModal._searchIdx = -1;
        var count = document.getElementById('cmSearchCount');
        if (!query || query.length < 2) { if (count) count.textContent = '0'; return; }
        var q = SearchQuery.norm(query);

        // Count total hits across all content (including off-screen chunks)
        var totalCount = 0;
        if (PA.sourceModal._content) {
            var raw = PA.sourceModal._content.toLowerCase();
            var pos = 0;
            while ((pos = raw.indexOf(q, pos)) !== -1) { totalCount++; pos++; }
        }

        // Highlight only currently-rendered DOM lines
        var lines = document.querySelectorAll('#srcModalBody .src-line');
        var matches = [];
        lines.forEach(function(line) {
            var codeEl = line.querySelector('.src-code');
            if (!codeEl) return;
            if (codeEl.textContent.toLowerCase().indexOf(q) !== -1) {
                matches.push(line);
                line.classList.add('cm-search-match');
            }
        });
        PA.sourceModal._searchMatches = matches;
        PA.sourceModal._searchTotalCount = totalCount;
        if (count) count.textContent = totalCount > 0 ? totalCount : '0';
        if (matches.length > 0) PA.sourceModal._activateMatch(0);
    },
    searchNext() {
        if (!PA.sourceModal._searchMatches.length) return;
        PA.sourceModal._activateMatch((PA.sourceModal._searchIdx + 1) % PA.sourceModal._searchMatches.length);
    },
    searchPrev() {
        if (!PA.sourceModal._searchMatches.length) return;
        var total = PA.sourceModal._searchMatches.length;
        PA.sourceModal._activateMatch((PA.sourceModal._searchIdx - 1 + total) % total);
    },
    searchKeydown(e) {
        if (e.key === 'Enter') { e.shiftKey ? PA.sourceModal.searchPrev() : PA.sourceModal.searchNext(); e.preventDefault(); }
        if (e.key === 'Escape') { PA.sourceModal.closeSearch(); e.preventDefault(); }
    },
    _activateMatch(idx) {
        PA.sourceModal._searchMatches.forEach(function(el) { el.classList.remove('cm-search-active'); });
        PA.sourceModal._searchIdx = idx;
        var el = PA.sourceModal._searchMatches[idx];
        if (el) {
            el.classList.add('cm-search-active');
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
        var count = document.getElementById('cmSearchCount');
        var total = PA.sourceModal._searchTotalCount || PA.sourceModal._searchMatches.length;
        if (count) count.textContent = (idx + 1) + '/' + total;
    },
    _clearSearchHighlights() {
        document.querySelectorAll('#srcModalBody .cm-search-match, #srcModalBody .cm-search-active').forEach(function(el) {
            el.classList.remove('cm-search-match', 'cm-search-active');
        });
    },

    // UI-024 (§163) — `ensureLoaded()` awaited once here, at this function's own top, rather than
    // in each of the several synchronous `PA.nodeIndex` lookups downstream (`_findNodeInfo`,
    // `_isEncrypted`, `_renderBreadcrumb`, `_findNodeBySourceFile`) — the same "preload once per
    // entry point" pattern `callGraphViz._buildGraph` already established, so those helpers stay
    // synchronous and correct rather than each needing their own async boundary.
    async _loadSidebar(procId) {
        await PA.nodeIndex.ensureLoaded();
        var searchIds = ['cmParamSearch', 'cmVarSearch', 'cmTableSearch', 'cmCursorSearch', 'cmCallsSearch', 'cmCalledBySearch', 'cmPkgVarSearch', 'cmTxnSearch', 'cmClaudeSearch'];
        for (var si = 0; si < searchIds.length; si++) {
            var inp = document.getElementById(searchIds[si]);
            if (inp) inp.value = '';
        }
        var detail = null;
        var node = PA.sourceView._findNodeInfo(procId);
        if (node && node.nodeId) {
            try {
                detail = await PA.api.getNodeDetail(node.nodeId.replace(/^\$/, '') + '.json');
            } catch(e) {}
        }
        if (!detail && PA.currentDetail) detail = PA.currentDetail;
        if (!detail) detail = {};

        var params = detail.parameters || [];
        var paramEl = document.getElementById('cmParamList');
        var paramCount = document.getElementById('cmParamCount');
        if (paramCount) paramCount.textContent = params.length || '';
        if (paramEl) {
            var ph = '';
            for (var i = 0; i < params.length; i++) {
                var p = params[i];
                ph += '<div class="cm-sb-item" data-tip="Navigate to this line" onclick="PA.sourceModal._jumpToLine(' + (p.line || 0) + ')">';
                ph += '<span class="cm-sb-mode">' + PA.esc(p.direction || 'IN') + '</span>';
                ph += '<span class="cm-sb-field">' + PA.esc(p.name || '') + '</span>';
                ph += '<span class="cm-sb-type">' + PA.esc(p.dataType || '') + '</span>';
                if (p.line) ph += '<span class="cm-sb-line">:' + p.line + '</span>';
                ph += '</div>';
            }
            paramEl.innerHTML = ph || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var vars = detail.variables || {};
        var allVars = [];
        if (vars.plain) allVars = allVars.concat(vars.plain);
        if (vars.typeRef) allVars = allVars.concat(vars.typeRef);
        if (vars.rowtypeRef) allVars = allVars.concat(vars.rowtypeRef);
        if (vars.collection) allVars = allVars.concat(vars.collection);
        var varEl = document.getElementById('cmVarList');
        var varCount = document.getElementById('cmVarCount');
        if (varCount) varCount.textContent = allVars.length || (vars.total || '');
        if (varEl) {
            var vh = '';
            for (var i = 0; i < allVars.length; i++) {
                var v = allVars[i];
                vh += '<div class="cm-sb-item" data-tip="Navigate to this line" onclick="PA.sourceModal._jumpToLine(' + (v.line || 0) + ')">';
                vh += '<span class="cm-sb-field">' + PA.esc(v.name || '') + '</span>';
                vh += '<span class="cm-sb-type">' + PA.esc(v.dataType || '') + '</span>';
                if (v.constant) vh += '<span class="cm-sb-type" style="color:var(--purple)">CONST</span>';
                if (v.line) vh += '<span class="cm-sb-line">:' + v.line + '</span>';
                vh += '</div>';
            }
            varEl.innerHTML = vh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var tables = detail.tables || detail.directTables || [];
        var tableEl = document.getElementById('cmTableList');
        var tableCount = document.getElementById('cmTableCount');
        if (tableCount) tableCount.textContent = tables.length || '';
        if (tableEl) {
            var th = '';
            for (var i = 0; i < tables.length; i++) {
                var t = tables[i];
                var ops = t.operations ? Object.keys(t.operations) : [];
                th += '<div class="cm-sb-item">';
                // name opens the table detail; the :line badges jump to each usage line in this source
                th += '<span class="cm-sb-field" style="cursor:pointer" data-tip="View table details" onclick="event.stopPropagation(); PA.tableDetail.open(\'' + PA.escJs(t.name || '') + '\', \'' + PA.escJs(t.schema || '') + '\')">' + PA.esc((t.schema ? t.schema + '.' : '') + (t.name || '')) + '</span>';
                for (var j = 0; j < ops.length; j++) {
                    var opLines = (t.operations[ops[j]] || []).filter(function(v, kk, a) { return v && a.indexOf(v) === kk; });
                    if (opLines.length === 0) {
                        th += '<span class="cm-sb-op ' + ops[j].charAt(0) + '">' + ops[j].charAt(0) + '</span>';
                    } else {
                        for (var k = 0; k < opLines.length; k++) {
                            th += '<span class="cm-sb-line" data-tip="Jump to ' + ops[j] + ' at line ' + opLines[k] + '" onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + opLines[k] + ')">' + ops[j].charAt(0) + ':' + opLines[k] + '</span>';
                        }
                    }
                }
                th += '</div>';
            }
            tableEl.innerHTML = th || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var cursors = detail.cursors || [];
        var cursorEl = document.getElementById('cmCursorList');
        var cursorCount = document.getElementById('cmCursorCount');
        if (cursorCount) cursorCount.textContent = cursors.length || '';
        if (cursorEl) {
            var crh = '';
            for (var i = 0; i < cursors.length; i++) {
                var cr = cursors[i];
                var crLine = cr.line || cr.lineNumber || 0;
                crh += '<div class="cm-sb-item" data-tip="Navigate to this line" onclick="PA.sourceModal._jumpToLine(' + crLine + ')">';
                crh += '<span class="cm-sb-badge" style="color:var(--badge-purple)">CUR</span>';
                crh += '<span class="cm-sb-field">' + PA.esc(cr.name || '') + '</span>';
                if (crLine) crh += '<span class="cm-sb-line">:' + crLine + '</span>';
                crh += '</div>';
            }
            cursorEl.innerHTML = crh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var callTree = null;
        try { callTree = await PA.api.getCallTree(procId); } catch(e) {}
        var calls = (callTree && callTree.children) ? callTree.children : [];
        var callsEl = document.getElementById('cmCallsList');
        var callsCount = document.getElementById('cmCallsCount');
        if (callsCount) callsCount.textContent = calls.length || '';
        if (callsEl) {
            var ch = '';
            for (var i = 0; i < calls.length; i++) {
                var c = calls[i];
                var cType = c.callType || '';
                // Two actions per row: the NAME opens the callee's source; the :line
                // badge jumps to the call site in THIS source.
                ch += '<div class="cm-sb-item">';
                if (cType === 'INTERNAL') ch += '<span class="cm-sb-badge int">INT</span>';
                else if (cType) ch += '<span class="cm-sb-badge ext">' + PA.esc(cType) + '</span>';
                ch += '<span class="cm-sb-field" style="cursor:pointer" data-tip="Open this procedure" onclick="event.stopPropagation(); PA.sourceView.openModal(\'' + PA.escJs(c.id || '') + '\')">' + PA.esc(c.name || c.id || '') + '</span>';
                if (c.callLineNumber) ch += '<span class="cm-sb-line" style="cursor:pointer" data-tip="Jump to call site (line ' + c.callLineNumber + ')" onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + c.callLineNumber + ')">:' + c.callLineNumber + '</span>';
                ch += '</div>';
            }
            callsEl.innerHTML = ch || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var callers = [];
        var callersUnknown = false;
        try {
            var callerData = await PA.api.getCallerTree(procId);
            // A failed lookup must not read as "nothing calls this". That claim is what someone
            // relies on to justify deleting a procedure.
            callersUnknown = PA.failed && PA.failed(callerData);
            callers = Array.isArray(callerData) ? callerData : (callerData && callerData.children ? callerData.children : []);
        } catch(e) { callersUnknown = true; }
        var calledByEl = document.getElementById('cmCalledByList');
        var calledByCount = document.getElementById('cmCalledByCount');
        if (calledByCount) calledByCount.textContent = callers.length || '';
        if (calledByEl) {
            var bh = '';
            for (var i = 0; i < callers.length; i++) {
                var cb = callers[i];
                // Two actions: NAME opens the caller's source; :line opens the caller
                // at the exact call site (the call lives in the caller's file).
                var _bLine = cb.callLineNumber || cb.lineNumber || 0;
                var _bId = PA.escJs(cb.id || cb.nodeId || '');
                bh += '<div class="cm-sb-item">';
                bh += '<span class="cm-sb-badge" style="color:var(--text-muted)">&larr;</span>';
                bh += '<span class="cm-sb-field" style="cursor:pointer" data-tip="Open caller" onclick="event.stopPropagation(); PA.sourceView.openModal(\'' + _bId + '\')">' + PA.esc(cb.name || cb.nodeName || cb.id || '') + '</span>';
                if (_bLine) bh += '<span class="cm-sb-line" style="cursor:pointer" data-tip="Open caller at call site (line ' + _bLine + ')" onclick="event.stopPropagation(); PA.sourceView._openAtCaller(\'' + _bId + '\', ' + _bLine + ')">:' + _bLine + '</span>';
                bh += '</div>';
            }
            calledByEl.innerHTML = bh || (callersUnknown
                ? '<div class="cm-sb-item" style="color:var(--badge-red,#c0392b);cursor:default">Could not load callers — unknown, not none</div>'
                : '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>');
        }

        // Package Variables — package-level (global) variables this procedure binds
        // to (reads/writes). Prefers the structured externalPackageVarRefDetails
        // (package, variable, line, READ/WRITE) added for line-level navigation and
        // a read/write badge; falls back to the older bare "PKG.VAR" string list
        // (no line/access info) for older cached artifacts that predate it.
        var pkgRefDetails = detail.externalPackageVarRefDetails || null;
        var pkgRefs = detail.externalPackageVarRefs || [];
        var pkgEl = document.getElementById('cmPkgVarList');
        var pkgCount = document.getElementById('cmPkgVarCount');
        var pkgRefCount = pkgRefDetails ? pkgRefDetails.length : pkgRefs.length;
        if (pkgCount) pkgCount.textContent = pkgRefCount || '';
        if (pkgEl) {
            var ph2 = '';
            if (pkgRefDetails && pkgRefDetails.length) {
                for (var pdi = 0; pdi < pkgRefDetails.length; pdi++) {
                    var pd = pkgRefDetails[pdi];
                    var pdLine = pd.line || 0;
                    var pdAccess = pd.accessType || 'UNKNOWN';
                    var pdBadgeColor = pdAccess === 'WRITE' ? 'var(--badge-red,#c0392b)'
                        : (pdAccess === 'READ' ? 'var(--badge-teal)' : 'var(--text-muted)');
                    var pdTip = 'Package-level variable ' + pdAccess.toLowerCase()
                        + (pdLine ? ' at line ' + pdLine + ' — click to navigate' : '');
                    var pdClick = pdLine ? ('onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + pdLine + ')" style="cursor:pointer"') : 'style="cursor:default"';
                    ph2 += '<div class="cm-sb-item" data-tip="' + PA.esc(pdTip) + '" ' + pdClick + '>';
                    ph2 += '<span class="cm-sb-badge" style="color:' + pdBadgeColor + '">' + PA.esc(pdAccess) + '</span>';
                    ph2 += '<span class="cm-sb-field">' + PA.esc(pd.variableName || '') + '</span>';
                    if (pd.packageName) ph2 += '<span class="cm-sb-line" style="color:var(--text-muted)">' + PA.esc(pd.packageName) + '</span>';
                    if (pdLine) ph2 += '<span class="cm-sb-line">:' + pdLine + '</span>';
                    ph2 += '</div>';
                }
            } else {
                for (var pi = 0; pi < pkgRefs.length; pi++) {
                    var ref = pkgRefs[pi];
                    var parts = String(ref).split('.');
                    var pkgName = parts.length > 1 ? parts.slice(0, -1).join('.') : '';
                    var varName = parts[parts.length - 1];
                    ph2 += '<div class="cm-sb-item" data-tip="Package-level variable bound by this procedure" style="cursor:default">';
                    ph2 += '<span class="cm-sb-badge" style="color:var(--badge-teal)">PKG</span>';
                    ph2 += '<span class="cm-sb-field">' + PA.esc(varName) + '</span>';
                    if (pkgName) ph2 += '<span class="cm-sb-line" style="color:var(--text-muted)">' + PA.esc(pkgName) + '</span>';
                    ph2 += '</div>';
                }
            }
            pkgEl.innerHTML = ph2 || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        // Foreign Keys — relationships this object DECLARES, via ALTER TABLE ... ADD CONSTRAINT
        // ... FOREIGN KEY ... REFERENCES (or the inline column-level REFERENCES spelling).
        //
        // Worth its own section rather than another row among the table operations: every other
        // table-to-table relationship on this screen is inferred from how the DML happens to be
        // written, whereas this one is the schema stating the relationship outright and carrying
        // the column pairing that makes it checkable. Shown as child(cols) -> parent(cols) so the
        // direction of the dependency is readable without expanding anything, and the line is
        // clickable because the constraint lives in THIS object's source.
        var fks = detail.foreignKeys || [];
        var fkEl = document.getElementById('cmFkList');
        var fkCount = document.getElementById('cmFkCount');
        if (fkCount) fkCount.textContent = fks.length || '';
        if (fkEl) {
            var fh = '';
            for (var fi = 0; fi < fks.length; fi++) {
                var fk = fks[fi];
                var fkLine = fk.line || 0;
                var cols = (fk.columns || []).join(', ');
                var refCols = (fk.referencedColumns || []).join(', ');
                var child = (fk.tableName || '?') + (cols ? '(' + cols + ')' : '');
                var parent = (fk.referencedTable || '?') + (refCols ? '(' + refCols + ')' : '');
                // The constraint name is the badge when there is one; an unnamed constraint is
                // still a real constraint, so it gets a neutral FK badge rather than being hidden.
                var fkBadge = fk.constraintName ? fk.constraintName : 'FK';
                var fkTip = 'Declared foreign key: ' + child + ' references ' + parent
                    + (fkLine ? ' — line ' + fkLine + ', click to navigate' : '');
                var fkClick = fkLine
                    ? ('onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + fkLine + ')" style="cursor:pointer"')
                    : 'style="cursor:default"';
                fh += '<div class="cm-sb-item" data-tip="' + PA.esc(fkTip) + '" ' + fkClick + '>';
                fh += '<span class="cm-sb-badge" style="color:var(--badge-purple)">' + PA.esc(fkBadge) + '</span>';
                fh += '<span class="cm-sb-field">' + PA.esc(child + ' → ' + parent) + '</span>';
                if (fkLine) fh += '<span class="cm-sb-line">:' + fkLine + '</span>';
                fh += '</div>';
            }
            fkEl.innerHTML = fh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        // Transaction Scope — COMMIT / ROLLBACK / SAVEPOINT (+ autonomous transaction).
        // Statements live in THIS proc's source, so clicking jumps to the line.
        var txn = detail.transactionScope || null;
        var txnEl = document.getElementById('cmTxnList');
        var txnCount = document.getElementById('cmTxnCount');
        var txnStmts = (txn && txn.statements) ? txn.statements : [];
        if (txnCount) txnCount.textContent = txnStmts.length || (txn && txn.autonomous ? '⚡' : '');
        if (txnEl) {
            var th2 = '';
            if (txn && txn.autonomous) {
                th2 += '<div class="cm-sb-item" style="cursor:default" data-tip="PRAGMA AUTONOMOUS_TRANSACTION">'
                    + '<span class="cm-sb-badge" style="color:var(--badge-orange)">AUTONOMOUS</span>'
                    + '<span class="cm-sb-field">independent transaction</span></div>';
            }
            var txnColor = { COMMIT: 'var(--green)', ROLLBACK: 'var(--red)', SAVEPOINT: 'var(--badge-purple)' };
            for (var ti = 0; ti < txnStmts.length; ti++) {
                var s = txnStmts[ti];
                var line = s.line || 0;
                var oc = line ? ('onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + line + ')" style="cursor:pointer"') : 'style="cursor:default"';
                th2 += '<div class="cm-sb-item" data-tip="Jump to line" ' + oc + '>'
                    + '<span class="cm-sb-badge" style="color:' + (txnColor[s.type] || 'var(--text-muted)') + '">' + PA.esc(s.type) + '</span>'
                    + '<span class="cm-sb-field">' + PA.esc(s.name || '') + '</span>'
                    + (line ? '<span class="cm-sb-line">:' + line + '</span>' : '')
                    + '</div>';
            }
            txnEl.innerHTML = th2 || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        // Claude verification section
        var claudeEl = document.getElementById('cmClaudeList');
        var claudeCount = document.getElementById('cmClaudeCount');
        var claudeResult = PA.claude ? PA.claude.getResult() : null;
        if (claudeEl) {
            var cvh = '';
            var cvTotal = 0;
            if (claudeResult && claudeResult.tables && tables.length > 0) {
                for (var cti = 0; cti < tables.length; cti++) {
                    var ctbl = tables[cti];
                    var ctblName = (ctbl.name || '').toUpperCase();
                    // Find matching claude table
                    var cvTable = claudeResult.tables.find(function(t) {
                        var cn = (t.tableName || '').toUpperCase();
                        return cn === ctblName || cn.endsWith('.' + ctblName);
                    });
                    if (!cvTable || !cvTable.claudeVerifications) continue;
                    var cvs = cvTable.claudeVerifications;
                    var conf = 0, rem = 0, newc = 0;
                    for (var cvi = 0; cvi < cvs.length; cvi++) {
                        var st = (cvs[cvi].status || '').toUpperCase();
                        if (st === 'CONFIRMED') conf++;
                        else if (st === 'REMOVED') rem++;
                        else if (st === 'NEW') newc++;
                    }
                    cvTotal += cvs.length;
                    cvh += '<div class="cm-sb-item" onclick="PA.tableDetail.open(\'' + PA.escJs(ctbl.name || '') + '\', \'' + PA.escJs(ctbl.schema || '') + '\', \'accesses\')">';
                    cvh += '<span class="cm-sb-field" style="flex:1">' + PA.esc(ctbl.name || '') + '</span>';
                    if (conf > 0) cvh += '<span class="cv-badge CONFIRMED" style="font-size:7px">' + conf + '</span>';
                    if (rem > 0) cvh += '<span class="cv-badge REMOVED" style="font-size:7px">' + rem + '</span>';
                    if (newc > 0) cvh += '<span class="cv-badge NEW" style="font-size:7px">' + newc + '</span>';
                    cvh += '</div>';
                }
            }
            if (claudeCount) claudeCount.textContent = cvTotal || '';
            claudeEl.innerHTML = cvh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">' + (claudeResult ? 'No verifications for this proc' : 'No Claude data') + '</div>';
        }

        return detail;
    },

    _jumpToLine(lineNumber) {
        if (!lineNumber) return;
        var body = document.getElementById('srcModalBody');
        if (!body) return;
        var prefix = body.dataset.chunkPrefix;
        var el = prefix ? document.getElementById(prefix + lineNumber) : null;
        if (!el) {
            var lines = body.querySelectorAll('.src-line');
            for (var i = 0; i < lines.length; i++) {
                var ln = lines[i].querySelector('.src-ln');
                if (ln && parseInt(ln.textContent) === lineNumber) { el = lines[i]; break; }
            }
        }
        if (el) {
            el.classList.add('highlight');
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
            el.classList.add('highlight-pulse');
            setTimeout(function(target) { target.classList.remove('highlight-pulse'); }.bind(null, el), 2000);
        }
    },

    _renderInfoBar(node, procId) {
        var bar = document.getElementById('cmInfoBar');
        if (!bar) return;
        if (!node) { bar.style.display = 'none'; return; }
        bar.style.display = '';
        var objType = node.objectType || node.unitType || '';
        var schema = node.schema || node.schemaName || '';
        var pkg = node.packageName || '';
        var name = node.name || procId || '';
        var objName = node.objectName || '';
        var startLine = node.lineStart || node.startLine || 0;
        var endLine = node.lineEnd || node.endLine || 0;
        var lines = (startLine && endLine) ? (endLine - startLine + 1) : 0;
        var h = '';
        if (objType) h += '<span class="cm-info-type ' + PA.esc(objType) + '">' + PA.esc(objType) + '</span>';
        h += '<span class="cm-info-name">' + PA.esc(name) + '</span>';
        if (schema || pkg) {
            // Use bare objectName for FQN to avoid doubling the pkg prefix when name is already "PKG.FUNC"
            var fqnPart = objName || name;
            var fqn = [schema, pkg, fqnPart].filter(Boolean).join('.');
            // Deduplicate if pkg already appears in fqnPart (e.g. name = "PKG.FUNC")
            if (pkg && fqnPart.toUpperCase().startsWith(pkg.toUpperCase() + '.')) {
                fqn = [schema, fqnPart].filter(Boolean).join('.');
            }
            h += '<span style="color:var(--text-muted);font-family:var(--font-mono)">' + PA.esc(fqn) + '</span>';
        }
        if (lines > 0) h += '<span style="color:var(--text-muted)">' + lines + ' lines (L' + startLine + '-' + endLine + ')</span>';
        bar.innerHTML = h;
    },

    toggleSection(sectionName) {
        var list = document.getElementById('cm' + sectionName + 'List');
        if (list) list.classList.toggle('collapsed');
    },

    filterSection(sectionName, query) {
        var list = document.getElementById('cm' + sectionName + 'List');
        if (!list) return;
        var items = list.querySelectorAll('.cm-sb-item');
        var q = SearchQuery.norm(query);
        var shown = 0;
        for (var i = 0; i < items.length; i++) {
            var text = items[i].textContent.toLowerCase();
            if (!q || text.indexOf(q) !== -1) {
                items[i].classList.remove('filtered-out');
                shown++;
            } else {
                items[i].classList.add('filtered-out');
            }
        }
        var countEl = document.getElementById('cm' + sectionName + 'Count');
        if (countEl && q) {
            countEl.textContent = shown + '/' + items.length;
        } else if (countEl) {
            countEl.textContent = items.length || '';
        }
    }
};

document.addEventListener('keydown', function(e) {
    var modal = document.getElementById('srcModal');
    if (!modal || modal.style.display === 'none') return;
    if (e.key === 'Escape') {
        if (document.getElementById('cmSearchBar').style.display !== 'none') {
            PA.sourceModal.closeSearch();
        } else {
            modal.style.display = 'none';
        }
        e.preventDefault();
    }
    if (e.key === 'f' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        PA.sourceModal.toggleSearch();
    }
});

PA.sourceView = {
    _currentSourceFile: null,
    _currentContent: null,
    _highlightedLine: null,
    _srcSearchMatches: [],
    _srcSearchIdx: -1,
    // Map<lineNumber(1-based) → { nodeId, label }> — built when opening source
    _callLineMap: {},
    _txnLineMap: {},   // line → COMMIT/ROLLBACK/SAVEPOINT — the gutter badges (set from the node's transactionScope)

    /**
     * Build the call-line map from a node detail's calls array.
     * Each call has a `lines` array of 1-based line numbers where it appears.
     * We resolve the nodeId as SCHEMA$PKG$FUNC so openModal can navigate to it.
     */
    _buildCallLineMap(calls) {
        var map = {};
        if (!calls || !calls.length) return map;
        for (var i = 0; i < calls.length; i++) {
            var c = calls[i];
            var objName = c.objectName || c.name || '';
            var pkg = c.packageName || '';
            var schema = c.schema || '';
            if (!objName) continue;
            // Build nodeId: SCHEMA$PKG$FUNC or SCHEMA$FUNC
            var nodeId = '';
            if (schema && pkg && objName) nodeId = schema + '$' + pkg + '$' + objName;
            else if (schema && objName) nodeId = schema + '$' + objName;
            else if (pkg && objName) nodeId = pkg + '$' + objName;
            else nodeId = objName;
            // Label shown in tooltip
            var label = pkg ? pkg + '.' + objName : objName;
            var lines = c.lines || [];
            for (var j = 0; j < lines.length; j++) {
                var ln = lines[j];
                if (ln > 0) map[ln] = { nodeId: nodeId, label: label, objName: objName };
            }
        }
        return map;
    },

    /**
     * Open source in the Source tab.
     * If PA.currentDetail has lineStart/lineEnd, shows focused proc view + package-level vars.
     * @param {string} sourceFile - the source filename (e.g. "MY_PKG.pkb")
     * @param {number} [lineNumber] - optional line to scroll to and highlight
     */
    async open(sourceFile, lineNumber) {
        // Stale-response guard (§B5). The MODAL path in this file has been guarded by
        // _reqSeq/_isCurrent for a while; the in-tab open() never was, even though it writes the
        // same kind of shared state (_currentSourceFile, _currentContent and the singleton
        // #srcPath / #srcContainer). Clicking proc A then proc B in the left panel could leave
        // B selected with A's source on screen.
        var isCurrent = UIBusy.seq(PA.sourceView, '_openSeq');
        if (PA.currentDetail && PA.currentDetail.readable === false) {
            document.getElementById('srcPath').textContent = sourceFile || 'Unknown';
            document.getElementById('srcContainer').innerHTML =
                '<div class="empty-msg" style="color:var(--badge-orange)">&var(--badge-teal); Source is encrypted/wrapped and cannot be displayed</div>';
            PA.sourceView.renderDetailBar();
            return;
        }
        // Clean up any previous chunked renderer scroll listener
        if (PA.sourceView._chunkCleanup) { PA.sourceView._chunkCleanup(); PA.sourceView._chunkCleanup = null; }

        // The parser serves source by filename (not owner.objectName)
        let data = null;
        if (sourceFile) {
            data = await PA.api.getSource(sourceFile);
        }

        if (!isCurrent()) return;

        if (!data || !data.content) {
            document.getElementById('srcPath').textContent = sourceFile || 'Unknown';
            document.getElementById('srcContainer').innerHTML =
                '<div class="empty-msg">Source not available for: ' + PA.esc(sourceFile || '') + '</div>';
            return;
        }

        // Check if we have proc boundaries to show focused view
        const detail = PA.currentDetail;
        const startLine = detail ? (detail.lineStart || 0) : 0;
        const endLine = detail ? (detail.lineEnd || 0) : 0;
        const procName = detail ? (detail.name || '') : '';

        // For package subprograms, lineStart/lineEnd refer to the full .pkb file
        let content = data.content;
        let resolvedFile = sourceFile;
        if (startLine > 0 && endLine > 0) {
            const pkbFile = PA.sourceView._resolvePackageSource(sourceFile);
            if (pkbFile && pkbFile !== sourceFile) {
                const pkbData = await PA.api.getSource(pkbFile);
                if (!isCurrent()) return;      // second await — re-check before writing
                if (pkbData && pkbData.content) {
                    content = pkbData.content;
                    resolvedFile = pkbFile;
                }
            }
        }

        PA.sourceView._currentSourceFile = resolvedFile;
        PA.sourceView._currentContent = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

        // Build call-line map so inline proc names become clickable
        PA.sourceView._callLineMap = PA.sourceView._buildCallLineMap(detail ? (detail.calls || []) : []);

        const displayName = data.objectName || sourceFile;

        // startLine=1 with a package subprogram means the parser couldn't locate the proc
        // (e.g. source is truncated). Fall through to full-file view in that case.
        const isFocusable = startLine > 1 && endLine > startLine;
        if (isFocusable) {
            // Focused view: show proc section + package-level variables
            document.getElementById('srcPath').innerHTML =
                '<span style="color:var(--text-muted)">' + PA.esc(displayName) + '</span>' +
                '<span style="margin:0 6px;opacity:0.5">&rsaquo;</span>' +
                '<span style="color:var(--accent);font-weight:700">' + PA.esc(procName) + '</span>' +
                '<span style="margin-left:8px;font-size:10px;color:var(--text-muted)">Lines ' + startLine + '-' + endLine + '</span>' +
                '<button class="btn btn-sm" style="margin-left:12px;font-size:10px" onclick="PA.sourceView.showFullPackage()" title="Switch to full source view">Show Full Source</button>' +
                '<button class="btn btn-sm" style="margin-left:4px;font-size:10px" onclick="PA.sourceView.copyProcSource()" title="Copy only this procedure\'s source code to clipboard">Copy Proc</button>' +
                '<button class="btn btn-sm" style="margin-left:4px;font-size:10px" onclick="PA.sourceView.toggleSearch()" title="Search source (Ctrl+F)">Find</button>';
            PA.sourceView.renderFocused(PA.sourceView._currentContent, startLine, endLine, resolvedFile);
        } else {
            // Full file view (also used when startLine=1, i.e. proc not locatable in truncated source)
            const truncatedNote = (startLine === 1 && procName && detail && detail.packageName)
                ? '<span style="margin-left:8px;font-size:10px;color:var(--badge-orange)" title="The database returned truncated source — procedure location unknown">&#9888; Truncated source — showing full package</span>'
                : '';
            document.getElementById('srcPath').innerHTML =
                '<span>' + PA.esc(displayName) + '</span>' +
                truncatedNote +
                '<button class="btn btn-sm" style="margin-left:12px;font-size:10px" onclick="PA.sourceView.copyFullSource()" title="Copy the entire source code to clipboard">Copy All</button>' +
                '<button class="btn btn-sm" style="margin-left:4px;font-size:10px" onclick="PA.sourceView.toggleSearch()" title="Search source (Ctrl+F)">Find</button>';
            PA.sourceView.renderContent(data.content, lineNumber);
        }
        PA.sourceView.renderDetailBar();
        PA.sourceView.closeSearch();
        if (PA.context.procId) PA.sourceView.loadSidebar(PA.context.procId);
    },

    /**
     * Open source from a procedure ID.
     * Looks up the sourceFile from the current analysis data and calls open().
     * @param {string} procId - the procedure nodeId to find source for
     */
    async openFromProc(procId) {
        if (!procId) return;
        await PA.nodeIndex.ensureLoaded();
        if (PA.sourceView._isEncrypted(procId)) { PA.toast('Source is encrypted/wrapped — cannot view', 'warn'); return; }
        const node = PA.sourceView._findNodeInfo(procId);
        if (node && node.sourceFile) {
            await PA.sourceView.open(node.sourceFile, node.lineStart || 0);
        } else if (PA.currentDetail && PA.currentDetail.sourceFile) {
            await PA.sourceView.open(PA.currentDetail.sourceFile, PA.currentDetail.lineStart || 0);
        }
    },

    /**
     * Switch to Source tab and open source at a specific line.
     * @param {string} sourceFile - the source filename
     * @param {number} lineNumber - line to scroll to
     */
    async openAtLine(sourceFile, lineNumber) {
        var resolved = PA.sourceView._resolvePackageSource(sourceFile) || sourceFile;
        await PA.sourceView.openModalAtLine(resolved, lineNumber);
    },

    /**
     * Open the source of a node (by its id/nodeId) at a specific line.
     * Resolves the source file from the loaded analysis data — the node id is
     * always present in Table Ops / References rows, whereas the per-row
     * sourceFile is often empty, which used to make line-jumps silently fail.
     */
    async openNodeAtLine(nodeId, lineNumber) {
        await PA.nodeIndex.ensureLoaded();
        var sf = null;
        var node = (typeof PA._findNodeInData === 'function') ? await PA._findNodeInData(nodeId) : null;
        if (node && node.sourceFile) sf = node.sourceFile;
        if (!sf) {
            var m = PA.nodeIndex.byIdIgnoringCase(nodeId);
            if (m) sf = m.sourceFile;
        }
        if (!sf) { PA.toast('No source file found for ' + (nodeId || 'this item'), 'error'); return; }
        await PA.sourceView.openAtLine(sf, lineNumber);
    },

    /**
     * Open a caller's source modal and jump to the line where it calls the
     * current procedure (the call site lives in the caller's file).
     */
    async _openAtCaller(nodeId, lineNumber) {
        await PA.sourceView.openModal(nodeId);
        if (lineNumber) {
            setTimeout(function() { PA.sourceModal._jumpToLine(lineNumber); }, 120);
        }
    },

    async openModalAtLine(sourceFile, lineNumber) {
        if (!sourceFile) return;
        await PA.nodeIndex.ensureLoaded();
        // Blank the modal BEFORE awaiting, so the previous file is never left on screen.
        const _tok = PA.sourceModal._beginLoad(sourceFile.replace(/\.(pkb|pks|prc|fnc|sql)$/i, ''));
        const _modalEarly = document.getElementById('srcModal');
        if (_modalEarly) {
            _modalEarly.style.display = '';
            if (PA.modalStack) _modalEarly.style.zIndex = PA.modalStack.nextZ();
        }
        let data = await PA.api.getSource(sourceFile);
        if (!PA.sourceModal._isCurrent(_tok)) return;   // a newer open superseded this one
        if (!data || !data.content) { PA.toast('Source not available: ' + sourceFile, 'error'); return; }
        const content = data.content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        const lines = content.split('\n');
        const name = sourceFile.replace(/\.(pkb|pks|prc|fnc|sql)$/i, '');

        // Resolve node info by sourceFile so sidebar can be populated
        const node = PA.sourceView._findNodeBySourceFile(sourceFile);
        const procId = (node && node.nodeId) || name;

        PA.sourceModal._history.push({ procId: procId, sourceFile: sourceFile, line: lineNumber });
        PA.sourceModal._historyFwd = [];
        PA.sourceModal._updateNav();

        const modal = document.getElementById('srcModal');
        const title = document.getElementById('srcModalTitle');
        const body = document.getElementById('srcModalBody');
        title.textContent = (node && node.name || name) + (lineNumber > 0 ? ' (Line ' + lineNumber + ')' : '');
        PA.sourceModal._renderInfoBar(node, procId);
        PA.sourceModal.closeSearch();

        PA.sourceView._renderModalBodyChunked(body, lines, 0, lines.length, lineNumber);
        modal.style.display = '';
        if (PA.modalStack) modal.style.zIndex = PA.modalStack.nextZ();

        PA.sourceModal._content = content;
        PA.sourceModal._renderBreadcrumb();

        // Load sidebar (params, variables, tables, calls, callers) just like openModal
        PA.sourceModal._loadSidebar(procId).then(function(detail) {
            if (!PA.sourceModal._isCurrent(_tok)) return;   // stale sidebar response
            if (detail && detail.calls) {
                PA.sourceView._callLineMap = PA.sourceView._buildCallLineMap(detail.calls);
            }
        });
    },

    _modalChunkCleanup: null,

    _renderModalBodyChunked(body, lines, from, to, scrollToLine) {
        if (PA.sourceView._modalChunkCleanup) { PA.sourceView._modalChunkCleanup(); PA.sourceView._modalChunkCleanup = null; }
        body.dataset.chunkPrefix = '';
        const totalLines = to - from;
        if (totalLines <= PA.sourceView.CHUNK_THRESHOLD) {
            let html = '<div class="source-code">';
            for (let i = from; i < to; i++) {
                const ln = i + 1;
                const isHL = scrollToLine && ln === scrollToLine;
                const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                html += '<div class="src-line' + (isHL ? ' highlight' : '') + '">';
                html += '<span class="src-ln clickable' + (isHL ? ' active' : '') + '" onclick="PA.sourceModal._jumpToLine(' + ln + ')">' + ln + '</span>';
                html += '<span class="src-code">' + highlighted + '</span>';
                html += '</div>';
            }
            html += '</div>';
            body.innerHTML = html;
            if (scrollToLine > 0) {
                setTimeout(function() { PA.sourceModal._jumpToLine(scrollToLine); }, 150);
            }
            return;
        }

        const lineHeight = 19;
        const chunkSize = PA.sourceView.CHUNK_SIZE;
        const buffer = PA.sourceView.CHUNK_BUFFER;
        const prefix = 'srcm-' + Date.now() + '-';
        body.dataset.chunkPrefix = prefix;
        const targetIdx = (scrollToLine && scrollToLine > 0) ? Math.max(from, scrollToLine - 1) : from;
        const initFrom = Math.max(from, targetIdx - chunkSize);
        const initTo = Math.min(to, targetIdx + chunkSize);

        const state = { lines, from, to, prefix, scrollToLine, lineHeight, renderedFrom: initFrom, renderedTo: initTo };

        body.innerHTML =
            '<div class="source-code src-chunked" style="position:relative">' +
            '<div class="src-chunk-info">' + totalLines.toLocaleString() + ' lines — chunked rendering</div>' +
            '<div id="' + prefix + 'top" style="height:' + ((initFrom - from) * lineHeight) + 'px"></div>' +
            '<div id="' + prefix + 'lines"></div>' +
            '<div id="' + prefix + 'bot" style="height:' + ((to - initTo) * lineHeight) + 'px"></div>' +
            '</div>';

        const linesDiv = document.getElementById(prefix + 'lines');
        const topSpacer = document.getElementById(prefix + 'top');
        const botSpacer = document.getElementById(prefix + 'bot');

        let initHtml = '';
        for (let i = initFrom; i < initTo; i++) {
            const ln = i + 1;
            const isHL = scrollToLine && ln === scrollToLine;
            const highlighted = PA.sourceView.highlightLine(lines[i], ln);
            initHtml += '<div class="src-line' + (isHL ? ' highlight' : '') + '" id="' + prefix + ln + '" style="height:' + lineHeight + 'px">';
            initHtml += '<span class="src-ln clickable' + (isHL ? ' active' : '') + '" onclick="PA.sourceModal._jumpToLine(' + ln + ')">' + ln + '</span>';
            initHtml += '<span class="src-code">' + highlighted + '</span>';
            initHtml += '</div>';
        }
        linesDiv.innerHTML = initHtml;

        if (scrollToLine > 0) {
            setTimeout(function() {
                const el = document.getElementById(prefix + scrollToLine);
                if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }, 100);
        }

        const scrollParent = body;
        let scrollTick = false;
        const onScroll = () => {
            if (scrollTick) return;
            scrollTick = true;
            requestAnimationFrame(() => {
                scrollTick = false;
                const viewTop = scrollParent.scrollTop || 0;
                const viewHeight = scrollParent.clientHeight || 600;
                const visibleFrom = from + Math.floor(viewTop / lineHeight);
                const visibleTo = from + Math.ceil((viewTop + viewHeight) / lineHeight);
                const needFrom = Math.max(from, visibleFrom - buffer);
                const needTo = Math.min(to, visibleTo + buffer);

                if (needFrom < state.renderedFrom) {
                    const expandFrom = Math.max(from, needFrom);
                    let html = '';
                    for (let i = expandFrom; i < state.renderedFrom; i++) {
                        const ln = i + 1;
                        const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                        html += '<div class="src-line" id="' + prefix + ln + '" style="height:' + lineHeight + 'px">';
                        html += '<span class="src-ln clickable" onclick="PA.sourceModal._jumpToLine(' + ln + ')">' + ln + '</span>';
                        html += '<span class="src-code">' + highlighted + '</span>';
                        html += '</div>';
                    }
                    linesDiv.insertAdjacentHTML('afterbegin', html);
                    state.renderedFrom = expandFrom;
                    topSpacer.style.height = ((expandFrom - from) * lineHeight) + 'px';
                }

                if (needTo > state.renderedTo) {
                    const expandTo = Math.min(to, needTo);
                    let html = '';
                    for (let i = state.renderedTo; i < expandTo; i++) {
                        const ln = i + 1;
                        const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                        html += '<div class="src-line" id="' + prefix + ln + '" style="height:' + lineHeight + 'px">';
                        html += '<span class="src-ln clickable" onclick="PA.sourceModal._jumpToLine(' + ln + ')">' + ln + '</span>';
                        html += '<span class="src-code">' + highlighted + '</span>';
                        html += '</div>';
                    }
                    linesDiv.insertAdjacentHTML('beforeend', html);
                    state.renderedTo = expandTo;
                    botSpacer.style.height = ((to - expandTo) * lineHeight) + 'px';
                }
            });
        };

        scrollParent.addEventListener('scroll', onScroll, { passive: true });
        PA.sourceView._modalChunkCleanup = () => scrollParent.removeEventListener('scroll', onScroll);
    },

    /**
     * Highlight a line in the current source view (used for line number clicks).
     * @param {number} lineNumber - the line to highlight
     */
    highlightLineInView(lineNumber) {
        // Remove previous highlight
        if (PA.sourceView._highlightedLine) {
            const prev = document.querySelector('.src-line.highlight');
            if (prev) prev.classList.remove('highlight');
        }
        // Find and highlight the new line
        const allLines = document.querySelectorAll('.src-line');
        for (const el of allLines) {
            const ln = el.querySelector('.src-ln');
            if (ln && parseInt(ln.textContent) === lineNumber) {
                el.classList.add('highlight');
                el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                el.classList.add('highlight-pulse');
                setTimeout(() => el.classList.remove('highlight-pulse'), 2000);
                PA.sourceView._highlightedLine = lineNumber;
                return;
            }
        }
    },

    /** Render focused proc view: package-level declarations + proc body */
    renderFocused(content, startLine, endLine, sourceFile) {
        const container = document.getElementById('srcContainer');
        const lines = content.split('\n');
        const prefix = 'src-ln-' + Date.now() + '-';

        // Extract package-level variable declarations (lines before first PROCEDURE/FUNCTION)
        let pkgVarEnd = 0;
        for (let i = 0; i < lines.length && i < startLine - 1; i++) {
            const trimmed = lines[i].trim().toUpperCase();
            if (trimmed.match(/^\s*(PROCEDURE|FUNCTION)\s+/)) {
                pkgVarEnd = i;
                break;
            }
            pkgVarEnd = i + 1;
        }

        // Find actual package header end (after IS/AS)
        let headerEnd = 0;
        for (let i = 0; i < Math.min(pkgVarEnd, 30); i++) {
            const trimmed = lines[i].trim().toUpperCase();
            if (trimmed === 'IS' || trimmed === 'AS' || trimmed.endsWith(' IS') || trimmed.endsWith(' AS')) {
                headerEnd = i + 1;
                break;
            }
        }

        let html = '<div class="source-code">';

        // Show package-level declarations if they exist and are meaningful
        const hasVars = pkgVarEnd > headerEnd + 1;
        if (hasVars) {
            html += '<div class="src-section-label">Package Variables & Types</div>';
            for (let i = headerEnd; i < pkgVarEnd; i++) {
                const ln = i + 1;
                const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                html += '<div class="src-line dim" id="' + prefix + ln + '">';
                html += '<span class="src-ln clickable" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
                html += '<span class="src-code">' + highlighted + '</span>';
                html += '</div>';
            }
            html += '<div class="src-section-divider"></div>';
        }

        // Show procedure body (the focused section)
        const procLineCount = endLine - startLine + 1;
        html += '<div class="src-section-label" style="color:var(--accent)">Procedure Body (Lines ' + startLine + '-' + endLine + ', ' + procLineCount.toLocaleString() + ' lines)</div>';

        // For large procs (2000+ lines), use chunked rendering for the body
        const from = Math.max(0, startLine - 1);
        const to = Math.min(lines.length, endLine);
        if (procLineCount > PA.sourceView.CHUNK_THRESHOLD) {
            // Render initial chunk around startLine, expand on scroll
            const chunkSize = PA.sourceView.CHUNK_SIZE;
            const initTo = Math.min(to, from + chunkSize);
            const lineHeight = 19;

            html += '<div class="src-chunked-body" style="position:relative">';
            html += '<div class="src-chunk-info">' + procLineCount.toLocaleString() + ' lines — chunked rendering</div>';
            html += '<div id="' + prefix + 'body-top" style="height:0"></div>';
            html += '<div id="' + prefix + 'body-lines"></div>';
            html += '<div id="' + prefix + 'body-bot" style="height:' + ((to - initTo) * lineHeight) + 'px"></div>';
            html += '</div></div>';
            container.innerHTML = html;

            // Render initial lines
            const bodyDiv = document.getElementById(prefix + 'body-lines');
            const bodyState = {
                lines, sf: sourceFile, prefix, lineNumber: startLine, lineHeight, totalLines: lines.length,
                rangeFrom: from, rangeTo: to,
                renderedFrom: from, renderedTo: initTo,
            };
            let bodyHtml = '';
            for (let i = from; i < initTo; i++) {
                const ln = i + 1;
                const isStart = ln === startLine;
                const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                bodyHtml += '<div class="src-line' + (isStart ? ' highlight' : '') + '" id="' + prefix + ln + '" style="height:' + lineHeight + 'px">';
                bodyHtml += '<span class="src-ln clickable' + (isStart ? ' active' : '') + '" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
                bodyHtml += '<span class="src-code">' + highlighted + '</span>';
                bodyHtml += '</div>';
            }
            bodyDiv.innerHTML = bodyHtml;

            // Scroll handler for focused chunked body
            const botSpacer = document.getElementById(prefix + 'body-bot');
            const scrollParent = container;
            let scrollTick = false;

            const onScroll = () => {
                if (scrollTick) return;
                scrollTick = true;
                requestAnimationFrame(() => {
                    scrollTick = false;
                    if (bodyState.renderedTo >= to) {
                        scrollParent.removeEventListener('scroll', onScroll);
                        return;
                    }
                    const bodyRect = bodyDiv.getBoundingClientRect();
                    const parentRect = scrollParent.getBoundingClientRect();
                    const distToBottom = bodyRect.bottom - parentRect.bottom;
                    if (distToBottom < 500) {
                        // Expand downward
                        const expandTo = Math.min(to, bodyState.renderedTo + chunkSize);
                        let moreHtml = '';
                        for (let i = bodyState.renderedTo; i < expandTo; i++) {
                            const ln = i + 1;
                            const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                            moreHtml += '<div class="src-line" id="' + prefix + ln + '" style="height:' + lineHeight + 'px">';
                            moreHtml += '<span class="src-ln clickable" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
                            moreHtml += '<span class="src-code">' + highlighted + '</span>';
                            moreHtml += '</div>';
                        }
                        bodyDiv.insertAdjacentHTML('beforeend', moreHtml);
                        bodyState.renderedTo = expandTo;
                        botSpacer.style.height = ((to - expandTo) * lineHeight) + 'px';
                    }
                });
            };
            scrollParent.addEventListener('scroll', onScroll, { passive: true });
            PA.sourceView._chunkCleanup = () => scrollParent.removeEventListener('scroll', onScroll);
        } else {
            // Small proc: render all lines at once
            for (let i = from; i < to; i++) {
                const ln = i + 1;
                const isStart = ln === startLine;
                const highlighted = PA.sourceView.highlightLine(lines[i], ln);
                html += '<div class="src-line' + (isStart ? ' highlight' : '') + '" id="' + prefix + ln + '">';
                html += '<span class="src-ln clickable' + (isStart ? ' active' : '') + '" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
                html += '<span class="src-code">' + highlighted + '</span>';
                html += '</div>';
            }
            html += '</div>';
            container.innerHTML = html;
        }

        // Scroll to start
        setTimeout(() => {
            const el = document.getElementById(prefix + startLine);
            if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 150);
    },

    /** Switch to full source view */
    showFullPackage() {
        if (PA.sourceView._currentContent) {
            const sf = PA.sourceView._currentSourceFile || '';
            const displayName = sf;
            document.getElementById('srcPath').innerHTML =
                '<span>' + PA.esc(displayName) + '</span>' +
                '<button class="btn btn-sm" style="margin-left:12px;font-size:10px" onclick="PA.sourceView.copyFullSource()" title="Copy the entire source code to clipboard">Copy All</button>' +
                '<button class="btn btn-sm" style="margin-left:4px;font-size:10px" onclick="PA.sourceView.toggleSearch()" title="Search source (Ctrl+F)">Find</button>';
            const startLine = PA.currentDetail ? (PA.currentDetail.lineStart || 0) : 0;
            PA.sourceView.renderContent(PA.sourceView._currentContent, startLine);
        }
    },

    /** Copy only the current proc's source lines */
    copyProcSource() {
        if (!PA.sourceView._currentContent || !PA.currentDetail) return;
        const lines = PA.sourceView._currentContent.split('\n');
        const from = Math.max(0, (PA.currentDetail.lineStart || 1) - 1);
        const to = Math.min(lines.length, PA.currentDetail.lineEnd || lines.length);
        const procText = lines.slice(from, to).join('\n');
        navigator.clipboard.writeText(procText).then(() => PA.toast('Proc source copied', 'success'));
    },

    /** Copy full source */
    copyFullSource() {
        if (!PA.sourceView._currentContent) return;
        navigator.clipboard.writeText(PA.sourceView._currentContent).then(() => PA.toast('Full source copied', 'success'));
    },

    renderContent(content, lineNumber) {
        const container = document.getElementById('srcContainer');
        PA.sourceView.renderContentTo(container, content, lineNumber);
    },

    /**
     * Threshold: files with more lines than this use chunked rendering.
     * Below this threshold we render all at once (faster for small files).
     */
    CHUNK_THRESHOLD: 2000,
    /** Lines to render per chunk in chunked mode */
    CHUNK_SIZE: 500,
    /** Extra lines above/below viewport to pre-render */
    CHUNK_BUFFER: 200,

    /** Render source into any container element */
    renderContentTo(container, content, lineNumber, sourceFile) {
        const lines = content.split('\n');
        const sf = sourceFile || PA.sourceView._currentSourceFile || '';

        // Small files: render all at once (fast path)
        if (lines.length <= PA.sourceView.CHUNK_THRESHOLD) {
            PA.sourceView._renderAllLines(container, lines, lineNumber, sf);
            return;
        }

        // Large files: chunked rendering — only render visible window
        PA.sourceView._renderChunked(container, lines, lineNumber, sf);
    },

    /** Fast path: render all lines at once for small files */
    _renderAllLines(container, lines, lineNumber, sf) {
        const prefix = 'src-ln-' + Date.now() + '-';
        let html = '<div class="source-code">';
        for (let i = 0; i < lines.length; i++) {
            const ln = i + 1;
            const isHL = lineNumber && ln === lineNumber;
            const highlighted = PA.sourceView.highlightLine(lines[i], ln);
            html += '<div class="src-line' + (isHL ? ' highlight' : '') + '" id="' + prefix + ln + '">';
            html += '<span class="src-ln clickable' + (isHL ? ' active' : '') + '" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
            html += '<span class="src-code">' + highlighted + '</span>';
            html += '</div>';
        }
        html += '</div>';
        container.innerHTML = html;

        if (lineNumber && lineNumber > 0) {
            setTimeout(() => {
                const el = document.getElementById(prefix + lineNumber);
                if (el) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.classList.add('highlight-pulse');
                    setTimeout(() => el.classList.remove('highlight-pulse'), 2000);
                }
            }, 150);
        }
    },

    /**
     * Chunked rendering for large files (2000+ lines).
     * Renders an initial window around the target line, then expands on scroll.
     * Uses a spacer div to maintain correct scroll position/height.
     */
    _renderChunked(container, lines, lineNumber, sf) {
        const totalLines = lines.length;
        const lineHeight = 19; // px per line (matches .src-line height)
        const prefix = 'src-chn-' + Date.now() + '-';
        const chunkSize = PA.sourceView.CHUNK_SIZE;
        const buffer = PA.sourceView.CHUNK_BUFFER;

        // State for this chunked view
        const state = {
            lines, sf, prefix, lineNumber, lineHeight, totalLines,
            renderedFrom: 0,  // first rendered line index (0-based)
            renderedTo: 0,    // last rendered line index (exclusive)
        };

        // Determine initial render window: center on lineNumber or start
        const targetIdx = (lineNumber && lineNumber > 0) ? lineNumber - 1 : 0;
        const initFrom = Math.max(0, targetIdx - chunkSize);
        const initTo = Math.min(totalLines, targetIdx + chunkSize);

        // Build container: spacer-top + rendered lines + spacer-bottom
        container.innerHTML =
            '<div class="source-code src-chunked" style="position:relative">' +
            '<div class="src-chunk-info">' + totalLines.toLocaleString() + ' lines — chunked rendering</div>' +
            '<div id="' + prefix + 'top" style="height:' + (initFrom * lineHeight) + 'px"></div>' +
            '<div id="' + prefix + 'lines"></div>' +
            '<div id="' + prefix + 'bot" style="height:' + ((totalLines - initTo) * lineHeight) + 'px"></div>' +
            '</div>';

        const linesDiv = document.getElementById(prefix + 'lines');
        const topSpacer = document.getElementById(prefix + 'top');
        const botSpacer = document.getElementById(prefix + 'bot');

        // Render initial chunk
        linesDiv.innerHTML = PA.sourceView._buildLinesHtml(state, initFrom, initTo);
        state.renderedFrom = initFrom;
        state.renderedTo = initTo;

        // Scroll to target line
        if (lineNumber && lineNumber > 0) {
            setTimeout(() => {
                const el = document.getElementById(prefix + lineNumber);
                if (el) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.classList.add('highlight-pulse');
                    setTimeout(() => el.classList.remove('highlight-pulse'), 2000);
                }
            }, 100);
        }

        // Scroll handler: expand rendered range when user scrolls near edges
        const scrollParent = container;
        let scrollTick = false;

        const onScroll = () => {
            if (scrollTick) return;
            scrollTick = true;
            requestAnimationFrame(() => {
                scrollTick = false;
                const rect = container.getBoundingClientRect();
                const viewTop = scrollParent.scrollTop || 0;
                const viewHeight = scrollParent.clientHeight || rect.height;

                // Which line range is visible?
                const codeEl = container.querySelector('.source-code');
                if (!codeEl) return;
                const codeTop = codeEl.getBoundingClientRect().top - rect.top + viewTop;
                const visibleFromPx = Math.max(0, viewTop - codeTop);
                const visibleFrom = Math.floor(visibleFromPx / lineHeight);
                const visibleTo = Math.ceil((visibleFromPx + viewHeight) / lineHeight);

                // Need to expand?
                const needFrom = Math.max(0, visibleFrom - buffer);
                const needTo = Math.min(totalLines, visibleTo + buffer);

                // Expand upward
                if (needFrom < state.renderedFrom) {
                    const expandFrom = Math.max(0, needFrom);
                    const expandTo = state.renderedFrom;
                    const html = PA.sourceView._buildLinesHtml(state, expandFrom, expandTo);
                    linesDiv.insertAdjacentHTML('afterbegin', html);
                    state.renderedFrom = expandFrom;
                    topSpacer.style.height = (expandFrom * lineHeight) + 'px';
                }

                // Expand downward
                if (needTo > state.renderedTo) {
                    const expandFrom = state.renderedTo;
                    const expandTo = Math.min(totalLines, needTo);
                    const html = PA.sourceView._buildLinesHtml(state, expandFrom, expandTo);
                    linesDiv.insertAdjacentHTML('beforeend', html);
                    state.renderedTo = expandTo;
                    botSpacer.style.height = ((totalLines - expandTo) * lineHeight) + 'px';
                }
            });
        };

        scrollParent.addEventListener('scroll', onScroll, { passive: true });

        // Store cleanup ref so we can remove listener on next render
        PA.sourceView._chunkCleanup = () => {
            scrollParent.removeEventListener('scroll', onScroll);
        };
    },

    /** Build HTML for a range of lines [from, to) — used by chunked renderer */
    _buildLinesHtml(state, from, to) {
        let html = '';
        for (let i = from; i < to; i++) {
            const ln = i + 1;
            const isHL = state.lineNumber && ln === state.lineNumber;
            const highlighted = PA.sourceView.highlightLine(state.lines[i], ln);
            html += '<div class="src-line' + (isHL ? ' highlight' : '') + '" id="' + state.prefix + ln + '" style="height:' + state.lineHeight + 'px">';
            html += '<span class="src-ln clickable' + (isHL ? ' active' : '') + '" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
            html += '<span class="src-code">' + highlighted + '</span>';
            html += '</div>';
        }
        return html;
    },

    /**
     * Resolves whatever the caller holds — node id, name, or a source-file path.
     *
     * <p>Was three successive linear scans over every node (exact, then case-insensitive, then by
     * source file). `PA.nodeIndex.resolve` does the same precedence with map lookups; the order is
     * preserved deliberately, because an exact id must beat a case-insensitive name or a procedure
     * whose name matches another's lower-cased id resolves to the wrong one.
     */
    _findNodeInfo(procId) {
        if (!procId) return null;
        const found = PA.nodeIndex.resolve(procId);
        if (found) return found;
        // Not in the index: the call tree carries nodes the index does not, so this is a real
        // fallback rather than a second attempt at the same question.
        if (PA.callTrace && PA.callTrace.treeData) {
            const treeNode = PA.sourceView._searchTree(PA.callTrace.treeData, procId);
            if (treeNode) return { nodeId: treeNode.id, name: treeNode.name, sourceFile: treeNode.sourceFile, depth: treeNode.depth };
        }
        return null;
    },

    _findNodeBySourceFile(sourceFile) {
        if (!sourceFile) return null;
        return PA.nodeIndex.bySourceFile(sourceFile);
    },

    _searchTree(node, id) {
        if (!node) return null;
        if (node.id === id || node.name === id) return node;
        if (node.children) {
            for (const child of node.children) {
                const found = PA.sourceView._searchTree(child, id);
                if (found) return found;
            }
        }
        return null;
    },

    _resolvePackageSource(sourceFile) {
        if (!sourceFile) return null;
        var parts = sourceFile.replace(/\.sql$/i, '').split('.');
        // Real package-subprogram files are schema.PKG_NAME.subprogram.sql, with a real combined
        // schema.PKG_NAME.pkb also on disk (e.g. ANALYZER.ERP_PKG_PLANNER.COMPUTE_TOTAL.sql ->
        // ANALYZER.ERP_PKG_PLANNER.pkb). Trigger source files are schema.TRG.triggerName.sql —
        // also 3 dot-separated segments, so the naive length check misfired here: "TRG" got
        // treated as a package name, producing a nonexistent "schema.TRG.pkb" (404, confirmed
        // against 2 real triggers in this corpus: ANALYZER_REF.TRG.TRG_REF_REGION.sql and
        // ANALYZER.TRG.ERP_TRG_SO_STMT_AD.sql). "TRG" is a naming-convention sentinel, never a
        // real package name, so exclude it explicitly rather than widen the check to need the
        // node's own objectType threaded through every call site.
        if (parts.length === 3 && parts[1].toUpperCase() !== 'TRG') {
            return parts[0] + '.' + parts[1] + '.pkb';
        }
        return null;
    },

    _isEncrypted(procId) {
        var node = PA.sourceView._findNodeInfo(procId);
        if (node && node.readable === false) return true;
        if (PA.currentDetail && PA.currentDetail.readable === false && (!procId || procId === PA.currentDetail.nodeId)) return true;
        return false;
    },

    toggleSearch() {
        var bar = document.getElementById('srcSearchBar');
        if (!bar) return;
        if (bar.style.display === 'none') {
            bar.style.display = '';
            var input = document.getElementById('srcSearchInput');
            input.focus();
            var sel = window.getSelection ? window.getSelection().toString().trim() : '';
            if (sel && sel.length >= 2 && sel.length <= 150) {
                input.value = sel;
                input.select();
                PA.sourceView.search(sel);
            }
        } else {
            PA.sourceView.closeSearch();
        }
    },
    closeSearch() {
        var bar = document.getElementById('srcSearchBar');
        if (bar) bar.style.display = 'none';
        PA.sourceView._clearSrcHighlights();
        PA.sourceView._srcSearchMatches = [];
        PA.sourceView._srcSearchIdx = -1;
        PA.sourceView._srcSearchTotalCount = 0;
        var input = document.getElementById('srcSearchInput');
        if (input) input.value = '';
        var count = document.getElementById('srcSearchCount');
        if (count) count.textContent = '0';
    },
    search(query) {
        PA.sourceView._clearSrcHighlights();
        PA.sourceView._srcSearchMatches = [];
        PA.sourceView._srcSearchIdx = -1;
        PA.sourceView._srcSearchTotalCount = 0;
        var count = document.getElementById('srcSearchCount');
        if (!query || query.length < 2) { if (count) count.textContent = '0'; return; }
        var q = SearchQuery.norm(query);

        // Count hits across all content (includes off-screen chunked lines)
        var totalCount = 0;
        if (PA.sourceView._currentContent) {
            var raw = PA.sourceView._currentContent.toLowerCase();
            var pos = 0;
            while ((pos = raw.indexOf(q, pos)) !== -1) { totalCount++; pos++; }
        }

        var lines = document.querySelectorAll('#srcContainer .src-line');
        var matches = [];
        lines.forEach(function(line) {
            var codeEl = line.querySelector('.src-code');
            if (!codeEl) return;
            if (codeEl.textContent.toLowerCase().indexOf(q) !== -1) {
                matches.push(line);
                line.classList.add('cm-search-match');
            }
        });
        PA.sourceView._srcSearchMatches = matches;
        PA.sourceView._srcSearchTotalCount = totalCount;
        if (count) count.textContent = totalCount > 0 ? totalCount : '0';
        if (matches.length > 0) PA.sourceView._activateSrcMatch(0);
    },
    searchNext() {
        if (!PA.sourceView._srcSearchMatches.length) return;
        PA.sourceView._activateSrcMatch((PA.sourceView._srcSearchIdx + 1) % PA.sourceView._srcSearchMatches.length);
    },
    searchPrev() {
        if (!PA.sourceView._srcSearchMatches.length) return;
        var total = PA.sourceView._srcSearchMatches.length;
        PA.sourceView._activateSrcMatch((PA.sourceView._srcSearchIdx - 1 + total) % total);
    },
    searchKeydown(e) {
        if (e.key === 'Enter') { e.shiftKey ? PA.sourceView.searchPrev() : PA.sourceView.searchNext(); e.preventDefault(); }
        if (e.key === 'Escape') { PA.sourceView.closeSearch(); e.preventDefault(); }
    },
    _activateSrcMatch(idx) {
        PA.sourceView._srcSearchMatches.forEach(function(el) { el.classList.remove('cm-search-active'); });
        PA.sourceView._srcSearchIdx = idx;
        var el = PA.sourceView._srcSearchMatches[idx];
        if (el) {
            el.classList.add('cm-search-active');
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
        var count = document.getElementById('srcSearchCount');
        var total = PA.sourceView._srcSearchTotalCount || PA.sourceView._srcSearchMatches.length;
        if (count) count.textContent = (idx + 1) + '/' + total;
    },
    _clearSrcHighlights() {
        document.querySelectorAll('#srcContainer .cm-search-match, #srcContainer .cm-search-active').forEach(function(el) {
            el.classList.remove('cm-search-match', 'cm-search-active');
        });
    },

    loadSidebar: async function(procId) {
        var sidebar = document.getElementById('srcTabSidebar');
        if (!sidebar) return;
        if (!procId) { sidebar.style.display = 'none'; return; }
        sidebar.style.display = '';

        var detail = null;
        var node = PA.sourceView._findNodeInfo(procId);
        if (node && node.nodeId) {
            try { detail = await PA.api.getNodeDetail(node.nodeId.replace(/^\$/, '') + '.json'); } catch(e) {}
        }
        if (!detail && PA.currentDetail) detail = PA.currentDetail;
        if (!detail) detail = {};

        var params = detail.parameters || [];
        var paramEl = document.getElementById('stParamList');
        var paramCount = document.getElementById('stParamCount');
        if (paramCount) paramCount.textContent = params.length || '';
        if (paramEl) {
            var ph = '';
            for (var i = 0; i < params.length; i++) {
                var p = params[i];
                ph += '<div class="cm-sb-item" data-tip="Click to highlight line" onclick="PA.sourceView.highlightLineInView(' + (p.line || 0) + ')">';
                ph += '<span class="cm-sb-mode">' + PA.esc(p.direction || 'IN') + '</span>';
                ph += '<span class="cm-sb-field">' + PA.esc(p.name || '') + '</span>';
                ph += '<span class="cm-sb-type">' + PA.esc(p.dataType || '') + '</span>';
                if (p.line) ph += '<span class="cm-sb-line">:' + p.line + '</span>';
                ph += '</div>';
            }
            paramEl.innerHTML = ph || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var vars = detail.variables || {};
        var allVars = [];
        if (vars.plain) allVars = allVars.concat(vars.plain);
        if (vars.typeRef) allVars = allVars.concat(vars.typeRef);
        if (vars.rowtypeRef) allVars = allVars.concat(vars.rowtypeRef);
        if (vars.collection) allVars = allVars.concat(vars.collection);
        var varEl = document.getElementById('stVarList');
        var varCount = document.getElementById('stVarCount');
        if (varCount) varCount.textContent = allVars.length || (vars.total || '');
        if (varEl) {
            var vh = '';
            for (var i = 0; i < allVars.length; i++) {
                var v = allVars[i];
                vh += '<div class="cm-sb-item" data-tip="Click to highlight line" onclick="PA.sourceView.highlightLineInView(' + (v.line || 0) + ')">';
                vh += '<span class="cm-sb-field">' + PA.esc(v.name || '') + '</span>';
                vh += '<span class="cm-sb-type">' + PA.esc(v.dataType || '') + '</span>';
                if (v.constant) vh += '<span class="cm-sb-type" style="color:var(--purple)">CONST</span>';
                if (v.line) vh += '<span class="cm-sb-line">:' + v.line + '</span>';
                vh += '</div>';
            }
            varEl.innerHTML = vh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var tables = detail.tables || detail.directTables || [];
        var tableEl = document.getElementById('stTableList');
        var tableCount = document.getElementById('stTableCount');
        if (tableCount) tableCount.textContent = tables.length || '';
        if (tableEl) {
            var th = '';
            for (var i = 0; i < tables.length; i++) {
                var t = tables[i];
                var ops = t.operations ? Object.keys(t.operations) : [];
                th += '<div class="cm-sb-item">';
                // name opens the table detail; the :line badges jump to each usage line in this source
                th += '<span class="cm-sb-field" style="cursor:pointer" data-tip="View table details" onclick="event.stopPropagation(); PA.tableDetail.open(\'' + PA.escJs(t.name || '') + '\', \'' + PA.escJs(t.schema || '') + '\')">' + PA.esc((t.schema ? t.schema + '.' : '') + (t.name || '')) + '</span>';
                for (var j = 0; j < ops.length; j++) {
                    var opLines = (t.operations[ops[j]] || []).filter(function(v, kk, a) { return v && a.indexOf(v) === kk; });
                    if (opLines.length === 0) {
                        th += '<span class="cm-sb-op ' + ops[j].charAt(0) + '">' + ops[j].charAt(0) + '</span>';
                    } else {
                        for (var k = 0; k < opLines.length; k++) {
                            th += '<span class="cm-sb-line" data-tip="Jump to ' + ops[j] + ' at line ' + opLines[k] + '" onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + opLines[k] + ')">' + ops[j].charAt(0) + ':' + opLines[k] + '</span>';
                        }
                    }
                }
                th += '</div>';
            }
            tableEl.innerHTML = th || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var callTree = null;
        try { callTree = await PA.api.getCallTree(procId); } catch(e) {}
        var calls = (callTree && callTree.children) ? callTree.children : [];
        var callsEl = document.getElementById('stCallsList');
        var callsCount = document.getElementById('stCallsCount');
        if (callsCount) callsCount.textContent = calls.length || '';
        if (callsEl) {
            var ch = '';
            for (var i = 0; i < calls.length; i++) {
                var c = calls[i];
                var cType = c.callType || '';
                // Two actions per row: the NAME opens the callee's source; the :line
                // badge jumps to the call site in THIS source.
                ch += '<div class="cm-sb-item">';
                if (cType === 'INTERNAL') ch += '<span class="cm-sb-badge int">INT</span>';
                else if (cType) ch += '<span class="cm-sb-badge ext">' + PA.esc(cType) + '</span>';
                ch += '<span class="cm-sb-field" style="cursor:pointer" data-tip="Open this procedure" onclick="event.stopPropagation(); PA.sourceView.openModal(\'' + PA.escJs(c.id || '') + '\')">' + PA.esc(c.name || c.id || '') + '</span>';
                if (c.callLineNumber) ch += '<span class="cm-sb-line" style="cursor:pointer" data-tip="Jump to call site (line ' + c.callLineNumber + ')" onclick="event.stopPropagation(); PA.sourceModal._jumpToLine(' + c.callLineNumber + ')">:' + c.callLineNumber + '</span>';
                ch += '</div>';
            }
            callsEl.innerHTML = ch || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var callers = [];
        var callersUnknown = false;
        try {
            var callerData = await PA.api.getCallerTree(procId);
            // A failed lookup must not read as "nothing calls this". That claim is what someone
            // relies on to justify deleting a procedure.
            callersUnknown = PA.failed && PA.failed(callerData);
            callers = Array.isArray(callerData) ? callerData : (callerData && callerData.children ? callerData.children : []);
        } catch(e) { callersUnknown = true; }
        var calledByEl = document.getElementById('stCalledByList');
        var calledByCount = document.getElementById('stCalledByCount');
        if (calledByCount) calledByCount.textContent = callers.length || '';
        if (calledByEl) {
            var bh = '';
            for (var i = 0; i < callers.length; i++) {
                var cb = callers[i];
                // Two actions: NAME opens the caller's source; :line opens the caller
                // at the exact call site (the call lives in the caller's file).
                var _bLine = cb.callLineNumber || cb.lineNumber || 0;
                var _bId = PA.escJs(cb.id || cb.nodeId || '');
                bh += '<div class="cm-sb-item">';
                bh += '<span class="cm-sb-badge" style="color:var(--text-muted)">&larr;</span>';
                bh += '<span class="cm-sb-field" style="cursor:pointer" data-tip="Open caller" onclick="event.stopPropagation(); PA.sourceView.openModal(\'' + _bId + '\')">' + PA.esc(cb.name || cb.nodeName || cb.id || '') + '</span>';
                if (_bLine) bh += '<span class="cm-sb-line" style="cursor:pointer" data-tip="Open caller at call site (line ' + _bLine + ')" onclick="event.stopPropagation(); PA.sourceView._openAtCaller(\'' + _bId + '\', ' + _bLine + ')">:' + _bLine + '</span>';
                bh += '</div>';
            }
            calledByEl.innerHTML = bh || (callersUnknown
                ? '<div class="cm-sb-item" style="color:var(--badge-red,#c0392b);cursor:default">Could not load callers — unknown, not none</div>'
                : '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>');
        }
    },

    toggleSideSection: function(name) {
        var list = document.getElementById(name + 'List');
        if (list) list.classList.toggle('collapsed');
    },

    filterSideSection: function(name, query) {
        var list = document.getElementById(name + 'List');
        if (!list) return;
        var items = list.querySelectorAll('.cm-sb-item');
        var q = SearchQuery.norm(query);
        var shown = 0;
        for (var i = 0; i < items.length; i++) {
            var text = items[i].textContent.toLowerCase();
            if (!q || text.indexOf(q) !== -1) {
                items[i].classList.remove('filtered-out');
                shown++;
            } else {
                items[i].classList.add('filtered-out');
            }
        }
        var countEl = document.getElementById(name + 'Count');
        if (countEl && q) countEl.textContent = shown + '/' + items.length;
        else if (countEl) countEl.textContent = items.length || '';
    },

    renderDetailBar() {
        var bar = document.getElementById('srcDetailBar');
        if (!bar) return;
        var detail = PA.currentDetail;
        if (!detail) { bar.style.display = 'none'; return; }
        bar.style.display = '';
        var counts = detail.counts || {};
        var h = '';
        var objType = detail.objectType || '';
        if (objType) {
            var cls = objType === 'FUNCTION' ? 'F' : objType === 'TRIGGER' ? 'T' : 'P';
            h += '<span class="lp-icon ' + cls + '" style="display:inline-flex;width:16px;height:16px;font-size:8px">' + cls + '</span>';
        }
        var schema = detail.schema || '';
        var name = detail.name || '';
        h += '<span style="font-weight:600;color:var(--accent);font-family:var(--font-mono);font-size:11px">' + PA.esc(schema ? schema + '.' + name : name) + '</span>';
        if (detail.packageName) h += '<span class="src-detail-stat"><small>PKG</small>' + PA.esc(detail.packageName) + '</span>';
        h += '<span class="src-detail-stat accent">' + (detail.linesOfCode || 0) + '<small>LOC</small></span>';
        var tblCount = counts.tables || (detail.tables ? detail.tables.length : 0);
        h += '<span class="src-detail-stat teal">' + tblCount + '<small>tables</small></span>';
        var callCount = (counts.internalCalls || 0) + (counts.externalCalls || 0);
        h += '<span class="src-detail-stat blue">' + callCount + '<small>calls</small></span>';
        var cursorCount = counts.cursors || (detail.cursors ? detail.cursors.length : 0);
        if (cursorCount > 0) h += '<span class="src-detail-stat purple">' + cursorCount + '<small>cursors</small></span>';
        var paramCount = counts.parameters || (detail.parameters ? detail.parameters.length : 0);
        if (paramCount > 0) h += '<span class="src-detail-stat">' + paramCount + '<small>params</small></span>';
        var varCount = detail.variables ? (detail.variables.total || 0) : (counts.variables || 0);
        if (varCount > 0) h += '<span class="src-detail-stat">' + varCount + '<small>vars</small></span>';
        if (detail.readable === false) h += '<span class="src-detail-stat orange">&#128274; ENCRYPTED</span>';
        if (detail.depth != null) h += '<span class="src-detail-stat"><small>D</small>' + detail.depth + '</span>';
        h += '<button class="btn btn-sm" style="margin-left:auto;font-size:10px" onclick="PA.sourceView.toggleSearch()" title="Search source (Ctrl+F)">Find</button>';
        bar.innerHTML = h;
    },

    async openModal(procId) {
        if (!procId) return;
        await PA.nodeIndex.ensureLoaded();
        if (PA.sourceView._isEncrypted(procId)) { PA.toast('Source is encrypted/wrapped — cannot view', 'warn'); return; }
        const node = PA.sourceView._findNodeInfo(procId);
        let sf = (node && node.sourceFile) || '';
        // Only fall back to the current detail's source when it is the SAME proc — never show a
        // different procedure's body under this node's title (e.g. a variable/collection
        // reference like l_cids(i) has no source of its own).
        if (!sf && PA.currentDetail && PA.currentDetail.sourceFile) {
            const cdId = (PA.currentDetail.nodeId || '').toUpperCase();
            if (cdId && cdId === (procId || '').toUpperCase()) sf = PA.currentDetail.sourceFile;
        }
        if (!sf) {
            PA.toast('No source for "' + ((node && node.name) || procId) + '" — it may be a variable or collection reference, not a procedure.', 'warn');
            return;
        }

        // Blank the modal BEFORE awaiting. This path is hit when navigating between procedures
        // from inside the modal (calls/callers/breadcrumb), where the modal is already open and
        // would otherwise keep showing the previous procedure until the fetch returned.
        const _tok = PA.sourceModal._beginLoad((node && node.name) || procId);
        const _modalEarly = document.getElementById('srcModal');
        if (_modalEarly) {
            _modalEarly.style.display = '';
            if (PA.modalStack) _modalEarly.style.zIndex = PA.modalStack.nextZ();
        }

        let data = await PA.api.getSource(sf);
        if (!PA.sourceModal._isCurrent(_tok)) return;   // superseded by a newer open
        if (!data || !data.content) { PA.toast('Source not available for: ' + sf, 'error'); return; }

        const startLine = (node && node.lineStart) || 0;
        const endLine = (node && node.lineEnd) || 0;
        const name = (node && node.name) || procId;

        // For package subprograms, load the .pkb file since lineStart/lineEnd refer to it
        if (startLine > 0 && endLine > 0) {
            const pkbFile = PA.sourceView._resolvePackageSource(sf);
            if (pkbFile && pkbFile !== sf) {
                const pkbData = await PA.api.getSource(pkbFile);
                if (!PA.sourceModal._isCurrent(_tok)) return;   // superseded during the .pkb fetch
                if (pkbData && pkbData.content) data = pkbData;
            }
        }
        const content = data.content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

        PA.sourceModal._history.push({ procId: procId });
        PA.sourceModal._historyFwd = [];
        PA.sourceModal._updateNav();

        const modal = document.getElementById('srcModal');
        const title = document.getElementById('srcModalTitle');
        const body = document.getElementById('srcModalBody');

        // startLine=1 for a package subprogram means proc was not locatable (truncated source)
        const isFocusableModal = startLine > 1 && endLine > startLine;
        title.textContent = name + (isFocusableModal ? ' (Lines ' + startLine + '-' + endLine + ')' : '');

        PA.sourceModal._renderInfoBar(node, procId);
        PA.sourceModal.closeSearch();

        const lines = content.split('\n');
        const from = isFocusableModal ? Math.max(0, startLine - 1) : 0;
        const to = isFocusableModal ? Math.min(lines.length, endLine) : lines.length;

        PA.sourceView._renderModalBodyChunked(body, lines, from, to, isFocusableModal ? startLine : 0);
        modal.style.display = '';
        if (PA.modalStack) modal.style.zIndex = PA.modalStack.nextZ();

        PA.sourceModal._content = lines.slice(from, to).join('\n');

        PA.sourceModal._renderBreadcrumb();

        // Load node detail to build call-line map before sidebar populates
        PA.sourceModal._loadSidebar(procId).then(function(detail) {
            if (!PA.sourceModal._isCurrent(_tok)) return;   // stale sidebar response
            if (detail && detail.calls) {
                PA.sourceView._callLineMap = PA.sourceView._buildCallLineMap(detail.calls);
            }
            // Transaction gutter markers (task: transaction capture reliability): the lines this
            // proc COMMITs / ROLLBACKs / SAVEPOINTs get a badge in the gutter — the sidebar's
            // "Transaction Scope" list already links them; now the source shows them in place.
            PA.sourceView._txnLineMap = {};
            var ts = detail && detail.transactionScope;
            if (ts && ts.statements) {
                ts.statements.forEach(function(t) {
                    if (t && t.line) PA.sourceView._txnLineMap[t.line] = t.type;
                });
            }
        });
    },

    highlightLine(line, ln) {
        let s = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        if (s.trimStart().startsWith('--')) return '<span class="cmt">' + s + '</span>';
        s = s.replace(/'([^']*)'/g, '<span class="str">\'$1\'</span>');
        s = s.replace(/\b(\d+\.?\d*)\b/g, '<span class="num">$1</span>');
        s = s.replace(/\b([A-Z_]+)\b/g, (m) => PLSQL_KEYWORDS.has(m) ? '<span class="kw">' + m + '</span>' : m);
        // Inline clickable call links: if this line has a known call, wrap the proc name
        if (ln && PA.sourceView._callLineMap && PA.sourceView._callLineMap[ln]) {
            var entry = PA.sourceView._callLineMap[ln];
            var objName = entry.objName;
            if (objName) {
                // Match the bare proc name as a word, case-insensitive (source may use mixed case)
                var re = new RegExp('\\b(' + objName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'i');
                s = s.replace(re, '<span class="src-call-link" data-tip="Open ' + PA.escAttr(entry.label) + '" onclick="PA.sourceView.openModal(\'' + PA.escJs(entry.nodeId) + '\')" >$1</span>');
            }
        }
        // Transaction gutter badge (task: transaction capture reliability) — the line COMMITs /
        // ROLLBACKs / SAVEPOINTs: mark it in place, matching the sidebar's Transaction Scope list.
        if (ln && PA.sourceView._txnLineMap && PA.sourceView._txnLineMap[ln]) {
            var tType = PA.sourceView._txnLineMap[ln];
            s = '<span class="src-txn-mark txn-' + tType.toLowerCase() + '" data-tip="' + PA.escAttr(tType) + '"></span>' + s;
        }
        return s;
    }
};
