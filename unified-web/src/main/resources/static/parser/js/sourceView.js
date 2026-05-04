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
    'PIPE', 'CASE', 'LIMIT', 'SAVE', 'EXCEPTIONS', 'EDITIONABLE', 'NONEDITIONABLE'
]);

PA.sourceModal = {
    _content: '',
    _history: [],
    _historyFwd: [],
    _searchMatches: [],
    _searchIdx: -1,

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        document.getElementById('srcModal').style.display = 'none';
        PA.sourceModal.closeSearch();
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
        var panel = document.querySelector('.src-modal');
        if (panel) panel.classList.toggle('cm-fullscreen');
    },

    toggleSearch() {
        var bar = document.getElementById('cmSearchBar');
        if (bar.style.display === 'none') {
            bar.style.display = '';
            document.getElementById('cmSearchInput').focus();
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
        var q = query.toLowerCase();
        var lines = document.querySelectorAll('#srcModalBody .src-line');
        var matches = [];
        lines.forEach(function(line) {
            var codeEl = line.querySelector('.src-code');
            if (!codeEl) return;
            var text = codeEl.textContent.toLowerCase();
            if (text.includes(q)) {
                matches.push(line);
                line.classList.add('cm-search-match');
            }
        });
        PA.sourceModal._searchMatches = matches;
        if (count) count.textContent = matches.length;
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
        if (count) count.textContent = (idx + 1) + '/' + PA.sourceModal._searchMatches.length;
    },
    _clearSearchHighlights() {
        document.querySelectorAll('#srcModalBody .cm-search-match, #srcModalBody .cm-search-active').forEach(function(el) {
            el.classList.remove('cm-search-match', 'cm-search-active');
        });
    },

    async _loadSidebar(procId) {
        var searchIds = ['cmParamSearch', 'cmVarSearch', 'cmTableSearch', 'cmCursorSearch', 'cmCallsSearch', 'cmCalledBySearch', 'cmClaudeSearch'];
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
                th += '<div class="cm-sb-item" data-tip="View table details" onclick="PA.tableDetail.open(\'' + PA.escJs(t.name || '') + '\', \'' + PA.escJs(t.schema || '') + '\')">';
                th += '<span class="cm-sb-field">' + PA.esc((t.schema ? t.schema + '.' : '') + (t.name || '')) + '</span>';
                for (var j = 0; j < ops.length; j++) {
                    th += '<span class="cm-sb-op ' + ops[j].charAt(0) + '">' + ops[j].charAt(0) + '</span>';
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
                ch += '<div class="cm-sb-item" data-tip="Navigate to this call" onclick="PA.sourceView.openModal(\'' + PA.escJs(c.id || '') + '\')">';
                if (cType === 'INTERNAL') ch += '<span class="cm-sb-badge int">INT</span>';
                else if (cType) ch += '<span class="cm-sb-badge ext">' + PA.esc(cType) + '</span>';
                ch += '<span class="cm-sb-field">' + PA.esc(c.name || c.id || '') + '</span>';
                if (c.callLineNumber) ch += '<span class="cm-sb-line">:' + c.callLineNumber + '</span>';
                ch += '</div>';
            }
            callsEl.innerHTML = ch || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var callers = [];
        try {
            var callerData = await PA.api.getCallerTree(procId);
            callers = Array.isArray(callerData) ? callerData : (callerData && callerData.children ? callerData.children : []);
        } catch(e) {}
        var calledByEl = document.getElementById('cmCalledByList');
        var calledByCount = document.getElementById('cmCalledByCount');
        if (calledByCount) calledByCount.textContent = callers.length || '';
        if (calledByEl) {
            var bh = '';
            for (var i = 0; i < callers.length; i++) {
                var cb = callers[i];
                bh += '<div class="cm-sb-item" data-tip="Navigate to caller" onclick="PA.sourceView.openModal(\'' + PA.escJs(cb.id || cb.nodeId || '') + '\')">';
                bh += '<span class="cm-sb-badge" style="color:var(--text-muted)">&larr;</span>';
                bh += '<span class="cm-sb-field">' + PA.esc(cb.name || cb.nodeName || cb.id || '') + '</span>';
                bh += '</div>';
            }
            calledByEl.innerHTML = bh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
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
        var startLine = node.lineStart || node.startLine || 0;
        var endLine = node.lineEnd || node.endLine || 0;
        var lines = (startLine && endLine) ? (endLine - startLine + 1) : 0;
        var h = '';
        if (objType) h += '<span class="cm-info-type ' + PA.esc(objType) + '">' + PA.esc(objType) + '</span>';
        h += '<span class="cm-info-name">' + PA.esc(name) + '</span>';
        if (schema || pkg) {
            var fqn = [schema, pkg, name].filter(Boolean).join('.');
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
        var q = (query || '').toLowerCase();
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

    /**
     * Open source in the Source tab.
     * If PA.currentDetail has lineStart/lineEnd, shows focused proc view + package-level vars.
     * @param {string} sourceFile - the source filename (e.g. "MY_PKG.pkb")
     * @param {number} [lineNumber] - optional line to scroll to and highlight
     */
    async open(sourceFile, lineNumber) {
        if (PA.currentDetail && PA.currentDetail.readable === false) {
            document.getElementById('srcPath').textContent = sourceFile || 'Unknown';
            document.getElementById('srcContainer').innerHTML =
                '<div class="empty-msg" style="color:var(--badge-orange)">&#128274; Source is encrypted/wrapped and cannot be displayed</div>';
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
                if (pkbData && pkbData.content) {
                    content = pkbData.content;
                    resolvedFile = pkbFile;
                }
            }
        }

        PA.sourceView._currentSourceFile = resolvedFile;
        PA.sourceView._currentContent = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

        const displayName = data.objectName || sourceFile;

        if (startLine > 0 && endLine > 0 && endLine > startLine) {
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
            // Full file view
            document.getElementById('srcPath').innerHTML =
                '<span>' + PA.esc(displayName) + '</span>' +
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

    async openModalAtLine(sourceFile, lineNumber) {
        if (!sourceFile) return;
        let data = await PA.api.getSource(sourceFile);
        if (!data || !data.content) { PA.toast('Source not available: ' + sourceFile, 'error'); return; }
        const content = data.content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        const lines = content.split('\n');
        const name = sourceFile.replace(/\.(pkb|pks|prc|fnc|sql)$/i, '');

        PA.sourceModal._history.push({ procId: name, sourceFile: sourceFile, line: lineNumber });
        PA.sourceModal._historyFwd = [];
        PA.sourceModal._updateNav();

        const modal = document.getElementById('srcModal');
        const title = document.getElementById('srcModalTitle');
        const body = document.getElementById('srcModalBody');
        title.textContent = name + (lineNumber > 0 ? ' (Line ' + lineNumber + ')' : '');
        PA.sourceModal._renderInfoBar(null, name);
        PA.sourceModal.closeSearch();

        PA.sourceView._renderModalBodyChunked(body, lines, 0, lines.length, lineNumber);
        modal.style.display = '';

        PA.sourceModal._content = content;
        PA.sourceModal._renderBreadcrumb();
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
                const highlighted = PA.sourceView.highlightLine(lines[i]);
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
            const highlighted = PA.sourceView.highlightLine(lines[i]);
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
                        const highlighted = PA.sourceView.highlightLine(lines[i]);
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
                        const highlighted = PA.sourceView.highlightLine(lines[i]);
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
                const highlighted = PA.sourceView.highlightLine(lines[i]);
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
                const highlighted = PA.sourceView.highlightLine(lines[i]);
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
                            const highlighted = PA.sourceView.highlightLine(lines[i]);
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
                const highlighted = PA.sourceView.highlightLine(lines[i]);
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
            const highlighted = PA.sourceView.highlightLine(lines[i]);
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
            const highlighted = PA.sourceView.highlightLine(state.lines[i]);
            html += '<div class="src-line' + (isHL ? ' highlight' : '') + '" id="' + state.prefix + ln + '" style="height:' + state.lineHeight + 'px">';
            html += '<span class="src-ln clickable' + (isHL ? ' active' : '') + '" onclick="PA.sourceView.highlightLineInView(' + ln + ')">' + ln + '</span>';
            html += '<span class="src-code">' + highlighted + '</span>';
            html += '</div>';
        }
        return html;
    },

    _findNodeInfo(procId) {
        if (!procId) return null;
        const nodes = (PA.analysisData && PA.analysisData.nodes) || [];
        let found = nodes.find(n => n.nodeId === procId || n.id === procId || n.name === procId);
        if (found) return found;
        const up = procId.toUpperCase();
        found = nodes.find(n => (n.nodeId || '').toUpperCase() === up || (n.id || '').toUpperCase() === up || (n.name || '').toUpperCase() === up);
        if (found) return found;
        if (PA.callTrace && PA.callTrace.treeData) {
            const treeNode = PA.sourceView._searchTree(PA.callTrace.treeData, procId);
            if (treeNode) return { nodeId: treeNode.id, name: treeNode.name, sourceFile: treeNode.sourceFile, lineStart: treeNode.startLine, lineEnd: treeNode.endLine, schema: treeNode.schemaName, packageName: treeNode.packageName };
        }
        return null;
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
        if (parts.length === 3) return parts[0] + '.' + parts[1] + '.pkb';
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
            document.getElementById('srcSearchInput').focus();
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
        var input = document.getElementById('srcSearchInput');
        if (input) input.value = '';
        var count = document.getElementById('srcSearchCount');
        if (count) count.textContent = '0';
    },
    search(query) {
        PA.sourceView._clearSrcHighlights();
        PA.sourceView._srcSearchMatches = [];
        PA.sourceView._srcSearchIdx = -1;
        var count = document.getElementById('srcSearchCount');
        if (!query || query.length < 2) { if (count) count.textContent = '0'; return; }
        var q = query.toLowerCase();
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
        if (count) count.textContent = matches.length;
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
        if (count) count.textContent = (idx + 1) + '/' + PA.sourceView._srcSearchMatches.length;
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
                th += '<div class="cm-sb-item" data-tip="View table details" onclick="PA.tableDetail.open(\'' + PA.escJs(t.name || '') + '\', \'' + PA.escJs(t.schema || '') + '\')">';
                th += '<span class="cm-sb-field">' + PA.esc((t.schema ? t.schema + '.' : '') + (t.name || '')) + '</span>';
                for (var j = 0; j < ops.length; j++) {
                    th += '<span class="cm-sb-op ' + ops[j].charAt(0) + '">' + ops[j].charAt(0) + '</span>';
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
                ch += '<div class="cm-sb-item" data-tip="Navigate to this call" onclick="PA.sourceView.openModal(\'' + PA.escJs(c.id || '') + '\')">';
                if (cType === 'INTERNAL') ch += '<span class="cm-sb-badge int">INT</span>';
                else if (cType) ch += '<span class="cm-sb-badge ext">' + PA.esc(cType) + '</span>';
                ch += '<span class="cm-sb-field">' + PA.esc(c.name || c.id || '') + '</span>';
                if (c.callLineNumber) ch += '<span class="cm-sb-line">:' + c.callLineNumber + '</span>';
                ch += '</div>';
            }
            callsEl.innerHTML = ch || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
        }

        var callers = [];
        try {
            var callerData = await PA.api.getCallerTree(procId);
            callers = Array.isArray(callerData) ? callerData : (callerData && callerData.children ? callerData.children : []);
        } catch(e) {}
        var calledByEl = document.getElementById('stCalledByList');
        var calledByCount = document.getElementById('stCalledByCount');
        if (calledByCount) calledByCount.textContent = callers.length || '';
        if (calledByEl) {
            var bh = '';
            for (var i = 0; i < callers.length; i++) {
                var cb = callers[i];
                bh += '<div class="cm-sb-item" data-tip="Navigate to caller" onclick="PA.sourceView.openModal(\'' + PA.escJs(cb.id || cb.nodeId || '') + '\')">';
                bh += '<span class="cm-sb-badge" style="color:var(--text-muted)">&larr;</span>';
                bh += '<span class="cm-sb-field">' + PA.esc(cb.name || cb.nodeName || cb.id || '') + '</span>';
                bh += '</div>';
            }
            calledByEl.innerHTML = bh || '<div class="cm-sb-item" style="color:var(--text-muted);cursor:default">None</div>';
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
        var q = (query || '').toLowerCase();
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
        if (PA.sourceView._isEncrypted(procId)) { PA.toast('Source is encrypted/wrapped — cannot view', 'warn'); return; }
        const node = PA.sourceView._findNodeInfo(procId);
        const sf = (node && node.sourceFile) || (PA.currentDetail && PA.currentDetail.sourceFile) || '';
        if (!sf) { PA.toast('No source file for: ' + (procId || ''), 'error'); return; }

        let data = await PA.api.getSource(sf);
        if (!data || !data.content) { PA.toast('Source not available for: ' + sf, 'error'); return; }

        const startLine = (node && node.lineStart) || 0;
        const endLine = (node && node.lineEnd) || 0;
        const name = (node && node.name) || procId;

        // For package subprograms, load the .pkb file since lineStart/lineEnd refer to it
        if (startLine > 0 && endLine > 0) {
            const pkbFile = PA.sourceView._resolvePackageSource(sf);
            if (pkbFile && pkbFile !== sf) {
                const pkbData = await PA.api.getSource(pkbFile);
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

        title.textContent = name + (startLine > 0 ? ' (Lines ' + startLine + '-' + endLine + ')' : '');

        PA.sourceModal._renderInfoBar(node, procId);
        PA.sourceModal.closeSearch();

        const lines = content.split('\n');
        const from = startLine > 0 ? Math.max(0, startLine - 1) : 0;
        const to = endLine > 0 ? Math.min(lines.length, endLine) : lines.length;

        PA.sourceView._renderModalBodyChunked(body, lines, from, to, startLine || 0);
        modal.style.display = '';

        PA.sourceModal._content = lines.slice(from, to).join('\n');

        PA.sourceModal._renderBreadcrumb();
        PA.sourceModal._loadSidebar(procId);
    },

    highlightLine(line) {
        let s = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        if (s.trimStart().startsWith('--')) return '<span class="cmt">' + s + '</span>';
        s = s.replace(/'([^']*)'/g, '<span class="str">\'$1\'</span>');
        s = s.replace(/\b(\d+\.?\d*)\b/g, '<span class="num">$1</span>');
        s = s.replace(/\b([A-Z_]+)\b/g, (m) => PLSQL_KEYWORDS.has(m) ? '<span class="kw">' + m + '</span>' : m);
        return s;
    }
};
