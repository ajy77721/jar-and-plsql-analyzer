window.PA = window.PA || {};

/**
 * Table Detail modal.
 *
 * The Joins tab is server-paged (PLAN-08 §4.3 step 10, Q59). It used to be filled by a
 * `Promise.all` over every node-detail document in the current call tree — 64 documents and
 * 3.6 MB on the measured ERP_USP_RUN_CYCLE analysis — to produce a couple of hundred rows. That
 * scan now runs in the endpoint and the tab fetches one page (17 KB for fifty rows, measured).
 *
 * A second fan-out of the same documents built `_columns` (%TYPE / %ROWTYPE references). Nothing
 * in the UI ever rendered it, so it is gone rather than converted; the data is served by
 * `/table-columns` if a tab is ever built for it.
 *
 * The other tabs are unchanged and still slice in the browser, deliberately. Accesses, Triggers
 * and the three database-metadata tabs read from documents the screen has already fetched for
 * other reasons (the tables index, the analysis node list), so paging them would add a request
 * per page turn and save nothing — the bytes on this screen were in the fan-outs, not in the row
 * counts.
 */
PA.tableDetail = {
    _data: null,
    _tableName: '',
    _schema: '',
    _activeOps: new Set(),
    _allAccess: [],
    _columns: [],
    _triggers: [],
    /** Header/tab counts for the Joins tab: `{total, scopedTotal, columnValues}` or null. */
    _joinsSummary: null,
    _currentTab: 'accesses',
    _metadata: null,
    _metadataLoading: false,
    _claudeResult: null,
    _claudeResultAnalysis: null,
    _openGen: 0, // incremented on every open() to cancel stale async callbacks

    // UI-024 (§163) — `ensureLoaded()` awaited once here, before any tab is first rendered. Every
    // render path this modal has (`_renderAccesses`, `_renderClaudeComparison`, tab switches,
    // filters, pager callbacks) runs synchronously per-row inside a hot render loop — the same
    // shape `callGraph.js`'s per-node risk badge already established can't take a per-lookup await
    // — but all of them only ever run *after* `open()` has already populated the modal, so one
    // preload here covers every one of them without needing any of those loops to become async.
    async open(tableName, schema, initialTab) {
        if (!tableName) return;
        await PA.nodeIndex.ensureLoaded();
        var gen = ++PA.tableDetail._openGen;

        PA.tableDetail._tableName = tableName;
        PA.tableDetail._schema = schema || '';
        PA.tableDetail._activeOps.clear();
        PA.tableDetail._metadata = null;
        PA.tableDetail._metadataLoading = false;
        PA.tableDetail._currentTab = initialTab || 'accesses';
        PA.tableDetail._joinsSummary = null;

        // New table opened — reset paged sub-lists back to page 1
        PA.pager.reset('td-claude-matched');
        PA.pager.reset('td-claude-new');

        var modal = document.getElementById('tableDetailModal');
        var title = document.getElementById('tdModalTitle');
        var infoBar = document.getElementById('tdInfoBar');
        var body = document.getElementById('tdModalBody');

        title.textContent = (schema ? schema + '.' : '') + tableName;
        body.innerHTML = '<div class="empty-msg">Loading...</div>';
        infoBar.innerHTML = '';
        modal.style.display = '';
        if (PA.modalStack) modal.style.zIndex = PA.modalStack.nextZ();

        var tableData = await PA.tableDetail._loadTableData(tableName);
        // Stale check: user may have opened a different table while we were fetching
        if (gen !== PA.tableDetail._openGen) return;

        PA.tableDetail._data = tableData;
        PA.tableDetail._allAccess = tableData.accesses || [];
        PA.tableDetail._columns = [];
        PA.tableDetail._triggers = tableData.triggers || [];
        PA.tableDetail._triggersError = tableData.triggersError || null;

        PA.tableDetail._renderInfoBar();
        PA.tableDetail._initPills(tableData.allOperations || [], tableData.isView || false);
        PA.tableDetail._renderTabs();
        PA.tableDetail._render();

        var capturedGen = gen;
        // The joins count in the header and on the tab used to be an array length. It is now the
        // endpoint's count, fetched alongside the modal rather than by reading every node
        // document. Non-blocking: the modal is already on screen, and the count appears when it
        // arrives instead of holding the open.
        PA.tableDetail._loadJoinsSummary().then(function () {
            if (capturedGen !== PA.tableDetail._openGen) return;
            PA.tableDetail._renderInfoBar();
            PA.tableDetail._renderTabs();
            // The Joins tab needs the summary for its column-filter value lists and its count,
            // so if the modal opened straight onto it, redraw now that they exist.
            if (PA.tableDetail._currentTab === 'joins') PA.tableDetail._render();
        });

        // Pre-load metadata from JSON (non-blocking) so tab counts are ready immediately.
        // This hits no live DB — reads from cached analysis JSON.
        PA.tableDetail._ensureMetadata().then(function() {
            if (capturedGen !== PA.tableDetail._openGen) return;
            // If JSON didn't already set isView, check metadata fallback result
            var md = PA.tableDetail._metadata;
            if (md && md.isView && PA.tableDetail._data && !PA.tableDetail._data.isView) {
                PA.tableDetail._data.isView = true;
                // Re-rendered rather than having a VIEW badge spliced into the existing bar: the
                // bar is now redrawn for the joins count anyway, and two ways of updating it
                // disagreed about where the badge goes.
                PA.tableDetail._renderInfoBar();
                PA.tableDetail._applyViewPillRestriction(true);
            }
            PA.tableDetail._renderTabs();
        });
    },

    /**
     * The header bar. Redrawn rather than built once, because the joins count and the VIEW badge
     * both arrive after the modal is already visible.
     */
    _renderInfoBar() {
        var infoBar = document.getElementById('tdInfoBar');
        if (!infoBar) return;
        var d = PA.tableDetail._data || {};
        var ops = d.allOperations || [];
        var isView = !!d.isView;
        var joins = PA.tableDetail._joinsSummary
            ? PA.tableDetail._joinsSummary.scopedTotal : 0;
        infoBar.innerHTML =
            '<span style="font-weight:600;color:var(--teal)">' + PA.esc(PA.tableDetail._tableName) + '</span>' +
            (isView ? '<span class="td-view-badge" style="margin-left:6px;font-size:9px;font-weight:700;padding:1px 6px;border-radius:var(--radius-sm);background:var(--badge-blue-bg);color:var(--badge-blue)">VIEW</span>' : '') +
            (PA.tableDetail._schema ? '<span style="color:var(--text-muted);margin-left:6px">(' + PA.esc(PA.tableDetail._schema) + ')</span>' : '') +
            '<span style="margin-left:12px;color:var(--text-muted)">' + PA.tableDetail._allAccess.length + ' accesses</span>' +
            (joins ? '<span style="margin-left:8px;color:var(--text-muted)">' + joins + ' joins</span>' : '') +
            (PA.tableDetail._triggers.length ? '<span style="margin-left:8px;color:var(--badge-orange)">' + PA.tableDetail._triggers.length + ' triggers</span>' : '') +
            '<span style="margin-left:8px">' + ops.map(function(op) { return '<span class="dh-op-badge ' + op + '" style="font-size:9px">' + op + '</span>'; }).join('') + '</span>';
    },

    /**
     * The request parameters that reproduce the fan-out's node set.
     *
     * The browser scan visited the open procedure's node plus every node in its call tree — which
     * is what `subflow` means server-side — and at a package root it visited every node in the
     * analysis, which is `full`. The Direct/Subtree toggle never applied to this modal, so it is
     * not sent; sending `PA._scope` would silently change what the modal shows.
     */
    _scopeParams() {
        var ctx = PA.context || {};
        if (!ctx.procId || PA._isPackageRoot()) return { scope: 'full' };
        return { scope: 'subflow', procId: PA._currentScopeId() };
    },

    /** A fresh page source over `/table-joins` for the table currently open. */
    _joinsPageSource() {
        var params = PA.tableDetail._scopeParams();
        params.table = PA.tableDetail._tableName;
        return PA.api.facetPageSource('table-joins', params);
    },

    /**
     * The joins counts and the column-filter value lists, from `/table-joins/summary`.
     *
     * Both used to be derived from the whole array. With one page in memory the popovers would
     * offer only the values on that page — a filter that visibly does the wrong thing — so they
     * come from the endpoint or not at all.
     */
    async _loadJoinsSummary() {
        var params = PA.tableDetail._scopeParams();
        params.table = PA.tableDetail._tableName;
        var thisTable = PA.tableDetail._tableName;
        try {
            var sum = await PA.api.facetSummary('table-joins', params);
            if (PA.tableDetail._tableName !== thisTable) return;
            PA.tableDetail._joinsSummary = sum;
        } catch (e) {
            PA.tableDetail._joinsSummary = null;
        }
    },

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        PA.tableDetail._cleanupTf();
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(document.getElementById('tableDetailModal'));
        document.getElementById('tableDetailModal').style.display = 'none';
    },

    // Close the table modal then navigate to a proc — prevents two stacked modals
    closeAndShowProcedure(procId) {
        PA.tableDetail._cleanupTf();
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(document.getElementById('tableDetailModal'));
        document.getElementById('tableDetailModal').style.display = 'none';
        if (procId) PA.showProcedure(procId);
    },

    async _loadTableData(tableName) {
        var upper = tableName.toUpperCase();
        var result = { accesses: [], allOperations: [], joins: [], columns: [], triggers: [] };

        // --- 1. Get table operations data (transformed UI format) ---
        // UI-001: if the grid already loaded PA.tableOps.data (the page the user is looking at),
        // reuse it — it may already contain this row. Otherwise ask for this one table by name
        // (PA.api.getTableDetailByName) rather than falling back to the whole-corpus
        // getTableOperations(), which flat-maps every table's every access line just to find one.
        var tableIndex = PA.tableOps && PA.tableOps.data ? PA.tableOps.data : [];
        var match = null;
        if (Array.isArray(tableIndex)) {
            match = tableIndex.find(function(t) { return (t.tableName || '').toUpperCase() === upper; });
        }
        if (!match) {
            try { match = await PA.api.getTableDetailByName(tableName); } catch(e) { match = null; }
        }

        if (match) {
            result.allOperations = match.operations || [];
            result.accesses = match.accessDetails || [];
        }

        // --- 2. Get raw tables API data (with triggers) ---
        var tablesApiData = null;
        try {
            var raw = PA._cachedTablesIndex || (await PA.api.getTablesIndex());
            if (raw) PA._cachedTablesIndex = raw;
            if (raw && raw.tables) tablesApiData = raw.tables;
        } catch(e) {}

        var rawTblMatch = null;
        if (tablesApiData) {
            rawTblMatch = tablesApiData.find(function(t) { return (t.name || '').toUpperCase() === upper; });
            if (rawTblMatch) {
                // Read VIEW/TABLE type directly from the validated JSON — no live DB call needed
                var resolvedType = (rawTblMatch.dbResolvedType || rawTblMatch.objectType || '').toUpperCase();
                if (resolvedType === 'VIEW' || resolvedType === 'MATERIALIZED VIEW') result.isView = true;

                if (rawTblMatch.usedBy) {
                    if (!result.accesses.length) {
                        var accesses = [];
                        for (var i = 0; i < rawTblMatch.usedBy.length; i++) {
                            var ub = rawTblMatch.usedBy[i];
                            var opKeys = ub.operations ? Object.keys(ub.operations) : [];
                            for (var k = 0; k < opKeys.length; k++) {
                                var lines = ub.operations[opKeys[k]] || [];
                                for (var l = 0; l < lines.length; l++) {
                                    accesses.push({ procedureId: ub.nodeId, procedureName: ub.nodeName, operation: opKeys[k], lineNumber: lines[l] });
                                }
                            }
                        }
                        result.accesses = accesses;
                    }
                    if (!result.allOperations.length) {
                        result.allOperations = rawTblMatch.allOperations || [];
                    }
                }
            }
        }

        // --- 3. Collect triggers from tables API (primary source) ---
        var apiTriggers = (match && match.triggers) || (rawTblMatch && rawTblMatch.triggers) || [];
        var triggerSeen = {};
        for (var ai = 0; ai < apiTriggers.length; ai++) {
            var at = apiTriggers[ai];
            var tKey = (at.name || '').toUpperCase();
            if (triggerSeen[tKey]) continue;
            triggerSeen[tKey] = true;
            result.triggers.push({
                nodeId: at.nodeId || '',
                name: at.name || '',
                schema: at.schema || '',
                timing: at.timing || '',
                event: at.event || '',
                triggerType: at.triggerType || '',
                status: at.status || '',
                source: at.source || '',
                definition: at.definition || '',
                tableOps: at.tableOps || null
            });
        }

        // --- 4. Merge triggers from call tree nodes (parsed source triggers) ---
        // UI-024: was a `PA.nodeIndex.all()` scan filtering for TRIGGER-type nodes whose
        // triggerTable matches this table. The table-name filter (including the "don't blanket-add
        // an unknown-table trigger to everything" guard the comment below used to implement by
        // hand) now happens server-side (TableTriggersService) — only candidates already narrowed
        // to this one table ever reach here. The treeIds/triggerSeen filtering below is untouched:
        // it isn't a scan (a `Set.has` per already-narrowed candidate), and its exact semantics
        // weren't independently re-verified against real data before this change, so it was left
        // exactly as it was rather than folded into the server request too.
        var triggerCandidates = await PA.api.getTriggersForTable(tableName);
        // PA.api returns the PA.FAILED sentinel on a non-ok response instead of throwing; without
        // this check a failed request silently contributes no parsed-source triggers and the tab
        // reads "No triggers found for this table". Record it so _renderTriggers can say so.
        if (PA.failed && PA.failed(triggerCandidates)) {
            result.triggersError = 'trigger request failed';
            triggerCandidates = [];
        }
        var treeIds = PA.context.callTreeNodeIds;
        for (var ni = 0; ni < triggerCandidates.length; ni++) {
            var n = triggerCandidates[ni];
            var nid = (n.nodeId || n.name || '').toUpperCase();
            if (treeIds && !treeIds.has(nid)) continue;
            var nKey = (n.name || n.nodeId || '').toUpperCase();
            if (triggerSeen[nKey]) continue;
            triggerSeen[nKey] = true;
            result.triggers.push({
                nodeId: n.nodeId || '',
                name: n.objectName || n.name || n.nodeId || '',
                schema: n.schema || '',
                timing: n.triggerTiming || '',
                event: n.triggerEvent || '',
                triggerType: '',
                status: '',
                source: 'PARSED',
                definition: '',
                tableOps: null
            });
        }

        // --- 5. Joins: served by /table-joins (PLAN-08 Q59) ---
        // This used to be a `Promise.all` over every node-detail document in the call tree,
        // built here so the Joins tab could slice an array. The endpoint runs the same scan with
        // the same per-procedure de-duplication and hands back a page; see _renderJoins.

        // --- 6. Column references: served by /table-columns (PLAN-08 Q59b) ---
        // The second fan-out over the same documents built these, and no tab ever rendered them.
        result.columns = [];

        // --- 7. Load Claude verification data (non-blocking: attach after render) ---
        var analysisName = (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || '';
        var thisTable = upper; // capture for stale-check in async callback
        if (analysisName) {
            var _attachClaude = function(claudeData) {
                if (!claudeData || !claudeData.tables) return;
                // Guard: only patch if the modal is still showing this same table
                if (PA.tableDetail._tableName.toUpperCase() !== thisTable) return;
                var tbl = claudeData.tables.find(function(t) {
                    var cName = (t.tableName || '').toUpperCase();
                    return cName === thisTable || cName.endsWith('.' + thisTable);
                });
                if (tbl && PA.tableDetail._data) {
                    PA.tableDetail._data.claudeVerification = tbl;
                    PA.tableDetail._renderTabs();
                    if (PA.tableDetail._currentTab === 'accesses' || PA.tableDetail._currentTab === 'claude') {
                        PA.tableDetail._render();
                    }
                }
            };
            if (PA.tableDetail._claudeResultAnalysis === analysisName && PA.tableDetail._claudeResult) {
                _attachClaude(PA.tableDetail._claudeResult);
            } else {
                PA.api.claudeResult(analysisName).then(function(claudeData) {
                    PA.tableDetail._claudeResult = claudeData;
                    PA.tableDetail._claudeResultAnalysis = analysisName;
                    _attachClaude(claudeData);
                }).catch(function() {});
            }
        }

        return result;
    },

    _renderTabs() {
        var toolbar = document.querySelector('#tableDetailModal .td-toolbar');
        if (!toolbar) return;
        var tabs = toolbar.querySelector('.td-tabs');
        if (!tabs) {
            tabs = document.createElement('div');
            tabs.className = 'td-tabs';
            toolbar.insertBefore(tabs, toolbar.firstChild);
        }
        var html = '';
        var cvData = PA.tableDetail._data && PA.tableDetail._data.claudeVerification;
        var md = PA.tableDetail._metadata;
        var colCount = md && md.found ? (md.columns || []).length : 0;
        var idxCount = md && md.found ? PA.tableDetail._groupBy(md.indexes || [], 'indexName').length : 0;
        var conCount = md && md.found ? PA.tableDetail._groupBy(md.constraints || [], 'constraintName').length : 0;
        var isView = (PA.tableDetail._data && PA.tableDetail._data.isView) || (md && md.isView);
        var items = [
            { id: 'accesses', label: 'Accesses', count: PA.tableDetail._allAccess.length },
            { id: 'dbcolumns', label: 'Columns', count: colCount },
            { id: 'dbindexes', label: 'Indexes', count: idxCount },
            { id: 'dbconstraints', label: 'Constraints', count: conCount },
            // The joins count comes from /table-joins/summary, so it is 0 until that lands and
            // the tab badge appears a moment after the modal — the same way the metadata counts
            // above already behave.
            { id: 'joins', label: 'Joins',
              count: PA.tableDetail._joinsSummary ? PA.tableDetail._joinsSummary.scopedTotal : 0 },
            { id: 'triggers', label: 'Triggers', count: PA.tableDetail._triggers.length },
            { id: 'claude', label: 'Claude', count: cvData ? (cvData.claudeVerifications || []).length : 0 }
        ];
        if (isView) items.splice(1, 0, { id: 'definition', label: 'Definition', count: 0 });
        for (var i = 0; i < items.length; i++) {
            var t = items[i];
            var active = PA.tableDetail._currentTab === t.id ? ' active' : '';
            html += '<button class="btn btn-sm td-tab' + active + '" onclick="PA.tableDetail.switchTab(\'' + t.id + '\')">' + t.label;
            if (t.count > 0) html += ' <span class="badge" style="font-size:9px">' + t.count + '</span>';
            html += '</button>';
        }
        tabs.innerHTML = html;
    },

    switchTab(tab) {
        if (tab === 'claude') {
            PA.pager.reset('td-claude-matched');
            PA.pager.reset('td-claude-new');
        }
        PA.tableDetail._currentTab = tab;
        PA.tableDetail._renderTabs();
        PA.tableDetail._render();
    },

    _initPills(ops, isView) {
        var container = document.getElementById('tdOpPills');
        if (!container) return;
        var allOps = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        var writeDml = ['INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        var html = '';
        for (var i = 0; i < allOps.length; i++) {
            var op = allOps[i];
            var has = ops.indexOf(op) >= 0;
            var isWrite = writeDml.indexOf(op) >= 0;
            var disabled = isView && isWrite;
            var style = disabled ? 'opacity:0.2;pointer-events:none;text-decoration:line-through' : (has ? '' : 'opacity:0.3;pointer-events:none');
            var title = disabled ? ' title="Views are read-only — ' + op + ' not applicable"' : '';
            html += '<span class="op-filter-pill ' + op + (has && !disabled ? ' active' : '') + '" data-op="' + op + '" onclick="PA.tableDetail.toggleOp(\'' + op + '\')" style="' + style + '"' + title + '>' + op + '</span>';
        }
        container.innerHTML = html;
    },

    _applyViewPillRestriction(isView) {
        if (!isView) return;
        var writeDml = ['INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        writeDml.forEach(function(op) {
            var pill = document.querySelector('#tdOpPills .op-filter-pill[data-op="' + op + '"]');
            if (pill) {
                pill.style.opacity = '0.2';
                pill.style.pointerEvents = 'none';
                pill.style.textDecoration = 'line-through';
                pill.classList.remove('active');
                pill.title = 'Views are read-only — ' + op + ' not applicable';
            }
        });
    },

    toggleOp(op) {
        if (PA.tableDetail._activeOps.size === 0) {
            PA.tableDetail._activeOps.add(op);
            document.querySelectorAll('#tdOpPills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.op === op);
            });
        } else if (PA.tableDetail._activeOps.has(op)) {
            PA.tableDetail._activeOps.delete(op);
            var pill = document.querySelector('#tdOpPills .op-filter-pill[data-op="' + op + '"]');
            if (pill) pill.classList.remove('active');
            if (PA.tableDetail._activeOps.size === 0) {
                document.querySelectorAll('#tdOpPills .op-filter-pill').forEach(function(p) {
                    if (p.style.opacity !== '0.3') p.classList.add('active');
                });
            }
        } else {
            PA.tableDetail._activeOps.add(op);
            var pill = document.querySelector('#tdOpPills .op-filter-pill[data-op="' + op + '"]');
            if (pill) pill.classList.add('active');
        }
        if (PA.tf.state('tda')) {
            PA.tf.filter('tda');
        } else {
            PA.tableDetail._render();
        }
    },

    filter() {
        var tab = PA.tableDetail._currentTab;
        var q = (document.getElementById('tdSearch') ? document.getElementById('tdSearch').value : '');
        var tfId = { accesses: 'tda', joins: 'tdj', triggers: 'tdt', dbcolumns: 'tdmc', dbindexes: 'tdmi', dbconstraints: 'tdmk' }[tab];
        if (tfId && PA.tf.state(tfId)) {
            PA.tf.setSearch(tfId, q);
            PA.tf.filter(tfId);
        } else if (tab === 'claude') {
            PA.pager.reset('td-claude-matched');
            PA.pager.reset('td-claude-new');
            PA.tableDetail._renderClaudeComparison();
        } else {
            PA.tableDetail._render();
        }
    },

    _cleanupTf: function() {
        var keys = ['tda', 'tdj', 'tdt', 'tdmc', 'tdmi', 'tdmk'];
        for (var i = 0; i < keys.length; i++) {
            delete PA.tf._state[keys[i]];
        }
    },

    _render() {
        PA.tableDetail._cleanupTf();
        var tab = PA.tableDetail._currentTab;
        if (tab === 'accesses') PA.tableDetail._renderAccesses();
        else if (tab === 'dbcolumns') PA.tableDetail._renderDbColumns();
        else if (tab === 'dbindexes') PA.tableDetail._renderDbIndexes();
        else if (tab === 'dbconstraints') PA.tableDetail._renderDbConstraints();
        else if (tab === 'definition') PA.tableDetail._renderDefinition();
        else if (tab === 'joins') PA.tableDetail._renderJoins();
        else if (tab === 'triggers') PA.tableDetail._renderTriggers();
        else if (tab === 'claude') PA.tableDetail._renderClaudeComparison();
    },

    /** Find the best Claude verification match for a static access row. */
    _findClaudeMatch(access, verifications) {
        if (!verifications || !verifications.length) return null;
        var opUpper = (access.operation || '').toUpperCase();
        var procUpper = (access.procedureName || '').toUpperCase();
        var _procMatch = function(a, b) {
            return a === b || a.endsWith('.' + b) || b.endsWith('.' + a);
        };
        var candidates = verifications.filter(function(v) {
            return (v.operation || '').toUpperCase() === opUpper &&
                   _procMatch((v.procedureName || '').toUpperCase(), procUpper);
        });
        if (!candidates.length) return null;
        if (candidates.length === 1) return candidates[0];
        // Multiple matches: prefer the one with the closest line number
        var targetLine = access.lineNumber || 0;
        candidates.sort(function(a, b) {
            return Math.abs((a.lineNumber || 0) - targetLine) - Math.abs((b.lineNumber || 0) - targetLine);
        });
        return candidates[0];
    },

    _renderAccesses() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        var items = PA.tableDetail._allAccess;
        var ops = PA.tableDetail._activeOps;

        // Enrich each access with Claude match (once, not per-render)
        var cv = PA.tableDetail._data ? PA.tableDetail._data.claudeVerification : null;
        var cvVerifications = (cv && cv.claudeVerifications) ? cv.claudeVerifications : [];
        for (var ei = 0; ei < items.length; ei++) {
            items[ei]._cvMatch = cv ? PA.tableDetail._findClaudeMatch(items[ei], cvVerifications) : null;
            var sf = items[ei].sourceFile || '';
            if (!sf && items[ei].procedureId) {
                // Indexed: this sits inside a loop over accesses, so a scan is accesses x nodes
                // — ~19.5M comparisons for one render at the stated target.
                var node = PA.nodeIndex.byId(items[ei].procedureId);
                if (node) sf = node.sourceFile || '';
            }
            items[ei]._sf = sf;
        }

        // Claude summary bar
        var summaryHtml = '';
        if (cv) {
            var cvNewOps = cvVerifications.filter(function(v) { return (v.status || '').toUpperCase() === 'NEW'; });
            var cvConfirmedCount = 0, cvRemovedCount = 0, cvNewCount = cvNewOps.length;
            var pendingCount = 0;
            for (var ci = 0; ci < cvVerifications.length; ci++) {
                var s = (cvVerifications[ci].status || '').toUpperCase();
                if (s === 'CONFIRMED') cvConfirmedCount++;
                else if (s === 'REMOVED') cvRemovedCount++;
                if (!cvVerifications[ci].userDecision) pendingCount++;
            }
            var overallStatus = cv.overallStatus || 'UNKNOWN';
            summaryHtml += '<div class="cv-summary-bar">';
            if (cvConfirmedCount > 0) summaryHtml += '<span class="cv-badge confirmed">&check; ' + cvConfirmedCount + ' confirmed</span>';
            if (cvRemovedCount > 0) summaryHtml += '<span class="cv-badge removed">&cross; ' + cvRemovedCount + ' removed</span>';
            if (cvNewCount > 0) summaryHtml += '<span class="cv-badge new">&starf; ' + cvNewCount + ' new</span>';
            summaryHtml += '<span class="cv-overall" title="Overall: ' + PA.escAttr(overallStatus) + '">' + PA.esc(overallStatus) + '</span>';
            if (pendingCount > 0) {
                summaryHtml += '<span class="cv-review-bulk">';
                summaryHtml += '<span style="color:var(--text-muted);font-size:10px;margin-right:4px">' + pendingCount + ' pending</span>';
                summaryHtml += '<button class="cv-btn cv-btn-yes" onclick="PA.tableDetail._bulkReview(\'ACCEPTED\')">Accept All</button>';
                summaryHtml += '<button class="cv-btn cv-btn-no" onclick="PA.tableDetail._bulkReview(\'REJECTED\')">Reject All</button>';
                summaryHtml += '</span>';
            }
            summaryHtml += '</div>';
        }

        body.innerHTML = summaryHtml +
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tda\',0)">Operation</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tda\',1)">Procedure</th>' +
            '<th data-sort-col="2" onclick="PA.tf.sort(\'tda\',2)">Line</th>' +
            // Sep05-07: what the statement actually did, plus its SQL — so the popup answers
            // "how is this table used here" without opening the source.
            '<th>Detail &amp; SQL</th>' +
            (cv ? '<th>Claude</th>' : '') +
            '</tr></thead><tbody id="tda-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tda-pager"></div>';

        // Sep05-07: enrich the popup's rows from the statement index, using the SAME helper the
        // inline panel uses. Asynchronous so the table paints immediately; re-rendered when it
        // lands. Without this the popup showed strictly less than the inline panel for the very
        // same statements, which is what made opening the source unavoidable.
        if (PA.api && PA.api._analysisName && PA.tableDetail._tableName && window.AccessDetails) {
            AccessDetails.fetchDetail(PA.api._analysisName, PA.tableDetail._tableName)
                .then(function(byKey) {
                    if (!byKey) return;                       // no statement index: leave as-is
                    items.forEach(function(x) {
                        AccessDetails.applyDetail(x, byKey[AccessDetails.detailKey(x)]);
                    });
                    // Only if this tab is still the one on screen — the user may have moved on.
                    if (PA.tableDetail._currentTab === 'accesses' && PA.tf.state('tda')) {
                        PA.tf.render('tda');
                    }
                });
        }

        PA.tf.init('tda', items, 50, function(d, idx, esc) {
            var cvMatch = d._cvMatch;
            var rowClass = 'to-row';
            if (cvMatch && (cvMatch.status || '').toUpperCase() === 'REMOVED') rowClass += ' cv-removed';
            var html = '<tr class="' + rowClass + '">';
            html += '<td><span class="dh-op-badge ' + esc(d.operation || '') + '" style="font-size:9px">' + esc(d.operation || '?') + '</span></td>';
            html += '<td><span class="to-detail-proc" onclick="PA.tableDetail.closeAndShowProcedure(\'' + PA.escJs(d.procedureId || d.procedureName || '') + '\')">' + esc(d.procedureName || '?') + '</span></td>';
            html += '<td>';
            if (d._sf && d.lineNumber) {
                html += '<span class="td-line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(d._sf) + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>';
            } else if (d.lineNumber) {
                html += '<span class="td-line" style="cursor:default">L' + d.lineNumber + '</span>';
            } else {
                html += '-';
            }
            html += '</td>';

            // Sep05-07: the detail cell. Reuses the same enriched fields the inline Access Details
            // panel uses, so the two screens cannot disagree about the same statement — the popup
            // previously showed strictly less and the user had to open the code to learn anything.
            html += '<td class="td-detail">';
            var bits = [];
            if (d._accessKind) {
                bits.push('<span class="td-kind">'
                        + esc(String(d._accessKind).replace(/_/g, ' ').toLowerCase()) + '</span>');
            }
            (d._joins || []).slice(0, 1).forEach(function(j) {
                bits.push('<span class="td-frag">' + esc(j.type || 'JOIN') + ' ' + esc(j.right || '')
                        + (j.on ? ' ON ' + esc(j.on) : '') + '</span>');
            });
            (d._predicates || []).slice(0, 2).forEach(function(pr) {
                bits.push('<span class="td-frag">' + esc(pr.column || '?') + ' '
                        + esc(pr.operator || '') + ' ' + esc(pr.value == null ? '?' : pr.value) + '</span>');
            });
            (d._columnDetail || []).filter(function(c) { return c.expression || c.into; })
                .slice(0, 1).forEach(function(c) {
                    bits.push('<span class="td-frag">' + esc(c.name || '')
                            + (c.expression ? ' = ' + esc(c.expression) : '')
                            + (c.into ? ' INTO ' + esc(c.into) : '') + '</span>');
                });
            html += bits.length ? bits.join(' ') : '<span class="td-dim">—</span>';
            // The statement itself, on hover and expandable — the "small snippet so I need not go
            // to the actual code" the user asked for. Truncated for the row; the title carries more.
            if (d._sqlText) {
                var oneLine = String(d._sqlText).replace(/\s+/g, ' ').trim();
                html += '<div class="td-sql" title="' + PA.escAttr(oneLine) + '">'
                      + esc(oneLine.length > 120 ? oneLine.substring(0, 120) + ' …' : oneLine)
                      + '</div>';
            }
            html += '</td>';

            if (cv) {
                html += '<td>';
                if (cvMatch) {
                    var st = (cvMatch.status || '').toUpperCase();
                    var ud = (cvMatch.userDecision || '').toUpperCase();
                    if (st === 'CONFIRMED') html += '<span class="cv-badge confirmed" title="' + PA.escAttr(cvMatch.reason || '') + '">&check;</span>';
                    else if (st === 'REMOVED') html += '<span class="cv-badge removed" title="' + PA.escAttr(cvMatch.reason || '') + '">&cross;</span>';
                    if (ud === 'ACCEPTED') html += '<span class="cv-decision accepted">Accepted</span>';
                    else if (ud === 'REJECTED') html += '<span class="cv-decision rejected">Rejected</span>';
                    else html += PA.tableDetail._reviewBtns(PA.tableDetail._tableName, d.operation, d.procedureName, d.lineNumber);
                }
                html += '</td>';
            }
            html += '</tr>';
            return html;
        }, {
            sortKeys: {
                0: { fn: function(d) { return (d.operation || '').toUpperCase(); } },
                1: { fn: function(d) { return (d.procedureName || '').toUpperCase(); } },
                2: { fn: function(d) { return d.lineNumber || 0; } }
            },
            searchFn: function(d, q) {
                return (d.procedureName || '').toUpperCase().includes(q) ||
                       (d.operation || '').toUpperCase().includes(q);
            },
            extraFilter: function(d) {
                if (ops.size > 0) return ops.has(d.operation);
                return true;
            },
            onFilter: function() {
                var st = PA.tf.state('tda');
                if (countEl) countEl.textContent = (st ? st.filtered.length : items.length) + ' / ' + items.length;
            }
        });

        var st = PA.tf.state('tda');
        if (st) { st.sortCol = 0; st.sortDir = 'asc'; }
        PA.tf.filter('tda');

        setTimeout(function() {
            PA.tf.initColFilters('tda', {
                0: { label: 'Operation', valueFn: function(d) { return d.operation || ''; } },
                1: { label: 'Procedure', valueFn: function(d) { return d.procedureName || ''; } }
            });
            PA.tf._updateSortIndicators('tda');
        }, 0);
    },

    async _renderDefinition() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        if (!PA.tableDetail._metadata && !PA.tableDetail._metadataLoading) {
            body.innerHTML = '<div class="empty-msg">Loading view definition...</div>';
            await PA.tableDetail._ensureMetadata();
        }
        var md = PA.tableDetail._metadata;
        if (!md || !md.found) { PA.tableDetail._metadataNotAvailable(body, countEl, 'definition'); return; }
        if (!md.isView) {
            body.innerHTML = '<div class="empty-msg">This object is a TABLE — definition view applies to views only.</div>';
            if (countEl) countEl.textContent = '';
            return;
        }
        var def = md.viewDefinition || '';
        if (!def) {
            body.innerHTML = '<div class="empty-msg">View definition not available (may require DBA privileges).</div>';
            if (countEl) countEl.textContent = '';
            return;
        }
        if (countEl) countEl.textContent = def.split('\n').length + ' lines';
        body.innerHTML =
            '<div style="margin-bottom:8px;display:flex;align-items:center;gap:8px">' +
            '<span style="font-size:11px;color:var(--text-muted)">SQL definition of view <strong style="color:var(--teal)">' +
            PA.esc(PA.tableDetail._schema ? PA.tableDetail._schema + '.' + PA.tableDetail._tableName : PA.tableDetail._tableName) +
            '</strong></span>' +
            '<button class="btn btn-sm" style="margin-left:auto;font-size:10px" onclick="PA.tableDetail._copyViewDef()" title="Copy to clipboard">Copy</button>' +
            '</div>' +
            '<pre style="background:var(--bg-secondary);border:1px solid var(--border);border-radius:var(--radius);padding:12px;' +
            'font-family:var(--font-mono);font-size:11px;line-height:1.6;overflow:auto;max-height:520px;white-space:pre-wrap;word-break:break-word">' +
            PA.esc(def) + '</pre>' +
            PA.tableDetail._renderViewDependencies(md);
    },

    /**
     * What the view's own SELECT body was found to depend on — the tables it reads from and how
     * it joins them (ViewDefinitionAnalyzer, run once per enrichment by parsing the definition
     * text above through the same dialect visitor a real CREATE VIEW statement goes through).
     * Kept deliberately simple (two lists, no paging/sorting) rather than reusing the
     * procedure-level Joins tab machinery, which is server-paged for call-tree-sized result sets
     * this never approaches.
     */
    _renderViewDependencies(md) {
        var ops = (md && md.viewTableOperations) || [];
        var joins = (md && md.viewJoins) || [];
        if (!ops.length && !joins.length) return '';

        var html = '<div class="td-group" style="margin-top:16px">';
        html += '<div class="td-group-title">Tables &amp; joins referenced by this view</div>';

        if (ops.length) {
            var seen = {};
            var tableNames = [];
            for (var i = 0; i < ops.length; i++) {
                var t = ops[i].tableName || '';
                var key = (ops[i].schema ? ops[i].schema + '.' : '') + t;
                if (!t || seen[key]) continue;
                seen[key] = true;
                tableNames.push({ schema: ops[i].schema || '', name: t });
            }
            html += '<div style="margin:6px 0;display:flex;flex-wrap:wrap;gap:6px">';
            for (var ti = 0; ti < tableNames.length; ti++) {
                var tn = tableNames[ti];
                html += '<span class="dh-op-badge SELECT" style="font-size:10px;cursor:pointer" ' +
                    'onclick="PA.tableDetail.open(\'' + PA.escJs(tn.name) + '\', \'' + PA.escJs(tn.schema) + '\')" ' +
                    'title="Open this table">' +
                    PA.esc(tn.schema ? tn.schema + '.' + tn.name : tn.name) + '</span>';
            }
            html += '</div>';
        }

        if (joins.length) {
            html += '<table class="to-table" style="margin-top:6px"><thead><tr>' +
                '<th>Join Type</th><th>Joined Table</th><th>Alias</th><th>Condition</th>' +
                '</tr></thead><tbody>';
            for (var ji = 0; ji < joins.length; ji++) {
                var j = joins[ji];
                html += '<tr class="to-row">';
                html += '<td><span class="td-join-type">' + PA.esc(j.joinType || 'JOIN') + '</span></td>';
                html += '<td style="font-weight:600;color:var(--teal)">' + PA.esc(j.joinedTable || '?') + '</td>';
                html += '<td style="font-size:11px;color:var(--text-muted)">' + PA.esc(j.joinedTableAlias || '-') + '</td>';
                html += '<td style="font-size:11px;font-family:var(--font-mono);max-width:320px;overflow:hidden;' +
                    'text-overflow:ellipsis" title="' + PA.escAttr(j.condition || '') + '">' + PA.esc(j.condition || '-') + '</td>';
                html += '</tr>';
            }
            html += '</tbody></table>';
        }

        html += '</div>';
        return html;
    },

    _copyViewDef() {
        var md = PA.tableDetail._metadata;
        if (!md || !md.viewDefinition) return;
        navigator.clipboard && navigator.clipboard.writeText(md.viewDefinition).catch(function() {});
    },

    async _ensureMetadata() {
        if (PA.tableDetail._metadata || PA.tableDetail._metadataLoading) return;

        // open() captures and re-checks _openGen for every other async branch — including the
        // .then() that calls this function — but this body never did. So: open table A (slow live
        // DB metadata path), close it, open table B, and B's Columns/Indexes/Constraints tabs
        // render A's schema as fact. Same counter, so a new open() invalidates this too.
        var gen = PA.tableDetail._openGen;
        var isCurrent = function () { return PA.tableDetail._openGen === gen; };

        // Try to build metadata from analysis JSON first (stored at analysis time — no live DB call)
        var raw = PA._cachedTablesIndex;
        if (!raw) {
            try { raw = await PA.api.getTablesIndex(); if (raw) PA._cachedTablesIndex = raw; } catch(e) {}
            if (!isCurrent()) return;
        }
        if (raw && raw.tables) {
            var upper = PA.tableDetail._tableName.toUpperCase();
            var entry = raw.tables.find(function(t) { return (t.name || '').toUpperCase() === upper; });
            if (entry && entry.dbValidated && Array.isArray(entry.dbColumns)) {
                PA.tableDetail._metadata = {
                    found:          true,
                    tableName:      entry.name,
                    schema:         entry.schema || PA.tableDetail._schema,
                    isView:         (entry.dbResolvedType || '').toUpperCase() === 'VIEW' ||
                                    (entry.dbResolvedType || '').toUpperCase() === 'MATERIALIZED VIEW',
                    columns:        entry.dbColumns      || [],
                    constraints:    entry.dbConstraints  || [],
                    indexes:        entry.dbIndexes      || [],
                    viewDefinition: entry.viewDefinition || null,
                    // Tables/joins pulled out of the view's own SELECT body (ViewDefinitionAnalyzer,
                    // run once at enrichment time — not a live call), so the Definition tab can show
                    // what a view depends on, not just its raw SQL text.
                    viewTableOperations: entry.viewTableOperations || [],
                    viewJoins:      entry.viewJoins || [],
                    _fromJson:      true
                };
                PA.tableDetail._renderTabs();
                return;
            }
        }

        // Fallback: live DB call (for analyses run before this feature was added)
        PA.tableDetail._metadataLoading = true;
        var md;
        try {
            // Into a local, then checked: assigning straight to _metadata would publish another
            // table's schema before we ever get the chance to discard it.
            md = await PA.api.dbTableInfo(PA.tableDetail._tableName, PA.tableDetail._schema);
        } catch (e) {
            md = { found: false, error: e.message };
        }
        PA.tableDetail._metadataLoading = false;
        if (!isCurrent()) return;
        PA.tableDetail._metadata = md;
        PA.tableDetail._renderTabs();
    },

    _metadataNotAvailable(body, countEl, label) {
        if (countEl) countEl.textContent = '0 ' + label;
        var md = PA.tableDetail._metadata;
        // Surface the backend's real reason for unsupported dialects (MySQL/DB2).
        if (md && (md.unsupported || md.error)) {
            body.innerHTML = '<div class="empty-msg">' + PA.esc(md.error || 'Table metadata not supported for this database.') + '</div>';
            return;
        }
        body.innerHTML = '<div class="empty-msg">Table metadata not available from database.' +
            '<br><span style="font-size:11px;color:var(--text-muted)">Ensure a database connection is configured and the table exists in a connected schema.</span></div>';
    },

    async _renderDbColumns() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        if (!PA.tableDetail._metadata && !PA.tableDetail._metadataLoading) {
            body.innerHTML = '<div class="empty-msg">Loading database metadata...</div>';
            await PA.tableDetail._ensureMetadata();
        }
        var md = PA.tableDetail._metadata;
        if (!md || !md.found) { PA.tableDetail._metadataNotAvailable(body, countEl, 'columns'); return; }

        var cols = md.columns || [];
        var constraints = md.constraints || [];

        var pkCols = {};
        for (var ci = 0; ci < constraints.length; ci++) {
            if (constraints[ci].constraintType === 'P') pkCols[constraints[ci].columnName] = true;
        }
        for (var i = 0; i < cols.length; i++) {
            cols[i]._isPk = !!pkCols[cols[i].columnName];
            cols[i]._typeStr = PA.tableDetail._formatColType(cols[i]);
            cols[i]._idx = cols[i].columnId || (i + 1);
        }

        body.innerHTML =
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tdmc\',0)">#</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tdmc\',1)">Column Name</th>' +
            '<th data-sort-col="2" onclick="PA.tf.sort(\'tdmc\',2)">Data Type</th>' +
            '<th data-sort-col="3" onclick="PA.tf.sort(\'tdmc\',3)">Nullable</th>' +
            '<th>Default</th>' +
            '</tr></thead><tbody id="tdmc-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tdmc-pager"></div>';

        PA.tf.init('tdmc', cols, 50, function(c, idx, esc) {
            var html = '<tr class="to-row">';
            html += '<td style="color:var(--text-muted);font-size:10px">' + c._idx + '</td>';
            html += '<td style="font-weight:600;font-family:var(--font-mono)">';
            if (c._isPk) html += '<span style="color:var(--orange);margin-right:4px" title="Primary Key">&#128273;</span>';
            html += esc(c.columnName || '') + '</td>';
            html += '<td style="font-family:var(--font-mono);font-size:11px;color:var(--blue)">' + esc(c._typeStr) + '</td>';
            html += '<td>' + (c.nullable ? '<span style="color:var(--text-muted)">YES</span>' : '<span style="color:var(--red);font-weight:600">NOT NULL</span>') + '</td>';
            html += '<td style="font-size:11px;color:var(--text-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + PA.escAttr(c.dataDefault || '') + '">' + esc(c.dataDefault || '-') + '</td>';
            html += '</tr>';
            return html;
        }, {
            sortKeys: {
                0: { fn: function(c) { return c._idx; } },
                1: { fn: function(c) { return (c.columnName || '').toUpperCase(); } },
                2: { fn: function(c) { return (c._typeStr || '').toUpperCase(); } },
                3: { fn: function(c) { return c.nullable ? 1 : 0; } }
            },
            searchFn: function(c, q) {
                return (c.columnName || '').toUpperCase().includes(q) ||
                       (c._typeStr || '').toUpperCase().includes(q) ||
                       (c.dataDefault || '').toUpperCase().includes(q);
            },
            onFilter: function() {
                var st = PA.tf.state('tdmc');
                if (countEl) countEl.textContent = (st ? st.filtered.length : cols.length) + ' / ' + cols.length + ' columns';
            }
        });

        PA.tf.filter('tdmc');

        setTimeout(function() {
            PA.tf.initColFilters('tdmc', {
                2: { label: 'Data Type', valueFn: function(c) { return c.dataType || ''; } },
                3: { label: 'Nullable', valueFn: function(c) { return c.nullable ? 'YES' : 'NOT NULL'; } }
            });
        }, 0);
    },

    async _renderDbIndexes() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        if (!PA.tableDetail._metadata && !PA.tableDetail._metadataLoading) {
            body.innerHTML = '<div class="empty-msg">Loading database metadata...</div>';
            await PA.tableDetail._ensureMetadata();
        }
        var md = PA.tableDetail._metadata;
        if (!md || !md.found) { PA.tableDetail._metadataNotAvailable(body, countEl, 'indexes'); return; }

        var idxGrouped = PA.tableDetail._groupBy(md.indexes || [], 'indexName');
        if (!idxGrouped.length) {
            if (countEl) countEl.textContent = '0 indexes';
            body.innerHTML = '<div class="empty-msg">No indexes found for this table</div>';
            return;
        }

        body.innerHTML =
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tdmi\',0)">Index Name</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tdmi\',1)">Uniqueness</th>' +
            '<th>Columns</th>' +
            '</tr></thead><tbody id="tdmi-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tdmi-pager"></div>';

        PA.tf.init('tdmi', idxGrouped, 50, function(ig, idx, esc) {
            var uniq = ig.items[0].uniqueness || '';
            var html = '<tr class="to-row">';
            html += '<td style="font-family:var(--font-mono);font-size:11px;font-weight:600">' + esc(ig.key) + '</td>';
            html += '<td><span style="color:' + (uniq === 'UNIQUE' ? 'var(--blue)' : 'var(--text-muted)') + ';font-weight:600;font-size:11px">' + esc(uniq) + '</span></td>';
            html += '<td style="font-family:var(--font-mono);font-size:11px">' + ig.items.map(function(it) { return esc(it.columnName || ''); }).join(', ') + '</td>';
            html += '</tr>';
            return html;
        }, {
            sortKeys: {
                0: { fn: function(ig) { return ig.key.toUpperCase(); } },
                1: { fn: function(ig) { return (ig.items[0].uniqueness || '').toUpperCase(); } }
            },
            searchFn: function(ig, q) {
                return ig.key.toUpperCase().includes(q) ||
                       (ig.items[0].uniqueness || '').toUpperCase().includes(q) ||
                       ig.items.some(function(it) { return (it.columnName || '').toUpperCase().includes(q); });
            },
            onFilter: function() {
                var st = PA.tf.state('tdmi');
                if (countEl) countEl.textContent = (st ? st.filtered.length : idxGrouped.length) + ' / ' + idxGrouped.length + ' indexes';
            }
        });

        PA.tf.filter('tdmi');

        setTimeout(function() {
            PA.tf.initColFilters('tdmi', {
                1: { label: 'Uniqueness', valueFn: function(ig) { return ig.items[0].uniqueness || ''; } }
            });
        }, 0);
    },

    async _renderDbConstraints() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        if (!PA.tableDetail._metadata && !PA.tableDetail._metadataLoading) {
            body.innerHTML = '<div class="empty-msg">Loading database metadata...</div>';
            await PA.tableDetail._ensureMetadata();
        }
        var md = PA.tableDetail._metadata;
        if (!md || !md.found) { PA.tableDetail._metadataNotAvailable(body, countEl, 'constraints'); return; }

        var grouped = PA.tableDetail._groupBy(md.constraints || [], 'constraintName');
        if (!grouped.length) {
            if (countEl) countEl.textContent = '0 constraints';
            body.innerHTML = '<div class="empty-msg">No constraints found for this table</div>';
            return;
        }

        var typeLabels = { P: 'PRIMARY KEY', U: 'UNIQUE', R: 'FOREIGN KEY', C: 'CHECK' };
        var typeColors = { P: 'var(--orange)', U: 'var(--blue)', R: 'var(--green)', C: 'var(--text-muted)' };

        body.innerHTML =
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tdmk\',0)">Constraint</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tdmk\',1)">Type</th>' +
            '<th>Columns</th>' +
            '<th>References</th>' +
            '</tr></thead><tbody id="tdmk-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tdmk-pager"></div>';

        PA.tf.init('tdmk', grouped, 50, function(g, idx, esc) {
            var typeCode = g.items[0].constraintType || '';
            var html = '<tr class="to-row">';
            html += '<td style="font-family:var(--font-mono);font-size:11px;font-weight:600">' + esc(g.key) + '</td>';
            html += '<td><span style="color:' + (typeColors[typeCode] || 'var(--text-muted)') + ';font-weight:600;font-size:11px">' + esc(typeLabels[typeCode] || typeCode) + '</span></td>';
            html += '<td style="font-family:var(--font-mono);font-size:11px">' + g.items.map(function(it) { return esc(it.columnName || ''); }).join(', ') + '</td>';
            html += '<td style="font-size:11px;color:var(--text-muted)">' + esc(g.items[0].refConstraint || '-') + '</td>';
            html += '</tr>';
            return html;
        }, {
            sortKeys: {
                0: { fn: function(g) { return g.key.toUpperCase(); } },
                1: { fn: function(g) { return (g.items[0].constraintType || '').toUpperCase(); } }
            },
            searchFn: function(g, q) {
                return g.key.toUpperCase().includes(q) ||
                       (typeLabels[g.items[0].constraintType] || '').toUpperCase().includes(q) ||
                       g.items.some(function(it) { return (it.columnName || '').toUpperCase().includes(q); }) ||
                       (g.items[0].refConstraint || '').toUpperCase().includes(q);
            },
            onFilter: function() {
                var st = PA.tf.state('tdmk');
                if (countEl) countEl.textContent = (st ? st.filtered.length : grouped.length) + ' / ' + grouped.length + ' constraints';
            }
        });

        PA.tf.filter('tdmk');

        setTimeout(function() {
            PA.tf.initColFilters('tdmk', {
                1: { label: 'Type', valueFn: function(g) { return typeLabels[g.items[0].constraintType] || g.items[0].constraintType || ''; } }
            });
        }, 0);
    },

    _formatColType: function(col) {
        var raw = col.dataType || '';
        var dt = raw.toUpperCase();
        // Numeric types carrying precision/scale (Oracle NUMBER, SQL Server/DB2 DECIMAL/NUMERIC)
        if (dt === 'NUMBER' || dt === 'DECIMAL' || dt === 'NUMERIC') {
            if (col.dataPrecision != null) {
                return raw + '(' + col.dataPrecision + (col.dataScale ? ',' + col.dataScale : '') + ')';
            }
            return raw;
        }
        // Character / binary types carrying a length (Oracle + SQL Server + DB2)
        var lengthTypes = ['VARCHAR2', 'NVARCHAR2', 'VARCHAR', 'NVARCHAR',
            'CHAR', 'NCHAR', 'RAW', 'VARBINARY', 'BINARY', 'VARGRAPHIC', 'GRAPHIC'];
        if (lengthTypes.indexOf(dt) >= 0 && col.dataLength) {
            return raw + '(' + col.dataLength + ')';
        }
        return raw;
    },

    _groupBy: function(arr, keyField) {
        var map = {};
        var order = [];
        for (var i = 0; i < arr.length; i++) {
            var k = arr[i][keyField] || '';
            if (!map[k]) { map[k] = []; order.push(k); }
            map[k].push(arr[i]);
        }
        return order.map(function(k) { return { key: k, items: map[k] }; });
    },

    /**
     * Joins tab — server-paged over `/table-joins` (PLAN-08 Q59).
     *
     * A fresh page source per render, deliberately: `_render` clears the framework state on every
     * tab switch, and reusing a source whose cursor is halfway through the result set would show
     * page 4 under the label "page 1".
     *
     * The search box, the column headers and the column-filter popovers all travel with the
     * request. Left in the browser they would apply to the fifty rows in memory and look like
     * they had worked.
     */
    _renderJoins() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        var sum = PA.tableDetail._joinsSummary;

        // Only claimed when the endpoint has actually said so. Before the summary arrives the
        // table renders and reports its own emptiness, rather than this asserting "0 joins" for a
        // table that has plenty.
        if (sum && sum.scopedTotal === 0) {
            if (countEl) countEl.textContent = '0 joins';
            body.innerHTML = '<div class="empty-msg">No joins found for this table in current procedure</div>';
            return;
        }

        body.innerHTML =
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tdj\',0)">Join Type</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tdj\',1)">Joined Table</th>' +
            '<th>Alias</th>' +
            '<th>Condition</th>' +
            '<th data-sort-col="4" onclick="PA.tf.sort(\'tdj\',4)">Procedure</th>' +
            '<th data-sort-col="5" onclick="PA.tf.sort(\'tdj\',5)">Line</th>' +
            '</tr></thead><tbody id="tdj-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tdj-pager"></div>';

        PA.tf.init('tdj', PA.tableDetail._joinsPageSource(), 50, function(j, idx, esc) {
            var sf = j.sourceFile || (PA.currentDetail ? PA.currentDetail.sourceFile || '' : '');
            var html = '<tr class="to-row">';
            html += '<td><span class="td-join-type">' + esc(j.joinType || 'JOIN') + '</span></td>';
            html += '<td style="font-weight:600;color:var(--teal)">' + esc(j.joinedTable || '?') + '</td>';
            html += '<td style="font-size:11px;color:var(--text-muted)">' + esc(j.joinedTableAlias || '-') + '</td>';
            html += '<td style="font-size:11px;font-family:var(--font-mono);max-width:300px;overflow:hidden;text-overflow:ellipsis" title="' + PA.escAttr(j.condition || '') + '">' + esc(j.condition || '-') + '</td>';
            html += '<td><span class="to-detail-proc" onclick="PA.tableDetail.closeAndShowProcedure(\'' + PA.escJs(j.procedureId || '') + '\')">' + esc(j.procedureName || '-') + '</span></td>';
            html += '<td>';
            if (j.line) {
                html += '<span class="td-line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + j.line + ')">L' + j.line + '</span>';
            }
            html += '</td></tr>';
            return html;
        }, {
            // The endpoint's closed sort enum, keyed by the column index the <th> carries.
            // Condition and Alias are absent because the endpoint does not offer them — sorting
            // them in the browser would reorder one page and nothing else.
            serverSorts: {
                0: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                1: { asc: 'TABLE_ASC', desc: 'TABLE_DESC' },
                4: { asc: 'PROCEDURE_ASC', desc: 'PROCEDURE_DESC' },
                5: { asc: 'LINE_ASC', desc: 'LINE_DESC' }
            },
            onFilter: function() {
                var st = PA.tf.state('tdj');
                if (!countEl) return;
                // `serverTotal` is the number of rows matching the current filters, not the size
                // of the page, so it does not shrink while paging. -1 means the endpoint could
                // not count cheaply, in which case no count is better than a guess.
                var n = st && st.totalIsExact ? st.serverTotal : -1;
                countEl.textContent = n >= 0 ? (n + ' joins') : '';
            }
        });

        setTimeout(function() {
            // The popover value lists come from the summary, which collected them over the whole
            // result set. Without them PA.tf declines to show the filter icons at all, which is
            // the honest answer — this page's values are not the table's values.
            var s = PA.tableDetail._joinsSummary;
            if (!s || !s.columnValues) return;
            PA.tf.initColFilters('tdj', {
                0: { label: 'Join Type', valueFn: function(j) { return j.joinType || 'JOIN'; } },
                1: { label: 'Joined Table', valueFn: function(j) { return j.joinedTable || ''; } },
                2: { label: 'Alias', valueFn: function(j) { return j.joinedTableAlias || '-'; } },
                4: { label: 'Procedure', valueFn: function(j) { return j.procedureName || ''; } }
            }, s.columnValues);
            PA.tf._updateSortIndicators('tdj');
        }, 0);
    },

    _renderTriggers() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        var triggers = PA.tableDetail._triggers;

        if (!triggers.length) {
            if (countEl) countEl.textContent = '0 triggers';
            // A failed /tables/{t}/triggers request must not read as "this table has none".
            body.innerHTML = PA.tableDetail._triggersError
                ? '<div class="empty-msg" style="color:var(--badge-red,#c0392b)">Failed to load triggers for this table</div>'
                : '<div class="empty-msg">No triggers found for this table</div>';
            return;
        }

        body.innerHTML =
            // The DB-catalog triggers below still loaded; only the parsed-source merge failed, so
            // the list is shown but flagged as incomplete rather than presented as the full set.
            (PA.tableDetail._triggersError
                ? '<div class="empty-msg" style="color:var(--badge-red,#c0392b);padding:6px 0">Some triggers could not be loaded; this list may be incomplete.</div>'
                : '') +
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tdt\',0)">Schema</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tdt\',1)">Trigger Name</th>' +
            '<th data-sort-col="2" onclick="PA.tf.sort(\'tdt\',2)">Timing</th>' +
            '<th data-sort-col="3" onclick="PA.tf.sort(\'tdt\',3)">Event</th>' +
            '<th>Type</th>' +
            '<th>WHEN Condition</th>' +
            '<th>Source</th>' +
            '<th>Actions</th>' +
            '</tr></thead><tbody id="tdt-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tdt-pager"></div>';

        PA.tf.init('tdt', triggers, 50, function(t, idx, esc) {
            var colorObj = PA.getSchemaColor(t.schema);
            var html = '<tr class="to-row" style="cursor:pointer" onclick="PA.triggerDetail.open(PA.tf.state(\'tdt\').filtered[' + idx + '], PA.tableDetail._tableName, PA.tableDetail._schema)">';
            if (t.schema) {
                html += '<td><span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '">' + esc(t.schema) + '</span></td>';
            } else {
                html += '<td style="color:var(--text-muted)">-</td>';
            }
            html += '<td style="font-weight:600">';
            html += '<span class="lp-icon T" style="display:inline-flex;width:16px;height:16px;font-size:8px;margin-right:4px">T</span>';
            if (t.nodeId) {
                html += '<span class="to-detail-proc" onclick="event.stopPropagation();PA.tableDetail.closeAndShowProcedure(\'' + PA.escJs(t.nodeId) + '\')">' + esc(t.name || '?') + '</span>';
            } else {
                html += esc(t.name || '?');
            }
            html += '</td>';
            html += '<td>';
            var timing = t.timing || '';
            if (!timing && t.triggerType) {
                var tt = t.triggerType.toUpperCase();
                if (tt.includes('BEFORE')) timing = 'BEFORE';
                else if (tt.includes('AFTER')) timing = 'AFTER';
            }
            if (timing) html += '<span class="td-trigger-timing">' + esc(timing) + '</span>';
            else html += '<span style="color:var(--text-muted)">-</span>';
            html += '</td>';
            html += '<td>';
            if (t.event) html += '<span class="td-trigger-event">' + esc(t.event) + '</span>';
            else html += '<span style="color:var(--text-muted)">-</span>';
            html += '</td>';
            html += '<td style="font-size:11px;color:var(--text-muted)">' + esc(t.triggerType || '-') + '</td>';
            // WHEN condition
            html += '<td style="font-size:11px;font-family:var(--font-mono);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">';
            if (t.whenCondition) {
                html += '<span title="' + PA.escAttr(t.whenCondition) + '" style="color:var(--orange)">' + esc(t.whenCondition) + '</span>';
            } else {
                html += '<span style="color:var(--text-muted)">-</span>';
            }
            html += '</td>';
            html += '<td>';
            if (t.source === 'DATABASE') html += '<span class="td-source-badge td-source-db">DB</span>';
            else if (t.source === 'PARSED') html += '<span class="td-source-badge td-source-parsed">SRC</span>';
            else html += '<span class="td-source-badge">' + esc(t.source || '-') + '</span>';
            html += '</td>';
            html += '<td>';
            if (t.nodeId) html += '<span class="btn btn-sm td-action-btn" onclick="event.stopPropagation();PA.sourceView.openModal(\'' + PA.escJs(t.nodeId) + '\')">View Source</span>';
            if (t.definition) html += '<span class="btn btn-sm td-action-btn" onclick="event.stopPropagation();PA.tableDetail._showTriggerDef(' + idx + ')">Definition</span>';
            html += '</td></tr>';
            return html;
        }, {
            sortKeys: {
                0: { fn: function(t) { return (t.schema || '').toUpperCase(); } },
                1: { fn: function(t) { return (t.name || '').toUpperCase(); } },
                2: { fn: function(t) { return (t.timing || '').toUpperCase(); } },
                3: { fn: function(t) { return (t.event || '').toUpperCase(); } }
            },
            searchFn: function(t, q) {
                return (t.name || '').toUpperCase().includes(q) ||
                       (t.schema || '').toUpperCase().includes(q) ||
                       (t.event || '').toUpperCase().includes(q) ||
                       (t.timing || '').toUpperCase().includes(q) ||
                       (t.triggerType || '').toUpperCase().includes(q) ||
                       (t.whenCondition || '').toUpperCase().includes(q);
            },
            onFilter: function() {
                var st = PA.tf.state('tdt');
                if (countEl) countEl.textContent = (st ? st.filtered.length : triggers.length) + ' triggers';
            }
        });

        PA.tf.filter('tdt');

        setTimeout(function() {
            PA.tf.initColFilters('tdt', {
                0: { label: 'Schema', valueFn: function(t) { return t.schema || '-'; } },
                1: { label: 'Trigger', valueFn: function(t) { return t.name || ''; } },
                2: { label: 'Timing', valueFn: function(t) { return t.timing || '-'; } },
                3: { label: 'Event', valueFn: function(t) { return t.event || '-'; } },
                4: { label: 'Type', valueFn: function(t) { return t.triggerType || '-'; } },
                5: { label: 'Source', valueFn: function(t) { return t.source || '-'; } }
            });
        }, 0);
    },

    _renderClaudeComparison() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        var cv = PA.tableDetail._data ? PA.tableDetail._data.claudeVerification : null;
        var verifications = (cv && cv.claudeVerifications) ? cv.claudeVerifications : [];
        var staticAccesses = PA.tableDetail._allAccess;

        if (!cv) {
            if (countEl) countEl.textContent = '0 verifications';
            body.innerHTML = '<div class="empty-msg">No Claude verification data available for this table.<br><span style="font-size:11px;color:var(--text-muted)">Run Claude enrichment to generate verification data.</span></div>';
            return;
        }

        // Classify verifications
        var confirmedCount = 0, removedCount = 0, newCount = 0, unverifiedCount = 0;
        var usedVerifications = new Set();
        var matchedRows = [];
        var newOps = [];

        // Match static accesses to claude verifications
        for (var i = 0; i < staticAccesses.length; i++) {
            var acc = staticAccesses[i];
            var match = PA.tableDetail._findClaudeMatch(acc, verifications);
            if (match) {
                var idx = verifications.indexOf(match);
                usedVerifications.add(idx);
                var st = (match.status || '').toUpperCase();
                if (st === 'CONFIRMED') confirmedCount++;
                else if (st === 'REMOVED') removedCount++;
                matchedRows.push({ access: acc, claude: match, matched: true });
            } else {
                unverifiedCount++;
                matchedRows.push({ access: acc, claude: null, matched: false });
            }
        }

        // Collect NEW operations (in claude but not matched to static)
        for (var vi = 0; vi < verifications.length; vi++) {
            var v = verifications[vi];
            var vStatus = (v.status || '').toUpperCase();
            if (vStatus === 'NEW') {
                newCount++;
                newOps.push(v);
            } else if (!usedVerifications.has(vi)) {
                // Claude verification that didn't match any static access
                newOps.push(v);
                if (vStatus === 'CONFIRMED') confirmedCount++;
                else if (vStatus === 'REMOVED') removedCount++;
            }
        }

        var totalRows = matchedRows.length + newOps.length;
        if (countEl) countEl.textContent = totalRows + ' comparisons';

        var html = '';

        // Summary bar
        html += '<div class="cv-compare-summary">';
        html += '<span class="cv-compare-stat"><span class="cv-dot cv-dot-confirmed"></span>' + confirmedCount + ' confirmed</span>';
        html += '<span class="cv-compare-stat"><span class="cv-dot cv-dot-removed"></span>' + removedCount + ' removed</span>';
        html += '<span class="cv-compare-stat"><span class="cv-dot cv-dot-new"></span>' + newCount + ' new</span>';
        html += '<span class="cv-compare-stat"><span class="cv-dot cv-dot-unverified"></span>' + unverifiedCount + ' unverified</span>';
        var overallStatus = cv.overallStatus || 'UNKNOWN';
        html += '<span class="cv-compare-overall">' + PA.esc(overallStatus) + '</span>';
        html += '</div>';

        // Side-by-side comparison table
        html += '<table class="to-table cv-compare-table"><thead><tr>';
        html += '<th>Operation</th><th>Procedure</th><th>Line</th><th>Static</th><th>Claude Status</th><th>Reason</th><th>Review</th>';
        html += '</tr></thead><tbody>';

        // Sort matched rows: REMOVED first, then CONFIRMED, then unverified
        var statusOrder = { REMOVED: 0, CONFIRMED: 1 };
        matchedRows.sort(function(a, b) {
            var sa = a.claude ? (statusOrder[(a.claude.status || '').toUpperCase()] != null ? statusOrder[(a.claude.status || '').toUpperCase()] : 2) : 3;
            var sb = b.claude ? (statusOrder[(b.claude.status || '').toUpperCase()] != null ? statusOrder[(b.claude.status || '').toUpperCase()] : 2) : 3;
            if (sa !== sb) return sa - sb;
            var opA = (a.access.operation || '').toUpperCase();
            var opB = (b.access.operation || '').toUpperCase();
            return opA < opB ? -1 : opA > opB ? 1 : 0;
        });

        var pgMatched = PA.pager.slice('td-claude-matched', matchedRows);
        PA.pager.register('td-claude-matched', function () { PA.tableDetail._renderClaudeComparison(); });
        var matchedPage = pgMatched.pageItems;
        for (var ri = 0; ri < matchedPage.length; ri++) {
            var row = matchedPage[ri];
            var acc = row.access;
            var cl = row.claude;
            var st = cl ? (cl.status || '').toUpperCase() : '';
            var rowClass = 'to-row';
            if (st === 'CONFIRMED') rowClass += ' cv-row-confirmed';
            else if (st === 'REMOVED') rowClass += ' cv-row-removed';
            else rowClass += ' cv-row-unverified';

            html += '<tr class="' + rowClass + '">';
            // Operation
            html += '<td><span class="dh-op-badge ' + PA.esc(acc.operation || '') + '" style="font-size:9px">' + PA.esc(acc.operation || '?') + '</span></td>';
            // Procedure
            html += '<td>';
            if (acc.procedureId || acc.procedureName) {
                html += '<span class="to-detail-proc" onclick="PA.tableDetail.closeAndShowProcedure(\'' + PA.escJs(acc.procedureId || acc.procedureName || '') + '\')">' + PA.esc(acc.procedureName || '?') + '</span>';
            } else {
                html += '<span style="color:var(--text-muted)">-</span>';
            }
            html += '</td>';
            // Line
            html += '<td>';
            if (acc.lineNumber) {
                var sf = acc.sourceFile || '';
                if (!sf && acc.procedureId) {
                    // Same shape as above: inside the per-access row loop.
                    var node = PA.nodeIndex.byId(acc.procedureId);
                    if (node) sf = node.sourceFile || '';
                }
                if (sf) {
                    html += '<span class="td-line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + acc.lineNumber + ')">L' + acc.lineNumber + '</span>';
                } else {
                    html += '<span class="td-line" style="cursor:default">L' + acc.lineNumber + '</span>';
                }
            } else {
                html += '<span style="color:var(--text-muted)">-</span>';
            }
            html += '</td>';
            // Static column
            html += '<td><span class="cv-source-badge cv-source-static">Static</span></td>';
            // Claude Status
            html += '<td>';
            if (cl) {
                if (st === 'CONFIRMED') html += '<span class="cv-badge confirmed">&check; Confirmed</span>';
                else if (st === 'REMOVED') html += '<span class="cv-badge removed">&cross; Removed</span>';
                else html += '<span class="cv-badge" style="background:var(--bg-secondary);color:var(--text-muted)">' + PA.esc(cl.status || '?') + '</span>';
            } else {
                html += '<span class="cv-badge cv-badge-unverified">Not verified</span>';
            }
            html += '</td>';
            // Reason
            html += '<td class="cv-reason-cell">';
            if (cl && cl.reason) {
                html += '<span title="' + PA.escAttr(cl.reason) + '">' + PA.esc(cl.reason.length > 60 ? cl.reason.substring(0, 60) + '...' : cl.reason) + '</span>';
            } else {
                html += '<span style="color:var(--text-muted)">-</span>';
            }
            html += '</td>';
            // Review
            html += '<td>';
            if (cl) {
                var ud = (cl.userDecision || '').toUpperCase();
                if (ud === 'ACCEPTED') {
                    html += '<span class="cv-decision accepted">Accepted</span>';
                } else if (ud === 'REJECTED') {
                    html += '<span class="cv-decision rejected">Rejected</span>';
                } else {
                    html += PA.tableDetail._reviewBtns(PA.tableDetail._tableName, acc.operation, acc.procedureName, acc.lineNumber);
                }
            }
            html += '</td>';
            html += '</tr>';
        }

        html += '</tbody></table>' + pgMatched.controls;

        // NEW operations section (found by Claude, not in static)
        if (newOps.length > 0) {
            html += '<div class="td-group" style="margin-top:16px">';
            html += '<div class="td-group-title"><span class="cv-badge new">&starf; NEW</span> Found by Claude (not in static analysis)</div>';
            html += '<table class="to-table cv-compare-table"><thead><tr>';
            html += '<th>Operation</th><th>Procedure</th><th>Line</th><th>Status</th><th>Reason</th><th>Review</th>';
            html += '</tr></thead><tbody>';

            var pgNew = PA.pager.slice('td-claude-new', newOps);
            PA.pager.register('td-claude-new', function () { PA.tableDetail._renderClaudeComparison(); });
            var newOpsPage = pgNew.pageItems;
            for (var ni = 0; ni < newOpsPage.length; ni++) {
                var nop = newOpsPage[ni];
                var nSt = (nop.status || '').toUpperCase();
                html += '<tr class="to-row cv-row-new">';
                html += '<td><span class="dh-op-badge ' + PA.esc(nop.operation || '') + '" style="font-size:9px">' + PA.esc(nop.operation || '?') + '</span></td>';
                html += '<td>';
                if (nop.procedureName) {
                    html += '<span class="to-detail-proc" onclick="PA.tableDetail.closeAndShowProcedure(\'' + PA.escJs(nop.procedureName || '') + '\')">' + PA.esc(nop.procedureName) + '</span>';
                } else {
                    html += '<span style="color:var(--text-muted)">-</span>';
                }
                html += '</td>';
                html += '<td>';
                if (nop.lineNumber) {
                    html += '<span class="td-line" style="cursor:default">L' + nop.lineNumber + '</span>';
                } else {
                    html += '<span style="color:var(--text-muted)">-</span>';
                }
                html += '</td>';
                html += '<td>';
                if (nSt === 'NEW') html += '<span class="cv-badge new">&starf; New</span>';
                else if (nSt === 'CONFIRMED') html += '<span class="cv-badge confirmed">&check; Confirmed</span>';
                else if (nSt === 'REMOVED') html += '<span class="cv-badge removed">&cross; Removed</span>';
                else html += '<span class="cv-badge">' + PA.esc(nop.status || '?') + '</span>';
                html += '</td>';
                html += '<td class="cv-reason-cell">';
                if (nop.reason) {
                    html += '<span title="' + PA.escAttr(nop.reason) + '">' + PA.esc(nop.reason.length > 60 ? nop.reason.substring(0, 60) + '...' : nop.reason) + '</span>';
                } else {
                    html += '<span style="color:var(--text-muted)">-</span>';
                }
                html += '</td>';
                html += '<td>';
                var nUd = (nop.userDecision || '').toUpperCase();
                if (nUd === 'ACCEPTED') {
                    html += '<span class="cv-decision accepted">Accepted</span>';
                } else if (nUd === 'REJECTED') {
                    html += '<span class="cv-decision rejected">Rejected</span>';
                } else {
                    html += PA.tableDetail._reviewBtns(PA.tableDetail._tableName, nop.operation, nop.procedureName, nop.lineNumber);
                }
                html += '</td>';
                html += '</tr>';
            }

            html += '</tbody></table>' + pgNew.controls;
            html += '</div>';
        }

        body.innerHTML = html;
    },

    _reviewBtns(tableName, operation, procedureName, lineNumber) {
        var args = PA.escJs(tableName) + "','" + PA.escJs(operation || '') + "','" + PA.escJs(procedureName || '') + "'," + (lineNumber || 0);
        return '<span class="cv-review-btns">' +
            '<button class="cv-btn cv-btn-yes" onclick="event.stopPropagation();PA.tableDetail._submitReview(\'' + args + ',\'ACCEPTED\')" title="Accept">Yes</button>' +
            '<button class="cv-btn cv-btn-no" onclick="event.stopPropagation();PA.tableDetail._submitReview(\'' + args + ',\'REJECTED\')" title="Reject">No</button>' +
            '</span>';
    },

    async _submitReview(tableName, operation, procedureName, lineNumber, decision) {
        var analysisName = (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || '';
        if (!analysisName) return;
        try {
            await PA.api.claudeReview(analysisName, {
                decisions: [{ tableName: tableName, operation: operation, procedureName: procedureName, lineNumber: lineNumber, decision: decision }]
            });
            PA.tableDetail._claudeResult = null;
            PA.tableDetail._claudeResultAnalysis = null;
            await PA.tableDetail.open(PA.tableDetail._tableName, PA.tableDetail._schema, PA.tableDetail._currentTab || 'accesses');
        } catch(e) {
            console.error('Review save failed:', e);
        }
    },

    async _bulkReview(decision) {
        var analysisName = (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || '';
        if (!analysisName) return;
        try {
            await PA.api.claudeReview(analysisName, { bulk: decision });
            PA.tableDetail._claudeResult = null;
            PA.tableDetail._claudeResultAnalysis = null;
            await PA.tableDetail.open(PA.tableDetail._tableName, PA.tableDetail._schema, PA.tableDetail._currentTab || 'accesses');
        } catch(e) {
            console.error('Bulk review save failed:', e);
        }
    },

    _showTriggerDef(trigIdx) {
        var s = PA.tf.state('tdt');
        var tr = s ? s.filtered[trigIdx] : PA.tableDetail._triggers[trigIdx];
        if (!tr) return;

        var existing = document.getElementById('triggerDefModal');
        if (existing) existing.remove();

        var html = '<div class="trigger-popup trigger-def-modal" id="triggerDefModal">';
        html += '<div class="trigger-popup-header">';
        html += '<span>' + PA.esc(tr.name || '') + '</span>';
        html += '<button class="btn btn-sm" onclick="document.getElementById(\'triggerDefModal\').remove()">&times;</button>';
        html += '</div>';

        html += '<div class="trigger-meta-bar">';
        if (tr.timing) html += '<span class="trigger-timing">' + PA.esc(tr.timing) + '</span>';
        if (tr.event) html += '<span class="trigger-event">' + PA.esc(tr.event) + '</span>';
        html += '<span class="trigger-meta">ON ' + PA.esc(PA.tableDetail._tableName || '') + '</span>';
        if (tr.triggerType) html += '<span class="trigger-meta">' + PA.esc(tr.triggerType) + '</span>';
        html += '</div>';

        if (tr.tableOps && tr.tableOps.length > 0) {
            html += '<div class="trigger-section">';
            html += '<div class="trigger-section-title">Table Operations in Trigger</div>';
            for (var i = 0; i < tr.tableOps.length; i++) {
                var op = tr.tableOps[i];
                html += '<span class="op-badge ' + PA.esc(op.operation || '') + '" style="margin:2px 4px 2px 0">' + PA.esc(op.operation || '') + '</span>';
                html += '<span class="trigger-table-ref">' + PA.esc(op.tableName || '') + '</span>';
            }
            html += '</div>';
        }

        if (tr.definition) {
            html += '<div class="trigger-section">';
            html += '<div class="trigger-section-title">Source</div>';
            html += '<pre class="trigger-source">' + PA.esc(tr.definition) + '</pre>';
            html += '</div>';
        }

        html += '</div>';
        PA.mountOverlay(html);
        var tdmEl = document.getElementById('triggerDefModal');
        if (tdmEl && PA.modalStack) tdmEl.style.zIndex = PA.modalStack.nextZ();
    }
};
