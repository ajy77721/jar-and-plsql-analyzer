window.PA = window.PA || {};

PA.tableDetail = {
    _data: null,
    _tableName: '',
    _schema: '',
    _activeOps: new Set(),
    _allAccess: [],
    _joins: [],
    _columns: [],
    _triggers: [],
    _currentTab: 'accesses',
    _metadata: null,
    _metadataLoading: false,
    _claudeResult: null,
    _claudeResultAnalysis: null,

    async open(tableName, schema, initialTab) {
        if (!tableName) return;
        PA.tableDetail._tableName = tableName;
        PA.tableDetail._schema = schema || '';
        PA.tableDetail._activeOps.clear();
        PA.tableDetail._metadata = null;
        PA.tableDetail._metadataLoading = false;
        PA.tableDetail._currentTab = initialTab || 'accesses';

        var modal = document.getElementById('tableDetailModal');
        var title = document.getElementById('tdModalTitle');
        var infoBar = document.getElementById('tdInfoBar');
        var body = document.getElementById('tdModalBody');

        title.textContent = (schema ? schema + '.' : '') + tableName;
        body.innerHTML = '<div class="empty-msg">Loading...</div>';
        modal.style.display = '';

        var tableData = await PA.tableDetail._loadTableData(tableName);
        PA.tableDetail._data = tableData;
        PA.tableDetail._allAccess = tableData.accesses || [];
        PA.tableDetail._joins = tableData.joins || [];
        PA.tableDetail._columns = tableData.columns || [];
        PA.tableDetail._triggers = tableData.triggers || [];

        var ops = tableData.allOperations || [];
        var totalAccess = PA.tableDetail._allAccess.length;
        infoBar.innerHTML =
            '<span style="font-weight:600;color:var(--teal)">' + PA.esc(tableName) + '</span>' +
            (schema ? '<span style="color:var(--text-muted);margin-left:6px">(' + PA.esc(schema) + ')</span>' : '') +
            '<span style="margin-left:12px;color:var(--text-muted)">' + totalAccess + ' accesses</span>' +
            (PA.tableDetail._joins.length ? '<span style="margin-left:8px;color:var(--text-muted)">' + PA.tableDetail._joins.length + ' joins</span>' : '') +
            (PA.tableDetail._triggers.length ? '<span style="margin-left:8px;color:var(--badge-orange)">' + PA.tableDetail._triggers.length + ' triggers</span>' : '') +
            '<span style="margin-left:8px">' + ops.map(function(op) { return '<span class="dh-op-badge ' + op + '" style="font-size:9px">' + op + '</span>'; }).join('') + '</span>';

        PA.tableDetail._initPills(ops);
        PA.tableDetail._renderTabs();
        PA.tableDetail._render();

        if (!PA.tableDetail._metadata) {
            PA.tableDetail._ensureMetadata().then(function() {
                PA.tableDetail._renderTabs();
            });
        }
    },

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        PA.tableDetail._cleanupTf();
        document.getElementById('tableDetailModal').style.display = 'none';
    },

    async _loadTableData(tableName) {
        var upper = tableName.toUpperCase();
        var result = { accesses: [], allOperations: [], joins: [], columns: [], triggers: [] };

        // --- 1. Get table operations data (transformed UI format) ---
        var tableIndex = PA.tableOps && PA.tableOps.data ? PA.tableOps.data : [];
        if (!tableIndex.length) {
            try { tableIndex = await PA.api.getTableOperations(); } catch(e) { tableIndex = []; }
        }

        var match = null;
        if (Array.isArray(tableIndex)) {
            match = tableIndex.find(function(t) { return (t.tableName || '').toUpperCase() === upper; });
        }

        if (match) {
            result.allOperations = match.operations || [];
            result.accesses = match.accessDetails || [];
        }

        // --- 2. Get raw tables API data (with triggers) ---
        var tablesApiData = null;
        try {
            var raw = PA._cachedTablesIndex || (await PA.api.getTablesIndex());
            if (raw && raw.tables) tablesApiData = raw.tables;
        } catch(e) {}

        var rawTblMatch = null;
        if (tablesApiData) {
            rawTblMatch = tablesApiData.find(function(t) { return (t.name || '').toUpperCase() === upper; });
            if (rawTblMatch && rawTblMatch.usedBy) {
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
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        var treeIds = PA.context.callTreeNodeIds;
        for (var ni = 0; ni < nodes.length; ni++) {
            var n = nodes[ni];
            if (n.objectType !== 'TRIGGER') continue;
            var nid = (n.nodeId || n.name || '').toUpperCase();
            if (treeIds && !treeIds.has(nid)) continue;
            var nKey = (n.name || n.nodeId || '').toUpperCase();
            if (triggerSeen[nKey]) continue;
            triggerSeen[nKey] = true;
            result.triggers.push({
                nodeId: n.nodeId || '',
                name: n.name || n.nodeId || '',
                schema: n.schema || '',
                timing: '',
                event: '',
                triggerType: '',
                status: '',
                source: 'PARSED',
                definition: '',
                tableOps: null
            });
        }

        // --- 5. Collect joins from current detail ---
        var detail = PA.currentDetail;
        if (detail) {
            var allTables = detail.tables || [];
            for (var ti = 0; ti < allTables.length; ti++) {
                var tbl = allTables[ti];
                if ((tbl.name || '').toUpperCase() !== upper) continue;
                if (tbl.joins) {
                    for (var ji = 0; ji < tbl.joins.length; ji++) {
                        var j = tbl.joins[ji];
                        j._sourceProc = detail.name || detail.nodeId || '';
                        j._sourceProcId = detail.nodeId || '';
                        result.joins.push(j);
                    }
                }
            }
        }

        // --- 6. Collect column references ---
        result.columns = await PA.tableDetail._collectColumnRefs(upper);

        // --- 7. Load Claude verification data ---
        try {
            var analysisName = (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || '';
            if (analysisName) {
                // Cache the Claude result per analysis so we only fetch once
                if (PA.tableDetail._claudeResultAnalysis !== analysisName || !PA.tableDetail._claudeResult) {
                    var claudeData = await PA.api.claudeResult(analysisName);
                    PA.tableDetail._claudeResult = claudeData;
                    PA.tableDetail._claudeResultAnalysis = analysisName;
                }
                var claudeResult = PA.tableDetail._claudeResult;
                if (claudeResult && claudeResult.tables) {
                    var claudeTable = claudeResult.tables.find(function(t) {
                        var cName = (t.tableName || '').toUpperCase();
                        return cName === upper || cName.endsWith('.' + upper);
                    });
                    if (claudeTable) result.claudeVerification = claudeTable;
                }
            }
        } catch(e) {
            // Claude data not available — silently continue
        }

        return result;
    },

    /** Aggregate column references from all node details for the given table. */
    async _collectColumnRefs(upperTableName) {
        var columns = [];
        var seen = {};

        var _addTypeRef = function(v, procName, procId, sourceFile) {
            if (!v.refTable || v.refTable.toUpperCase() !== upperTableName) return;
            var key = (procId || '') + '::' + (v.name || '') + '::' + (v.refField || '') + '::' + (v.line || 0);
            if (seen[key]) return;
            seen[key] = true;
            columns.push({
                varName: v.name || '',
                refField: v.refField || '',
                dataType: v.dataType || '',
                line: v.line || 0,
                procName: procName || '',
                procId: procId || '',
                sourceFile: sourceFile || '',
                refType: 'TYPE_REF'
            });
        };

        // From current detail
        var detail = PA.currentDetail;
        if (detail) {
            var vars = detail.variables || {};
            var allVars = [].concat(vars.typeRef || [], vars.rowtypeRef || [], vars.typeRefs || [], vars.rowtypeRefs || []);
            for (var vi = 0; vi < allVars.length; vi++) {
                _addTypeRef(allVars[vi], detail.name || '', detail.nodeId || '', detail.sourceFile || '');
            }
            // Parameters with %TYPE refs
            var params = detail.parameters || [];
            for (var pi = 0; pi < params.length; pi++) {
                var p = params[pi];
                if (p.dataType && p.dataType.toUpperCase().includes(upperTableName + '.')) {
                    var pField = p.dataType.replace(/%TYPE$/i, '').split('.').pop();
                    var pKey = (detail.nodeId || '') + '::param::' + p.name + '::' + pField;
                    if (!seen[pKey]) {
                        seen[pKey] = true;
                        columns.push({
                            varName: p.name || '',
                            refField: pField || '',
                            dataType: p.dataType || '',
                            line: p.line || 0,
                            procName: detail.name || '',
                            procId: detail.nodeId || '',
                            sourceFile: detail.sourceFile || '',
                            refType: 'PARAMETER'
                        });
                    }
                }
            }
        }

        // From all nodes in analysis (parallel fetch for performance)
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        var treeIds = PA.context.callTreeNodeIds;
        var nodesToFetch = [];
        for (var ni = 0; ni < nodes.length; ni++) {
            var n = nodes[ni];
            var nid = (n.nodeId || '').toUpperCase();
            if (detail && nid === (detail.nodeId || '').toUpperCase()) continue;
            if (treeIds && !treeIds.has(nid)) continue;
            nodesToFetch.push(n);
        }

        var _processNodeDetail = function(nd, fallbackNode) {
            if (!nd) return;
            var ndVars = nd.variables || {};
            var ndAllVars = [].concat(ndVars.typeRef || [], ndVars.rowtypeRef || [], ndVars.typeRefs || [], ndVars.rowtypeRefs || []);
            for (var nvi = 0; nvi < ndAllVars.length; nvi++) {
                _addTypeRef(ndAllVars[nvi], nd.name || fallbackNode.name || '', nd.nodeId || fallbackNode.nodeId || '', nd.sourceFile || fallbackNode.sourceFile || '');
            }
            var ndParams = nd.parameters || [];
            for (var npi = 0; npi < ndParams.length; npi++) {
                var np = ndParams[npi];
                if (np.dataType && np.dataType.toUpperCase().includes(upperTableName + '.')) {
                    var npField = np.dataType.replace(/%TYPE$/i, '').split('.').pop();
                    var npKey = (nd.nodeId || fallbackNode.nodeId || '') + '::param::' + np.name + '::' + npField;
                    if (!seen[npKey]) {
                        seen[npKey] = true;
                        columns.push({
                            varName: np.name || '',
                            refField: npField || '',
                            dataType: np.dataType || '',
                            line: np.line || 0,
                            procName: nd.name || fallbackNode.name || '',
                            procId: nd.nodeId || fallbackNode.nodeId || '',
                            sourceFile: nd.sourceFile || fallbackNode.sourceFile || '',
                            refType: 'PARAMETER'
                        });
                    }
                }
            }
        };

        // Fetch all node details in parallel
        var fetchPromises = nodesToFetch.map(function(n) {
            return PA.api.getNodeDetail(n.nodeId + '.json').catch(function() { return null; });
        });
        var nodeDetails = await Promise.all(fetchPromises);
        for (var fi = 0; fi < nodeDetails.length; fi++) {
            _processNodeDetail(nodeDetails[fi], nodesToFetch[fi]);
        }

        // Extract column names from join conditions
        if (detail) {
            var detTables = detail.tables || [];
            for (var dti = 0; dti < detTables.length; dti++) {
                var dtbl = detTables[dti];
                if ((dtbl.name || '').toUpperCase() !== upperTableName) continue;
                var dtJoins = dtbl.joins || [];
                for (var dji = 0; dji < dtJoins.length; dji++) {
                    var dj = dtJoins[dji];
                    if (!dj.condition) continue;
                    var condCols = PA.tableDetail._extractColumnsFromCondition(dj.condition, upperTableName);
                    for (var ci = 0; ci < condCols.length; ci++) {
                        var cKey = 'join::' + (detail.nodeId || '') + '::' + condCols[ci] + '::' + (dj.line || 0);
                        if (!seen[cKey]) {
                            seen[cKey] = true;
                            columns.push({
                                varName: '-',
                                refField: condCols[ci],
                                dataType: '',
                                line: dj.line || 0,
                                procName: detail.name || '',
                                procId: detail.nodeId || '',
                                sourceFile: detail.sourceFile || '',
                                refType: 'JOIN_CONDITION'
                            });
                        }
                    }
                }
            }
        }

        return columns;
    },

    /** Extract column names from a join condition string that reference the given table. */
    _extractColumnsFromCondition(condition, upperTableName) {
        if (!condition) return [];
        var cols = [];
        var seen = {};
        // Match patterns like TABLE.COLUMN or ALIAS.COLUMN
        var regex = /\b([A-Z_][A-Z0-9_$#]*)\s*\.\s*([A-Z_][A-Z0-9_$#]*)\b/gi;
        var m;
        while ((m = regex.exec(condition)) !== null) {
            var tbl = m[1].toUpperCase();
            var col = m[2].toUpperCase();
            if (tbl === upperTableName && !seen[col]) {
                seen[col] = true;
                cols.push(col);
            }
        }
        return cols;
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
        var items = [
            { id: 'accesses', label: 'Accesses', count: PA.tableDetail._allAccess.length },
            { id: 'dbcolumns', label: 'Columns', count: colCount },
            { id: 'dbindexes', label: 'Indexes', count: idxCount },
            { id: 'dbconstraints', label: 'Constraints', count: conCount },
            { id: 'joins', label: 'Joins', count: PA.tableDetail._joins.length },
            { id: 'triggers', label: 'Triggers', count: PA.tableDetail._triggers.length },
            { id: 'claude', label: 'Claude', count: cvData ? (cvData.claudeVerifications || []).length : 0 }
        ];
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
        PA.tableDetail._currentTab = tab;
        PA.tableDetail._renderTabs();
        PA.tableDetail._render();
    },

    _initPills(ops) {
        var container = document.getElementById('tdOpPills');
        if (!container) return;
        var allOps = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        var html = '';
        for (var i = 0; i < allOps.length; i++) {
            var op = allOps[i];
            var has = ops.indexOf(op) >= 0;
            html += '<span class="op-filter-pill ' + op + (has ? ' active' : '') + '" data-op="' + op + '" onclick="PA.tableDetail.toggleOp(\'' + op + '\')" style="' + (has ? '' : 'opacity:0.3;pointer-events:none') + '">' + op + '</span>';
        }
        container.innerHTML = html;
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
            if (!sf && items[ei].procedureId && PA.analysisData && PA.analysisData.nodes) {
                var node = PA.analysisData.nodes.find(function(n) { return n.nodeId === items[ei].procedureId; });
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
            (cv ? '<th>Claude</th>' : '') +
            '</tr></thead><tbody id="tda-tbody"></tbody></table>' +
            '<div class="pagination-bar" id="tda-pager"></div>';

        PA.tf.init('tda', items, 50, function(d, idx, esc) {
            var cvMatch = d._cvMatch;
            var rowClass = 'to-row';
            if (cvMatch && (cvMatch.status || '').toUpperCase() === 'REMOVED') rowClass += ' cv-removed';
            var html = '<tr class="' + rowClass + '">';
            html += '<td><span class="dh-op-badge ' + esc(d.operation || '') + '" style="font-size:9px">' + esc(d.operation || '?') + '</span></td>';
            html += '<td><span class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(d.procedureId || d.procedureName || '') + '\')">' + esc(d.procedureName || '?') + '</span></td>';
            html += '<td>';
            if (d._sf && d.lineNumber) {
                html += '<span class="td-line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(d._sf) + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>';
            } else if (d.lineNumber) {
                html += '<span class="td-line" style="cursor:default">L' + d.lineNumber + '</span>';
            } else {
                html += '-';
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

    async _ensureMetadata() {
        if (PA.tableDetail._metadata || PA.tableDetail._metadataLoading) return;
        PA.tableDetail._metadataLoading = true;
        try {
            PA.tableDetail._metadata = await PA.api.dbTableInfo(PA.tableDetail._tableName, PA.tableDetail._schema);
        } catch (e) {
            PA.tableDetail._metadata = { found: false, error: e.message };
        }
        PA.tableDetail._metadataLoading = false;
        PA.tableDetail._renderTabs();
    },

    _metadataNotAvailable(body, countEl, label) {
        if (countEl) countEl.textContent = '0 ' + label;
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
        var dt = col.dataType || '';
        if (dt === 'NUMBER') {
            if (col.dataPrecision != null) {
                return 'NUMBER(' + col.dataPrecision + (col.dataScale ? ',' + col.dataScale : '') + ')';
            }
            return 'NUMBER';
        }
        if (dt === 'VARCHAR2' || dt === 'CHAR' || dt === 'NVARCHAR2' || dt === 'RAW') {
            return dt + '(' + (col.dataLength || '') + ')';
        }
        return dt;
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

    _renderJoins() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        var joins = PA.tableDetail._joins;

        if (!joins.length) {
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

        PA.tf.init('tdj', joins, 50, function(j, idx, esc) {
            var sf = PA.currentDetail ? PA.currentDetail.sourceFile || '' : '';
            var html = '<tr class="to-row">';
            html += '<td><span class="td-join-type">' + esc(j.joinType || 'JOIN') + '</span></td>';
            html += '<td style="font-weight:600;color:var(--teal)">' + esc(j.joinedTable || '?') + '</td>';
            html += '<td style="font-size:11px;color:var(--text-muted)">' + esc(j.joinedTableAlias || '-') + '</td>';
            html += '<td style="font-size:11px;font-family:var(--font-mono);max-width:300px;overflow:hidden;text-overflow:ellipsis" title="' + PA.escAttr(j.condition || '') + '">' + esc(j.condition || '-') + '</td>';
            html += '<td><span class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(j._sourceProcId || '') + '\')">' + esc(j._sourceProc || '-') + '</span></td>';
            html += '<td>';
            if (j.line) {
                html += '<span class="td-line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + j.line + ')">L' + j.line + '</span>';
            }
            html += '</td></tr>';
            return html;
        }, {
            sortKeys: {
                0: { fn: function(j) { return (j.joinType || '').toUpperCase(); } },
                1: { fn: function(j) { return (j.joinedTable || '').toUpperCase(); } },
                4: { fn: function(j) { return (j._sourceProc || '').toUpperCase(); } },
                5: { fn: function(j) { return j.line || 0; } }
            },
            searchFn: function(j, q) {
                return (j.joinType || '').toUpperCase().includes(q) ||
                       (j.joinedTable || '').toUpperCase().includes(q) ||
                       (j._sourceProc || '').toUpperCase().includes(q) ||
                       (j.condition || '').toUpperCase().includes(q);
            },
            onFilter: function() {
                var st = PA.tf.state('tdj');
                if (countEl) countEl.textContent = (st ? st.filtered.length : joins.length) + ' joins';
            }
        });

        PA.tf.filter('tdj');

        setTimeout(function() {
            PA.tf.initColFilters('tdj', {
                0: { label: 'Join Type', valueFn: function(j) { return j.joinType || 'JOIN'; } },
                1: { label: 'Joined Table', valueFn: function(j) { return j.joinedTable || ''; } },
                2: { label: 'Alias', valueFn: function(j) { return j.joinedTableAlias || '-'; } },
                4: { label: 'Procedure', valueFn: function(j) { return j._sourceProc || ''; } }
            });
        }, 0);
    },




    _renderTriggers() {
        var body = document.getElementById('tdModalBody');
        var countEl = document.getElementById('tdCount');
        var triggers = PA.tableDetail._triggers;

        if (!triggers.length) {
            if (countEl) countEl.textContent = '0 triggers';
            body.innerHTML = '<div class="empty-msg">No triggers found for this table</div>';
            return;
        }

        body.innerHTML =
            '<table class="to-table"><thead><tr>' +
            '<th data-sort-col="0" onclick="PA.tf.sort(\'tdt\',0)">Schema</th>' +
            '<th data-sort-col="1" onclick="PA.tf.sort(\'tdt\',1)">Trigger Name</th>' +
            '<th data-sort-col="2" onclick="PA.tf.sort(\'tdt\',2)">Timing</th>' +
            '<th data-sort-col="3" onclick="PA.tf.sort(\'tdt\',3)">Event</th>' +
            '<th>Type</th>' +
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
                html += '<span class="to-detail-proc" onclick="event.stopPropagation();PA.showProcedure(\'' + PA.escJs(t.nodeId) + '\')">' + esc(t.name || '?') + '</span>';
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
                       (t.triggerType || '').toUpperCase().includes(q);
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

        for (var ri = 0; ri < matchedRows.length; ri++) {
            var row = matchedRows[ri];
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
                html += '<span class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(acc.procedureId || acc.procedureName || '') + '\')">' + PA.esc(acc.procedureName || '?') + '</span>';
            } else {
                html += '<span style="color:var(--text-muted)">-</span>';
            }
            html += '</td>';
            // Line
            html += '<td>';
            if (acc.lineNumber) {
                var sf = acc.sourceFile || '';
                if (!sf && acc.procedureId && PA.analysisData && PA.analysisData.nodes) {
                    var node = PA.analysisData.nodes.find(function(n) { return n.nodeId === acc.procedureId; });
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

        html += '</tbody></table>';

        // NEW operations section (found by Claude, not in static)
        if (newOps.length > 0) {
            html += '<div class="td-group" style="margin-top:16px">';
            html += '<div class="td-group-title"><span class="cv-badge new">&starf; NEW</span> Found by Claude (not in static analysis)</div>';
            html += '<table class="to-table cv-compare-table"><thead><tr>';
            html += '<th>Operation</th><th>Procedure</th><th>Line</th><th>Status</th><th>Reason</th><th>Review</th>';
            html += '</tr></thead><tbody>';

            for (var ni = 0; ni < newOps.length; ni++) {
                var nop = newOps[ni];
                var nSt = (nop.status || '').toUpperCase();
                html += '<tr class="to-row cv-row-new">';
                html += '<td><span class="dh-op-badge ' + PA.esc(nop.operation || '') + '" style="font-size:9px">' + PA.esc(nop.operation || '?') + '</span></td>';
                html += '<td>';
                if (nop.procedureName) {
                    html += '<span class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(nop.procedureName || '') + '\')">' + PA.esc(nop.procedureName) + '</span>';
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

            html += '</tbody></table>';
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
        document.body.insertAdjacentHTML('beforeend', html);
    }
};
