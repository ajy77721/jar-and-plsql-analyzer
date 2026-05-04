PA.init = async function() {
    if (PA.home && PA.home.init) PA.home.init();
    await PA.loadHistory();
    window.addEventListener('popstate', function() { PA._handleRoute(); });
    PA._handleRoute();
    if (PA.claude && PA.claude.init) PA.claude.init();
};

PA.loadAnalysis = async function(name, opts) {
    var fromRoute = opts && opts.fromRoute;
    var routeProcId = opts && opts.procId;
    PA.toast('Loading ' + name + '...', 'success');
    try {
        PA.api.setAnalysis(name);
        var result = await PA.api.getIndex();
        if (!result) { PA.toast('Analysis not found', 'error'); return; }
        result.name = name;
        PA.analysisData = result;
        PA._rightTabLoaded = {};
        PA.goAnalysis();
        PA.updateTopbar(result);
        PA._renderLeftProcedures();
        PA._renderLeftTables();
        PA._cachedTablesIndex = null;
        PA._tablesIndexPromise = null;
        PA.toast('Loaded: ' + (result.entryPoint || name) + ' (' + (result.totalNodes || 0) + ' procs)', 'success');

        var targetHash = '#/analysis/' + encodeURIComponent(name);
        if (window.location.hash !== targetHash && !routeProcId) {
            if (fromRoute) { history.replaceState(null, '', targetHash); }
            else { history.pushState(null, '', targetHash); }
        }

        if (routeProcId) { PA.showProcedure(routeProcId, { fromRoute: true }); }
        else { PA._autoSelectFirstProc(result); }
    } catch (e) {
        PA.toast('Failed: ' + (e.message || e), 'error');
        if (fromRoute) history.replaceState(null, '', '#/home');
    }
};

PA._autoSelectFirstProc = function(result) {
    if (!result || !result.nodes || !result.nodes.length) return;
    var root = result.nodes.find(function(n) { return n.depth === 0; });
    if (!root) root = result.nodes[0];
    var id = root.nodeId || root.name;
    if (id) PA.showProcedure(id);
};

PA._tablesIndexPromise = null;
PA._ensureTablesIndex = function() {
    if (PA._cachedTablesIndex) return Promise.resolve(PA._cachedTablesIndex);
    if (PA._tablesIndexPromise) return PA._tablesIndexPromise;
    PA._tablesIndexPromise = PA.api.getTablesIndex().then(function(d) {
        PA._cachedTablesIndex = d;
        return d;
    }).catch(function() {
        PA._tablesIndexPromise = null;
        return null;
    });
    return PA._tablesIndexPromise;
};

PA.updateTopbar = function(result) {
    var title = document.getElementById('topbarTitle');
    var stats = document.getElementById('topbarStats');
    if (!title || !stats) return;
    title.textContent = result.entryPoint || result.name || '';
    var html = '';
    html += '<span style="margin-right:6px" title="Total procedures and functions analyzed in the call tree">' + (result.totalNodes || 0) + ' procs</span>';
    html += '<span style="margin-right:6px" title="Total unique database tables accessed by all procedures">' + (result.totalTables || 0) + ' tables</span>';
    html += '<span style="margin-right:6px" title="Total call edges between procedures in the dependency graph">' + (result.totalEdges || 0) + ' edges</span>';
    html += '<span style="margin-right:6px" title="Total lines of PL/SQL source code across all procedures">' + (result.totalLinesOfCode || 0).toLocaleString() + ' LOC</span>';
    html += '<span style="margin-right:6px" title="Maximum call depth — deepest nesting level in the call tree">depth ' + (result.maxDepth || 0) + '</span>';
    var errors = (result.errors && result.errors.length) || 0;
    if (errors > 0) html += '<span class="topbar-error-link" onclick="PA.errorModal.show()">' + errors + ' errors</span>';
    stats.innerHTML = html;
};

PA._showProcGen = 0;

PA._clearTabContents = function() {
    var ids = ['toContainer', 'joinContainer', 'jsContainer', 'curContainer', 'seqContainer',
               'stmtContainer', 'exhContainer', 'summaryContainer', 'claudeInsightsContainer',
               'claudeCorrectionsContainer', 'graphContainer'];
    for (var i = 0; i < ids.length; i++) {
        var el = document.getElementById(ids[i]);
        if (el) el.innerHTML = '';
    }
    if (PA.tf && PA.tf._state) {
        var tabKeys = ['to', 'join', 'js', 'cur', 'seq', 'stmt', 'exh'];
        for (var j = 0; j < tabKeys.length; j++) {
            delete PA.tf._state[tabKeys[j]];
        }
    }
    PA.summaryTab.teardown();
};

PA.showProcedure = async function(procId, opts) {
    var fromRoute = opts && opts.fromRoute;
    var gen = ++PA._showProcGen;

    PA.currentDetail = null;
    PA._rightTabLoaded = {};
    PA._clearTabContents();
    PA._scope = 'direct';
    PA._renderScopeToggle();
    PA.refs._loaded = false;
    PA.refs.callsItems = [];
    PA.refs.callersItems = [];
    PA.sourceModal._history = [];
    PA.sourceModal._historyFwd = [];
    PA.sourceModal._searchMatches = [];
    PA.sourceModal._searchIdx = -1;

    document.querySelectorAll('.lp-item').forEach(function(el) { el.classList.remove('active'); });
    var item = document.querySelector('.lp-item[data-id="' + CSS.escape(procId) + '"]');
    if (item) { item.classList.add('active'); item.scrollIntoView({ block: 'nearest' }); }

    PA.context.procId = procId;
    PA.switchRightTab('explore');

    if (PA.analysisData && PA.analysisData.name) {
        var targetHash = '#/analysis/' + encodeURIComponent(PA.analysisData.name) + '/proc/' + encodeURIComponent(procId);
        if (window.location.hash !== targetHash) {
            if (fromRoute) { history.replaceState(null, '', targetHash); }
            else { history.pushState(null, '', targetHash); }
        }
    }

    var node = PA._findNodeInData(procId);
    var detailFile = node ? node.detailFile : null;

    if (detailFile) {
        try {
            var detail = await PA.api.getNodeDetail(detailFile.replace(/^nodes\//, ''));
            if (gen !== PA._showProcGen) return;
            if (detail) {
                if (node) {
                    detail.nodeId = node.nodeId || procId;
                    detail.schema = detail.schema || node.schema;
                    detail.packageName = detail.packageName || node.packageName;
                    detail.objectType = detail.objectType || node.objectType;
                    detail.depth = detail.depth != null ? detail.depth : node.depth;
                    detail.lineStart = detail.lineStart || node.lineStart;
                    detail.lineEnd = detail.lineEnd || node.lineEnd;
                    detail.linesOfCode = detail.linesOfCode || node.linesOfCode;
                    detail.sourceFile = detail.sourceFile || node.sourceFile;
                    if (node.readable === false) detail.readable = false;
                    if (!detail.counts) detail.counts = node.counts || {};
                    else if (node.counts) {
                        if (!detail.counts.subtreeTablesCount && node.counts.subtreeTablesCount) detail.counts.subtreeTablesCount = node.counts.subtreeTablesCount;
                        if (!detail.counts.subtreeNodesCount && node.counts.subtreeNodesCount) detail.counts.subtreeNodesCount = node.counts.subtreeNodesCount;
                        if (!detail.counts.flowLinesOfCode && node.counts.flowLinesOfCode) detail.counts.flowLinesOfCode = node.counts.flowLinesOfCode;
                    }
                }
                PA.currentDetail = detail;
                PA._updateDetailHeader(detail);
                PA.context.procDetail = detail;
                PA.context.scopedTables = detail.tables || detail.subtreeTables || [];
                PA._rightTabLoaded = {};
            }
        } catch (e) {
            console.warn('[PA] Failed to load node detail for', procId, e);
        }
    }

    try {
        var treeData = await PA.api.getCallTree(procId);
        if (gen !== PA._showProcGen) return;
        if (treeData) {
            PA.context.callTreeNodeIds = PA._collectTreeNodeIds(treeData);
            PA.callTrace.treeData = treeData;
            PA.callTrace.breadcrumbStack = [{ id: treeData.id || procId, name: treeData.name || procId }];
            PA.callTrace.render(treeData);
            PA.trace.renderFromTree(treeData);
        } else {
            document.getElementById('ctContainer').innerHTML = '<div class="empty-msg">No call tree data for: ' + PA.esc(procId) + '</div>';
        }
    } catch (e) {
        console.error('[PA] callTrace.load failed for', procId, e);
        document.getElementById('ctContainer').innerHTML = '<div class="empty-msg">Failed to load call tree for: ' + PA.esc(procId) + '</div>';
    }
};

PA._findNodeInData = function(procId) {
    if (!PA.analysisData || !PA.analysisData.nodes) return null;
    var nodes = PA.analysisData.nodes;
    for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].nodeId === procId || nodes[i].name === procId) return nodes[i];
    }
    return null;
};

PA._loadNodeDetailTimer = null;
PA.loadNodeDetail = function(procId) {
    if (PA.context.procId === procId) return;
    clearTimeout(PA._loadNodeDetailTimer);
    PA._loadNodeDetailTimer = setTimeout(async function() {
        var node = PA._findNodeInData(procId);
        var detailFile = node ? node.detailFile : null;
        if (!detailFile) return;
        try {
            var detail = await PA.api.getNodeDetail(detailFile.replace(/^nodes\//, ''));
            if (detail) {
                if (node) {
                    detail.nodeId = node.nodeId || procId;
                    detail.schema = detail.schema || node.schema;
                    detail.objectType = detail.objectType || node.objectType;
                    detail.depth = detail.depth != null ? detail.depth : node.depth;
                    detail.lineStart = detail.lineStart || node.lineStart;
                    detail.lineEnd = detail.lineEnd || node.lineEnd;
                    detail.linesOfCode = detail.linesOfCode || node.linesOfCode;
                    detail.sourceFile = detail.sourceFile || node.sourceFile;
                    detail.packageName = detail.packageName || node.packageName;
                    if (node.readable === false) detail.readable = false;
                    if (!detail.counts) detail.counts = node.counts || {};
                    else if (node.counts) {
                        if (!detail.counts.subtreeTablesCount && node.counts.subtreeTablesCount) detail.counts.subtreeTablesCount = node.counts.subtreeTablesCount;
                        if (!detail.counts.subtreeNodesCount && node.counts.subtreeNodesCount) detail.counts.subtreeNodesCount = node.counts.subtreeNodesCount;
                        if (!detail.counts.flowLinesOfCode && node.counts.flowLinesOfCode) detail.counts.flowLinesOfCode = node.counts.flowLinesOfCode;
                    }
                }
                PA.currentDetail = detail;
                PA._updateDetailHeader(detail);
                PA.context.procId = procId;
                PA.context.procDetail = detail;
                PA.context.scopedTables = detail.tables || detail.subtreeTables || [];
                PA._rightTabLoaded = {};

                document.querySelectorAll('.lp-item').forEach(function(el) { el.classList.remove('active'); });
                var item = document.querySelector('.lp-item[data-id="' + CSS.escape(procId) + '"]');
                if (item) item.classList.add('active');
            }
        } catch (e) {
            console.warn('[PA] loadNodeDetail failed for', procId, e);
        }
    }, 150);
};

PA._heavyTabs = { tableOps: 'toContainer', cursors: 'curContainer',
    statements: 'stmtContainer', exHandlers: 'exhContainer',
    complexity: 'cxContainer', callGraphViz: 'graphContainer' };
PA._heavyTabTfKeys = { tableOps: 'to', cursors: 'cur',
    statements: 'stmt', exHandlers: 'exh', complexity: 'cx' };

PA._detailsSubLoaded = { sequences: false, joinQuery: false };
PA._detailsCurrentSub = 'sequences';

PA._exploreCurrentSub = 'hierarchy';

PA.exploreTab = {
    switchSub: function(sub) {
        PA._exploreCurrentSub = sub;
        document.querySelectorAll('#viewExplore .dt-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.exview === sub);
        });
        document.querySelectorAll('#viewExplore .dt-subview').forEach(function(v) { v.classList.remove('active'); });
        var viewMap = { hierarchy: 'exViewHierarchy', trace: 'exViewTrace', references: 'exViewReferences' };
        var el = document.getElementById(viewMap[sub]);
        if (el) el.classList.add('active');
        PA.exploreTab._loadSub(sub);
    },
    _loadSub: function(sub) {
        if (sub === 'references' && PA.context.procId && !PA.refs._loaded) {
            PA.refs.load(PA.context.procId);
        }
    },
    applyScope: function() {
        if (PA._exploreCurrentSub === 'references' && PA.refs._loaded) {
            PA.refs._buildTable();
        }
    }
};

PA._summaryCurrentSub = 'dashboard';
PA._summarySubLoaded = { dashboard: false, claudeInsights: false, claudeCorrections: false };

PA.summaryTab = {
    switchSub: function(sub) {
        PA._summaryCurrentSub = sub;
        document.querySelectorAll('#viewSummary .dt-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.sumview === sub);
        });
        document.querySelectorAll('#viewSummary .dt-subview').forEach(function(v) { v.classList.remove('active'); });
        var viewMap = { dashboard: 'sumViewDashboard', claudeInsights: 'sumViewClaudeInsights', claudeCorrections: 'sumViewClaudeCorrections' };
        var el = document.getElementById(viewMap[sub]);
        if (el) el.classList.add('active');
        PA.summaryTab._loadSub(sub);
    },
    _loadSub: function(sub) {
        if (sub === 'dashboard' && !PA._summarySubLoaded.dashboard) {
            PA._summarySubLoaded.dashboard = true;
            if (PA.summary && PA.summary.load) PA.summary.load();
        }
        if (sub === 'claudeInsights' && !PA._summarySubLoaded.claudeInsights) {
            PA._summarySubLoaded.claudeInsights = true;
            if (PA.claudeInsights && PA.claudeInsights.load) PA.claudeInsights.load();
        }
        if (sub === 'claudeCorrections' && !PA._summarySubLoaded.claudeCorrections) {
            PA._summarySubLoaded.claudeCorrections = true;
            if (PA.claudeCorrections && PA.claudeCorrections.load) PA.claudeCorrections.load();
        }
    },
    teardown: function() {
        PA._summarySubLoaded = { dashboard: false, claudeInsights: false, claudeCorrections: false };
    }
};

PA.detailsTab = {
    switchSub: function(sub) {
        PA._detailsCurrentSub = sub;
        document.querySelectorAll('#viewDetails .dt-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.dtview === sub);
        });
        document.querySelectorAll('#viewDetails .dt-subview').forEach(function(v) { v.classList.remove('active'); });
        var viewMap = { sequences: 'dtViewSequences', joinQuery: 'dtViewJoinQuery' };
        var el = document.getElementById(viewMap[sub]);
        if (el) el.classList.add('active');
        PA.detailsTab._loadSub(sub);
    },
    _loadSub: function(sub) {
        if (sub === 'sequences' && !PA._detailsSubLoaded.sequences) {
            PA._detailsSubLoaded.sequences = true;
            if (PA.sequences && PA.sequences.load) PA.sequences.load();
        }
        if (sub === 'joinQuery' && !PA._detailsSubLoaded.joinQuery) {
            PA._detailsSubLoaded.joinQuery = true;
            if (PA.joinSummary && PA.joinSummary.loadQueryOnly) PA.joinSummary.loadQueryOnly();
        }
    },
    teardown: function() {
        var subs = [
            { key: 'sequences', container: 'seqContainer', tfKey: 'seq' },
            { key: 'joinQuery', container: 'jqContainer', tfKey: 'jq' }
        ];
        for (var i = 0; i < subs.length; i++) {
            var s = subs[i];
            if (PA._detailsSubLoaded[s.key]) {
                var cel = document.getElementById(s.container);
                if (cel) cel.innerHTML = '';
                if (s.tfKey && PA.tf && PA.tf._state) delete PA.tf._state[s.tfKey];
                PA._detailsSubLoaded[s.key] = false;
            }
        }
    },
    applyScope: function() {
        if (PA._detailsSubLoaded.sequences && PA.sequences && PA.sequences.applyScope) PA.sequences.applyScope();
        if (PA._detailsSubLoaded.joinQuery && PA.joinSummary && PA.joinSummary.applyScope) PA.joinSummary.applyScope();
    }
};

PA.switchRightTab = function(view) {
    var prev = PA._currentRightTab;
    if (prev && prev !== view) {
        if (prev === 'details') {
            PA.detailsTab.teardown();
        } else if (prev === 'summary') {
            PA.summaryTab.teardown();
        } else if (PA._heavyTabs[prev] && PA._rightTabLoaded[prev]) {
            var cid = PA._heavyTabs[prev];
            var cel = document.getElementById(cid);
            if (cel) cel.innerHTML = '';
            var tfKey = PA._heavyTabTfKeys[prev];
            if (tfKey && PA.tf && PA.tf._state) delete PA.tf._state[tfKey];
            PA._rightTabLoaded[prev] = false;
        }
    }

    document.querySelectorAll('.rtab').forEach(function(t) { t.classList.remove('active'); });
    var btn = document.querySelector('.rtab[data-view="' + view + '"]');
    if (btn) btn.classList.add('active');
    document.querySelectorAll('.right-view').forEach(function(v) { v.classList.remove('active'); });
    var viewId = {
        explore: 'viewExplore', tableOps: 'viewTableOps',
        /* callTrace: 'viewCallTrace', trace: 'viewTrace', refs: 'viewRefs', */
        details: 'viewDetails', cursors: 'viewCursors',
        statements: 'viewStatements', exHandlers: 'viewExHandlers',
        summary: 'viewSummary', complexity: 'viewComplexity', callGraphViz: 'viewCallGraphViz', source: 'viewSource'
    }[view];
    var el = document.getElementById(viewId);
    if (el) el.classList.add('active');

    if (view === 'tableOps') {
        if (!PA._rightTabLoaded.tableOps && PA.tableOps && PA.tableOps.load) {
            PA._rightTabLoaded.tableOps = true; PA.tableOps.load();
        }
    }
    if (view === 'details') {
        PA.detailsTab._loadSub(PA._detailsCurrentSub);
    }
    if (view === 'cursors') {
        if (!PA._rightTabLoaded.cursors && PA.cursors && PA.cursors.load) {
            PA._rightTabLoaded.cursors = true; PA.cursors.load();
        }
    }
    if (view === 'statements') {
        if (!PA._rightTabLoaded.statements && PA.statements && PA.statements.load) {
            PA._rightTabLoaded.statements = true; PA.statements.load();
        }
    }
    if (view === 'summary') {
        if (!PA._rightTabLoaded.summary) {
            PA._rightTabLoaded.summary = true;
        }
        PA.summaryTab._loadSub(PA._summaryCurrentSub);
    }
    if (view === 'callGraphViz') {
        if (!PA._rightTabLoaded.callGraphViz && PA.callGraphViz && PA.callGraphViz.load) {
            PA._rightTabLoaded.callGraphViz = true; PA.callGraphViz.load();
        }
    }
    if (view === 'exHandlers') {
        if (!PA._rightTabLoaded.exHandlers && PA.exHandlers && PA.exHandlers.load) {
            PA._rightTabLoaded.exHandlers = true; PA.exHandlers.load();
        }
    }
    if (view === 'complexity') {
        if (!PA._rightTabLoaded.complexity && PA.complexity && PA.complexity.load) {
            PA._rightTabLoaded.complexity = true; PA.complexity.load();
        }
    }
    if (view === 'source' && PA.currentDetail && !PA._skipSourceAutoLoad) {
        var sf = PA.currentDetail.sourceFile;
        var startLine = PA.currentDetail.lineStart || 0;
        if (sf) PA.sourceView.open(sf, startLine);
    }
    if (view === 'explore') {
        PA.exploreTab._loadSub(PA._exploreCurrentSub);
    }
    /* if (view === 'refs' && PA.context.procId) {
        PA.refs._loaded = false;
        PA.refs.load(PA.context.procId);
    } */

    PA._skipSourceAutoLoad = false;
    PA._currentRightTab = view;
};

PA.trace = {
    flatNodes: [],
    _rendered: 0,
    _batchSize: 200,
    _maxDepth: 1,
    renderFromTree: function(tree) {
        PA.trace.flatNodes = [];
        PA.trace._flatten(tree, 0);
        PA.trace._rendered = 0;
        var searchBox = document.getElementById('traceSearch');
        if (searchBox) searchBox.value = '';
        PA.trace._render();
    },
    _flatten: function(node, depth, parentSourceFile) {
        PA.trace.flatNodes.push(Object.assign({}, node, { _depth: depth, _parentSourceFile: parentSourceFile || '' }));
        var sf = node.sourceFile || '';
        if (node.children && !node.circular) {
            for (var i = 0; i < node.children.length; i++) PA.trace._flatten(node.children[i], depth + 1, sf);
        }
    },
    _renderRow: function(n, i, maxDepth) {
        var schema = n.schemaName || '';
        var pkg = n.packageName || '';
        var name = n.name || n.id || '?';
        var fullName = [schema, pkg, name].filter(Boolean).join('.');
        var colorObj = PA.getSchemaColor(schema);
        var barWidth = Math.max(20, ((n._depth + 1) / (maxDepth + 1)) * 200);
        var callType = n.callType || '';
        var startL = n.startLine || 0;
        var endL = n.endLine || 0;
        var callLine = n.callLineNumber || 0;
        var sf = n.sourceFile || '';

        var html = '<div class="trace-row" data-depth="' + n._depth + '">';
        html += '<span class="ct-depth-badge" data-tip="Call depth ' + n._depth + '">L' + n._depth + '</span>';
        html += '<span class="trace-step" style="color:' + colorObj.fg + '">' + (i + 1) + '</span>';
        html += '<span class="trace-bar" style="width:' + barWidth + 'px;background:' + colorObj.bg + ';margin-left:' + (n._depth * 8) + 'px"></span>';
        if (schema) html += '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '" data-tip="Owner schema">' + PA.esc(schema) + '</span>';
        if (n.readable === false) html += '<span class="ct-lock" data-tip="Encrypted/wrapped">&#128274;</span>';
        html += '<span class="trace-name" data-tip="Click to view source" onclick="PA.sourceView.openModal(\'' + PA.escJs(n.id || '') + '\'); event.stopPropagation();">' + PA.esc(fullName) + '</span>';
        html += '<span class="trace-info">';
        if (startL > 0) {
            var bodyLabel = (endL > 0 && endL !== startL) ? ('L' + startL + '-' + endL) : ('L' + startL);
            html += '<span class="ct-line" data-tip="Open source at line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + startL + '); event.stopPropagation();">' + bodyLabel + '</span>';
        }
        if (callLine > 0 && n._parentSourceFile) {
            html += '<span class="ct-call-line" data-tip="Called at line ' + callLine + '" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(n._parentSourceFile) + '\', ' + callLine + '); event.stopPropagation();">@' + callLine + '</span>';
        }
        if (n.circular) html += '<span class="ct-call-badge CIRCULAR">CIRCULAR</span>';
        else if (callType) html += '<span class="ct-call-badge ' + PA.escAttr(callType) + '">' + PA.esc(callType) + '</span>';
        html += '</span></div>';
        return html;
    },
    _render: function() {
        var container = document.getElementById('traceContainer');
        var nodes = PA.trace.flatNodes;
        if (!nodes || nodes.length === 0) { container.innerHTML = '<div class="empty-msg">No trace data</div>'; return; }
        PA.trace._maxDepth = 1;
        for (var k = 0; k < nodes.length; k++) { if (nodes[k]._depth > PA.trace._maxDepth) PA.trace._maxDepth = nodes[k]._depth; }

        container.innerHTML = '<div id="traceRows"></div>';
        PA.trace._rendered = 0;
        PA.trace._renderBatch();
    },
    _renderBatch: function() {
        var rowsContainer = document.getElementById('traceRows');
        if (!rowsContainer) return;
        var nodes = PA.trace.flatNodes;
        var start = PA.trace._rendered;
        var end = Math.min(start + PA.trace._batchSize, nodes.length);
        var html = '';
        for (var i = start; i < end; i++) {
            html += PA.trace._renderRow(nodes[i], i, PA.trace._maxDepth);
        }
        var existing = rowsContainer.querySelector('.trace-show-more');
        if (existing) existing.remove();
        rowsContainer.insertAdjacentHTML('beforeend', html);
        PA.trace._rendered = end;

        if (end < nodes.length) {
            var remaining = nodes.length - end;
            rowsContainer.insertAdjacentHTML('beforeend',
                '<div class="trace-show-more" onclick="PA.trace._renderBatch()" style="padding:8px;text-align:center;font-size:11px;color:var(--accent);cursor:pointer;border-top:1px solid var(--border)">Show more (' + remaining + ' remaining of ' + nodes.length + ' total)</div>');
        }
    },
    _renderAll: function() {
        while (PA.trace._rendered < PA.trace.flatNodes.length) {
            PA.trace._renderBatch();
        }
    },
    search: function(query) {
        var q = (query || '').trim().toLowerCase();
        if (q && PA.trace._rendered < PA.trace.flatNodes.length) {
            PA.trace._renderAll();
        }
        var rows = document.querySelectorAll('#traceContainer .trace-row');
        if (!q) { rows.forEach(function(r) { r.style.display = ''; r.classList.remove('ct-search-hit'); }); return; }
        rows.forEach(function(r) {
            var name = (r.querySelector('.trace-name') ? r.querySelector('.trace-name').textContent : '').toLowerCase();
            var schema = (r.querySelector('.ct-schema-badge') ? r.querySelector('.ct-schema-badge').textContent : '').toLowerCase();
            var hit = name.includes(q) || schema.includes(q);
            r.style.display = hit ? '' : 'none';
            r.classList.toggle('ct-search-hit', hit);
        });
    },
    expandAll: function() {
        if (PA.trace._rendered < PA.trace.flatNodes.length) PA.trace._renderAll();
        document.querySelectorAll('#traceContainer .trace-row').forEach(function(r) { r.style.display = ''; });
    },
    collapseAll: function() {
        document.querySelectorAll('#traceContainer .trace-row').forEach(function(r) {
            r.style.display = (parseInt(r.dataset.depth, 10) > 0) ? 'none' : '';
        });
    }
};


PA.globalSearch = function(query) {
    if (!query || query.length < 2) {
        document.querySelectorAll('.left-panel-list .lp-item').forEach(function(item) { item.style.display = ''; });
        return;
    }
    var q = query.toLowerCase();
    document.querySelectorAll('.left-panel-list .lp-item').forEach(function(item) {
        item.style.display = (item.dataset.filter || '').toLowerCase().includes(q) ? '' : 'none';
    });
};

PA.pullData = async function() {
    if (!PA.analysisData || !PA.analysisData.name) { PA.toast('No analysis loaded', 'error'); return; }
    PA.toast('Pulling latest data...', 'success');
    try {
        PA.api.setAnalysis(PA.analysisData.name);
        var result = await PA.api.getIndex();
        if (!result) { PA.toast('Analysis not found', 'error'); return; }
        result.name = PA.analysisData.name;
        PA.analysisData = result;
        PA._cachedTablesIndex = null;
        PA._tablesIndexPromise = null;
        PA.updateTopbar(result);
        PA._renderLeftProcedures();
        PA._renderLeftTables();
        PA.toast('Data refreshed (' + (result.totalNodes || 0) + ' procs)', 'success');
    } catch(e) {
        PA.toast('Pull failed: ' + (e.message || e), 'error');
    }
};

PA.refreshTab = function() {
    var tab = PA._currentRightTab;
    if (!tab) return;
    if (tab === 'details') {
        PA.detailsTab.teardown();
        PA.detailsTab._loadSub(PA._detailsCurrentSub);
        PA.toast('Refreshed: Details/' + PA._detailsCurrentSub, 'success');
        return;
    }
    if (tab === 'explore') {
        if (PA._exploreCurrentSub === 'references') {
            PA.refs._loaded = false;
            PA.refs.load(PA.context.procId);
        }
        PA.toast('Refreshed: Explore/' + PA._exploreCurrentSub, 'success');
        return;
    }
    PA._rightTabLoaded[tab] = false;
    var cid = PA._heavyTabs[tab];
    if (cid) {
        var cel = document.getElementById(cid);
        if (cel) cel.innerHTML = '';
    }
    var tfKey = PA._heavyTabTfKeys[tab];
    if (tfKey && PA.tf && PA.tf._state) delete PA.tf._state[tfKey];
    PA.switchRightTab(tab);
    PA.toast('Refreshed: ' + tab, 'success');
};

PA.onScopeChange(function() {
    PA._dhRenderStats();
    var tab = PA._currentRightTab;
    if (tab === 'tableOps' && PA._rightTabLoaded.tableOps && PA.tableOps && PA.tableOps.applyScope) PA.tableOps.applyScope();
    if (tab === 'details') PA.detailsTab.applyScope();
    else if (tab === 'cursors' && PA._rightTabLoaded.cursors && PA.cursors.applyScope) PA.cursors.applyScope();
    else if (tab === 'statements' && PA._rightTabLoaded.statements && PA.statements.applyScope) PA.statements.applyScope();
    else if (tab === 'exHandlers' && PA._rightTabLoaded.exHandlers && PA.exHandlers.applyScope) PA.exHandlers.applyScope();
    else if (tab === 'summary' && PA._rightTabLoaded.summary && PA.summary && PA.summary.applyScope) PA.summary.applyScope();
    else if (tab === 'complexity' && PA._rightTabLoaded.complexity && PA.complexity && PA.complexity.applyScope) PA.complexity.applyScope();
    else if (tab === 'callGraphViz' && PA._rightTabLoaded.callGraphViz && PA.callGraphViz && PA.callGraphViz.applyScope) PA.callGraphViz.applyScope();
    if (tab === 'explore') PA.exploreTab.applyScope();
    /* if (tab === 'refs' && PA.refs._loaded) PA.refs._buildTable(); */
});

document.addEventListener('DOMContentLoaded', function() { PA.init(); });
