window.PA = window.PA || {};

PA.complexity = {
    data: [],
    _activeRisks: new Set(),
    _weights: null,
    _thresholds: null,

    // Built-in defaults from the server config; overrides (per-analysis / default-for-all)
    // are layered on top and persisted in localStorage.
    _serverWeights: null,
    _serverThresholds: null,
    // Scoring algorithm: how each metric value is transformed before it is weighted.
    _algorithm: 'linear',
    _exponent: 1.3,

    _ALGORITHMS: {
        linear:      { label: 'Linear (weighted sum)',        formula: 'score = &sum;(weight &times; value)' },
        logarithmic: { label: 'Logarithmic (diminishing)',    formula: 'score = &sum;(weight &times; log&#8322;(1 + value))' },
        exponential: { label: 'Exponential (compounding)',    formula: 'score = &sum;(weight &times; ((1 + value)^p &minus; 1))' },
        balanced:    { label: 'Balanced (&radic; dampening)', formula: 'score = &sum;(weight &times; &radic;value)' }
    },

    _KEY_DEFAULT: 'PA_cxCfg_default',
    _keyForAnalysis: function() {
        return 'PA_cxCfg_' + (PA.api._analysisName || '__none__');
    },

    _readStore: function(key) {
        try { var v = localStorage.getItem(key); return v ? JSON.parse(v) : null; } catch(e) { return null; }
    },

    /** Transform one metric value by the active algorithm, then apply its weight. */
    _metricContribution: function(value, weight) {
        var v = value || 0, w = weight || 0;
        switch (PA.complexity._algorithm) {
            case 'logarithmic': return w * (Math.log(1 + v) / Math.LN2);
            case 'exponential': return w * (Math.pow(1 + v, PA.complexity._exponent || 1.3) - 1);
            case 'balanced':    return w * Math.sqrt(v);
            case 'linear':
            default:            return w * v;
        }
    },

    /**
     * Single source of truth for a node's score+risk from its raw metrics, using the active
     * weights/algorithm/thresholds. The call-tree risk badge (callGraph._calcRisk) calls this for
     * one node at a time while rendering, client-side. The complexity *table*'s own whole-corpus
     * version of this same formula moved server-side under UI-024 (`ComplexityScoringService`,
     * used by `_recompute`) — kept identical there field-for-field, so the two still never drift
     * for the non-linear formulas (logarithmic/exponential/balanced) either, just no longer via one
     * shared function call.
     * @param m {loc, tables, callsOut, cursors, dynSql, depth}
     */
    scoreRaw: function(m) {
        var w = PA.complexity._weights || {};
        var contrib = PA.complexity._metricContribution;
        var score = Math.round(
              contrib(m.loc || 0,      w.loc        != null ? w.loc        : 0.3)
            + contrib(m.tables || 0,   w.tables     != null ? w.tables     : 15)
            + contrib(m.callsOut || 0, w.callsOut   != null ? w.callsOut   : 10)
            + contrib(m.cursors || 0,  w.cursors    != null ? w.cursors    : 8)
            + contrib(m.dynSql || 0,   w.dynamicSql != null ? w.dynamicSql : 20)
            + contrib(m.depth || 0,    w.depth      != null ? w.depth      : 5)
        );
        var th = PA.complexity._thresholds || {};
        var thMed = th.medium != null ? th.medium : 50;
        var thHigh = th.high != null ? th.high : 150;
        var risk = score >= thHigh ? 'HIGH' : (score >= thMed ? 'MEDIUM' : 'LOW');
        return { score: score, risk: risk };
    },

    /** Resolve effective config: per-analysis override > default-for-all override > server built-in. */
    _resolveConfig: function() {
        var base = {
            weights: Object.assign({}, PA.complexity._serverWeights || {}),
            thresholds: Object.assign({}, PA.complexity._serverThresholds || {}),
            algorithm: 'linear',
            exponent: 1.3
        };
        var apply = function(o) {
            if (!o) return;
            if (o.weights) Object.assign(base.weights, o.weights);
            if (o.thresholds) Object.assign(base.thresholds, o.thresholds);
            if (o.algorithm) base.algorithm = o.algorithm;
            if (o.exponent != null) base.exponent = o.exponent;
        };
        apply(PA.complexity._readStore(PA.complexity._KEY_DEFAULT));
        apply(PA.complexity._readStore(PA.complexity._keyForAnalysis()));
        PA.complexity._weights = base.weights;
        PA.complexity._thresholds = base.thresholds;
        PA.complexity._algorithm = PA.complexity._ALGORITHMS[base.algorithm] ? base.algorithm : 'linear';
        PA.complexity._exponent = base.exponent || 1.3;
    },

    load: async function() {
        if (!PA.complexity._serverWeights) {
            try {
                var cfg = await PA.api.getComplexityConfig();
                if (cfg) {
                    PA.complexity._serverWeights = cfg.weights;
                    PA.complexity._serverThresholds = cfg.thresholds;
                }
            } catch(e) {}
        }
        PA.complexity._resolveConfig();
        await PA.complexity._recompute();
        PA.complexity._initRiskPills();
        PA.complexity._initTable();
        PA.complexity.applyScope();
        PA.complexity._applyLoadError();
    },

    /**
     * UI-024 (user decision: server-side, accept the debounce) — was a `PA.nodeIndex.all()` scan
     * plus a `_computeMetrics` pass over every node in the browser; both are now one fetch to
     * `ComplexityScoringService`, sending the current `_weights`/`_thresholds`/`_algorithm`/
     * `_exponent` (this file's own already-resolved effective config — server defaults layered
     * with `localStorage` overrides, `_resolveConfig`) as the request. `scoreRaw`/
     * `_metricContribution` above are untouched: `callGraph.js`'s call-tree risk badge scores one
     * node at a time, inline, during tree rendering, and stays client-side — this fix is only for
     * the whole-corpus table.
     */
    _recompute: async function() {
        try {
            PA.complexity.data = await PA.api.getComplexityScores(
                PA.complexity._weights, PA.complexity._thresholds,
                PA.complexity._algorithm, PA.complexity._exponent);
            // PA.api swallows a non-ok response and returns the PA.FAILED sentinel rather than
            // throwing (page-envelope.js), so the catch below alone would never fire on an HTTP
            // error — only on a network/parse throw. Both paths must reach the error row.
            if (PA.failed && PA.failed(PA.complexity.data)) {
                PA.complexity.data = [];
                PA.complexity._loadError = 'complexity/scores request failed';
                return;
            }
            PA.complexity._loadError = null;
        } catch (e) {
            // An empty array here used to reach the table as "No data found", so a dead
            // /complexity/scores call looked exactly like an analysis with no procedures in it.
            // The message is kept and re-applied after _initTable (which resets tf state).
            console.error('[complexity] scores failed', e);
            PA.complexity.data = [];
            PA.complexity._loadError = (e && e.message) ? e.message : String(e);
        }
    },

    /** Set by _recompute; re-applied to the table after every _initTable. */
    _loadError: null,

    /** Pushes (or clears) the error row on the complexity table. */
    _applyLoadError: function() {
        if (!PA.tf || !PA.tf.setLoadError) return;
        PA.tf.setLoadError('cx', PA.complexity._loadError, function() { PA.complexity._refresh(); });
    },

    _initRiskPills: function() {
        var container = document.getElementById('cxRiskPills');
        if (!container) return;
        var risks = [
            { k: 'LOW', label: 'Low', cls: 'low' },
            { k: 'MEDIUM', label: 'Medium', cls: 'medium' },
            { k: 'HIGH', label: 'High', cls: 'high' }
        ];
        var html = '';
        for (var i = 0; i < risks.length; i++) {
            var r = risks[i];
            html += '<span class="op-filter-pill active cx-pill-' + r.cls + '" data-risk="' + r.k + '" onclick="PA.complexity.toggleRisk(\'' + r.k + '\')">' + r.label + '</span>';
        }
        container.innerHTML = html;
        PA.complexity._activeRisks = new Set();
    },

    toggleRisk: function(r) {
        var pill = document.querySelector('#cxRiskPills .op-filter-pill[data-risk="' + r + '"]');
        if (!pill) return;
        if (PA.complexity._activeRisks.size === 0) {
            PA.complexity._activeRisks.add(r);
            document.querySelectorAll('#cxRiskPills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.risk === r);
            });
        } else if (PA.complexity._activeRisks.has(r)) {
            PA.complexity._activeRisks.delete(r);
            pill.classList.remove('active');
            if (PA.complexity._activeRisks.size === 0) {
                document.querySelectorAll('#cxRiskPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.complexity._activeRisks.add(r);
            pill.classList.add('active');
        }
        PA.tf.filter('cx');
    },

    _renderRiskSummary: function() {
        var el = document.getElementById('cxRiskSummary');
        if (!el) return;
        var data = PA.complexity.data;
        var counts = { LOW: 0, MEDIUM: 0, HIGH: 0 };
        for (var i = 0; i < data.length; i++) counts[data[i].risk] = (counts[data[i].risk] || 0) + 1;
        var total = data.length || 1;
        var items = [
            { k: 'HIGH', label: 'High', color: 'var(--badge-red)', bg: 'var(--badge-red-bg)' },
            { k: 'MEDIUM', label: 'Medium', color: 'var(--badge-orange)', bg: 'var(--badge-orange-bg)' },
            { k: 'LOW', label: 'Low', color: 'var(--badge-green)', bg: 'var(--badge-green-bg)' }
        ];
        var html = '<div class="cx-risk-summary">';
        for (var j = 0; j < items.length; j++) {
            var r = items[j];
            var c = counts[r.k] || 0;
            var pct = Math.round((c / total) * 100);
            html += '<div class="cx-risk-card cx-card-' + r.k.toLowerCase() + '" onclick="PA.complexity.toggleRisk(\'' + r.k + '\')" title="Click to filter ' + r.label + '">';
            html += '<div class="cx-risk-card-count" style="color:' + r.color + '">' + c + '</div>';
            html += '<div class="cx-risk-card-label">' + r.label + '</div>';
            html += '<div class="cx-risk-card-bar"><div style="width:' + pct + '%;background:' + r.color + ';height:100%;border-radius:var(--radius-sm)"></div></div>';
            html += '<div class="cx-risk-card-pct">' + pct + '%</div>';
            html += '</div>';
        }
        html += '</div>';
        el.innerHTML = html;
    },

    _initTable: function() {
        var container = document.getElementById('cxContainer');
        if (!container) return;

        var html = '<div id="cxRiskSummary"></div>';
        html += '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'cx\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'cx\',1)">Schema</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'cx\',2)">LOC</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'cx\',3)">Tables</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'cx\',4)">Dependencies</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'cx\',5)">Dynamic SQL</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'cx\',6)">Depth</th>';
        html += '<th data-sort-col="7" onclick="PA.tf.sort(\'cx\',7)">Score</th>';
        html += '<th data-sort-col="8" onclick="PA.tf.sort(\'cx\',8)">Risk</th>';
        html += '</tr></thead><tbody id="cx-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.complexity._renderRiskSummary();

        PA.tf.init('cx', PA.complexity.data, 50, PA.complexity._renderRow, {
            sortKeys: {
                0: { fn: function(i) { return (i.name || '').toUpperCase(); } },
                1: { fn: function(i) { return (i.schema || '').toUpperCase(); } },
                2: { fn: function(i) { return i.loc; } },
                3: { fn: function(i) { return i.tables; } },
                4: { fn: function(i) { return i.callsOut; } },
                5: { fn: function(i) { return i.dynSql; } },
                6: { fn: function(i) { return i.depth; } },
                7: { fn: function(i) { return i.score; } },
                8: { fn: function(i) { var order = { LOW: 0, MEDIUM: 1, HIGH: 2 }; return order[i.risk] || 0; } }
            },
            renderDetail: PA.complexity._renderDetail,
            searchFn: function(item, q) {
                return (item.name || '').toUpperCase().includes(q) ||
                    (item.schema || '').toUpperCase().includes(q) ||
                    (item.objectType || '').toUpperCase().includes(q);
            },
            extraFilter: PA.complexity._filter,
            onFilter: PA.complexity._updateCounts
        });

        var s = PA.tf.state('cx');
        if (s) { s.sortCol = 7; s.sortDir = 'desc'; }
        PA.tf.filter('cx');

        setTimeout(function() {
            PA.tf.initColFilters('cx', {
                0: { label: 'Procedure', valueFn: function(i) { return i.name || ''; } },
                1: { label: 'Schema', valueFn: function(i) { return i.schema || ''; } },
                2: { label: 'LOC', valueFn: function(i) { return i.loc <= 50 ? '0-50' : i.loc <= 200 ? '51-200' : i.loc <= 500 ? '201-500' : i.loc <= 1000 ? '501-1K' : '1K+'; } },
                3: { label: 'Tables', valueFn: function(i) { return String(i.tables); } },
                4: { label: 'Dependencies', valueFn: function(i) { return String(i.callsOut); } },
                5: { label: 'Dynamic SQL', valueFn: function(i) { return String(i.dynSql); } },
                6: { label: 'Depth', valueFn: function(i) { return String(i.depth); } },
                7: { label: 'Score', valueFn: function(i) { return i.score < 50 ? '0-49' : i.score < 150 ? '50-149' : '150+'; } },
                8: { label: 'Risk', valueFn: function(i) { return i.risk || ''; } }
            });
            PA.tf._updateSortIndicators('cx');
        }, 0);
    },

    _filter: function(item) {
        var risks = PA.complexity._activeRisks;
        if (risks.size > 0 && !risks.has(item.risk)) return false;
        return true;
    },

    _renderRow: function(item, idx, esc) {
        var riskCls = item.risk.toLowerCase();
        var scoreColor = item.risk === 'HIGH' ? 'var(--red)' :
                         item.risk === 'MEDIUM' ? 'var(--orange)' : 'var(--green)';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'cx\',' + idx + ')">';
        html += '<td><span class="cx-proc-name" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(item.nodeId) + '\')">' + esc(item.name) + '</span>';
        if (!item.readable) html += ' <span class="ct-lock" title="Encrypted/wrapped">&#128274;</span>';
        html += '</td>';
        html += '<td><span class="cx-schema">' + esc(item.schema) + '</span></td>';
        html += '<td class="cx-num">' + item.loc.toLocaleString() + '</td>';
        html += '<td class="cx-num">' + item.tables + '</td>';
        html += '<td class="cx-num">' + item.callsOut + '</td>';
        html += '<td class="cx-num">' + (item.dynSql > 0 ? '<span style="color:var(--orange);font-weight:700">' + item.dynSql + '</span>' : '0') + '</td>';
        html += '<td class="cx-num">' + item.depth + '</td>';
        html += '<td class="cx-num" style="font-weight:700;color:' + scoreColor + '">' + item.score + '</td>';
        html += '<td><span class="cx-risk ' + riskCls + '">' + item.risk + '</span></td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(item, idx, esc) {
        var w = item._w || {};
        var contrib = PA.complexity._metricContribution;
        var html = '<div class="to-detail">';

        var factors = [
            { label: 'Lines of Code', value: item.loc, weight: w.loc, contribution: Math.round(contrib(item.loc, w.loc)) },
            { label: 'Tables', value: item.tables, weight: w.tables, contribution: Math.round(contrib(item.tables, w.tables)) },
            { label: 'Outgoing Calls', value: item.callsOut, weight: w.callsOut, contribution: Math.round(contrib(item.callsOut, w.callsOut)) },
            { label: 'Cursors', value: item.cursors, weight: w.cursors, contribution: Math.round(contrib(item.cursors, w.cursors)) },
            { label: 'Dynamic SQL', value: item.dynSql, weight: w.dynSql, contribution: Math.round(contrib(item.dynSql, w.dynSql)) },
            { label: 'Call Depth', value: item.depth, weight: w.depth, contribution: Math.round(contrib(item.depth, w.depth)) }
        ];
        factors.sort(function(a, b) { return b.contribution - a.contribution; });
        var maxContrib = factors[0] ? factors[0].contribution : 1;

        var algoLabel = (PA.complexity._ALGORITHMS[PA.complexity._algorithm] || {}).label || 'Linear';
        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Score Breakdown — <span class="cx-risk ' + item.risk.toLowerCase() + '">' + item.risk + '</span> (' + item.score + ' points) <span style="color:var(--text-muted);font-weight:400;font-size:10px">· ' + esc(algoLabel) + '</span></div>';
        html += '<div class="cx-score-reason" style="font-size:11px;color:var(--text-muted);margin-bottom:8px">';
        var topFactors = factors.filter(function(f) { return f.contribution > 0; }).slice(0, 3);
        if (topFactors.length) {
            html += 'Top contributors: ' + topFactors.map(function(f) { return '<strong>' + f.label + '</strong> (' + f.value + ' &times; ' + f.weight + ' = ' + f.contribution + ')'; }).join(', ');
        }
        html += '</div>';
        html += '<div class="cx-stmt-grid">';
        for (var fi = 0; fi < factors.length; fi++) {
            var f = factors[fi];
            if (f.contribution === 0) continue;
            var pct = Math.round((f.contribution / (maxContrib || 1)) * 100);
            var barColor = f.contribution >= 100 ? 'var(--red)' : f.contribution >= 30 ? 'var(--orange)' : 'var(--blue)';
            html += '<div class="cx-stmt-row">';
            html += '<span class="cx-stmt-label">' + esc(f.label) + ' <span style="color:var(--text-muted)">(' + f.value + ' &times; ' + f.weight + ')</span></span>';
            html += '<div class="cx-stmt-bar"><div class="cx-stmt-bar-fill" style="width:' + pct + '%;background:' + barColor + '"></div></div>';
            html += '<span class="cx-stmt-val">' + f.contribution + '</span>';
            html += '</div>';
        }
        html += '</div></div>';

        /* Statement Breakdown and Details sections hidden per feedback */

        if (item.nodeId || item.sourceFile) {
            // Open the source POPUP modal (openModal), not the inline panel — the old
            // open(sourceFile) call only filled the hidden panel, so nothing appeared.
            html += '<div style="padding:6px 0">';
            html += '<span class="to-detail-line" onclick="event.stopPropagation(); PA.sourceView.openModal(\'' + PA.escJs(item.nodeId) + '\')" title="Open source in a popup">View Source</span>';
            html += '</div>';
        }

        html += '</div>';
        return html;
    },

    _kv: function(label, value) {
        return '<div class="cx-kv"><span class="cx-kv-label">' + label + '</span><span class="cx-kv-value">' + value + '</span></div>';
    },

    onSearch: function() {
        var el = document.getElementById('cxSearch');
        PA.tf.setSearch('cx', el ? el.value : '');
        PA.tf.filter('cx');
    },

    clearFilters: function() {
        var el = document.getElementById('cxSearch');
        if (el) el.value = '';
        PA.complexity._activeRisks.clear();
        document.querySelectorAll('#cxRiskPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('cx', '');
        var s = PA.tf.state('cx');
        if (s) { s.colFilters = {}; }
        if (PA.tf._state['cx']) PA.tf.setData('cx', PA.complexity.data);
        PA.tf.filter('cx');
        PA.tf._cfUpdateIcons('cx');
    },

    _updateCounts: function() {
        var s = PA.tf.state('cx');
        var allTotal = (PA.complexity.data || []).length;
        var dataTotal = s ? s.data.length : allTotal;
        var shown = s ? s.filtered.length : allTotal;
        var totalEl = document.getElementById('cxTotalCount');
        var scope = PA._scope;
        if (totalEl) {
            if (scope !== 'full' && PA.context && PA.context.procId) {
                totalEl.textContent = dataTotal + '/' + allTotal + ' procedures (' + scope + ')';
            } else {
                totalEl.textContent = allTotal + ' procedures';
            }
        }
        var fc = document.getElementById('cxFilteredCount');
        if (fc) {
            if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
            else fc.style.display = 'none';
        }
        PA.complexity._updateActiveAlgo();
    },

    /** Show which scoring algorithm is currently applied, next to the ⚙ Scoring button, and
     *  whether it comes from a per-analysis override, the default, or the built-in default. */
    _updateActiveAlgo: function() {
        var el = document.getElementById('cxActiveAlgo');
        if (!el) return;
        var algo = PA.complexity._ALGORITHMS[PA.complexity._algorithm] || PA.complexity._ALGORITHMS.linear;
        var scope = PA.complexity._readStore(PA.complexity._keyForAnalysis()) ? 'this analysis'
            : (PA.complexity._readStore(PA.complexity._KEY_DEFAULT) ? 'default' : 'built-in');
        el.innerHTML = '<b>' + PA.esc(algo.label) + '</b> <span class="cx-algo-scope">(' + scope + ')</span>';
    },

    applyScope: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId || PA._scope === 'full') {
            PA.tf.setData('cx', PA.complexity.data);
            PA.complexity._updateCounts();
            return;
        }
        var mode = PA._scope;
        var nodeIds = ctx.callTreeNodeIds;
        var currentProcId = (ctx.procId || '').toUpperCase();

        var filtered = PA.complexity.data.filter(function(item) {
            var nid = (item.nodeId || '').toUpperCase();
            if (mode === 'direct') return nid === currentProcId;
            if (mode === 'subtree') return nid !== currentProcId && nodeIds && nodeIds.has(nid);
            return nid === currentProcId || (nodeIds && nodeIds.has(nid));
        });
        PA.tf.setData('cx', filtered);
        PA.complexity._updateCounts();
    },

    // ==================== SCORING ALGORITHM CONFIG ====================

    _WEIGHT_FIELDS: [
        { key: 'loc', label: 'Lines of Code', step: '0.1', desc: 'Points per line of code in the procedure' },
        { key: 'tables', label: 'Tables', step: '1', desc: 'Points per distinct table touched' },
        { key: 'callsOut', label: 'Outgoing Calls', step: '1', desc: 'Points per call to another procedure' },
        { key: 'cursors', label: 'Cursors', step: '1', desc: 'Points per cursor declared' },
        { key: 'dynamicSql', label: 'Dynamic SQL', step: '1', desc: 'Points per EXECUTE IMMEDIATE / dynamic SQL' },
        { key: 'depth', label: 'Call Depth', step: '1', desc: 'Points per level of call nesting' }
    ],

    /** Rebuild the table + summary from the current (possibly previewed) weights. */
    _refresh: async function() {
        await PA.complexity._recompute();
        PA.complexity._initTable();
        PA.complexity.applyScope();
        PA.complexity._applyLoadError();
    },

    openConfig: function() {
        PA.complexity._closeConfig();
        var w = PA.complexity._weights || {};
        var th = PA.complexity._thresholds || {};
        var esc = PA.esc;

        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'cxCfgOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) PA.complexity._closeConfig(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal cx-cfg-modal';
        modal.style.maxWidth = '660px';
        modal.style.width = '92vw';
        modal.onclick = function(e) { e.stopPropagation(); };

        var html = '<div class="tds-header"><div class="tds-header-left">';
        html += '<span style="font-weight:700;font-size:13px">&#9881; Complexity Scoring Algorithm</span>';
        html += '</div><button class="btn btn-sm tds-close" onclick="PA.complexity._closeConfig()">&times;</button></div>';

        html += '<div class="tds-body" style="max-height:70vh;overflow:auto">';
        html += '<div style="font-size:11px;color:var(--text-muted);margin-bottom:10px">'
             + 'Each metric is transformed by the chosen <b>algorithm</b>, multiplied by its <b>weight</b>, '
             + 'and summed. A row is <b>HIGH</b> at or above the High threshold, <b>MEDIUM</b> at or above '
             + 'the Medium threshold, otherwise <b>LOW</b>.</div>';

        html += '<div class="cx-cfg-scope" style="font-size:11px;margin-bottom:10px">';
        var perAnalysis = !!PA.complexity._readStore(PA.complexity._keyForAnalysis());
        var perDefault = !!PA.complexity._readStore(PA.complexity._KEY_DEFAULT);
        html += 'Active overrides: '
             + '<span class="badge">' + (perAnalysis ? 'this analysis' : 'no analysis override') + '</span> '
             + '<span class="badge">' + (perDefault ? 'default (all)' : 'no default override') + '</span>';
        html += '</div>';

        // Algorithm selector + live formula description.
        html += '<div class="tds-section-title">Algorithm</div>';
        html += '<div class="cx-cfg-grid" style="grid-template-columns:1fr">';
        html += '<label class="cx-cfg-row"><span>Formula</span><select id="cxCfg_algorithm" style="width:220px" onchange="PA.complexity._onAlgoChange()">';
        var algos = PA.complexity._ALGORITHMS;
        for (var ak in algos) {
            html += '<option value="' + ak + '"' + (ak === PA.complexity._algorithm ? ' selected' : '') + '>' + esc(algos[ak].label) + '</option>';
        }
        html += '</select></label>';
        html += '<label class="cx-cfg-row" id="cxCfg_expRow" style="' + (PA.complexity._algorithm === 'exponential' ? '' : 'display:none') + '"><span>Exponent (p)</span>'
             + '<input type="number" step="0.1" min="1" id="cxCfg_exponent" value="' + (PA.complexity._exponent || 1.3) + '"></label>';
        html += '</div>';
        html += '<div id="cxCfg_formula" class="cx-formula-box">'
             + (algos[PA.complexity._algorithm] || algos.linear).formula + '</div>';

        html += '<div class="tds-section-title" style="margin-top:16px">Weights <span class="cx-cfg-hint">points contributed per unit</span></div>';
        html += '<div class="cx-weights">';
        for (var i = 0; i < PA.complexity._WEIGHT_FIELDS.length; i++) {
            var f = PA.complexity._WEIGHT_FIELDS[i];
            var val = w[f.key] != null ? w[f.key] : '';
            html += '<div class="cx-weight-card">'
                 + '<div class="cx-weight-head"><span class="cx-weight-label">' + esc(f.label) + '</span>'
                 + '<input type="number" min="0" step="' + f.step + '" id="cxCfg_' + f.key + '" value="' + val + '" class="cx-weight-input"></div>'
                 + '<div class="cx-weight-desc">' + esc(f.desc || '') + '</div>'
                 + '</div>';
        }
        html += '</div>';

        html += '<div class="tds-section-title" style="margin-top:16px">Risk Thresholds <span class="cx-cfg-hint">score cutoffs</span></div>';
        html += '<div class="cx-thresholds">';
        html += '<div class="cx-thr-card medium"><span class="cx-thr-label">Medium &ge;</span><input type="number" min="0" step="1" id="cxCfg_thMedium" value="' + (th.medium != null ? th.medium : '') + '"></div>';
        html += '<div class="cx-thr-card high"><span class="cx-thr-label">High &ge;</span><input type="number" min="0" step="1" id="cxCfg_thHigh" value="' + (th.high != null ? th.high : '') + '"></div>';
        html += '</div>';

        html += '<div id="cxCfgMsg" style="font-size:11px;color:var(--green);margin-top:10px;min-height:14px"></div>';
        html += '</div>';

        html += '<div class="tds-footer" style="display:flex;gap:6px;flex-wrap:wrap;padding:10px 14px;border-top:1px solid var(--border)">';
        html += '<button class="btn btn-sm" onclick="PA.complexity.previewConfig()" title="Recompute scores without saving">Preview</button>';
        html += '<button class="btn btn-sm" onclick="PA.complexity.resetConfig()" title="Restore built-in weights">Reset to built-in</button>';
        html += '<span style="flex:1"></span>';
        html += '<button class="btn btn-sm" onclick="PA.complexity.saveConfig(\'analysis\')" title="Persist for the current analysis only">Save for this analysis</button>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.complexity.saveConfig(\'default\')" title="Persist as the default for all analyses">Save as default (all)</button>';
        html += '</div>';

        modal.innerHTML = html;
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        if (PA.modalStack) overlay.style.zIndex = PA.modalStack.nextZ();
        requestAnimationFrame(function() { overlay.classList.add('open'); });
        document.addEventListener('keydown', PA.complexity._cfgEsc);
    },

    _cfgEsc: function(e) { if (e.key === 'Escape') PA.complexity._closeConfig(); },

    _closeConfig: function() {
        var el = document.getElementById('cxCfgOverlay');
        if (el) { el.classList.remove('open'); setTimeout(function() { if (el.parentNode) el.remove(); }, 150); }
        document.removeEventListener('keydown', PA.complexity._cfgEsc);
    },

    /** Toggle the exponent field + formula hint when the algorithm changes. */
    _onAlgoChange: function() {
        var sel = document.getElementById('cxCfg_algorithm');
        var algo = sel ? sel.value : 'linear';
        var expRow = document.getElementById('cxCfg_expRow');
        if (expRow) expRow.style.display = (algo === 'exponential') ? '' : 'none';
        var fEl = document.getElementById('cxCfg_formula');
        if (fEl) fEl.innerHTML = (PA.complexity._ALGORITHMS[algo] || PA.complexity._ALGORITHMS.linear).formula;
    },

    /**
     * Parse a form value to a non-negative finite number.
     * Returns null for empty/blank/NaN input so the caller falls back to the default
     * (a negative weight would invert the tool's meaning; NaN would poison every downstream calc).
     */
    _nonNegNum: function(raw) {
        if (raw === null || raw === undefined || String(raw).trim() === '') return null;
        var n = parseFloat(raw);
        if (!isFinite(n)) return null;   // non-numeric → keep default
        return n < 0 ? 0 : n;            // clamp negatives to 0
    },

    /** Read the modal inputs into { weights, thresholds, algorithm, exponent }. */
    _readConfigForm: function() {
        var weights = {};
        for (var i = 0; i < PA.complexity._WEIGHT_FIELDS.length; i++) {
            var k = PA.complexity._WEIGHT_FIELDS[i].key;
            var el = document.getElementById('cxCfg_' + k);
            if (el) { var wv = PA.complexity._nonNegNum(el.value); if (wv !== null) weights[k] = wv; }
        }
        var thresholds = {};
        var m = document.getElementById('cxCfg_thMedium');
        var h = document.getElementById('cxCfg_thHigh');
        if (m) { var mv = PA.complexity._nonNegNum(m.value); if (mv !== null) thresholds.medium = mv; }
        if (h) { var hv = PA.complexity._nonNegNum(h.value); if (hv !== null) thresholds.high = hv; }
        var algoEl = document.getElementById('cxCfg_algorithm');
        var expEl = document.getElementById('cxCfg_exponent');
        var algorithm = algoEl ? algoEl.value : 'linear';
        var ev = expEl ? PA.complexity._nonNegNum(expEl.value) : null;
        var exponent = (ev !== null && ev > 0) ? ev : 1.3;   // exponent must stay positive
        return { weights: weights, thresholds: thresholds, algorithm: algorithm, exponent: exponent };
    },

    /** Apply a form config to the live scoring state (used by preview & save). */
    _applyConfig: function(cfg) {
        PA.complexity._weights = Object.assign({}, PA.complexity._serverWeights, cfg.weights);
        PA.complexity._thresholds = Object.assign({}, PA.complexity._serverThresholds, cfg.thresholds);
        PA.complexity._algorithm = PA.complexity._ALGORITHMS[cfg.algorithm] ? cfg.algorithm : 'linear';
        PA.complexity._exponent = cfg.exponent || 1.3;
    },

    _cfgMsg: function(text, color) {
        var el = document.getElementById('cxCfgMsg');
        if (el) { el.textContent = text; el.style.color = color || 'var(--green)'; }
    },

    previewConfig: function() {
        PA.complexity._applyConfig(PA.complexity._readConfigForm());
        PA.complexity._refresh();
        PA.complexity._cfgMsg('Preview applied (not saved).', 'var(--orange)');
    },

    saveConfig: function(scope) {
        var cfg = PA.complexity._readConfigForm();
        var key = scope === 'default' ? PA.complexity._KEY_DEFAULT : PA.complexity._keyForAnalysis();
        try { localStorage.setItem(key, JSON.stringify(cfg)); } catch(e) {}
        PA.complexity._resolveConfig();
        PA.complexity._refresh();
        // Save applies the scores and closes the popup; a toast confirms since the modal is gone.
        if (PA.toast) PA.toast(scope === 'default' ? 'Saved as default for all analyses' : 'Saved for this analysis', 'success');
        PA.complexity._closeConfig();
    },

    resetConfig: function() {
        // Clear both overrides and restore the server built-in algorithm.
        try {
            localStorage.removeItem(PA.complexity._keyForAnalysis());
            localStorage.removeItem(PA.complexity._KEY_DEFAULT);
        } catch(e) {}
        PA.complexity._resolveConfig();
        PA.complexity._refresh();
        PA.complexity._closeConfig();
        PA.complexity.openConfig();
        PA.complexity._cfgMsg('Reset to built-in weights.', 'var(--green)');
    }
};
