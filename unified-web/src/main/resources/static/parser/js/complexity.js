window.PA = window.PA || {};

PA.complexity = {
    data: [],
    _activeRisks: new Set(),
    _weights: null,
    _thresholds: null,

    load: async function() {
        if (!PA.complexity._weights) {
            try {
                var cfg = await PA.api.getComplexityConfig();
                if (cfg) {
                    PA.complexity._weights = cfg.weights;
                    PA.complexity._thresholds = cfg.thresholds;
                }
            } catch(e) {}
        }
        var nodes = PA.analysisData && PA.analysisData.nodes ? PA.analysisData.nodes : [];
        PA.complexity.data = PA.complexity._computeMetrics(nodes);
        PA.complexity._initRiskPills();
        PA.complexity._initTable();
        PA.complexity.applyScope();
    },

    _computeMetrics: function(nodes) {
        var w = PA.complexity._weights || {};
        var wLoc = w.loc != null ? w.loc : 0.3;
        var wTables = w.tables != null ? w.tables : 15;
        var wCalls = w.callsOut != null ? w.callsOut : 10;
        var wCursors = w.cursors != null ? w.cursors : 8;
        var wDyn = w.dynamicSql != null ? w.dynamicSql : 20;
        var wDepth = w.depth != null ? w.depth : 5;

        var th = PA.complexity._thresholds || {};
        var thMed = th.medium != null ? th.medium : 50;
        var thHigh = th.high != null ? th.high : 150;

        return nodes.map(function(n) {
            var c = n.counts || {};
            var stmts = c.statements || {};
            var totalStmts = 0;
            for (var k in stmts) totalStmts += stmts[k];
            var dynSql = (stmts.EXECUTE_IMMEDIATE || 0) + (stmts.DBMS_SQL || 0);
            var tables = c.tables || 0;
            var callsOut = c.callsOut || 0;
            var cursors = c.cursors || 0;
            var exHandlers = c.exceptionHandlers || 0;
            var loc = n.linesOfCode || 0;
            var depth = n.depth || 0;

            var score = Math.round(
                (loc * wLoc) + (tables * wTables) + (callsOut * wCalls) + (cursors * wCursors)
                + (dynSql * wDyn) + (depth * wDepth)
            );

            var risk;
            if (score >= thHigh) risk = 'HIGH';
            else if (score >= thMed) risk = 'MEDIUM';
            else risk = 'LOW';

            return {
                nodeId: n.nodeId,
                name: n.name || n.objectName || '',
                schema: n.schema || '',
                objectType: n.objectType || '',
                loc: loc,
                depth: depth,
                tables: tables,
                callsOut: callsOut,
                cursors: cursors,
                exHandlers: exHandlers,
                dynSql: dynSql,
                totalStmts: totalStmts,
                stmts: stmts,
                params: c.parameters || 0,
                variables: c.variables || 0,
                sequences: c.sequences || 0,
                readable: n.readable !== false,
                score: score,
                risk: risk,
                sourceFile: n.sourceFile || '',
                detailFile: n.detailFile || '',
                _w: { loc: wLoc, tables: wTables, callsOut: wCalls, cursors: wCursors, dynSql: wDyn, depth: wDepth }
            };
        });
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
            { k: 'HIGH', label: 'High', color: '#e11d48', bg: '#fff1f2' },
            { k: 'MEDIUM', label: 'Medium', color: 'var(--badge-orange)', bg: 'var(--badge-orange-bg)' },
            { k: 'LOW', label: 'Low', color: 'var(--badge-green)', bg: 'var(--badge-green-bg)' }
        ];
        var html = '<div class="cx-risk-summary">';
        for (var j = 0; j < items.length; j++) {
            var r = items[j];
            var c = counts[r.k] || 0;
            var pct = Math.round((c / total) * 100);
            html += '<div class="cx-risk-card" onclick="PA.complexity.toggleRisk(\'' + r.k + '\')" title="Click to filter ' + r.label + '">';
            html += '<div class="cx-risk-card-count" style="color:' + r.color + '">' + c + '</div>';
            html += '<div class="cx-risk-card-label">' + r.label + '</div>';
            html += '<div class="cx-risk-card-bar"><div style="width:' + pct + '%;background:' + r.color + ';height:100%;border-radius:2px"></div></div>';
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
        var scoreColor = item.risk === 'HIGH' ? '#e11d48' :
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
        var html = '<div class="to-detail">';

        var factors = [
            { label: 'Lines of Code', value: item.loc, weight: w.loc, contribution: Math.round(item.loc * (w.loc || 0.3)) },
            { label: 'Tables', value: item.tables, weight: w.tables, contribution: Math.round(item.tables * (w.tables || 15)) },
            { label: 'Outgoing Calls', value: item.callsOut, weight: w.callsOut, contribution: Math.round(item.callsOut * (w.callsOut || 10)) },
            { label: 'Cursors', value: item.cursors, weight: w.cursors, contribution: Math.round(item.cursors * (w.cursors || 8)) },
            { label: 'Dynamic SQL', value: item.dynSql, weight: w.dynSql, contribution: Math.round(item.dynSql * (w.dynSql || 20)) },
            { label: 'Call Depth', value: item.depth, weight: w.depth, contribution: Math.round(item.depth * (w.depth || 5)) }
        ];
        factors.sort(function(a, b) { return b.contribution - a.contribution; });
        var maxContrib = factors[0] ? factors[0].contribution : 1;

        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Score Breakdown — <span class="cx-risk ' + item.risk.toLowerCase() + '">' + item.risk + '</span> (' + item.score + ' points)</div>';
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

        if (item.sourceFile) {
            html += '<div style="padding:6px 0">';
            html += '<span class="to-detail-line" onclick="event.stopPropagation(); PA.sourceView.open(\'' + PA.escJs(item.sourceFile) + '\', 1)" title="Open source">View Source</span>';
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
        if (scope !== 'full' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' procedures (' + scope + ')';
        } else {
            totalEl.textContent = allTotal + ' procedures';
        }
        var fc = document.getElementById('cxFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
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
    }
};
