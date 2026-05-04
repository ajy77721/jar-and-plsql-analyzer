window.PA = window.PA || {};

PA.joinSummary = {
    data: [],
    queryData: [],
    _nodeCache: {},
    _currentView: 'table',
    _jcWeights: null,
    _jcThresholds: null,

    async _loadConfig() {
        if (PA.joinSummary._jcWeights) return;
        try {
            var cfg = await PA.api.getJoinComplexityConfig();
            if (cfg) {
                PA.joinSummary._jcWeights = cfg.weights;
                PA.joinSummary._jcThresholds = cfg.thresholds;
            }
        } catch(e) {}
    },

    async load() {
        await PA.joinSummary._loadConfig();
        var raw = PA.joins.data;
        if (!raw || !raw.length) {
            raw = await PA.api.getJoinOperations();
        }
        PA.joinSummary._nodeCache = PA.joins._nodeCache || {};
        PA.joinSummary.data = PA.joinSummary._buildItems(raw);
        PA.joinSummary.queryData = PA.joinSummary._buildQueryItems(raw);
        PA.joinSummary._initTable();
        PA.joinSummary._initQueryTable();
        PA.joinSummary.applyScope();
    },

    async loadQueryOnly() {
        await PA.joinSummary._loadConfig();
        var raw = PA.joins.data;
        if (!raw || !raw.length) {
            raw = await PA.api.getJoinOperations();
        }
        PA.joinSummary._nodeCache = PA.joins._nodeCache || {};
        PA.joinSummary.queryData = PA.joinSummary._buildQueryItems(raw);
        PA.joinSummary._initQueryTable();
        PA.joinSummary._applyScopeQueryOnly();
    },

    switchView: function(view) {
        PA.joinSummary._currentView = view;
        document.querySelectorAll('.js-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.jsview === view);
        });
        document.querySelectorAll('.js-subview').forEach(function(v) { v.classList.remove('active'); });
        var target = view === 'query' ? 'jsViewQuery' : 'jsViewTable';
        var el = document.getElementById(target);
        if (el) el.classList.add('active');
    },

    _buildItems: function(raw) {
        var items = [];
        for (var i = 0; i < raw.length; i++) {
            var pair = raw[i];
            var details = pair.accessDetails || [];
            for (var j = 0; j < details.length; j++) {
                var d = details[j];
                var predParts = (d.onPredicate || '').split(/\bAND\b/i);
                var predCount = d.onPredicate ? predParts.length : 0;

                var tableSet = {};
                tableSet[(pair.leftTable || '').toUpperCase()] = 1;
                tableSet[(pair.rightTable || '').toUpperCase()] = 1;

                var complexity = PA.joinSummary._calcComplexity(d.joinType, predCount);

                items.push({
                    leftTable: pair.leftTable || '',
                    rightTable: pair.rightTable || '',
                    joinType: d.joinType || 'UNKNOWN',
                    predicate: d.onPredicate || '',
                    predCount: predCount,
                    procedureId: d.procedureId || '',
                    procedureName: d.procedureName || '',
                    lineNumber: d.lineNumber || 0,
                    sourceFile: d.sourceFile || '',
                    tableCount: Object.keys(tableSet).length,
                    complexity: complexity,
                    complexityLabel: PA.joinSummary._complexityLabel(complexity),
                    _pairIdx: i,
                    _detailIdx: j
                });
            }
        }
        return items;
    },

    _buildQueryItems: function(raw) {
        var byKey = {};
        for (var i = 0; i < raw.length; i++) {
            var pair = raw[i];
            var details = pair.accessDetails || [];
            for (var j = 0; j < details.length; j++) {
                var d = details[j];
                var procId = d.procedureId || d.procedureName || '';
                var line = d.lineNumber || 0;
                var bucket = Math.floor(line / 50);
                var key = procId + '::' + bucket;

                if (!byKey[key]) {
                    byKey[key] = {
                        procedureId: procId,
                        procedureName: d.procedureName || '',
                        sourceFile: d.sourceFile || '',
                        minLine: line,
                        maxLine: line,
                        joins: [],
                        tableSet: {},
                        joinTypeSet: {},
                        totalPreds: 0
                    };
                }
                var g = byKey[key];
                if (line < g.minLine) g.minLine = line;
                if (line > g.maxLine) g.maxLine = line;

                var predParts = (d.onPredicate || '').split(/\bAND\b/i);
                var predCount = d.onPredicate ? predParts.length : 0;
                g.totalPreds += predCount;

                g.tableSet[(pair.leftTable || '').toUpperCase()] = 1;
                g.tableSet[(pair.rightTable || '').toUpperCase()] = 1;
                g.joinTypeSet[d.joinType || 'UNKNOWN'] = (g.joinTypeSet[d.joinType || 'UNKNOWN'] || 0) + 1;

                g.joins.push({
                    leftTable: pair.leftTable || '',
                    rightTable: pair.rightTable || '',
                    joinType: d.joinType || 'UNKNOWN',
                    predicate: d.onPredicate || '',
                    predCount: predCount,
                    lineNumber: line
                });
            }
        }

        var items = [];
        var keys = Object.keys(byKey);
        for (var k = 0; k < keys.length; k++) {
            var g = byKey[keys[k]];
            var tables = Object.keys(g.tableSet);
            var joinTypes = Object.keys(g.joinTypeSet);
            var maxComplexity = 0;
            for (var ji = 0; ji < g.joins.length; ji++) {
                var c = PA.joinSummary._calcComplexity(g.joins[ji].joinType, g.joins[ji].predCount);
                if (c > maxComplexity) maxComplexity = c;
            }
            var lineSpan = (g.maxLine > g.minLine) ? (g.maxLine - g.minLine + 1) : 1;
            items.push({
                procedureId: g.procedureId,
                procedureName: g.procedureName,
                sourceFile: g.sourceFile,
                lineNumber: g.minLine,
                lineEnd: g.maxLine,
                lineSpan: lineSpan,
                joinCount: g.joins.length,
                tableCount: tables.length,
                tables: tables,
                joinTypes: joinTypes,
                joinTypeMap: g.joinTypeSet,
                totalPreds: g.totalPreds,
                joins: g.joins,
                complexity: maxComplexity,
                complexityLabel: PA.joinSummary._complexityLabel(maxComplexity)
            });
        }
        return items;
    },

    _calcComplexity: function(joinType, predCount) {
        var w = PA.joinSummary._jcWeights || {};
        var th = PA.joinSummary._jcThresholds || {};
        var base = w.baseScore != null ? w.baseScore : 1;
        var outerPen = w.outerJoinPenalty != null ? w.outerJoinPenalty : 0.5;
        var crossPen = w.crossFullJoinPenalty != null ? w.crossFullJoinPenalty : 1;
        var multiPred = w.multiPredBonus != null ? w.multiPredBonus : 0.5;
        var highPred = w.highPredBonus != null ? w.highPredBonus : 1;
        var noPred = w.noPredPenalty != null ? w.noPredPenalty : 0.5;
        var multiTh = w.multiPredThreshold != null ? w.multiPredThreshold : 2;
        var highTh = w.highPredThreshold != null ? w.highPredThreshold : 3;

        var score = base;
        var jt = (joinType || '').toUpperCase();
        if (jt.includes('LEFT') || jt.includes('RIGHT')) score += outerPen;
        if (jt.includes('FULL') || jt.includes('CROSS')) score += crossPen;
        if (predCount >= highTh) score += highPred;
        else if (predCount >= multiTh) score += multiPred;
        if (predCount === 0 && jt !== 'CROSS') score += noPred;
        return score;
    },

    _complexityLabel: function(score) {
        var th = PA.joinSummary._jcThresholds || {};
        var thMed = th.medium != null ? th.medium : 2;
        var thHigh = th.high != null ? th.high : 3;
        if (score >= thHigh) return 'HIGH';
        if (score >= thMed) return 'MEDIUM';
        return 'LOW';
    },

    // ==================== TABLE VIEW ====================

    _initTable: function() {
        var container = document.getElementById('jsContainer');
        if (!container) return;

        var html = '<div id="jsSummaryCards"></div>';
        html += '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'js\',0)">Left Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'js\',1)">Right Table</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'js\',2)">Join Type</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'js\',3)">Predicates</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'js\',4)">Procedure</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'js\',5)">Line</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'js\',6)">Complexity</th>';
        html += '</tr></thead><tbody id="js-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.joinSummary._renderSummary();

        PA.tf.init('js', PA.joinSummary.data, 50, PA.joinSummary._renderRow, {
            sortKeys: {
                0: { fn: function(i) { return (i.leftTable || '').toUpperCase(); } },
                1: { fn: function(i) { return (i.rightTable || '').toUpperCase(); } },
                2: { fn: function(i) { return (i.joinType || '').toUpperCase(); } },
                3: { fn: function(i) { return i.predCount; } },
                4: { fn: function(i) { return (i.procedureName || '').toUpperCase(); } },
                5: { fn: function(i) { return i.lineNumber || 0; } },
                6: { fn: function(i) { return i.complexity; } }
            },
            searchFn: function(item, q) {
                return (item.leftTable || '').toUpperCase().includes(q) ||
                    (item.rightTable || '').toUpperCase().includes(q) ||
                    (item.procedureName || '').toUpperCase().includes(q) ||
                    (item.joinType || '').toUpperCase().includes(q) ||
                    (item.predicate || '').toUpperCase().includes(q);
            },
            onFilter: PA.joinSummary._updateCounts
        });

        var s = PA.tf.state('js');
        if (s) { s.sortCol = 6; s.sortDir = 'desc'; }
        PA.tf.filter('js');

        setTimeout(function() {
            PA.tf.initColFilters('js', {
                0: { label: 'Left Table', valueFn: function(i) { return i.leftTable || ''; } },
                1: { label: 'Right Table', valueFn: function(i) { return i.rightTable || ''; } },
                2: { label: 'Join Type', valueFn: function(i) { return i.joinType || ''; } },
                3: { label: 'Predicates', valueFn: function(i) { return String(i.predCount); } },
                4: { label: 'Procedure', valueFn: function(i) { return i.procedureName || ''; } },
                6: { label: 'Complexity', valueFn: function(i) { return i.complexityLabel; } }
            });
            PA.tf._updateSortIndicators('js');
        }, 0);
    },

    _renderSummary: function() {
        var el = document.getElementById('jsSummaryCards');
        if (!el) return;
        var s = PA.tf.state('js');
        var data = s ? s.data : PA.joinSummary.data;
        var byType = {}, byComplexity = { LOW: 0, MEDIUM: 0, HIGH: 0 };
        var tableSet = {};
        for (var i = 0; i < data.length; i++) {
            var jt = data[i].joinType || 'UNKNOWN';
            byType[jt] = (byType[jt] || 0) + 1;
            byComplexity[data[i].complexityLabel]++;
            tableSet[(data[i].leftTable || '').toUpperCase()] = 1;
            tableSet[(data[i].rightTable || '').toUpperCase()] = 1;
        }
        var typeColors = {
            'INNER': 'var(--blue)', 'LEFT OUTER': 'var(--green)', 'RIGHT OUTER': 'var(--orange)',
            'LEFT': 'var(--green)', 'RIGHT': 'var(--orange)', 'CROSS': 'var(--red)',
            'FULL': 'var(--purple)', 'IMPLICIT': 'var(--text-muted)', 'UNKNOWN': 'var(--text-muted)'
        };

        var html = '<div class="js-summary">';
        html += '<div class="js-summary-card"><div class="js-summary-num">' + data.length + '</div><div class="js-summary-label">Total Joins</div></div>';
        html += '<div class="js-summary-card"><div class="js-summary-num">' + Object.keys(tableSet).length + '</div><div class="js-summary-label">Tables Involved</div></div>';

        var typeKeys = Object.keys(byType).sort(function(a, b) { return byType[b] - byType[a]; });
        html += '<div class="js-summary-card js-summary-types">';
        for (var ti = 0; ti < typeKeys.length; ti++) {
            var tk = typeKeys[ti];
            var color = typeColors[tk] || 'var(--text-muted)';
            html += '<span class="op-badge" style="background:color-mix(in srgb, ' + color + ' 15%, transparent);color:' + color + ';font-size:9px">' + PA.esc(tk) + ' <strong>' + byType[tk] + '</strong></span>';
        }
        html += '</div>';

        html += '<div class="js-summary-card">';
        html += '<span class="cx-risk low" style="margin-right:6px">LOW ' + byComplexity.LOW + '</span>';
        html += '<span class="cx-risk medium" style="margin-right:6px">MEDIUM ' + byComplexity.MEDIUM + '</span>';
        html += '<span class="cx-risk high">HIGH ' + byComplexity.HIGH + '</span>';
        html += '</div>';

        html += '</div>';
        el.innerHTML = html;
    },

    _renderRow: function(item, idx, esc) {
        var jtColor = item.joinType === 'INNER' ? 'var(--blue)' :
            item.joinType.includes('LEFT') ? 'var(--green)' :
            item.joinType.includes('RIGHT') ? 'var(--orange)' :
            item.joinType === 'CROSS' ? 'var(--red)' :
            item.joinType.includes('FULL') ? 'var(--purple)' : 'var(--text-muted)';
        var cxCls = item.complexityLabel.toLowerCase();

        var html = '<tr class="to-row" style="cursor:pointer" onclick="PA.joinSummary.openDetail(' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--teal)">' + esc(item.leftTable) + '</span></td>';
        html += '<td><span style="font-weight:600;color:var(--teal)">' + esc(item.rightTable) + '</span></td>';
        html += '<td><span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + '">' + esc(item.joinType) + '</span></td>';
        html += '<td class="cx-num">' + item.predCount + '</td>';
        html += '<td><span class="to-detail-proc" onclick="event.stopPropagation();PA.showProcedure(\'' + PA.escJs(item.procedureId) + '\')">' + esc(item.procedureName) + '</span></td>';
        html += '<td>';
        if (item.lineNumber && item.sourceFile) {
            html += '<span class="td-line" onclick="event.stopPropagation();PA.sourceView.openAtLine(\'' + PA.escJs(item.sourceFile) + '\',' + item.lineNumber + ')">L' + item.lineNumber + '</span>';
        } else if (item.lineNumber) {
            html += '<span class="td-line" style="cursor:default">L' + item.lineNumber + '</span>';
        }
        html += '</td>';
        html += '<td><span class="cx-risk ' + cxCls + '">' + item.complexityLabel + '</span></td>';
        html += '</tr>';
        return html;
    },

    // ==================== QUERY VIEW ====================

    _initQueryTable: function() {
        var container = document.getElementById('jqContainer');
        if (!container) return;

        var html = '<div id="jqSummaryCards"></div>';
        html += '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'jq\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'jq\',1)">Line</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'jq\',2)">Joins</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'jq\',3)">Tables</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'jq\',4)">Lines</th>';
        html += '<th>Join Types</th>';
        html += '<th>Tables Involved</th>';
        html += '<th data-sort-col="7" onclick="PA.tf.sort(\'jq\',7)">Complexity</th>';
        html += '</tr></thead><tbody id="jq-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.joinSummary._renderQuerySummary();

        PA.tf.init('jq', PA.joinSummary.queryData, 50, PA.joinSummary._renderQueryRow, {
            sortKeys: {
                0: { fn: function(i) { return (i.procedureName || '').toUpperCase(); } },
                1: { fn: function(i) { return i.lineNumber || 0; } },
                2: { fn: function(i) { return i.joinCount; } },
                3: { fn: function(i) { return i.tableCount; } },
                4: { fn: function(i) { return i.lineSpan || 1; } },
                7: { fn: function(i) { return i.complexity; } }
            },
            searchFn: function(item, q) {
                return (item.procedureName || '').toUpperCase().includes(q) ||
                    item.tables.some(function(t) { return t.includes(q); }) ||
                    item.joinTypes.some(function(jt) { return jt.toUpperCase().includes(q); });
            },
            renderDetail: PA.joinSummary._renderQueryDetail,
            onFilter: PA.joinSummary._updateQueryCounts
        });

        var s = PA.tf.state('jq');
        if (s) { s.sortCol = 2; s.sortDir = 'desc'; }
        PA.tf.filter('jq');

        setTimeout(function() {
            PA.tf.initColFilters('jq', {
                0: { label: 'Procedure', valueFn: function(i) { return i.procedureName || ''; } },
                2: { label: 'Joins', valueFn: function(i) { return String(i.joinCount); } },
                3: { label: 'Tables', valueFn: function(i) { return String(i.tableCount); } },
                4: { label: 'Lines', valueFn: function(i) { return i.lineSpan <= 10 ? '1-10' : i.lineSpan <= 50 ? '11-50' : i.lineSpan <= 100 ? '51-100' : '100+'; } },
                5: { label: 'Join Types', valueFn: function(i) { return (i.joinTypes || []).slice(); } },
                6: { label: 'Tables Involved', valueFn: function(i) { return (i.tables || []).slice(); } },
                7: { label: 'Complexity', valueFn: function(i) { return i.complexityLabel; } }
            });
            PA.tf._updateSortIndicators('jq');
        }, 0);
    },

    _renderQuerySummary: function() {
        var el = document.getElementById('jqSummaryCards');
        if (!el) return;
        var s = PA.tf.state('jq');
        var data = s ? s.data : PA.joinSummary.queryData;
        var totalJoins = 0, totalTables = {}, maxJoins = 0;
        for (var i = 0; i < data.length; i++) {
            totalJoins += data[i].joinCount;
            if (data[i].joinCount > maxJoins) maxJoins = data[i].joinCount;
            for (var ti = 0; ti < data[i].tables.length; ti++) {
                totalTables[data[i].tables[ti]] = 1;
            }
        }
        var html = '<div class="js-summary">';
        html += '<div class="js-summary-card"><div class="js-summary-num">' + data.length + '</div><div class="js-summary-label">Query Groups</div></div>';
        html += '<div class="js-summary-card"><div class="js-summary-num">' + totalJoins + '</div><div class="js-summary-label">Total Joins</div></div>';
        html += '<div class="js-summary-card"><div class="js-summary-num">' + Object.keys(totalTables).length + '</div><div class="js-summary-label">Tables Involved</div></div>';
        html += '<div class="js-summary-card"><div class="js-summary-num">' + maxJoins + '</div><div class="js-summary-label">Max Joins/Query</div></div>';
        html += '</div>';
        el.innerHTML = html;
    },

    _renderQueryRow: function(item, idx, esc) {
        var cxCls = item.complexityLabel.toLowerCase();
        var typeColors = {
            'INNER': 'var(--blue)', 'LEFT OUTER': 'var(--green)', 'RIGHT OUTER': 'var(--orange)',
            'LEFT': 'var(--green)', 'RIGHT': 'var(--orange)', 'CROSS': 'var(--red)',
            'FULL': 'var(--purple)', 'IMPLICIT': 'var(--text-muted)', 'UNKNOWN': 'var(--text-muted)'
        };

        var typeBadges = '';
        for (var ti = 0; ti < item.joinTypes.length; ti++) {
            var jt = item.joinTypes[ti];
            var color = typeColors[jt] || 'var(--text-muted)';
            var cnt = item.joinTypeMap[jt] || 0;
            typeBadges += '<span class="op-badge" style="background:color-mix(in srgb, ' + color + ' 15%, transparent);color:' + color + ';font-size:9px">' + esc(jt) + (cnt > 1 ? ' x' + cnt : '') + '</span>';
        }

        var tableSpans = '';
        var maxShow = 4;
        for (var tbi = 0; tbi < Math.min(item.tables.length, maxShow); tbi++) {
            if (tbi > 0) tableSpans += '<span style="color:var(--text-muted);font-size:9px">, </span>';
            tableSpans += '<span>' + esc(item.tables[tbi]) + '</span>';
        }
        if (item.tables.length > maxShow) {
            tableSpans += '<span style="color:var(--text-muted);font-size:9px"> +' + (item.tables.length - maxShow) + '</span>';
        }

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'jq\',' + idx + ')">';
        html += '<td><span class="to-detail-proc" onclick="event.stopPropagation();PA.showProcedure(\'' + PA.escJs(item.procedureId) + '\')">' + esc(item.procedureName) + '</span></td>';
        html += '<td>';
        if (item.lineNumber && item.sourceFile) {
            html += '<span class="td-line" onclick="event.stopPropagation();PA.sourceView.openAtLine(\'' + PA.escJs(item.sourceFile) + '\',' + item.lineNumber + ')">L' + item.lineNumber + '</span>';
        } else if (item.lineNumber) {
            html += 'L' + item.lineNumber;
        }
        if (item.lineEnd && item.lineEnd !== item.lineNumber) html += '<span style="color:var(--text-muted);font-size:9px">-' + item.lineEnd + '</span>';
        html += '</td>';
        html += '<td class="cx-num" style="font-weight:700;color:var(--accent)">' + item.joinCount + '</td>';
        html += '<td class="cx-num">' + item.tableCount + '</td>';
        html += '<td class="cx-num">' + item.lineSpan + '</td>';
        html += '<td><div class="jq-join-badges">' + typeBadges + '</div></td>';
        html += '<td><div class="jq-query-tables">' + tableSpans + '</div></td>';
        html += '<td><span class="cx-risk ' + cxCls + '">' + item.complexityLabel + '</span></td>';
        html += '</tr>';
        return html;
    },

    _renderQueryDetail: function(item, idx, esc) {
        var html = '<div class="to-detail">';

        html += PA.joinSummary._renderComplexityBreakdown(item, esc);

        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Joins in this Query (' + item.joinCount + ')</div>';
        for (var i = 0; i < item.joins.length; i++) {
            var j = item.joins[i];
            var jtColor = j.joinType === 'INNER' ? 'var(--blue)' :
                j.joinType.includes('LEFT') ? 'var(--green)' :
                j.joinType.includes('RIGHT') ? 'var(--orange)' :
                j.joinType === 'CROSS' ? 'var(--red)' :
                j.joinType.includes('FULL') ? 'var(--purple)' : 'var(--text-muted)';
            html += '<div class="js-pred-item">';
            html += '<span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + ';font-size:9px;flex-shrink:0">' + esc(j.joinType) + '</span>';
            html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(j.leftTable) + '</span>';
            html += '<span style="color:var(--text-muted);font-size:9px">&harr;</span>';
            html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(j.rightTable) + '</span>';
            if (j.predicate) {
                html += '<code style="font-size:9px;color:var(--text-muted);margin-left:6px;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block">' + esc(j.predicate.substring(0, 120)) + '</code>';
            }
            html += '</div>';
        }
        html += '</div>';

        html += '<div class="to-detail-section" id="jqSql-' + idx + '">';
        html += '<div class="tds-section-title">SQL Query</div>';
        html += '<div class="join-sql-loading">Loading query...</div>';
        html += '</div>';

        html += '</div>';

        setTimeout(function() { PA.joinSummary._loadQuerySql(item, idx); }, 0);
        return html;
    },

    _renderComplexityBreakdown: function(item, esc) {
        var w = PA.joinSummary._jcWeights || {};
        var th = PA.joinSummary._jcThresholds || {};
        var base = w.baseScore != null ? w.baseScore : 1;
        var outerPen = w.outerJoinPenalty != null ? w.outerJoinPenalty : 0.5;
        var crossPen = w.crossFullJoinPenalty != null ? w.crossFullJoinPenalty : 1;
        var multiPred = w.multiPredBonus != null ? w.multiPredBonus : 0.5;
        var highPred = w.highPredBonus != null ? w.highPredBonus : 1;
        var noPred = w.noPredPenalty != null ? w.noPredPenalty : 0.5;
        var multiTh = w.multiPredThreshold != null ? w.multiPredThreshold : 2;
        var highTh = w.highPredThreshold != null ? w.highPredThreshold : 3;
        var thMed = th.medium != null ? th.medium : 2;
        var thHigh = th.high != null ? th.high : 3;

        var cxCls = item.complexityLabel.toLowerCase();
        var html = '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Complexity Breakdown — <span class="cx-risk ' + cxCls + '">' + item.complexityLabel + '</span> (score: ' + item.complexity.toFixed(1) + ')</div>';

        html += '<div style="font-size:11px;color:var(--text-muted);margin-bottom:6px">Thresholds: LOW &lt; ' + thMed + ' &le; MEDIUM &lt; ' + thHigh + ' &le; HIGH</div>';

        html += '<div class="cx-stmt-grid">';
        var factors = [];
        for (var ji = 0; ji < item.joins.length; ji++) {
            var j = item.joins[ji];
            var jt = (j.joinType || '').toUpperCase();
            var score = base;
            var parts = ['Base: ' + base];

            if (jt.includes('LEFT') || jt.includes('RIGHT')) {
                score += outerPen;
                parts.push('Outer join: +' + outerPen);
            }
            if (jt.includes('FULL') || jt.includes('CROSS')) {
                score += crossPen;
                parts.push('Cross/Full join: +' + crossPen);
            }
            if (j.predCount >= highTh) {
                score += highPred;
                parts.push('High predicates (&ge;' + highTh + '): +' + highPred);
            } else if (j.predCount >= multiTh) {
                score += multiPred;
                parts.push('Multi predicates (&ge;' + multiTh + '): +' + multiPred);
            }
            if (j.predCount === 0 && jt !== 'CROSS') {
                score += noPred;
                parts.push('No predicate: +' + noPred);
            }

            var jtColor = jt === 'INNER' ? 'var(--blue)' :
                jt.includes('LEFT') ? 'var(--green)' : jt.includes('RIGHT') ? 'var(--orange)' :
                jt.includes('CROSS') ? 'var(--red)' : jt.includes('FULL') ? 'var(--purple)' : 'var(--text-muted)';

            var barPct = Math.round((score / Math.max(thHigh + 1, item.complexity + 0.5)) * 100);
            var barColor = score >= thHigh ? 'var(--red)' : score >= thMed ? 'var(--orange)' : 'var(--green)';

            html += '<div class="cx-stmt-row">';
            html += '<span class="cx-stmt-label"><span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + ';font-size:8px;padding:1px 4px">' + esc(j.joinType) + '</span> ' + esc(j.leftTable) + ' &harr; ' + esc(j.rightTable) + '</span>';
            html += '<div class="cx-stmt-bar"><div class="cx-stmt-bar-fill" style="width:' + barPct + '%;background:' + barColor + '"></div></div>';
            html += '<span class="cx-stmt-val">' + score.toFixed(1) + '</span>';
            html += '</div>';
            html += '<div style="font-size:9px;color:var(--text-muted);padding:0 0 4px 138px">' + parts.join(' + ') + '</div>';
        }
        html += '</div></div>';
        return html;
    },

    _loadQuerySql: async function(item, idx) {
        var section = document.getElementById('jqSql-' + idx);
        if (!section) return;

        var detailFile = PA.joins._resolveDetailFile(item.procedureId);
        var nodeFile = detailFile ? detailFile.replace(/^nodes\//, '') : null;

        try {
            var nodeDetail = null;
            if (nodeFile) {
                nodeDetail = PA.joinSummary._nodeCache[nodeFile];
                if (!nodeDetail) {
                    nodeDetail = await PA.api.getNodeDetail(nodeFile);
                    if (nodeDetail) {
                        PA.joinSummary._nodeCache[nodeFile] = nodeDetail;
                        PA.joins._nodeCache[nodeFile] = nodeDetail;
                    }
                }
            }

            var firstJoin = item.joins[0];
            var sql = null;
            if (nodeDetail) {
                sql = PA.joins._findSqlContext(nodeDetail, item.lineNumber, firstJoin.leftTable, firstJoin.rightTable);
            }
            if (!sql) {
                var sf = (nodeDetail && nodeDetail.sourceFile) || item.sourceFile || PA.joinSummary._resolveSourceFile(item.procedureId);
                var ls = (nodeDetail && nodeDetail.lineStart) || 1;
                if (sf) {
                    sql = await PA.joins._findSqlFromSourceAsync(sf, item.lineNumber, ls, firstJoin.leftTable, firstJoin.rightTable);
                }
            }
            if (sql) {
                item._sql = sql;
                var esc = PA.esc;
                var highlighted = PA.joins._highlightSql(esc(sql), null, null);
                for (var ti = 0; ti < item.tables.length; ti++) {
                    var tName = item.tables[ti];
                    var re = new RegExp('\\b(' + tName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'gi');
                    highlighted = highlighted.replace(re, '<span class="join-sql-tbl">$1</span>');
                }

                var joinMatches = sql.match(/\bJOIN\b/gi) || [];
                var html = '<div style="display:flex;align-items:center;gap:8px">';
                html += '<div class="tds-section-title" style="margin:0">SQL Query</div>';
                html += '<span class="badge" style="font-size:9px">' + joinMatches.length + ' joins</span>';
                html += '<span class="badge" style="font-size:9px">' + item.tableCount + ' tables</span>';
                html += '<button class="btn btn-sm" style="margin-left:auto;font-size:10px;padding:2px 8px" onclick="event.stopPropagation();try{PA.joinSummary.openQueryModal(' + idx + ')}catch(e){console.error(e)}">View Full Query</button>';
                html += '</div>';
                html += '<pre class="join-sql-code" style="max-height:200px">' + highlighted + '</pre>';
                section.innerHTML = html;
            } else {
                section.innerHTML = '<div class="tds-section-title">SQL Query</div>' +
                    '<div class="join-sql-none">No cursor/query found near line ' + item.lineNumber + '</div>';
            }
        } catch(e) {
            section.innerHTML = '<div class="tds-section-title">SQL Query</div>' +
                '<div class="join-sql-none">Failed to load: ' + PA.esc(e.message || '') + '</div>';
        }
    },

    // ==================== TABLE VIEW DETAIL MODAL ====================

    openDetail: async function(idx) {
        var s = PA.tf.state('js');
        var item = s ? s.filtered[idx] : PA.joinSummary.data[idx];
        if (!item) return;

        PA.joinSummary._closeDetail();

        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'jsDetailOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) PA.joinSummary._closeDetail(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal';
        modal.style.maxWidth = '700px';
        modal.onclick = function(e) { e.stopPropagation(); };

        var esc = PA.esc;
        var jtColor = item.joinType === 'INNER' ? 'var(--blue)' :
            item.joinType.includes('LEFT') ? 'var(--green)' :
            item.joinType.includes('RIGHT') ? 'var(--orange)' :
            item.joinType === 'CROSS' ? 'var(--red)' :
            item.joinType.includes('FULL') ? 'var(--purple)' : 'var(--text-muted)';

        var html = '';
        html += '<div class="tds-header">';
        html += '<div class="tds-header-left">';
        html += '<span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + ';font-size:10px">' + esc(item.joinType) + ' JOIN</span>';
        html += '<span style="font-weight:700;color:var(--teal);margin:0 6px">' + esc(item.leftTable) + '</span>';
        html += '<span style="color:var(--text-muted)">&harr;</span>';
        html += '<span style="font-weight:700;color:var(--teal);margin:0 6px">' + esc(item.rightTable) + '</span>';
        html += '</div>';
        html += '<button class="btn btn-sm tds-close" onclick="PA.joinSummary._closeDetail()">&times;</button>';
        html += '</div>';

        html += '<div class="tds-meta">';
        html += '<span class="tds-meta-tag">Procedure: <strong class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(item.procedureId) + '\')">' + esc(item.procedureName) + '</strong></span>';
        if (item.lineNumber) {
            html += '<span class="tds-meta-tag">';
            if (item.sourceFile) {
                html += '<span class="td-line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(item.sourceFile) + '\',' + item.lineNumber + ')">Line ' + item.lineNumber + '</span>';
            } else {
                html += 'Line ' + item.lineNumber;
            }
            html += '</span>';
        }
        html += '<span class="cx-risk ' + item.complexityLabel.toLowerCase() + '">' + item.complexityLabel + '</span>';
        html += '</div>';

        html += '<div class="tds-body">';

        html += '<div class="tds-section" id="jsDetailSql">';
        html += '<div class="tds-section-title">SQL Query Context</div>';
        html += '<div class="join-sql-loading">Loading query...</div>';
        html += '</div>';

        html += '<div class="tds-section">';
        html += '<div class="tds-section-title">ON Predicate (' + item.predCount + ' condition' + (item.predCount !== 1 ? 's' : '') + ')</div>';
        if (item.predicate) {
            var parts = item.predicate.split(/\b(AND)\b/gi);
            html += '<div class="js-pred-list">';
            var condNum = 0;
            for (var pi = 0; pi < parts.length; pi++) {
                var part = parts[pi].trim();
                if (!part || part.toUpperCase() === 'AND') continue;
                condNum++;
                html += '<div class="js-pred-item"><span class="js-pred-num">' + condNum + '</span><code>' + esc(part.replace(/^ON\s+/i, '')) + '</code></div>';
            }
            html += '</div>';
        } else {
            html += '<div style="color:var(--text-muted);font-size:11px;font-style:italic">No explicit ON predicate (implicit join via WHERE clause)</div>';
        }
        html += '</div>';

        html += '</div>';

        modal.innerHTML = html;
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        requestAnimationFrame(function() { overlay.classList.add('open'); });

        document.addEventListener('keydown', PA.joinSummary._escHandler);

        PA.joinSummary._loadSqlForDetail(item);
    },

    _escHandler: function(e) {
        if (e.key === 'Escape') PA.joinSummary._closeDetail();
    },

    _closeDetail: function() {
        var el = document.getElementById('jsDetailOverlay');
        if (el) {
            el.classList.remove('open');
            setTimeout(function() { if (el.parentNode) el.remove(); }, 200);
        }
        document.removeEventListener('keydown', PA.joinSummary._escHandler);
    },

    openQueryModal: function(idx) {
        var s = PA.tf.state('jq');
        var item = s ? s.filtered[idx] : PA.joinSummary.queryData[idx];
        if (!item || !item._sql) {
            console.warn('[joinSummary] openQueryModal: no item or _sql for idx', idx);
            return;
        }

        var existing = document.getElementById('jqQueryOverlay');
        if (existing) existing.remove();

        var esc = PA.esc;
        var sql = item._sql;

        var sqlTables = {};
        var tblRe = /\bFROM\s+([A-Z_][A-Z0-9_]*)|JOIN\s+([A-Z_][A-Z0-9_]*)/gi;
        var m;
        while ((m = tblRe.exec(sql.toUpperCase())) !== null) {
            sqlTables[m[1] || m[2]] = 1;
        }
        var tableNames = Object.keys(sqlTables);
        var joinMatches = sql.match(/\bJOIN\b/gi) || [];

        var highlighted = PA.joins._highlightSql(esc(sql), null, null);
        for (var ti = 0; ti < item.tables.length; ti++) {
            var tName = item.tables[ti];
            var re = new RegExp('\\b(' + tName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'gi');
            highlighted = highlighted.replace(re, '<span class="join-sql-tbl">$1</span>');
        }

        var connections = PA.joinSummary._extractConnections(sql);
        var typeColors = {
            'INNER': 'var(--blue)', 'LEFT OUTER': 'var(--green)', 'RIGHT OUTER': 'var(--orange)',
            'LEFT': 'var(--green)', 'RIGHT': 'var(--orange)', 'CROSS': 'var(--red)',
            'FULL': 'var(--purple)', 'IMPLICIT': 'var(--text-muted)', 'JOIN': 'var(--blue)'
        };

        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'jqQueryOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) PA.joinSummary._closeQueryModal(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal';
        modal.style.maxWidth = '900px';
        modal.style.maxHeight = '90vh';
        modal.onclick = function(e) { e.stopPropagation(); };

        var html = '<div class="tds-header">';
        html += '<div class="tds-header-left">';
        html += '<span style="font-weight:700;font-size:13px">Full SQL Query</span>';
        html += '<span style="color:var(--text-muted);font-size:11px;margin-left:8px">' + esc(item.procedureName) + '</span>';
        if (item.lineNumber) html += '<span class="td-line" style="margin-left:6px;font-size:10px">L' + item.lineNumber + (item.lineEnd && item.lineEnd !== item.lineNumber ? '-' + item.lineEnd : '') + '</span>';
        html += '</div>';
        html += '<button class="btn btn-sm tds-close" onclick="PA.joinSummary._closeQueryModal()">&times;</button>';
        html += '</div>';

        html += '<div class="tds-body" style="overflow-y:auto;max-height:calc(90vh - 50px)">';

        html += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px">';
        html += '<span class="badge" style="font-size:10px">' + tableNames.length + ' tables</span>';
        html += '<span class="badge" style="font-size:10px">' + joinMatches.length + ' explicit joins</span>';
        html += '<span class="badge" style="font-size:10px">' + item.joinCount + ' total joins (incl. implicit)</span>';
        html += '<span class="cx-risk ' + item.complexityLabel.toLowerCase() + '" style="font-size:10px">' + item.complexityLabel + '</span>';
        html += '</div>';

        html += '<div class="tds-section" style="margin-bottom:10px">';
        html += '<div class="tds-section-title">Tables (' + tableNames.length + ')</div>';
        html += '<div style="display:flex;gap:4px;flex-wrap:wrap">';
        for (var tni = 0; tni < tableNames.length; tni++) {
            html += '<span style="font-weight:600;color:var(--teal);font-size:11px;background:color-mix(in srgb, var(--teal) 8%, transparent);padding:2px 6px;border-radius:3px">' + esc(tableNames[tni]) + '</span>';
        }
        html += '</div></div>';

        if (connections.length > 0) {
            html += '<div class="tds-section" style="margin-bottom:10px">';
            html += '<div class="tds-section-title">Table Connections (' + connections.length + ')</div>';
            for (var ci = 0; ci < connections.length; ci++) {
                var c = connections[ci];
                var jtColor = typeColors[c.joinType] || 'var(--text-muted)';
                html += '<div class="js-pred-item">';
                html += '<span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + ';font-size:8px;flex-shrink:0">' + esc(c.joinType) + '</span>';
                html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(c.leftTable) + '</span>';
                html += '<span style="color:var(--text-muted);font-size:9px">&harr;</span>';
                html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(c.rightTable) + '</span>';
                if (c.columns.length) {
                    html += '<span style="font-size:9px;color:var(--text-muted);margin-left:6px">ON ';
                    html += c.columns.map(function(col) { return '<code style="font-size:9px">' + esc(col) + '</code>'; }).join(' AND ');
                    html += '</span>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (item.joins && item.joins.length > 0) {
            html += '<div class="tds-section" style="margin-bottom:10px">';
            html += '<div class="tds-section-title">Detected Joins (' + item.joins.length + ')</div>';
            for (var ji = 0; ji < item.joins.length; ji++) {
                var j = item.joins[ji];
                var jc = typeColors[j.joinType] || 'var(--text-muted)';
                html += '<div class="js-pred-item">';
                html += '<span class="op-badge" style="background:color-mix(in srgb, ' + jc + ' 15%, transparent);color:' + jc + ';font-size:8px;flex-shrink:0">' + esc(j.joinType) + '</span>';
                html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(j.leftTable) + '</span>';
                html += '<span style="color:var(--text-muted);font-size:9px">&harr;</span>';
                html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(j.rightTable) + '</span>';
                if (j.predicate) {
                    html += '<code style="font-size:9px;color:var(--text-muted);margin-left:6px">' + esc(j.predicate.substring(0, 200)) + '</code>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        html += '<div class="tds-section">';
        html += '<div class="tds-section-title">SQL Query</div>';
        html += '<pre class="join-sql-code" style="max-height:none;white-space:pre-wrap;word-break:break-word">' + highlighted + '</pre>';
        html += '</div>';

        html += '</div>';

        modal.innerHTML = html;
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        requestAnimationFrame(function() { overlay.classList.add('open'); });

        var escHandler = function(e) { if (e.key === 'Escape') PA.joinSummary._closeQueryModal(); };
        document.addEventListener('keydown', escHandler);
        overlay._escHandler = escHandler;
    },

    _closeQueryModal: function() {
        var el = document.getElementById('jqQueryOverlay');
        if (el) {
            if (el._escHandler) document.removeEventListener('keydown', el._escHandler);
            el.classList.remove('open');
            setTimeout(function() { if (el.parentNode) el.remove(); }, 200);
        }
    },

    _loadSqlForDetail: async function(item) {
        var section = document.getElementById('jsDetailSql');
        if (!section) return;

        var detailFile = PA.joins._resolveDetailFile(item.procedureId);
        var nodeFile = detailFile ? detailFile.replace(/^nodes\//, '') : null;

        try {
            var nodeDetail = null;
            if (nodeFile) {
                nodeDetail = PA.joinSummary._nodeCache[nodeFile];
                if (!nodeDetail) {
                    nodeDetail = await PA.api.getNodeDetail(nodeFile);
                    if (nodeDetail) {
                        PA.joinSummary._nodeCache[nodeFile] = nodeDetail;
                        PA.joins._nodeCache[nodeFile] = nodeDetail;
                    }
                }
            }

            var sql = null;
            if (nodeDetail) {
                sql = PA.joins._findSqlContext(nodeDetail, item.lineNumber, item.leftTable, item.rightTable);
            }
            if (!sql) {
                var sf = (nodeDetail && nodeDetail.sourceFile) || item.sourceFile || PA.joinSummary._resolveSourceFile(item.procedureId);
                var ls = (nodeDetail && nodeDetail.lineStart) || 1;
                if (sf) {
                    sql = await PA.joins._findSqlFromSourceAsync(sf, item.lineNumber, ls, item.leftTable, item.rightTable);
                }
            }
            if (sql) {
                PA.joinSummary._renderSqlSection(section, sql, item);
            } else {
                section.innerHTML = '<div class="tds-section-title">SQL Query Context</div>' +
                    '<div class="join-sql-none">No cursor/query found at line ' + item.lineNumber + '</div>';
            }
        } catch(e) {
            section.innerHTML = '<div class="tds-section-title">SQL Query Context</div>' +
                '<div class="join-sql-none">Failed to load: ' + PA.esc(e.message || '') + '</div>';
        }
    },

    _renderSqlSection: function(section, sql, item) {
        var esc = PA.esc;
        var tableMatches = sql.toUpperCase().match(/\bFROM\s+([A-Z_][A-Z0-9_]*)|JOIN\s+([A-Z_][A-Z0-9_]*)/gi) || [];
        var queryTables = {};
        for (var mi = 0; mi < tableMatches.length; mi++) {
            var m = tableMatches[mi].replace(/^(FROM|JOIN)\s+/i, '').toUpperCase();
            queryTables[m] = 1;
        }
        var tableNames = Object.keys(queryTables);
        var joinMatches = sql.match(/\bJOIN\b/gi) || [];

        var connections = PA.joinSummary._extractConnections(sql);
        var highlighted = PA.joins._highlightSql(esc(sql), item.leftTable, item.rightTable);

        var html = '<div class="tds-section-title">SQL Query Context</div>';
        html += '<div class="js-sql-info">';
        html += '<span class="badge" style="font-size:9px">' + tableNames.length + ' tables</span>';
        html += '<span class="badge" style="font-size:9px">' + joinMatches.length + ' joins</span>';
        if (tableNames.length) {
            html += '<span style="font-size:10px;color:var(--text-muted);margin-left:8px">';
            html += tableNames.map(function(t) {
                return '<span style="color:var(--teal);font-weight:600">' + esc(t) + '</span>';
            }).join(', ');
            html += '</span>';
        }
        html += '</div>';

        if (connections.length > 0) {
            html += '<div style="margin:8px 0">';
            html += '<div class="tds-section-title" style="margin-bottom:4px">Table Connections (' + connections.length + ')</div>';
            for (var ci = 0; ci < connections.length; ci++) {
                var c = connections[ci];
                var jtColor = c.joinType.includes('INNER') ? 'var(--blue)' :
                    c.joinType.includes('LEFT') ? 'var(--green)' :
                    c.joinType.includes('RIGHT') ? 'var(--orange)' :
                    c.joinType.includes('CROSS') ? 'var(--red)' :
                    c.joinType.includes('FULL') ? 'var(--purple)' : 'var(--text-muted)';
                var isCurrentJoin = (c.rightTable === item.leftTable.toUpperCase() || c.rightTable === item.rightTable.toUpperCase()) &&
                    (c.leftTable === item.leftTable.toUpperCase() || c.leftTable === item.rightTable.toUpperCase());
                html += '<div class="js-pred-item" style="' + (isCurrentJoin ? 'border-left:3px solid var(--accent);background:color-mix(in srgb, var(--accent) 5%, var(--bg-secondary))' : '') + '">';
                html += '<span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + ';font-size:8px;flex-shrink:0">' + esc(c.joinType) + '</span>';
                html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(c.leftTable) + '</span>';
                html += '<span style="color:var(--text-muted);font-size:9px">&harr;</span>';
                html += '<span style="font-weight:600;color:var(--teal);font-size:10px">' + esc(c.rightTable) + '</span>';
                if (c.columns.length) {
                    html += '<span style="font-size:9px;color:var(--text-muted);margin-left:6px">ON ';
                    html += c.columns.map(function(col) {
                        return '<code style="font-size:9px;color:var(--text)">' + esc(col) + '</code>';
                    }).join(' AND ');
                    html += '</span>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        html += '<pre class="join-sql-code" style="max-height:300px">' + highlighted + '</pre>';
        section.innerHTML = html;
    },

    _extractConnections: function(sql) {
        var connections = [];
        var lines = sql.replace(/\r/g, '').split('\n');
        var flat = lines.join(' ').replace(/\s+/g, ' ');

        var joinRe = /\b(FROM|(?:LEFT|RIGHT|FULL|CROSS|INNER)?\s*(?:OUTER\s+)?JOIN)\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)\s+([A-Z_][A-Z0-9_]*)?/gi;
        var tableList = [];
        var m;
        while ((m = joinRe.exec(flat)) !== null) {
            var kw = m[1].trim().toUpperCase();
            var table = m[2].toUpperCase();
            var alias = (m[3] || '').toUpperCase();
            if (['ON', 'SET', 'VALUES', 'WHERE', 'INTO', 'WHEN', 'THEN', 'AS'].indexOf(alias) >= 0) alias = '';
            tableList.push({ keyword: kw, table: table, alias: alias, matchIdx: m.index });
        }

        var prevTable = null;
        var flatUpper = flat.toUpperCase();
        for (var i = 0; i < tableList.length; i++) {
            var entry = tableList[i];
            if (entry.keyword === 'FROM') {
                prevTable = entry.table;
                continue;
            }
            if (!prevTable) { prevTable = entry.table; continue; }

            var joinType = entry.keyword.replace(/\s+/g, ' ');

            var onCols = [];
            var joinPos = entry.matchIdx;
            if (joinPos >= 0) {
                var onPos = flatUpper.indexOf(' ON ', joinPos);
                var nextEntryPos = (i + 1 < tableList.length) ? tableList[i + 1].matchIdx : flat.length;
                if (onPos >= 0 && onPos < joinPos + 300 && onPos < nextEntryPos) {
                    var onText = flat.substring(onPos + 4);
                    var nextJoin = onText.search(/\b(LEFT|RIGHT|FULL|CROSS|INNER|JOIN|WHERE|GROUP|ORDER|HAVING|UNION)\b/i);
                    if (nextJoin > 0) onText = onText.substring(0, nextJoin);
                    var conditions = onText.split(/\bAND\b/i);
                    for (var ci = 0; ci < conditions.length && ci < 5; ci++) {
                        var cond = conditions[ci].trim();
                        if (cond.length > 3 && cond.length < 120) onCols.push(cond);
                    }
                }
            }

            connections.push({
                leftTable: prevTable,
                rightTable: entry.table,
                joinType: joinType,
                columns: onCols
            });
            prevTable = entry.table;
        }
        return connections;
    },

    _resolveSourceFile: function(procId) {
        if (!PA.analysisData || !PA.analysisData.nodes) return null;
        for (var i = 0; i < PA.analysisData.nodes.length; i++) {
            var n = PA.analysisData.nodes[i];
            if (n.nodeId === procId) return n.sourceFile || null;
        }
        return null;
    },

    // ==================== SEARCH / FILTERS / SCOPE ====================

    onSearch: function() {
        var el = document.getElementById('jsSearch');
        PA.tf.setSearch('js', el ? el.value : '');
        PA.tf.filter('js');
    },

    onSearchQuery: function() {
        var el = document.getElementById('jqSearch');
        PA.tf.setSearch('jq', el ? el.value : '');
        PA.tf.filter('jq');
    },

    clearFilters: function() {
        var el = document.getElementById('jsSearch');
        if (el) el.value = '';
        PA.tf.setSearch('js', '');
        var s = PA.tf.state('js');
        if (s) { s.colFilters = {}; }
        if (PA.tf._state['js']) PA.tf.setData('js', PA.joinSummary.data);
        PA.tf.filter('js');
        PA.tf._cfUpdateIcons('js');
    },

    clearFiltersQuery: function() {
        var el = document.getElementById('jqSearch');
        if (el) el.value = '';
        PA.tf.setSearch('jq', '');
        var s = PA.tf.state('jq');
        if (s) { s.colFilters = {}; }
        if (PA.tf._state['jq']) PA.tf.setData('jq', PA.joinSummary.queryData);
        PA.tf.filter('jq');
        PA.tf._cfUpdateIcons('jq');
    },

    _updateCounts: function() {
        var s = PA.tf.state('js');
        var allTotal = (PA.joinSummary.data || []).length;
        var dataTotal = s ? s.data.length : allTotal;
        var shown = s ? s.filtered.length : allTotal;
        var totalEl = document.getElementById('jsTotalCount');
        var scope = PA._scope;
        if (scope !== 'full' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' joins (' + scope + ')';
        } else {
            totalEl.textContent = allTotal + ' joins';
        }
        var fc = document.getElementById('jsFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    _updateQueryCounts: function() {
        var s = PA.tf.state('jq');
        var allTotal = (PA.joinSummary.queryData || []).length;
        var dataTotal = s ? s.data.length : allTotal;
        var shown = s ? s.filtered.length : allTotal;
        var totalEl = document.getElementById('jqTotalCount');
        var scope = PA._scope;
        if (scope !== 'full' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' queries (' + scope + ')';
        } else {
            totalEl.textContent = allTotal + ' queries';
        }
        var fc = document.getElementById('jqFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    applyScope: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId || PA._scope === 'full') {
            if (PA.tf._state && PA.tf._state['js']) PA.tf.setData('js', PA.joinSummary.data);
            if (PA.tf._state && PA.tf._state['jq']) PA.tf.setData('jq', PA.joinSummary.queryData);
            if (PA.tf._state && PA.tf._state['js']) { PA.joinSummary._updateCounts(); PA.joinSummary._renderSummary(); }
            if (PA.tf._state && PA.tf._state['jq']) { PA.joinSummary._updateQueryCounts(); PA.joinSummary._renderQuerySummary(); }
            return;
        }
        var mode = PA._scope;
        var nodeIds = ctx.callTreeNodeIds;
        var currentProcId = (ctx.procId || '').toUpperCase();

        var scopeFilter = function(pid) {
            pid = (pid || '').toUpperCase();
            if (mode === 'direct') return pid === currentProcId;
            if (mode === 'subtree') return pid !== currentProcId && nodeIds && nodeIds.has(pid);
            return pid === currentProcId || (nodeIds && nodeIds.has(pid));
        };

        if (PA.tf._state && PA.tf._state['js']) {
            var filtered = PA.joinSummary.data.filter(function(item) { return scopeFilter(item.procedureId); });
            PA.tf.setData('js', filtered);
            PA.joinSummary._updateCounts();
            PA.joinSummary._renderSummary();
        }

        if (PA.tf._state && PA.tf._state['jq']) {
            var filteredQ = PA.joinSummary.queryData.filter(function(item) { return scopeFilter(item.procedureId); });
            PA.tf.setData('jq', filteredQ);
            PA.joinSummary._updateQueryCounts();
            PA.joinSummary._renderQuerySummary();
        }
    },

    _applyScopeQueryOnly: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId || PA._scope === 'full') {
            PA.tf.setData('jq', PA.joinSummary.queryData);
            PA.joinSummary._updateQueryCounts();
            PA.joinSummary._renderQuerySummary();
            return;
        }
        var mode = PA._scope;
        var nodeIds = ctx.callTreeNodeIds;
        var currentProcId = (ctx.procId || '').toUpperCase();

        var scopeFilter = function(pid) {
            pid = (pid || '').toUpperCase();
            if (mode === 'direct') return pid === currentProcId;
            if (mode === 'subtree') return pid !== currentProcId && nodeIds && nodeIds.has(pid);
            return pid === currentProcId || (nodeIds && nodeIds.has(pid));
        };

        var filteredQ = PA.joinSummary.queryData.filter(function(item) { return scopeFilter(item.procedureId); });
        PA.tf.setData('jq', filteredQ);
        PA.joinSummary._updateQueryCounts();
        PA.joinSummary._renderQuerySummary();
    }
};
