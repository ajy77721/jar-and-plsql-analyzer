window.PA = window.PA || {};

PA.joins = {
    data: [],
    _nodeCache: {},

    async load() {
        PA.joins.data = await PA.api.getJoinOperations();
        PA.joins._initTypePills();
        PA.joins._initTable();
        PA.joins.applyScope();
    },

    _initTypePills: function() {
        var container = document.getElementById('joinTypePills');
        if (!container) return;
        var types = ['INNER', 'LEFT', 'RIGHT', 'CROSS', 'FULL'];
        var html = '';
        for (var i = 0; i < types.length; i++) {
            var t = types[i];
            html += '<span class="op-filter-pill active" data-jt="' + t + '" onclick="PA.joins.toggleType(\'' + t + '\')" title="Toggle ' + t + ' JOIN">' + t + '</span>';
        }
        container.innerHTML = html;
        PA.joins._activeTypes = new Set();
    },

    _activeTypes: new Set(),

    toggleType: function(t) {
        var pill = document.querySelector('#joinTypePills .op-filter-pill[data-jt="' + t + '"]');
        if (!pill) return;
        if (PA.joins._activeTypes.size === 0) {
            PA.joins._activeTypes.add(t);
            document.querySelectorAll('#joinTypePills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.jt === t);
            });
        } else if (PA.joins._activeTypes.has(t)) {
            PA.joins._activeTypes.delete(t);
            pill.classList.remove('active');
            if (PA.joins._activeTypes.size === 0) {
                document.querySelectorAll('#joinTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.joins._activeTypes.add(t);
            pill.classList.add('active');
        }
        PA.tf.filter('join');
    },

    _initTable: function() {
        var container = document.getElementById('joinContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'join\',0)">Left Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'join\',1)">Right Table</th>';
        html += '<th>Join Types</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'join\',3)">Usages</th>';
        html += '<th>ON Predicate</th>';
        html += '</tr></thead><tbody id="join-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('join', PA.joins.data, 50, PA.joins._renderRow, {
            sortKeys: {
                0: { fn: function(j) { return (j.leftTable || '').toUpperCase(); } },
                1: { fn: function(j) { return (j.rightTable || '').toUpperCase(); } },
                3: { fn: function(j) { return j.accessCount || 0; } }
            },
            renderDetail: PA.joins._renderDetail,
            searchFn: function(j, q) {
                return (j.leftTable || '').toUpperCase().includes(q) ||
                    (j.rightTable || '').toUpperCase().includes(q) ||
                    ((j.accessDetails || []).some(function(d) { return (d.onPredicate || '').toUpperCase().includes(q); }));
            },
            extraFilter: PA.joins._filter,
            onFilter: PA.joins._updateCounts
        });

        var s = PA.tf.state('join');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }
        PA.tf.filter('join');

        setTimeout(function() {
            PA.tf.initColFilters('join', {
                0: { label: 'Left Table', valueFn: function(j) { return j.leftTable || ''; } },
                1: { label: 'Right Table', valueFn: function(j) { return j.rightTable || ''; } },
                2: { label: 'Join Types', valueFn: function(j) { return (j.joinTypes || []).slice(); } }
            });
            PA.tf._updateSortIndicators('join');
        }, 0);
    },

    _filter: function(j) {
        var types = PA.joins._activeTypes;
        if (types.size > 0) {
            var jTypes = j.joinTypes || [];
            if (!jTypes.some(function(jt) { return types.has(jt); })) return false;
        }
        return true;
    },

    onSearch: function() {
        var el = document.getElementById('joinSearch');
        PA.tf.setSearch('join', el ? el.value : '');
        PA.tf.filter('join');
    },

    clearFilters: function() {
        document.getElementById('joinSearch').value = '';
        PA.joins._activeTypes.clear();
        document.querySelectorAll('#joinTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('join', '');
        var s = PA.tf.state('join');
        if (s) { s.colFilters = {}; }
        if (PA.tf._state['join']) PA.tf.setData('join', PA.joins.data);
        PA.tf.filter('join');
        PA.tf._cfUpdateIcons('join');
    },

    _updateCounts: function() {
        var s = PA.tf.state('join');
        var allTotal = (PA.joins.data || []).length;
        var dataTotal = s ? s.data.length : allTotal;
        var shown = s ? s.filtered.length : allTotal;
        var totalEl = document.getElementById('joinTotalCount');
        var scope = PA._scope;
        if (scope !== 'full' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' joins (' + scope + ')';
        } else {
            totalEl.textContent = allTotal + ' joins';
        }
        var fc = document.getElementById('joinFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    _renderRow: function(j, idx, esc) {
        var typeBadges = (j.joinTypes || []).map(function(jt) {
            var color = jt === 'INNER' ? 'var(--blue)' : jt === 'LEFT' ? 'var(--green)' :
                        jt === 'RIGHT' ? 'var(--orange)' : jt === 'CROSS' ? 'var(--red)' :
                        jt === 'FULL' ? 'var(--purple)' : 'var(--text-muted)';
            return '<span class="op-badge" style="background:color-mix(in srgb, ' + color + ' 15%, transparent);color:' + color + '">' + jt + '</span>';
        }).join('');

        var firstDetail = (j.accessDetails || [])[0];
        var predPreview = firstDetail && firstDetail.onPredicate
            ? esc(firstDetail.onPredicate.substring(0, 80)) + (firstDetail.onPredicate.length > 80 ? '...' : '')
            : '-';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'join\',' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--teal)" data-tip="Left table in join">' + esc(j.leftTable || '') + '</span></td>';
        html += '<td><span style="font-weight:600;color:var(--teal)" data-tip="Right table in join">' + esc(j.rightTable || '') + '</span></td>';
        html += '<td>' + typeBadges + '</td>';
        html += '<td>' + (j.accessCount || 0) + '</td>';
        html += '<td style="font-size:10px;font-family:var(--font-mono);color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + predPreview + '</td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(j, idx, esc) {
        var details = j.accessDetails || [];
        var html = '<div class="to-detail">';

        if (details.length > 0) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Join Occurrences (' + details.length + ')</div>';
            for (var i = 0; i < details.length; i++) {
                var d = details[i];
                var jtColor = d.joinType === 'INNER' ? 'var(--blue)' : d.joinType === 'LEFT' ? 'var(--green)' :
                              d.joinType === 'RIGHT' ? 'var(--orange)' : d.joinType === 'CROSS' ? 'var(--red)' :
                              d.joinType === 'FULL' ? 'var(--purple)' : 'var(--text-muted)';
                html += '<div class="to-detail-item">';
                html += '<span class="op-badge" style="background:color-mix(in srgb, ' + jtColor + ' 15%, transparent);color:' + jtColor + '">' + (d.joinType || '?') + ' JOIN</span>';
                html += '<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(d.procedureId || d.procedureName || '') + '\')">' + esc(d.procedureName || '') + '</span>';
                if (d.lineNumber) {
                    html += '<span class="to-detail-line" data-tip="Open source at line" onclick="event.stopPropagation(); PA.sourceView.openAtLine(\'' + PA.escJs(d.sourceFile || '') + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>';
                }
                html += '<button class="join-sql-toggle" onclick="event.stopPropagation(); PA.joins.toggleSql(' + idx + ',' + i + ')">Show SQL</button>';
                if (d.onPredicate) {
                    html += '<div class="to-join-predicate">' + esc(d.onPredicate.substring(0, 200)) + (d.onPredicate.length > 200 ? '...' : '') + '</div>';
                }
                html += '</div>';
                html += '<div class="join-sql-block" id="join-sql-' + idx + '-' + i + '" style="display:none"><div class="join-sql-loading">Loading...</div></div>';
            }
            html += '</div>';
        }

        if (details.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No details available</div>';
        }

        html += '</div>';
        return html;
    },

    toggleSql: async function(joinIdx, detailIdx) {
        var block = document.getElementById('join-sql-' + joinIdx + '-' + detailIdx);
        if (!block) return;
        if (block.style.display !== 'none') {
            block.style.display = 'none';
            var btn = block.previousElementSibling;
            while (btn && !btn.classList.contains('to-detail-item')) btn = btn.previousElementSibling;
            if (btn) { var b = btn.querySelector('.join-sql-toggle'); if (b) b.textContent = 'Show SQL'; }
            return;
        }
        block.style.display = '';
        var btn2 = block.previousElementSibling;
        while (btn2 && !btn2.classList.contains('to-detail-item')) btn2 = btn2.previousElementSibling;
        if (btn2) { var b2 = btn2.querySelector('.join-sql-toggle'); if (b2) b2.textContent = 'Hide SQL'; }

        if (block.dataset.loaded === '1') return;

        var s = PA.tf.state('join');
        if (!s) return;
        var j = s.data[joinIdx];
        if (!j || !j.accessDetails || !j.accessDetails[detailIdx]) return;
        var d = j.accessDetails[detailIdx];

        var detailFile = PA.joins._resolveDetailFile(d.procedureId);
        var nodeFile = detailFile ? detailFile.replace(/^nodes\//, '') : null;

        try {
            var nodeDetail = null;
            if (nodeFile) {
                nodeDetail = PA.joins._nodeCache[nodeFile];
                if (!nodeDetail) {
                    nodeDetail = await PA.api.getNodeDetail(nodeFile);
                    if (nodeDetail) PA.joins._nodeCache[nodeFile] = nodeDetail;
                }
            }

            var sql = null;
            if (nodeDetail) {
                sql = PA.joins._findSqlContext(nodeDetail, d.lineNumber, j.leftTable, j.rightTable);
            }
            if (!sql) {
                var sf = (nodeDetail && nodeDetail.sourceFile) || d.sourceFile;
                var ls = (nodeDetail && nodeDetail.lineStart) || 1;
                if (sf) {
                    sql = await PA.joins._findSqlFromSourceAsync(sf, d.lineNumber, ls, j.leftTable, j.rightTable);
                }
            }
            if (sql) {
                var highlighted = PA.joins._highlightSql(PA.esc(sql), j.leftTable, j.rightTable);
                block.innerHTML = '<pre class="join-sql-code">' + highlighted + '</pre>';
            } else {
                block.innerHTML = '<div class="join-sql-none">No cursor/query found at line ' + d.lineNumber + '</div>';
            }
        } catch (e) {
            block.innerHTML = '<div class="join-sql-none">Failed to load: ' + PA.esc(e.message || '') + '</div>';
        }
        block.dataset.loaded = '1';
    },

    _resolveDetailFile: function(procId) {
        if (!PA.analysisData || !PA.analysisData.nodes) return null;
        for (var i = 0; i < PA.analysisData.nodes.length; i++) {
            var n = PA.analysisData.nodes[i];
            if (n.nodeId === procId) return n.detailFile || null;
        }
        return null;
    },

    _isTruncated: function(q) {
        return q && q.length > 1900 && /\.\.\.\s*$/.test(q);
    },

    _findSqlContext: function(nodeDetail, lineNumber, leftTable, rightTable) {
        if (!nodeDetail || !lineNumber) return null;
        var cursors = nodeDetail.cursors || [];
        var lt = (leftTable || '').toUpperCase();
        var rt = (rightTable || '').toUpperCase();
        var TOLERANCE = 15;

        for (var i = 0; i < cursors.length; i++) {
            var c = cursors[i];
            if (!c.query || PA.joins._isTruncated(c.query)) continue;
            var cStart = c.line - TOLERANCE;
            var cEnd = (c.lineEnd || c.line) + TOLERANCE;
            if (cStart <= lineNumber && cEnd >= lineNumber) {
                return c.query;
            }
        }

        var bestMatch = null, bestDist = Infinity;
        for (var j = 0; j < cursors.length; j++) {
            var c2 = cursors[j];
            if (!c2.query || PA.joins._isTruncated(c2.query)) continue;
            var q = c2.query.toUpperCase();
            var hasLeft = lt && q.includes(lt);
            var hasRight = rt && q.includes(rt);
            if (hasLeft && hasRight) {
                var dist = Math.abs(c2.line - lineNumber);
                if (dist < bestDist) { bestMatch = c2.query; bestDist = dist; }
            }
        }
        if (bestMatch) return bestMatch;

        for (var k = 0; k < cursors.length; k++) {
            var c3 = cursors[k];
            if (!c3.query || PA.joins._isTruncated(c3.query)) continue;
            var q3 = c3.query.toUpperCase();
            if ((lt && q3.includes(lt)) || (rt && q3.includes(rt))) {
                var dist3 = Math.abs(c3.line - lineNumber);
                if (dist3 < 200 && dist3 < bestDist) { bestMatch = c3.query; bestDist = dist3; }
            }
        }
        return bestMatch;
    },

    _extractSqlFromSource: function(sourceText, lineNumber, nodeLineStart) {
        if (!sourceText || !lineNumber) return null;
        var lines = sourceText.split(/\n/);
        var localLine = lineNumber - (nodeLineStart || 1);
        if (localLine < 0 || localLine >= lines.length) return null;

        var sqlStart = /^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH)\b/i;
        var cursorIs = /\bCURSOR\b.*\bIS\s*$/i;
        var openFor = /\bOPEN\b.*\bFOR\s*$/i;

        var _stripStrings = function(ln) {
            return ln.replace(/'[^']*'/g, '').replace(/"[^"]*"/g, '');
        };
        var _countParens = function(ln) {
            var s = _stripStrings(ln);
            var d = 0;
            for (var i = 0; i < s.length; i++) {
                if (s[i] === '(') d++;
                else if (s[i] === ')') d--;
            }
            return d;
        };
        var _endsWithSemicolon = function(ln) {
            return /;\s*$/.test(_stripStrings(ln));
        };

        var depth = 0;
        var startIdx = localLine;
        for (var up = localLine; up >= 0; up--) {
            var line = lines[up];
            depth -= _countParens(line);
            if (depth <= 0 && sqlStart.test(line)) {
                startIdx = up;
                if (up > 0 && (cursorIs.test(lines[up - 1]) || openFor.test(lines[up - 1]))) {
                    startIdx = up;
                }
                break;
            }
            if (up < localLine && depth <= 0 && _endsWithSemicolon(line)) {
                if (up + 1 < lines.length && sqlStart.test(lines[up + 1])) {
                    startIdx = up + 1;
                } else {
                    startIdx = up + 1;
                }
                break;
            }
            if (localLine - up > 400) { startIdx = up; break; }
        }

        var endIdx = localLine;
        var pd = 0;
        for (var dn = startIdx; dn < lines.length && dn < startIdx + 500; dn++) {
            var ln = lines[dn];
            pd += _countParens(ln);
            if (dn >= localLine && pd <= 0 && _endsWithSemicolon(ln)) { endIdx = dn; break; }
            endIdx = dn;
        }
        var sql = lines.slice(startIdx, endIdx + 1).join('\n').replace(/;\s*$/, '').trim();
        if (sql.length < 10) return null;
        return sql;
    },

    _findSqlFromSourceAsync: async function(sourceFile, lineNumber, nodeLineStart, leftTable, rightTable) {
        if (!sourceFile) return null;
        var cacheKey = 'src_' + sourceFile;
        var sourceText = PA.joins._nodeCache[cacheKey];
        if (!sourceText) {
            try {
                var result = await PA.api.getSource(sourceFile);
                if (result && result.content) {
                    sourceText = result.content;
                    PA.joins._nodeCache[cacheKey] = sourceText;
                } else return null;
            } catch(e) { return null; }
        }
        var lt = leftTable ? leftTable.toUpperCase() : '';
        var rt = rightTable ? rightTable.toUpperCase() : '';
        var offsets = [0, 1, 2, 3, 4, 5, -1, -2, -3, 6, 7, 8, 9, 10, -4, -5];
        for (var i = 0; i < offsets.length; i++) {
            var tryLine = lineNumber + offsets[i];
            var sql = PA.joins._extractSqlFromSource(sourceText, tryLine, nodeLineStart);
            if (!sql) continue;
            if (!lt && !rt) return sql;
            var upper = sql.toUpperCase();
            if (upper.includes(lt) || upper.includes(rt)) return sql;
        }
        return null;
    },

    _highlightSql: function(escapedSql, leftTable, rightTable) {
        var keywords = /\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|INTO|INSERT|UPDATE|DELETE|MERGE|SET|VALUES|GROUP|ORDER|BY|HAVING|IN|NOT|NULL|IS|AS|CASE|WHEN|THEN|ELSE|END|EXISTS|BETWEEN|LIKE|DISTINCT|COUNT|SUM|MAX|MIN|AVG|NVL|DECODE|TRIM|UPPER|LOWER|SUBSTR|TO_CHAR|TO_DATE|TO_NUMBER|CURSOR|FOR|LOOP|ROWNUM|FETCH|FIRST|ROWS|ONLY|UNION|ALL|INTERSECT|MINUS|WITH|RECURSIVE|CONNECT|START|PRIOR|LEVEL|ROWID|DUAL|TRUNC|SYSDATE|BULK|COLLECT|FORALL|RETURNING|NEXTVAL|CURRVAL)\b/gi;
        var result = escapedSql.replace(keywords, '<span class="join-sql-kw">$1</span>');
        if (leftTable) {
            var ltRe = new RegExp('\\b(' + leftTable.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'gi');
            result = result.replace(ltRe, '<span class="join-sql-tbl">$1</span>');
        }
        if (rightTable) {
            var rtRe = new RegExp('\\b(' + rightTable.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'gi');
            result = result.replace(rtRe, '<span class="join-sql-tbl">$1</span>');
        }
        return result;
    },

    applyScope: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tf.setData('join', PA.joins.data);
            PA.joins._updateCounts();
            return;
        }
        PA.joins._applyScopeData();
    },

    _applyScopeData: function() {
        var mode = PA._scope;
        var ctx = PA.context;
        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('join', PA.joins.data);
            PA.joins._updateCounts();
            return;
        }
        var nodeIds = ctx.callTreeNodeIds;
        var currentProcId = (ctx.procId || '').toUpperCase();

        var filtered = PA.joins.data.filter(function(j) {
            return (j.accessDetails || []).some(function(d) {
                var pid = (d.procedureId || d.procedureName || '').toUpperCase();
                if (mode === 'direct') return pid === currentProcId;
                if (mode === 'subtree') return pid !== currentProcId && nodeIds && nodeIds.has(pid);
                return pid === currentProcId || (nodeIds && nodeIds.has(pid));
            });
        });
        PA.tf.setData('join', filtered);
        PA.joins._updateCounts();
    }
};
