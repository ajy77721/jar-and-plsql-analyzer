window.JA = window.JA || {};

/*
 * Database Calls — the JAR -> DB procedure/function/package facet, shown as its own tab.
 *
 * Availability: the list endpoint carries `X-Analyzer-Db-Calls-Available` (read by
 * `JA.api.getDbCalls`). A JAR analysed before this facet existed answers `false` with an empty
 * envelope and a stated reason — that is rendered as a "re-analysis needed" note, never as an
 * empty list (an empty list would say "this JAR makes no DB calls", which is a different claim).
 *
 * The summary chips come from `/db-calls/summary` (by kind / package / procedure). The list itself
 * is server-paged through `PA.pageSource`, so the search box and the kind pills travel with the
 * page request.
 */
JA.dbCalls = {
    _loaded: false,
    _src: null,
    _activeKinds: new Set(),
    _available: null,

    reset() {
        JA.dbCalls._loaded = false;
        JA.dbCalls._src = null;
        JA.dbCalls._activeKinds = new Set();
        JA.dbCalls._available = null;
        if (PA.tf && PA.tf._state) delete PA.tf._state['dbc'];
        var c = document.getElementById('dbc-container');
        if (c) c.innerHTML = '';
        var s = document.getElementById('dbc-summary');
        if (s) s.innerHTML = '';
        var p = document.getElementById('dbc-kind-pills');
        if (p) p.innerHTML = '';
    },

    async load() {
        var jarId = JA.app.currentJarId;
        var container = document.getElementById('dbc-container');
        if (!jarId || !container) return;
        container.innerHTML = '<p class="sum-muted" style="padding:20px">Loading database calls...</p>';
        var summaryEl = document.getElementById('dbc-summary');
        if (summaryEl) summaryEl.innerHTML = '';

        // Probe first: the header on the list endpoint is the authoritative "does this facet exist"
        // signal, and the summary is the chip source. Either saying unavailable is enough.
        var probe = await JA.api.getDbCalls(jarId, { limit: 1 });
        var summary = await JA.api.getDbCallsSummary(jarId);

        if (!probe || probe.available === false || (summary && summary.available === false)) {
            JA.dbCalls._available = false;
            JA.dbCalls._renderUnavailable((probe && probe.reason) || (summary && summary.reason));
            return;
        }

        JA.dbCalls._available = true;
        JA.dbCalls._renderSummary(summary);
        JA.dbCalls._initKinds(summary);
        JA.dbCalls._initTable(jarId);
    },

    _renderUnavailable(reason) {
        var esc = JA.utils.escapeHtml;
        var summaryEl = document.getElementById('dbc-summary');
        if (summaryEl) summaryEl.innerHTML = '';
        var container = document.getElementById('dbc-container');
        if (!container) return;
        container.innerHTML = '<div class="dbc-note">'
            + '<div class="dbc-note-title">Database calls were not analyzed for this JAR</div>'
            + '<p>DB-call detection runs at analysis time, so there is nothing to backfill lazily. '
            + 'Re-analyze this JAR to populate this tab.</p>'
            + (reason ? '<p class="dbc-note-reason">' + esc(reason) + '</p>' : '')
            + '</div>';
    },

    _renderSummary(summary) {
        var esc = JA.utils.escapeHtml;
        var el = document.getElementById('dbc-summary');
        if (!el) return;
        if (!summary) { el.innerHTML = ''; return; }

        var byKind = summary.byKind || {};
        var byPackage = summary.byPackage || [];
        var byProcedure = summary.byProcedure || [];

        var html = '<div class="dbc-summary">';
        html += '<span class="sum-chip dbc-chip-total"><b>' + (summary.total || 0) + '</b> DB Calls</span>';

        var kinds = Object.keys(byKind);
        if (kinds.length) {
            html += '<span class="dbc-summary-label">By Kind</span>';
            for (var i = 0; i < kinds.length; i++) {
                html += '<span class="sum-chip dbc-kind-chip ' + JA.dbCalls._kindClass(kinds[i]) + '">'
                    + esc(kinds[i]) + ' <b>' + byKind[kinds[i]] + '</b></span>';
            }
        }
        if (byPackage.length) {
            html += '<span class="dbc-summary-label">By Package</span>';
            for (var p = 0; p < byPackage.length; p++) {
                html += '<span class="sum-chip" title="' + esc(byPackage[p].package) + '">'
                    + esc(byPackage[p].package) + ' <b>' + byPackage[p].count + '</b></span>';
            }
        }
        if (byProcedure.length) {
            html += '<span class="dbc-summary-label">By Procedure</span>';
            for (var q = 0; q < byProcedure.length; q++) {
                html += '<span class="sum-chip" title="' + esc(byProcedure[q].procedure) + '">'
                    + esc(byProcedure[q].procedure) + ' <b>' + byProcedure[q].count + '</b></span>';
            }
        }
        html += '</div>';
        el.innerHTML = html;
    },

    _initKinds(summary) {
        var container = document.getElementById('dbc-kind-pills');
        if (!container) return;
        var kinds = summary && summary.byKind ? Object.keys(summary.byKind) : [];
        if (!kinds.length) kinds = ['callable', 'stmt-function', 'jpa', 'mybatis', 'spring-jdbc'];
        var html = '';
        for (var i = 0; i < kinds.length; i++) {
            var k = kinds[i];
            html += '<span class="dbc-pill active" data-dbc="' + JA.utils.escapeHtml(k) + '" '
                + 'onclick="JA.dbCalls.toggleKind(\'' + Esc.js(k) + '\')">' + JA.utils.escapeHtml(k) + '</span>';
        }
        container.innerHTML = html;
        JA.dbCalls._activeKinds = new Set();
    },

    toggleKind(k) {
        var pill = document.querySelector('#dbc-kind-pills .dbc-pill[data-dbc="' + CSS.escape(k) + '"]');
        if (!pill) return;
        if (JA.dbCalls._activeKinds.size === 0) {
            JA.dbCalls._activeKinds.add(k);
            document.querySelectorAll('#dbc-kind-pills .dbc-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.dbc === k);
            });
        } else if (JA.dbCalls._activeKinds.has(k)) {
            JA.dbCalls._activeKinds.delete(k);
            pill.classList.remove('active');
            if (JA.dbCalls._activeKinds.size === 0) {
                document.querySelectorAll('#dbc-kind-pills .dbc-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            JA.dbCalls._activeKinds.add(k);
            pill.classList.add('active');
        }
        JA.dbCalls._pushKinds();
    },

    _pushKinds() {
        var src = JA.dbCalls._src;
        if (!src) return;
        var t = JA.dbCalls._activeKinds;
        src.setParam('types', t.size ? [...t].join(',') : '');
        PA.tf._restart('dbc');
    },

    _initTable(jarId) {
        var container = document.getElementById('dbc-container');
        if (!container) return;

        var html = '<table class="to-table dbc-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'dbc\',0)">Kind</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'dbc\',1)">DB Object</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'dbc\',2)">Calling Class.Method</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'dbc\',3)">Params</th>';
        html += '<th>Source</th>';
        html += '</tr></thead><tbody id="dbc-tbody"></tbody></table>';
        container.innerHTML = html;

        JA.dbCalls._src = PA.pageSource(
            '/api/jar/jars/' + encodeURIComponent(jarId) + '/db-calls', { sort: 'PACKAGE' });

        PA.tf.init('dbc', JA.dbCalls._src, 50, JA.dbCalls._renderRow, {
            serverSorts: {
                0: { asc: 'KIND_ASC', desc: 'KIND_ASC' },
                1: { asc: 'PROCEDURE_ASC', desc: 'PROCEDURE_DESC' },
                2: { asc: 'CLASS_ASC', desc: 'CLASS_DESC' },
                3: { asc: 'PARAMS_DESC', desc: 'PARAMS_DESC' }
            },
            renderDetail: JA.dbCalls._renderDetail,
            onFilter: function() {
                JA.dbCalls._updateCounts();
                JA.dbCalls._maybeEmpty();
            }
        });
    },

    onSearch() {
        var el = document.getElementById('dbcSearch');
        if (JA.dbCalls._src) JA.dbCalls._src.setParam('q', el ? el.value : '');
        PA.tf._restart('dbc');
    },

    clearFilters() {
        var el = document.getElementById('dbcSearch');
        if (el) el.value = '';
        JA.dbCalls._activeKinds.clear();
        document.querySelectorAll('#dbc-kind-pills .dbc-pill').forEach(function(p) { p.classList.add('active'); });
        var src = JA.dbCalls._src;
        if (src) { src.setParam('q', ''); src.setParam('types', ''); }
        PA.tf._restart('dbc');
    },

    _isFiltered() {
        var src = JA.dbCalls._src;
        return !!(src && src.param('q')) || JA.dbCalls._activeKinds.size > 0;
    },

    _updateCounts() {
        var st = PA.tf.state('dbc');
        var el = document.getElementById('dbcTotalCount');
        if (!el || !st) return;
        var total = st.totalIsExact ? st.serverTotal : -1;
        el.textContent = (total >= 0 ? total : (st.filtered ? st.filtered.length : 0))
            + (JA.dbCalls._isFiltered() ? ' matched' : ' calls');
    },

    _maybeEmpty() {
        var st = PA.tf.state('dbc');
        var tb = document.getElementById('dbc-tbody');
        if (!st || !tb) return;
        if (st.loading || st.loadError) return;
        if (st.totalIsExact && st.serverTotal === 0) {
            tb.innerHTML = '<tr><td colspan="20" class="dbc-empty" style="padding:24px;text-align:center">'
                + (JA.dbCalls._isFiltered()
                    ? 'No database calls match the current filters.'
                    : 'No database calls detected in this JAR.')
                + '</td></tr>';
        }
    },

    _kindClass(kind) {
        switch (String(kind || '').toLowerCase()) {
            case 'callable': return 'dbc-kind-callable';
            case 'stmt-function': return 'dbc-kind-stmt';
            case 'jpa': return 'dbc-kind-jpa';
            case 'mybatis': return 'dbc-kind-mybatis';
            case 'spring-jdbc': return 'dbc-kind-spring';
            default: return '';
        }
    },

    _renderRow(r, idx, _esc) {
        var esc = JA.utils.escapeHtml;
        var kind = r.kind || 'unknown';
        var params = (r.paramCount != null && r.paramCount >= 0) ? r.paramCount : '-';
        var source = r.source || '';
        var line = r.lineNumber ? ('L' + r.lineNumber) : '';

        var html = '<tr class="dbc-row" onclick="PA.tf.toggleDetail(\'dbc\',' + idx + ')">';
        html += '<td><span class="dbc-kind-badge ' + JA.dbCalls._kindClass(kind) + '">' + esc(kind) + '</span></td>';
        html += '<td><span class="dbc-obj">' + esc(r.dbObject || '') + '</span>'
            + (r.schema ? '<span class="dbc-sub">' + esc(r.schema) + '</span>' : '') + '</td>';
        html += '<td><span class="dbc-caller">' + esc(r.className || '') + '</span>'
            + '<span class="dbc-method">' + esc(r.methodName || '') + '</span></td>';
        html += '<td>' + params + '</td>';
        html += '<td><span class="dbc-sub">' + esc(source) + (line ? ' ' + line : '') + '</span></td>';
        html += '</tr>';
        return html;
    },

    _renderDetail(r, idx, _esc) {
        var esc = JA.utils.escapeHtml;
        var html = '<div class="dbc-detail">';
        html += '<div class="dbc-detail-meta">';
        html += '<span class="dbc-kind-badge ' + JA.dbCalls._kindClass(r.kind) + '">' + esc(r.kind || 'unknown') + '</span>';
        if (r.dbObject) html += '<span class="dbc-detail-item">object <b>' + esc(r.dbObject) + '</b></span>';
        if (r.schema) html += '<span class="dbc-detail-item">schema <b>' + esc(r.schema) + '</b></span>';
        if (r.className) html += '<span class="dbc-detail-item">in <b>' + esc(r.className) + '.' + esc(r.methodName || '') + '</b></span>';
        if (r.lineNumber) html += '<span class="dbc-detail-item">line <b>' + r.lineNumber + '</b></span>';
        if (r.paramCount != null && r.paramCount >= 0) html += '<span class="dbc-detail-item"><b>' + r.paramCount + '</b> bind params</span>';
        if (r.source) html += '<span class="dbc-detail-item">source <b>' + esc(r.source) + '</b></span>';
        html += '</div>';
        if (r.rawCall) {
            html += '<div class="dbc-detail-title">Matched Call</div>';
            html += '<pre class="dbc-sql">' + esc(r.rawCall) + '</pre>';
        }
        html += '</div>';
        return html;
    }
};
