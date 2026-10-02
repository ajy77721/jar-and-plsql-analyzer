window.PA = window.PA || {};

/*
 * Package Globals — the estate facet's third screen.
 *
 * One row per package (server-paged over `/globals`, search `q`). Expanding a package lazily
 * fetches `/globals/{pkg}`, whose `usedBy[]` carries every read/write of a variable of that
 * package with the referencing procedure and line. The detail groups that list by variable, so the
 * "who reads/writes it" question is answered per variable, not just per package.
 *
 * Oracle-only by nature: the other four dialects report an empty facet, which the empty state says.
 */
PA.estateGlobals = {
    data: [],
    _src: null,
    _details: {},

    async load() {
        PA.estateGlobals._details = {};
        PA.estateGlobals._src = PA.api.globalsPageSource({});
        PA.estateGlobals._src.setParam('sort', 'PACKAGE_ASC');
        PA.estateGlobals._initTable();
    },

    reset() {
        PA.estateGlobals._details = {};
        PA.estateGlobals.data = [];
    },

    /**
     * Cross-estate link target: switch to the Details → Globals sub-tab, filter to this package and
     * open its detail row. Used by the per-procedure footprint and by any package link.
     */
    showPackage: function(pkg) {
        if (!pkg) return;
        if (PA.switchRightTab) PA.switchRightTab('details');
        if (PA.estateTab && PA.estateTab.switchSub) PA.estateTab.switchSub('globals');
        var el = document.getElementById('glSearch');
        if (el) el.value = pkg;
        var src = PA.estateGlobals._src;
        if (src && PA.tf) {
            src.setParam('q', pkg);
            PA.tf._restart('gl');
        }
        PA.estateGlobals._focus('gl', pkg, 'package');
    },

    /** Poll for the named row in a server-paged table, then toggle its detail open and scroll to it. */
    _focus: function(tfId, name, field) {
        var tries = 0;
        var want = String(name).toUpperCase();
        var iv = setInterval(function() {   // poll-exempt: local DOM poll for a just-navigated row, no network request
            var st = PA.tf && PA.tf.state ? PA.tf.state(tfId) : null;
            var rows = st && st.filtered;
            if (rows && rows.length) {
                for (var i = 0; i < rows.length; i++) {
                    if (String(rows[i][field] || '').toUpperCase() === want) {
                        clearInterval(iv);
                        var idx = st.page * st.pageSize + i;
                        PA.tf.toggleDetail(tfId, idx);
                        var rowEl = document.getElementById(tfId + '-detail-' + idx);
                        if (rowEl && rowEl.scrollIntoView) rowEl.scrollIntoView({ block: 'center' });
                        return;
                    }
                }
            }
            if (++tries > 40) clearInterval(iv);
        }, 100);
    },

    _initTable: function() {
        var container = document.getElementById('glContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'gl\',0)">Package</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'gl\',1)">Variables</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'gl\',2)">Usage</th>';
        html += '<th>Reads</th>';
        html += '<th>Writes</th>';
        html += '</tr></thead><tbody id="gl-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('gl', PA.estateGlobals._src, 50, PA.estateGlobals._renderRow, {
            serverSorts: {
                0: { asc: 'PACKAGE_ASC', desc: 'PACKAGE_DESC' },
                1: { asc: 'VARIABLES_ASC', desc: 'VARIABLES_DESC' },
                2: { asc: 'USAGES_ASC', desc: 'USAGES_DESC' }
            },
            renderDetail: PA.estateGlobals._renderDetail,
            onFilter: function() {
                PA.estateGlobals._updateCounts();
                PA.estateGlobals._maybeEmpty();
            }
        });
        var st = PA.tf.state('gl');
        if (st) { st.sortCol = 0; st.sortDir = 'asc'; }
        PA.tf._updateSortIndicators('gl');
    },

    onSearch: function() {
        var el = document.getElementById('glSearch');
        PA.estateGlobals._src.setParam('q', el ? el.value : '');
        PA.tf._restart('gl');
    },

    clearFilters: function() {
        var el = document.getElementById('glSearch');
        if (el) el.value = '';
        var src = PA.estateGlobals._src;
        if (src) src.setParam('q', '');
        PA.tf._restart('gl');
    },

    _isFiltered: function() {
        var src = PA.estateGlobals._src;
        return !!(src && src.param('q'));
    },

    _updateCounts: function() {
        var st = PA.tf.state('gl');
        var totalEl = document.getElementById('glTotalCount');
        if (!totalEl || !st) return;
        var total = st.totalIsExact ? st.serverTotal : -1;
        totalEl.textContent = (total >= 0 ? total : (st.filtered ? st.filtered.length : 0))
            + (PA.estateGlobals._isFiltered() ? ' matched' : ' packages');
    },

    _maybeEmpty: function() {
        var st = PA.tf.state('gl');
        var tb = document.getElementById('gl-tbody');
        if (!st || !tb) return;
        if (st.loading || st.loadError) return;
        if (st.totalIsExact && st.serverTotal === 0) {
            tb.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:24px;text-align:center">'
                + (PA.estateGlobals._isFiltered()
                    ? 'No packages match the current filter.'
                    : 'No package global variables in this analysis (package globals are Oracle PL/SQL only).')
                + '</td></tr>';
        }
    },

    _renderRow: function(p, idx, esc) {
        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'gl\',' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--badge-purple);font-family:var(--font-mono)">' + esc(p.package || '') + '</span></td>';
        html += '<td>' + (p.variableCount || 0) + '</td>';
        html += '<td>' + (p.usageCount || 0) + '</td>';
        html += '<td>' + (p.readCount || 0) + '</td>';
        html += '<td>' + (p.writeCount || 0) + '</td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(p, idx, esc) {
        var cached = PA.estateGlobals._details[p.package];
        if (cached === undefined) {
            PA.estateGlobals._fetchDetail(p, idx);
            return '<div class="est-detail"><div class="est-loading">Loading package detail…</div></div>';
        }
        if (!cached) return '<div class="est-detail">' + PA.estateGlobals._notAnalyzed(p.package) + '</div>';
        return '<div class="est-detail">' + PA.estateGlobals._detailHtml(cached, esc, idx) + '</div>';
    },

    _fetchDetail: function(p, idx) {
        var rowEl = document.getElementById('gl-detail-' + idx);
        var fill = function(detail) {
            PA.estateGlobals._details[p.package] = detail;
            if (rowEl && rowEl.isConnected) {
                rowEl.innerHTML = '<td colspan="20">' + PA.estateGlobals._renderDetail(p, idx, PA.esc) + '</td>';
            }
        };
        PA.api.getGlobalDetail(p.package).then(function(d) {
            fill((d && !(PA.failed && PA.failed(d))) ? d : null);
        }).catch(function() {
            fill(null);
        });
    },

    _notAnalyzed: function(name) {
        return '<div class="est-notanalyzed"><b>Not analyzed — ' + PA.esc(name) + '</b>'
            + 'This package is listed, but its declaration detail is not present in this analysis. '
            + 'Re-run the analysis to populate its variable types and defaults.</div>';
    },

    _detailHtml: function(d, esc, idx) {
        var html = '';

        html += '<div class="est-meta">';
        html += '<span class="est-badge" style="background:var(--badge-purple-bg);color:var(--badge-purple)">PACKAGE</span>';
        html += '<span class="est-meta-item">package <b style="font-family:var(--font-mono)">' + esc(d.package || '') + '</b></span>';
        html += '<span class="est-meta-item"><b>' + (d.variableCount || 0) + '</b> variables</span>';
        html += '<span class="est-meta-item"><b>' + (d.readCount || 0) + '</b> reads</span>';
        html += '<span class="est-meta-item"><b>' + (d.writeCount || 0) + '</b> writes</span>';
        html += '</div>';

        var vars = d.variables || [];
        html += '<div class="est-section"><div class="est-section-title">Variables (' + vars.length + ')</div>';
        if (vars.length) {
            html += '<table class="est-cols"><thead><tr><th>Name</th><th>Data Type</th><th>Default</th><th>Const</th><th>Reads</th><th>Writes</th></tr></thead><tbody>';
            for (var i = 0; i < vars.length; i++) {
                var v = vars[i];
                var declared = v.declared !== false;
                html += '<tr>'
                    + '<td class="est-col-name est-var-name">' + esc(v.name || '') + '</td>'
                    + '<td class="est-col-type">' + (v.dataType ? esc(v.dataType) : '<span style="color:var(--text-muted)">not recorded</span>') + '</td>'
                    + '<td class="est-col-type">' + (v.defaultValue != null ? esc(v.defaultValue) : '<span style="color:var(--text-muted)">-</span>') + '</td>'
                    + '<td>' + (v.constant ? 'YES' : 'NO') + '</td>'
                    + '<td>' + (v.readCount || 0) + '</td>'
                    + '<td>' + (v.writeCount || 0) + '</td>'
                    + '</tr>';
                if (!declared) {
                    html += '<tr><td colspan="6" style="font-size:10px;color:var(--text-muted);padding-top:0">'
                        + 'Reference-only — no declaration captured (analysis predates declaration persistence).</td></tr>';
                }
            }
            html += '</tbody></table>';
        } else {
            html += '<div class="est-empty">No variables recorded for this package.</div>';
        }
        html += '</div>';

        // Variable-name metadata, so each usage row can show its type/default/const badges.
        var varMeta = {};
        for (var vi = 0; vi < vars.length; vi++) {
            if (vars[vi] && vars[vi].name) varMeta[String(vars[vi].name).toUpperCase()] = vars[vi];
        }

        html += '<div class="est-section"><div class="est-section-title">Readers &amp; Writers (' + ((d.usedBy || []).length) + ')</div>';
        html += PA.estateGlobals._usedByHtml(d, esc, idx, varMeta);
        html += '</div>';

        // The reverse view: each procedure's footprint on this package's globals.
        if (d.byProcedure && d.byProcedure.length) {
            html += '<div class="est-section"><div class="est-section-title">Per-procedure footprint (' + d.byProcedure.length + ')</div>';
            html += PA.estateGlobals._byProcedureHtml(d.byProcedure, esc, idx, varMeta);
            html += '</div>';
        }

        return html;
    },

    /**
     * The per-procedure footprint, one usage row per variable read/written. Per the UX standard the
     * VARIABLE name leads (bold, first column); the referencing procedure is the secondary link and
     * the variable's type/default/const ride along as badges.
     */
    _byProcedureHtml: function(byProc, esc, idx, varMeta) {
        var rows = [];
        for (var i = 0; i < (byProc || []).length; i++) {
            var p = byProc[i];
            ['reads', 'writes'].forEach(function (side) {
                var kind = side === 'writes' ? 'WRITE' : 'READ';
                (p[side] || []).forEach(function (r) {
                    rows.push({
                        variable: r.variable, lineNumber: r.lineNumber, accessType: kind,
                        procedureId: p.procedureId, procedureName: p.procedureName, sourceFile: p.sourceFile
                    });
                });
            });
        }
        return PA.usageTable.attach('ut-gl-footprint-' + idx, rows, {
            showTitle: false, nameLabel: 'Variable', countLabel: 'usages',
            nameClass: 'est-var-name',
            name: function (r) { return r.variable; },
            sub: function (r) {
                return { text: r.procedureName || r.procedureId || '',
                         link: "PA.showProcedure('" + PA.escJs(r.procedureId || '') + "')" };
            },
            badges: function (r) {
                var m = (varMeta || {})[String(r.variable || '').toUpperCase()] || {};
                return [r.accessType, m.dataType, m.constant ? 'CONST' : null,
                        m.defaultValue != null ? 'DEFAULT ' + m.defaultValue : null];
            },
            line: function (r) { return r.lineNumber; },
            source: PA.usageTable.nodeSource
        });
    },

    /**
     * The package's readers & writers, one row per usage. The VARIABLE name leads (bold, first
     * column); the referencing procedure is the secondary link.
     */
    _usedByHtml: function(d, esc, idx, varMeta) {
        var usedBy = d.usedBy || [];
        if (!usedBy.length) return '<div class="est-empty">No reads or writes of this package\'s variables in this analysis.</div>';
        return PA.usageTable.attach('ut-gl-usedby-' + idx, usedBy, {
            showTitle: false, nameLabel: 'Variable', countLabel: 'usages',
            nameClass: 'est-var-name',
            name: function (u) { return u.variable; },
            sub: function (u) {
                return { text: u.procedureName || u.procedureId || '',
                         link: "PA.showProcedure('" + PA.escJs(u.procedureId || '') + "')" };
            },
            badges: function (u) {
                var m = (varMeta || {})[String(u.variable || '').toUpperCase()] || {};
                return [u.accessType || 'READ', m.dataType, m.constant ? 'CONST' : null];
            },
            line: function (u) { return u.lineNumber; },
            lineLink: function (u) {
                return u.lineNumber
                    ? "PA.sourceView.openAtLine('" + PA.escJs(u.sourceFile || '') + "'," + u.lineNumber + ")"
                    : null;
            },
            source: PA.usageTable.nodeSource
        });
    }
};
