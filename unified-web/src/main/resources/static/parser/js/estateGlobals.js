window.PA = window.PA || {};

/*
 * Package Globals — the estate facet's third screen.
 *
 * One row PER VARIABLE (not per package): the package aggregate hid the variable name, which is the
 * thing a reader is looking for. `/globals` lists packages, so the variable rows are built by
 * fetching each package's own `/globals/{pkg}` detail (variables[] with per-variable read/write
 * counts + the estate-wide usedBy[]) and flattening it. The table is then client-paged/filtered by
 * `PA.tf`; the expand row keeps the per-procedure footprint for that one variable.
 *
 * Oracle-only by nature: the other four dialects report an empty facet, which the empty state says.
 */
PA.estateGlobals = {
    data: [],
    _vars: [],
    _details: {},

    /** Upper bound on package detail fetches, so a huge estate cannot fan out unboundedly. */
    _MAX_PACKAGES: 200,

    async load() {
        PA.estateGlobals._details = {};
        PA.estateGlobals._vars = [];
        var container = document.getElementById('glContainer');
        if (container) container.innerHTML = '<div class="empty-msg">Loading globals…</div>';
        var rows = [];
        var err = null;
        try {
            var src = PA.api.globalsPageSource({});
            src.setParam('sort', 'PACKAGE_ASC');
            var packages = [], env;
            do {
                env = await src.fetchPage(500);
                packages = packages.concat((env && env.items) || []);
            } while (env && env.page && env.page.hasMore && packages.length < PA.estateGlobals._MAX_PACKAGES);
            rows = await PA.estateGlobals._buildVarRows(packages);
        } catch (e) {
            err = (e && e.message) ? e.message : String(e);
        }
        PA.estateGlobals._vars = rows;
        PA.estateGlobals._initTable();
        if (err) PA.tf.setLoadError('gl', err, function () { PA.estateGlobals.load(); });
        else { PA.tf.setData('gl', rows); }
        PA.estateGlobals._updateCounts();
        PA.estateGlobals._maybeEmpty();
    },

    /**
     * Flattens the package list into one row per variable. Fetches each package's detail once
     * (cached), then reads its declared `variables[]` plus any usedBy variable not declared
     * (reference-only) so nothing silently disappears.
     */
    _buildVarRows: async function(packages) {
        var details = {};
        var results = await Promise.all((packages || []).map(function (p) {
            return PA.api.getGlobalDetail(p.package).then(function (d) {
                details[p.package] = (d && !(PA.failed && PA.failed(d))) ? d : null;
            }).catch(function () { details[p.package] = null; });
        }));
        PA.estateGlobals._details = details;

        var rows = [];
        (packages || []).forEach(function (p) {
            var d = details[p.package];
            var byVar = {};
            var order = [];
            function ensure(name) {
                var k = String(name || '').toUpperCase();
                if (!k) return null;
                if (!byVar[k]) {
                    byVar[k] = { variable: name, package: p.package, reads: 0, writes: 0, procedures: {}, sourceFile: '' };
                    order.push(k);
                }
                return byVar[k];
            }
            ((d && d.variables) || []).forEach(function (v) {
                var r = ensure(v.name);
                if (!r) return;
                r.dataType = v.dataType;
                r.constant = v.constant;
                r.defaultValue = v.defaultValue;
                r.declared = v.declared !== false;
            });
            ((d && d.usedBy) || []).forEach(function (u) {
                var r = ensure(u.variable);
                if (!r) return;
                var key = String(u.procedureId || u.procedureName || '').toUpperCase();
                if (key) r.procedures[key] = { procedureId: u.procedureId, procedureName: u.procedureName };
                if (u.sourceFile) r.sourceFile = r.sourceFile || u.sourceFile;
                // Counts come from the per-usage list itself, so declared and reference-only
                // variables are counted the same way and can never disagree with the rows shown.
                if (String(u.accessType || '').toUpperCase() === 'WRITE') r.writes++;
                else r.reads++;
            });
            order.forEach(function (k) {
                var r = byVar[k];
                r.procedures = Object.keys(r.procedures).map(function (pk) { return r.procedures[pk]; });
                r.procCount = r.procedures.length;
                r.usage = (r.reads || 0) + (r.writes || 0);
                rows.push(r);
            });
        });
        rows.sort(function (a, b) { return String(a.variable).localeCompare(String(b.variable)); });
        return rows;
    },

    reset() {
        PA.estateGlobals._details = {};
        PA.estateGlobals._vars = [];
        PA.estateGlobals.data = [];
    },

    /**
     * Cross-estate link target: switch to the Details → Globals sub-tab, filter to this package and
     * open its first variable row. Used by the per-procedure footprint and by any package link.
     */
    showPackage: function(pkg) {
        if (!pkg) return;
        if (PA.switchRightTab) PA.switchRightTab('details');
        if (PA.estateTab && PA.estateTab.switchSub) PA.estateTab.switchSub('globals');
        var el = document.getElementById('glSearch');
        if (el) el.value = pkg;
        if (PA.tf) { PA.tf.setSearch('gl', pkg); PA.tf.filter('gl'); }
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
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'gl\',0)">Variable</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'gl\',1)">Package</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'gl\',2)">Used By</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'gl\',3)">Usage</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'gl\',4)">Reads</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'gl\',5)">Writes</th>';
        html += '</tr></thead><tbody id="gl-tbody"></tbody></table>';
        container.innerHTML = html;

        // Array-backed (client paged/filtered): the rows are one-per-variable, built in load().
        PA.tf.init('gl', [], 50, PA.estateGlobals._renderRow, {
            sortKeys: {
                0: { fn: function (r) { return String(r.variable || '').toUpperCase(); } },
                1: { fn: function (r) { return String(r.package || '').toUpperCase(); } },
                2: { fn: function (r) { return r.procCount || 0; } },
                3: { fn: function (r) { return r.usage || 0; } },
                4: { fn: function (r) { return r.reads || 0; } },
                5: { fn: function (r) { return r.writes || 0; } }
            },
            searchFn: function (r, q) {
                var text = (r.variable + ' ' + r.package + ' ' + (r.dataType || '') + ' ' +
                    (r.procedures || []).map(function (p) { return p.procedureName || ''; }).join(' ')).toUpperCase();
                return text.indexOf(q) !== -1;
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
        PA.tf.setSearch('gl', el ? el.value : '');
        PA.tf.filter('gl');
    },

    clearFilters: function() {
        var el = document.getElementById('glSearch');
        if (el) el.value = '';
        PA.tf.setSearch('gl', '');
        PA.tf.filter('gl');
    },

    _isFiltered: function() {
        var st = PA.tf.state('gl');
        return !!(st && st.searchQuery);
    },

    _updateCounts: function() {
        var st = PA.tf.state('gl');
        var totalEl = document.getElementById('glTotalCount');
        if (!totalEl || !st) return;
        var total = (PA.estateGlobals._vars || []).length;
        var shown = st.filtered ? st.filtered.length : total;
        totalEl.textContent = (shown === total ? total + ' variables' : shown + ' of ' + total + ' variables');
    },

    _maybeEmpty: function() {
        var st = PA.tf.state('gl');
        var tb = document.getElementById('gl-tbody');
        if (!st || !tb || st.loading || st.loadError) return;
        if (!(PA.estateGlobals._vars || []).length) {
            tb.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:24px;text-align:center">'
                + 'No package global variables in this analysis (package globals are Oracle PL/SQL only).</td></tr>';
        }
    },

    /** One row PER VARIABLE — the variable name leads; the package and its users follow. */
    _renderRow: function(v, idx, esc) {
        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'gl\',' + idx + ')">';
        html += '<td><span class="est-var-name" style="font-weight:700;font-family:var(--font-mono)">' + esc(v.variable || '') + '</span></td>';
        html += '<td><span style="font-family:var(--font-mono);color:var(--text-muted)">' + esc(v.package || '') + '</span></td>';
        html += '<td>' + PA.estateGlobals._procsCell(v, esc) + '</td>';
        html += '<td style="font-weight:700">' + (v.usage || 0) + '</td>';
        html += '<td>' + (v.reads || 0) + '</td>';
        html += '<td>' + (v.writes || 0) + '</td>';
        html += '</tr>';
        return html;
    },

    /** The used-by cell: the distinct-procedure count, then up to three clickable names. */
    _procsCell: function(v, esc) {
        var procs = v.procedures || [];
        if (!procs.length) return '<span style="color:var(--text-muted)">—</span>';
        var html = '<span class="to-usage-count">' + procs.length + '</span> ';
        var max = Math.min(procs.length, 3);
        for (var i = 0; i < max; i++) {
            var p = procs[i];
            var id = p.procedureId || p.procedureName || '';
            html += '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.showProcedure(\''
                + PA.escJs(id) + '\')">' + esc(p.procedureName || p.procedureId || '') + '</span>';
        }
        if (procs.length > max) html += ' <span style="color:var(--text-muted)">+' + (procs.length - max) + ' more</span>';
        return html;
    },

    /**
     * The expand row: this variable's per-procedure footprint (who reads/writes it, and where).
     * The package detail is already in memory from load(); a missing one is "not analyzed".
     */
    _renderDetail: function(v, idx, esc) {
        var d = PA.estateGlobals._details[v.package];
        if (d === undefined || d === null) {
            return '<div class="est-detail">' + PA.estateGlobals._notAnalyzed(v.package) + '</div>';
        }
        var key = String(v.variable || '').toUpperCase();
        var usedBy = (d.usedBy || []).filter(function (u) { return String(u.variable || '').toUpperCase() === key; });
        var html = '<div class="est-detail"><div class="est-section">';
        html += '<div class="est-section-title">' + esc(v.variable) + ' — per-procedure footprint (' + usedBy.length + ')</div>';
        if (!usedBy.length) {
            html += '<div class="est-empty">No procedure in this analysis reads or writes this variable.</div>';
        } else {
            html += PA.usageTable.attach('ut-gl-var-' + idx, usedBy, {
                showTitle: false, nameLabel: 'Variable', countLabel: 'usages',
                nameClass: 'est-var-name',
                name: function (u) { return u.variable; },
                procedure: function (u) { return u.procedureName || u.procedureId || ''; },
                procedureLink: function (u) { return "PA.showProcedure('" + PA.escJs(u.procedureId || '') + "')"; },
                badges: function (u) { return [u.accessType || 'READ']; },
                line: function (u) { return u.lineNumber; },
                lineLink: function (u) {
                    return u.lineNumber
                        ? "PA.sourceView.openAtLine('" + PA.escJs(u.sourceFile || '') + "'," + u.lineNumber + ")"
                        : null;
                },
                source: PA.usageTable.nodeSource
            });
        }
        html += '</div></div>';
        return html;
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
            // The VARIABLE name leads (bold, first column); type/default/const/read-write ride as badges.
            html += PA.usageTable.attach('ut-gl-vars-' + idx, vars, {
                showTitle: false, nameLabel: 'Variable', countLabel: 'variables', snippetLabel: 'Default',
                nameClass: 'est-var-name',
                name: function (v) { return v.name; },
                snippet: function (v) { return v.defaultValue != null ? String(v.defaultValue) : ''; },
                badges: function (v) {
                    return [v.dataType || 'not recorded', v.constant ? 'CONST' : null,
                            v.declared === false ? 'REFERENCE-ONLY' : null,
                            (v.readCount || 0) + ' reads', (v.writeCount || 0) + ' writes'];
                },
                search: function (v) { return v.declared === false ? 'reference-only' : ''; }
            });
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
            // Both names are visible at row level: the VARIABLE name leads; the procedure is the
            // second, clickable column.
            procedure: function (r) { return r.procedureName || r.procedureId || ''; },
            procedureLink: function (r) { return "PA.showProcedure('" + PA.escJs(r.procedureId || '') + "')"; },
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
            // Both names are visible at row level: the VARIABLE name leads; the procedure is the
            // second, clickable column.
            procedure: function (u) { return u.procedureName || u.procedureId || ''; },
            procedureLink: function (u) { return "PA.showProcedure('" + PA.escJs(u.procedureId || '') + "')"; },
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
