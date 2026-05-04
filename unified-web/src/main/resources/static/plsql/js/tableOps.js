/**
 * PA.tableOps — Table Operations with operation pill filters, pagination, expandable details.
 * Uses PA.tf (shared table framework) for pagination, sorting, column filters.
 */
window.PA = window.PA || {};

PA.tableOps = {
    data: [],
    activeOps: new Set(),  // active operation filters (empty = all)
    scopeMode: 'all',      // 'all' | 'scoped'

    async load() {
        PA.tableOps.data = await PA.api.getTableOperations();
        PA.tableOps.activeOps.clear();
        PA.tableOps.scopeMode = 'all';
        PA.tableOps._initOpPills();
        PA.tableOps._initTable();
        PA.tableOps._updateScopeToggle();
    },

    /** Initialize the table framework with data */
    _initTable() {
        const container = document.getElementById('toContainer');
        if (!container) return;

        // Build table shell with sortable headers
        let html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'to\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'to\',1)">Type</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'to\',2)">Schema</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'to\',4)">Count</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'to\',5)">Triggers</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'to\',6)">Scope</th>';
        html += '</tr></thead><tbody id="to-tbody"></tbody></table>';
        container.innerHTML = html;

        // Initialize framework
        PA.tf.init('to', PA.tableOps.data, 50, PA.tableOps._renderRow, {
            sortKeys: {
                0: { fn: t => (t.tableName || '').toUpperCase() },
                1: { fn: t => (t.tableType || 'TABLE').toUpperCase() },
                2: { fn: t => (t.schemaName || '').toUpperCase() },
                4: { fn: t => t.accessCount || (t.accessDetails || []).length },
                5: { fn: t => (t.triggers || []).filter(tr => tr.status === 'ENABLED').length },
                6: { fn: t => t.external ? 1 : 0 }
            },
            renderDetail: PA.tableOps._renderDetail,
            searchFn: (t, q) => (t.tableName || '').toUpperCase().includes(q) ||
                                (t.schemaName || '').toUpperCase().includes(q),
            extraFilter: PA.tableOps._opFilter,
            onFilter: PA.tableOps._updateCounts
        });

        // Default sort: by count descending
        const s = PA.tf.state('to');
        if (s) { s.sortCol = 4; s.sortDir = 'desc'; }

        PA.tf.filter('to');

        // Init column filters after table is in DOM
        setTimeout(() => {
            PA.tf.initColFilters('to', {
                1: { label: 'Type', valueFn: t => (t.tableType || 'TABLE').toUpperCase() },
                2: { label: 'Schema', valueFn: t => t.schemaName || '-' },
                6: { label: 'Scope', valueFn: t => t.external ? 'EXTERNAL' : 'INTERNAL' }
            });
            PA.tf._updateSortIndicators('to');
        }, 0);
    },

    /** Combined filter predicate: operation pills + type dropdown + ext/trig checkboxes */
    _opFilter(t) {
        const ops = PA.tableOps.activeOps;
        if (ops.size > 0 && !(t.operations || []).some(op => ops.has(op))) return false;
        const typeFilter = document.getElementById('toTypeFilter')?.value || '';
        if (typeFilter && (t.tableType || 'TABLE').toUpperCase() !== typeFilter) return false;
        if (document.getElementById('toExtOnly')?.checked && !t.external) return false;
        if (document.getElementById('toTrigOnly')?.checked && (t.triggers || []).length === 0) return false;
        return true;
    },

    /** Initialize operation filter pills */
    _initOpPills() {
        const container = document.getElementById('toOpPills');
        if (!container) return;
        const ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        let html = '';
        for (const op of ops) {
            html += `<span class="op-filter-pill ${op} active" data-op="${op}" onclick="PA.tableOps.toggleOp('${op}')" title="Toggle ${op} filter">${op}</span>`;
        }
        container.innerHTML = html;
    },

    toggleOp(op) {
        const pill = document.querySelector(`#toOpPills .op-filter-pill[data-op="${op}"]`);
        if (!pill) return;
        if (PA.tableOps.activeOps.size === 0) {
            PA.tableOps.activeOps.add(op);
            document.querySelectorAll('#toOpPills .op-filter-pill').forEach(p => {
                p.classList.toggle('active', p.dataset.op === op);
            });
        } else if (PA.tableOps.activeOps.has(op)) {
            PA.tableOps.activeOps.delete(op);
            pill.classList.remove('active');
            if (PA.tableOps.activeOps.size === 0) {
                document.querySelectorAll('#toOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
            }
        } else {
            PA.tableOps.activeOps.add(op);
            pill.classList.add('active');
        }
        PA.tf.filter('to');
    },

    clearFilters() {
        document.getElementById('toSearch').value = '';
        document.getElementById('toTypeFilter').value = '';
        const extBox = document.getElementById('toExtOnly'); if (extBox) extBox.checked = false;
        const trigBox = document.getElementById('toTrigOnly'); if (trigBox) trigBox.checked = false;
        PA.tableOps.activeOps.clear();
        document.querySelectorAll('#toOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
        PA.tf.setSearch('to', '');
        const s = PA.tf.state('to');
        if (s) { s.colFilters = {}; }
        PA.tf.filter('to');
        PA.tf._cfUpdateIcons('to');
    },

    /** Called from search input */
    onSearch() {
        PA.tf.setSearch('to', document.getElementById('toSearch')?.value || '');
        PA.tf.filter('to');
    },

    _updateCounts() {
        const s = PA.tf.state('to');
        const allTotal = (PA.tableOps.data || []).length;
        const dataTotal = s ? s.data.length : allTotal;
        const shown = s ? s.filtered.length : allTotal;
        const totalEl = document.getElementById('toTotalCount');
        if (PA.tableOps.scopeMode === 'scoped' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' tables (scoped)';
        } else {
            totalEl.textContent = allTotal + ' tables';
        }
        const fc = document.getElementById('toFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else { fc.style.display = 'none'; }
    },

    /** Render a single table row */
    _renderRow(t, idx, esc) {
        const tid = PA.escAttr(t.tableName || '');
        const ops = (t.operations || []).map(op => `<span class="op-badge ${op}">${op}</span>`).join('');
        const count = t.accessCount || (t.accessDetails || []).length;
        const claudeCount = (t.accessDetails || []).filter(d => d.sourceFile === 'claude-verified').length;
        const triggers = t.triggers || [];
        const enabledTriggers = triggers.filter(tr => tr.status === 'ENABLED');
        const scope = t.external
            ? '<span class="scope-badge ext">EXTERNAL</span>'
            : '<span class="scope-badge int">INTERNAL</span>';
        const trigLabel = enabledTriggers.length > 0
            ? `<span class="trig-badge">${enabledTriggers.length} trigger${enabledTriggers.length > 1 ? 's' : ''}</span>`
            : '<span style="color:var(--text-muted);font-size:10px">-</span>';

        const tt = (t.tableType || 'TABLE').toUpperCase();
        const ttColor = tt === 'VIEW' ? 'var(--orange)' : tt === 'MATERIALIZED VIEW' ? 'var(--purple,#7e22ce)' : 'var(--text-muted)';
        const ttLabel = tt === 'MATERIALIZED VIEW' ? 'MV' : tt;
        let ttBadge = `<span style="font-size:9px;font-weight:700;color:${ttColor}">${ttLabel}</span>`;
        if (tt === 'VIEW' || tt === 'MATERIALIZED VIEW') {
            ttBadge += ` <span style="font-size:9px;cursor:pointer;color:var(--accent);text-decoration:underline" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName || '')}', '${PA.escJs(t.schemaName || '')}')">def</span>`;
        }

        let html = `<tr class="to-row" onclick="PA.tf.toggleDetail('to',${idx})">`;
        html += `<td><span style="font-weight:600;color:var(--teal);cursor:pointer" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName || '')}', '${PA.escJs(t.schemaName || '')}')">${esc(t.tableName || '')}</span></td>`;
        html += `<td>${ttBadge}</td>`;
        html += `<td>${esc(t.schemaName || '-')}</td>`;
        html += `<td>${ops}</td>`;
        html += `<td>${count}${claudeCount > 0 ? ' <span class="claude-badge" title="' + claudeCount + ' found by Claude AI">AI</span>' : ''}</td>`;
        html += `<td>${trigLabel}</td>`;
        html += `<td>${scope}</td>`;
        html += `</tr>`;
        return html;
    },

    /** Render detail content for expandable row */
    _renderDetail(table, idx, esc) {
        const details = table.accessDetails || [];
        const triggers = (table.triggers || []).filter(tr => tr.status === 'ENABLED');
        const fullCount = table._fullAccessCount || details.length;
        const isScoped = PA.tableOps.scopeMode === 'scoped' && table._fullAccessDetails;
        let html = '<div class="to-detail">';

        if (details.length > 0) {
            html += '<div class="to-detail-section">';
            const scopeNote = isScoped && fullCount > details.length
                ? ` <span style="color:var(--text-muted);font-weight:400">(${fullCount} total)</span>` : '';
            html += '<div class="to-detail-section-title">Access Details (' + details.length + ')' + scopeNote + '</div>';
            for (const d of details) {
                const isClaude = d.sourceFile === 'claude-verified';
                html += `<div class="to-detail-item${isClaude ? ' claude-found' : ''}">`;
                if (isClaude) html += '<span class="claude-badge" title="Discovered by Claude AI">AI</span>';
                html += `<span class="op-badge ${d.operation || ''}">${d.operation || '?'}</span>`;
                html += `<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(d.procedureId || d.procedureName || '')}')">${esc(d.procedureName || '')}</span>`;
                if (!isClaude && d.sourceFile && d.lineNumber) {
                    html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(d.sourceFile)}', ${d.lineNumber})">L${d.lineNumber}</span>`;
                } else if (isClaude && d.lineNumber) {
                    html += `<span class="to-detail-line">L${d.lineNumber}</span>`;
                }
                if (d.whereFilters && d.whereFilters.length > 0) {
                    const wf = d.whereFilters.map(f => `${f.columnName || ''} ${f.operator || ''} ${f.value || ''}`).join(', ');
                    html += `<span class="to-where">WHERE: ${esc(wf)}</span>`;
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (triggers.length > 0) {
            html += '<div class="to-detail-section" style="border-top:1px solid var(--border);padding-top:8px">';
            html += '<div class="to-detail-section-title" style="color:var(--orange)">Enabled Triggers (' + triggers.length + ')</div>';
            for (const tr of triggers) {
                html += '<div class="to-trigger-item">';
                html += `<span class="trig-badge">${esc(tr.triggerType || '')}</span>`;
                html += `<span class="to-trigger-name" style="cursor:pointer;color:var(--accent);text-decoration:underline" onclick="event.stopPropagation(); PA.triggerModal.open('${PA.escJs(table.tableName || '')}', '${PA.escJs(tr.triggerName || '')}')">${esc(tr.triggerName || '')}</span>`;
                html += `<span class="to-trigger-event">${esc(tr.triggeringEvent || '')}</span>`;
                if (tr.calledProcedures && tr.calledProcedures.length > 0) {
                    html += '<div class="to-trigger-calls">Calls: ';
                    for (const proc of tr.calledProcedures) {
                        html += `<span class="to-detail-proc" style="margin-right:8px" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(proc)}')">${esc(proc)}</span>`;
                    }
                    html += '</div>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (details.length === 0 && triggers.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No access details or triggers available</div>';
        }

        html += '</div>';
        return html;
    },

    /** Apply scope based on PA.context — switches dataset in table framework */
    applyScope() {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tableOps.scopeMode = 'all';
            PA.tableOps._updateScopeToggle();
            if (PA.tf._state['to']) PA.tf.setData('to', PA.tableOps.data);
            PA.tableOps._updateCounts();
            return;
        }
        // Default to 'all' when proc has no direct tables to avoid empty view
        if (!ctx.scopedTables || ctx.scopedTables.length === 0) {
            PA.tableOps.scopeMode = 'all';
        } else {
            PA.tableOps.scopeMode = 'scoped';
        }
        PA.tableOps._updateScopeToggle();
        PA.tableOps._applyScopeData();
    },

    /** Switch scope mode: 'scoped' or 'all' */
    setScope(mode) {
        PA.tableOps.scopeMode = mode;
        PA.tableOps._updateScopeToggle();
        PA.tableOps._applyScopeData();
    },

    /**
     * Internal: apply scope filter to table data.
     *
     * "Direct" (scoped): tables accessed by the SELECTED proc only (not its children).
     *   - Filters tables to those in scopedTables
     *   - Filters accessDetails to only the selected proc (PA.context.procId)
     *
     * "Full Flow" (all): all tables in the call subtree.
     *   - Filters tables to those in scopedTables
     *   - Filters accessDetails to procs in callTreeNodeIds (entire subtree)
     *
     * When no proc is selected: shows all tables from the analysis.
     */
    _applyScopeData() {
        const mode = PA.tableOps.scopeMode;
        const ctx = PA.context;

        // No proc selected → show all analysis tables
        if (!ctx || !ctx.procId) {
            PA.tf.setData('to', PA.tableOps.data);
            PA.tableOps._updateCounts();
            return;
        }

        // Proc selected but has no scoped tables → show empty in Direct, use callTree for Full Flow
        if (!ctx.scopedTables || ctx.scopedTables.length === 0) {
            if (mode === 'scoped') {
                PA.tf.setData('to', []);
                PA.tableOps._updateCounts();
                return;
            }
            // Full Flow: fall through to use callTreeNodeIds
        }

        // Build set of flow-scoped table keys from the detail endpoint
        const scopedKeys = new Set();
        const scopedNames = new Set();
        for (const t of (ctx.scopedTables || [])) {
            const name = (t.tableName || '').toUpperCase();
            const schema = (t.schemaName || '').toUpperCase();
            scopedNames.add(name);
            if (schema) scopedKeys.add(schema + '.' + name);
        }

        const nodeIds = ctx.callTreeNodeIds;
        const _matchNode = (pid) => {
            if (!nodeIds || nodeIds.size === 0) return false;
            const up = pid.toUpperCase();
            if (nodeIds.has(up)) return true;
            for (const nid of nodeIds) {
                if (up.includes(nid) || nid.includes(up)) return true;
            }
            return false;
        };

        if (mode === 'scoped') {
            const currentProcId = (ctx.procId || '').toUpperCase();
            const parts = currentProcId.split('.');
            const currentLast = parts.pop();
            const currentPkg = parts.length > 0 ? parts.pop() : '';
            const direct = PA.tableOps.data.map(t => {
                const allDetails = t.accessDetails || [];
                const directDetails = allDetails.filter(d => {
                    const pid = (d.procedureId || d.procedureName || '').toUpperCase();
                    if (pid === currentProcId) return true;
                    if (currentPkg) {
                        return pid.endsWith('.' + currentPkg + '.' + currentLast);
                    }
                    return pid === currentLast;
                });
                if (directDetails.length === 0) return null;
                return Object.assign({}, t, {
                    accessDetails: directDetails,
                    accessCount: directDetails.length,
                    _fullAccessDetails: allDetails,
                    _fullAccessCount: t.accessCount || allDetails.length
                });
            }).filter(Boolean);
            PA.tf.setData('to', direct);
        } else {
            // "Full Flow": show tables accessed by any node in the call subtree
            if (nodeIds && nodeIds.size > 0) {
                const flowScoped = PA.tableOps.data.map(t => {
                    const allDetails = t.accessDetails || [];
                    const scopedDetails = allDetails.filter(d => {
                        const pid = (d.procedureId || d.procedureName || '').toUpperCase();
                        return _matchNode(pid);
                    });
                    if (scopedDetails.length === 0) return null;
                    return Object.assign({}, t, {
                        accessDetails: scopedDetails,
                        accessCount: scopedDetails.length,
                        _fullAccessDetails: allDetails,
                        _fullAccessCount: t.accessCount || allDetails.length
                    });
                }).filter(Boolean);
                PA.tf.setData('to', flowScoped);
            } else if (scopedNames.size > 0) {
                const flowTables = PA.tableOps.data.filter(t => {
                    const name = (t.tableName || '').toUpperCase();
                    const schema = (t.schemaName || '').toUpperCase();
                    if (schema && scopedKeys.size > 0) return scopedKeys.has(schema + '.' + name);
                    return scopedNames.has(name);
                });
                PA.tf.setData('to', flowTables);
            } else {
                PA.tf.setData('to', []);
            }
        }
        PA.tableOps._updateCounts();
    },

    /** Update scope toggle button states + visibility */
    _updateScopeToggle() {
        const toggle = document.getElementById('toScopeToggle');
        if (!toggle) return;
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            toggle.style.display = 'none';
            return;
        }
        toggle.style.display = '';
        const btnScoped = toggle.querySelector('[data-scope="scoped"]');
        const btnAll = toggle.querySelector('[data-scope="all"]');
        if (btnScoped) btnScoped.classList.toggle('active', PA.tableOps.scopeMode === 'scoped');
        if (btnAll) btnAll.classList.toggle('active', PA.tableOps.scopeMode === 'all');
    },

    focusTable(tableName) {
        PA.switchRightTab('tableOps');
        // Set search to find the table
        const searchInput = document.getElementById('toSearch');
        if (searchInput) {
            searchInput.value = tableName;
            PA.tf.setSearch('to', tableName);
            PA.tf.filter('to');
        }
    }
};
