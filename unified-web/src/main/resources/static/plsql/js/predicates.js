/**
 * PA.predicates — Predicates tab: shows WHERE clause filter conditions
 * per table, with column names, operators, values, and procedure links.
 * Data is extracted from tableOperations accessDetails.whereFilters.
 * Uses PA.tf (shared table framework) for pagination, sorting, column filters.
 * Supports Direct / Full Flow scoping like tableOps.
 */
window.PA = window.PA || {};

PA.predicates = {
    data: [],
    scopeMode: 'all',

    async load() {
        const tables = await PA.api.getTableOperations();
        PA.predicates.data = PA.predicates._buildPredicateData(tables);
        PA.predicates.scopeMode = 'all';
        PA.predicates._initOpPills();
        PA.predicates._initTable();
        PA.predicates.applyScope();
    },

    _buildPredicateData(tables) {
        const result = [];
        for (const table of (tables || [])) {
            const predicates = [];
            const details = table.accessDetails || [];
            for (const d of details) {
                const filters = d.whereFilters || [];
                for (const f of filters) {
                    predicates.push({
                        columnName: f.columnName || '',
                        operator: f.operator || '',
                        value: f.value || '',
                        lineNumber: f.lineNumber || d.lineNumber || 0,
                        procedureId: d.procedureId || '',
                        procedureName: d.procedureName || '',
                        sourceFile: d.sourceFile || '',
                        operation: d.operation || ''
                    });
                }
            }
            if (predicates.length > 0) {
                result.push({
                    tableName: table.tableName || '',
                    schemaName: table.schemaName || '',
                    tableType: table.tableType || 'TABLE',
                    predicateCount: predicates.length,
                    uniqueColumns: [...new Set(predicates.map(p => p.columnName.toUpperCase()))].length,
                    predicates: predicates,
                    operations: [...new Set(predicates.map(p => p.operation))]
                });
            }
        }
        result.sort((a, b) => b.predicateCount - a.predicateCount);
        return result;
    },

    // ── Scope: Direct / Full Flow ──

    applyScope() {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.predicates.scopeMode = 'all';
            PA.predicates._updateScopeToggle();
            if (PA.tf._state['pred']) PA.tf.setData('pred', PA.predicates.data);
            PA.predicates._updateCounts();
            return;
        }
        PA.predicates.scopeMode = 'scoped';
        PA.predicates._updateScopeToggle();
        PA.predicates._applyScopeData();
    },

    setScope(mode) {
        PA.predicates.scopeMode = mode;
        PA.predicates._updateScopeToggle();
        PA.predicates._applyScopeData();
    },

    _updateScopeToggle() {
        const toggle = document.getElementById('predScopeToggle');
        if (!toggle) return;
        const ctx = PA.context;
        if (!ctx || !ctx.procId) { toggle.style.display = 'none'; return; }
        toggle.style.display = '';
        const btnS = toggle.querySelector('[data-scope="scoped"]');
        const btnA = toggle.querySelector('[data-scope="all"]');
        if (btnS) btnS.classList.toggle('active', PA.predicates.scopeMode === 'scoped');
        if (btnA) btnA.classList.toggle('active', PA.predicates.scopeMode === 'all');
    },

    _applyScopeData() {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tf.setData('pred', PA.predicates.data);
            PA.predicates._updateCounts();
            return;
        }
        const nodeIds = ctx.callTreeNodeIds;
        const currentProcId = (ctx.procId || '').toUpperCase();
        const parts = currentProcId.split('.');
        const currentLast = parts.pop();
        const currentPkg = parts.length > 0 ? parts.pop() : '';

        const _matchDirect = (pid) => {
            const up = (pid || '').toUpperCase();
            if (up === currentProcId) return true;
            if (currentPkg) return up.endsWith('.' + currentPkg + '.' + currentLast);
            return up === currentLast;
        };
        const _matchNode = (pid) => {
            const up = (pid || '').toUpperCase();
            if (nodeIds && nodeIds.has(up)) return true;
            if (nodeIds) for (const nid of nodeIds) { if (up.includes(nid) || nid.includes(up)) return true; }
            return false;
        };

        const matchFn = PA.predicates.scopeMode === 'scoped' ? _matchDirect : _matchNode;
        const scoped = PA.predicates.data.map(t => {
            const allPreds = t.predicates || [];
            const filtered = allPreds.filter(p => matchFn(p.procedureId || p.procedureName || ''));
            if (filtered.length === 0) return null;
            return Object.assign({}, t, {
                predicates: filtered,
                predicateCount: filtered.length,
                uniqueColumns: [...new Set(filtered.map(p => p.columnName.toUpperCase()))].length,
                operations: [...new Set(filtered.map(p => p.operation))]
            });
        }).filter(Boolean);
        scoped.sort((a, b) => b.predicateCount - a.predicateCount);
        PA.tf.setData('pred', scoped);
        PA.predicates._updateCounts();
    },

    // ── Operation pills ──

    _initOpPills() {
        const container = document.getElementById('predOpPills');
        if (!container) return;
        const ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        let html = '';
        for (const op of ops) {
            html += `<span class="op-filter-pill active" data-pop="${op}" onclick="PA.predicates.toggleOp('${op}')" title="Toggle ${op}">${op}</span>`;
        }
        container.innerHTML = html;
        PA.predicates._activeOps = new Set();
    },

    _activeOps: new Set(),

    toggleOp(op) {
        const pill = document.querySelector(`#predOpPills .op-filter-pill[data-pop="${op}"]`);
        if (!pill) return;
        if (PA.predicates._activeOps.size === 0) {
            PA.predicates._activeOps.add(op);
            document.querySelectorAll('#predOpPills .op-filter-pill').forEach(p =>
                p.classList.toggle('active', p.dataset.pop === op));
        } else if (PA.predicates._activeOps.has(op)) {
            PA.predicates._activeOps.delete(op);
            pill.classList.remove('active');
            if (PA.predicates._activeOps.size === 0) {
                document.querySelectorAll('#predOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
            }
        } else {
            PA.predicates._activeOps.add(op);
            pill.classList.add('active');
        }
        PA.tf.filter('pred');
    },

    // ── Table framework ──

    _initTable() {
        const container = document.getElementById('predContainer');
        if (!container) return;

        let html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'pred\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'pred\',1)">Schema</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'pred\',2)">Columns</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'pred\',3)">Predicates</th>';
        html += '<th>Operations</th>';
        html += '</tr></thead><tbody id="pred-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('pred', PA.predicates.data, 50, PA.predicates._renderRow, {
            sortKeys: {
                0: { fn: p => (p.tableName || '').toUpperCase() },
                1: { fn: p => (p.schemaName || '').toUpperCase() },
                2: { fn: p => p.uniqueColumns || 0 },
                3: { fn: p => p.predicateCount || 0 }
            },
            renderDetail: PA.predicates._renderDetail,
            searchFn: (p, q) => (p.tableName || '').toUpperCase().includes(q) ||
                                (p.schemaName || '').toUpperCase().includes(q) ||
                                (p.predicates || []).some(pr => (pr.columnName || '').toUpperCase().includes(q) ||
                                    (pr.value || '').toUpperCase().includes(q)),
            extraFilter: PA.predicates._filter,
            onFilter: PA.predicates._updateCounts
        });

        const s = PA.tf.state('pred');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }
        PA.tf.filter('pred');

        setTimeout(() => {
            PA.tf.initColFilters('pred', {
                1: { label: 'Schema', valueFn: p => p.schemaName || '' }
            });
            PA.tf._updateSortIndicators('pred');
        }, 0);
    },

    _filter(p) {
        const ops = PA.predicates._activeOps;
        if (ops.size > 0) {
            if (!p.operations.some(op => ops.has(op))) return false;
        }
        const opFilter = document.getElementById('predOpFilter')?.value || '';
        if (opFilter && !(p.operations || []).some(op => op === opFilter)) return false;
        return true;
    },

    onSearch() {
        PA.tf.setSearch('pred', document.getElementById('predSearch')?.value || '');
        PA.tf.filter('pred');
    },

    applyFilters() {
        PA.tf.filter('pred');
    },

    clearFilters() {
        const el = document.getElementById('predSearch');
        if (el) el.value = '';
        const opDd = document.getElementById('predOpFilter');
        if (opDd) opDd.value = '';
        PA.predicates._activeOps.clear();
        document.querySelectorAll('#predOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
        PA.tf.setSearch('pred', '');
        const s = PA.tf.state('pred');
        if (s) { s.colFilters = {}; }
        PA.predicates.scopeMode = 'all';
        PA.predicates._updateScopeToggle();
        if (PA.tf._state['pred']) PA.tf.setData('pred', PA.predicates.data);
        PA.tf.filter('pred');
        PA.tf._cfUpdateIcons('pred');
    },

    _updateCounts() {
        const s = PA.tf.state('pred');
        const allTotal = (PA.predicates.data || []).length;
        const dataTotal = s ? s.data.length : allTotal;
        const shown = s ? s.filtered.length : allTotal;
        const totalEl = document.getElementById('predTotalCount');
        if (PA.predicates.scopeMode === 'scoped' && PA.context && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' tables (scoped)';
        } else if (PA.predicates.scopeMode === 'all' && PA.context && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' tables (flow)';
        } else {
            totalEl.textContent = allTotal + ' tables';
        }
        const fc = document.getElementById('predFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    _renderRow(p, idx, esc) {
        const opBadges = (p.operations || []).map(op => {
            return `<span class="op-badge ${op}">${op}</span>`;
        }).join('');

        let html = `<tr class="to-row" onclick="PA.tf.toggleDetail('pred',${idx})">`;
        html += `<td><span style="font-weight:600;color:var(--teal)">${esc(p.tableName || '')}</span></td>`;
        html += `<td>${esc(p.schemaName || '')}</td>`;
        html += `<td>${p.uniqueColumns || 0}</td>`;
        html += `<td>${p.predicateCount || 0}</td>`;
        html += `<td>${opBadges}</td>`;
        html += '</tr>';
        return html;
    },

    _renderDetail(p, idx, esc) {
        const preds = p.predicates || [];
        let html = '<div class="to-detail">';

        // Group predicates by column
        const byCol = {};
        for (const pr of preds) {
            const col = pr.columnName || '(unknown)';
            if (!byCol[col]) byCol[col] = [];
            byCol[col].push(pr);
        }

        for (const [col, items] of Object.entries(byCol)) {
            html += '<div class="to-detail-section">';
            html += `<div class="to-detail-section-title" style="color:var(--teal)">${esc(col)} <span style="color:var(--text-muted);font-weight:400">(${items.length} uses)</span></div>`;
            for (const pr of items) {
                const isClaude = pr.sourceFile === 'claude-verified';
                const opColor = pr.operation === 'SELECT' ? 'var(--green)' :
                                pr.operation === 'INSERT' ? 'var(--blue,#3b82f6)' :
                                pr.operation === 'UPDATE' ? 'var(--orange)' :
                                pr.operation === 'DELETE' ? 'var(--red)' :
                                pr.operation === 'MERGE' ? 'var(--purple)' : 'var(--text-muted)';
                html += `<div class="to-detail-item${isClaude ? ' claude-found' : ''}">`;
                if (isClaude) html += '<span class="claude-badge" title="Discovered by Claude AI">AI</span>';
                html += `<span class="op-badge" style="background:color-mix(in srgb, ${opColor} 15%, transparent);color:${opColor};font-size:9px">${pr.operation || '?'}</span>`;
                html += `<span style="font-family:var(--font-mono);font-size:11px;color:var(--text)">${esc(pr.operator || '')} ${esc(pr.value || '')}</span>`;
                html += `<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(pr.procedureId || pr.procedureName || '')}')">${esc(pr.procedureName || '')}</span>`;
                if (!isClaude && pr.lineNumber) {
                    html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(pr.sourceFile || '')}', ${pr.lineNumber})">L${pr.lineNumber}</span>`;
                } else if (isClaude && pr.lineNumber) {
                    html += `<span class="to-detail-line">L${pr.lineNumber}</span>`;
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (preds.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No predicate details available</div>';
        }

        html += '</div>';
        return html;
    }
};
