/**
 * PA.cursors — Cursors tab: shows all cursor declarations and usage with
 * definitions, predicates, usage count, type badges, expandable detail rows.
 * Uses PA.tf (shared table framework) for pagination, sorting, column filters.
 *
 * UI-028 — server-paged for the plsql live-crawl backend, unchanged for a parser-backed analysis.
 * Same conversion as `plsql/js/tableOps.js` (§51). Two independently-filterable fields here, not
 * one: `_activeTypes` (the cursorType pills — EXPLICIT/REF_CURSOR/FOR_LOOP/OPEN_FOR, multi-select)
 * maps to `cf=0:...`; `curOpFilter` (the single-select DECLARE/OPEN/FETCH/CLOSE/FOR_LOOP dropdown,
 * a row's `operations[]`) maps to `types`, matching `PlsqlFacetSpecs.CURSORS`'s own `typeValues`.
 */
window.PA = window.PA || {};

PA.cursors = {
    data: [],
    _src: null,
    scopeMode: 'all',

    async load() {
        // BUG-FIX (§259, real-audit §258 finding #2): scope and sort baked into the first request
        // — see joins.js's `load()` for the full reasoning (same "create default, restart, restart
        // again" 3-call duplicate this mirrors and fixes).
        const ctx = PA.context;
        PA.cursors.scopeMode = (ctx && ctx.procId) ? 'scoped' : 'all';
        // MID-021 (§107): both backends now go through the same paged source —
        // cursorsPageSource itself dispatches by _parserName(). The old parser-backend branch
        // fetched the whole unpaged array here; _initTypePills (below) never reads PA.cursors.data
        // at all (its pills are a fixed 4-item list), so that fetch bought nothing — see joins.js's
        // identical fix for the same reasoning.
        PA.cursors.data = [];
        const initialParams = { sort: 'USAGES_DESC' };
        if (ctx && ctx.procId) {
            if (PA.cursors.scopeMode === 'scoped') initialParams.directProcId = ctx.procId;
            else if (ctx.callTreeNodeIds && ctx.callTreeNodeIds.size > 0) initialParams.nodeIds = Array.from(ctx.callTreeNodeIds);
        }
        PA.cursors._src = PA.api.cursorsPageSource(initialParams);
        PA.cursors._initTypePills();
        PA.cursors._initTable();
        PA.cursors._updateScopeToggle();
    },

    applyScope() {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.cursors.scopeMode = 'all';
            PA.cursors._updateScopeToggle();
            if (PA.cursors._src) { PA.cursors._applyScopeData(); return; }
            if (PA.tf._state['cur']) PA.tf.setData('cur', PA.cursors.data);
            return;
        }
        PA.cursors.scopeMode = 'scoped';
        PA.cursors._updateScopeToggle();
        PA.cursors._applyScopeData();
    },

    setScope(mode) {
        PA.cursors.scopeMode = mode;
        PA.cursors._updateScopeToggle();
        PA.cursors._applyScopeData();
    },

    _updateScopeToggle() {
        const toggle = document.getElementById('curScopeToggle');
        if (!toggle) return;
        const ctx = PA.context;
        if (!ctx || !ctx.procId) { toggle.style.display = 'none'; return; }
        toggle.style.display = '';
        const btnS = toggle.querySelector('[data-scope="scoped"]');
        const btnA = toggle.querySelector('[data-scope="all"]');
        if (btnS) btnS.classList.toggle('active', PA.cursors.scopeMode === 'scoped');
        if (btnA) btnA.classList.toggle('active', PA.cursors.scopeMode === 'all');
    },

    _applyScopeData() {
        const ctx = PA.context;
        const src = PA.cursors._src;

        if (src) {
            if (!ctx || !ctx.procId) {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', []);
            } else if (PA.cursors.scopeMode === 'scoped') {
                src.setParam('directProcId', ctx.procId);
                src.setParam('nodeIds', []);
            } else if (ctx.callTreeNodeIds && ctx.callTreeNodeIds.size > 0) {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', Array.from(ctx.callTreeNodeIds));
            } else {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', []);
            }
            PA.tf._restart('cur');
            PA.cursors._updateCounts();
            return;
        }

        if (!ctx || !ctx.procId) {
            PA.tf.setData('cur', PA.cursors.data);
            return;
        }
        const nodeIds = ctx.callTreeNodeIds;
        const currentProcId = (ctx.procId || '').toUpperCase();
        const _matchNode = (pid) => {
            const up = (pid || '').toUpperCase();
            if (nodeIds && nodeIds.has(up)) return true;
            if (nodeIds) for (const nid of nodeIds) { if (up.includes(nid) || nid.includes(up)) return true; }
            return false;
        };
        const parts = currentProcId.split('.');
        const currentLast = parts.pop();
        const currentPkg = parts.length > 0 ? parts.pop() : '';
        const _matchDirect = (pid) => {
            const up = (pid || '').toUpperCase();
            if (up === currentProcId) return true;
            if (currentPkg) return up.endsWith('.' + currentPkg + '.' + currentLast);
            return up === currentLast;
        };
        const mode = PA.cursors.scopeMode;
        if (mode === 'scoped') {
            const scoped = PA.cursors.data.map(c => {
                const allDetails = c.accessDetails || [];
                const filtered = allDetails.filter(d => _matchDirect(d.procedureId || d.procedureName || ''));
                if (filtered.length === 0) return null;
                return Object.assign({}, c, { accessDetails: filtered, accessCount: filtered.length });
            }).filter(Boolean);
            PA.tf.setData('cur', scoped);
        } else {
            if (nodeIds && nodeIds.size > 0) {
                const scoped = PA.cursors.data.map(c => {
                    const allDetails = c.accessDetails || [];
                    const filtered = allDetails.filter(d => _matchNode(d.procedureId || d.procedureName || ''));
                    if (filtered.length === 0) return null;
                    return Object.assign({}, c, { accessDetails: filtered, accessCount: filtered.length });
                }).filter(Boolean);
                PA.tf.setData('cur', scoped);
            } else {
                PA.tf.setData('cur', PA.cursors.data);
            }
        }
    },

    _initTypePills() {
        const container = document.getElementById('curTypePills');
        if (!container) return;
        const types = ['EXPLICIT', 'REF_CURSOR', 'FOR_LOOP', 'OPEN_FOR'];
        let html = '';
        for (const t of types) {
            const label = t === 'REF_CURSOR' ? 'REF' : t === 'FOR_LOOP' ? 'FOR' : t === 'OPEN_FOR' ? 'OPEN FOR' : t;
            html += `<span class="op-filter-pill active" data-ct="${t}" onclick="PA.cursors.toggleType('${t}')" title="Toggle ${t}">${label}</span>`;
        }
        container.innerHTML = html;
        PA.cursors._activeTypes = new Set();
    },

    _activeTypes: new Set(),

    toggleType(t) {
        const pill = document.querySelector(`#curTypePills .op-filter-pill[data-ct="${t}"]`);
        if (!pill) return;
        if (PA.cursors._activeTypes.size === 0) {
            PA.cursors._activeTypes.add(t);
            document.querySelectorAll('#curTypePills .op-filter-pill').forEach(p =>
                p.classList.toggle('active', p.dataset.ct === t));
        } else if (PA.cursors._activeTypes.has(t)) {
            PA.cursors._activeTypes.delete(t);
            pill.classList.remove('active');
            if (PA.cursors._activeTypes.size === 0) {
                document.querySelectorAll('#curTypePills .op-filter-pill').forEach(p => p.classList.add('active'));
            }
        } else {
            PA.cursors._activeTypes.add(t);
            pill.classList.add('active');
        }
        PA.cursors.applyFilters();
    },

    _initTable() {
        const container = document.getElementById('curContainer');
        if (!container) return;

        let html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'cur\',0)">Cursor</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'cur\',1)">Type</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'cur\',3)">Usages</th>';
        html += '<th>Definition</th>';
        html += '</tr></thead><tbody id="cur-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('cur', PA.cursors._src || PA.cursors.data, 50, PA.cursors._renderRow, {
            sortKeys: {
                0: { fn: c => (c.cursorName || '').toUpperCase() },
                1: { fn: c => (c.cursorType || '').toUpperCase() },
                3: { fn: c => c.accessCount || 0 }
            },
            serverSorts: {
                0: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                1: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                3: { asc: 'USAGES_ASC', desc: 'USAGES_DESC' }
            },
            renderDetail: PA.cursors._renderDetail,
            searchFn: (c, q) => (c.cursorName || '').toUpperCase().includes(q) ||
                                (c.queryText || '').toUpperCase().includes(q),
            extraFilter: PA.cursors._filter,
            onFilter: PA.cursors._updateCounts
        });

        const s = PA.tf.state('cur');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }
        // BUG-FIX (§259): sort is already baked into `_src` by `load()`, before `PA.tf.init` above
        // ran its own first fetch — see joins.js's `_initTable` for the full reasoning.
        if (!PA.cursors._src) PA.tf.filter('cur');

        setTimeout(() => {
            PA.tf.initColFilters('cur', {
                1: { label: 'Type', valueFn: c => c.cursorType || 'EXPLICIT' }
            });
            PA.tf._updateSortIndicators('cur');
        }, 0);
    },

    _filter(c) {
        const types = PA.cursors._activeTypes;
        if (types.size > 0 && !types.has(c.cursorType || 'EXPLICIT')) return false;
        const opFilter = document.getElementById('curOpFilter')?.value || '';
        if (opFilter && !(c.operations || []).some(op => op === opFilter)) return false;
        return true;
    },

    onSearch() {
        PA.tf.setSearch('cur', document.getElementById('curSearch')?.value || '');
        PA.tf.filter('cur');
    },

    /**
     * Sends the cursorType pills as `cf=0:...` and the operation dropdown as `types` — see the
     * file header for why these two map to different parameters. Falls through to the unchanged
     * client-side filter on the array (parser-backed) path.
     */
    applyFilters() {
        const src = PA.cursors._src;
        if (!src) { PA.tf.filter('cur'); return; }
        const types = PA.cursors._activeTypes;
        src.setParam('cf', types.size ? ['0:' + [...types].join('|')] : []);
        const opFilter = document.getElementById('curOpFilter')?.value || '';
        src.setParam('types', opFilter || '');
        PA.tf._restart('cur');
    },

    clearFilters() {
        document.getElementById('curSearch').value = '';
        document.getElementById('curOpFilter').value = '';
        PA.cursors._activeTypes.clear();
        document.querySelectorAll('#curTypePills .op-filter-pill').forEach(p => p.classList.add('active'));
        PA.tf.setSearch('cur', '');
        const s = PA.tf.state('cur');
        if (s) { s.colFilters = {}; }
        PA.cursors.scopeMode = 'all';
        PA.cursors._updateScopeToggle();
        const src = PA.cursors._src;
        if (src) { src.setParam('q', ''); }
        if (!src && PA.tf._state['cur']) PA.tf.setData('cur', PA.cursors.data);
        PA.cursors.applyFilters();
        PA.tf._cfUpdateIcons('cur');
    },

    _updateCounts() {
        const s = PA.tf.state('cur');
        const totalEl = document.getElementById('curTotalCount');
        if (PA.cursors._src) {
            const total = (s && s.totalIsExact) ? s.serverTotal : (s ? s.filtered.length : 0);
            const suffix = PA.cursors.scopeMode === 'scoped' ? ' (scoped)'
                          : (PA.context && PA.context.procId) ? ' (flow)' : '';
            totalEl.textContent = total + ' cursors' + suffix;
            const fcEl = document.getElementById('curFilteredCount');
            if (fcEl) fcEl.style.display = 'none';
            return;
        }
        const allTotal = (PA.cursors.data || []).length;
        const dataTotal = s ? s.data.length : allTotal;
        const shown = s ? s.filtered.length : allTotal;
        if (PA.cursors.scopeMode === 'scoped' && PA.context && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' cursors (scoped)';
        } else if (PA.cursors.scopeMode === 'all' && PA.context && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' cursors (flow)';
        } else {
            totalEl.textContent = allTotal + ' cursors';
        }
        const fc = document.getElementById('curFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    _renderRow(c, idx, esc) {
        const ops = (c.operations || []).map(op => {
            const color = op === 'DECLARE' ? 'var(--purple)' : op === 'OPEN' ? 'var(--green)' :
                          op === 'FETCH' || op === 'FETCH_BULK' ? 'var(--blue)' : op === 'CLOSE' ? 'var(--red)' :
                          op === 'FOR_LOOP' ? 'var(--teal)' : 'var(--text-muted)';
            return `<span class="op-badge" style="background:color-mix(in srgb, ${color} 15%, transparent);color:${color}">${op}</span>`;
        }).join('');

        const typeColor = c.cursorType === 'REF_CURSOR' ? 'var(--orange)' :
                          c.cursorType === 'FOR_LOOP' ? 'var(--teal)' :
                          c.cursorType === 'OPEN_FOR' ? 'var(--blue)' : 'var(--purple)';
        const typeLabel = c.cursorType === 'REF_CURSOR' ? 'REF' :
                          c.cursorType === 'FOR_LOOP' ? 'FOR' :
                          c.cursorType === 'OPEN_FOR' ? 'OPEN FOR' : 'EXPLICIT';

        const defPreview = c.queryText ? esc(c.queryText.substring(0, 60)) + (c.queryText.length > 60 ? '...' : '') : '-';

        let html = `<tr class="to-row" onclick="PA.tf.toggleDetail('cur',${idx})">`;
        html += `<td><span style="font-weight:600;color:var(--purple)">${esc(c.cursorName || '')}</span></td>`;
        html += `<td><span style="font-size:9px;font-weight:700;color:${typeColor}">${typeLabel}</span></td>`;
        html += `<td>${ops}</td>`;
        html += `<td>${c.accessCount || 0}</td>`;
        html += `<td style="font-size:10px;font-family:var(--font-mono);color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${defPreview}</td>`;
        html += '</tr>';
        return html;
    },

    _renderDetail(c, idx, esc) {
        const details = c.accessDetails || [];
        let html = '<div class="to-detail">';

        if (c.queryText) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title" style="color:var(--purple)">Cursor Definition</div>';
            html += `<pre style="font-size:11px;line-height:1.4;background:var(--bg);padding:8px;border-radius:var(--radius-sm);max-height:200px;overflow:auto;margin:4px 0;white-space:pre-wrap">${esc(c.queryText)}</pre>`;
            html += '</div>';
        }

        if (details.length > 0) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Usage Details (' + details.length + ')</div>';
            for (const d of details) {
                const isClaude = d.sourceFile === 'claude-verified';
                const opColor = d.operation === 'DECLARE' ? 'var(--purple)' :
                                d.operation === 'OPEN' ? 'var(--green)' :
                                d.operation === 'FETCH' || d.operation === 'FETCH_BULK' ? 'var(--blue)' :
                                d.operation === 'CLOSE' ? 'var(--red)' :
                                d.operation === 'FOR_LOOP' ? 'var(--teal)' : 'var(--text-muted)';
                html += `<div class="to-detail-item${isClaude ? ' claude-found' : ''}">`;
                if (isClaude) html += '<span class="claude-badge" title="Discovered by Claude AI">AI</span>';
                html += `<span class="op-badge" style="background:color-mix(in srgb, ${opColor} 15%, transparent);color:${opColor}">${d.operation || '?'}</span>`;
                html += `<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(d.procedureId || d.procedureName || '')}')">${esc(d.procedureName || '')}</span>`;
                if (!isClaude && d.lineNumber) {
                    html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(d.sourceFile || '')}', ${d.lineNumber})">L${d.lineNumber}</span>`;
                } else if (isClaude && d.lineNumber) {
                    html += `<span class="to-detail-line">L${d.lineNumber}</span>`;
                }
                if (d.queryText && d.operation !== 'DECLARE') {
                    html += `<div class="to-join-predicate">${esc(d.queryText.substring(0, 120))}${d.queryText.length > 120 ? '...' : ''}</div>`;
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (!c.queryText && details.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No details available</div>';
        }

        html += '</div>';
        return html;
    }
};
