/**
 * PA.joins — Joins tab: shows all JOIN relationships between tables with
 * join types, ON predicates, procedures, line numbers.
 * Uses PA.tf (shared table framework) for pagination, sorting, column filters.
 *
 * UI-028 — server-paged for the plsql live-crawl backend, unchanged for a parser-backed analysis.
 * Same conversion as `plsql/js/tableOps.js` (§51) — see its own header comment for why the split
 * exists and `PlsqlScopeFilter`'s javadoc for exactly which scope fallbacks are ported.
 */
window.PA = window.PA || {};

PA.joins = {
    data: [],
    _src: null,
    scopeMode: 'all',

    async load() {
        // BUG-FIX (§259, real-audit §258 finding #2): scope and sort are now baked into the very
        // first request `joinsPageSource` builds (mirroring `parser/js/tableOps.js`'s own
        // `_scopeParams()`-at-creation pattern), instead of creating an unscoped/unsorted source and
        // immediately re-fetching twice more to correct it — that "create default, then restart,
        // then restart again" shape was exactly the 3-call `joins?limit=50` -> `joins?sort=...` ->
        // `joins?sort=...` (identical) duplicate the audit captured.
        const ctx = PA.context;
        PA.joins.scopeMode = (ctx && ctx.procId) ? 'scoped' : 'all';
        // MID-021 (§107): both backends now go through the same paged source — joinsPageSource
        // itself dispatches by _parserName(). The old parser-backend branch fetched the whole
        // unpaged array here; checked _initTypePills (below) and confirmed it never reads
        // PA.joins.data at all (its pills are a fixed 5-item list) — so that fetch bought nothing,
        // and PA.joins.data now stays empty except as _applyScopeData's own defensive fallback for
        // the case _src is unexpectedly absent.
        PA.joins.data = [];
        const initialParams = { sort: 'USAGES_DESC' };
        if (ctx && ctx.procId) {
            if (PA.joins.scopeMode === 'scoped') initialParams.directProcId = ctx.procId;
            else if (ctx.callTreeNodeIds && ctx.callTreeNodeIds.size > 0) initialParams.nodeIds = Array.from(ctx.callTreeNodeIds);
        }
        PA.joins._src = PA.api.joinsPageSource(initialParams);
        PA.joins._initTypePills();
        PA.joins._initTable();
        PA.joins._updateScopeToggle();
    },

    applyScope() {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.joins.scopeMode = 'all';
            PA.joins._updateScopeToggle();
            if (PA.joins._src) { PA.joins._applyScopeData(); return; }
            if (PA.tf._state['join']) PA.tf.setData('join', PA.joins.data);
            return;
        }
        PA.joins.scopeMode = 'scoped';
        PA.joins._updateScopeToggle();
        PA.joins._applyScopeData();
    },

    setScope(mode) {
        PA.joins.scopeMode = mode;
        PA.joins._updateScopeToggle();
        PA.joins._applyScopeData();
    },

    _updateScopeToggle() {
        const toggle = document.getElementById('joinScopeToggle');
        if (!toggle) return;
        const ctx = PA.context;
        if (!ctx || !ctx.procId) { toggle.style.display = 'none'; return; }
        toggle.style.display = '';
        const btnS = toggle.querySelector('[data-scope="scoped"]');
        const btnA = toggle.querySelector('[data-scope="all"]');
        if (btnS) btnS.classList.toggle('active', PA.joins.scopeMode === 'scoped');
        if (btnA) btnA.classList.toggle('active', PA.joins.scopeMode === 'all');
    },

    _applyScopeData() {
        const ctx = PA.context;
        const src = PA.joins._src;

        if (src) {
            if (!ctx || !ctx.procId) {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', []);
            } else if (PA.joins.scopeMode === 'scoped') {
                src.setParam('directProcId', ctx.procId);
                src.setParam('nodeIds', []);
            } else if (ctx.callTreeNodeIds && ctx.callTreeNodeIds.size > 0) {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', Array.from(ctx.callTreeNodeIds));
            } else {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', []);
            }
            PA.tf._restart('join');
            PA.joins._updateCounts();
            return;
        }

        if (!ctx || !ctx.procId) {
            PA.tf.setData('join', PA.joins.data);
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
        const mode = PA.joins.scopeMode;
        if (mode === 'scoped') {
            const scoped = PA.joins.data.map(j => {
                const allDetails = j.accessDetails || [];
                const filtered = allDetails.filter(d => _matchDirect(d.procedureId || d.procedureName || ''));
                if (filtered.length === 0) return null;
                return Object.assign({}, j, { accessDetails: filtered, accessCount: filtered.length });
            }).filter(Boolean);
            PA.tf.setData('join', scoped);
        } else {
            if (nodeIds && nodeIds.size > 0) {
                const scoped = PA.joins.data.map(j => {
                    const allDetails = j.accessDetails || [];
                    const filtered = allDetails.filter(d => _matchNode(d.procedureId || d.procedureName || ''));
                    if (filtered.length === 0) return null;
                    return Object.assign({}, j, { accessDetails: filtered, accessCount: filtered.length });
                }).filter(Boolean);
                PA.tf.setData('join', scoped);
            } else {
                PA.tf.setData('join', PA.joins.data);
            }
        }
    },

    _initTypePills() {
        const container = document.getElementById('joinTypePills');
        if (!container) return;
        const types = ['INNER', 'LEFT', 'RIGHT', 'CROSS', 'FULL'];
        let html = '';
        for (const t of types) {
            html += `<span class="op-filter-pill active" data-jt="${t}" onclick="PA.joins.toggleType('${t}')" title="Toggle ${t} JOIN">${t}</span>`;
        }
        container.innerHTML = html;
        PA.joins._activeTypes = new Set();
    },

    _activeTypes: new Set(),

    toggleType(t) {
        const pill = document.querySelector(`#joinTypePills .op-filter-pill[data-jt="${t}"]`);
        if (!pill) return;
        if (PA.joins._activeTypes.size === 0) {
            PA.joins._activeTypes.add(t);
            document.querySelectorAll('#joinTypePills .op-filter-pill').forEach(p =>
                p.classList.toggle('active', p.dataset.jt === t));
        } else if (PA.joins._activeTypes.has(t)) {
            PA.joins._activeTypes.delete(t);
            pill.classList.remove('active');
            if (PA.joins._activeTypes.size === 0) {
                document.querySelectorAll('#joinTypePills .op-filter-pill').forEach(p => p.classList.add('active'));
            }
        } else {
            PA.joins._activeTypes.add(t);
            pill.classList.add('active');
        }
        PA.joins.applyFilters();
    },

    /** Sends the type-pill selection as `types` and reloads page 1 — see tableOps.js's own. */
    applyFilters() {
        const src = PA.joins._src;
        if (!src) { PA.tf.filter('join'); return; }
        const types = PA.joins._activeTypes;
        src.setParam('types', types.size ? [...types].join(',') : '');
        PA.tf._restart('join');
    },

    _initTable() {
        const container = document.getElementById('joinContainer');
        if (!container) return;

        let html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'join\',0)">Left Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'join\',1)">Right Table</th>';
        html += '<th>Join Types</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'join\',3)">Usages</th>';
        html += '<th>ON Predicate</th>';
        html += '</tr></thead><tbody id="join-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('join', PA.joins._src || PA.joins.data, 50, PA.joins._renderRow, {
            sortKeys: {
                0: { fn: j => (j.leftTable || '').toUpperCase() },
                1: { fn: j => (j.rightTable || '').toUpperCase() },
                3: { fn: j => j.accessCount || 0 }
            },
            serverSorts: {
                0: { asc: 'LEFT_ASC', desc: 'LEFT_DESC' },
                1: { asc: 'RIGHT_ASC', desc: 'RIGHT_DESC' },
                3: { asc: 'USAGES_ASC', desc: 'USAGES_DESC' }
            },
            renderDetail: PA.joins._renderDetail,
            searchFn: (j, q) => (j.leftTable || '').toUpperCase().includes(q) ||
                                (j.rightTable || '').toUpperCase().includes(q) ||
                                ((j.accessDetails || []).some(d => (d.onPredicate || '').toUpperCase().includes(q))),
            extraFilter: PA.joins._filter,
            onFilter: PA.joins._updateCounts
        });

        const s = PA.tf.state('join');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }
        // BUG-FIX (§259): sort is already baked into `_src` by `load()`, before `PA.tf.init` above
        // ran its own first fetch — so on the server-paged path, `filter('join')` here would only
        // re-fire an identical restart. Array-path fallback still needs it for its own sort/search.
        if (!PA.joins._src) PA.tf.filter('join');

        PA.tf.pendingColFilters('join', {
            0: { label: 'Left Table' }, 1: { label: 'Right Table' }
        });
        setTimeout(() => {
            // Text-mode (contains/regex): table names are not an enumerable set, and — unlike a
            // checkbox column — this needs no serverValues, so it works on the server-paged path
            // without a summary fetch. (Was declared checkbox with no serverValues, which
            // `initColFilters` silently refuses to attach on a server-paged table — these two icons
            // never rendered at all before this.)
            PA.tf.initColFilters('join', {
                0: { label: 'Left Table', valueFn: j => j.leftTable || '', mode: 'text' },
                1: { label: 'Right Table', valueFn: j => j.rightTable || '', mode: 'text' }
            });
            PA.tf._updateSortIndicators('join');
        }, 0);
    },

    _filter(j) {
        const types = PA.joins._activeTypes;
        if (types.size > 0) {
            const jTypes = j.joinTypes || [];
            if (!jTypes.some(jt => types.has(jt))) return false;
        }
        return true;
    },

    onSearch() {
        PA.tf.setSearch('join', document.getElementById('joinSearch')?.value || '');
        PA.tf.filter('join');
    },

    clearFilters() {
        document.getElementById('joinSearch').value = '';
        PA.joins._activeTypes.clear();
        document.querySelectorAll('#joinTypePills .op-filter-pill').forEach(p => p.classList.add('active'));
        PA.tf.setSearch('join', '');
        const s = PA.tf.state('join');
        if (s) { s.colFilters = {}; }
        PA.joins.scopeMode = 'all';
        PA.joins._updateScopeToggle();
        const src = PA.joins._src;
        if (src) { src.setParam('q', ''); src.setParam('types', ''); }
        if (!src && PA.tf._state['join']) PA.tf.setData('join', PA.joins.data);
        PA.joins.applyFilters();
        PA.tf._cfUpdateIcons('join');
    },

    _updateCounts() {
        const s = PA.tf.state('join');
        const totalEl = document.getElementById('joinTotalCount');
        if (PA.joins._src) {
            const total = (s && s.totalIsExact) ? s.serverTotal : (s ? s.filtered.length : 0);
            const suffix = PA.joins.scopeMode === 'scoped' ? ' (scoped)'
                          : (PA.context && PA.context.procId) ? ' (flow)' : '';
            totalEl.textContent = total + ' joins' + suffix;
            const fcEl = document.getElementById('joinFilteredCount');
            if (fcEl) fcEl.style.display = 'none';
            return;
        }
        const allTotal = (PA.joins.data || []).length;
        const dataTotal = s ? s.data.length : allTotal;
        const shown = s ? s.filtered.length : allTotal;
        if (PA.joins.scopeMode === 'scoped' && PA.context && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' joins (scoped)';
        } else if (PA.joins.scopeMode === 'all' && PA.context && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' joins (flow)';
        } else {
            totalEl.textContent = allTotal + ' joins';
        }
        const fc = document.getElementById('joinFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    _renderRow(j, idx, esc) {
        const typeBadges = (j.joinTypes || []).map(jt => {
            const color = jt === 'INNER' ? 'var(--blue)' : jt === 'LEFT' ? 'var(--green)' :
                          jt === 'RIGHT' ? 'var(--orange)' : jt === 'CROSS' ? 'var(--red)' :
                          jt === 'FULL' ? 'var(--purple)' : 'var(--text-muted)';
            return `<span class="op-badge" style="background:color-mix(in srgb, ${color} 15%, transparent);color:${color}">${jt}</span>`;
        }).join('');

        const firstDetail = (j.accessDetails || [])[0];
        const predPreview = firstDetail && firstDetail.onPredicate
            ? esc(firstDetail.onPredicate.substring(0, 80)) + (firstDetail.onPredicate.length > 80 ? '...' : '')
            : '-';

        let html = `<tr class="to-row" onclick="PA.tf.toggleDetail('join',${idx})">`;
        html += `<td><span style="font-weight:600;color:var(--teal)">${esc(j.leftTable || '')}</span></td>`;
        html += `<td><span style="font-weight:600;color:var(--teal)">${esc(j.rightTable || '')}</span></td>`;
        html += `<td>${typeBadges}</td>`;
        html += `<td>${j.accessCount || 0}</td>`;
        html += `<td style="font-size:10px;font-family:var(--font-mono);color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${predPreview}</td>`;
        html += '</tr>';
        return html;
    },

    _renderDetail(j, idx, esc) {
        const details = j.accessDetails || [];
        let html = '<div class="to-detail">';

        if (details.length > 0) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Join Details (' + details.length + ')</div>';
            for (const d of details) {
                const isClaude = d.sourceFile === 'claude-verified';
                const jtColor = d.joinType === 'INNER' ? 'var(--blue)' : d.joinType === 'LEFT' ? 'var(--green)' :
                                d.joinType === 'RIGHT' ? 'var(--orange)' : d.joinType === 'CROSS' ? 'var(--red)' :
                                d.joinType === 'FULL' ? 'var(--purple)' : 'var(--text-muted)';
                html += `<div class="to-detail-item${isClaude ? ' claude-found' : ''}">`;
                if (isClaude) html += '<span class="claude-badge" title="Discovered by Claude AI">AI</span>';
                html += `<span class="op-badge" style="background:color-mix(in srgb, ${jtColor} 15%, transparent);color:${jtColor}">${d.joinType || '?'} JOIN</span>`;
                html += `<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(d.procedureId || d.procedureName || '')}')">${esc(d.procedureName || '')}</span>`;
                if (!isClaude && d.lineNumber) {
                    html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(d.sourceFile || '')}', ${d.lineNumber})">L${d.lineNumber}</span>`;
                } else if (isClaude && d.lineNumber) {
                    html += `<span class="to-detail-line">L${d.lineNumber}</span>`;
                }
                if (d.onPredicate) {
                    html += `<div class="to-join-predicate">${esc(d.onPredicate.substring(0, 200))}${d.onPredicate.length > 200 ? '...' : ''}</div>`;
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (details.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No details available</div>';
        }

        html += '</div>';
        return html;
    }
};
