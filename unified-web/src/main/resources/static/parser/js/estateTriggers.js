window.PA = window.PA || {};

/*
 * Triggers — the estate facet's second screen.
 *
 * Server-paged over `/triggers` (search `q`, event pills `types`). Expanding a row lazily fetches
 * `/triggers/{name}`, which is the only source of the chain (trigger -> touched table -> other
 * triggers) and the fired-by list (the procedures whose DML on the trigger's table can fire it).
 *
 * The trigger body and WHEN clause are DB-sourced DATA: rendered escaped inside <pre>, never as
 * markup.
 */
PA.estateTriggers = {
    data: [],
    _src: null,
    _details: {},
    _activeEvents: new Set(),

    async load() {
        PA.estateTriggers._details = {};
        PA.estateTriggers._activeEvents = new Set();
        PA.estateTriggers._src = PA.api.triggersPageSource({});
        PA.estateTriggers._src.setParam('sort', 'NAME_ASC');
        PA.estateTriggers._initEventPills();
        PA.estateTriggers._initTable();
    },

    reset() {
        PA.estateTriggers._details = {};
        PA.estateTriggers.data = [];
    },

    /**
     * Cross-estate link target: switch to the Estate → Triggers screen, filter to this trigger and
     * open its detail row. Used by view base-table trigger links and trigger-chain nodes.
     */
    showTrigger: function(name) {
        if (!name) return;
        if (PA.switchRightTab) PA.switchRightTab('estate');
        if (PA.estateTab && PA.estateTab.switchSub) PA.estateTab.switchSub('triggers');
        var el = document.getElementById('tgSearch');
        if (el) el.value = name;
        var src = PA.estateTriggers._src;
        if (src && PA.tf) {
            src.setParam('q', name);
            PA.tf._restart('tg');
        }
        PA.estateTriggers._focus('tg', name, 'name');
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

    _initEventPills: function() {
        var container = document.getElementById('tgEventPills');
        if (!container) return;
        var events = ['INSERT', 'UPDATE', 'DELETE'];
        var html = '';
        for (var i = 0; i < events.length; i++) {
            html += '<span class="op-filter-pill active" data-tge="' + events[i] + '" '
                + 'onclick="PA.estateTriggers.toggleEvent(\'' + events[i] + '\')">' + events[i] + '</span>';
        }
        container.innerHTML = html;
    },

    toggleEvent: function(t) {
        var pill = document.querySelector('#tgEventPills .op-filter-pill[data-tge="' + t + '"]');
        if (!pill) return;
        if (PA.estateTriggers._activeEvents.size === 0) {
            PA.estateTriggers._activeEvents.add(t);
            document.querySelectorAll('#tgEventPills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.tge === t);
            });
        } else if (PA.estateTriggers._activeEvents.has(t)) {
            PA.estateTriggers._activeEvents.delete(t);
            pill.classList.remove('active');
            if (PA.estateTriggers._activeEvents.size === 0) {
                document.querySelectorAll('#tgEventPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.estateTriggers._activeEvents.add(t);
            pill.classList.add('active');
        }
        var src = PA.estateTriggers._src;
        if (src) {
            var ev = PA.estateTriggers._activeEvents;
            src.setParam('types', ev.size ? [...ev].join(',') : '');
            PA.tf._restart('tg');
        }
    },

    _initTable: function() {
        var container = document.getElementById('tgContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'tg\',0)">Trigger</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'tg\',1)">Table</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'tg\',2)">Timing</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'tg\',3)">Event</th>';
        html += '<th>Level</th>';
        html += '<th>Touches</th>';
        html += '</tr></thead><tbody id="tg-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('tg', PA.estateTriggers._src, 50, PA.estateTriggers._renderRow, {
            serverSorts: {
                0: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                1: { asc: 'TABLE_ASC', desc: 'TABLE_DESC' },
                2: { asc: 'TIMING_ASC', desc: 'TIMING_DESC' },
                3: { asc: 'EVENT_ASC', desc: 'EVENT_DESC' }
            },
            renderDetail: PA.estateTriggers._renderDetail,
            onFilter: function() {
                PA.estateTriggers._updateCounts();
                PA.estateTriggers._maybeEmpty();
            }
        });
        var st = PA.tf.state('tg');
        if (st) { st.sortCol = 0; st.sortDir = 'asc'; }
        PA.tf._updateSortIndicators('tg');
    },

    onSearch: function() {
        var el = document.getElementById('tgSearch');
        PA.estateTriggers._src.setParam('q', el ? el.value : '');
        PA.tf._restart('tg');
    },

    clearFilters: function() {
        var el = document.getElementById('tgSearch');
        if (el) el.value = '';
        PA.estateTriggers._activeEvents.clear();
        document.querySelectorAll('#tgEventPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        var src = PA.estateTriggers._src;
        if (src) { src.setParam('q', ''); src.setParam('types', ''); }
        PA.tf._restart('tg');
    },

    _isFiltered: function() {
        var src = PA.estateTriggers._src;
        return !!(src && src.param('q')) || PA.estateTriggers._activeEvents.size > 0;
    },

    _updateCounts: function() {
        var st = PA.tf.state('tg');
        var totalEl = document.getElementById('tgTotalCount');
        if (!totalEl || !st) return;
        var total = st.totalIsExact ? st.serverTotal : -1;
        totalEl.textContent = (total >= 0 ? total : (st.filtered ? st.filtered.length : 0))
            + (PA.estateTriggers._isFiltered() ? ' matched' : ' triggers');
    },

    _maybeEmpty: function() {
        var st = PA.tf.state('tg');
        var tb = document.getElementById('tg-tbody');
        if (!st || !tb) return;
        if (st.loading || st.loadError) return;
        if (st.totalIsExact && st.serverTotal === 0) {
            tb.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:24px;text-align:center">'
                + (PA.estateTriggers._isFiltered()
                    ? 'No triggers match the current filters.'
                    : 'No triggers in this analysis.')
                + '</td></tr>';
        }
    },

    _renderRow: function(t, idx, esc) {
        var timingBadge = t.timing
            ? '<span class="est-badge ' + (t.timing === 'BEFORE' ? 'warn' : 'view') + '">' + esc(t.timing) + '</span>'
            : '<span style="color:var(--text-muted)">-</span>';
        var eventBadge = t.event
            ? '<span class="est-op-badge ' + String(t.event).toLowerCase().split(' ')[0] + '">' + esc(t.event) + '</span>'
            : '<span style="color:var(--text-muted)">-</span>';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'tg\',' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--badge-orange)">' + esc(t.name || '') + '</span>'
            + (t.insteadOf ? '<span class="est-badge warn" style="margin-left:4px">INSTEAD OF</span>' : '')
            + (t.compound ? '<span class="est-badge warn" style="margin-left:4px">COMPOUND</span>' : '')
            + '</td>';
        html += '<td><span class="est-chip-sub" style="font-family:var(--font-mono)">' + esc(t.table || '-') + '</span></td>';
        html += '<td>' + timingBadge + '</td>';
        html += '<td>' + eventBadge + '</td>';
        html += '<td>' + (t.level ? esc(t.level) : '<span style="color:var(--text-muted)">-</span>') + '</td>';
        html += '<td>' + (t.touchesCount || 0) + '</td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(t, idx, esc) {
        var cached = PA.estateTriggers._details[t.name];
        if (cached === undefined) {
            PA.estateTriggers._fetchDetail(t, idx);
            return '<div class="est-detail"><div class="est-loading">Loading trigger detail…</div></div>';
        }
        if (!cached) return '<div class="est-detail">' + PA.estateTriggers._notAnalyzed(t.name) + '</div>';
        return '<div class="est-detail">' + PA.estateTriggers._detailHtml(cached, esc) + '</div>';
    },

    _fetchDetail: function(t, idx) {
        var rowEl = document.getElementById('tg-detail-' + idx);
        var fill = function(detail) {
            PA.estateTriggers._details[t.name] = detail;
            if (rowEl && rowEl.isConnected) {
                rowEl.innerHTML = '<td colspan="20">' + PA.estateTriggers._renderDetail(t, idx, PA.esc) + '</td>';
            }
        };
        PA.api.getTriggerDetail(t.name).then(function(d) {
            fill((d && !(PA.failed && PA.failed(d))) ? d : null);
        }).catch(function() {
            fill(null);
        });
    },

    _notAnalyzed: function(name) {
        return '<div class="est-notanalyzed"><b>Not analyzed — ' + PA.esc(name) + '</b>'
            + 'This trigger is listed, but its detail artifact is not present in this analysis. '
            + 'Re-run the analysis to populate its WHEN clause, body and chain.</div>';
    },

    _detailHtml: function(t, esc) {
        var html = '';

        html += '<div class="est-meta">';
        if (t.timing) html += '<span class="est-badge ' + (t.timing === 'BEFORE' ? 'warn' : 'view') + '">' + esc(t.timing) + '</span>';
        if (t.event) html += '<span class="est-op-badge ' + String(t.event).toLowerCase().split(' ')[0] + '">' + esc(t.event) + '</span>';
        if (t.level) html += '<span class="est-badge">' + esc(t.level) + '</span>';
        if (t.triggerType) html += '<span class="est-meta-item">' + esc(t.triggerType) + '</span>';
        if (t.status) html += '<span class="est-badge ' + (String(t.status).toUpperCase() === 'ENABLED' ? 'ok' : 'warn') + '">' + esc(t.status) + '</span>';
        if (t.insteadOf) html += '<span class="est-badge warn">INSTEAD OF</span>';
        if (t.compound) html += '<span class="est-badge warn">COMPOUND</span>';
        if (t.dbDiscovered) html += '<span class="est-badge">CATALOG ONLY</span>';
        if (t.source) html += '<span class="est-badge">' + esc(t.source) + '</span>';
        if (t.table) {
            html += '<span class="est-meta-item">on <b style="color:var(--teal);cursor:pointer" '
                + 'onclick="event.stopPropagation();PA.tableDetail.open(\'' + PA.escJs(t.table) + '\',\'' + PA.escJs(t.tableSchema || '') + '\')">'
                + esc((t.tableSchema ? t.tableSchema + '.' : '') + t.table) + '</b></span>';
        }
        html += '</div>';

        // The event list (a trigger can fire on INSERT OR UPDATE, or INSERT,UPDATE,DELETE).
        if (t.eventList && t.eventList.length > 1) {
            html += '<div class="est-meta">';
            for (var ei = 0; ei < t.eventList.length; ei++) {
                html += '<span class="est-op-badge ' + String(t.eventList[ei]).toLowerCase() + '">' + esc(t.eventList[ei]) + '</span>';
            }
            html += '</div>';
        }

        if (t.whenClause) {
            html += '<div class="est-section"><div class="est-section-title">WHEN Clause</div>';
            html += '<pre class="est-when">' + esc(t.whenClause) + '</pre></div>';
        }

        html += '<div class="est-section"><div class="est-section-title">Body</div>';
        if (t.body) {
            html += '<pre class="est-sql est-body">' + esc(t.body) + '</pre>';
        } else {
            html += '<div class="est-empty">Body not captured — the trigger definition was not readable from the catalog'
                + (t.dbDiscovered ? ' (this trigger was discovered from the catalog only; a body is not available on that path).'
                    : (t.source === 'DATABASE' ? ' (check the connection user\'s privileges).' : '.')) + '</div>';
        }
        html += '</div>';

        // The procedures the trigger body calls (when the crawl captured them on the trigger node).
        if (t.bodyCalls && t.bodyCalls.length) {
            html += '<div class="est-section"><div class="est-section-title">Calls from the body (' + t.bodyCalls.length + ')</div>';
            html += PA.estateTriggers._callsHtml(t.bodyCalls, esc);
            html += '</div>';
        }

        html += '<div class="est-section"><div class="est-section-title">Touches (' + (t.touchesCount || 0) + ')</div>';
        html += PA.estateTriggers._touchesHtml(t.touches, esc);
        html += '</div>';

        if (t.chain) {
            html += '<div class="est-section">' + PA.estateTriggers._chainHtml(t.chain, esc) + '</div>';
        }

        // The inverse map — procedures whose operation on the trigger's table MATCHES the event.
        html += '<div class="est-section"><div class="est-section-title">Will fire this trigger ('
            + ((t.willFire || []).length) + ')</div>';
        html += PA.estateTriggers._firedByHtml(t.willFire, esc,
            'No procedure in this analysis performs the trigger\'s event on its table.');
        html += '</div>';

        // Every procedure that writes the trigger's table (any DML), for completeness.
        html += '<div class="est-section"><div class="est-section-title">Writes the table — any DML ('
            + ((t.firedBy || []).length) + ')</div>';
        html += PA.estateTriggers._firedByHtml(t.firedBy, esc,
            'No procedure in this analysis writes to the trigger\'s table.');
        html += '</div>';

        return html;
    },

    /** Procedures the trigger body calls — each a clickable link into the procedure explorer. */
    _callsHtml: function(calls, esc) {
        var html = '<div class="est-usedby">';
        for (var i = 0; i < calls.length; i++) {
            var c = calls[i];
            var name = c.objectName || c.target || c.name || '';
            html += '<div class="est-usedby-item">'
                + (c.type ? '<span class="est-badge">' + esc(c.type) + '</span>' : '')
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.showProcedure(\''
                + PA.escJs(name) + '\')">' + esc(name) + '</span>'
                + (c.lines && c.lines.length ? '<span class="est-usedby-line">L' + c.lines[0] + '</span>' : '')
                + (c.count ? '<span class="est-usedby-line">' + c.count + 'x</span>' : '')
                + '</div>';
        }
        html += '</div>';
        return html;
    },

    _touchesHtml: function(touches, esc) {
        if (!touches || !touches.length) return '<div class="est-empty">This trigger body does not touch any table.</div>';
        var html = '<div class="est-chips">';
        for (var i = 0; i < touches.length; i++) {
            var b = touches[i];
            var op = (b.operation || 'SELECT').toUpperCase();
            var cls = op === 'INSERT' ? 'insert' : op === 'UPDATE' ? 'update' : op === 'DELETE' ? 'delete' : 'select';
            html += '<span class="est-chip">'
                + '<span class="est-op-badge ' + cls + '">' + esc(op) + '</span>'
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.tableDetail.open(\''
                + PA.escJs(b.name || '') + '\',\'' + PA.escJs(b.schema || '') + '\')">' + esc(b.name || '') + '</span>'
                + (b.schema ? '<span class="est-chip-sub">' + esc(b.schema) + '</span>' : '')
                + (b.objectType && b.objectType !== 'TABLE' ? '<span class="est-chip-sub">' + esc(b.objectType.replace('_', ' ')) + '</span>' : '')
                + '</span>';
        }
        html += '</div>';
        return html;
    },

    _firedByHtml: function(firedBy, esc, emptyMsg) {
        if (!firedBy || !firedBy.length) {
            return '<div class="est-empty">' + (emptyMsg || 'No procedure in this analysis writes to the trigger\'s table.') + '</div>';
        }
        var html = '<div class="est-usedby">';
        for (var i = 0; i < firedBy.length; i++) {
            var u = firedBy[i];
            html += '<div class="est-usedby-item">';
            html += '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.showProcedure(\'' + PA.escJs(u.nodeId || '') + '\')">' + esc(u.nodeName || u.nodeId || '') + '</span>';
            // When the row carries the event-matched subset, show it; else fall back to all ops.
            var ops = (u.matchingOperations && Object.keys(u.matchingOperations).length) ? u.matchingOperations : u.operations;
            html += PA.estateTriggers._opsHtml(ops, esc);
            if (u.nodeDepth != null) html += '<span class="est-usedby-line">depth ' + u.nodeDepth + '</span>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    _opsHtml: function(operations, esc) {
        if (!operations || typeof operations !== 'object') return '';
        var html = '<span class="est-usedby-ops">';
        for (var op in operations) {
            if (!Object.prototype.hasOwnProperty.call(operations, op)) continue;
            var lines = operations[op] || [];
            var cls = op === 'INSERT' ? 'insert' : op === 'UPDATE' ? 'update' : op === 'DELETE' ? 'delete' : op === 'SELECT' ? 'select' : '';
            html += '<span class="est-op-badge ' + cls + '">' + esc(op) + '</span>';
            if (lines.length) html += '<span class="est-usedby-line">L' + lines[0] + '</span>';
        }
        html += '</span>';
        return html;
    },

    /**
     * Trigger chain: level 0 is the trigger; each later level is the triggers bound to a table the
     * previous level touches. The arrow between levels is labelled with the touched table that
     * links them.
     */
    _chainHtml: function(chain, esc) {
        var levels = chain.levels || [];
        if (!levels.length) return '';
        var nodeBy = {};
        (chain.nodes || []).forEach(function(n) { nodeBy[(n.name || '').toUpperCase()] = n; });
        var edgesByFrom = {};
        (chain.edges || []).forEach(function(e) {
            var k = (e.from || '').toUpperCase();
            (edgesByFrom[k] = edgesByFrom[k] || []).push(e);
        });

        var html = '<div class="est-chain">';
        html += '<div class="est-chain-title">Trigger Chain</div>';
        html += '<div class="est-chain-meta">' + levels.length + ' level' + (levels.length === 1 ? '' : 's');
        if (chain.table) html += ' &middot; fires on <b>' + esc(chain.table) + '</b>';
        if (chain.cyclic) html += '<span class="est-flag est-flag-warn">cycle detected</span>';
        if (chain.truncated) html += '<span class="est-flag est-flag-warn">truncated</span>';
        html += '</div>';

        for (var i = 0; i < levels.length; i++) {
            var names = levels[i] || [];
            if (i > 0) {
                var vias = {};
                var prev = levels[i - 1] || [];
                for (var p = 0; p < prev.length; p++) {
                    var es = edgesByFrom[(prev[p] || '').toUpperCase()] || [];
                    for (var e = 0; e < es.length; e++) { if (es[e].via) vias[es[e].via] = true; }
                }
                var viaTxt = Object.keys(vias).join(', ');
                html += '<div class="est-chain-arrow">&#8595;'
                    + (viaTxt ? '<span class="est-chain-via">via ' + esc(viaTxt) + '</span>' : '')
                    + '</div>';
            }
            html += '<div class="est-chain-step"><span class="est-step-idx">L' + i + '</span><div class="est-step-nodes">';
            if (!names.length) {
                html += '<span class="est-node est-node-empty">(none)</span>';
            } else {
                for (var j = 0; j < names.length; j++) {
                    var nm = names[j] || '';
                    var nd = nodeBy[nm.toUpperCase()];
                    var sub = nd ? ((nd.table ? nd.table + ' ' : '') + (nd.event || '')).trim() : '';
                    var onclick = ' onclick="event.stopPropagation();PA.estateTriggers.showTrigger(\''
                        + PA.escJs(nm) + '\')"';
                    html += '<span class="est-node est-node-trigger"' + onclick + '>' + esc(nm)
                        + (sub ? '<span class="est-node-sub">' + esc(sub) + '</span>' : '')
                        + '</span>';
                }
            }
            html += '</div></div>';
        }
        if (chain.touchedTables && chain.touchedTables.length) {
            html += '<div class="est-chain-meta" style="margin-top:8px">Touched tables: '
                + chain.touchedTables.map(function(x) { return esc(x); }).join(', ') + '</div>';
        }
        html += '</div>';
        return html;
    }
};
