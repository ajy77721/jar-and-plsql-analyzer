window.PA = window.PA || {};

PA.triggerDetail = {
    _el: null,

    open(trigger, tableName, tableSchema) {
        if (!trigger) return;
        PA.triggerDetail.close();

        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'triggerDetailOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) PA.triggerDetail.close(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal';
        modal.onclick = function(e) { e.stopPropagation(); };

        var html = '';

        // Header
        html += '<div class="tds-header">';
        html += '<div class="tds-header-left">';
        html += '<span class="lp-icon T" style="display:inline-flex;width:18px;height:18px;font-size:9px;margin-right:4px">T</span>';
        html += '<span class="tds-title">' + PA.esc(trigger.name || '') + '</span>';
        if (trigger.schema) {
            var c = PA.getSchemaColor(trigger.schema);
            html += '<span class="ct-schema-badge" style="background:' + c.bg + ';color:' + c.fg + ';margin-left:6px">' + PA.esc(trigger.schema) + '</span>';
        }
        html += '</div>';
        html += '<button class="btn btn-sm tds-close" onclick="PA.triggerDetail.close()">&times;</button>';
        html += '</div>';

        // Meta bar
        html += '<div class="tds-meta">';
        if (trigger.timing) html += '<span class="trigger-timing">' + PA.esc(trigger.timing) + '</span>';
        if (trigger.event) html += '<span class="trigger-event">' + PA.esc(trigger.event) + '</span>';
        if (trigger.triggerType) html += '<span class="tds-meta-tag">' + PA.esc(trigger.triggerType) + '</span>';
        html += '<span class="tds-meta-tag">ON <strong style="color:var(--teal);cursor:pointer" onclick="PA.tableDetail.open(\'' + PA.escJs(tableName || '') + '\',\'' + PA.escJs(tableSchema || '') + '\')">' + PA.esc((tableSchema ? tableSchema + '.' : '') + (tableName || '')) + '</strong></span>';
        if (trigger.source) {
            var srcCls = trigger.source === 'DATABASE' ? 'td-source-db' : trigger.source === 'PARSED' ? 'td-source-parsed' : '';
            var srcLabel = trigger.source === 'DATABASE' ? 'DB' : trigger.source === 'PARSED' ? 'SRC' : trigger.source;
            html += '<span class="td-source-badge ' + srcCls + '">' + PA.esc(srcLabel) + '</span>';
        }
        html += '</div>';

        // Body
        html += '<div class="tds-body">';

        // Actions
        if (trigger.nodeId) {
            html += '<div class="tds-actions">';
            html += '<span class="btn btn-sm tds-action-btn" onclick="PA.showProcedure(\'' + PA.escJs(trigger.nodeId) + '\')">View in Call Trace</span>';
            html += '<span class="btn btn-sm tds-action-btn" onclick="PA.sourceView.openModal(\'' + PA.escJs(trigger.nodeId) + '\')">View Source</span>';
            html += '</div>';
        }

        // Table Operations (tables the trigger reads/writes)
        var tableOps = trigger.tableOps || [];
        if (tableOps.length > 0) {
            html += '<div class="tds-section">';
            html += '<div class="tds-section-title">Table Operations (' + tableOps.length + ')</div>';
            for (var i = 0; i < tableOps.length; i++) {
                var op = tableOps[i];
                html += '<div class="tds-table-op">';
                html += '<span class="op-badge ' + PA.esc(op.operation || '') + '">' + PA.esc(op.operation || '') + '</span>';
                var opTable = op.tableName || '';
                var opSchema = op.schema || '';
                html += '<span class="tds-table-link" onclick="PA.tableDetail.open(\'' + PA.escJs(opTable) + '\',\'' + PA.escJs(opSchema) + '\')">' + PA.esc((opSchema ? opSchema + '.' : '') + opTable) + '</span>';
                if (op.objectType) {
                    var ot = op.objectType;
                    var cls = ot === 'VIEW' ? 'view' : (ot === 'MATERIALIZED_VIEW' || ot === 'MATERIALIZED VIEW') ? 'mv' : 'table';
                    html += '<span class="to-type-badge ' + cls + '" style="margin-left:4px;font-size:8px">' + PA.esc(ot.replace('_', ' ')) + '</span>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        // Sequences
        var sequences = trigger.sequences || [];
        if (sequences.length > 0) {
            html += '<div class="tds-section">';
            html += '<div class="tds-section-title">Sequences (' + sequences.length + ')</div>';
            for (var si = 0; si < sequences.length; si++) {
                var seq = sequences[si];
                html += '<div class="tds-table-op">';
                html += '<span class="op-badge" style="background:var(--badge-purple-bg);color:var(--badge-purple)">' + PA.esc(seq.operation || 'NEXTVAL') + '</span>';
                html += '<span style="font-family:var(--font-mono);font-size:11px;font-weight:600;color:var(--text)">' + PA.esc((seq.schema ? seq.schema + '.' : '') + (seq.sequenceName || '')) + '</span>';
                html += '</div>';
            }
            html += '</div>';
        }

        // Firing table info
        html += '<div class="tds-section">';
        html += '<div class="tds-section-title">Fires On</div>';
        html += '<div class="tds-table-op">';
        html += '<span class="trigger-event" style="font-size:10px">' + PA.esc(trigger.event || 'INSERT') + '</span>';
        html += '<span class="tds-table-link" onclick="PA.tableDetail.open(\'' + PA.escJs(tableName || '') + '\',\'' + PA.escJs(tableSchema || '') + '\')">' + PA.esc((tableSchema ? tableSchema + '.' : '') + (tableName || '')) + '</span>';
        html += '</div>';
        html += '</div>';

        // Definition section
        if (trigger.definition) {
            html += '<div class="tds-section tds-def-section">';
            html += '<div class="tds-section-title">Definition</div>';
            html += '<pre class="trigger-source">' + PA.esc(trigger.definition) + '</pre>';
            html += '</div>';
        } else if (trigger.nodeId) {
            html += '<div class="tds-section tds-def-section" id="tds-def-section">';
            html += '<div class="tds-section-title">Definition</div>';
            html += '<div class="tds-def-loading" style="font-size:11px;color:var(--text-muted);padding:8px 0">Loading source...</div>';
            html += '</div>';
        } else {
            html += '<div class="tds-section tds-def-section" id="tds-def-section">';
            html += '<div class="tds-section-title">Definition</div>';
            html += '<div style="font-size:11px;color:var(--text-muted);font-style:italic;padding:8px 0">' +
                'Source not available — trigger body was not accessible from the database. ' +
                'Ensure the connection user has permission to read trigger definitions from the catalog.</div>';
            html += '</div>';
        }

        html += '</div>'; // end body

        modal.innerHTML = html;
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        PA.triggerDetail._el = overlay;
        if (PA.modalStack) overlay.style.zIndex = PA.modalStack.nextZ();

        requestAnimationFrame(function() { overlay.classList.add('open'); });

        if (!trigger.definition && trigger.nodeId) {
            // Pass the modal we just built rather than letting _loadSource look the section up by
            // id. close() defers removal by 200ms for the fade, so re-opening on a second trigger
            // leaves TWO #tds-def-section nodes in the document and getElementById returns the
            // DYING one — the source landed in the modal being removed and the new one sat on
            // "Loading source..." forever. Scoping the lookup to this modal removes the ambiguity.
            PA.triggerDetail._loadSource(trigger, modal);
        }
    },

    async _loadSource(trigger, modal) {
        // Stale-response guard (§B5): two awaits below, after which we write into a panel. A newer
        // open() must win. The seq is issued via UIBusy so the counter is initialised — an
        // uninitialised `++undefined` is NaN and would reject every response.
        var isCurrent = UIBusy.seq(PA.triggerDetail, '_loadSeq');
        var section = (modal || document).querySelector('#tds-def-section');
        if (!section) return;

        var source = null;

        // Try 1: fetch node detail if trigger was parsed from source
        if (trigger.nodeId) {
            // UI-024 (§163) — already inside an async function, one await added.
            await PA.nodeIndex.ensureLoaded();
            var node = PA.nodeIndex.byId(trigger.nodeId);
            if (node && node.detailFile) {
                try {
                    var detail = await PA.api.getNodeDetail(node.detailFile.replace(/^nodes\//, ''));
                    if (detail && detail.sourceText) {
                        source = detail.sourceText;
                    } else if (detail && node.sourceFile) {
                        // Try loading raw source file via sourceView
                        try {
                            var rawResp = await PA.api.getSource(node.sourceFile);
                            var raw = rawResp && rawResp.content;
                            if (raw && node.lineStart) {
                                var lines = raw.split('\n');
                                var start = Math.max(0, (node.lineStart || 1) - 1);
                                var end = node.lineEnd || lines.length;
                                source = lines.slice(start, end).join('\n');
                            }
                        } catch(e2) {}
                    }
                } catch(e) {}
            }
        }

        // Try 2: Use nodeId to load source via the parser source API
        if (!source && trigger.nodeId) {
            try {
                var srcData = await PA.api.getSource(trigger.nodeId);
                if (srcData && srcData.content) source = srcData.content;
            } catch(e) {}
        }

        if (!isCurrent()) return;   // a different trigger was opened while this was in flight

        if (source) {
            section.innerHTML = '<div class="tds-section-title">Definition</div>' +
                '<pre class="trigger-source">' + PA.esc(source) + '</pre>';
        } else {
            section.innerHTML = '<div class="tds-section-title">Definition</div>' +
                '<div style="font-size:11px;color:var(--text-muted);font-style:italic;padding:8px 0">' +
                'Source not available' +
                (trigger.nodeId ? ' — <span class="btn btn-sm" style="font-size:10px" onclick="PA.sourceView.openModal(\'' + PA.escJs(trigger.nodeId) + '\')">Try View Source</span>' : '') +
                '</div>';
        }
    },

    close() {
        var el = PA.triggerDetail._el || document.getElementById('triggerDetailOverlay');
        if (el) {
            el.classList.remove('open');
            setTimeout(function() { if (el.parentNode) el.remove(); }, 200);
        }
        PA.triggerDetail._el = null;
    }
};

(function() {
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && PA.triggerDetail._el) {
            PA.triggerDetail.close();
        }
    });
})();
