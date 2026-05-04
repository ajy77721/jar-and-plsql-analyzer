window.PA = window.PA || {};

PA.chunkViewer = {
    _chunks: [],
    _activeChunkId: null,
    _detailCache: {},

    open: function() {
        var name = PA.claude._analysisName();
        if (!name) { PA.toast('Load an analysis first', 'warn'); return; }

        this._ensureOverlay();
        document.getElementById('cvOverlay').classList.add('open');
        this._detailCache = {};
        this._activeChunkId = null;
        this._loadChunks(name);
    },

    close: function(e) {
        if (e && e.target && e.target.id !== 'cvOverlay') return;
        var ov = document.getElementById('cvOverlay');
        if (ov) ov.classList.remove('open');
    },

    _ensureOverlay: function() {
        if (document.getElementById('cvOverlay')) return;
        var h = '<div class="cv-overlay" id="cvOverlay" onclick="PA.chunkViewer.close(event)">';
        h += '<div class="cv-panel" onclick="event.stopPropagation()">';
        h += '<div class="cv-top-bar">';
        h += '<span class="cv-title">Claude Verification Chunks</span>';
        h += '<div class="cv-top-stats" id="cvTopStats"></div>';
        h += '<button class="btn btn-sm" onclick="PA.chunkViewer.close()" title="Close">&times;</button>';
        h += '</div>';
        h += '<div class="cv-split">';
        h += '<div class="cv-left" id="cvLeft"><div class="cv-empty">Loading...</div></div>';
        h += '<div class="cv-right" id="cvRight"><div class="cv-empty">Select a chunk</div></div>';
        h += '</div>';
        h += '</div></div>';
        document.body.insertAdjacentHTML('beforeend', h);
    },

    _loadChunks: function(name) {
        PA.api.claudeChunksSummary(name).then(function(data) {
            PA.chunkViewer._chunks = (data && data.chunks) || [];
            PA.chunkViewer._renderStats(data);
            PA.chunkViewer._renderList();
        }).catch(function(e) {
            PA.toast('Failed to load chunks: ' + (e.message || e), 'error');
        });
    },

    _renderStats: function(data) {
        var el = document.getElementById('cvTopStats');
        if (!el) return;
        var complete = data.completeCount || 0;
        var errors = data.errorCount || 0;
        var pending = data.pendingCount || 0;
        var total = data.total || 0;

        var result = PA.claude._result;
        var confirmed = 0, removed = 0, newOps = 0;
        if (result && result.tables) {
            for (var i = 0; i < result.tables.length; i++) {
                var vs = result.tables[i].claudeVerifications || [];
                for (var j = 0; j < vs.length; j++) {
                    var s = (vs[j].status || '').toUpperCase();
                    if (s === 'CONFIRMED') confirmed++;
                    else if (s === 'REMOVED') removed++;
                    else if (s === 'NEW') newOps++;
                }
            }
        }

        el.innerHTML =
            '<span class="badge cv-stat-badge complete">' + complete + ' complete</span>' +
            (errors > 0 ? '<span class="badge cv-stat-badge error">' + errors + ' errors</span>' : '') +
            (pending > 0 ? '<span class="badge cv-stat-badge pending">' + pending + ' pending</span>' : '') +
            '<span class="badge cv-stat-badge total">' + total + ' chunks</span>' +
            '<span class="cv-stat-sep">|</span>' +
            '<span class="badge cv-stat-badge confirmed">' + confirmed + ' confirmed</span>' +
            '<span class="badge cv-stat-badge removed">' + removed + ' removed</span>' +
            '<span class="badge cv-stat-badge new-ops">' + newOps + ' new</span>';
    },

    _renderList: function() {
        var el = document.getElementById('cvLeft');
        if (!el) return;
        var chunks = this._chunks;
        if (!chunks.length) { el.innerHTML = '<div class="cv-empty">No chunks found</div>'; return; }

        var h = '<div class="cv-search-wrap"><input class="cv-search" id="cvSearch" placeholder="Filter chunks..." oninput="PA.chunkViewer._filterList()"></div>';
        h += '<div class="cv-chunk-scroll" id="cvChunkScroll">';
        for (var i = 0; i < chunks.length; i++) {
            var c = chunks[i];
            var status = c.status || 'PENDING';
            var chunkName = c.name || c.chunkId || ('chunk_' + (i + 1));
            var shortName = chunkName.length > 60 ? chunkName.substring(0, 57) + '...' : chunkName;
            h += '<div class="cv-chunk-row" data-idx="' + i + '" data-chunk-id="' + PA.escAttr(c.chunkId || c.id || '') + '" onclick="PA.chunkViewer._select(' + i + ')">';
            h += '<span class="cv-dot ' + PA.esc(status) + '"></span>';
            h += '<div class="cv-chunk-info">';
            h += '<span class="cv-chunk-name" title="' + PA.escAttr(chunkName) + '">' + PA.esc(shortName) + '</span>';
            h += '<span class="cv-chunk-meta">';
            if (c.tableCount !== undefined) h += c.tableCount + ' tables';
            if (c.nodeIds && c.nodeIds.length) h += ' &middot; ' + c.nodeIds.length + ' procs';
            h += '</span>';
            h += '</div>';
            h += '<span class="cv-chunk-status-label ' + PA.esc(status) + '">' + PA.esc(status) + '</span>';
            h += '</div>';
        }
        h += '</div>';
        el.innerHTML = h;
    },

    _filterList: function() {
        var q = (document.getElementById('cvSearch') || {}).value || '';
        q = q.toLowerCase();
        var rows = document.querySelectorAll('.cv-chunk-row');
        for (var i = 0; i < rows.length; i++) {
            var name = (rows[i].querySelector('.cv-chunk-name') || {}).textContent || '';
            rows[i].style.display = (!q || name.toLowerCase().indexOf(q) >= 0) ? '' : 'none';
        }
    },

    _select: function(idx) {
        var chunks = this._chunks;
        if (!chunks[idx]) return;
        var c = chunks[idx];
        var chunkId = c.chunkId || c.id || '';
        this._activeChunkId = chunkId;

        document.querySelectorAll('.cv-chunk-row').forEach(function(r) {
            r.classList.toggle('active', parseInt(r.dataset.idx) === idx);
        });

        var right = document.getElementById('cvRight');
        if (!right) return;

        if (this._detailCache[chunkId]) {
            this._renderDetail(this._detailCache[chunkId], c);
            return;
        }

        right.innerHTML = '<div class="cv-empty">Loading chunk ' + PA.esc(chunkId) + '...</div>';
        var name = PA.claude._analysisName();
        PA.api.claudeChunkDetail(name, chunkId).then(function(data) {
            PA.chunkViewer._detailCache[chunkId] = data;
            if (PA.chunkViewer._activeChunkId === chunkId) {
                PA.chunkViewer._renderDetail(data, c);
            }
        }).catch(function(e) {
            right.innerHTML = '<div class="cv-empty" style="color:var(--red)">Failed: ' + PA.esc(e.message || '') + '</div>';
        });
    },

    _renderDetail: function(data, chunkMeta) {
        var right = document.getElementById('cvRight');
        if (!right) return;

        var h = '<div class="cv-detail-header">';
        h += '<span class="cv-detail-name">' + PA.esc(chunkMeta.name || chunkMeta.chunkId || '') + '</span>';
        h += '<div class="cv-detail-tabs">';
        h += '<button class="cv-tab active" data-tab="response" onclick="PA.chunkViewer._switchTab(\'response\')">Response</button>';
        h += '<button class="cv-tab" data-tab="prompt" onclick="PA.chunkViewer._switchTab(\'prompt\')">Prompt</button>';
        if (data.error) h += '<button class="cv-tab cv-tab-error" data-tab="error" onclick="PA.chunkViewer._switchTab(\'error\')">Error</button>';
        h += '</div>';
        h += '</div>';

        // Response tab
        h += '<div class="cv-tab-content active" data-tab="response">';
        h += this._renderResponse(data);
        h += '</div>';

        // Prompt tab
        h += '<div class="cv-tab-content" data-tab="prompt">';
        h += this._renderPrompt(data);
        h += '</div>';

        // Error tab
        if (data.error) {
            h += '<div class="cv-tab-content" data-tab="error">';
            h += '<pre class="cv-error-pre">' + PA.esc(data.error) + '</pre>';
            h += '</div>';
        }

        right.innerHTML = h;
    },

    _switchTab: function(tab) {
        document.querySelectorAll('#cvRight .cv-tab').forEach(function(t) {
            t.classList.toggle('active', t.dataset.tab === tab);
        });
        document.querySelectorAll('#cvRight .cv-tab-content').forEach(function(c) {
            c.classList.toggle('active', c.dataset.tab === tab);
        });
    },

    _parseOutput: function(output) {
        if (!output) return null;
        if (typeof output === 'object' && output.tables) return output;
        var raw = typeof output === 'string' ? output : JSON.stringify(output);
        var m = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
        if (m) raw = m[1].trim();
        try { return JSON.parse(raw); } catch (e) { return null; }
    },

    _renderResponse: function(data) {
        var output = this._parseOutput(data.output);
        if (!output) {
            var raw = typeof data.output === 'string' ? data.output : JSON.stringify(data.output, null, 2);
            if (!raw) return '<div class="cv-empty">No response data</div>';
            return '<pre class="cv-code-block">' + PA.esc(raw) + '</pre>';
        }

        var tables = output.tables || [];
        if (!Array.isArray(tables)) {
            return '<pre class="cv-code-block">' + PA.esc(JSON.stringify(output, null, 2)) + '</pre>';
        }

        var h = '';
        if (output.summary) {
            h += '<div class="cv-resp-summary">' + PA.esc(output.summary) + '</div>';
        }

        var counts = { CONFIRMED: 0, REMOVED: 0, NEW: 0 };
        for (var i = 0; i < tables.length; i++) {
            var ops = tables[i].operations || [];
            for (var j = 0; j < ops.length; j++) {
                var s = (ops[j].status || '').toUpperCase();
                if (counts[s] !== undefined) counts[s]++;
            }
        }
        h += '<div class="cv-resp-counts">';
        h += '<span class="badge cv-stat-badge confirmed">' + counts.CONFIRMED + ' confirmed</span>';
        h += '<span class="badge cv-stat-badge removed">' + counts.REMOVED + ' removed</span>';
        h += '<span class="badge cv-stat-badge new-ops">' + counts.NEW + ' new</span>';
        h += '</div>';

        for (var i = 0; i < tables.length; i++) {
            var t = tables[i];
            h += '<div class="cv-resp-table">';
            h += '<div class="cv-resp-table-name">' + PA.esc(t.tableName || '?') + '</div>';
            var ops = t.operations || [];
            h += '<table class="cv-resp-ops"><tbody>';
            for (var j = 0; j < ops.length; j++) {
                var op = ops[j];
                var statusCls = (op.status || '').toUpperCase();
                h += '<tr class="cv-resp-op-row ' + statusCls + '">';
                h += '<td><span class="op-badge ' + PA.esc(op.operation || '') + '">' + PA.esc(op.operation || '?') + '</span></td>';
                h += '<td><span class="cv-badge ' + statusCls + '">' + PA.esc(op.status || '') + '</span></td>';
                h += '<td class="cv-resp-proc">' + PA.esc(op.procedureName || '') + '</td>';
                h += '<td class="cv-resp-line">' + (op.lineNumber ? 'L' + op.lineNumber : '') + '</td>';
                h += '<td class="cv-resp-reason">' + PA.esc(op.reason || '') + '</td>';
                h += '</tr>';
            }
            h += '</tbody></table>';
            h += '</div>';
        }
        return h;
    },

    _renderPrompt: function(data) {
        var input = data.input;
        if (!input) return '<div class="cv-empty">No prompt data</div>';

        var prompt = '';
        if (typeof input === 'object') {
            prompt = input.prompt || '';
        } else {
            prompt = String(input);
        }
        if (!prompt) return '<div class="cv-empty">No prompt text</div>';

        var h = '';
        if (typeof input === 'object') {
            h += '<div class="cv-prompt-meta">';
            if (input.timestamp) h += '<span class="cv-prompt-meta-item">Time: ' + PA.esc(input.timestamp) + '</span>';
            if (input.promptLength) h += '<span class="cv-prompt-meta-item">Prompt length: ' + input.promptLength.toLocaleString() + ' chars</span>';
            if (input.tableCount) h += '<span class="cv-prompt-meta-item">Tables: ' + input.tableCount + '</span>';
            if (input.nodeIds && input.nodeIds.length) h += '<span class="cv-prompt-meta-item">Nodes: ' + input.nodeIds.join(', ') + '</span>';
            h += '</div>';
        }

        h += this._formatPrompt(prompt);
        return h;
    },

    _formatPrompt: function(prompt) {
        var sections = prompt.split(/^(=== .+ ===)$/m);
        if (sections.length <= 1) {
            return '<pre class="cv-code-block">' + PA.esc(prompt) + '</pre>';
        }

        var h = '';
        for (var i = 0; i < sections.length; i++) {
            var s = sections[i].trim();
            if (!s) continue;
            if (/^=== .+ ===$/.test(s)) {
                h += '<div class="cv-prompt-section-title">' + PA.esc(s.replace(/^=== | ===$/g, '')) + '</div>';
            } else if (s.indexOf('-- Source:') >= 0 || s.indexOf('CREATE OR REPLACE') >= 0) {
                h += '<pre class="cv-code-block cv-plsql">' + this._highlightPlsql(PA.esc(s)) + '</pre>';
            } else if (s.indexOf('TABLE:') >= 0) {
                h += '<pre class="cv-code-block cv-findings">' + this._highlightFindings(PA.esc(s)) + '</pre>';
            } else {
                h += '<pre class="cv-code-block">' + PA.esc(s) + '</pre>';
            }
        }
        return h;
    },

    _highlightPlsql: function(escaped) {
        escaped = escaped.replace(/^(\s*\d+:)/gm, '<span class="cv-line-num">$1</span>');
        escaped = escaped.replace(/(-- Source:[^\n]+)/g, '<span class="cv-src-header">$1</span>');
        return escaped;
    },

    _highlightFindings: function(escaped) {
        escaped = escaped.replace(/(TABLE:\s*)([^\n]+)/g, '$1<strong>$2</strong>');
        escaped = escaped.replace(/(SELECT|INSERT|UPDATE|DELETE|MERGE|TRUNCATE)/g, '<span class="cv-op-hl">$1</span>');
        return escaped;
    }
};
