window.PA = window.PA || {};

/**
 * Explore → Transactions sub-view.
 * Groups the flow's transaction-control statements (SAVEPOINT / COMMIT / ROLLBACK, + autonomous
 * procs) into transaction SEGMENTS, tagged T1, T2, T3 … in call order. For each Tn it shows
 * where it starts, its savepoints, and where it commits / rolls back — as a Table and a Graph.
 * Scope-aware (Direct/Subtree/SubFlow/Full + Make Root) and dynamic as the current proc changes.
 * Data source: PA.analysisData.transactionStatements (read-only; no server call).
 */
PA.txnTrace = {
    _view: 'table',   // 'runs' | 'table' | 'graph' — runs = the runtime-accurate flow (needs a rooted proc)
    _q: '',
    _page: 1,
    _pageSize: 25,    // keep the DOM small even when the call trace has 100s of procs

    setView: function (v) {
        PA.txnTrace._view = v;
        var r = document.getElementById('txnViewRunsBtn'), t = document.getElementById('txnViewTableBtn'), g = document.getElementById('txnViewGraphBtn');
        if (r) r.classList.toggle('active', v === 'runs');
        if (t) t.classList.toggle('active', v === 'table');
        if (g) g.classList.toggle('active', v === 'graph');
        PA.txnTrace._page = 1;
        PA.txnTrace.render();
    },

    onSearch: function (v) { PA.txnTrace._q = (v || '').toUpperCase(); PA.txnTrace._page = 1; PA.txnTrace.render(); },
    goPage: function (p) { PA.txnTrace._page = p; PA.txnTrace.render(); },

    _matches: function (s) {
        var q = PA.txnTrace._q;
        if (!q) return true;
        var hay = (s.id + ' ' + ((s.open && s.open.proc) || '')
            + (s.commit ? ' COMMIT' : '') + (s.rollback ? ' ROLLBACK' : '')
            + (s.autonomous ? ' AUTO AUTONOMOUS' : '')).toUpperCase();
        return hay.indexOf(q) >= 0;
    },

    // Per-proc DML write lines (INSERT/UPDATE/DELETE/MERGE) — used to derive a real transaction
    // start (first write after the previous boundary). Fetched once per analysis from the raw
    // tables endpoint (getTableOperations drops the per-line detail).
    _writesByProc: null,
    _writesByNode: null,   // same write list keyed by nodeId (upper) — robust proc match in the Details tree
    _writesKey: null,
    _ensureWrites: function (cb) {
        var self = PA.txnTrace;
        var key = (PA.api && PA.api._analysisName) || '';
        if (self._writesByProc && self._writesKey === key) { cb(); return; }
        var WRITE = { INSERT: 1, UPDATE: 1, DELETE: 1, MERGE: 1 };
        fetch('/api/parser/analyses/' + encodeURIComponent(key) + '/tables')
            .then(function (r) { return r.ok ? r.json() : { tables: [] }; })
            .then(function (data) {
                var map = {}, byNode = {};
                (data.tables || []).forEach(function (tb) {
                    (tb.usedBy || []).forEach(function (u) {
                        var proc = (u.nodeName || '').toUpperCase();
                        var nid = (u.nodeId || '').toUpperCase();
                        var ops = u.operations || {};
                        Object.keys(ops).forEach(function (op) {
                            var OP = op.toUpperCase();
                            if (!WRITE[OP]) return;
                            (ops[op] || []).forEach(function (ln) {
                                var rec = { line: ln, op: OP, nodeId: u.nodeId, proc: u.nodeName, table: tb.name || tb.tableName || '' };
                                (map[proc] = map[proc] || []).push(rec);
                                if (nid) (byNode[nid] = byNode[nid] || []).push(rec);
                            });
                        });
                    });
                });
                var sortL = function (a, b) { return a.line - b.line; };
                Object.keys(map).forEach(function (p) { map[p].sort(sortL); });
                Object.keys(byNode).forEach(function (p) { byNode[p].sort(sortL); });
                self._writesByProc = map; self._writesByNode = byNode; self._writesKey = key;
                cb();
            })
            .catch(function () { self._writesByProc = {}; self._writesByNode = {}; self._writesKey = key; cb(); });
    },

    /** The genuine start of a transaction: earliest of its first DML write or its first savepoint,
     *  after the previous boundary. Null → truly implicit (no recorded write/savepoint). */
    _startEvent: function (s) {
        var w = s.start;                        // first write in window (computed in _buildSegments)
        var firstSp = (s.savepoints && s.savepoints.length) ? s.savepoints[0] : null;   // earliest savepoint
        if (w && (!firstSp || w.line < firstSp.line)) return { proc: w.proc, line: w.line, nodeId: w.nodeId, kind: w.op };
        if (firstSp) return { proc: firstSp.proc, line: firstSp.line, nodeId: firstSp.nodeId, kind: firstSp.type || 'SAVEPOINT' };
        return null;
    },

    /** Order statements by call-flow, scope-filter, then group into T# segments. */
    _buildSegments: function () {
        var d = PA.analysisData || {};
        var stmts = (d.transactionStatements || []).slice();

        // Scope to the CURRENT ROOT's whole call tree (NOT the Direct/Subtree/SubFlow/Full radio).
        // So the view reflects everything reachable from the current root, and changes only when
        // the root changes (Make Root / selecting a procedure). Null/empty set = full analysis.
        var ids = (PA.context && PA.context.callTreeNodeIds) || null;
        if (ids && ids.size) stmts = stmts.filter(function (s) { return !s.nodeId || ids.has((s.nodeId || '').toUpperCase()); });

        // Call-flow order: nodeId -> node order (fallback large).
        var order = {};
        (d.nodes || []).forEach(function (n) { order[(n.nodeId || '').toUpperCase()] = (n.order != null ? n.order : 9999); });
        var ord = function (s) { var o = order[(s.nodeId || '').toUpperCase()]; return o != null ? o : 9999; };
        stmts.sort(function (a, b) { return ord(a) !== ord(b) ? ord(a) - ord(b) : (a.line || 0) - (b.line || 0); });

        // Group statements per procedure. A procedure's transaction almost always has TWO exits that
        // are mutually exclusive at run time: the happy-path COMMIT (in TRY / the main body) and the
        // error-path ROLLBACK (in the CATCH / EXCEPTION handler). They are ONE transaction, so we pair
        // them into a single segment (seg.commit = success exit, seg.rollback = error exit) instead of
        // counting them as two. A named ROLLBACK TO <savepoint> is a partial revert (an inner marker).
        // Autonomous statements run in an independent transaction, so they form their own group.
        var groups = {};
        stmts.forEach(function (s) {
            var key = (s.autonomous ? 'A::' : 'N::') + (s.nodeId || '').toUpperCase();
            (groups[key] = groups[key] || { auto: !!s.autonomous, list: [] }).list.push(s);
        });

        var segments = [];
        var mkSeg = function (list, auto, commit, rollback) {
            var savepts = list.filter(function (x) { return x.type === 'SAVEPOINT' || (x.type === 'ROLLBACK' && x.name); })
                .map(function (x) { return x.type === 'ROLLBACK' ? Object.assign({ _revert: true }, x) : x; })
                .sort(function (a, b) { return (a.line || 0) - (b.line || 0); });
            return {
                events: list.slice(), savepoints: savepts, autonomous: auto,
                open: list[0],
                commit: commit || null,
                rollback: rollback || null,
                close: commit || rollback || null   // primary close: prefer the COMMIT (success) exit
            };
        };
        Object.keys(groups).forEach(function (k) {
            var g = groups[k];
            var list = g.list.slice().sort(function (a, b) { return (a.line || 0) - (b.line || 0); });
            var commits = list.filter(function (x) { return x.type === 'COMMIT'; });
            var fullRB = list.filter(function (x) { return x.type === 'ROLLBACK' && !x.name; });

            if (commits.length <= 1 && fullRB.length <= 1) {
                segments.push(mkSeg(list, g.auto, commits[0], fullRB[0]));   // one transaction, ≤2 exits
            } else {
                // Rare: several real transactions in one proc — pair each COMMIT with the next full ROLLBACK.
                var used = {};
                commits.forEach(function (c) {
                    var rb = null;
                    for (var i = 0; i < fullRB.length; i++) { if (!used[i] && fullRB[i].line > c.line) { rb = fullRB[i]; used[i] = 1; break; } }
                    segments.push(mkSeg(list, g.auto, c, rb));
                });
                fullRB.forEach(function (r, i) { if (!used[i]) segments.push(mkSeg(list, g.auto, null, r)); });
            }
        });

        // Stable T# numbering by first-event call order.
        segments.sort(function (a, b) {
            var oa = a.open ? ord(a.open) : 0, ob = b.open ? ord(b.open) : 0;
            return oa !== ob ? oa - ob : ((a.open && a.open.line) || 0) - ((b.open && b.open.line) || 0);
        });
        segments.forEach(function (seg, i) { seg.id = 'T' + (i + 1); });

        // Derive a real start: the first DML write in the segment's proc that falls after the
        // previous transaction boundary in that proc and at/before this segment's close.
        var writes = PA.txnTrace._writesByProc || {};
        var lastBoundary = {};
        segments.forEach(function (seg) {
            var closeE = seg.close || seg.open;
            var proc = ((closeE && closeE.proc) || '').toUpperCase();
            var closeLine = (seg.close && seg.close.line) || (closeE && closeE.line) || Number.MAX_SAFE_INTEGER;
            var prev = lastBoundary[proc] || 0;
            var list = writes[proc] || [];
            for (var i = 0; i < list.length; i++) {
                if (list[i].line > prev && list[i].line <= closeLine) { seg.start = list[i]; break; }
            }
            if (seg.close && seg.close.line) lastBoundary[proc] = seg.close.line;
        });
        return segments;
    },

    render: function () {
        var c = document.getElementById('txnContainerTrace');
        if (!c) return;
        if (PA.txnTrace._view === 'runs') { PA.txnTrace._renderRuns(c); return; }
        PA.txnTrace._ensureWrites(function () { PA.txnTrace._renderNow(c); });
    },

    // ─────────────────────────────────────────────────────────────────────────
    // RUNS view — the runtime-accurate transaction flow (task: transaction capture
    // reliability). Data: GET /analyses/{name}/txn-runs/{rootNodeId} — the backend's
    // interprocedural resolver: per execution path (guarded conditional commits/rollbacks
    // enumerate alternative runs), a merged event timeline ACROSS procedures (a callee's
    // COMMIT persists the caller's work on Oracle — what the per-proc segments cannot
    // show), and a per-write survival verdict replayed under the dialect's ownership
    // semantics. Falls back to the statement views when no root proc is selected or the
    // endpoint yields nothing.
    // ─────────────────────────────────────────────────────────────────────────

    _runsKey: null,
    _runsCache: {},

    _renderRuns: function (c) {
        var name = (PA.api && PA.api._analysisName) || '';
        var entry = (PA.context && PA.context.procId) || (PA.context && PA.context.callTreeRootId) || '';
        if (!name || !entry) {
            c.innerHTML = '<div class="empty-msg">The runtime flow needs a rooted procedure — select one (or “Make Root”), or use the Table/Graph statement views for the whole analysis.</div>';
            return;
        }
        var key = name + '|' + entry;
        if (PA.txnTrace._runsCache[key]) { PA.txnTrace._renderRunsNow(c, PA.txnTrace._runsCache[key]); return; }
        c.innerHTML = '<div class="empty-msg">Resolving transaction runs…</div>';
        fetch('/api/parser/analyses/' + encodeURIComponent(name) + '/txn-runs/' + encodeURIComponent(entry))
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (data) {
                PA.txnTrace._runsCache[key] = data;
                PA.txnTrace._renderRunsNow(c, data);
            })
            .catch(function () { PA.txnTrace._runsCache[key] = null; PA.txnTrace._renderRunsNow(c, null); });
    },

    _renderRunsNow: function (c, data) {
        var runs = (data && data.runs) || [];
        if (!runs.length) {
            c.innerHTML = '<div class="empty-msg">No transaction runs resolved for this entry — use the Table/Graph statement views.</div>';
            return;
        }
        var outcomeCls = { COMMITTED: 'commit', ROLLED_BACK: 'rollback', PARTIAL: 'partial', OPEN: 'open', AUTONOMOUS: 'auto' };
        var SURV = {
            survived: { label: 'survived', cls: 'commit', tip: 'committed — permanent' },
            discarded: { label: 'discarded', cls: 'rollback', tip: 'undone by a rollback on this path' },
            open: { label: 'open', cls: 'open', tip: 'transaction still open — survives only if a caller commits later' }
        };
        var html = '';

        if (data.pathCountExceeded) html += '<div class="txn-resolve-line" style="color:var(--warn,#b58900)">More guarded statements than the path limit — showing the conservative run (all conditions assumed true). Check each event’s guard on the Table view.</div>';
        html += '<div class="txn-summary"><span class="txn-sum-chip">' + runs.length + ' execution path' + (runs.length === 1 ? '' : 's') + '</span>'
            + '<span class="txn-sum-chip">' + PA.esc((data.dialect || '') + (data.dialect ? ' semantics' : '')) + '</span></div>';

        runs.forEach(function (run) {
            var oc = outcomeCls[run.outcome] || 'open';
            html += '<div class="txn-track' + (run.kind === 'autonomous' ? ' auto' : '') + '" style="display:block">';
            html += '<div class="txn-track-tag"><span class="txn-tag' + (run.kind === 'autonomous' ? ' auto' : '') + '">R' + run.runId + '</span>'
                + '<span class="txn-track-proc">' + PA.esc(run.kind === 'autonomous' ? 'autonomous side transaction' : 'execution path') + '</span>'
                + '<span class="txn-exit-tag ' + (oc === 'commit' ? 'ok' : oc === 'rollback' ? 'err' : '') + '" style="margin-left:8px">' + PA.esc(run.outcome || '') + '</span>'
                + '</div>';
            // The resolver's own plain-English outcome — derived from the replay, not a template.
            if (run.summary) html += '<div class="txn-resolve-line" style="padding:4px 8px">' + run.summary + '</div>';
            if (run.assumptions && run.assumptions.length) {
                html += '<div style="padding:0 8px;font-size:10px;color:var(--text-muted)">assuming: '
                    + run.assumptions.map(function (a) {
                        return PA.esc(a.condition || '?') + ' → <b style="color:var(--' + (a.taken ? 'badge-green' : 'red') + ')">' + (a.taken ? 'true' : 'false') + '</b>';
                    }).join(', ') + '</div>';
            }
            // The merged event timeline — COLLAPSED like the other views: consecutive same-proc
            // DML writes become ONE group chip per operation with the survival summary (a busy
            // proc carried 200+ individual nodes — unreadable); savepoints collapse into their
            // group; only the BOUNDARY events (COMMIT / ROLLBACK / BEGIN TRAN, with guards) and
            // the CALL edges stay individual. The Details modal carries the full per-write list.
            html += '<div class="txn-track-line" style="flex-wrap:wrap">';
            var groups = [];
            var cur = null;
            (run.events || []).forEach(function (e) {
                var boundary = (e.kind === 'COMMIT' || e.kind === 'ROLLBACK' || e.kind === 'ROLLBACK_TO' || e.kind === 'BEGIN_TRAN');
                var isCall = e.kind === 'CALL';
                if (boundary || isCall) {
                    groups.push({ type: boundary ? 'boundary' : 'call', ev: e });
                    cur = null;
                    return;
                }
                // Group key: proc + op for WRITEs; proc for savepoint-ish markers.
                var key = e.kind === 'WRITE'
                    ? e.nodeId + '|' + e.op
                    : e.nodeId + '|MARK';
                if (!cur || cur.key !== key) {
                    cur = { type: 'group', key: key, nodeId: e.nodeId, proc: e.proc, depth: e.depth,
                            op: e.kind === 'WRITE' ? e.op : null, lines: [], surv: {}, marks: 0 };
                    groups.push({ type: 'group', g: cur });
                }
                if (e.kind === 'WRITE') {
                    cur.lines.push(e.line);
                    var sv = e.survival || 'open';
                    cur.surv[sv] = (cur.surv[sv] || 0) + 1;
                } else {
                    cur.marks++;
                }
            });

            groups.forEach(function (g) {
                if (g.type === 'boundary') {
                    var e = g.ev;
                    var map = { ROLLBACK_TO: ['RB→SP', 'sp'], ROLLBACK: ['ROLLBACK', 'rollback'], COMMIT: ['COMMIT', 'commit'], BEGIN_TRAN: ['BEGIN TRAN', 'begin'] };
                    var m = map[e.kind] || [e.kind, 'begin'];
                    var lbl = '<b>' + m[0] + '</b>' + (e.name ? ' ' + PA.esc(e.name) : '')
                        + (e.guard ? '<br><span class="txn-implicit">if ' + PA.esc(e.guard.condition || '') + '</span>' : '');
                    var jump = e.nodeId ? ' onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(e.nodeId) + '\',' + (e.line || 0) + ')" style="cursor:pointer"' : '';
                    html += '<div class="txn-node"' + jump + ' title="' + PA.esc(e.proc || '') + ' :line ' + (e.line || 0) + '">'
                        + '<span class="txn-dot ' + m[1] + '"></span><span class="txn-node-lbl">' + lbl + '<br><span class="txn-implicit">' + PA.esc((e.proc || '').split('$').pop() || '') + ' :' + (e.line || 0) + '</span></span></div>';
                    return;
                }
                if (g.type === 'call') {
                    var ce = g.ev;
                    html += '<div class="txn-node" title="calls ' + PA.esc(ce.proc || '') + '">'
                        + '<span class="txn-dot call"></span><span class="txn-node-lbl"><b>CALLS</b> ' + PA.esc((ce.proc || '').replace(/\$/g, '.')) + '</span></div>';
                    return;
                }
                var grp = g.g;
                var procShort = PA.esc((grp.proc || '').split('$').pop() || '');
                var jumpG = grp.nodeId ? ' onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(grp.nodeId) + '\',' + (grp.lines.length ? grp.lines[0] : 0) + ')" style="cursor:pointer"' : '';
                if (grp.op) {
                    // One chip per proc+operation: "6 UPD — 4 survived · 2 discarded".
                    var bits = [];
                    var total = 0;
                    Object.keys(grp.surv).forEach(function (sv) {
                        var s = SURV[sv] || SURV.open;
                        bits.push('<span class="txn-survival ' + s.cls + '" title="' + s.tip + '">' + grp.surv[sv] + ' ' + s.label + '</span>');
                        total += grp.surv[sv];
                    });
                    html += '<div class="txn-node"' + jumpG + ' title="' + PA.esc(grp.proc || '') + ' lines ' + (grp.lines.length ? grp.lines[0] : '') + '–' + (grp.lines.length ? grp.lines[grp.lines.length - 1] : '') + '">'
                        + '<span class="txn-dot ' + (grp.surv.discarded ? 'rollback' : grp.surv.open ? 'open' : 'commit') + '"></span>'
                        + '<span class="txn-node-lbl"><b>' + total + ' ' + PA.esc(grp.op) + '</b> ' + procShort
                        + '<br>' + bits.join(' ') + '</span></div>';
                } else if (grp.marks) {
                    html += '<div class="txn-node" title="savepoints in ' + PA.esc(grp.proc || '') + '">'
                        + '<span class="txn-dot sp"></span><span class="txn-node-lbl"><b>' + grp.marks + ' savepoint' + (grp.marks === 1 ? '' : 's') + '</b> ' + procShort + '</span></div>';
                }
            });
            html += '</div></div>';
        });
        html += '<div class="txn-legend">'
            + '<span><i class="txn-dot commit"></i> survives (committed)</span>'
            + '<span><i class="txn-dot rollback"></i> discarded by a rollback</span>'
            + '<span><i class="txn-dot open"></i> open — needs a caller commit</span>'
            + '<span><i class="txn-dot call"></i> call edge</span>'
            + '<span><i class="txn-dot sp"></i> savepoint</span></div>';
        c.innerHTML = html;
    },

    _scopeKey: null,
    _renderNow: function (c) {
        // Reset per-view state whenever the analysis OR the current root changes, so reopening a
        // procedure (back button, Home, Make Root) shows a fresh view instead of stale filter/page/data.
        var scope = ((PA.api && PA.api._analysisName) || '') + '|' + ((PA.context && PA.context.callTreeRootId) || (PA.context && PA.context.procId) || '');
        if (scope !== PA.txnTrace._scopeKey) {
            PA.txnTrace._scopeKey = scope;
            PA.txnTrace._q = '';
            PA.txnTrace._page = 1;
            PA.txnTrace._lastSegs = null;
            var si = document.getElementById('txnSearchTrace');
            if (si) si.value = '';
        }

        var note = document.getElementById('txnScopeNote');
        if (note) {
            var root = (PA.context && PA.context.procId) || ((PA.analysisData || {}).entryPoint) || '';
            var ids = (PA.context && PA.context.callTreeNodeIds) || null;
            note.textContent = (root && ids && ids.size)
                ? '(within ' + String(root).replace(/\$/g, '.') + ')'
                : '(full analysis)';
        }

        var all = PA.txnTrace._buildSegments();
        all.forEach(function (s, i) { s._idx = i; });
        PA.txnTrace._lastSegs = all;   // for the per-transaction Details modal
        var cnt = document.getElementById('txnCount');
        if (cnt) cnt.textContent = all.length + ' transaction' + (all.length === 1 ? '' : 's');

        if (!all.length) {
            var rootName = (PA.context && PA.context.procId) ? String(PA.context.procId).replace(/\$/g, '.') : '';
            c.innerHTML = '<div class="empty-msg">No transaction-control statements (COMMIT / ROLLBACK / SAVEPOINT) in '
                + (rootName ? '<b>' + PA.esc(rootName) + '</b> or the procedures it calls.' : 'this analysis.')
                + ' Try “Make Root” on a different procedure.</div>';
            return;
        }

        // Compact summary over ALL in-scope transactions (replaces the verbose per-txn legend).
        var summary = PA.txnTrace._summaryHtml(all);

        // Filter + paginate so a 100+ proc trace stays light in the DOM.
        var filtered = all.filter(PA.txnTrace._matches);
        var pageSize = PA.txnTrace._pageSize;
        var pages = Math.max(1, Math.ceil(filtered.length / pageSize));
        if (PA.txnTrace._page > pages) PA.txnTrace._page = pages;
        var start = (PA.txnTrace._page - 1) * pageSize;
        var pageSegs = filtered.slice(start, start + pageSize);

        var body = pageSegs.length
            ? (PA.txnTrace._view === 'graph' ? PA.txnTrace._graphHtml(pageSegs) : PA.txnTrace._tableHtml(pageSegs))
            : '<div class="empty-msg">No transactions match the filter.</div>';

        c.innerHTML = summary + PA.txnTrace._pagerHtml(filtered.length, start, pageSegs.length) + body;
    },

    _summaryHtml: function (segs) {
        var commit = 0, rollback = 0, sp = 0, auto = 0, open = 0;
        segs.forEach(function (s) {
            if (s.autonomous) auto++;
            sp += s.savepoints.length;
            if (s.commit) commit++;
            if (s.rollback) rollback++;
            if (!s.commit && !s.rollback) open++;
        });
        var chip = function (label, val, cls) { return '<span class="txn-sum-chip ' + (cls || '') + '">' + val + ' ' + label + '</span>'; };
        return '<div class="txn-summary">'
            + chip('transactions', segs.length, '')
            + chip('commit exit', commit, 'commit')
            + chip('error rollback', rollback, 'rollback')
            + chip('savepoints', sp, 'sp')
            + chip('autonomous', auto, 'auto')
            + (open ? chip('open (no close)', open, 'open') : '')
            + '</div>';
    },

    _pagerHtml: function (total, start, shown) {
        var pageSize = PA.txnTrace._pageSize;
        if (total <= pageSize) return '';
        var page = PA.txnTrace._page, pages = Math.max(1, Math.ceil(total / pageSize));
        var from = total === 0 ? 0 : start + 1, to = start + shown;
        var pd = page <= 1 ? 'disabled' : '', nd = page >= pages ? 'disabled' : '';
        return '<div class="pagination-bar txn-pager">'
            + '<span class="page-info">' + from + '–' + to + ' of ' + total + '</span>'
            + '<span class="page-btns">'
            + '<button ' + pd + ' onclick="PA.txnTrace.goPage(1)">&laquo;</button>'
            + '<button ' + pd + ' onclick="PA.txnTrace.goPage(' + (page - 1) + ')">&lsaquo;</button>'
            + '<span class="page-info">Page ' + page + ' / ' + pages + '</span>'
            + '<button ' + nd + ' onclick="PA.txnTrace.goPage(' + (page + 1) + ')">&rsaquo;</button>'
            + '<button ' + nd + ' onclick="PA.txnTrace.goPage(' + pages + ')">&raquo;</button>'
            + '</span></div>';
    },

    _loc: function (e) { return e ? (PA.esc(e.proc || '') + (e.line ? ' :' + e.line : '')) : '—'; },
    /** Opens label: show the real start (first DML write, or first savepoint). Only when there is
     *  no recorded write/savepoint before the close do we mark it "(implicit start)". */
    _openLabel: function (s) {
        var e = PA.txnTrace._startEvent(s);
        if (e) return PA.esc(e.proc || '') + ' :' + e.line + ' <span class="txn-op-kind">' + PA.esc(e.kind) + '</span>';
        return PA.esc((s.open && s.open.proc) || '') + ' <span class="txn-implicit">(implicit start)</span>';
    },
    _jumpStart: function (s) {
        var e = PA.txnTrace._startEvent(s);
        return (e && e.nodeId)
            ? 'onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(e.nodeId) + '\',' + (e.line || 0) + ')" style="cursor:pointer"'
            : '';
    },
    _jump: function (e) {
        return (e && e.nodeId)
            ? 'onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(e.nodeId) + '\',' + (e.line || 0) + ')" style="cursor:pointer"'
            : '';
    },

    _tableHtml: function (segs) {
        var html = '<table class="to-table txn-table" style="width:100%"><thead><tr>'
            + '<th>Txn</th><th>Opens</th><th>Savepoints</th><th>Success exit</th><th>Error exit</th><th>Auto</th><th>Ops</th><th></th></tr></thead><tbody>';
        segs.forEach(function (s) {
            // The savepoint WALL made the table unreadable (a busy proc carries 12+ savepoints,
            // each on its own line). A count chip now, the full list expandable beneath the row.
            var spCount = s.savepoints.length;
            var spCell = spCount
                ? '<span class="txn-sp-count" onclick="PA.txnTrace.toggleSpList(\'' + s.id + '\')" title="Click to list the savepoints">' + spCount + ' savepoint' + (spCount === 1 ? '' : 's') + ' &#9662;</span>'
                + '<div class="txn-sp-list" id="txn-sp-list-' + s.id + '" style="display:none">'
                + s.savepoints.map(function (p) {
                    return '<div>' + (p._revert ? '&#8617; rollback to ' : '') + PA.esc(p.name || '(unnamed)') + ' :' + (p.line || '') + '</div>';
                }).join('') + '</div>'
                : '<span style="color:var(--text-muted)">—</span>';
            var commitTxt = s.commit
                ? '<span class="txn-close" style="color:var(--badge-green)">COMMIT</span> ' + PA.txnTrace._loc(s.commit)
                : '<span class="txn-open-flag">no commit</span>';
            var rbTxt = s.rollback
                ? '<span class="txn-close" style="color:var(--red)">ROLLBACK</span> ' + PA.txnTrace._loc(s.rollback)
                : '<span style="color:var(--text-muted)">—</span>';
            html += '<tr class="txn-row">'
                + '<td><span class="txn-tag' + (s.autonomous ? ' auto' : '') + '">' + s.id + '</span></td>'
                + '<td ' + PA.txnTrace._jumpStart(s) + '>' + PA.txnTrace._openLabel(s) + '</td>'
                + '<td>' + spCell + '</td>'
                + '<td ' + PA.txnTrace._jump(s.commit) + '>' + commitTxt + '</td>'
                + '<td ' + PA.txnTrace._jump(s.rollback) + ' title="error / exception handler path">' + rbTxt + '</td>'
                + '<td>' + (s.autonomous ? '<span class="txn-badge-auto">AUTO</span>' : '—') + '</td>'
                + '<td style="text-align:center">' + s.events.length + '</td>'
                + '<td style="text-align:center"><button class="btn btn-sm txn-detail-btn" onclick="PA.txnTrace.openDetail(' + s._idx + ')" title="Show the full call tree of this transaction">Details</button></td>'
                + '</tr>';
        });
        html += '</tbody></table>';
        return html;
    },

    /** Expand/collapse the savepoint list under a table row (the count chip toggles it). */
    toggleSpList: function (id) {
        var el = document.getElementById('txn-sp-list-' + id);
        if (el) el.style.display = (el.style.display === 'none') ? '' : 'none';
    },

    _graphHtml: function (segs) {
        var html = '<div class="txn-graph">';
        segs.forEach(function (s) {
            html += '<div class="txn-track' + (s.autonomous ? ' auto' : '') + '">';
            html += '<div class="txn-track-tag"><span class="txn-tag' + (s.autonomous ? ' auto' : '') + '">' + s.id + '</span>'
                + '<span class="txn-track-proc">' + PA.esc((s.open && s.open.proc) || '') + '</span>'
                + (s.autonomous ? '<span class="txn-badge-auto">AUTO</span>' : '')
                + '<button class="btn btn-sm txn-detail-btn" style="float:right" onclick="PA.txnTrace.openDetail(' + s._idx + ')" title="Show the full call tree of this transaction">Details</button>'
                + '</div>';
            html += '<div class="txn-track-line">';
            // Start (implicit — first observed activity of this transaction)
            html += '<div class="txn-node" ' + PA.txnTrace._jumpStart(s) + ' title="Transaction start">'
                + '<span class="txn-dot begin"></span><span class="txn-node-lbl"><b>BEGIN</b><br>' + PA.txnTrace._openLabel(s) + '</span></div>';
            // The savepoint WALL made the track unreadable — one summary node now, the Details
            // button (or the table view's expandable list) carries the individual points.
            var spReverts = s.savepoints.filter(function (p) { return p._revert; }).length;
            if (s.savepoints.length) {
                html += '<span class="txn-conn"></span>'
                    + '<div class="txn-node" title="' + PA.escAttr(s.savepoints.map(function (p) {
                        return (p._revert ? 'rollback to ' : '') + (p.name || '(unnamed)') + ' :' + (p.line || '');
                    }).join('\n')) + '">'
                    + '<span class="txn-dot sp"></span><span class="txn-node-lbl"><b>SAVEPOINTS</b><br>'
                    + s.savepoints.length + ' mark' + (s.savepoints.length === 1 ? '' : 's')
                    + (spReverts ? ' · ' + spReverts + ' revert' + (spReverts === 1 ? '' : 's') : '')
                    + '</span></div>';
            }
            html += '<span class="txn-conn"></span>';
            // Two mutually-exclusive exits: COMMIT (success) and ROLLBACK (error / handler path).
            if (s.commit || s.rollback) {
                if (s.commit) {
                    html += '<div class="txn-node" ' + PA.txnTrace._jump(s.commit) + ' title="Success path">'
                        + '<span class="txn-dot commit"></span><span class="txn-node-lbl"><b>COMMIT</b><br>' + PA.txnTrace._loc(s.commit) + '<br><span class="txn-exit-tag ok">success</span></span></div>';
                }
                if (s.rollback) {
                    if (s.commit) html += '<span class="txn-conn alt"></span>';
                    html += '<div class="txn-node" ' + PA.txnTrace._jump(s.rollback) + ' title="Error / exception-handler path">'
                        + '<span class="txn-dot rollback"></span><span class="txn-node-lbl"><b>ROLLBACK</b><br>' + PA.txnTrace._loc(s.rollback) + '<br><span class="txn-exit-tag err">on error</span></span></div>';
                }
            } else {
                html += '<div class="txn-node"><span class="txn-dot open"></span><span class="txn-node-lbl"><b>OPEN</b><br>no close in scope</span></div>';
            }
            html += '</div></div>';
        });
        html += '</div>';
        return html;
    },

    // ─────────────────────────────────────────────────────────────────────────
    // Per-transaction "Details" modal: the complete call tree of the procedures
    // involved in this transaction, laid out as a SNAKE (boustrophedon: row 1
    // left→right, row 2 right→left, …) with the transaction points marked on the
    // procedures where they occur.
    // ─────────────────────────────────────────────────────────────────────────

    _lastSegs: null,

    // Per-dialect transaction internals (ACID/CAP/BASE + how COMMIT/ROLLBACK/SAVEPOINT work).
    _DB: {
        ORACLE: {
            name: 'Oracle', model: 'ACID',
            begin: 'Implicit — the first DML statement starts the transaction (no explicit BEGIN).',
            savepoint: 'SAVEPOINT <name>',
            rbToSp: 'ROLLBACK TO <name> — undoes work back to the savepoint, transaction stays open.',
            close: 'COMMIT (persist) / ROLLBACK (discard all)',
            engine: 'Before-images are kept in UNDO segments (used for ROLLBACK and read-consistency); REDO logs are flushed on COMMIT to guarantee durability.',
            isolation: 'READ COMMITTED (default) · SERIALIZABLE available',
            notes: 'Any DDL issues an implicit COMMIT. An AUTONOMOUS transaction (PRAGMA AUTONOMOUS_TRANSACTION) commits/rolls back independently of its caller.'
        },
        SQLSERVER: {
            name: 'SQL Server', model: 'ACID',
            begin: 'BEGIN TRAN / BEGIN TRANSACTION (or implicit under an outer trancount).',
            savepoint: 'SAVE TRANSACTION <name>',
            rbToSp: 'ROLLBACK TRANSACTION <name> — reverts to the save point; the transaction remains open.',
            close: 'COMMIT TRANSACTION / ROLLBACK TRANSACTION (unnamed rolls back to trancount 0 = the whole nest)',
            engine: 'Write-Ahead Logging: every change is logged before the data page is written; ROLLBACK replays the log backwards, COMMIT hardens it.',
            isolation: 'READ COMMITTED (default) · RCSI / SNAPSHOT / SERIALIZABLE available',
            notes: 'Nested BEGIN TRAN only increments @@TRANCOUNT — an inner COMMIT does NOT persist; only the outermost COMMIT does. A bare ROLLBACK undoes the entire nest. SET XACT_ABORT ON auto-rolls-back on error.'
        },
        MYSQL: {
            name: 'MySQL (InnoDB)', model: 'ACID',
            begin: 'START TRANSACTION / BEGIN (autocommit is ON by default — each statement is its own transaction until you start one).',
            savepoint: 'SAVEPOINT <name>',
            rbToSp: 'ROLLBACK TO SAVEPOINT <name> — partial undo, transaction stays open.',
            close: 'COMMIT / ROLLBACK',
            engine: 'InnoDB keeps undo logs (rollback + MVCC) and a redo log (durability). MyISAM is NON-transactional — COMMIT/ROLLBACK are silently ignored there.',
            isolation: 'REPEATABLE READ (default) · READ COMMITTED / SERIALIZABLE available',
            notes: 'DDL (CREATE/ALTER/DROP) causes an implicit COMMIT and cannot be rolled back.'
        },
        DB2: {
            name: 'Db2', model: 'ACID',
            begin: 'Implicit — the first statement of a unit of work starts the transaction.',
            savepoint: 'SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS',
            rbToSp: 'ROLLBACK TO SAVEPOINT <name> — partial undo, transaction stays open.',
            close: 'COMMIT / ROLLBACK',
            engine: 'Changes are written to the transaction logs first; ROLLBACK backs them out, COMMIT makes them durable.',
            isolation: 'CURSOR STABILITY (default) · RR / RS / UR available',
            notes: 'A unit of work ends at COMMIT or ROLLBACK; the next statement implicitly begins a new one.'
        },
        POSTGRES: {
            name: 'PostgreSQL', model: 'ACID (MVCC)',
            begin: 'BEGIN / START TRANSACTION (single statements auto-commit until you open one).',
            savepoint: 'SAVEPOINT <name>',
            rbToSp: 'ROLLBACK TO SAVEPOINT <name> — partial undo, transaction stays open.',
            close: 'COMMIT / ROLLBACK',
            engine: 'Multi-Version Concurrency Control: each row change writes a new tuple version; COMMIT makes versions visible and the WAL guarantees durability, ROLLBACK simply abandons them.',
            isolation: 'READ COMMITTED (default) · REPEATABLE READ / SERIALIZABLE available',
            notes: 'A block that raises an unhandled exception rolls back the whole transaction. Inside PL/pgSQL a BEGIN … EXCEPTION block creates an implicit sub-transaction (savepoint) that can be rolled back on its own.'
        },
        UNKNOWN: {
            name: 'Relational database', model: 'ACID',
            begin: 'The first data change starts the transaction.',
            savepoint: 'SAVEPOINT <name>', rbToSp: 'ROLLBACK TO <name>',
            close: 'COMMIT / ROLLBACK',
            engine: 'Changes are logged before being applied so they can be undone (ROLLBACK) or made durable (COMMIT).',
            isolation: 'Engine default', notes: ''
        }
    },

    _dialect: function () {
        var d = (PA._dbType || (PA.analysisData || {}).dbType || '').toUpperCase();
        return PA.txnTrace._DB[d] || PA.txnTrace._DB.UNKNOWN;
    },

    _findNode: function (node, idU) {
        if (!node) return null;
        if ((node.id || '').toUpperCase() === idU) return node;
        var ch = node.children || [];
        for (var i = 0; i < ch.length; i++) {
            if (node.circular) break;
            var r = PA.txnTrace._findNode(ch[i], idU);
            if (r) return r;
        }
        return null;
    },

    /** The transaction points that belong to THIS segment only (not every txn statement in the host). */
    _segPoints: function (s) {
        var pts = [];
        var st = PA.txnTrace._startEvent(s);
        if (st) pts.push({ cls: 'begin', kind: 'BEGIN · ' + (st.kind || 'write'), line: st.line, nodeId: st.nodeId });
        else if (s.open) pts.push({ cls: 'begin', kind: 'BEGIN (implicit)', line: s.open.line, nodeId: s.open.nodeId });
        (s.savepoints || []).forEach(function (p) {
            pts.push({ cls: 'sp', kind: p._revert ? ('RB→SP ' + (p.name || '')) : ('SAVEPOINT ' + (p.name || '')), line: p.line, nodeId: p.nodeId });
        });
        if (s.commit) pts.push({ cls: 'commit', kind: 'COMMIT (success)', line: s.commit.line, nodeId: s.commit.nodeId });
        if (s.rollback) pts.push({ cls: 'rollback', kind: 'ROLLBACK (on error)', line: s.rollback.line, nodeId: s.rollback.nodeId });
        pts.sort(function (a, b) { return a.line - b.line; });
        return pts;
    },

    /** Merged transaction points for ANY node, from its transaction-control statements
     *  (savepoints + the success COMMIT + the error ROLLBACK). Used for the child procs. */
    _nodePoints: function (list) {
        var pts = [];
        (list || []).forEach(function (t) {
            if (t.type === 'SAVEPOINT') pts.push({ cls: 'sp', kind: 'SAVEPOINT ' + (t.name || ''), line: t.line, nodeId: t.nodeId });
            else if (t.type === 'ROLLBACK' && t.name) pts.push({ cls: 'sp', kind: 'RB→SP ' + t.name, line: t.line, nodeId: t.nodeId });
            else if (t.type === 'COMMIT') pts.push({ cls: 'commit', kind: 'COMMIT (success)', line: t.line, nodeId: t.nodeId });
            else if (t.type === 'ROLLBACK') pts.push({ cls: 'rollback', kind: 'ROLLBACK (on error)', line: t.line, nodeId: t.nodeId });
        });
        pts.sort(function (a, b) { return a.line - b.line; });
        return pts;
    },

    /** The FULL call tree of the transaction's host procedure, so you can see how it calls its
     *  children. Each proc shows its own DML operations + transaction points; a child that opens
     *  its OWN transaction is tagged ("own txn T#") but still expanded. Pure read-only branches
     *  (functions with no writes anywhere below them) are pruned and just counted. */
    _txnProcs: function (s) {
        var tree = PA.callTrace && PA.callTrace.treeData;
        var hostU = ((s.close && s.close.nodeId) || (s.open && s.open.nodeId) || (s.start && s.start.nodeId) || '').toUpperCase();
        var hostNode = (tree && hostU) ? PA.txnTrace._findNode(tree, hostU) : null;

        // Transaction-control statements grouped by node, + which procs open their own transaction.
        var stmtByNode = {}, txnHost = {};
        ((PA.analysisData || {}).transactionStatements || []).forEach(function (t) {
            var u = (t.nodeId || '').toUpperCase();
            (stmtByNode[u] = stmtByNode[u] || []).push(t);
            if (t.type === 'COMMIT' || (t.type === 'ROLLBACK' && !t.name)) txnHost[u] = 1;
        });
        var segByHost = {};
        (PA.txnTrace._lastSegs || []).forEach(function (seg) {
            var h = ((seg.close && seg.close.nodeId) || (seg.open && seg.open.nodeId) || '').toUpperCase();
            if (h) (segByHost[h] = segByHost[h] || []).push(seg.id);
        });

        var writesByNode = PA.txnTrace._writesByNode || {};
        var writesByProc = PA.txnTrace._writesByProc || {};
        var opsFor = function (o) { return (writesByNode[o.id] || writesByProc[(o.name || '').toUpperCase()] || []).slice(); };

        var seen = {}, readOnly = 0;
        var loLine = ((PA.txnTrace._startEvent(s) || {}).line) || (s.open && s.open.line) || 0;
        var hiLine = (s.close && s.close.line) || Number.MAX_SAFE_INTEGER;

        var walk = function (node, depth, isHost) {
            var u = (node.id || '').toUpperCase();
            if (seen[u]) return [];
            seen[u] = 1;
            var rec = { id: u, name: node.name || node.id, depth: depth };

            if (isHost) {
                rec.isHost = true;
                rec.points = PA.txnTrace._segPoints(s);
                rec.ops = opsFor(rec).filter(function (w) { return w.line >= loLine && w.line <= hiLine; }); // only writes in this txn's window
            } else {
                rec.points = PA.txnTrace._nodePoints(stmtByNode[u]);
                rec.ops = opsFor(rec);
                if (txnHost[u]) { rec.ownTxn = true; rec.nestedIds = segByHost[u] || []; }   // opens its own transaction — tag it
            }

            var childRecs = [];
            if (!node.circular) (node.children || []).forEach(function (c) { childRecs = childRecs.concat(walk(c, depth + 1, false)); });

            var relevant = isHost || (rec.ops && rec.ops.length) || (rec.points && rec.points.length) || childRecs.length;
            if (!relevant) { readOnly++; return []; }        // pure read-only leaf → prune + count
            return [rec].concat(childRecs);
        };

        var out;
        if (hostNode) out = walk(hostNode, 0, true);
        else out = [{ id: hostU, name: (s.open && s.open.proc) || hostU, depth: 0, isHost: true, points: PA.txnTrace._segPoints(s), ops: [] }];

        return { procs: out, hostU: hostU, readOnly: readOnly };
    },

    _detailIdx: null,
    _fullscreen: false,

    openDetail: function (idx) {
        if (!(PA.txnTrace._lastSegs || [])[idx]) return;
        PA.txnTrace._detailIdx = idx;
        PA.txnTrace._fullscreen = false;

        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'txnDetailOverlay';
        overlay.onclick = function (e) { if (e.target === overlay) PA.txnTrace._closeDetail(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal txn-detail-modal';
        modal.id = 'txnDetailModal';
        modal.onclick = function (e) { e.stopPropagation(); };

        modal.innerHTML = PA.txnTrace._detailHtml();
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        requestAnimationFrame(function () { overlay.classList.add('open'); });  // fade-in (opacity:0 → 1)
    },

    /** Toggle the modal between the default (3-per-row) size and full screen (5-per-row). */
    _toggleFull: function () {
        PA.txnTrace._fullscreen = !PA.txnTrace._fullscreen;
        var modal = document.getElementById('txnDetailModal');
        if (!modal) return;
        modal.classList.toggle('fullscreen', PA.txnTrace._fullscreen);
        modal.innerHTML = PA.txnTrace._detailHtml();   // re-render snake with the new per-row count
    },

    /** Builds the whole modal (header + body). Re-run on fullscreen toggle. */
    _detailHtml: function () {
        var s = (PA.txnTrace._lastSegs || [])[PA.txnTrace._detailIdx];
        if (!s) return '';
        var res = PA.txnTrace._txnProcs(s);
        var procs = res.procs;
        var db = PA.txnTrace._dialect();
        var full = PA.txnTrace._fullscreen;
        var writes = procs.reduce(function (n, p) { return n + (p.ops ? p.ops.length : 0); }, 0);

        var html = '<div class="tds-header"><div class="tds-header-left">'
            + '<span class="txn-tag' + (s.autonomous ? ' auto' : '') + '">' + s.id + '</span>'
            + '<span style="font-weight:700;margin-left:8px">Transaction call tree &amp; internals</span>'
            + (s.autonomous ? '<span class="txn-badge-auto" style="margin-left:6px">AUTONOMOUS</span>' : '')
            + '<span class="txn-db-chip">' + PA.esc(db.name) + ' · ' + PA.esc(db.model) + '</span>'
            + '</div><div class="tds-header-btns">'
            + '<button class="btn btn-sm tds-close" title="' + (full ? 'Restore' : 'Full screen') + '" onclick="PA.txnTrace._toggleFull()">' + (full ? '&#10064;' : '&#10063;') + '</button>'
            + '<button class="btn btn-sm tds-close" title="Close" onclick="PA.txnTrace._closeDetail()">&times;</button>'
            + '</div></div>';

        html += '<div class="tds-body" style="max-height:' + (full ? '90vh' : '78vh') + ';overflow:auto">';
        html += PA.txnTrace._detailSummaryHtml(s, db, procs, writes);
        html += '<div class="tds-section-title" style="margin-top:6px">Procedures in <b>this</b> transaction — with their operations &amp; boundary points (execution order, snake). Procs that open their own transaction are marked and not expanded.'
            + (res.readOnly ? ' <span style="font-weight:500;color:var(--text-muted)">(' + res.readOnly + ' read-only call' + (res.readOnly === 1 ? '' : 's') + ' with no writes hidden)</span>' : '')
            + '</div>';
        html += '<div class="txn-legend">'
            + '<span><i class="txn-dot begin"></i> transaction start</span>'
            + '<span><i class="txn-dot sp"></i> savepoint</span>'
            + '<span><i class="txn-dot commit"></i> commit</span>'
            + '<span><i class="txn-dot rollback"></i> rollback</span>'
            + '<span><i class="txn-op-swatch"></i> DML write</span>'
            + '<span><i class="txn-nested-swatch"></i> calls its own transaction</span></div>';
        html += PA.txnTrace._serpentineHtml(procs, res.hostU);
        html += '</div>';
        return html;
    },

    /** Compact, plain-language summary: which kind of transaction (ACID/BASE), the totals,
     *  and a single one-line outcome — kept simple and readable, not a wall of text. */
    _detailSummaryHtml: function (s, db, procs, writes) {
        var st = PA.txnTrace._startEvent(s);
        var sp = (s.savepoints || []).length;

        var exitChips = '';
        if (s.commit) exitChips += '<span class="txn-chip2 commit">COMMIT :' + s.commit.line + ' (success)</span>';
        if (s.rollback) exitChips += '<span class="txn-chip2 rollback">ROLLBACK :' + s.rollback.line + ' (on error)</span>';
        if (!s.commit && !s.rollback) exitChips += '<span class="txn-chip2 open">OPEN — no close</span>';

        var chips = '<div class="txn-sum2">'
            + '<span class="txn-chip2 model" title="Transaction guarantee">' + PA.esc(db.model) + '</span>'
            + exitChips
            + '<span class="txn-chip2">' + procs.length + ' proc' + (procs.length === 1 ? '' : 's') + '</span>'
            + '<span class="txn-chip2">' + writes + ' operation' + (writes === 1 ? '' : 's') + '</span>'
            + (sp ? '<span class="txn-chip2 sp">' + sp + ' savepoint' + (sp === 1 ? '' : 's') + '</span>' : '')
            + (db.name ? '<span class="txn-chip2 db">' + PA.esc(db.name) + '</span>' : '')
            + '</div>';

        var begin = st ? (PA.esc(st.proc) + ' :' + st.line + ' (' + PA.esc(st.kind || 'first write') + ')') : 'the first data change';
        var line = 'Begins at <b>' + begin + '</b> and runs across <b>' + procs.length + '</b> procedure' + (procs.length === 1 ? '' : 's') + ' (<b>' + writes + '</b> operation' + (writes === 1 ? '' : 's') + '). ';
        if (s.commit && s.rollback) line += 'On success it <b class="txn-good">COMMITs</b> (:' + s.commit.line + ') — all changes become permanent; if an error is raised, the handler <b class="txn-bad">ROLLs BACK</b> (:' + s.rollback.line + ') and nothing is saved.';
        else if (s.commit) line += 'It <b class="txn-good">COMMITs</b> at :' + s.commit.line + ', making all changes permanent and visible to other sessions.';
        else if (s.rollback) line += 'It <b class="txn-bad">ROLLs BACK</b> at :' + s.rollback.line + ' — every change is discarded.';
        else line += 'There is no COMMIT/ROLLBACK in scope — the caller decides the outcome.';
        if (sp) line += ' Savepoints let it undo part of the work without losing everything.';

        var dbline = '<b>' + PA.esc(db.name) + '</b> is an <b>' + PA.esc(db.model) + '</b> engine — ' + PA.esc(db.engine)
            + (db.isolation ? ' Isolation: ' + PA.esc(db.isolation) + '.' : '');

        return chips
            + '<div class="txn-resolve-line">' + line + '</div>'
            + '<div class="txn-db-line">' + dbline + '</div>';
    },

    _closeDetail: function () {
        var o = document.getElementById('txnDetailOverlay');
        if (o && o.parentNode) o.parentNode.removeChild(o);
    },

    /** Snake layout: 3 cards per row; rows alternate left→right / right→left. The rows live in a
     *  fixed-height scroll box with ▲/▼ buttons, so a long call tree stays contained and scrollable. */
    _serpentineHtml: function (procs, hostU) {
        if (!procs.length) return '<div class="empty-msg">No procedures resolved for this transaction.</div>';
        var perRow = PA.txnTrace._fullscreen ? 5 : 3;   // full screen fits 5 per row, default 3
        // Grid template: N equal card columns with 30px arrow columns between them.
        var tmpl = '1fr'; for (var t = 1; t < perRow; t++) tmpl += ' 30px 1fr';
        var turnPad = 'calc(' + (50 / perRow).toFixed(2) + '% - 10px)';

        var rows = [];
        for (var i = 0; i < procs.length; i += perRow) rows.push(procs.slice(i, i + perRow));
        var empty = '<div class="txn-snake-empty"></div>';
        var inner = '';
        rows.forEach(function (row, ri) {
            var rtl = (ri % 2 === 1);
            // Fixed column-slots so every row lines up. Right→left rows keep the SAME columns but
            // reversed order, and are padded on the left so the flow starts under the previous row's end.
            var slots;
            if (rtl) { slots = []; while (slots.length < perRow - row.length) slots.push(null); slots = slots.concat(row.slice().reverse()); }
            else { slots = row.slice(); while (slots.length < perRow) slots.push(null); }

            inner += '<div class="txn-snake-row" style="grid-template-columns:' + tmpl + '">';
            for (var c = 0; c < perRow; c++) {
                inner += slots[c] ? PA.txnTrace._snakeCard(slots[c], hostU) : empty;
                if (c < perRow - 1) {
                    var both = slots[c] && slots[c + 1];
                    inner += '<span class="txn-snake-arrow">' + (both ? (rtl ? '&larr;' : '&rarr;') : '') + '</span>';
                }
            }
            inner += '</div>';
            if (ri < rows.length - 1) inner += '<div class="txn-snake-turn ' + (rtl ? 'left' : 'right') + '" style="' + (rtl ? 'padding-left:' : 'padding-right:') + turnPad + '">&#8623;</div>';
        });
        // Only show the scroll buttons when there's more than a couple of rows to scroll through.
        var scrollable = rows.length > 2;
        return '<div class="txn-snake-wrap">'
            + (scrollable ? '<button class="txn-snake-btn up" onclick="PA.txnTrace._scrollSnake(-1)" title="Scroll up">&#9650;</button>' : '')
            + '<div class="txn-snake" id="txnSnakeBox">' + inner + '</div>'
            + (scrollable ? '<button class="txn-snake-btn down" onclick="PA.txnTrace._scrollSnake(1)" title="Scroll down">&#9660;</button>' : '')
            + '</div>';
    },

    _scrollSnake: function (dir) {
        var el = document.getElementById('txnSnakeBox');
        if (el) el.scrollBy({ top: dir * Math.max(120, el.clientHeight * 0.7), behavior: 'smooth' });
    },

    _snakeCard: function (p, hostU) {
        var points = (p.points || []).map(function (m) {
            var jump = m.nodeId ? 'onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(m.nodeId) + '\',' + (m.line || 0) + ')" style="cursor:pointer"' : '';
            return '<div class="txn-mark" ' + jump + '><span class="txn-dot ' + m.cls + '"></span>'
                + '<span class="txn-mark-lbl">' + PA.esc(m.kind) + (m.line ? ' :' + m.line : '') + '</span></div>';
        }).join('');

        var ops = p.ops || [];
        var opsHtml = '';
        if (ops.length) {
            var shown = ops.slice(0, 6).map(function (w) {
                var jump = w.nodeId ? 'onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(w.nodeId) + '\',' + (w.line || 0) + ')" style="cursor:pointer"' : '';
                return '<span class="txn-op-chip ' + w.op + '" ' + jump + ' title="' + PA.esc(w.op + (w.table ? ' ' + w.table : '')) + '">' + w.op.slice(0, 3) + ' :' + w.line + '</span>';
            }).join('');
            opsHtml = '<div class="txn-snake-ops">' + shown + (ops.length > 6 ? '<span class="txn-op-more">+' + (ops.length - 6) + '</span>' : '') + '</div>';
        }

        var isHost = (p.id === hostU);
        var cls = 'txn-snake-card' + (isHost ? ' host' : '') + (p.ownTxn ? ' nested' : '') + ((p.points && p.points.length) ? ' has-txn' : '');
        var jumpProc = p.id ? 'onclick="PA.sourceView.openModal(\'' + PA.escJs(p.id) + '\')" style="cursor:pointer"' : '';
        var ownTag = p.ownTxn ? '<span class="txn-own-badge" title="Opens its own transaction">own txn' + (p.nestedIds && p.nestedIds.length ? ' ' + p.nestedIds.join(',') : '') + '</span>' : '';
        return '<div class="' + cls + '">'
            + '<div class="txn-snake-head">'
            + '<span class="txn-snake-depth">d' + (p.depth != null ? p.depth : '?') + '</span>'
            + '<span class="txn-snake-name" ' + jumpProc + ' title="Open source">' + PA.esc(String(p.name).replace(/\$/g, '.')) + '</span>'
            + (isHost ? '<span class="txn-host-badge">host</span>' : ownTag)
            + '</div>'
            + (points ? '<div class="txn-snake-marks">' + points + '</div>' : '')
            + opsHtml
            + (!points && !opsHtml ? '<div class="txn-snake-nomark">no writes / txn points</div>' : '')
            + '</div>';
    }
};
