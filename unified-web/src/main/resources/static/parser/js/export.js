window.PA = window.PA || {};

PA.export = {
    _fmt: 'xlsx',

    show() {
        var modal = document.getElementById('exportModal');
        if (!modal) return;
        var info = document.getElementById('exportInfo');
        if (info && PA.analysisData) {
            var d = PA.analysisData;
            info.innerHTML =
                '<div style="font-weight:700;color:var(--accent);margin-bottom:6px;font-size:13px">' + PA.esc(d.entryPoint || d.name || '') + '</div>' +
                '<div style="display:flex;gap:14px;flex-wrap:wrap;font-size:11px;color:var(--text-muted)">' +
                '<span>&var(--badge-teal); ' + (d.totalNodes || 0) + ' procedures</span>' +
                '<span>&#128200; ' + (d.totalTables || 0) + ' tables</span>' +
                '<span>&#128279; ' + (d.totalEdges || 0) + ' edges</span>' +
                '<span>&#128196; ' + (d.totalLinesOfCode || 0).toLocaleString() + ' LOC</span>' +
                '</div>';
        }
        // Default scope radio to current active scope
        var curScope = PA._scope || 'full';
        var scopeMap = { full: 'expScopeFull', subtree: 'expScopeSub', subflow: 'expScopeSubflow', direct: 'expScopeDirect' };
        var scopeEl = document.getElementById(scopeMap[curScope] || 'expScopeFull');
        if (scopeEl) scopeEl.checked = true;
        PA.export._setStatus('');
        PA.export.setFormat('xlsx');
        modal.style.display = '';
    },

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        var m = document.getElementById('exportModal');
        if (m) m.style.display = 'none';
    },

    setFormat(fmt) {
        PA.export._fmt = fmt;
        ['json','csv','xlsx','zip'].forEach(function(f) {
            var el = document.getElementById('expFmt' + f.charAt(0).toUpperCase() + f.slice(1));
            if (el) el.classList.toggle('active', f === fmt);
        });
    },

    _setStatus(msg, type) {
        var el = document.getElementById('expStatus');
        if (!el) return;
        el.textContent = msg;
        el.className = 'exp-status' + (type ? ' ' + type : '');
    },

    async download() {
        if (!PA.analysisData) { PA.export._setStatus('No analysis loaded.', 'err'); return; }
        // Ensure analysis name is set in case setAnalysis was never called after reload
        if (!PA.api._analysisName && PA.analysisData.name) PA.api.setAnalysis(PA.analysisData.name);
        var sections = {
            summary:    document.getElementById('expSummary')    ? document.getElementById('expSummary').checked    : true,
            procs:      document.getElementById('expProcs')      ? document.getElementById('expProcs').checked      : true,
            tables:     document.getElementById('expTables')     ? document.getElementById('expTables').checked     : true,
            joins:      document.getElementById('expJoins')      ? document.getElementById('expJoins').checked      : true,
            cursors:    document.getElementById('expCursors')    ? document.getElementById('expCursors').checked    : true,
            sequences:  document.getElementById('expSequences')  ? document.getElementById('expSequences').checked  : true,
            callGraph:  document.getElementById('expCallGraph')  ? document.getElementById('expCallGraph').checked  : true,
            complexity: document.getElementById('expComplexity') ? document.getElementById('expComplexity').checked : true,
        };
        var detailLevel = document.querySelector('input[name="expDetail"]:checked');
        var isDetail = !detailLevel || detailLevel.value === 'detail';
        var btn = document.getElementById('expDownloadBtn');
        if (btn) btn.disabled = true;
        PA.export._setStatus('Gathering data…');
        try {
            // UI-040: "Download as ZIP" is a genuinely separate mode (one server-streamed zip via
            // UI-042's bundle endpoint), not another branch of the existing JSON/CSV/XLSX
            // client-side gather-and-build path below — that path stays completely untouched.
            if (PA.export._fmt === 'zip') {
                await PA.export._downloadZipBundle(sections);
                PA.export._setStatus('ZIP export complete ✓', 'ok');
                return;
            }
            var data = await PA.export._gather(sections);
            PA.export._setStatus('Building export…');
            if (PA.export._fmt === 'xlsx')      PA.export._downloadXLSX(data, sections, isDetail);
            else if (PA.export._fmt === 'json') PA.export._downloadJSON(data);
            else                                PA.export._downloadCSV(data, sections, isDetail);
            PA.export._setStatus('Export complete ✓', 'ok');
        } catch (e) {
            PA.export._setStatus('Export failed: ' + (e.message || e), 'err');
            console.error('Export error', e);
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    /**
     * UI-040/UI-042 — "Download as ZIP" mode. POSTs the same section checkboxes the existing
     * JSON/CSV/XLSX path reads (mapped onto the bundle endpoint's facet names) plus the new
     * "Include source code" checkbox, to the new shared server-side zip endpoint, and saves the
     * streamed response as one file. No client-side gathering/zipping — the server does the work
     * (see the endpoint's own doc comment for why), so this function is deliberately thin.
     */
    async _downloadZipBundle(sections) {
        var name = PA.api._analysisName;
        if (!name) throw new Error('No analysis loaded.');
        var facets = [];
        if (sections.procs)     facets.push('procedures');
        if (sections.tables)    facets.push('tables');
        if (sections.joins)     facets.push('joins');
        if (sections.callGraph) facets.push('callGraph');
        if (sections.summary)   facets.push('index');
        var srcEl = document.getElementById('expSourceCode');
        var includeSources = srcEl ? srcEl.checked : false;
        if (!facets.length && !includeSources) {
            throw new Error('Nothing selected — check at least one sheet or "Include source code".');
        }
        var res = await fetch('/api/parser/analyses/' + encodeURIComponent(name) + '/export/bundle', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ facets: facets, sources: includeSources })
        });
        if (!res.ok) throw new Error('Bundle request failed (HTTP ' + res.status + ')');
        var blob = await res.blob();
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = name.replace(/~/g, '_') + '-export.zip';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(function() { URL.revokeObjectURL(url); }, 1000);
    },

    async _gather(sections) {
        // Determine export scope from the modal radio
        var scopeEl = document.querySelector('input[name="expScope"]:checked');
        var exportScope = scopeEl ? scopeEl.value : 'full';

        // Build the set of node IDs in scope (null = all nodes)
        var scopedNodeIds = null;
        if (exportScope !== 'full' && PA.context && PA.context.procId) {
            var savedScope = PA._scope;
            PA._scope = exportScope;
            var scopedNodes = PA.getScopedNodes ? await PA.getScopedNodes() : null;
            PA._scope = savedScope;
            if (scopedNodes) {
                scopedNodeIds = new Set(scopedNodes.map(function(n) {
                    return (n.nodeId || n.name || '').toUpperCase();
                }));
            }
        }

        // Filter helper: keep rows in scope AND rebuild their accumulated inner fields
        // (accessDetails / accessCount) so a scoped export reflects only the scoped procedures,
        // not the whole dataset. Produces NEW row objects — never mutates the fetched aggregate.
        var filterByScope = function(rows) {
            if (!scopedNodeIds || !Array.isArray(rows)) return rows;
            return rows.reduce(function(out, r) {
                var pn = (r.procedureName || r.procedure || r.owner || '').toUpperCase();
                var selfInScope = pn && scopedNodeIds.has(pn);
                var dets = Array.isArray(r.accessDetails) ? r.accessDetails : null;
                // No accessor info on this row → cannot scope it by who-accesses; keep it as-is
                // (dropping would make legitimately-relevant rows silently vanish).
                if (!dets) { out.push(r); return out; }
                var scopedDets = dets.filter(function(ad) {
                    return scopedNodeIds.has((ad.procedureName || '').toUpperCase());
                });
                // Row has accessors: keep if it is itself in scope, or ≥1 accessor is in scope.
                if (!selfInScope && scopedDets.length === 0) return out;
                // Rebuild accumulated fields scoped to this export (new object, no mutation).
                out.push(Object.assign({}, r, { accessDetails: scopedDets, accessCount: scopedDets.length }));
                return out;
            }, []);
        };

        // Scoped nodes list used by Call Flow / Complexity / Summary sheets.
        // UI-024 (§112) — fetched fresh from /procedures/export rather than read off
        // PA.nodeIndex.all() (which requires the client-side node index to already be fully
        // resident): the same "always fetch fresh" reasoning already applied to joins/tables/
        // cursors/sequences above, now applied here too. This was the export half of the row's
        // remaining gap — the other consumer files are already converted (§92-99).
        var allNodes = await PA.api.getProceduresForExport();
        if (!Array.isArray(allNodes)) allNodes = [];
        var scopedNodes = scopedNodeIds
            ? allNodes.filter(function(n) {
                return scopedNodeIds.has((n.nodeId || n.name || '').toUpperCase());
              })
            : allNodes;

        var result = {
            meta: PA.analysisData,
            exportScope: exportScope,
            scopedNodeIds: scopedNodeIds,
            nodes: scopedNodes
        };

        // Always fetch fresh data from the API for export to avoid stale tab state
        // (e.g., a previous analysis's tab data still cached in PA.*.data).
        try { result.joins = filterByScope(await PA.api.getJoinOperations()); } catch(e) { console.warn('[Export] joins fetch failed', e); result.joins = []; }
        if (!Array.isArray(result.joins)) result.joins = [];

        try { result.tables = filterByScope(await PA.api.getTableOperations()); } catch(e) { console.warn('[Export] tables fetch failed', e); result.tables = []; }
        if (!Array.isArray(result.tables)) result.tables = [];

        if (sections.cursors) {
            try { result.cursors = filterByScope(await PA.api.getCursorOperations()); } catch(e) { console.warn('[Export] cursors fetch failed', e); result.cursors = []; }
            if (!Array.isArray(result.cursors)) result.cursors = [];
        }
        if (sections.sequences) {
            try { result.sequences = filterByScope(await PA.api.getSequenceOperations()); } catch(e) { console.warn('[Export] sequences fetch failed', e); result.sequences = []; }
            if (!Array.isArray(result.sequences)) result.sequences = [];
        }
        if (sections.callGraph) {
            try { result.callGraph = await PA.api.getCallGraph(); } catch(e) { console.warn('[Export] callGraph fetch failed', e); result.callGraph = null; }
        }
        return result;
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // XLSX CORE
    // ═══════════════════════════════════════════════════════════════════════════

    // Per-sheet palette: even row bg / odd row bg / number text color / accent color / border color
    _PAL: {
        summary:   { e: 'EEF2FF', o: 'E0E7FF', n: '3730A3', a: '4F46E5', b: 'A5B4FC' },
        callFlow:  { e: 'ECFDF5', o: 'D1FAE5', n: '065F46', a: '059669', b: '6EE7B7' },
        tableOps:  { e: 'EFF6FF', o: 'DBEAFE', n: '1E40AF', a: '2563EB', b: '93C5FD' },
        joins:     { e: 'FFFBEB', o: 'FEF3C7', n: '92400E', a: 'D97706', b: 'FCD34D' },
        cursors:   { e: 'FDF2F8', o: 'FCE7F3', n: '9D174D', a: 'DB2777', b: 'F9A8D4' },
        sequences: { e: 'F0FDFA', o: 'CCFBF1', n: '134E4A', a: '0D9488', b: '5EEAD4' },
        callGraph: { e: 'F5F3FF', o: 'EDE9FE', n: '5B21B6', a: '7C3AED', b: 'C4B5FD' },
        complexity:{ e: 'FFF7F7', o: 'FEE2E2', n: 'B91C1C', a: 'DC2626', b: 'FCA5A5' },
    },

    // Depth palette for Call Flow — each depth level gets its OWN full-row color
    _DEPTH_PAL: [
        { e: 'BFDBFE', o: 'BFDBFE', fg: '1E3A8A', bold: true  },   // 0 — vivid blue
        { e: 'BBF7D0', o: 'BBF7D0', fg: '14532D', bold: true  },   // 1 — vivid green
        { e: 'FDE68A', o: 'FDE68A', fg: '78350F', bold: false },   // 2 — vivid amber
        { e: 'DDD6FE', o: 'DDD6FE', fg: '4C1D95', bold: false },   // 3 — vivid purple
        { e: 'FBCFE8', o: 'FBCFE8', fg: '831843', bold: false },   // 4 — vivid pink
        { e: 'FED7AA', o: 'FED7AA', fg: '7C2D12', bold: false },   // 5 — vivid orange
        { e: 'A7F3D0', o: 'A7F3D0', fg: '064E3B', bold: false },   // 6 — vivid teal
        { e: 'E9D5FF', o: 'E9D5FF', fg: '3B0764', bold: false },   // 7+ — vivid violet
    ],

    // Risk full-row colors for Complexity
    _RISK_ROW: {
        CRITICAL: { e: 'FEE2E2', o: 'FECACA', fg: '991B1B', badge: { bg: 'DC2626', fg: 'FFFFFF' } },
        HIGH:     { e: 'FEF3C7', o: 'FDE68A', fg: '92400E', badge: { bg: 'F59E0B', fg: 'FFFFFF' } },
        MEDIUM:   { e: 'DBEAFE', o: 'BFDBFE', fg: '1E40AF', badge: { bg: '3B82F6', fg: 'FFFFFF' } },
        LOW:      { e: 'DCFCE7', o: 'BBF7D0', fg: '166534', badge: { bg: '22C55E', fg: 'FFFFFF' } },
    },

    // Op badge colors — vivid, punchy
    _OP_BADGE: {
        SELECT:  { bg: '16A34A', fg: 'FFFFFF' },
        INSERT:  { bg: '2563EB', fg: 'FFFFFF' },
        UPDATE:  { bg: 'D97706', fg: 'FFFFFF' },
        DELETE:  { bg: 'DC2626', fg: 'FFFFFF' },
        DYNAMIC: { bg: '7C3AED', fg: 'FFFFFF' },
        COMMIT:  { bg: '059669', fg: 'FFFFFF' },
        MERGE:   { bg: 'EA580C', fg: 'FFFFFF' },
        NEXTVAL: { bg: '0284C7', fg: 'FFFFFF' },
        CURRVAL: { bg: '0284C7', fg: 'FFFFFF' },
        EXPLICIT:{ bg: '7C3AED', fg: 'FFFFFF' },
        IMPLICIT:{ bg: '64748B', fg: 'FFFFFF' },
        CURSOR:  { bg: 'DB2777', fg: 'FFFFFF' },
        JOIN:    { bg: 'D97706', fg: 'FFFFFF' },
        INNER:   { bg: '2563EB', fg: 'FFFFFF' },
        LEFT:    { bg: '059669', fg: 'FFFFFF' },
        RIGHT:   { bg: 'DC2626', fg: 'FFFFFF' },
        FULL:    { bg: '7C3AED', fg: 'FFFFFF' },
        CROSS:   { bg: 'D97706', fg: 'FFFFFF' },
    },

    // Normalize a 6-char hex to 8-char ARGB (xlsx-style requirement).
    // 8-char values and anything else are passed through unchanged.
    _rgb(c) { return c && c.length === 6 ? 'FF' + c : (c || 'FFD1D5DB'); },

    // Solid fill object that xlsx-style actually writes (bgColor indexed:64 is required).
    _fill(c) {
        return { patternType: 'solid', fgColor: { rgb: PA.export._rgb(c) }, bgColor: { indexed: 64 } };
    },

    _brd(c) {
        var rc = PA.export._rgb(c || 'D1D5DB');
        return {
            top:    { style: 'thin', color: { rgb: rc } },
            bottom: { style: 'thin', color: { rgb: rc } },
            left:   { style: 'thin', color: { rgb: rc } },
            right:  { style: 'thin', color: { rgb: rc } },
        };
    },

    // Themed cell using per-sheet palette
    _tc(ri, pal, align, bold) {
        return {
            fill:      PA.export._fill(ri % 2 === 0 ? pal.e : pal.o),
            font:      { color: { rgb: 'FF0F172A' }, sz: 9, name: 'Calibri', bold: !!bold },
            alignment: { horizontal: align || 'left', vertical: 'top', wrapText: false },
            border:    PA.export._brd(pal.b),
        };
    },

    // Themed number cell
    _tn(ri, pal) {
        return {
            fill:      PA.export._fill(ri % 2 === 0 ? pal.e : pal.o),
            font:      { color: { rgb: PA.export._rgb(pal.n) }, sz: 9, bold: true, name: 'Calibri' },
            alignment: { horizontal: 'right', vertical: 'top' },
            border:    PA.export._brd(pal.b),
        };
    },

    // Op badge cell
    _ob(opName, ri, pal) {
        var c = PA.export._OP_BADGE[opName] || PA.export._OP_BADGE[(opName || '').toUpperCase()];
        if (!c) return PA.export._tc(ri, pal, 'center');
        return {
            fill:      PA.export._fill(c.bg),
            font:      { color: { rgb: PA.export._rgb(c.fg) }, sz: 9, bold: true, name: 'Calibri' },
            alignment: { horizontal: 'center', vertical: 'top' },
            border:    PA.export._brd(c.bg),
        };
    },

    // Header cell using tab color
    _hc(tabColor) {
        return {
            font:      { bold: true, color: { rgb: 'FFFFFFFF' }, sz: 11, name: 'Calibri' },
            fill:      PA.export._fill(tabColor),
            alignment: { horizontal: 'center', vertical: 'center', wrapText: false },
            border:    PA.export._brd('FFFFFF'),
        };
    },

    // Section divider cell
    _sc(tabColor, pal) {
        return {
            fill:      PA.export._fill(pal.o),
            font:      { bold: true, sz: 9, color: { rgb: PA.export._rgb(tabColor) }, name: 'Calibri' },
            border:    PA.export._brd(pal.b),
            alignment: { horizontal: 'left' },
        };
    },

    // Row with full color (for depth / risk)
    _rowCell(bg, fg, align, bold, border) {
        return {
            fill:      PA.export._fill(bg),
            font:      { color: { rgb: PA.export._rgb(fg) }, sz: 9, bold: !!bold, name: 'Calibri' },
            alignment: { horizontal: align || 'left', vertical: 'top', wrapText: false },
            border:    PA.export._brd(border || bg),
        };
    },

    _makeWs(headers) {
        var ws  = {};
        var ref = { s: { r: 0, c: 0 }, e: { r: 0, c: headers.length - 1 } };
        ws['!ref_obj'] = ref;
        headers.forEach(function(h, c) {
            ws[XLSX.utils.encode_cell({ r: 0, c: c })] = { v: h, t: 's' };
        });
        return ws;
    },

    _sr(ws, ri, values, styles, ref) {
        values.forEach(function(val, c) {
            var addr = XLSX.utils.encode_cell({ r: ri, c: c });
            var cell = { v: val == null ? '' : val, t: typeof val === 'number' ? 'n' : 's' };
            if (styles && styles[c]) cell.s = styles[c];
            ws[addr] = cell;
            if (ref.e.r < ri) ref.e.r = ri;
            if (ref.e.c < c)  ref.e.c = c;
        });
    },

    _seal(ws) {
        ws['!ref'] = XLSX.utils.encode_range(ws['!ref_obj']);
        delete ws['!ref_obj'];
    },

    // Excel hard-limits a cell to 32,767 chars; clamp strings at 32,000 (headroom for the
    // marker) so the XLSX writer never throws. Marker reports exact chars removed and is kept
    // in sync with the backend (ExcelStyleHelper.clampCell).
    _MAX_CELL: 32000,
    _clampStr(v) {
        if (typeof v !== 'string' || v.length <= PA.export._MAX_CELL) return v;
        return v.substring(0, PA.export._MAX_CELL)
            + '... (+' + (v.length - PA.export._MAX_CELL) + ' more chars truncated)';
    },
    /** Chokepoint: clamp every string cell (.v) in a worksheet right before it is appended. */
    _sanitizeWs(ws) {
        if (!ws) return ws;
        Object.keys(ws).forEach(function(k) {
            if (k.charAt(0) === '!') return;
            var cell = ws[k];
            if (cell && cell.t === 's' && typeof cell.v === 'string') {
                cell.v = PA.export._clampStr(cell.v);
            }
        });
        return ws;
    },

    _finish(wb, ws, name, headers, widths, tabColor) {
        var hStyle = PA.export._hc(tabColor);
        headers.forEach(function(_, c) {
            var a = XLSX.utils.encode_cell({ r: 0, c: c });
            if (ws[a]) ws[a].s = hStyle;
        });
        ws['!cols']      = widths.map(function(w) { return { wch: w }; });
        ws['!sheetView'] = [{ state: 'frozen', ySplit: 1 }];
        PA.export._sanitizeWs(ws);
        XLSX.utils.book_append_sheet(wb, ws, name);
        if (wb.Sheets[name]) wb.Sheets[name]['!tabColor'] = { rgb: tabColor };
    },

    // ── XLSX top-level ────────────────────────────────────────────────────────

    _downloadXLSX(data, sections, isDetail) {
        if (typeof XLSX === 'undefined') {
            PA.export._setStatus('SheetJS not loaded.', 'err'); return;
        }
        var wb = XLSX.utils.book_new();
        var d  = data.meta;
        var P  = PA.export._PAL;
        var TC = { summary:'4F46E5', callFlow:'059669', tableOps:'2563EB', joins:'D97706',
                   cursors:'DB2777', sequences:'0D9488', callGraph:'7C3AED', complexity:'DC2626' };

        if (sections.summary)    PA.export._sheetSummary(wb, d, data.joins || [], data.tables || [], TC.summary, P.summary, data.nodes);
        if (sections.procs)      PA.export._sheetCallFlow(wb, d, data.nodes,  TC.callFlow,   P.callFlow,  isDetail);
        if (sections.tables)     PA.export._sheetTableOps(wb, data.tables,    TC.tableOps,   P.tableOps,  isDetail);
        if (sections.joins)      PA.export._sheetJoins(wb,   data.joins,      TC.joins,      P.joins,     isDetail);
        if (sections.cursors)    PA.export._sheetCursors(wb, data.cursors,    TC.cursors,    P.cursors,   isDetail);
        if (sections.sequences)  PA.export._sheetSeq(wb,     data.sequences,  TC.sequences,  P.sequences, isDetail);
        if (sections.callGraph)  PA.export._sheetCallGraph(wb, data.callGraph, TC.callGraph,  P.callGraph, data.scopedNodeIds, data.nodes);
        if (sections.complexity) PA.export._sheetComplexity(wb, d, data.nodes, TC.complexity, P.complexity);

        var base = (d.entryPoint || d.name || 'analysis').replace(/[/\\:*?"<>|]/g, '_');
        XLSX.writeFile(wb, base + (isDetail ? '_detail' : '_summary') + '_' +
            new Date().toISOString().slice(0,10).replace(/-/g,'') + '.xlsx');
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // SUMMARY SHEET — human-readable KPI card layout, no raw tech breakdown
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetSummary(wb, d, joinsData, tablesData, tc, pal, scopedNodes) {
        // Stash so _sheetComplexity can reuse the same joins data
        PA.export._lastJoinsData = joinsData || [];

        // Summary uses 4 columns: icon | label | value | bar/badge
        var ws  = {};
        var ref = { s: { r: 0, c: 0 }, e: { r: 0, c: 3 } };

        var TOTAL_COLS = 13; // widened for 13-col ALL PROCEDURES sub-table
        var putWide = function(ri, text, style) {
            for (var ci = 0; ci < TOTAL_COLS; ci++) {
                var addr = XLSX.utils.encode_cell({ r: ri, c: ci });
                ws[addr] = { v: ci === 0 ? (text == null ? '' : text) : '', t: 's', s: style };
                if (ref.e.r < ri) ref.e.r = ri;
                if (ref.e.c < ci) ref.e.c = ci;
            }
        };
        var put4 = function(ri, a, b, c2, c3, s0, s1, s2, s3) {
            [[a,s0],[b,s1],[c2,s2],[c3,s3]].forEach(function(pair, ci) {
                var v = pair[0] == null ? '' : pair[0];
                ws[XLSX.utils.encode_cell({ r: ri, c: ci })] = { v: v, t: typeof v === 'number' ? 'n' : 's', s: pair[1] };
                if (ref.e.r < ri) ref.e.r = ri;
                if (ref.e.c < ci) ref.e.c = ci;
            });
        };

        var BLANK  = { fill: PA.export._fill('F8F9FF') };
        var TITLEB = { font: { bold: true, sz: 20, color: { rgb: PA.export._rgb(tc) }, name: 'Calibri' }, fill: PA.export._fill('F0F4FF') };
        var SUBB   = { font: { sz: 11, color: { rgb: 'FF4F46E5' }, name: 'Calibri' }, fill: PA.export._fill('F0F4FF') };
        var DATEB  = { font: { sz: 9, color: { rgb: 'FF94A3B8' }, name: 'Calibri' }, fill: PA.export._fill('F0F4FF') };

        // Section header: full-width colored bar
        var mkSec = function(bg, fg) {
            return { fill: PA.export._fill(bg),
                     font: { bold: true, sz: 10, color: { rgb: PA.export._rgb(fg) }, name: 'Calibri' },
                     border: PA.export._brd(bg), alignment: { horizontal: 'left' } };
        };
        // KPI label
        var mkLbl = function(ri) {
            return { fill: PA.export._fill(ri % 2 === 0 ? pal.e : pal.o),
                     font: { bold: true, sz: 9, color: { rgb: 'FF374151' }, name: 'Calibri' },
                     border: PA.export._brd(pal.b), alignment: { horizontal: 'left' } };
        };
        // KPI value
        var mkVal = function(ri) {
            return { fill: PA.export._fill(ri % 2 === 0 ? pal.e : pal.o),
                     font: { sz: 9, color: { rgb: 'FF0F172A' }, name: 'Calibri' },
                     border: PA.export._brd(pal.b), alignment: { horizontal: 'left' } };
        };
        var mkNum = function(ri) {
            return { fill: PA.export._fill(ri % 2 === 0 ? pal.e : pal.o),
                     font: { bold: true, sz: 11, color: { rgb: PA.export._rgb(pal.n) }, name: 'Calibri' },
                     border: PA.export._brd(pal.b), alignment: { horizontal: 'center', vertical: 'center' } };
        };
        var mkBadge = function(bg, fg) {
            return { fill: PA.export._fill(bg),
                     font: { bold: true, sz: 9, color: { rgb: PA.export._rgb(fg) }, name: 'Calibri' },
                     border: PA.export._brd(bg), alignment: { horizontal: 'center' } };
        };

        var ri = 0;
        putWide(ri++, '  Database Analysis Report', TITLEB);
        putWide(ri++, '  ' + (d.entryPoint || d.name || '—') + (d.entrySchema ? '   ·   Schema: ' + d.entrySchema : ''), SUBB);
        putWide(ri++, '  Generated: ' + new Date().toLocaleString(), DATEB);
        putWide(ri++, '', BLANK);

        // ── Derive flow-wide object type counts from tablesData ──────────────
        var glanceTblCnt = 0, glanceViewCnt = 0, glanceMVCnt = 0, glanceTrigSet = new Set();
        (tablesData || []).forEach(function(t) {
            var tp = (t.objectType || 'TABLE').toUpperCase().replace(/ /g,'_');
            if (tp === 'TABLE')              glanceTblCnt++;
            else if (tp === 'VIEW')          glanceViewCnt++;
            else if (tp === 'MATERIALIZED_VIEW') glanceMVCnt++;
            (t.triggers || []).forEach(function(tr){ if (tr.name) glanceTrigSet.add(tr.name); });
        });
        var glanceNodes   = scopedNodes || d.nodes || [];
        var glanceProcCnt = glanceNodes.filter(function(n){ return (n.objectType||'').toUpperCase()==='PROCEDURE'; }).length;
        var glanceFuncCnt = glanceNodes.filter(function(n){ return (n.objectType||'').toUpperCase()==='FUNCTION';  }).length;
        // Internal/External Calls tiles describe the ENTRY procedure — always use the true root
        // (depth 0), never an arbitrary scoped node[0], so the KPI isn't misleading when scoped.
        var glanceRoot    = ((d.nodes || []).filter(function(n){ return (n.depth||0)===0; })[0]) || (d.nodes && d.nodes[0]) || glanceNodes[0];
        var glanceIntCalls = glanceRoot && glanceRoot.counts ? (glanceRoot.counts.internalCalls || 0) : 0;
        var glanceExtCalls = glanceRoot && glanceRoot.counts ? (glanceRoot.counts.externalCalls || 0) : 0;

        // ── KPI tiles (col 0=label, col 1=value, col 2=label, col 3=value)
        putWide(ri++, '   ANALYSIS AT A GLANCE', mkSec('4F46E5', 'FFFFFF'));
        var kpis = [
            ['Lines of Code',       d.totalLinesOfCode || 0,  'DB Operations',       d.dbCallCount || 0],
            ['Tables',              glanceTblCnt,              'Views',               glanceViewCnt],
            ['Materialized Views',  glanceMVCnt,               'Triggers on Tables',  glanceTrigSet.size],
            ['Procedures in Flow',  glanceProcCnt,             'Functions in Flow',   glanceFuncCnt],
            ['Call Edges',          d.uniqueEdges || d.totalEdges || 0, 'Max Call Depth', d.maxDepth || 0],
            ['Internal Calls',      glanceIntCalls,            'External Calls',      glanceExtCalls],
            ['Analysis Time (ms)',  d.crawlTimeMs || 0,        '',                    ''],
        ];
        kpis.forEach(function(row) {
            put4(ri++, '  ' + row[0], row[1], '  ' + row[2], row[3],
                mkLbl(ri), mkNum(ri), mkLbl(ri+1), mkNum(ri+1));
        });
        putWide(ri++, '', BLANK);

        // ── Entry info
        putWide(ri++, '   ENTRY POINT DETAILS', mkSec('059669', 'FFFFFF'));
        [
            ['Entry Procedure',   d.entryPoint  || '—'],
            ['Schema / Owner',    d.entrySchema || '—'],
            ['Analysis Name',     d.name        || '—'],
        ].forEach(function(p) {
            put4(ri++, '  ' + p[0], '', '  ' + p[1], '',
                mkLbl(ri), BLANK, mkVal(ri), BLANK);
        });
        putWide(ri++, '', BLANK);

        // ── Datasource / connection details (works for every dialect via jdbcUrl)
        var dsType = (function(u){ u=(u||'').toLowerCase();
            if (u.indexOf('sqlserver')>=0) return 'SQL Server';
            if (u.indexOf('oracle')>=0)    return 'Oracle';
            if (u.indexOf('mysql')>=0)     return 'MySQL';
            if (u.indexOf('db2')>=0)       return 'IBM Db2';
            return (d.dbType || '—');
        })(d.jdbcUrl);
        putWide(ri++, '   DATASOURCE / CONNECTION', mkSec('B45309', 'FFFFFF'));
        [
            ['Datasource Type',   dsType],
            ['Project',           d.project        || '—'],
            ['Environment',       d.environment    || '—'],
            ['Connection',        d.connectionName || '—'],
            ['JDBC URL',          d.jdbcUrl        || '—'],
        ].forEach(function(p) {
            put4(ri++, '  ' + p[0], '', '  ' + p[1], '',
                mkLbl(ri), BLANK, mkVal(ri), BLANK);
        });
        putWide(ri++, '', BLANK);

        // ── Transaction scope (flow-wide)
        var tx = d.transactionSummary;
        if (tx) {
            putWide(ri++, '   TRANSACTION SCOPE', mkSec('7C3AED', 'FFFFFF'));
            [
                ['Commits',      (tx.commits || 0)],
                ['Rollbacks',    (tx.rollbacks || 0)],
                ['Savepoints',   (tx.savepoints || 0)],
                ['Autonomous procedures', (tx.autonomousProcs || 0)],
            ].forEach(function(p) {
                put4(ri++, '  ' + p[0], '', '  ' + p[1], '',
                    mkLbl(ri), BLANK, mkVal(ri), BLANK);
            });
            putWide(ri++, '', BLANK);
        }

        // ── Root procedure quick stats
        var rootNode = d.nodes && d.nodes[0];
        if (rootNode && rootNode.counts) {
            var cnt = rootNode.counts;
            putWide(ri++, '   ROOT PROCEDURE — QUICK STATS  (' + (rootNode.name || d.entryPoint || '') + ')', mkSec('2563EB', 'FFFFFF'));

            // ── Derive table/view/MV/trigger counts for root proc from tablesData ──
            var rootNodeName = (rootNode.name || '').toUpperCase();
            var rootTableCnt = 0, rootViewCnt = 0, rootMVCnt = 0;
            var rootTrigSet = new Set();
            (tablesData || []).forEach(function(t) {
                var usedByRoot = (t.accessDetails || []).some(function(ad) {
                    return (ad.procedureName || '').toUpperCase() === rootNodeName;
                });
                if (!usedByRoot) return;
                var tp = (t.objectType || 'TABLE').toUpperCase().replace(/ /g, '_');
                if (tp === 'TABLE') rootTableCnt++;
                else if (tp === 'VIEW') rootViewCnt++;
                else if (tp === 'MATERIALIZED_VIEW') rootMVCnt++;
                (t.triggers || []).forEach(function(tr) { if (tr.name) rootTrigSet.add(tr.name); });
            });

            // ── Derive unique/total call counts from nodes array ──
            var allNodes   = d.nodes || [];
            var rootSchema = rootNode.schema || '';
            var calledNodes = allNodes.slice(1);  // all except root = unique called objects

            // Unique procs and funcs in subtree
            var uniqProcNodes = calledNodes.filter(function(n){ return (n.objectType||'').toUpperCase() === 'PROCEDURE'; });
            var uniqFuncNodes = calledNodes.filter(function(n){ return (n.objectType||'').toUpperCase() === 'FUNCTION'; });
            var uniqProcCnt = uniqProcNodes.length;
            var uniqFuncCnt = uniqFuncNodes.length;

            // Unique internal/external targets (by schema of called node)
            var uniqIntTargets = calledNodes.filter(function(n){ return n.schema === rootSchema; }).length;
            var uniqExtTargets = calledNodes.filter(function(n){ return n.schema && n.schema !== rootSchema; }).length;

            // Total calls from cnt (counts how many times each call happens, incl. duplicates)
            var totalInt = cnt.internalCalls || 0;
            var totalExt = cnt.externalCalls || 0;

            // ── Build rows: [label, value, description] ──
            var qStats = [];
            qStats.push(['Tables Accessed', rootTableCnt,
                'How many distinct TABLE-type objects this procedure directly reads or writes (SELECT / INSERT / UPDATE / DELETE). Each table counted once regardless of how many times it is accessed.']);
            if (rootViewCnt > 0)
                qStats.push(['Views Accessed', rootViewCnt,
                    'How many distinct VIEW objects this procedure queries directly. Views are counted separately from base tables because they may hide additional table joins underneath.']);
            if (rootMVCnt > 0)
                qStats.push(['MVs Accessed', rootMVCnt,
                    'How many distinct MATERIALIZED VIEW objects this procedure accesses directly. MVs hold pre-computed data and refreshes may be triggered by DML on their source tables.']);
            if (rootTrigSet.size > 0)
                qStats.push(['Triggers on Tables', rootTrigSet.size,
                    'How many database triggers are attached to the tables accessed by this procedure. Triggers fire automatically on INSERT / UPDATE / DELETE and can cause hidden side-effects or extra transactions.']);
            if (uniqProcCnt > 0)
                qStats.push(['Procedures Called', uniqProcCnt,
                    'How many distinct PROCEDURE objects appear anywhere in the full call flow rooted at this entry point. Each procedure counted once even if it is called multiple times from different paths.']);
            if (uniqFuncCnt > 0)
                qStats.push(['Functions Called', uniqFuncCnt,
                    'How many distinct FUNCTION objects appear anywhere in the full call flow. Functions are counted separately from procedures because they return values and are often embedded inside SQL statements.']);
            qStats.push(['Internal Calls', totalInt,
                'Number of calls made from this procedure to other objects that belong to the same schema. A high internal call count indicates deep logic decomposition within the same owner.']);
            qStats.push(['External Calls', totalExt,
                'Number of calls made from this procedure to objects owned by a different schema. Each external call is a cross-schema dependency — a change in the target schema can break this procedure.']);
            // row = [label, value, description]
            var mkDesc = function(ri) {
                return { fill: PA.export._fill(ri % 2 === 0 ? 'F0F4FF' : 'F8F9FF'),
                         font: { italic: true, sz: 8, color: { rgb: 'FF6B7280' }, name: 'Calibri' },
                         border: PA.export._brd('E2E8F0'), alignment: { horizontal: 'left', wrapText: true } };
            };
            qStats.forEach(function(row) {
                var cur = ri++;
                put4(cur, '  ' + row[0], row[1], '', '',
                    mkLbl(cur), mkNum(cur), BLANK, BLANK);
                ws[XLSX.utils.encode_cell({ r: cur, c: 4 })] = { v: row[2], t: 's', s: mkDesc(cur) };
                ws[XLSX.utils.encode_cell({ r: cur, c: 5 })] = { v: '', t: 's', s: mkDesc(cur) };
                if (ref.e.c < 5) ref.e.c = 5;
                if (ref.e.r < cur) ref.e.r = cur;
            });
            putWide(ri++, '', BLANK);

            // ── SQL Operation summary (human-readable, only real SQL ops)
            if (cnt.statements) {
                var sqlOps = ['SELECT','INSERT','UPDATE','DELETE','COMMIT'];
                var sqlBg  = { SELECT:'16A34A',INSERT:'2563EB',UPDATE:'D97706',DELETE:'DC2626',COMMIT:'059669' };
                var any = sqlOps.some(function(op){ return cnt.statements[op]; });
                if (any) {
                    putWide(ri++, '   SQL OPERATIONS IN ROOT PROCEDURE', mkSec('7C3AED', 'FFFFFF'));
                    // Two per row
                    for (var si = 0; si < sqlOps.length; si += 2) {
                        var op0 = sqlOps[si],   v0 = cnt.statements[op0] || 0;
                        var op1 = sqlOps[si+1], v1 = op1 ? (cnt.statements[op1] || 0) : null;
                        var bg0c = sqlBg[op0] || tc;
                        var bg1c = op1 ? (sqlBg[op1] || tc) : 'F8F9FF';
                        put4(ri++,
                            op0, v0,
                            op1 || '', v1 === null ? '' : v1,
                            mkBadge(bg0c, 'FFFFFF'),
                            Object.assign({}, mkBadge(bg0c, 'FFFFFF'), { alignment: { horizontal: 'right' } }),
                            op1 ? mkBadge(bg1c, 'FFFFFF') : BLANK,
                            op1 && v1 !== null ? Object.assign({}, mkBadge(bg1c, 'FFFFFF'), { alignment: { horizontal: 'right' } }) : BLANK
                        );
                    }
                    putWide(ri++, '', BLANK);
                }
            }
        }

        // ── All-procedures rating sub-table ──────────────────────────────────
        var allNodes = scopedNodes || d.nodes || [];
        if (allNodes.length) {
            // Build per-procedure join count from joins dataset
            var joinsByProc = {};
            (joinsData || []).forEach(function(j) {
                (j.accessDetails || []).forEach(function(ad) {
                    var pn = (ad.procedureName || '').toUpperCase();
                    if (pn) joinsByProc[pn] = (joinsByProc[pn] || 0) + 1;
                });
            });

            // Build per-procedure TABLE / VIEW / MV / trigger counts from tablesData
            // tablesData[].accessDetails[].procedureName matches n.name exactly
            var tblByProc = {}, viewByProc = {}, mvByProc = {}, trigByProc = {};
            (tablesData || []).forEach(function(t) {
                var tp = (t.objectType || 'TABLE').toUpperCase().replace(/ /g, '_');
                var trigs = (t.triggers || []).map(function(tr){ return tr.name||''; }).filter(Boolean);
                (t.accessDetails || []).forEach(function(ad) {
                    var pn = (ad.procedureName || '').toUpperCase();
                    if (!pn) return;
                    if (tp === 'TABLE') {
                        if (!tblByProc[pn]) tblByProc[pn] = new Set();
                        tblByProc[pn].add(t.tableName);
                    } else if (tp === 'VIEW') {
                        if (!viewByProc[pn]) viewByProc[pn] = new Set();
                        viewByProc[pn].add(t.tableName);
                    } else if (tp === 'MATERIALIZED_VIEW') {
                        if (!mvByProc[pn]) mvByProc[pn] = new Set();
                        mvByProc[pn].add(t.tableName);
                    }
                    if (trigs.length) {
                        if (!trigByProc[pn]) trigByProc[pn] = new Set();
                        trigs.forEach(function(tr){ trigByProc[pn].add(tr); });
                    }
                });
            });

            var ratedNodes = allNodes.map(function(n) {
                var c       = n.counts || {};
                var stm     = c.statements || {};
                var loc     = n.linesOfCode || 0;
                var procKey = (n.name || '').toUpperCase();
                // Split table counts from tablesData; fall back to combined c.tables if no tablesData
                var tblCnt  = tblByProc[procKey]  ? tblByProc[procKey].size  : 0;
                var viewCnt = viewByProc[procKey] ? viewByProc[procKey].size : 0;
                var mvCnt   = mvByProc[procKey]   ? mvByProc[procKey].size   : 0;
                var trigCnt = trigByProc[procKey] ? trigByProc[procKey].size : 0;
                // Total for scoring = all object types (same behaviour as before)
                var tbl     = (tblByProc[procKey] || viewByProc[procKey] || mvByProc[procKey])
                                ? tblCnt + viewCnt + mvCnt
                                : (c.tables || 0);
                var cur     = c.cursors || 0;
                var ops     = (stm.SELECT||0)+(stm.INSERT||0)+(stm.UPDATE||0)+(stm.DELETE||0)+(stm.MERGE||0);
                var joins   = joinsByProc[procKey] || 0;
                var dyn     = (stm.EXECUTE_IMMEDIATE||0)+(stm.DBMS_SQL||0);
                // Score: LOC×1 + tables×5 + cursors×2 + ops×2 + joins×3 + dynamic×10
                var locPart  = loc;
                var tblPart  = tbl  * 5;
                var curPart  = cur  * 2;
                var opsPart  = ops  * 2;
                var joinPart = joins * 3;
                var dynPart  = dyn  * 10;
                var score    = locPart + tblPart + curPart + opsPart + joinPart + dynPart;
                var risk     = score > 2000 ? 'HIGH' : score > 500 ? 'MEDIUM' : 'LOW';
                // n.name already contains "PKG.SUBPROGRAM" — don't add packageName again
                var fullName = n.name || [n.schema||'', n.packageName||'', n.objectName||''].filter(Boolean).join('.');
                // Score reason uses the actual split total (tbl = tblCnt+viewCnt+mvCnt)
                var parts = [];
                parts.push('LOC ' + loc + '×1=' + locPart);
                if (tbl) {
                    var tblDesc = tblCnt + (viewCnt ? '+' + viewCnt + 'V' : '') + (mvCnt ? '+' + mvCnt + 'MV' : '');
                    parts.push('Tables(' + tblDesc + ')×5=' + tblPart);
                }
                if (cur)   parts.push('Cursors ' + cur  + '×2=' + curPart);
                if (ops)   parts.push('Ops ' + ops      + '×2=' + opsPart);
                if (joins) parts.push('Joins ' + joins  + '×3=' + joinPart);
                if (dyn)   parts.push('DynSQL ' + dyn   + '×10=' + dynPart);
                var reason = parts.join(' + ') + ' = ' + score + ' → ' + risk;
                return { fullName: fullName, loc: loc,
                         tblCnt: tblCnt, viewCnt: viewCnt, mvCnt: mvCnt, trigCnt: trigCnt,
                         cursors: cur, ops: ops, joins: joins, dyn: dyn,
                         score: score, risk: risk, reason: reason };
            }).sort(function(a,b){ return b.score - a.score; });

            putWide(ri++, '   ALL PROCEDURES — COMPLEXITY RATING', mkSec('DC2626', 'FFFFFF'));

            // 13-column sub-table:
            // Procedure/Function | LOC | Tables | Views | MVs | Triggers | Cursors | SQL Ops | Joins | Dyn SQL | Score | Risk | Score Reason
            var subHdr = [
                '  Procedure / Function',
                'LOC',
                'Tables',
                'Views',
                'MVs',
                'Triggers',
                'Cursors',
                'SQL Ops',
                'Joins',
                'Dynamic SQL',
                'Score',
                'Risk',
                'Score Reason'
            ];
            var hdrStyle = { fill:PA.export._fill('1E3A5F'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('1E3A5F'), alignment:{horizontal:'center'} };
            subHdr.forEach(function(h, ci) {
                var addr = XLSX.utils.encode_cell({ r: ri, c: ci });
                ws[addr] = { v: h, t: 's', s: hdrStyle };
                if (ref.e.c < ci) ref.e.c = ci;
            });
            if (ref.e.r < ri) ref.e.r = ri;
            ri++;

            var riskBg = { HIGH:'991B1B', MEDIUM:'92400E', LOW:'14532D' };

            ratedNodes.forEach(function(n) {
                var bg = ri%2===0 ? 'F8FAFF' : 'EFF2FF';
                var rBg = riskBg[n.risk] || '14532D';
                var txStyle = function(bold) {
                    return { fill:PA.export._fill(bg), font:{sz:9,bold:!!bold,color:{rgb:'FF0F172A'},name:'Calibri'}, border:PA.export._brd('E2E8F0'), alignment:{horizontal:'left',wrapText:true} };
                };
                var numStyle = { fill:PA.export._fill(bg), font:{bold:true,sz:9,color:{rgb:'FF1E3A5F'},name:'Calibri'}, border:PA.export._brd('E2E8F0'), alignment:{horizontal:'right'} };
                var zeroStyle = { fill:PA.export._fill(bg), font:{sz:9,color:{rgb:'FFCBD5E1'},name:'Calibri'}, border:PA.export._brd('E2E8F0'), alignment:{horizontal:'right'} };
                var riskStyle = { fill:PA.export._fill(rBg), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd(rBg), alignment:{horizontal:'center'} };
                var reasonStyle = { fill:PA.export._fill(bg), font:{italic:true,sz:8,color:{rgb:'FF374151'},name:'Calibri'}, border:PA.export._brd('E2E8F0'), alignment:{horizontal:'left',wrapText:true} };
                var row    = [n.fullName, n.loc, n.tblCnt, n.viewCnt, n.mvCnt, n.trigCnt,
                              n.cursors, n.ops, n.joins, n.dyn, n.score, n.risk, n.reason];
                var numOrZero = function(v) { return v > 0 ? numStyle : zeroStyle; };
                var styles = [txStyle(true), numStyle,
                              numOrZero(n.tblCnt), numOrZero(n.viewCnt), numOrZero(n.mvCnt), numOrZero(n.trigCnt),
                              numOrZero(n.cursors), numOrZero(n.ops), numOrZero(n.joins), numOrZero(n.dyn),
                              numStyle, riskStyle, reasonStyle];
                row.forEach(function(v, ci) {
                    var addr = XLSX.utils.encode_cell({ r: ri, c: ci });
                    ws[addr] = { v: v == null ? '' : v, t: typeof v==='number'?'n':'s', s: styles[ci] };
                    if (ref.e.c < ci) ref.e.c = ci;
                });
                if (ref.e.r < ri) ref.e.r = ri;
                ri++;
            });

            // ── Totals + Risk distribution summary row ────────────────────────
            var highCnt   = ratedNodes.filter(function(n){ return n.risk === 'HIGH';   }).length;
            var medCnt    = ratedNodes.filter(function(n){ return n.risk === 'MEDIUM'; }).length;
            var lowCnt    = ratedNodes.filter(function(n){ return n.risk === 'LOW';    }).length;
            var totalUniq = ratedNodes.length;

            // Sum each numeric column across all rated nodes
            var sumLoc  = ratedNodes.reduce(function(a,n){ return a + (n.loc     || 0); }, 0);
            var sumTbl  = ratedNodes.reduce(function(a,n){ return a + (n.tblCnt  || 0); }, 0);
            var sumView = ratedNodes.reduce(function(a,n){ return a + (n.viewCnt || 0); }, 0);
            var sumMV   = ratedNodes.reduce(function(a,n){ return a + (n.mvCnt   || 0); }, 0);
            var sumTrig = ratedNodes.reduce(function(a,n){ return a + (n.trigCnt || 0); }, 0);
            var sumCur  = ratedNodes.reduce(function(a,n){ return a + (n.cursors || 0); }, 0);
            var sumOps  = ratedNodes.reduce(function(a,n){ return a + (n.ops     || 0); }, 0);
            var sumJoin = ratedNodes.reduce(function(a,n){ return a + (n.joins   || 0); }, 0);
            var sumDyn  = ratedNodes.reduce(function(a,n){ return a + (n.dyn     || 0); }, 0);

            var riskText = 'HIGH: ' + highCnt + '   MEDIUM: ' + medCnt + '   LOW: ' + lowCnt;
            var summaryText = totalUniq + ' unique procedures/functions   |   ' + riskText;

            var sumStyle  = { fill:PA.export._fill('1E3A5F'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('1E3A5F'), alignment:{horizontal:'left'} };
            var sumNumSty = { fill:PA.export._fill('1E3A5F'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('1E3A5F'), alignment:{horizontal:'right'} };
            var badgeH    = { fill:PA.export._fill('991B1B'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('991B1B'), alignment:{horizontal:'center'} };
            var badgeM    = { fill:PA.export._fill('92400E'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('92400E'), alignment:{horizontal:'center'} };
            var badgeL    = { fill:PA.export._fill('14532D'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('14532D'), alignment:{horizontal:'center'} };

            // col: 0=label  1=LOC  2=Tables  3=Views  4=MVs  5=Triggers  6=Cursors
            //      7=SQLOps  8=Joins  9=DynSQL  10=Unique count  11=Risk dist  12=Summary
            var sumRow    = [ '  TOTALS', sumLoc, sumTbl, sumView, sumMV, sumTrig,
                               sumCur, sumOps, sumJoin, sumDyn,
                               totalUniq, riskText, summaryText ];
            var sumStyles = [ sumStyle,  sumNumSty, sumNumSty, sumNumSty, sumNumSty, sumNumSty,
                               sumNumSty, sumNumSty, sumNumSty, sumNumSty,
                               sumNumSty,
                               // Risk column: split badge colours per level
                               (highCnt > 0 ? badgeH : medCnt > 0 ? badgeM : badgeL),
                               sumStyle ];
            sumRow.forEach(function(val, ci2) {
                var addr2 = XLSX.utils.encode_cell({ r: ri, c: ci2 });
                ws[addr2] = { v: val == null ? '' : val, t: typeof val==='number'?'n':'s', s: sumStyles[ci2] };
                if (ref.e.c < ci2) ref.e.c = ci2;
            });
            if (ref.e.r < ri) ref.e.r = ri;
            ri++;

            putWide(ri++, '', BLANK);
        }

        if (d.errors && d.errors.length) {
            putWide(ri++, '   ANALYSIS WARNINGS / ERRORS', mkSec('DC2626', 'FFFFFF'));
            d.errors.forEach(function(e) {
                put4(ri++, '  ' + (e.type || 'ERROR'), '', '  ' + (e.message || String(e)), '',
                    { fill:PA.export._fill('FEE2E2'), font:{bold:true,sz:9,color:{rgb:'FF991B1B'},name:'Calibri'}, border:PA.export._brd('FCA5A5') },
                    BLANK,
                    { fill:PA.export._fill('FFF1F2'), font:{sz:9,color:{rgb:'FF7F1D1D'},name:'Calibri'}, border:PA.export._brd('FCA5A5') },
                    BLANK
                );
            });
        }

        ws['!ref']       = XLSX.utils.encode_range(ref);
        ws['!cols']      = [
            { wch: 45 },  // 0  Procedure / Function
            { wch:  7 },  // 1  LOC
            { wch:  8 },  // 2  Tables
            { wch:  7 },  // 3  Views
            { wch:  6 },  // 4  MVs
            { wch:  9 },  // 5  Triggers
            { wch:  8 },  // 6  Cursors
            { wch:  8 },  // 7  SQL Ops
            { wch:  7 },  // 8  Joins
            { wch: 11 },  // 9  Dynamic SQL
            { wch:  8 },  // 10 Score
            { wch:  8 },  // 11 Risk
            { wch: 70 },  // 12 Score Reason
        ];
        ws['!sheetView'] = [{ state: 'normal' }];
        PA.export._sanitizeWs(ws);
        XLSX.utils.book_append_sheet(wb, ws, 'Summary');
        if (wb.Sheets['Summary']) wb.Sheets['Summary']['!tabColor'] = { rgb: tc };
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // CALL FLOW SHEET — full-row depth colors
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetCallFlow(wb, d, scopedNodes, tc, pal) {
        var hdrs = ['#', 'Procedure', 'Schema', 'Package', 'Type', 'Depth',
                    'LOC', 'Tables', 'Calls Out', 'Int.Calls', 'Ext.Calls',
                    'Cursors', 'Call Count', 'Source File'];
        var ws  = PA.export._makeWs(hdrs);
        var ref = ws['!ref_obj'];
        var DP  = PA.export._DEPTH_PAL;
        var numCols = new Set([0, 5, 6, 7, 8, 9, 10, 11, 12]);

        (scopedNodes || d.nodes || []).forEach(function(n, i) {
            var ri  = i + 1;
            var cnt = n.counts || {};
            var dep = n.depth != null ? Math.min(n.depth, DP.length - 1) : DP.length - 1;
            var dp  = DP[dep];
            var bg  = ri % 2 === 0 ? dp.e : dp.o;

            var styles = hdrs.map(function(_, ci) {
                if (numCols.has(ci)) {
                    return { fill:  PA.export._fill(bg),
                             font:  { bold: true, color: { rgb: PA.export._rgb(dp.fg) }, sz: 9, name: 'Calibri' },
                             alignment: { horizontal: 'right', vertical: 'top' },
                             border: PA.export._brd(bg) };
                }
                return { fill:  PA.export._fill(bg),
                         font:  { color: { rgb: PA.export._rgb(dp.fg) }, sz: 9, bold: dp.bold, name: 'Calibri' },
                         alignment: { horizontal: 'left', vertical: 'top' },
                         border: PA.export._brd(bg) };
            });

            PA.export._sr(ws, ri, [
                ri, n.name || '', n.schema || '', n.packageName || '', n.objectType || '',
                n.depth != null ? n.depth : '',
                n.linesOfCode || 0, cnt.tables || 0, cnt.callsOut || 0,
                cnt.internalCalls || 0, cnt.externalCalls || 0,
                cnt.cursors || 0, n.callCount || 0, n.sourceFile || ''
            ], styles, ref);
        });

        PA.export._seal(ws);
        PA.export._finish(wb, ws, 'Call Flow', hdrs,
            [4, 36, 10, 22, 12, 6, 7, 7, 8, 8, 8, 7, 8, 34], tc);
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // TABLE OPS SHEET
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetTableOps(wb, tables, tc, pal, isDetail) {
        var arr = Array.isArray(tables) ? tables : [];

        if (isDetail) {
            var hdrs = ['#', 'Table Name', 'Schema', 'Object Type', 'Operation',
                        'Procedure', 'Line #', 'Access Count', 'Trigger Count', 'Triggers'];
            var ws  = PA.export._makeWs(hdrs);
            var ref = ws['!ref_obj'];
            var ri  = 1;

            arr.forEach(function(t) {
                var tName  = t.tableName || t.name || '';
                var tSch   = t.tableSchema || t.schema || '';
                var tType  = t.objectType || 'TABLE';
                var dets   = t.accessDetails || [];
                var trigs  = t.triggers || [];
                var tCnt   = trigs.length;
                var tNames = trigs.map(function(tr) { return tr.name || ''; }).filter(Boolean).join(', ');

                if (!dets.length) {
                    var s = [PA.export._tn(ri,pal), PA.export._tc(ri,pal,'left',true),
                             PA.export._tc(ri,pal), PA.export._tc(ri,pal,'center'),
                             PA.export._tc(ri,pal,'center'), PA.export._tc(ri,pal),
                             PA.export._tn(ri,pal), PA.export._tn(ri,pal),
                             PA.export._tn(ri,pal), PA.export._tc(ri,pal)];
                    PA.export._sr(ws, ri++, [ri-1,tName,tSch,tType,(t.operations||[]).join(', '),'','',t.accessCount||0,tCnt,tNames], s, ref);
                    return;
                }

                var opG = {};
                dets.forEach(function(ad) {
                    var op = ad.operation || 'UNKNOWN';
                    if (!opG[op]) opG[op] = { procs: new Set(), lines: [] };
                    var pn = ad.procedureName || ad.procedureId || '';
                    if (pn) opG[op].procs.add(pn);
                    if (ad.lineNumber != null) opG[op].lines.push(ad.lineNumber);
                });

                Object.entries(opG).forEach(function(kv) {
                    var op = kv[0], g = kv[1];
                    var s = [PA.export._tn(ri,pal), PA.export._tc(ri,pal,'left',true),
                             PA.export._tc(ri,pal), PA.export._tc(ri,pal,'center'),
                             PA.export._ob(op, ri, pal), PA.export._tc(ri,pal),
                             PA.export._tn(ri,pal), PA.export._tn(ri,pal),
                             PA.export._tn(ri,pal), PA.export._tc(ri,pal)];
                    PA.export._sr(ws, ri++, [
                        ri-1, tName, tSch, tType, op,
                        Array.from(g.procs).join(', '),
                        g.lines.length ? g.lines[0] : '', g.lines.length,
                        tCnt, tNames
                    ], s, ref);
                });
            });

            PA.export._seal(ws);
            PA.export._finish(wb, ws, 'Table Ops', hdrs, [4,30,10,10,10,42,8,10,8,40], tc);

        } else {
            var opCols = ['SELECT','INSERT','UPDATE','DELETE','DYNAMIC','COMMIT'];
            var hdrs2  = ['#','Table Name','Schema','Object Type'].concat(opCols).concat(['Total','Triggers','Trigger Names']);
            var ws2    = PA.export._makeWs(hdrs2);
            var ref2   = ws2['!ref_obj'];

            // Trigger badge style — amber when > 0
            var _trigStyle = function(cnt, ri) {
                if (cnt > 0) return { fill:PA.export._fill('D97706'), font:{bold:true,sz:9,color:{rgb:'FFFFFFFF'},name:'Calibri'}, border:PA.export._brd('D97706'), alignment:{horizontal:'center'} };
                return PA.export._tc(ri,pal,'center');
            };

            arr.forEach(function(t, i) {
                var ri    = i + 1;
                var dets  = t.accessDetails || [];
                var tName = t.tableName || t.name || '';
                var tSch  = t.tableSchema || t.schema || '';
                var tType = t.objectType || 'TABLE';
                var trigs = t.triggers || [];
                var tCnt  = trigs.length;
                var tNames = trigs.map(function(tr) { return tr.name || ''; }).filter(Boolean).join(', ');
                var cts   = {};
                opCols.forEach(function(op) { cts[op] = dets.filter(function(a){ return a.operation===op; }).length; });

                var styles = [PA.export._tn(ri,pal), PA.export._tc(ri,pal,'left',true),
                              PA.export._tc(ri,pal), PA.export._tc(ri,pal,'center')];
                opCols.forEach(function(op) {
                    styles.push(cts[op] > 0 ? PA.export._ob(op, ri, pal) : PA.export._tc(ri,pal,'center'));
                });
                styles.push(PA.export._tn(ri,pal));           // Total
                styles.push(_trigStyle(tCnt, ri));             // Triggers count
                styles.push(PA.export._tc(ri,pal));            // Trigger names

                PA.export._sr(ws2, ri,
                    [ri,tName,tSch,tType,cts.SELECT,cts.INSERT,cts.UPDATE,cts.DELETE,cts.DYNAMIC,cts.COMMIT,t.accessCount||dets.length,tCnt,tNames],
                    styles, ref2);
            });

            PA.export._seal(ws2);
            PA.export._finish(wb, ws2, 'Table Ops', hdrs2, [4,30,10,10,9,9,9,9,9,9,10,8,40], tc);
        }
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // JOINS SHEET
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetJoins(wb, joins, tc, pal, isDetail) {
        var arr = Array.isArray(joins) ? joins : Object.values(joins || {});

        if (isDetail) {
            var hdrs = ['#','Left Table','Right Table','Join Type','On Predicate','Procedure','Line'];
            var ws   = PA.export._makeWs(hdrs);
            var ref  = ws['!ref_obj'];
            var ri   = 1;

            arr.forEach(function(j) {
                var dets = j.accessDetails || [];
                if (!dets.length) {
                    var s = hdrs.map(function(_,ci){ return ci===0||ci===6 ? PA.export._tn(ri,pal) : PA.export._tc(ri,pal); });
                    PA.export._sr(ws,ri++,[ri-1,j.leftTable||'',j.rightTable||'',(j.joinTypes||[]).join(', '),'','',''],s,ref);
                } else {
                    dets.forEach(function(det) {
                        var s = hdrs.map(function(_,ci){
                            if (ci===0||ci===6) return PA.export._tn(ri,pal);
                            if (ci===3)         return PA.export._ob(det.joinType||'JOIN', ri, pal);
                            return PA.export._tc(ri,pal);
                        });
                        PA.export._sr(ws,ri++,[ri-1,j.leftTable||'',j.rightTable||'',det.joinType||'',det.onPredicate||'',det.procedureName||det.procedureId||'',det.lineNumber||''],s,ref);
                    });
                }
            });
            PA.export._seal(ws);
            PA.export._finish(wb, ws, 'Joins', hdrs, [4,28,28,14,32,36,7], tc);

        } else {
            var hdrs2 = ['#','Left Table','Right Table','Join Types','Access Count'];
            var ws2   = PA.export._makeWs(hdrs2);
            var ref2  = ws2['!ref_obj'];
            arr.forEach(function(j,i) {
                var ri = i+1;
                PA.export._sr(ws2, ri, [ri,j.leftTable||'',j.rightTable||'',(j.joinTypes||[]).join(', '),j.accessCount||0],
                    [PA.export._tn(ri,pal),PA.export._tc(ri,pal,'left',true),PA.export._tc(ri,pal),PA.export._tc(ri,pal),PA.export._tn(ri,pal)], ref2);
            });
            PA.export._seal(ws2);
            PA.export._finish(wb, ws2, 'Joins', hdrs2, [4,28,28,16,10], tc);
        }
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // CURSORS SHEET
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetCursors(wb, cursors, tc, pal, isDetail) {
        var arr = Array.isArray(cursors) ? cursors : Object.values(cursors || {});

        if (isDetail) {
            var hdrs = ['#','Cursor Name','Type','Operation','Procedure','Line','Query Text'];
            var ws   = PA.export._makeWs(hdrs);
            var ref  = ws['!ref_obj'];
            var ri   = 1;

            arr.forEach(function(c) {
                var dets = c.accessDetails || [];
                if (!dets.length) {
                    var s = hdrs.map(function(_,ci){
                        if (ci===0||ci===5) return PA.export._tn(ri,pal);
                        if (ci===2) return PA.export._ob(c.cursorType||'CURSOR', ri, pal);
                        if (ci===3) return PA.export._ob((c.operations||[])[0]||'', ri, pal);
                        return PA.export._tc(ri,pal);
                    });
                    PA.export._sr(ws,ri++,[ri-1,c.cursorName||'',c.cursorType||'',(c.operations||[]).join(', '),'','',(c.queryText||'').substring(0,300)],s,ref);
                } else {
                    dets.forEach(function(det) {
                        var s = hdrs.map(function(_,ci){
                            if (ci===0||ci===5) return PA.export._tn(ri,pal);
                            if (ci===2) return PA.export._ob(c.cursorType||'CURSOR', ri, pal);
                            if (ci===3) return PA.export._ob(det.operation||'', ri, pal);
                            return PA.export._tc(ri,pal);
                        });
                        PA.export._sr(ws,ri++,[ri-1,c.cursorName||'',c.cursorType||'',det.operation||'',det.procedureName||det.procedureId||'',det.lineNumber||'',(c.queryText||'').substring(0,300)],s,ref);
                    });
                }
            });
            PA.export._seal(ws);
            PA.export._finish(wb, ws, 'Cursors', hdrs, [4,28,12,14,36,7,60], tc);

        } else {
            var hdrs2 = ['#','Cursor Name','Type','Operations','Access Count','Query (preview)'];
            var ws2   = PA.export._makeWs(hdrs2);
            var ref2  = ws2['!ref_obj'];
            arr.forEach(function(c,i){
                var ri = i+1;
                PA.export._sr(ws2, ri, [ri,c.cursorName||'',c.cursorType||'',(c.operations||[]).join(', '),c.accessCount||0,(c.queryText||'').substring(0,200)],
                    [PA.export._tn(ri,pal),PA.export._tc(ri,pal,'left',true),PA.export._ob(c.cursorType||'CURSOR',ri,pal),PA.export._tc(ri,pal),PA.export._tn(ri,pal),PA.export._tc(ri,pal)],ref2);
            });
            PA.export._seal(ws2);
            PA.export._finish(wb, ws2, 'Cursors', hdrs2, [4,28,12,20,10,50], tc);
        }
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // SEQUENCES SHEET
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetSeq(wb, sequences, tc, pal, isDetail) {
        var arr = Array.isArray(sequences) ? sequences : Object.values(sequences || {});

        if (isDetail) {
            var hdrs = ['#','Sequence Name','Schema','Operation','Procedure','Line'];
            var ws   = PA.export._makeWs(hdrs);
            var ref  = ws['!ref_obj'];
            var ri   = 1;

            arr.forEach(function(s) {
                var dets = s.accessDetails || [];
                if (!dets.length) {
                    var st = hdrs.map(function(_,ci){
                        if (ci===0||ci===5) return PA.export._tn(ri,pal);
                        if (ci===3) return PA.export._ob((s.operations||[])[0]||'NEXTVAL', ri, pal);
                        return PA.export._tc(ri,pal);
                    });
                    PA.export._sr(ws,ri++,[ri-1,s.sequenceName||'',s.sequenceSchema||'',(s.operations||[]).join(', '),'',''],st,ref);
                } else {
                    dets.forEach(function(det) {
                        var st = hdrs.map(function(_,ci){
                            if (ci===0||ci===5) return PA.export._tn(ri,pal);
                            if (ci===3) return PA.export._ob(det.operation||'NEXTVAL', ri, pal);
                            return PA.export._tc(ri,pal);
                        });
                        PA.export._sr(ws,ri++,[ri-1,s.sequenceName||'',s.sequenceSchema||'',det.operation||'',det.procedureName||det.procedureId||'',det.lineNumber||''],st,ref);
                    });
                }
            });
            PA.export._seal(ws);
            PA.export._finish(wb, ws, 'Sequences', hdrs, [4,30,12,12,36,7], tc);

        } else {
            var hdrs2 = ['#','Sequence Name','Schema','Operations','Access Count'];
            var ws2   = PA.export._makeWs(hdrs2);
            var ref2  = ws2['!ref_obj'];
            arr.forEach(function(s,i){
                var ri = i+1;
                PA.export._sr(ws2, ri, [ri,s.sequenceName||'',s.sequenceSchema||'',(s.operations||[]).join(', '),s.accessCount||0],
                    [PA.export._tn(ri,pal),PA.export._tc(ri,pal,'left',true),PA.export._tc(ri,pal),PA.export._ob((s.operations||[])[0]||'NEXTVAL',ri,pal),PA.export._tn(ri,pal)],ref2);
            });
            PA.export._seal(ws2);
            PA.export._finish(wb, ws2, 'Sequences', hdrs2, [4,30,12,14,10], tc);
        }
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // CALL GRAPH SHEET
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetCallGraph(wb, callGraph, tc, pal, scopedNodeIds, nodes) {
        var hdrs = ['#','From Procedure','From Schema','To Procedure','To Schema','Call Count','Line','Back Edge'];
        var ws   = PA.export._makeWs(hdrs);
        var ref  = ws['!ref_obj'];
        var allEdges = callGraph && callGraph.edges ? callGraph.edges : [];

        // Filter edges: keep only edges where both from AND to are in scope
        var edges = scopedNodeIds
            ? allEdges.filter(function(e) {
                var from = (e.fromNodeId || e.from || '').toUpperCase();
                var to   = (e.toNodeId   || e.to   || '').toUpperCase();
                return scopedNodeIds.has(from) || scopedNodeIds.has(to);
              })
            : allEdges;

        // UI-024 (§112) — was PA.nodeIndex.all()[0].nodeId; nodes now travels as a parameter,
        // fetched fresh the same way as everything else in this file, rather than reading the
        // client-side index this sheet has no other reason to depend on.
        var rootId = (nodes && nodes.length) ? nodes[0].nodeId : null;

        edges.forEach(function(e, i) {
            var ri = i + 1;
            // API returns: from, fromNodeId, fromSchema, to, toNodeId, toSchema, lines (array), count, minDepth, backEdge
            var isRoot = rootId && e.fromNodeId === rootId;
            var lineVal = Array.isArray(e.lines) && e.lines.length ? e.lines[0] : (e.lineNumber || '');
            var callCount = e.count != null ? e.count : (e.callCount || 1);
            var backEdge  = e.backEdge ? 'YES' : '';

            var styles;
            if (isRoot) {
                var rb = PA.export._DEPTH_PAL[0];
                styles = hdrs.map(function(_,ci) {
                    if (ci===0||ci===5||ci===6) return { fill:PA.export._fill(rb.e), font:{bold:true,color:{rgb:PA.export._rgb(rb.fg)},sz:9,name:'Calibri'}, alignment:{horizontal:'right'}, border:PA.export._brd(rb.e) };
                    return { fill:PA.export._fill(rb.e), font:{bold:true,color:{rgb:PA.export._rgb(rb.fg)},sz:9,name:'Calibri'}, alignment:{horizontal:'left'}, border:PA.export._brd(rb.e) };
                });
            } else {
                styles = hdrs.map(function(_,ci){
                    return ci===0||ci===5||ci===6 ? PA.export._tn(ri,pal) : PA.export._tc(ri,pal);
                });
            }

            PA.export._sr(ws, ri, [ri, e.from||e.fromNodeId||'', e.fromSchema||'', e.to||e.toNodeId||'', e.toSchema||'', callCount, lineVal, backEdge], styles, ref);
        });

        if (callGraph) {
            var baseRi = edges.length + 2;
            var fStyle = { fill:PA.export._fill(pal.o), font:{bold:true,color:{rgb:PA.export._rgb(tc)},sz:10,name:'Calibri'}, border:PA.export._brd(pal.b) };
            [['Total Edges',callGraph.totalEdges||0],['Unique Edges',callGraph.uniqueEdges||0],['Back Edges',callGraph.backEdges||0]].forEach(function(p,pi){
                ws[XLSX.utils.encode_cell({r:baseRi,c:pi*2})]   = {v:p[0],t:'s',s:fStyle};
                ws[XLSX.utils.encode_cell({r:baseRi,c:pi*2+1})] = {v:p[1],t:'n',s:Object.assign({},fStyle,{alignment:{horizontal:'right'}})};
                if (ref.e.r < baseRi) ref.e.r = baseRi;
                if (ref.e.c < pi*2+1) ref.e.c = pi*2+1;
            });
        }

        PA.export._seal(ws);
        PA.export._finish(wb, ws, 'Call Graph', hdrs, [4,38,10,38,10,9,8,10], tc);
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // COMPLEXITY SHEET — full-row risk colors
    // ═══════════════════════════════════════════════════════════════════════════

    _sheetComplexity(wb, d, scopedNodes, tc, pal) {
        var hdrs = ['#','Procedure','Schema','Package','Type','Depth',
                    'LOC','Tables','Calls Out','Cursors',
                    'SELECT','INSERT','UPDATE','DELETE','COMMIT','Score','Risk'];
        var ws  = PA.export._makeWs(hdrs);
        var ref = ws['!ref_obj'];
        var RP  = PA.export._RISK_ROW;
        var numIdx = new Set([0,5,6,7,8,9,10,11,12,13,14,15]);

        // Build join lookup from summary sheet's joinsData — same approach
        var joinsData = PA.export._lastJoinsData || [];
        var joinsByProc = {};
        joinsData.forEach(function(j) {
            (j.accessDetails || []).forEach(function(ad) {
                var pn = (ad.procedureName || '').toUpperCase();
                if (pn) joinsByProc[pn] = (joinsByProc[pn] || 0) + 1;
            });
        });

        var nodes = (scopedNodes || d.nodes || []).slice().sort(function(a,b){
            var sc = function(n){
                var c=n.counts||{}; var stm=c.statements||{};
                var loc=n.linesOfCode||0, tbl=c.tables||0, cur=c.cursors||0;
                var ops=(stm.SELECT||0)+(stm.INSERT||0)+(stm.UPDATE||0)+(stm.DELETE||0)+(stm.MERGE||0);
                var procKey=(n.name||'').toUpperCase();
                var joins=joinsByProc[procKey]||0;
                var dyn=(stm.EXECUTE_IMMEDIATE||0)+(stm.DBMS_SQL||0);
                return loc + tbl*5 + cur*2 + ops*2 + joins*3 + dyn*10;
            };
            return sc(b)-sc(a);
        });

        nodes.forEach(function(n, i) {
            var ri  = i + 1;
            var cnt = n.counts || {};
            var stm = cnt.statements || {};
            var loc = n.linesOfCode || 0;
            var tbl = cnt.tables || 0;
            var co  = cnt.callsOut || 0;
            var cur = cnt.cursors || 0;
            var ops = (stm.SELECT||0)+(stm.INSERT||0)+(stm.UPDATE||0)+(stm.DELETE||0)+(stm.MERGE||0);
            var procKey = (n.name||'').toUpperCase();
            var joins = joinsByProc[procKey] || 0;
            var dyn = (stm.EXECUTE_IMMEDIATE||0)+(stm.DBMS_SQL||0);
            var score = loc + tbl*5 + cur*2 + ops*2 + joins*3 + dyn*10;

            var risk = score>2000?'HIGH':score>500?'MEDIUM':'LOW';
            var rp   = RP[risk] || RP['LOW'];
            var bg   = ri%2===0 ? rp.e : rp.o;

            var styles = hdrs.map(function(_,ci){
                if (ci === hdrs.length-1) {
                    return { fill:PA.export._fill(rp.badge.bg), font:{bold:true,color:{rgb:PA.export._rgb(rp.badge.fg)},sz:9,name:'Calibri'}, alignment:{horizontal:'center'}, border:PA.export._brd(rp.badge.bg) };
                }
                if (numIdx.has(ci)) {
                    return { fill:PA.export._fill(bg), font:{bold:true,color:{rgb:PA.export._rgb(rp.fg)},sz:9,name:'Calibri'}, alignment:{horizontal:'right'}, border:PA.export._brd(bg) };
                }
                return { fill:PA.export._fill(bg), font:{color:{rgb:PA.export._rgb(rp.fg)},sz:9,bold:risk==='CRITICAL'||risk==='HIGH',name:'Calibri'}, alignment:{horizontal:'left'}, border:PA.export._brd(bg) };
            });

            PA.export._sr(ws, ri, [
                ri, n.name||'', n.schema||'', n.packageName||'', n.objectType||'',
                n.depth!=null?n.depth:'',
                loc, tbl, co, cur,
                stm.SELECT||0, stm.INSERT||0, stm.UPDATE||0, stm.DELETE||0,
                stm.COMMIT||0,
                score, risk
            ], styles, ref);
        });

        PA.export._seal(ws);
        PA.export._finish(wb, ws, 'Complexity', hdrs,
            [4,34,10,22,12,6,7,7,8,7,7,7,7,7,7,9,10], tc);
    },

    // ═══════════════════════════════════════════════════════════════════════════
    // JSON / CSV
    // ═══════════════════════════════════════════════════════════════════════════

    _downloadJSON(data) {
        var out = Object.assign({}, data);
        // A Set serializes to {} — emit the scoped ids as an array so consumers can read them.
        if (out.scopedNodeIds instanceof Set) out.scopedNodeIds = Array.from(out.scopedNodeIds);
        // When scoped, don't embed the full unscoped analysis under meta — reflect the scoped nodes.
        if (out.exportScope && out.exportScope !== 'full' && Array.isArray(out.nodes) && out.meta) {
            out.meta = Object.assign({}, out.meta, { nodes: out.nodes });
        }
        PA.export._triggerDownload(new Blob([JSON.stringify(out,null,2)],{type:'application/json'}),
            (PA.analysisData.name||'analysis')+'_export.json');
    },

    _downloadCSV(data, sections, isDetail) {
        var d    = data.meta;
        var name = d.name || 'analysis';
        var files = [];
        // Honour the export scope in CSV too (XLSX/JSON already do). When scoped, derive the
        // procedure list + totals from data.nodes (the scoped set) instead of the full analysis.
        var scoped = data.exportScope && data.exportScope !== 'full' && data.scopedNodeIds;
        var nodes = (Array.isArray(data.nodes) ? data.nodes : d.nodes) || [];

        if (sections.summary) {
            var sProc = scoped ? nodes.length : (d.totalNodes || 0);
            var sLoc  = scoped ? nodes.reduce(function(s,n){return s+(n.linesOfCode||0);},0) : (d.totalLinesOfCode || 0);
            var sDep  = scoped ? nodes.reduce(function(m,n){return Math.max(m,n.depth||0);},0) : (d.maxDepth || 0);
            var sTbl  = scoped ? (Array.isArray(data.tables) ? data.tables.length : (d.totalTables||0)) : (d.totalTables || 0);
            files.push({ name:'summary.csv', content:
                [['Entry Point',d.entryPoint||''],['Schema',d.entrySchema||''],['Scope',data.exportScope||'full'],
                 ['Total Procedures',sProc],['Max Depth',sDep],['Total LOC',sLoc],['Total Tables',sTbl],['Total Edges',d.totalEdges||0]]
                .map(function(r){return r.map(PA.export._csvEsc).join(',');}).join('\n')
            });
        }
        if (sections.procs && nodes.length) {
            files.push({ name:'callflow.csv', content:
                'Order,Name,Schema,Package,Type,Depth,LOC,Tables,CallsOut,Variables,Cursors,Exceptions,SourceFile\n' +
                nodes.map(function(n){ var c=n.counts||{}; return [n.order,n.name,n.schema,n.packageName,n.objectType,n.depth,n.linesOfCode,c.tables,c.callsOut,c.variables,c.cursors,c.exceptionHandlers,n.sourceFile].map(PA.export._csvEsc).join(','); }).join('\n')
            });
        }
        if (sections.tables && data.tables) {
            var arr = Array.isArray(data.tables) ? data.tables : [];
            var rows2 = [];
            arr.forEach(function(t) {
                var tN=t.tableName||t.name||'', tS=t.tableSchema||t.schema||'', tT=t.objectType||'TABLE', dets=t.accessDetails||[];
                if (isDetail) {
                    var opG={};
                    dets.forEach(function(ad){ var op=ad.operation||'UNKNOWN'; if(!opG[op])opG[op]={procs:new Set(),count:0}; var pn=ad.procedureName||ad.procedureId||''; if(pn)opG[op].procs.add(pn); opG[op].count++; });
                    if (!Object.keys(opG).length) rows2.push([tN,tS,tT,(t.operations||[]).join(';'),'',t.accessCount||0].map(PA.export._csvEsc).join(','));
                    else Object.entries(opG).forEach(function(kv){ rows2.push([tN,tS,tT,kv[0],Array.from(kv[1].procs).join(';'),kv[1].count].map(PA.export._csvEsc).join(',')); });
                } else {
                    var co=function(op){return dets.filter(function(a){return a.operation===op;}).length;};
                    rows2.push([tN,tS,tT,co('SELECT'),co('INSERT'),co('UPDATE'),co('DELETE'),co('DYNAMIC'),t.accessCount||dets.length].map(PA.export._csvEsc).join(','));
                }
            });
            files.push({ name:'tableops.csv', content:(isDetail?'TableName,Schema,ObjectType,Operation,Procedure,AccessCount':'TableName,Schema,ObjectType,SELECT,INSERT,UPDATE,DELETE,DYNAMIC,Total')+'\n'+rows2.join('\n') });
        }
        if (sections.joins && data.joins) {
            var arr3=Array.isArray(data.joins)?data.joins:Object.values(data.joins||{});
            var rows3=[];
            arr3.forEach(function(j){ if(isDetail)(j.accessDetails||[{}]).forEach(function(d){ rows3.push([j.leftTable,j.rightTable,d.joinType||'',d.onPredicate||'',d.procedureName||'',d.lineNumber||''].map(PA.export._csvEsc).join(',')); }); else rows3.push([j.leftTable,j.rightTable,(j.joinTypes||[]).join(';'),j.accessCount||0].map(PA.export._csvEsc).join(',')); });
            files.push({ name:'joins.csv', content:(isDetail?'LeftTable,RightTable,JoinType,Predicate,Procedure,Line':'LeftTable,RightTable,JoinTypes,AccessCount')+'\n'+rows3.join('\n') });
        }
        if (sections.cursors && data.cursors) {
            var arr4=Array.isArray(data.cursors)?data.cursors:Object.values(data.cursors||{});
            var rows4=[];
            arr4.forEach(function(c){ if(isDetail)(c.accessDetails||[{}]).forEach(function(d){ rows4.push([c.cursorName,c.cursorType,d.operation||'',d.procedureName||'',d.lineNumber||'',(c.queryText||'').substring(0,400)].map(PA.export._csvEsc).join(',')); }); else rows4.push([c.cursorName,c.cursorType,(c.operations||[]).join(';'),c.accessCount||0,(c.queryText||'').substring(0,200)].map(PA.export._csvEsc).join(',')); });
            files.push({ name:'cursors.csv', content:(isDetail?'CursorName,Type,Operation,Procedure,Line,QueryText':'CursorName,Type,Operations,AccessCount,QueryText')+'\n'+rows4.join('\n') });
        }
        if (sections.sequences && data.sequences) {
            var arr5=Array.isArray(data.sequences)?data.sequences:Object.values(data.sequences||{});
            files.push({ name:'sequences.csv', content:'SequenceName,Schema,Operations,AccessCount\n'+arr5.map(function(s){return [s.sequenceName,s.sequenceSchema,(s.operations||[]).join(';'),s.accessCount||0].map(PA.export._csvEsc).join(',');}).join('\n') });
        }
        files.forEach(function(f){ PA.export._triggerDownload(new Blob([f.content],{type:'text/csv'}), name+'_'+f.name); });
    },

    _csvEsc(v) {
        var s = String(v == null ? '' : v);
        return (s.indexOf(',')>=0||s.indexOf('"')>=0||s.indexOf('\n')>=0) ? '"'+s.replace(/"/g,'""')+'"' : s;
    },

    _triggerDownload(blob, filename) {
        var url = URL.createObjectURL(blob);
        var a   = document.createElement('a');
        a.href = url; a.download = filename;
        document.body.appendChild(a); a.click();
        setTimeout(function(){ document.body.removeChild(a); URL.revokeObjectURL(url); }, 150);
    }
};
