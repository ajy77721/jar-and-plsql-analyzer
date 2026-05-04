/**
 * Summary export — proper styled .xlsx with multiple worksheets via xlsx-js-style.
 * Each analysis section gets its own sheet with colored headers, borders, alternating rows.
 *
 * Load order: summary-export-style.js (shared styles) -> this file
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /** Sanitize a string value for safe Excel cell insertion (prevent formula injection). */
    _xlsSafe(v) {
        if (typeof v !== 'string') return v;
        // Prefix with single quote if starts with formula trigger chars
        if (v.length > 0 && '=+\-@\t\r'.indexOf(v.charAt(0)) !== -1) return "'" + v;
        return v;
    },

    /** Frontend (browser-side) Excel export using xlsx-js-style */
    _exportXlsxFrontend(sel) {
        const jarName = JA.app.currentAnalysis?.jarName || 'analysis';
        JA.toast.info('Generating Excel report (frontend)...');
        try {
            const wb = XLSX.utils.book_new();
            this._xlsxSummarySheet(wb);
            if (sel.endpoints) this._xlsxEndpointsSheet(wb);
            if (sel.endpoints) this._xlsxEndpointCollsSheet(wb);
            if (sel.collections) this._xlsxCollectionsSheet(wb);
            if (sel.collections) this._xlsxCollSummarySheet(wb);
            if (sel.collections) this._xlsxCollUsageDetailSheet(wb);
            if (sel.collections) this._xlsxCallPathsSheet(wb);
            if (sel.transactions) this._xlsxTransactionsSheet(wb);
            if (sel.batch) this._xlsxBatchSheet(wb);
            if (sel.views) this._xlsxViewsSheet(wb);
            if (sel.external) this._xlsxExternalSheet(wb);
            if (sel.external) this._xlsxExtCallsDetailSheet(wb);
            this._xlsxVertMethodCallsSheet(wb);
        this._xlsxVertCrossDomainSheet(wb);
            XLSX.writeFile(wb, this._modalFilename || this._exportFileName(jarName, 'xlsx'), { cellStyles: true });
            JA.toast.success('Excel report downloaded (frontend)');
        } catch (err) {
            console.error('Frontend Excel export failed:', err);
            JA.toast.error('Frontend Excel export failed: ' + err.message);
        }
    },

    /** Backend (server-side) Excel export via Apache POI */
    async _exportXlsx(sel) {
        const jarName = JA.app.currentAnalysis?.jarName || 'analysis';

        // Build report payload — only include selected sections
        // Use a replacer to convert JS Sets to arrays for JSON serialization
        const setReplacer = (key, value) => value instanceof Set ? [...value] : value;

        const payload = { jarName };
        // Always include epReports for Summary sheet
        if (this._epReports?.length) payload.epReports = JSON.parse(JSON.stringify(this._epReports, setReplacer));
        if (sel.collections && this._vertReport?.length) payload.vertReport = JSON.parse(JSON.stringify(this._vertReport, setReplacer));
        if (sel.transactions && this._distReport?.length) payload.distReport = JSON.parse(JSON.stringify(this._distReport, setReplacer));
        if (sel.batch && this._batchReport?.length) payload.batchReport = JSON.parse(JSON.stringify(this._batchReport, setReplacer));
        if (sel.views && this._viewsReport?.length) payload.viewsReport = JSON.parse(JSON.stringify(this._viewsReport, setReplacer));
        if (sel.external && this._extReport) payload.extReport = JSON.parse(JSON.stringify(this._extReport, setReplacer));
        // Compute vertVerReport lazily if tab was never opened
        if (!this._vertVerReport && this._epReports?.length && this._buildVertVerification) {
            this._vertVerReport = this._buildVertVerification(this._epReports);
        }
        if (this._vertVerReport) payload.vertVerReport = JSON.parse(JSON.stringify(this._vertVerReport, setReplacer));

        JA.toast.info('Generating Excel report...');
        try {
            const resp = await fetch('/api/jar/jars/export-excel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!resp.ok) throw new Error('Server returned ' + resp.status);
            const blob = await resp.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = this._modalFilename || this._exportFileName(jarName, 'xlsx');
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);
            JA.toast.success('Excel report downloaded');
        } catch (err) {
            console.error('Excel export failed:', err);
            JA.toast.error('Excel export failed: ' + err.message);
        }
    },

    /* ===== Sheet: Summary ===== */
    _xlsxSummarySheet(wb) {
        const ep = this._epReports || [];
        const vert = this._vertReport || [];
        const dist = this._distReport || [];
        const batch = this._batchReport || [];
        const views = this._viewsReport || [];
        const ext = this._extReport?.crossModule || [];
        const jarName = JA.app.currentAnalysis?.jarName || '';

        const domains = new Set();
        vert.forEach(c => { if (c.domain && c.domain !== 'Other') domains.add(c.domain); });
        const txnReq = dist.filter(d => d.transactionRequirement?.startsWith('REQUIRED')).length;
        const crossEps = ep.filter(r => r.externalScopeCalls > 0).length;

        const rows = [
            ['JAR Analyzer — Analysis Report', '', '', '', ''],
            ['JAR', jarName, '', 'Generated', new Date().toLocaleString()],
            [],
            ['KEY METRICS', '', '', '', ''],
            ['Metric', 'Count', 'Details', '', ''],
            ['Total Endpoints', ep.length, 'REST + Batch endpoints analyzed', '', ''],
            ['Total Collections', vert.filter(c => c.type !== 'VIEW').length, 'MongoDB data collections', '', ''],
            ['Total Views', views.length, 'MongoDB views detected', '', ''],
            ['Domains', domains.size, [...domains].sort().join(', '), '', ''],
            ['Cross-Module Endpoints', crossEps, 'Endpoints calling external modules', '', ''],
            ['Transactions Required', txnReq, 'Multi-domain write endpoints', '', ''],
            ['Batch Jobs', batch.length, 'Scheduled/batch endpoint flows', '', ''],
            ['External JAR Dependencies', ext.length, 'Internal dependency JARs called', '', ''],
            [],
            ['SIZE DISTRIBUTION', '', '', '', ''],
            ['Category', 'Count', 'Threshold', '', ''],
        ];
        const sizes = { S: 0, M: 0, L: 0, XL: 0 };
        ep.forEach(r => sizes[r.sizeCategory] = (sizes[r.sizeCategory] || 0) + 1);
        rows.push(['S (Small)', sizes.S, '<= 5 methods', '', '']);
        rows.push(['M (Medium)', sizes.M, '6-20 methods', '', '']);
        rows.push(['L (Large)', sizes.L, '21-50 methods', '', '']);
        rows.push(['XL (Extra Large)', sizes.XL, '> 50 methods', '', '']);

        rows.push([]);
        rows.push(['HTTP METHOD BREAKDOWN', '', '', '', '']);
        rows.push(['Method', 'Count', '', '', '']);
        const methods = {};
        ep.forEach(r => methods[r.httpMethod] = (methods[r.httpMethod] || 0) + 1);
        for (const [m, c] of Object.entries(methods).sort()) rows.push([m, c, '', '', '']);

        rows.push([]);
        const opsRowStart = rows.length;
        rows.push(['OPERATION TYPES', '', '', '', '']);
        rows.push(['Operation', 'Endpoints Using', '', '', '']);
        const ops = {};
        ep.forEach(r => r.operationTypes.forEach(op => ops[op] = (ops[op] || 0) + 1));
        for (const [op, c] of Object.entries(ops).sort()) rows.push([op, c, '', '', '']);

        rows.push([]);
        rows.push(['DOMAIN BREAKDOWN', '', '', '', '']);
        rows.push(['Domain', 'Collections', 'Views', 'Endpoints', '']);
        const domData = {};
        vert.forEach(c => {
            if (!domData[c.domain]) domData[c.domain] = { colls: 0, views: 0, eps: new Set() };
            if (c.type === 'VIEW') domData[c.domain].views++;
            else domData[c.domain].colls++;
            c.endpoints.forEach(ep => domData[c.domain].eps.add(ep));
        });
        for (const [d, data] of Object.entries(domData).sort((a, b) => a[0].localeCompare(b[0]))) {
            rows.push([d, data.colls, data.views, data.eps.size, '']);
        }

        const cols = [{ wch: 28 }, { wch: 14 }, { wch: 44 }, { wch: 14 }, { wch: 24 }];
        const ws = XLSX.utils.aoa_to_sheet(rows);
        ws['!cols'] = cols;

        // Style: title row
        this._xlsxStyleRow(ws, 5, 0, 'title');
        // Section headers
        const sectionIdxs = [3, 14, 21, opsRowStart, rows.length - Object.keys(domData).length - 2];
        for (const si of sectionIdxs) {
            if (si >= 0 && si < rows.length) this._xlsxStyleRow(ws, 5, si, 'section');
        }
        // Sub-headers
        for (const hi of [1, 4, 15, 22, opsRowStart + 1]) {
            if (hi < rows.length) this._xlsxStyleRow(ws, 5, hi, 'header');
        }
        // Data rows styling
        const S = this._XS;
        let altIdx = 0;
        for (let r = 0; r < rows.length; r++) {
            if (!rows[r] || rows[r].every(v => v === '' || v === undefined || v === null)) continue;
            const addr0 = XLSX.utils.encode_cell({ r, c: 0 });
            if (ws[addr0]?.s?.fill) continue; // already styled as header/section
            const isAlt = altIdx % 2 === 1;
            for (let c = 0; c < 5; c++) {
                const addr = XLSX.utils.encode_cell({ r, c });
                if (!ws[addr]) ws[addr] = { v: '', t: 's' };
                ws[addr].s = {
                    font: S.dataFont, border: S.thinBorder,
                    alignment: { vertical: 'top' },
                    ...(isAlt ? { fill: S.altRowFill } : {})
                };
            }
            altIdx++;
        }

        XLSX.utils.book_append_sheet(wb, ws, 'Summary');
    },

    /* ===== Sheet: Endpoints Overview ===== */
    _xlsxEndpointsSheet(wb) {
        const ep = this._epReports || [];
        const headers = [
            '#', 'HTTP', 'Path', 'Endpoint Name', 'Type', 'ProcName', 'ProcName Source', 'All ProcNames',
            'Domain', 'Domains', 'Collections', 'Views', 'DB Ops', 'Methods', 'LOC',
            'Internal', 'External', 'Operations', 'Write Collections', 'Read Collections',
            'Agg Collections', 'Views Used', 'Service Beans', 'Modules', 'Size',
            'Performance', 'Cross-Domain'
        ];
        const rows = [headers];
        const safe = this._xlsSafe.bind(this);
        ep.forEach((r, i) => {
            const procSrc = r.procNameSources && r.procName && r.procNameSources[r.procName];
            const procSrcStr = procSrc ? (procSrc.simpleClassName || '') + '.' + (procSrc.methodName || '') : '';
            rows.push([
                i + 1, r.httpMethod, safe(r.fullPath), safe(r.endpointName),
                r.typeOfEndpoint || '', safe(r.procName || ''), safe(procSrcStr),
                safe((r.allProcNames || []).join(', ')),
                r.primaryDomain, (r.domains || []).join(', '),
                r.totalCollections, r.totalViews, r.totalDbOperations, r.totalMethods,
                r.totalLoc || 0,
                r.inScopeCalls, r.externalScopeCalls,
                (r.operationTypes || []).join(', '),
                safe((r.writeCollections || []).join(', ')),
                safe((r.readCollections || []).join(', ')),
                safe((r.aggregateCollections || []).join(', ')),
                safe((r.viewsUsed || []).join(', ')),
                safe((r.serviceClasses || []).join(', ')),
                (r.modules || []).join(', '),
                r.sizeCategory, r.performanceImplication, r.crossDomainCount
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 8 }, { wch: 34 }, { wch: 28 }, { wch: 12 }, { wch: 18 },
            { wch: 28 }, { wch: 24 }, { wch: 16 }, { wch: 20 }, { wch: 8 }, { wch: 7 }, { wch: 8 },
            { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 22 }, { wch: 26 }, { wch: 26 },
            { wch: 22 }, { wch: 22 }, { wch: 28 }, { wch: 18 }, { wch: 6 }, { wch: 16 },
            { wch: 8 }
        ];
        this._xlsxAddStyledSheet(wb, 'Endpoints', rows, cols, {
            conditionalFn: (ws, rows) => {
                // Highlight size/performance columns
                for (let r = 1; r < rows.length; r++) {
                    const size = rows[r][24];
                    if (size === 'XL') this._xlsxCellFill(ws, r, 24, 'redFill');
                    else if (size === 'L') this._xlsxCellFill(ws, r, 24, 'orangeFill');
                    const perf = rows[r][25];
                    if (perf && perf.includes('SLOW')) this._xlsxCellFill(ws, r, 25, 'redFill');
                    else if (perf && perf.includes('MEDIUM')) this._xlsxCellFill(ws, r, 25, 'yellowFill');
                    // Highlight cross-module endpoints
                    if (rows[r][16] > 0) this._xlsxCellFill(ws, r, 16, 'orangeFill');
                    // Highlight high LOC
                    const loc = rows[r][14];
                    if (loc > 500) this._xlsxCellFill(ws, r, 14, 'redFill');
                    else if (loc > 200) this._xlsxCellFill(ws, r, 14, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet: Endpoint-Collection Detail ===== */
    _xlsxEndpointCollsSheet(wb) {
        const ep = this._epReports || [];
        const headers = [
            'Endpoint', 'HTTP', 'Path', 'Type', 'ProcName', 'Collection', 'Coll Domain',
            'Detected Via', 'Operation', 'Coll Type', 'References (All)', 'Call Path'
        ];
        const rows = [headers];
        ep.forEach(r => {
            const colls = r.collections || {};
            for (const [collName, info] of Object.entries(colls)) {
                const opsArr = info.operations ? [...info.operations] : [];
                const refsArr = info.sources ? [...info.sources] : [];
                const detectedVia = info.detectedVia ? [...info.detectedVia].join(', ') : '';
                const breadcrumbs = (r.collBreadcrumbs && r.collBreadcrumbs[collName]) || [];
                const allPaths = breadcrumbs.map(bc =>
                    bc.map(n => (n.simpleClassName || '?') + '.' + (n.methodName || '?')).join(' > ')
                ).join(' || ');
                for (const op of (opsArr.length > 0 ? opsArr : [''])) {
                    rows.push([
                        r.endpointName, r.httpMethod, r.fullPath,
                        r.typeOfEndpoint || '', r.procName || '',
                        collName, info.domain || '', detectedVia,
                        op, info.type || 'COLLECTION',
                        refsArr.join('; '), allPaths
                    ]);
                }
            }
        });
        const cols = [
            { wch: 28 }, { wch: 8 }, { wch: 32 }, { wch: 12 }, { wch: 18 }, { wch: 24 },
            { wch: 16 }, { wch: 20 }, { wch: 14 }, { wch: 12 }, { wch: 38 }, { wch: 46 }
        ];
        this._xlsxAddStyledSheet(wb, 'Endpoint-Collections', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    const op = (rows[r][8] || '').toUpperCase();
                    if (op.includes('WRITE') || op.includes('INSERT') || op.includes('UPDATE') || op.includes('DELETE'))
                        this._xlsxCellFill(ws, r, 8, 'orangeFill');
                    else if (op.includes('AGGREGATE'))
                        this._xlsxCellFill(ws, r, 8, 'purpleFill');
                    const type = (rows[r][9] || '');
                    if (type === 'VIEW') this._xlsxCellFill(ws, r, 9, 'yellowFill');
                }
            }
        });
    },

    /* ===== Sheet: Collections ===== */
    _xlsxCollectionsSheet(wb) {
        const vert = this._vertReport || [];
        const headers = [
            '#', 'Collection', 'Type', 'Domain', 'Status',
            'Detected Via', 'Read Ops', 'Write Ops', 'All Operations',
            'Find', 'Agg', 'Save', 'Update', 'Delete', 'Call',
            'Usage Count', 'Endpoint Count', 'Complexity', 'Score',
            'Endpoints', 'ProcNames', 'References (All)'
        ];
        const rows = [headers];
        vert.forEach((c, i) => {
            const oc = c.opCounts || {};
            const verifLabel = c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED' ? 'Verified'
                : c.verification === 'NOT_IN_DB' ? 'Ambiguous' : 'Unknown';
            rows.push([
                i + 1, c.name, c.type || 'COLLECTION', c.domain, verifLabel,
                c.detectedVia ? [...c.detectedVia].join(', ') : '',
                c.readOps ? [...c.readOps].join(', ') : '',
                c.writeOps ? [...c.writeOps].join(', ') : '',
                [...c.operations].join(', '),
                (oc.READ || 0) + (oc.COUNT || 0), oc.AGGREGATE || 0,
                oc.WRITE || 0, oc.UPDATE || 0, oc.DELETE || 0, oc.CALL || 0,
                c.usageCount, c.endpoints.size,
                c._complexity || 'Low', c._complexityScore || 0,
                [...c.endpoints].join(', '),
                c.procNames ? [...c.procNames].join(', ') : '',
                [...c.sources].join('; ')
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 28 }, { wch: 12 }, { wch: 16 }, { wch: 12 },
            { wch: 20 }, { wch: 20 }, { wch: 20 }, { wch: 22 },
            { wch: 6 }, { wch: 6 }, { wch: 6 }, { wch: 7 }, { wch: 7 },
            { wch: 10 }, { wch: 10 }, { wch: 12 }, { wch: 8 },
            { wch: 38 }, { wch: 26 }, { wch: 38 }
        ];
        this._xlsxAddStyledSheet(wb, 'Collections', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    if (rows[r][2] === 'VIEW') this._xlsxCellFill(ws, r, 2, 'yellowFill');
                    if (rows[r][4] === 'Verified') this._xlsxCellFill(ws, r, 4, 'greenFill');
                    else if (rows[r][4] === 'Ambiguous') this._xlsxCellFill(ws, r, 4, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet: Collection Summary (aggregated counts from vertReport) ===== */
    _xlsxCollSummarySheet(wb) {
        const vert = this._vertReport || [];

        const headers = [
            '#', 'Collection', 'Type', 'Domain', 'Status', 'Detected Via',
            'Read', 'Write', 'Agg', 'Total Ops',
            'Endpoints', 'Usage', 'Complexity', 'Score',
            'Distinct Classes', 'Distinct Methods'
        ];
        const rows = [headers];
        const sorted = [...vert].sort((a, b) => {
            const ae = a.endpoints ? (a.endpoints.size != null ? a.endpoints.size : a.endpoints.length || 0) : 0;
            const be = b.endpoints ? (b.endpoints.size != null ? b.endpoints.size : b.endpoints.length || 0) : 0;
            return be - ae;
        });
        sorted.forEach((c, i) => {
            const oc = c.opCounts || {};
            const read = (oc.READ || 0) + (oc.COUNT || 0) + (oc.EXISTS || 0);
            const write = (oc.WRITE || 0) + (oc.UPDATE || 0) + (oc.DELETE || 0) + (oc.CALL || 0);
            const agg = oc.AGGREGATE || 0;
            const totalOps = read + write + agg;
            const epCount = c.endpoints ? (c.endpoints.size != null ? c.endpoints.size : c.endpoints.length || 0) : 0;
            const verifLabel = c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED' ? 'Verified'
                : c.verification === 'NOT_IN_DB' ? 'Ambiguous' : 'Unknown';
            const sources = c.sources ? [...c.sources] : [];
            const classes = new Set();
            const methods = new Set();
            for (const ref of sources) {
                const dotIdx = ref.lastIndexOf('.');
                if (dotIdx > 0) { classes.add(ref.substring(0, dotIdx)); methods.add(ref); }
            }
            rows.push([
                i + 1, c.name, c.type || 'COLLECTION', c.domain || '', verifLabel,
                c.detectedVia ? [...c.detectedVia].join(', ') : '',
                read, write, agg, totalOps,
                epCount, c.usageCount || 0,
                c._complexity || 'Low', c._complexityScore || 0,
                classes.size, methods.size
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 28 }, { wch: 12 }, { wch: 16 }, { wch: 12 }, { wch: 20 },
            { wch: 6 }, { wch: 6 }, { wch: 6 }, { wch: 8 },
            { wch: 10 }, { wch: 8 }, { wch: 12 }, { wch: 8 },
            { wch: 14 }, { wch: 14 }
        ];
        this._xlsxAddStyledSheet(wb, 'Coll Summary', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    if (rows[r][2] === 'VIEW') this._xlsxCellFill(ws, r, 2, 'yellowFill');
                    if (rows[r][4] === 'Verified') this._xlsxCellFill(ws, r, 4, 'greenFill');
                    else if (rows[r][4] === 'Ambiguous') this._xlsxCellFill(ws, r, 4, 'orangeFill');
                    if (rows[r][10] >= 10) this._xlsxCellFill(ws, r, 10, 'greenFill');
                }
            }
        });
    },

    /* ===== Sheet: Collection Usage Detail (per endpoint-operation row) ===== */
    _xlsxCollUsageDetailSheet(wb) {
        const ep = this._epReports || [];
        const headers = [
            'Collection', 'Domain', 'Type', 'Endpoint', 'HTTP', 'Path',
            'Operation', 'Accessing Class (Full)', 'Accessing Class (Simple)',
            'Accessing Method', 'Detected Via', 'Call Path'
        ];
        const rows = [headers];

        for (const r of ep) {
            for (const [collName, info] of Object.entries(r.collections || {})) {
                const opsArr = info.operations ? [...info.operations] : [''];
                const refsArr = info.sources ? [...info.sources] : [''];
                const detectedVia = info.detectedVia ? [...info.detectedVia].join(', ') : '';
                const breadcrumbs = (r.collBreadcrumbs && r.collBreadcrumbs[collName]) || [];
                const pathStr = breadcrumbs.map(bc =>
                    bc.map(n => (n.simpleClassName || '?') + '.' + (n.methodName || '?')).join(' > ')
                ).join(' || ');

                for (const op of opsArr) {
                    for (const ref of refsArr) {
                        const dotIdx = ref.lastIndexOf('.');
                        const fullClass = dotIdx > 0 ? ref.substring(0, dotIdx) : ref;
                        const simpleClass = fullClass.includes('.') ? fullClass.substring(fullClass.lastIndexOf('.') + 1) : fullClass;
                        const method = dotIdx > 0 ? ref.substring(dotIdx + 1) : '';
                        rows.push([
                            collName, info.domain || '', info.type || 'COLLECTION',
                            r.endpointName, r.httpMethod, r.fullPath,
                            op, fullClass, simpleClass,
                            method, detectedVia, pathStr
                        ]);
                    }
                }
            }
        }

        if (rows.length <= 1) return;

        // Sort by collection name for readability
        const header = rows.shift();
        rows.sort((a, b) => (a[0] || '').localeCompare(b[0] || '') || (a[3] || '').localeCompare(b[3] || ''));
        rows.unshift(header);

        const cols = [
            { wch: 26 }, { wch: 16 }, { wch: 12 }, { wch: 28 }, { wch: 8 }, { wch: 32 },
            { wch: 14 }, { wch: 34 }, { wch: 20 },
            { wch: 20 }, { wch: 18 }, { wch: 42 }
        ];
        this._xlsxAddStyledSheet(wb, 'Coll Usage Detail', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    const op = (rows[r][6] || '').toUpperCase();
                    if (op.includes('WRITE') || op.includes('INSERT') || op.includes('UPDATE') || op.includes('DELETE'))
                        this._xlsxCellFill(ws, r, 6, 'orangeFill');
                    else if (op.includes('AGGREGATE'))
                        this._xlsxCellFill(ws, r, 6, 'purpleFill');
                }
            }
        });
    },

    /* ===== Sheet: Call Paths (per collection per endpoint) ===== */
    _xlsxCallPathsSheet(wb) {
        const ep = this._epReports || [];
        if (!ep.length) return;

        const headers = [
            '#', 'Endpoint', 'HTTP', 'Path', 'ProcName',
            'Collection', 'Domain', 'Type', 'Operations',
            'Call Path', 'Depth',
            'Entry Class', 'Entry Method', 'Exit Class', 'Exit Method',
            'Detected Via'
        ];
        const rows = [headers];
        const safe = this._xlsSafe.bind(this);
        let rowNum = 1;

        for (const r of ep) {
            for (const [collName, info] of Object.entries(r.collections || {})) {
                const breadcrumbs = (r.collBreadcrumbs && r.collBreadcrumbs[collName]) || [];
                const opsStr = info.operations ? [...info.operations].join(', ') : '';
                const detectedStr = info.detectedVia ? [...info.detectedVia].join(', ') : '';

                if (breadcrumbs.length === 0) {
                    rows.push([
                        rowNum++,
                        safe(r.endpointName), r.httpMethod, safe(r.fullPath), safe(r.procName || ''),
                        safe(collName), safe(info.domain || ''), info.type || 'COLLECTION',
                        safe(opsStr),
                        '', 0,
                        '', '', '', '',
                        safe(detectedStr)
                    ]);
                } else {
                    for (const bc of breadcrumbs) {
                        const pathStr = bc.map(n =>
                            (n.simpleClassName || '?') + '.' + (n.methodName || '?')
                        ).join(' \u2192 ');
                        const depth = bc.length;
                        const entry = bc[0] || {};
                        const exit = bc[bc.length - 1] || {};
                        rows.push([
                            rowNum++,
                            safe(r.endpointName), r.httpMethod, safe(r.fullPath), safe(r.procName || ''),
                            safe(collName), safe(info.domain || ''), info.type || 'COLLECTION',
                            safe(opsStr),
                            safe(pathStr), depth,
                            safe(entry.simpleClassName || ''), safe(entry.methodName || ''),
                            safe(exit.simpleClassName || ''), safe(exit.methodName || ''),
                            safe(detectedStr)
                        ]);
                    }
                }
            }
        }

        if (rows.length <= 1) return;

        // Sort by collection name, then endpoint name
        const header = rows.shift();
        rows.sort((a, b) =>
            (a[5] || '').localeCompare(b[5] || '') || (a[1] || '').localeCompare(b[1] || '')
        );
        // Re-number after sort
        rows.forEach((row, i) => { row[0] = i + 1; });
        rows.unshift(header);

        const cols = [
            { wch: 5 }, { wch: 28 }, { wch: 8 }, { wch: 32 }, { wch: 18 },
            { wch: 26 }, { wch: 16 }, { wch: 12 }, { wch: 20 },
            { wch: 60 }, { wch: 6 },
            { wch: 22 }, { wch: 22 }, { wch: 22 }, { wch: 22 },
            { wch: 20 }
        ];
        this._xlsxAddStyledSheet(wb, 'Call Paths', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    const ops = (rows[r][8] || '').toUpperCase();
                    if (ops.includes('WRITE') || ops.includes('UPDATE') || ops.includes('DELETE'))
                        this._xlsxCellFill(ws, r, 8, 'orangeFill');
                    else if (ops.includes('AGGREGATE'))
                        this._xlsxCellFill(ws, r, 8, 'purpleFill');
                    // Highlight deep call paths
                    if (rows[r][10] >= 5) this._xlsxCellFill(ws, r, 10, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet: Distributed Transactions ===== */
    _xlsxTransactionsSheet(wb) {
        const dist = this._distReport || [];
        const headers = [
            '#', 'Endpoint', 'ProcName', 'All ProcNames', 'HTTP', 'Path', 'Type',
            'Primary Domain', 'Transaction Req', 'Total Collections', 'LOC',
            'Write Domains', 'Read Domains', 'Agg Domains', 'Performance',
            'Cross-Domain Details'
        ];
        const rows = [headers];
        dist.forEach((d, i) => {
            const deps = d.crossDomainDependencies || {};
            const wDoms = Object.entries(deps).filter(([_, v]) => v.write?.length > 0).map(([k, v]) => k + ': ' + v.write.join(','));
            const rDoms = Object.entries(deps).filter(([_, v]) => v.read?.length > 0).map(([k, v]) => k + ': ' + v.read.join(','));
            const aDoms = Object.entries(deps).filter(([_, v]) => v.aggregate?.length > 0).map(([k, v]) => k + ': ' + v.aggregate.join(','));
            const details = Object.entries(deps).map(([domain, v]) => {
                const parts = [];
                if (v.write?.length) parts.push('WRITE: ' + v.write.join(', '));
                if (v.read?.length) parts.push('READ: ' + v.read.join(', '));
                if (v.aggregate?.length) parts.push('AGG: ' + v.aggregate.join(', '));
                return domain + ' [' + parts.join('; ') + ']';
            }).join(' | ');
            rows.push([
                i + 1, d.endpointName, d.procName || '',
                (d.allProcNames || []).join(', '),
                d.httpMethod || '', d.fullPath || '', d.typeOfEndpoint || '',
                d.primaryDomain, d.transactionRequirement || 'NONE', d.totalCollections,
                d.totalLoc || 0,
                wDoms.join('; '), rDoms.join('; '), aDoms.join('; '),
                d.performanceImplication, details
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 28 }, { wch: 18 }, { wch: 24 }, { wch: 8 }, { wch: 32 },
            { wch: 14 }, { wch: 16 }, { wch: 16 }, { wch: 10 }, { wch: 8 }, { wch: 28 }, { wch: 28 },
            { wch: 24 }, { wch: 16 }, { wch: 42 }
        ];
        this._xlsxAddStyledSheet(wb, 'Transactions', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    const txn = (rows[r][8] || '');
                    if (txn.startsWith('REQUIRED')) this._xlsxCellFill(ws, r, 8, 'redFill');
                    else if (txn === 'RECOMMENDED') this._xlsxCellFill(ws, r, 8, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet: Batch Jobs ===== */
    _xlsxBatchSheet(wb) {
        const batch = this._batchReport || [];
        const headers = [
            '#', 'Name', 'ProcName', 'All ProcNames', 'URL', 'Primary Domain',
            'Domains', 'Collections', 'Methods', 'LOC', 'Size', 'Performance',
            'External Dependencies'
        ];
        const rows = [headers];
        batch.forEach((b, i) => {
            const extDeps = Object.entries(b.externalDependencies || {}).map(([dom, deps]) =>
                dom + ': ' + deps.map(d => d.collection + ' (' + (d.usageTypes || []).join(',') + ')').join(', ')
            ).join(' | ');
            rows.push([
                i + 1, b.batchName, b.procName || '',
                (b.allProcNames || []).join(', '),
                b.endpointUrl || '',
                b.primaryDomain, (b.domains || []).join(', '),
                b.totalCollections, b.totalMethods, b.totalLoc || 0, b.sizeCategory,
                b.performanceImplication, extDeps
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 30 }, { wch: 18 }, { wch: 24 }, { wch: 30 }, { wch: 16 },
            { wch: 20 }, { wch: 10 }, { wch: 10 }, { wch: 8 }, { wch: 6 }, { wch: 16 }, { wch: 42 }
        ];
        this._xlsxAddStyledSheet(wb, 'Batch Jobs', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    const size = rows[r][10];
                    if (size === 'XL') this._xlsxCellFill(ws, r, 10, 'redFill');
                    else if (size === 'L') this._xlsxCellFill(ws, r, 10, 'orangeFill');
                    const loc = rows[r][9];
                    if (loc > 500) this._xlsxCellFill(ws, r, 9, 'redFill');
                    else if (loc > 200) this._xlsxCellFill(ws, r, 9, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet: Views ===== */
    _xlsxViewsSheet(wb) {
        const views = this._viewsReport || [];
        const headers = [
            '#', 'View Name', 'Domain', 'Complexity', 'Usage Count',
            'Used By Endpoints', 'Possible Alternative', 'Recursive', 'View Definition'
        ];
        const rows = [headers];
        views.forEach((v, i) => {
            rows.push([
                i + 1, v.viewName, v.domain, v.complexity, v.usageCount || 0,
                (v.usedByEndpoints || []).join(', '), v.possibleAlternative || '',
                v.recursive ? 'YES' : 'No',
                (v.viewDefinition || []).join(', ')
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 28 }, { wch: 16 }, { wch: 12 }, { wch: 10 },
            { wch: 38 }, { wch: 30 }, { wch: 10 }, { wch: 34 }
        ];
        this._xlsxAddStyledSheet(wb, 'Views', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    if (rows[r][7] === 'YES') this._xlsxCellFill(ws, r, 7, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet: External Dependencies ===== */
    _xlsxExternalSheet(wb) {
        const ext = (this._extReport?.crossModule || []);
        const headers = [
            '#', 'JAR / Module', 'Project', 'Domain', 'Classes', 'Methods',
            'Endpoints Using', 'Total Calls', 'All Methods', 'All Endpoints', 'All Classes'
        ];
        const rows = [headers];
        ext.forEach((e, i) => {
            rows.push([
                i + 1, e.jar || '', e.project || '', e.domain || '',
                e.classes ? e.classes.size || e.classes.length || 0 : 0,
                e.methods ? e.methods.size || e.methods.length || 0 : 0,
                e.endpoints ? e.endpoints.size || e.endpoints.length || 0 : 0,
                e.count || 0,
                e.methods ? [...e.methods].join(', ') : '',
                e.endpoints ? [...e.endpoints].join(', ') : '',
                e.classes ? [...e.classes].join(', ') : ''
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 34 }, { wch: 24 }, { wch: 16 }, { wch: 10 }, { wch: 10 },
            { wch: 12 }, { wch: 10 }, { wch: 42 }, { wch: 38 }, { wch: 34 }
        ];
        this._xlsxAddStyledSheet(wb, 'External Deps', rows, cols);
    },

    /* ===== Sheet: External Calls Detail (per-endpoint) ===== */
    _xlsxExtCallsDetailSheet(wb) {
        const ep = this._epReports || [];
        const headers = [
            'Endpoint', 'HTTP', 'Path', 'ProcName', 'Endpoint Domain',
            'Call Type', 'External Class (Package)', 'External Class (Simple)', 'Method',
            'Stereotype', 'Source JAR', 'Target Module', 'Target Domain', 'URL', 'Call Path'
        ];
        const rows = [headers];
        ep.forEach(r => {
            // Cross-module direct calls
            for (const ext of (r.externalCalls || [])) {
                const breadcrumb = ext.breadcrumb
                    ? ext.breadcrumb.map(s => (s.simpleClassName || '?') + '.' + (s.methodName || '?')).join(' > ')
                    : '';
                rows.push([
                    r.endpointName, r.httpMethod, r.fullPath, r.procName || '',
                    r.primaryDomain, 'CROSS_MODULE',
                    ext.className || '', ext.simpleClassName || '', ext.methodName || '',
                    ext.stereotype || '', ext.sourceJar || '',
                    ext.module || '', ext.domain || '', '', breadcrumb
                ]);
            }
            // HTTP/REST calls
            for (const h of (r.httpCalls || [])) {
                const breadcrumb = h.breadcrumb
                    ? h.breadcrumb.map(s => (s.simpleClassName || '?') + '.' + (s.methodName || '?')).join(' > ')
                    : '';
                rows.push([
                    r.endpointName, r.httpMethod, r.fullPath, r.procName || '',
                    r.primaryDomain, 'HTTP_CALL',
                    h.className || '', h.simpleClassName || '', h.methodName || '',
                    '', '', '', '', h.url || '', breadcrumb
                ]);
            }
        });
        if (rows.length <= 1) return;
        const cols = [
            { wch: 28 }, { wch: 8 }, { wch: 32 }, { wch: 18 }, { wch: 16 },
            { wch: 14 }, { wch: 36 }, { wch: 24 }, { wch: 24 },
            { wch: 14 }, { wch: 28 }, { wch: 20 }, { wch: 16 }, { wch: 34 }, { wch: 44 }
        ];
        this._xlsxAddStyledSheet(wb, 'External Calls Detail', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    if (rows[r][5] === 'HTTP_CALL') this._xlsxCellFill(ws, r, 5, 'purpleFill');
                }
            }
        });
    },

    /** Ensure vertVerReport is computed (lazy if tab never opened) */
    _ensureVertVerReport() {
        if (!this._vertVerReport && this._epReports?.length && this._buildVertVerification) {
            this._vertVerReport = this._buildVertVerification(this._epReports);
        }
        return this._vertVerReport;
    },

    /* ===== Sheet 1: Vert - Method Calls ===== */
    _xlsxVertMethodCallsSheet(wb) {
        const report = this._ensureVertVerReport();
        if (!report) return;
        const beans = report.beans || [];
        if (!beans.length) return;

        const headers = ['#', 'Full Class', 'Simple Class', 'Method', 'Stereotype', 'Owner Module', 'Owner Domain',
            'Caller Domains', 'Calls', 'Endpoints', 'Called By (All)', 'Current', 'Recommended'];
        const rows = [headers];
        beans.forEach((b, i) => {
            rows.push([
                i + 1, b.className || '', b.simpleClassName || '', b.methodName || '',
                b.stereotype || '', b.sourceModule || '', b.sourceDomain || '',
                b.callerDomains ? [...b.callerDomains].join(', ') : '',
                b.callCount || 0,
                b.calledByEndpoints ? (b.calledByEndpoints.size || 0) : 0,
                b.calledByEndpoints ? [...b.calledByEndpoints].join(', ') : '',
                'DIRECT', 'REST API'
            ]);
        });
        const cols = [
            { wch: 5 }, { wch: 44 }, { wch: 30 }, { wch: 26 }, { wch: 14 },
            { wch: 22 }, { wch: 18 }, { wch: 26 },
            { wch: 8 }, { wch: 10 }, { wch: 50 }, { wch: 10 }, { wch: 14 }
        ];
        this._xlsxAddStyledSheet(wb, 'Vert - Method Calls', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    this._xlsxCellFill(ws, r, 12, 'orangeFill');
                }
            }
        });
    },

    /* ===== Sheet 2: Vert - Cross Domain ===== */
    _xlsxVertCrossDomainSheet(wb) {
        const report = this._ensureVertVerReport();
        if (!report) return;
        const allColls = report.collections || [];
        if (!allColls.length) return;
        const pd = report.primaryDomain || '';

        const c2eColls = allColls.filter(c => c.ownerDomain !== pd && c.accessedByDomains && c.accessedByDomains.has(pd));
        const e2cColls = allColls.filter(c => c.ownerDomain === pd && c.accessedByDomains && [...c.accessedByDomains].some(d => d !== pd));
        const flattenOps = (colls) => {
            const flat = [];
            for (const c of colls) {
                for (const op of (c.operations || [])) {
                    flat.push({ name: c.name, type: c.type, ownerDomain: c.ownerDomain,
                        accessedByDomains: c.accessedByDomains, accessedByEndpoints: c.accessedByEndpoints,
                        operations: new Set([op]), _singleOp: op });
                }
            }
            return flat;
        };
        const c2eOps = flattenOps(c2eColls);
        const e2cOps = flattenOps(e2cColls);

        const headers = ['#', 'Collection', 'Section', 'Direction', 'Type', 'Owner Domain',
            'Accessed By Domains', 'Operations', 'Endpoints', 'Accessed By (All)', 'Current', 'Recommended'];
        const rows = [headers];
        let n = 0;
        const addRows = (data, section) => {
            data.forEach(c => {
                const hasWrite = c.operations && (c.operations.has('WRITE') || c.operations.has('UPDATE'));
                const direction = c.ownerDomain === pd ? 'External\u2192' + pd : pd + '\u2192External';
                rows.push([
                    ++n, c.name || '', section, direction, c.type || '', c.ownerDomain || '',
                    c.accessedByDomains ? [...c.accessedByDomains].join(', ') : '',
                    c.operations ? [...c.operations].join(', ') : '',
                    c.accessedByEndpoints ? (c.accessedByEndpoints.size || 0) : 0,
                    c.accessedByEndpoints ? [...c.accessedByEndpoints].join(', ') : '',
                    'DIRECT', hasWrite ? 'REST API' : 'EVALUATE'
                ]);
            });
        };
        addRows(c2eColls, pd + '\u2192Ext Collections');
        addRows(c2eOps,   pd + '\u2192Ext Operations');
        addRows(e2cColls,  'Ext\u2192' + pd + ' Collections');
        addRows(e2cOps,    'Ext\u2192' + pd + ' Operations');

        const cols = [
            { wch: 5 }, { wch: 30 }, { wch: 24 }, { wch: 20 }, { wch: 12 }, { wch: 18 },
            { wch: 26 }, { wch: 22 }, { wch: 10 }, { wch: 50 }, { wch: 10 }, { wch: 14 }
        ];
        this._xlsxAddStyledSheet(wb, 'Vert - Cross Domain', rows, cols, {
            conditionalFn: (ws, rows) => {
                for (let r = 1; r < rows.length; r++) {
                    const rec = rows[r][11];
                    if (rec === 'REST API') this._xlsxCellFill(ws, r, 11, 'orangeFill');
                    const ops = (rows[r][7] || '').toUpperCase();
                    if (ops.includes('WRITE') || ops.includes('UPDATE') || ops.includes('DELETE'))
                        this._xlsxCellFill(ws, r, 7, 'orangeFill');
                }
            }
        });
    }
});
