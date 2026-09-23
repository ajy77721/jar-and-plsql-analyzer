/**
 * Summary export modal — decorative export dialog with JSON/Excel format options.
 * Supports full project export, per-endpoint, and per-collection scoped export.
 *
 * Usage:
 *   JA.summary.showExportModal()                         — full project export
 *   JA.summary.showExportModal({ endpointIdx: 5 })       — single endpoint export
 *   JA.summary.showExportModal({ collectionName: 'xyz' }) — single collection export
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /**
     * @param {object} [scope] - optional scope
     *   null = full project
     *   {endpointIdx: N} = single endpoint
     *   {collectionName: 'X'} = single collection
     */
    showExportModal(scope) {
        if (!this._epReports) { JA.toast.warn('No analysis loaded'); return; }
        const old = document.getElementById('export-modal-overlay');
        if (old) old.remove();

        // Determine scope type
        const isEpScoped = scope && scope.endpointIdx != null;
        const isCollScoped = scope && scope.collectionName != null;
        const isScoped = isEpScoped || isCollScoped;

        let ep, vert, dist, batch, views, scopeLabel = '';
        const esc = JA.utils.escapeHtml;

        if (isEpScoped) {
            const r = this._epReports[scope.endpointIdx];
            if (!r) { JA.toast.warn('Endpoint not found'); return; }
            ep = [r];
            vert = this._scopeCollections(r);
            dist = this._scopeTransactions(r);
            batch = this._scopeBatch(r);
            views = this._scopeViews(r);
            scopeLabel = `<div class="export-scope-badge">Single Endpoint: <strong>${esc(r.endpointName)}</strong></div>`;
        } else if (isCollScoped) {
            const cn = scope.collectionName;
            ep = this._scopeEpByCollection(cn);
            vert = this._scopeVertByCollection(cn);
            dist = this._scopeDistByCollection(cn, ep);
            batch = this._scopeBatchByCollection(cn, ep);
            views = this._scopeViewsByCollection(cn);
            scopeLabel = `<div class="export-scope-badge">Single Collection: <strong>${esc(cn)}</strong></div>`;
        } else {
            // Eagerly build any reports that were lazily skipped (user never clicked those tabs)
            if (!this._vertReport) {
                this._vertReport = this._buildVerticalisation(this._epReports);
                this._viewsReport = this._buildViewsAnalysis(this._vertReport, this._epReports);
            }
            if (!this._extReport) this._extReport = this._buildExternalDeps(this._epReports);
            if (!this._distReport) this._distReport = this._buildDistributedTransactions(this._epReports);
            if (!this._batchReport) this._batchReport = this._buildBatchAnalysis(this._epReports);
            ep = this._epReports || [];
            vert = this._vertReport || [];
            dist = this._distReport || [];
            batch = this._batchReport || [];
            views = this._viewsReport || [];
        }

        const jarName = esc(JA.app.currentAnalysis?.jarName || 'analysis');

        let html = `<div class="export-modal-overlay" id="export-modal-overlay" onclick="if(event.target===this)this.remove()">
        <div class="export-modal-panel">
            <div class="export-modal-header">
                <span class="export-modal-title">${isEpScoped ? 'Export Endpoint' : isCollScoped ? 'Export Collection' : 'Export Analysis'}</span>
                <button class="btn-sm" onclick="document.getElementById('export-modal-overlay').remove()">Close</button>
            </div>
            <div class="export-modal-body">
                ${scopeLabel}
                <div class="export-summary-bar">
                    <div class="export-stat"><span class="export-stat-num">${ep.length}</span><span class="export-stat-lbl">Endpoints</span></div>
                    <div class="export-stat"><span class="export-stat-num">${vert.length}</span><span class="export-stat-lbl">Collections</span></div>
                    <div class="export-stat"><span class="export-stat-num">${views.length}</span><span class="export-stat-lbl">Views</span></div>
                    <div class="export-stat"><span class="export-stat-num">${dist.filter(d => d.transactionRequirement?.startsWith('REQUIRED')).length}</span><span class="export-stat-lbl">Txn Required</span></div>
                    <div class="export-stat"><span class="export-stat-num">${batch.length}</span><span class="export-stat-lbl">Batch Jobs</span></div>
                </div>
                <div class="export-jar-name">${jarName}</div>

                <div class="export-section-title">Select Reports</div>
                <div class="export-checks">
                    <label class="export-check"><input type="checkbox" id="exp-endpoints" checked><span>Endpoint Report</span><span class="export-check-count">${ep.length}</span></label>
                    <label class="export-check"><input type="checkbox" id="exp-collections" checked><span>Collection Analysis</span><span class="export-check-count">${vert.length}</span></label>
                    <label class="export-check"><input type="checkbox" id="exp-transactions" checked><span>Distributed Transactions</span><span class="export-check-count">${dist.length}</span></label>
                    <label class="export-check"><input type="checkbox" id="exp-batch" checked><span>Batch Jobs</span><span class="export-check-count">${batch.length}</span></label>
                    <label class="export-check"><input type="checkbox" id="exp-views" checked><span>Views Analysis</span><span class="export-check-count">${views.length}</span></label>
                    <label class="export-check"><input type="checkbox" id="exp-external" checked><span>External Dependencies</span><span class="export-check-count">${isEpScoped ? this._epReports[scope.endpointIdx]?.externalScopeCalls || 0 : (this._extReport?.crossModule || []).length}</span></label>
                </div>

                <div class="export-section-title">Format</div>
                <div class="export-formats">
                    <button class="export-format-btn" id="exp-fmt-json" onclick="JA.summary._setExportFmt('json')">
                        <span class="export-fmt-icon">{ }</span>
                        <span class="export-fmt-name">JSON</span>
                        <span class="export-fmt-desc">Structured data, re-importable</span>
                    </button>
                    <button class="export-format-btn" id="exp-fmt-csv" onclick="JA.summary._setExportFmt('csv')">
                        <span class="export-fmt-icon">&#9776;</span>
                        <span class="export-fmt-name">CSV</span>
                        <span class="export-fmt-desc">Flat table, spreadsheet-ready</span>
                    </button>
                    <button class="export-format-btn export-format-active" id="exp-fmt-excel" onclick="JA.summary._setExportFmt('excel')">
                        <span class="export-fmt-icon">&#9638;</span>
                        <span class="export-fmt-name">Excel (.xlsx)</span>
                        <span class="export-fmt-desc">Multi-sheet workbook, all details</span>
                    </button>
                    <button class="export-format-btn" id="exp-fmt-zip" onclick="JA.summary._setExportFmt('zip')">
                        <span class="export-fmt-icon">&#128230;</span>
                        <span class="export-fmt-name">Download as ZIP</span>
                        <span class="export-fmt-desc">One bundle: reports + decompiled source</span>
                    </button>
                </div>

                <div class="export-section-title" id="exp-zip-section" style="display:none">Bundle options (ZIP only)</div>
                <div class="export-checks" id="exp-zip-opts" style="display:none">
                    <label class="export-check"><input type="checkbox" id="exp-zip-cached-source" checked><span>Include already-cached decompiled source</span><span class="export-check-count" id="exp-zip-cached-count">&hellip;</span></label>
                    <label class="export-check"><input type="checkbox" id="exp-zip-all-source"><span>Include ALL decompiled source (may take a while — runs as a background job)</span></label>
                </div>

                <div class="export-section-title" id="exp-source-section">Excel Source</div>
                <div class="export-formats" id="exp-source-btns">
                    <button class="export-format-btn export-format-active" id="exp-src-backend" onclick="JA.summary._setExportSrc('backend')">
                        <span class="export-fmt-icon">&#9881;</span>
                        <span class="export-fmt-name">Server (Backend)</span>
                        <span class="export-fmt-desc">Apache POI — full colors &amp; styling</span>
                    </button>
                    <button class="export-format-btn" id="exp-src-frontend" onclick="JA.summary._setExportSrc('frontend')">
                        <span class="export-fmt-icon">&#9998;</span>
                        <span class="export-fmt-name">Browser (Frontend)</span>
                        <span class="export-fmt-desc">xlsx-js-style — no server needed</span>
                    </button>
                </div>

                <div class="export-section-title">Filename</div>
                <div class="export-filename-row">
                    <input type="text" id="exp-filename" class="export-filename-input" value="">
                    <span class="export-filename-ext" id="exp-filename-ext">.xlsx</span>
                </div>

                <div class="export-actions">
                    <button class="export-go-btn" onclick="JA.summary._runExport()">Export Selected Reports</button>
                </div>
            </div>
        </div></div>`;
        JA.mountOverlay(html);
        this._exportFmt = 'excel';
        this._exportSrc = 'backend';
        // Store scope for _runExport
        this._exportScope = isScoped ? scope : null;
        // Populate filename field
        this._updateExportFilename();
    },

    _exportFmt: 'excel',
    _exportSrc: 'backend',
    _exportScope: null,

    _setExportFmt(fmt) {
        this._exportFmt = fmt;
        document.querySelectorAll('#exp-fmt-json, #exp-fmt-excel, #exp-fmt-csv, #exp-fmt-zip').forEach(b => b.classList.remove('export-format-active'));
        document.getElementById('exp-fmt-' + fmt)?.classList.add('export-format-active');
        // Show/hide source toggle (only relevant for Excel)
        const srcSection = document.getElementById('exp-source-section');
        const srcBtns = document.getElementById('exp-source-btns');
        if (srcSection) srcSection.style.display = fmt === 'excel' ? '' : 'none';
        if (srcBtns) srcBtns.style.display = fmt === 'excel' ? '' : 'none';
        // UI-041/UI-043: ZIP-only bundle options (cached source / full background decompile)
        const zipSection = document.getElementById('exp-zip-section');
        const zipOpts = document.getElementById('exp-zip-opts');
        if (zipSection) zipSection.style.display = fmt === 'zip' ? '' : 'none';
        if (zipOpts) zipOpts.style.display = fmt === 'zip' ? '' : 'none';
        if (fmt === 'zip') this._loadZipCacheStatus();
        // Update filename extension
        this._updateExportFilename();
    },

    /** UI-043 — shows "N cached" against the real per-jar decompile cache before the user opts
     *  into the background "decompile everything" job, per the task's own cached-vs-full ask. */
    async _loadZipCacheStatus() {
        const id = JA.app.currentJarId;
        const countEl = document.getElementById('exp-zip-cached-count');
        if (!id || !countEl) return;
        try {
            const res = await fetch(JA.apiBase() + '/' + encodeURIComponent(id) + '/export/bundle/cache-status', { cache: 'no-store' });
            const data = await res.json();
            const total = JA.app.currentAnalysis?.totalClasses;
            countEl.textContent = total != null ? (data.cachedClasses + ' of ' + total) : String(data.cachedClasses);
        } catch (e) { countEl.textContent = '?'; }
    },

    _setExportSrc(src) {
        this._exportSrc = src;
        document.querySelectorAll('#exp-src-backend, #exp-src-frontend').forEach(b => b.classList.remove('export-format-active'));
        document.getElementById('exp-src-' + src)?.classList.add('export-format-active');
    },

    /** Update the filename input + extension label based on current format and scope */
    _updateExportFilename() {
        const extMap = { excel: 'xlsx', json: 'json', csv: 'csv', zip: 'zip' };
        const ext = extMap[this._exportFmt] || 'xlsx';
        const jarName = JA.app.currentAnalysis?.jarName || 'analysis';
        const baseName = this._exportFileName(jarName, ext).replace('.' + ext, '');
        const input = document.getElementById('exp-filename');
        const extLabel = document.getElementById('exp-filename-ext');
        if (input) input.value = baseName;
        if (extLabel) extLabel.textContent = '.' + ext;
    },

    /** Get the user-entered filename (with extension) from the modal input */
    _getModalFilename() {
        const input = document.getElementById('exp-filename');
        const extLabel = document.getElementById('exp-filename-ext');
        const ext = (extLabel?.textContent || '.xlsx').replace('.', '');
        // Only strip the current extension suffix — not version dots like 0.0.1
        const raw = (input?.value || 'export').trim();
        const base = raw.endsWith('.' + ext) ? raw.slice(0, -(ext.length + 1)) : raw;
        return base + '.' + ext;
    },

    _runExport() {
        const sel = {
            endpoints: document.getElementById('exp-endpoints')?.checked,
            collections: document.getElementById('exp-collections')?.checked,
            transactions: document.getElementById('exp-transactions')?.checked,
            batch: document.getElementById('exp-batch')?.checked,
            views: document.getElementById('exp-views')?.checked,
            external: document.getElementById('exp-external')?.checked
        };

        // If scoped, temporarily swap report data to scoped versions
        const scope = this._exportScope;
        const isEpScoped = scope && scope.endpointIdx != null;
        const isCollScoped = scope && scope.collectionName != null;
        let orig = null;

        if (isEpScoped || isCollScoped) {
            orig = {
                ep: this._epReports, vert: this._vertReport, dist: this._distReport,
                batch: this._batchReport, views: this._viewsReport, ext: this._extReport,
                vertVer: this._vertVerReport
            };

            if (isEpScoped) {
                const r = this._epReports[scope.endpointIdx];
                if (r) {
                    this._epReports = [r];
                    this._vertReport = this._scopeCollections(r);
                    this._distReport = this._scopeTransactions(r);
                    this._batchReport = this._scopeBatch(r);
                    this._viewsReport = this._scopeViews(r);
                    this._extReport = this._scopeExternal(r);
                    this._vertVerReport = this._scopeVertVer([r]);
                }
            } else if (isCollScoped) {
                const cn = scope.collectionName;
                const scopedEp = this._scopeEpByCollection(cn);
                this._epReports = scopedEp;
                this._vertReport = this._scopeVertByCollection(cn);
                this._distReport = this._scopeDistByCollection(cn, scopedEp);
                this._batchReport = this._scopeBatchByCollection(cn, scopedEp);
                this._viewsReport = this._scopeViewsByCollection(cn);
                this._extReport = this._scopeExtByCollection(scopedEp);
                this._vertVerReport = this._scopeVertVer(scopedEp);
            }
        }

        // Store user-chosen filename for export functions to use
        this._modalFilename = this._getModalFilename();

        // UI-041/UI-043: ZIP mode is a genuinely separate code path from the 3 pre-existing
        // formats below (mirrors §251's own PL/SQL `_downloadZipBundle` separation) so those
        // three provably keep working unmodified — see summary-export-modal.js's own
        // regression note in TASK-REGISTRY §254.
        if (this._exportFmt === 'zip') {
            this._runZipExport();
            // ZIP mode manages its own modal lifecycle (background-job progress needs the modal
            // to stay open), so it returns before the generic close/restore below.
            if (orig) {
                this._epReports = orig.ep; this._vertReport = orig.vert; this._distReport = orig.dist;
                this._batchReport = orig.batch; this._viewsReport = orig.views; this._extReport = orig.ext;
                this._vertVerReport = orig.vertVer;
            }
            return;
        }

        if (this._exportFmt === 'json') {
            this._exportSelectedJSON(sel);
        } else if (this._exportFmt === 'csv') {
            // UI-BUG: there was no `csv` branch here, so picking CSV fell through to the
            // `else` below and ran the XLSX exporter — which wrote a real XLSX (ZIP) binary
            // under the `.csv` name `_getModalFilename()` had already produced. Excel would
            // open it (it sniffs content), but every actual CSV consumer choked. CSV now has
            // its own real text serializer; `excel` remains separately and correctly .xlsx.
            this._exportCsv(sel);
        } else if (this._exportSrc === 'frontend') {
            this._exportXlsxFrontend(sel);
        } else {
            this._exportXlsx(sel);
        }

        // Restore original data
        if (orig) {
            this._epReports = orig.ep;
            this._vertReport = orig.vert;
            this._distReport = orig.dist;
            this._batchReport = orig.batch;
            this._viewsReport = orig.views;
            this._extReport = orig.ext;
            this._vertVerReport = orig.vertVer;
        }

        this._exportScope = null;
        document.getElementById('export-modal-overlay')?.remove();
    },

    /**
     * UI-041 fast path / UI-043 background path. Builds the same 5 report objects
     * {@code exportAllReports()} (`summary-export.js`) already downloads separately today —
     * {@link JA.summary._buildFiveReportFiles} below is the exact same
     * map/build calls as `exportAllReports()`, kept deliberately read-only against that
     * pre-existing function (not touched, to keep its own already-verified download path
     * provably unchanged) — then either:
     *  - "Include ALL decompiled source" checked: submits a background queue job
     *    (`POST .../export/bundle/full-source-job`) and polls `/api/queue/{id}` the same way
     *    every other job in this app is polled, showing a "Download ready" link on COMPLETE; or
     *  - otherwise: POSTs those reports straight to the fast synchronous bundle endpoint
     *    (optionally including only already-cached decompiled source), same as §251's PL/SQL
     *    `_downloadZipBundle` — no new decompilation triggered.
     */
    async _runZipExport() {
        const id = JA.app.currentJarId;
        if (!id) { JA.toast.warn('No analysis loaded'); return; }
        const includeAll = document.getElementById('exp-zip-all-source')?.checked;
        const includeCached = document.getElementById('exp-zip-cached-source')?.checked;

        if (includeAll) {
            await this._submitFullSourceJob(id);
            return; // modal stays open, showing job progress — closed by the user or on completion click
        }

        const reports = this._buildFiveReportFiles();
        const lt = JA.toast.loading('Building ZIP bundle...');
        try {
            const res = await fetch(JA.apiBase() + '/' + encodeURIComponent(id) + '/export/bundle', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ reports, includeCachedSource: !!includeCached })
            });
            if (!res.ok) throw new Error('HTTP ' + res.status);
            const blob = await res.blob();
            const filename = this._modalFilename || this._exportFileName(JA.app.currentAnalysis?.jarName, 'zip');
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
            URL.revokeObjectURL(url);
            JA.toast.dismiss(lt);
            JA.toast.success('Exported ZIP bundle');
            document.getElementById('export-modal-overlay')?.remove();
        } catch (e) {
            JA.toast.dismiss(lt);
            JA.toast.error('ZIP export failed: ' + e.message);
        }
    },

    /** The exact same 5 report objects {@code exportAllReports()} already builds and downloads
     *  separately today — extracted so the fast-path ZIP bundle reuses this one computation
     *  instead of a second copy. Lazily builds any report not yet computed, same as
     *  {@code exportAllReports()} itself. */
    _buildFiveReportFiles() {
        if (!this._vertReport) {
            this._vertReport = this._buildVerticalisation(this._epReports);
            this._viewsReport = this._buildViewsAnalysis(this._vertReport, this._epReports);
        }
        if (!this._distReport) this._distReport = this._buildDistributedTransactions(this._epReports);
        if (!this._batchReport) this._batchReport = this._buildBatchAnalysis(this._epReports);
        return {
            'full_endpoint_report.json': this._epReports.map(r => ({
                endpointName: r.endpointName, procName: r.procName, allProcNames: r.allProcNames,
                endpointUrl: r.fullPath, typeOfEndpoint: r.typeOfEndpoint, primaryDomain: r.primaryDomain,
                domains: r.domains, crossDomainCount: r.crossDomainCount, totalCollections: r.totalCollections,
                totalViews: r.totalViews, totalDbOperations: r.totalDbOperations, totalMethods: r.totalMethods,
                operationTypes: r.operationTypes,
                domainCollections: Object.fromEntries(Object.entries(r.collDomainGroups).map(([k, v]) => [k, v])),
                writeCollections: r.writeCollections, readCollections: r.readCollections,
                aggregateCollections: r.aggregateCollections, viewsUsed: r.viewsUsed,
                serviceClasses: r.serviceClasses, sizeCategory: r.sizeCategory, performanceImplication: r.performanceImplication
            })),
            'full_verticalisation.json': this._vertReport.map(c => ({
                object: c.name, listOfEndpoints: [...c.endpoints], typeOfObject: c.type, domain: c.domain,
                typeOfUsage: [...c.operations], complexity: c.usageCount <= 2 ? 'Low' : c.usageCount <= 5 ? 'Medium' : 'High',
                countOfUsage: c.usageCount, endpointCount: c.endpoints.size,
                sampleReferences: [...c.sources].slice(0, 5), procNames: [...c.procNames],
                performanceImplication: c.operations.has('AGGREGATE') ? 'Medium Impact (100-200ms)' : c.usageCount > 5 ? 'High Impact' : 'No Impact'
            })),
            'full_distributed_transaction.json': this._distReport,
            'full_batch_job.json': this._batchReport,
            'full_views.json': this._viewsReport
        };
    },

    /** UI-043 — submits the background job and switches the modal body into a polling progress
     *  view, reusing the exact `/api/queue/{id}` shape every other job in this app already
     *  reports through (no new progress mechanism). */
    async _submitFullSourceJob(id, maxClasses) {
        const body = {};
        if (maxClasses != null) body.maxClasses = maxClasses;
        let jobId;
        try {
            const res = await fetch(JA.apiBase() + '/' + encodeURIComponent(id) + '/export/bundle/full-source-job', {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
            });
            if (!res.ok) throw new Error('HTTP ' + res.status);
            const data = await res.json();
            jobId = data.jobId;
        } catch (e) {
            JA.toast.error('Failed to submit background export job: ' + e.message);
            return;
        }
        this._fullSourceJobId = jobId;
        const actions = document.querySelector('#export-modal-overlay .export-actions');
        if (actions) {
            actions.innerHTML = '<div id="exp-zip-job-status" style="width:100%">Background job <code>' + jobId + '</code> submitted — decompiling...</div>';
        }
        this._pollFullSourceJob(id, jobId);
    },

    async _pollFullSourceJob(id, jobId) {
        const statusEl = document.getElementById('exp-zip-job-status');
        if (!statusEl) return; // modal closed
        try {
            const res = await fetch('/api/queue/' + encodeURIComponent(jobId), { cache: 'no-store' });
            const job = await res.json();
            if (job.status === 'COMPLETE') {
                statusEl.innerHTML = 'Done — ' + (job.resultName || 'bundle ready') +
                    '. <a href="' + JA.apiBase() + '/' + encodeURIComponent(id) + '/export/bundle/download" download id="exp-zip-download-link">Download ready &#8595;</a>';
                return;
            }
            if (job.status === 'FAILED') {
                statusEl.textContent = 'Export job failed: ' + (job.error || 'unknown error');
                return;
            }
            if (job.status === 'CANCELLED') {
                statusEl.textContent = 'Export job cancelled.';
                return;
            }
            statusEl.textContent = job.currentStep || ('Processing... ' + (job.progressPercent || 0) + '%');
            // Interval from PollConfig, not a literal (see flow-analysis.js's identical pattern):
            // this is a job-status poller and must move with the configured cadence like every
            // other one, not stay pinned to a hard-coded value the config panel can't reach.
            setTimeout(() => this._pollFullSourceJob(id, jobId),
                (window.PollConfig && PollConfig.jobStatusMs) || 1000);
        } catch (e) {
            setTimeout(() => this._pollFullSourceJob(id, jobId),
                ((window.PollConfig && PollConfig.jobStatusMs) || 1000) * 2);
        }
    },

    /* --- Scope helpers: filter full reports down to a single endpoint --- */

    _scopeCollections(r) {
        if (!this._vertReport) return [];
        const collNames = new Set(Object.keys(r.collections || {}));
        return this._vertReport.filter(c => collNames.has(c.name));
    },

    _scopeTransactions(r) {
        if (!this._distReport) return [];
        return this._distReport.filter(d => d.endpointName === r.endpointName);
    },

    _scopeBatch(r) {
        if (!this._batchReport) return [];
        return this._batchReport.filter(b => b.endpointName === r.endpointName);
    },

    _scopeViews(r) {
        if (!this._viewsReport) return [];
        const viewNames = new Set(r.viewsUsed || []);
        return this._viewsReport.filter(v => viewNames.has(v.viewName || v.name));
    },

    _scopeExternal(r) {
        if (!this._extReport) return { crossModule: [], httpCalls: [] };
        const epName = r.endpointName;
        return {
            crossModule: (this._extReport.crossModule || []).filter(e =>
                e.usedByEndpoints?.includes?.(epName) || e.endpoints?.has?.(epName)),
            httpCalls: (this._extReport.httpCalls || []).filter(e =>
                e.endpoint === epName)
        };
    },

    /** Scope vertVerReport to only beans/collections touched by given endpoints */
    _scopeVertVer(scopedEpReports) {
        if (!this._vertVerReport) {
            if (this._buildVertVerification) return this._buildVertVerification(scopedEpReports);
            return null;
        }
        const epNames = new Set(scopedEpReports.map(r => r.endpointName));
        const report = this._vertVerReport;
        return {
            primaryDomain: report.primaryDomain,
            beans: (report.beans || []).filter(b =>
                b.calledByEndpoints && [...b.calledByEndpoints].some(ep => epNames.has(ep))),
            collections: (report.collections || []).filter(c =>
                c.accessedByEndpoints && [...c.accessedByEndpoints].some(ep => epNames.has(ep)))
        };
    },

    /* --- Scope helpers: filter full reports down to a single collection --- */

    _scopeEpByCollection(collName) {
        if (!this._epReports) return [];
        return this._epReports.filter(r => r.collections && r.collections[collName]);
    },

    _scopeVertByCollection(collName) {
        if (!this._vertReport) return [];
        return this._vertReport.filter(c => c.name === collName);
    },

    _scopeDistByCollection(collName, scopedEp) {
        if (!this._distReport) return [];
        const epNames = new Set(scopedEp.map(r => r.endpointName));
        return this._distReport.filter(d => epNames.has(d.endpointName));
    },

    _scopeBatchByCollection(collName, scopedEp) {
        if (!this._batchReport) return [];
        const epNames = new Set(scopedEp.map(r => r.endpointName));
        return this._batchReport.filter(b => epNames.has(b.endpointName));
    },

    _scopeViewsByCollection(collName) {
        if (!this._viewsReport) return [];
        return this._viewsReport.filter(v => (v.viewName || v.name) === collName);
    },

    _scopeExtByCollection(scopedEp) {
        if (!this._extReport) return { crossModule: [], httpCalls: [] };
        const epNames = new Set(scopedEp.map(r => r.endpointName));
        return {
            crossModule: (this._extReport.crossModule || []).filter(e => {
                if (e.endpoints instanceof Set) return [...e.endpoints].some(ep => epNames.has(ep));
                if (Array.isArray(e.endpoints)) return e.endpoints.some(ep => epNames.has(ep));
                return false;
            }),
            httpCalls: (this._extReport.httpCalls || []).filter(e => epNames.has(e.endpoint))
        };
    },

    /**
     * Build export filename with scope awareness.
     * Single endpoint: {jarName}_{Controller}.{method}_report.{ext}
     * Full export: {jarName}_full_report.{ext}
     */
    _exportFileName(jarName, ext) {
        const base = (jarName || 'analysis').replace('.jar', '');
        const scope = this._exportScope;
        if (scope && scope.endpointIdx != null) {
            const epName = (this._epReports?.[scope.endpointIdx]?.endpointName || 'endpoint')
                .replace(/[^a-zA-Z0-9._-]/g, '_');
            return base + '_' + epName + '_report.' + ext;
        }
        if (scope && scope.collectionName) {
            const collName = scope.collectionName.replace(/[^a-zA-Z0-9._-]/g, '_');
            return base + '_' + collName + '_report.' + ext;
        }
        return base + '_full_report.' + ext;
    },

    _exportSelectedJSON(sel) {
        const scope = this._exportScope;
        const isScoped = (scope && scope.endpointIdx != null) || (scope && scope.collectionName);
        const jarName = JA.app.currentAnalysis?.jarName || 'analysis';
        const scopeType = scope?.endpointIdx != null ? 'endpoint' : scope?.collectionName ? 'collection' : 'full';
        const report = {
            _exportedAt: new Date().toISOString(),
            _tool: 'JAR Analyzer',
            jarName: jarName,
            scope: scopeType,
            summary: {
                totalEndpoints: (this._epReports || []).length,
                totalCollections: (this._vertReport || []).length,
                totalViews: (this._viewsReport || []).length,
                totalBatchJobs: (this._batchReport || []).length,
                transactionsRequired: (this._distReport || []).filter(d => d.transactionRequirement?.startsWith('REQUIRED')).length
            }
        };
        let sections = 0;
        if (sel.endpoints && this._epReports?.length) {
            report.endpointReport = this._buildEndpointJSON();
            sections++;
        }
        if (sel.collections && this._vertReport?.length) {
            report.collectionAnalysis = this._buildCollectionJSON();
            sections++;
        }
        if (sel.transactions && this._distReport?.length) {
            report.distributedTransactions = this._distReport;
            sections++;
        }
        if (sel.batch && this._batchReport?.length) {
            report.batchJobs = this._batchReport;
            sections++;
        }
        if (sel.views && this._viewsReport?.length) {
            report.viewsAnalysis = this._viewsReport;
            sections++;
        }

        const filename = this._modalFilename || this._exportFileName(jarName, 'json');
        this._downloadJSON(filename, report);
        JA.toast.success('Exported ' + scopeType + ' JSON with ' + sections + ' sections');
    },

    _buildEndpointJSON() {
        return (this._epReports || []).map(r => ({
            endpointName: r.endpointName, procName: r.procName || '',
            httpMethod: r.httpMethod, path: r.fullPath, typeOfEndpoint: r.typeOfEndpoint,
            primaryDomain: r.primaryDomain, domains: r.domains, modules: r.modules,
            totalCollections: r.totalCollections, totalViews: r.totalViews,
            totalDbOperations: r.totalDbOperations, totalMethods: r.totalMethods,
            internalCalls: r.inScopeCalls, externalCalls: r.externalScopeCalls,
            operationTypes: r.operationTypes,
            sizeCategory: r.sizeCategory, performanceImplication: r.performanceImplication,
            writeCollections: r.writeCollections, readCollections: r.readCollections,
            aggregateCollections: r.aggregateCollections, viewsUsed: r.viewsUsed,
            serviceClasses: r.serviceClasses,
            crossDomainCount: r.crossDomainCount
        }));
    },

    _buildCollectionJSON() {
        return (this._vertReport || []).map(c => ({
            name: c.name, type: c.type, domain: c.domain,
            operations: [...c.operations], usageCount: c.usageCount,
            endpointCount: c.endpoints.size, endpoints: [...c.endpoints],
            references: [...c.sources].slice(0, 10)
        }));
    },

});
