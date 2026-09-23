/**
 * Targeted (developer) analysis — analyze a local JAR from specific entry points
 * (className / methodName / paramTypes) instead of the whole project.
 *
 * Additive: reuses the existing progress panel, job-queue polling, and result
 * renderers (JA.app._showAnalysis) so completed results look identical to a
 * normal analysis. Only the submit endpoint (analyze-targeted) differs.
 */
window.JA = window.JA || {};

JA.targeted = {

    _rowSeq: 0,

    showModal() {
        if (JA.app && JA.app._uploading) return;
        this._close();
        const overlay = document.createElement('div');
        overlay.id = 'targeted-modal-overlay';
        overlay.className = 'upload-modal-overlay';
        overlay.innerHTML = `
            <div class="upload-modal-panel" style="max-width:640px">
                <div class="upload-modal-header">
                    <span>Targeted Analysis <span class="upload-modal-optional">(developer)</span></span>
                    <button class="upload-modal-close" onclick="JA.targeted._close()">&#x2715;</button>
                </div>
                <div class="upload-modal-body">
                    <div class="upload-modal-field">
                        <label class="upload-modal-label">Local JAR Path</label>
                        <input type="text" id="tgt-jar-path" class="claude-path-input"
                            placeholder="Absolute path, e.g. /path/to/app-0.0.1-SNAPSHOT.jar">
                    </div>
                    <div class="upload-modal-field">
                        <label class="upload-modal-label">Name <span class="upload-modal-optional">(optional)</span></label>
                        <input type="text" id="tgt-name" class="claude-path-input" placeholder="Label for this analysis">
                    </div>
                    <div class="upload-modal-field">
                        <label class="upload-modal-label">Entry Points <span class="upload-modal-optional">(className required per row — method/paramTypes optional; leave method blank to include every method in that class, or give a package to include every class under it)</span></label>
                        <div id="tgt-entries"></div>
                        <button class="btn-sm" style="margin-top:8px" onclick="JA.targeted.addRow()">+ Add Entry</button>
                    </div>
                </div>
                <div class="upload-modal-footer">
                    <button class="btn-sm" onclick="JA.targeted._close()">Cancel</button>
                    <button class="upload-btn upload-modal-submit-btn" onclick="JA.targeted.submit()">Analyze</button>
                </div>
            </div>`;
        document.body.appendChild(overlay);
        overlay.addEventListener('click', e => { if (e.target === overlay) this._close(); });
        overlay.addEventListener('keydown', e => { if (e.key === 'Escape') this._close(); });
        this.addRow();
        setTimeout(() => overlay.querySelector('#tgt-jar-path')?.focus(), 50);
    },

    addRow() {
        const host = document.getElementById('tgt-entries');
        if (!host) return;
        const id = ++this._rowSeq;
        const row = document.createElement('div');
        row.className = 'targeted-entry-row';
        row.dataset.rowId = id;
        row.style.cssText = 'display:flex;gap:6px;margin-bottom:6px;align-items:center';
        row.innerHTML = `
            <input type="text" class="claude-path-input tgt-class" placeholder="className or package (required) — or paste FQCN#method(...) here" style="flex:2">
            <input type="text" class="claude-path-input tgt-method" placeholder="method (optional — blank = all methods)" style="flex:1">
            <input type="text" class="claude-path-input tgt-params" placeholder="paramTypes (optional) e.g. int, java.lang.String" style="flex:2">
            <button class="btn-sm" title="Remove" onclick="JA.targeted.removeRow(${id})">&#x2715;</button>`;
        host.appendChild(row);
    },

    removeRow(id) {
        const host = document.getElementById('tgt-entries');
        if (!host) return;
        const rows = host.querySelectorAll('.targeted-entry-row');
        if (rows.length <= 1) { JA.toast?.info('At least one entry is required'); return; }
        host.querySelector(`.targeted-entry-row[data-row-id="${id}"]`)?.remove();
    },

    /**
     * If className was pasted in the combined "FQCN#method" or "FQCN#method(Type1,Type2)"
     * form (same syntax as the jar-upload "scope to endpoint/method" field), split it into
     * {className, methodName, paramTypes} so a full copy-paste works without manually
     * splitting fields across the row. Returns null if className has no '#' to split on.
     */
    _splitCombined(rawClassName) {
        const hash = rawClassName.indexOf('#');
        if (hash <= 0) return null;
        const fqcn = rawClassName.slice(0, hash).trim();
        let rest = rawClassName.slice(hash + 1).trim();
        let paramTypes = null;
        const paren = rest.indexOf('(');
        if (paren > 0 && rest.endsWith(')')) {
            const inner = rest.slice(paren + 1, -1).trim();
            paramTypes = inner ? inner.split(',').map(p => p.trim()).filter(p => p.length) : [];
            rest = rest.slice(0, paren).trim();
        }
        if (!fqcn || !rest) return null;
        return { className: fqcn, methodName: rest, paramTypes };
    },

    /**
     * Collect entries, marking (via CSS) only genuinely incomplete rows instead of silently
     * dropping them. className is the only required field per row — a bare className (no
     * method) is valid on its own: it expands server-side to every method on that class, or
     * (if it prefixes indexed classes' FQNs instead of matching one exactly) every method on
     * every class under that package. Only a method with no owning class is incomplete.
     */
    _collectEntries(overlay) {
        const entries = [];
        const incompleteRows = [];
        overlay.querySelectorAll('.targeted-entry-row').forEach(row => {
            const classInput = row.querySelector('.tgt-class');
            const methodInput = row.querySelector('.tgt-method');
            const paramsInput = row.querySelector('.tgt-params');
            let className = classInput.value.trim();
            let methodName = methodInput.value.trim();
            const rawParams = paramsInput.value.trim();

            // Combined "FQCN#method(...)" pasted into className with method left blank —
            // split it now so it isn't treated as a bare class/package instead.
            if (!methodName) {
                const combined = this._splitCombined(className);
                if (combined) {
                    className = combined.className;
                    methodName = combined.methodName;
                    classInput.value = className;
                    methodInput.value = methodName;
                    if (combined.paramTypes && combined.paramTypes.length && !rawParams) {
                        paramsInput.value = combined.paramTypes.join(', ');
                    }
                }
            }

            classInput.classList.toggle('input-error', !className && !!methodName);
            methodInput.classList.toggle('input-error', false);
            // A method with no className can't resolve to anything — flag it. A bare
            // className with no method is valid (see doc comment above), and a fully blank
            // row (never touched) is silently skipped, not flagged.
            if (!className && methodName) {
                incompleteRows.push(row);
                return;
            }
            if (!className) return;
            const finalRawParams = paramsInput.value.trim();
            const paramTypes = finalRawParams
                ? finalRawParams.split(',').map(p => p.trim()).filter(p => p.length)
                : [];
            entries.push({ className, methodName, paramTypes });
        });
        return { entries, incompleteRows };
    },

    _close() {
        document.getElementById('targeted-modal-overlay')?.remove();
    },

    async submit() {
        const overlay = document.getElementById('targeted-modal-overlay');
        if (!overlay) return;
        const path = overlay.querySelector('#tgt-jar-path').value.trim();
        const name = overlay.querySelector('#tgt-name').value.trim();
        if (!path) { JA.toast?.error('Enter a JAR path'); return; }
        const { entries, incompleteRows } = this._collectEntries(overlay);
        if (incompleteRows.length) {
            JA.toast?.error('A method needs a className too (highlighted in red)', 5000);
            return;
        }
        if (!entries.length) { JA.toast?.error('Add at least one entry (className required)'); return; }

        this._close();
        const app = JA.app;
        app._uploading = true;
        document.querySelector('.sidebar')?.classList.add('sidebar-blocked');
        app._showLoading(true);
        app._showProgressPanel(true);
        const log = document.getElementById('progress-log');
        if (log) { log.innerHTML = ''; app._progressLines = []; }
        JA.toast?.info('Submitting targeted analysis (' + entries.length + ' entr' + (entries.length === 1 ? 'y' : 'ies') + ')...');

        try {
            const body = { path, mode: 'static', entries };
            if (name) body.name = name;
            const res = await fetch('/api/jar/jars/analyze-targeted', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.message || data.error || res.statusText);

            const jobId = data.jobId;
            const jarId = data.id || data.jarName;
            app._addProgressLine('step', 'Queued for targeted analysis (job ' + jobId + ')...');
            app._updateStatusButton('pending', 'Queued...');

            await this._pollJob(jobId);

            await app.loadJars();
            app._addProgressLine('complete', 'Loading results...');
            let analysis;
            analysis = await JA.api.loadSummary(jarId);
            app._showAnalysis(analysis);
            app._updateStatusButton('static-done', 'Analysis Complete');
            JA.toast?.success('Targeted analysis complete — ' + (analysis.totalEndpoints || 0) + ' endpoints', 4000);
            if (JA.chatToggle) JA.chatToggle.onJarChanged(jarId);
        } catch (err) {
            console.error('Targeted analysis error:', err);
            JA.toast?.error('Targeted analysis failed: ' + err.message, 5000);
            app._updateStatusButton('error', 'Failed');
            app._addProgressLine('error', 'Failed: ' + err.message);
            setTimeout(() => app._showProgressPanel(false), 3000);
        } finally {
            app._uploading = false;
            document.querySelector('.sidebar')?.classList.remove('sidebar-blocked');
            app._showLoading(false);
        }
    },

    /** Poll /api/queue/{jobId} at jobStatusMs until terminal state (same mechanism as analyze-local). */
    _pollJob(jobId) {
        return new Promise((resolve, reject) => {
            let notFound = 0;
            let timer = PollConfig.every('targetedJobMs', async () => {
                try {
                    const d = await fetch('/api/queue/' + jobId).then(r => r.json());
                    if (!d) return;
                    if (d.error) {
                        if (++notFound >= 3) { timer = PollConfig.clear(timer); reject(new Error('Job lost (server may have restarted).')); }
                        return;
                    }
                    notFound = 0;
                    if (d.status === 'RUNNING') {
                        JA.app._addProgressLine('step', d.currentStep || '');
                        if (d.progressPercent > 0) JA.app._updateStatusButton('pending', d.progressPercent + '%');
                    } else if (d.status === 'COMPLETE') {
                        JA.app._addProgressLine('complete', 'Analysis complete!');
                        timer = PollConfig.clear(timer); resolve();
                    } else if (d.status === 'FAILED') {
                        timer = PollConfig.clear(timer); reject(new Error(d.error || 'Analysis failed'));
                    } else if (d.status === 'CANCELLED') {
                        timer = PollConfig.clear(timer); reject(new Error('Job cancelled'));
                    }
                } catch (e) { /* retry next interval */ }
            });
        });
    }
};
