/**
 * Summary export — JSON export/import + per-endpoint detail export.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    exportAllReports() {
        if (!this._epReports) { JA.toast.warn('No analysis loaded'); return; }
        // Build any reports not yet computed (lazy-loaded on first tab access)
        if (!this._vertReport) {
            this._vertReport = this._buildVerticalisation(this._epReports);
            this._viewsReport = this._buildViewsAnalysis(this._vertReport, this._epReports);
        }
        if (!this._distReport) this._distReport = this._buildDistributedTransactions(this._epReports);
        if (!this._batchReport) this._batchReport = this._buildBatchAnalysis(this._epReports);
        const base = (JA.app.currentAnalysis?.jarName || 'analysis').replace('.jar', '');
        this._downloadJSON(base + '_full_endpoint_report.json', this._epReports.map(r => ({
            endpointName: r.endpointName, procName: r.procName, allProcNames: r.allProcNames,
            endpointUrl: r.fullPath, typeOfEndpoint: r.typeOfEndpoint, primaryDomain: r.primaryDomain,
            domains: r.domains, crossDomainCount: r.crossDomainCount, totalCollections: r.totalCollections,
            totalViews: r.totalViews, totalDbOperations: r.totalDbOperations, totalMethods: r.totalMethods,
            operationTypes: r.operationTypes,
            domainCollections: Object.fromEntries(Object.entries(r.collDomainGroups).map(([k, v]) => [k, v])),
            writeCollections: r.writeCollections, readCollections: r.readCollections,
            aggregateCollections: r.aggregateCollections, viewsUsed: r.viewsUsed,
            serviceClasses: r.serviceClasses, sizeCategory: r.sizeCategory, performanceImplication: r.performanceImplication
        })));
        this._downloadJSON(base + '_full_verticalisation.json', this._vertReport.map(c => ({
            object: c.name, listOfEndpoints: [...c.endpoints], typeOfObject: c.type, domain: c.domain,
            typeOfUsage: [...c.operations], complexity: c.usageCount <= 2 ? 'Low' : c.usageCount <= 5 ? 'Medium' : 'High',
            countOfUsage: c.usageCount, endpointCount: c.endpoints.size,
            sampleReferences: [...c.sources].slice(0, 5), procNames: [...c.procNames],
            performanceImplication: c.operations.has('AGGREGATE') ? 'Medium Impact (100-200ms)' : c.usageCount > 5 ? 'High Impact' : 'No Impact'
        })));
        this._downloadJSON(base + '_full_distributed_transaction.json', this._distReport);
        this._downloadJSON(base + '_full_batch_job.json', this._batchReport);
        this._downloadJSON(base + '_full_views.json', this._viewsReport);
        JA.toast.success('Exported 5 report files');
    },

    async exportEndpointDetail(idx) {
        const r = this._epReports[idx];
        if (!r) return;

        // Fetch callTree on demand if not loaded
        let callTree = r.endpoint.callTree;
        if (!callTree && r.sourceIdx != null && JA.app.currentJarId) {
            try {
                callTree = await JA.api.getCallTree(JA.app.currentJarId, r.sourceIdx);
            } catch (e) { console.warn('Failed to fetch call tree for export:', e); }
        }

        const controllerJar = callTree ? callTree.sourceJar || null : null;
        const detail = {
            meta: { controller: r.endpoint.controllerSimpleName, method: r.endpoint.methodName, endpoint: r.fullPath, httpMethod: r.httpMethod, analyzedAt: new Date().toISOString(), analysisMode: 'static', procName: r.procName, primaryDomain: r.primaryDomain, domains: r.domains },
            modules: r.modules,
            summary: { totalMethods: r.totalMethods, totalCollections: r.totalCollections, totalDbOperations: r.totalDbOperations, inScopeCalls: r.inScopeCalls, externalScopeCalls: r.externalScopeCalls, crossDomainCount: r.crossDomainCount, operationTypes: r.operationTypes },
            callTree: callTree ? this._exportCallTree(callTree, controllerJar) : null,
            collections: Object.fromEntries(Object.entries(r.collections).map(([name, c]) => [name, { type: c.type, domain: c.domain, operations: [...c.operations], sources: [...c.sources] }])),
            beans: r.beans.map(b => ({ className: b.className, simpleClassName: b.simpleClassName, stereotype: b.stereotype, module: this._jarToProject(b.sourceJar || 'main'), scope: (b.sourceJar || null) === controllerJar ? 'in-scope' : 'external-scope' }))
        };
        const base = (JA.app.currentAnalysis?.jarName || 'analysis').replace('.jar', '');
        const epSafe = (r.endpointName || 'endpoint').replace(/[^a-zA-Z0-9._-]/g, '_');
        this._downloadJSON(base + '_' + epSafe + '_detail.json', detail);
    },

    _exportCallTree(node, controllerJar) {
        if (!node) return null;
        const isExt = node.id && node.id.startsWith('ext:');
        return {
            class: node.simpleClassName, method: node.methodName, stereotype: node.stereotype || '',
            module: this._jarToProject(node.sourceJar || 'main'),
            scope: isExt ? 'external-scope' : ((node.sourceJar || null) === controllerJar ? 'in-scope' : 'external-scope'),
            children: (node.children || []).map(c => this._exportCallTree(c, controllerJar))
        };
    },

    _downloadJSON(filename, data) {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
        URL.revokeObjectURL(url);
    },

    exportJSON() {
        const analysis = JA.app.currentAnalysis;
        if (!analysis) { JA.toast.warn('No analysis loaded'); return; }
        const key = 'JA-' + Date.now().toString(36) + '-' + Math.random().toString(36).substring(2, 8);
        const report = { _key: key, _exportedAt: new Date().toISOString(), _tool: 'JAR Analyzer',
            jarName: analysis.jarName, jarSize: analysis.jarSize, analyzedAt: analysis.analyzedAt,
            totalClasses: analysis.totalClasses, totalEndpoints: analysis.totalEndpoints,
            endpoints: (analysis.endpoints || []).map(ep => {
                const e = { httpMethod: ep.httpMethod, fullPath: ep.fullPath, controllerClass: ep.controllerClass, controllerSimpleName: ep.controllerSimpleName, methodName: ep.methodName, returnType: ep.returnType, parameters: ep.parameters };
                if (ep.callTree) e.callTree = ep.callTree;
                if (ep.aggregatedCollections) e.aggregatedCollections = ep.aggregatedCollections;
                if (ep.externalCalls) e.externalCalls = ep.externalCalls;
                if (ep.modules) e.modules = ep.modules;
                if (ep.beans) e.beans = ep.beans;
                return e;
            }),
            classes: analysis.classes || analysis.classIndex };
        this._downloadJSON((analysis.jarName || 'analysis').replace('.jar', '') + '_report_' + key + '.json', report);
        JA.toast.success('Exported with key: ' + key, 4000);
    },

    importJSON(event) {
        const file = event.target.files[0];
        if (!file) return;
        const lt = JA.toast.loading('Loading JSON report...');
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const data = JSON.parse(e.target.result);
                JA.toast.dismiss(lt);
                if (!data.endpoints && !data.classes) { JA.toast.error('Invalid JSON'); return; }
                JA.app.currentAnalysis = data;
                JA.app.currentJarId = data._key || data.jarName || 'imported';
                JA.app._showAnalysis(data);
                JA.toast.success('Loaded: ' + (data.jarName || 'unknown'), 4000);
            } catch (err) { JA.toast.dismiss(lt); JA.toast.error('Parse error: ' + err.message); }
        };
        reader.readAsText(file);
        event.target.value = '';
    }
});
