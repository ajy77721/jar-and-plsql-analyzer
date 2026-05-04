/**
 * REST API client — all fetch calls live here.
 */
window.JA = window.JA || {};

JA.api = {

    async listJars() {
        const res = await fetch('/api/jar/jars', { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Failed to list JARs');
        return res.json();
    },

    async uploadJar(file, mode, projectPath, renameSuffix, basePackage) {
        const form = new FormData();
        form.append('file', file);
        let url = '/api/jar/jars?mode=' + encodeURIComponent(mode || 'static');
        if (projectPath) url += '&projectPath=' + encodeURIComponent(projectPath);
        if (renameSuffix) url += '&renameSuffix=' + encodeURIComponent(renameSuffix);
        if (basePackage && basePackage.trim()) url += '&basePackage=' + encodeURIComponent(basePackage.trim());
        const res = await fetch(url, { method: 'POST', body: form });
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || res.statusText);
        }
        return res.json();
    },

    async getAnalysis(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id);
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Analysis not found');
        return res.json();
    },

    async getSummary(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary not found');
        return res.json();
    },

    async getSummaryHeaders(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/headers';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary headers not found');
        return res.json();
    },

    async getExternalCalls(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/external-calls';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'External calls not found');
        return res.json();
    },

    async getConnections(id) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(id) + '/connections', { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    async getDynamicFlows(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/dynamic-flows';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Dynamic flows not found');
        return res.json();
    },

    async getAggregationFlows(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/aggregation-flows';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Aggregation flows not found');
        return res.json();
    },

    async getBeans(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/beans';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Beans not found');
        return res.json();
    },

    async getClassTree(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/classes/tree';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Classes not found');
        return res.json();
    },

    async getClassByIndex(id, idx, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/classes/by-index/' + idx;
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Class not found');
        return res.json();
    },

    async getCallTree(id, idx, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/endpoints/by-index/' + idx + '/call-tree';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Call tree not found');
        return res.json();
    },

    async deleteJar(id) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(id), { method: 'DELETE' });
        if (!res.ok) throw new Error('Delete failed');
        return res.json();
    },

    async getClaudeStatus() {
        try {
            const res = await fetch('/api/jar/jars/settings/claude-status');
            if (!res.ok) return { configured: false };
            return res.json();
        } catch (e) {
            return { configured: false };
        }
    },

    async getClaudeProgress(jarId) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-progress', { cache: 'no-store' });
            if (!res.ok) return { status: 'IDLE' };
            return res.json();
        } catch (e) {
            return { status: 'IDLE' };
        }
    },

    async claudeEnrichSingle(jarId, endpointName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-enrich-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeRescan(jarId, resume) {
        const url = '/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-rescan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async listSessions() {
        const res = await fetch('/api/jar/jars/sessions');
        if (!res.ok) throw new Error('Failed to list sessions');
        return res.json();
    },

    async killSession(sessionId) {
        const res = await fetch('/api/jar/jars/sessions/' + encodeURIComponent(sessionId) + '/kill', {
            method: 'POST'
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrect(jarId, resume) {
        const url = '/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-correct'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrectSingle(jarId, endpointName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-correct-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrections(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/corrections');
        if (!res.ok) throw new Error('Corrections not found');
        return res.json();
    },

    async getVersions(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/versions', { cache: 'no-store' });
        if (!res.ok) throw new Error('Failed to get version info');
        return res.json();
    },

    async revertClaude(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/revert-claude', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeFullScan(jarId, resume) {
        const url = '/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-full-scan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrectionForEndpoint(jarId, endpoint) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/corrections/' + encodeURIComponent(endpoint));
        if (!res.ok) throw new Error('Correction not found');
        return res.json();
    },

    async getCorrectionLogs(jarId, endpointName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/correction-logs/' + encodeURIComponent(endpointName));
        if (!res.ok) throw new Error('Correction logs not found');
        return res.json();
    },

    async listAllCorrectionLogs(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/correction-logs');
        if (!res.ok) throw new Error('Failed to list correction logs');
        return res.json();
    },

    async getCorrectionLogFile(jarId, fileName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/correction-logs/file/' + encodeURIComponent(fileName));
        if (!res.ok) throw new Error('Log file not found');
        return res.text();
    },

    async listRunLogs(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/run-logs');
        if (!res.ok) throw new Error('Failed to list run logs');
        return res.json();
    },

    async getRunLog(jarId, logName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/run-logs/' + encodeURIComponent(logName));
        if (!res.ok) throw new Error('Log not found');
        return res.text();
    },

    async getCatalog(jarId) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/catalog', { cache: 'no-store' });
            if (!res.ok) return null;
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async killJarSessions(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/kill-sessions', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getClaudeStats(jarName) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarName) + '/claude-stats', { cache: 'no-store' });
            if (!res.ok) return null;
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async getClaudeSessionDetail(jarName, sessionId) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarName) + '/claude-stats/' + encodeURIComponent(sessionId), { cache: 'no-store' });
            if (!res.ok) return null;
            return res.json();
        } catch (e) {
            return null;
        }
    }
};

// Wrap user-initiated API calls with activity tracker notifications.
// Background/polling calls are excluded.
(function() {
    const silent = new Set([
        'getClaudeStatus', 'getClaudeProgress', 'listSessions',
        'getCatalog', 'listRunLogs', 'listAllCorrectionLogs',
        'getClaudeStats', 'getClaudeSessionDetail',
        'getExternalCalls', 'getDynamicFlows', 'getAggregationFlows', 'getBeans'
    ]);
    const labels = {
        listJars: 'Loading JARs',
        getSummary: 'Loading Summary',
        getSummaryHeaders: 'Loading Summary',
        getClassTree: 'Loading Code Structure',
        getClassByIndex: 'Loading Class',
        getCallTree: 'Loading Call Tree',
        getAnalysis: 'Loading Full Analysis',
        uploadJar: 'Uploading JAR',
        deleteJar: 'Deleting JAR',
        claudeEnrichSingle: 'Claude Enrich',
        claudeRescan: 'Claude Rescan',
        claudeCorrect: 'Claude Correct',
        claudeCorrectSingle: 'Claude Correct',
        claudeFullScan: 'Claude Full Scan',
        getCorrections: 'Loading Corrections',
        getCorrectionForEndpoint: 'Loading Correction',
        getCorrectionLogs: 'Loading Logs',
        getCorrectionLogFile: 'Loading Log',
        getVersions: 'Loading Versions',
        revertClaude: 'Reverting Claude',
        getRunLog: 'Loading Log',
        killSession: 'Killing Session',
        killJarSessions: 'Killing Sessions'
    };
    for (const key of Object.keys(JA.api)) {
        if (typeof JA.api[key] !== 'function' || silent.has(key)) continue;
        const orig = JA.api[key];
        JA.api[key] = function() {
            const label = labels[key] || key;
            if (!JA.apiTracker) return orig.apply(JA.api, arguments);
            return JA.apiTracker.track(label, () => orig.apply(JA.api, arguments));
        };
    }
})();
