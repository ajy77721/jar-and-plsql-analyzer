/**
 * REST API client for WAR Analyzer — all fetch calls point to /api/war/wars.
 * Overrides JA.api set by the jar api.js loaded before this file.
 */
window.JA = window.JA || {};

JA.api = {

    async listJars() {
        const res = await fetch('/api/war/wars', { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Failed to list WARs');
        return res.json();
    },

    async uploadJar(file, mode, projectPath, renameSuffix, basePackage) {
        const form = new FormData();
        form.append('file', file);
        let url = '/api/war/wars?mode=' + encodeURIComponent(mode || 'static');
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
        let url = '/api/war/wars/' + encodeURIComponent(id);
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Analysis not found');
        return res.json();
    },

    async getSummary(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary not found');
        return res.json();
    },

    async getSummaryHeaders(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/headers';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary headers not found');
        return res.json();
    },

    async getExternalCalls(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/external-calls';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'External calls not found');
        return res.json();
    },

    async getDynamicFlows(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/dynamic-flows';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Dynamic flows not found');
        return res.json();
    },

    async getAggregationFlows(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/aggregation-flows';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Aggregation flows not found');
        return res.json();
    },

    async getBeans(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/beans';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Beans not found');
        return res.json();
    },

    async getClassTree(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/classes/tree';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Classes not found');
        return res.json();
    },

    async getClassByIndex(id, idx, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/classes/by-index/' + idx;
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Class not found');
        return res.json();
    },

    async getCallTree(id, idx, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/endpoints/by-index/' + idx + '/call-tree';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Call tree not found');
        return res.json();
    },

    async deleteJar(id) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(id), { method: 'DELETE' });
        if (!res.ok) throw new Error('Delete failed');
        return res.json();
    },

    async getClaudeStatus() {
        return { configured: false };
    },

    async getClaudeProgress(warId) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/claude-progress', { cache: 'no-store' });
            if (!res.ok) return { status: 'IDLE' };
            return res.json();
        } catch (e) {
            return { status: 'IDLE' };
        }
    },

    async claudeEnrichSingle(warId, endpointName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/claude-enrich-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeRescan(warId, resume) {
        const url = '/api/war/wars/' + encodeURIComponent(warId) + '/claude-rescan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async listSessions() {
        const res = await fetch('/api/war/wars/sessions');
        if (!res.ok) throw new Error('Failed to list sessions');
        return res.json();
    },

    async killSession(sessionId) {
        const res = await fetch('/api/war/wars/sessions/' + encodeURIComponent(sessionId) + '/kill', {
            method: 'POST'
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrect(warId, resume) {
        const url = '/api/war/wars/' + encodeURIComponent(warId) + '/claude-correct'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrectSingle(warId, endpointName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/claude-correct-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrections(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/corrections');
        if (!res.ok) throw new Error('Corrections not found');
        return res.json();
    },

    async getVersions(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/versions', { cache: 'no-store' });
        if (!res.ok) throw new Error('Failed to get version info');
        return res.json();
    },

    async revertClaude(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/revert-claude', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeFullScan(warId, resume) {
        const url = '/api/war/wars/' + encodeURIComponent(warId) + '/claude-full-scan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrectionForEndpoint(warId, endpoint) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/corrections/' + encodeURIComponent(endpoint));
        if (!res.ok) throw new Error('Correction not found');
        return res.json();
    },

    async getCorrectionLogs(warId, endpointName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/correction-logs/' + encodeURIComponent(endpointName));
        if (!res.ok) throw new Error('Correction logs not found');
        return res.json();
    },

    async listAllCorrectionLogs(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/correction-logs');
        if (!res.ok) throw new Error('Failed to list correction logs');
        return res.json();
    },

    async getCorrectionLogFile(warId, fileName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/correction-logs/file/' + encodeURIComponent(fileName));
        if (!res.ok) throw new Error('Log file not found');
        return res.text();
    },

    async listRunLogs(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/run-logs');
        if (!res.ok) throw new Error('Failed to list run logs');
        return res.json();
    },

    async getRunLog(warId, logName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/run-logs/' + encodeURIComponent(logName));
        if (!res.ok) throw new Error('Log not found');
        return res.text();
    },

    async getCatalog(warId) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/catalog', { cache: 'no-store' });
            if (!res.ok) return null;
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async killJarSessions(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/kill-sessions', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getClaudeStats(warName) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warName) + '/claude-stats', { cache: 'no-store' });
            if (!res.ok) return null;
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async getClaudeSessionDetail(warName, sessionId) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warName) + '/claude-stats/' + encodeURIComponent(sessionId), { cache: 'no-store' });
            if (!res.ok) return null;
            return res.json();
        } catch (e) {
            return null;
        }
    }
};

// Wrap user-initiated API calls with activity tracker notifications.
(function() {
    const silent = new Set([
        'getClaudeStatus', 'getClaudeProgress', 'listSessions',
        'getCatalog', 'listRunLogs', 'listAllCorrectionLogs',
        'getClaudeStats', 'getClaudeSessionDetail',
        'getExternalCalls', 'getDynamicFlows', 'getAggregationFlows', 'getBeans'
    ]);
    const labels = {
        listJars: 'Loading WARs',
        getSummary: 'Loading Summary',
        getSummaryHeaders: 'Loading Summary',
        getClassTree: 'Loading Code Structure',
        getClassByIndex: 'Loading Class',
        getCallTree: 'Loading Call Tree',
        getAnalysis: 'Loading Full Analysis',
        uploadJar: 'Uploading WAR',
        deleteJar: 'Deleting WAR',
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
