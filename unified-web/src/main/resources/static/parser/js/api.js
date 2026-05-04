window.PA = window.PA || {};

PA.api = {
    _analysisName: null,

    setAnalysis(name) { PA.api._analysisName = name; },

    async listHistory() {
        const res = await fetch('/api/parser/analyses');
        if (!res.ok) return [];
        return res.json();
    },

    async runAnalysis(entryPoint, body) {
        const payload = body || { entryPoint: entryPoint };
        if (!payload.entryPoint) payload.entryPoint = entryPoint;
        const res = await fetch('/api/parser/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        return res.json();
    },

    async getIndex() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/index`);
        if (!res.ok) return null;
        return res.json();
    },

    async getProcedures() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/procedures`);
        if (!res.ok) return [];
        return res.json();
    },

    async getNodeDetail(nodeFile) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/node/${encodeURIComponent(nodeFile)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async getTableOperations() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables`);
        if (!res.ok) return { tables: [] };
        const data = await res.json();
        return (data.tables || []).map(t => ({
            tableName: t.name,
            tableSchema: t.schema || '',
            objectType: t.objectType || 'TABLE',
            triggers: t.triggers || [],
            operations: t.allOperations || [],
            accessCount: (t.usedBy || []).reduce((sum, u) => {
                return sum + Object.values(u.operations || {}).reduce((s, lines) => s + lines.length, 0);
            }, 0),
            accessDetails: (t.usedBy || []).flatMap(u => {
                return Object.entries(u.operations || {}).flatMap(([op, lines]) => {
                    return (lines || []).map(line => ({
                        operation: op,
                        procedureId: u.nodeId || '',
                        procedureName: u.nodeName || '',
                        lineNumber: line,
                        sourceFile: ''
                    }));
                });
            })
        }));
    },

    async getTablesIndex() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables`);
        if (!res.ok) return null;
        return res.json();
    },

    async getJoinOperations() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/joins`);
        if (!res.ok) return [];
        return res.json();
    },

    async getCursorOperations() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/cursors`);
        if (!res.ok) return [];
        return res.json();
    },

    async getSequenceOperations() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/sequences`);
        if (!res.ok) return [];
        return res.json();
    },

    async dbTableInfo(tableName, schema) {
        let url = `/api/plsql/db/table-info/${encodeURIComponent(tableName)}`;
        if (schema) url += `?schema=${encodeURIComponent(schema)}`;
        const res = await fetch(url);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async getCallTree(nodeId) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/call-tree/${encodeURIComponent(nodeId)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async getCallerTree(nodeId) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/call-tree/${encodeURIComponent(nodeId)}/callers`);
        if (!res.ok) return [];
        return res.json();
    },

    async getCallGraph() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/call-graph`);
        if (!res.ok) return null;
        return res.json();
    },

    async getSource(sourceFile) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/source/${encodeURIComponent(sourceFile)}`);
        if (!res.ok) return null;
        const text = await res.text();
        return { content: text, owner: '', objectName: sourceFile };
    },

    async claudeStatus() {
        const res = await fetch('/api/parser/claude/status');
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeVerify(analysisName, resume) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resume: !!resume })
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeProgress(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/progress`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeResult(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/result`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeChunksSummary(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/chunks/summary`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeChunkDetail(analysisName, chunkId) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/chunks/${encodeURIComponent(chunkId)}`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeKillSession(analysisName, sessionId) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/sessions/${encodeURIComponent(sessionId)}/kill`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeVersions(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeLoadStatic(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/load-static`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeLoadClaude(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/load-claude`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeRevert(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/revert`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeReview(analysisName, payload) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/review`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeLoadPrev(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/load-prev`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeApply(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/apply`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async getComplexityConfig() {
        const res = await fetch('/api/parser/config/complexity');
        if (!res.ok) return null;
        return res.json();
    },

    async getJoinComplexityConfig() {
        const res = await fetch('/api/parser/config/join-complexity');
        if (!res.ok) return null;
        return res.json();
    }
};
