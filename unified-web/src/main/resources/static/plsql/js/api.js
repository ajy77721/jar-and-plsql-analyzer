/**
 * PA.api — API module for PL/SQL Analyzer.
 */
window.PA = window.PA || {};

PA.api = {
    // ---- Analysis ----
    async analyze(username, objectName, objectType, procedureName, project, env, fast) {
        const body = { username, objectName, objectType };
        if (procedureName) body.procedureName = procedureName;
        if (project) body.project = project;
        if (env) body.env = env;
        const endpoint = fast ? '/api/plsql/analysis/analyze-fast' : '/api/plsql/analysis/analyze';
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        return res.json();
    },

    async getLatestResult() {
        const res = await fetch('/api/plsql/analysis');
        return res.json();
    },

    async listVersions() {
        const res = await fetch('/api/plsql/analysis/versions');
        if (!res.ok) return [];
        return res.json();
    },

    async loadVersion(version) {
        const res = await fetch(`/api/plsql/analysis/versions/${version}`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- Connection info ----
    async getDbConnections(project, env) {
        let url = '/api/plsql/db/connections';
        const params = [];
        if (project) params.push('project=' + encodeURIComponent(project));
        if (env) params.push('env=' + encodeURIComponent(env));
        if (params.length) url += '?' + params.join('&');
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    async getAnalysisConnections(name) {
        const res = await fetch(`/api/plsql/analysis/history/${encodeURIComponent(name)}/connections`, { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    // ---- History (named analyses) ----
    async listHistory() {
        const res = await fetch('/api/plsql/analysis/history');
        if (!res.ok) return [];
        return res.json();
    },

    async loadAnalysis(name) {
        const res = await fetch(`/api/plsql/analysis/history/${encodeURIComponent(name)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async deleteAnalysis(name) {
        const res = await fetch(`/api/plsql/analysis/history/${encodeURIComponent(name)}`, { method: 'DELETE' });
        if (!res.ok) return null;
        return res.json();
    },

    // ---- Call Graph ----
    async getCallGraph() {
        const res = await fetch('/api/plsql/analysis/call-graph');
        if (!res.ok) return null;
        return res.json();
    },

    async getProcDetail(procName) {
        const res = await fetch(`/api/plsql/analysis/detail/${encodeURIComponent(procName)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async getCallTree(procName) {
        const res = await fetch(`/api/plsql/analysis/call-tree/${encodeURIComponent(procName)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async getCallerTree(procName) {
        const res = await fetch(`/api/plsql/analysis/call-tree/${encodeURIComponent(procName)}/callers`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- Tables ----
    async getTableOperations() {
        const res = await fetch('/api/plsql/analysis/tables');
        if (!res.ok) return [];
        return res.json();
    },

    async getTableDetail(tableName) {
        const res = await fetch(`/api/plsql/analysis/tables/${encodeURIComponent(tableName)}`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- Sequences ----
    async getSequenceOperations() {
        const res = await fetch('/api/plsql/analysis/sequences');
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Joins ----
    async getJoinOperations() {
        const res = await fetch('/api/plsql/analysis/joins');
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Cursors ----
    async getCursorOperations() {
        const res = await fetch('/api/plsql/analysis/cursors');
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Errors ----
    async getParseErrors() {
        const res = await fetch('/api/plsql/analysis/errors');
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Procedures ----
    async getProcedures() {
        const res = await fetch('/api/plsql/analysis/procedures');
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Source ----
    async getSource(owner, objectName) {
        const res = await fetch(`/api/plsql/analysis/source/${encodeURIComponent(owner)}/${encodeURIComponent(objectName)}`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- References ----
    async getReferences(objectName, owner) {
        let url = `/api/plsql/analysis/references/${encodeURIComponent(objectName)}`;
        if (owner) url += `?owner=${encodeURIComponent(owner)}`;
        const res = await fetch(url);
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Search ----
    async search(query, type = 'all') {
        const res = await fetch(`/api/plsql/analysis/search?q=${encodeURIComponent(query)}&type=${type}`);
        if (!res.ok) return [];
        return res.json();
    },

    // ---- Database ----
    async getDbUsers() {
        const res = await fetch('/api/plsql/db/users');
        if (!res.ok) return [];
        return res.json();
    },

    async testDbConnection(username) {
        const res = await fetch('/api/plsql/db/test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username })
        });
        return res.json();
    },

    async listDbObjects(username) {
        const res = await fetch(`/api/plsql/db/objects/${encodeURIComponent(username)}`);
        if (!res.ok) return [];
        return res.json();
    },

    async listPackages(username) {
        const res = await fetch(`/api/plsql/db/packages/${encodeURIComponent(username)}`);
        if (!res.ok) return [];
        return res.json();
    },

    async listPackageProcs(username, packageName) {
        const res = await fetch(`/api/plsql/db/package/${encodeURIComponent(username)}/${encodeURIComponent(packageName)}`);
        if (!res.ok) return [];
        return res.json();
    },

    async getDbSource(username, objectName, objectType) {
        const res = await fetch(`/api/plsql/db/source/${encodeURIComponent(username)}/${encodeURIComponent(objectName)}?objectType=${encodeURIComponent(objectType)}`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- Smart Find ----
    async findObject(input) {
        const res = await fetch(`/api/plsql/db/find/${encodeURIComponent(input)}`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- Claude Verification ----
    async getClaudeStatus() {
        const res = await fetch('/api/plsql/claude/status');
        if (!res.ok) return null;
        return res.json();
    },

    async startClaudeVerification(resume) {
        const res = await fetch('/api/plsql/claude/verify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resume: resume ? 'true' : 'false' })
        });
        return res.json();
    },

    async getClaudeProgress() {
        const res = await fetch('/api/plsql/claude/progress');
        if (!res.ok) return null;
        return res.json();
    },

    async getClaudeResult() {
        const res = await fetch('/api/plsql/claude/result');
        if (!res.ok) return null;
        return res.json();
    },

    async getClaudeResultByName(name) {
        const res = await fetch(`/api/plsql/claude/result/${encodeURIComponent(name)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async listClaudeChunks() {
        const res = await fetch('/api/plsql/claude/chunks');
        if (!res.ok) return [];
        return res.json();
    },

    async listClaudeChunkSummaries() {
        const res = await fetch('/api/plsql/claude/chunks/summary');
        if (!res.ok) return [];
        return res.json();
    },

    async getClaudeChunk(chunkId) {
        const res = await fetch(`/api/plsql/claude/chunks/${encodeURIComponent(chunkId)}`);
        if (!res.ok) return null;
        return res.json();
    },

    async getTableChunkMapping() {
        const res = await fetch('/api/plsql/claude/table-chunks');
        if (!res.ok) return {};
        return res.json();
    },

    async listClaudeSessions(analysisName) {
        let url = '/api/plsql/claude/sessions';
        if (analysisName) url += `?analysisName=${encodeURIComponent(analysisName)}`;
        const res = await fetch(url);
        if (!res.ok) return [];
        return res.json();
    },

    async killClaudeSession(sessionId) {
        const res = await fetch(`/api/plsql/claude/sessions/${encodeURIComponent(sessionId)}/kill`, { method: 'POST' });
        return res.json();
    },

    async killAllClaudeSessions() {
        const res = await fetch('/api/plsql/claude/sessions/kill-all', { method: 'POST' });
        return res.json();
    },

    connectClaudeProgress(onProgress) {
        const es = new EventSource('/api/plsql/claude/progress/stream');
        es.addEventListener('claude-progress', e => onProgress(e.data));
        let errCount = 0;
        es.addEventListener('error', () => {
            if (++errCount > 3 || es.readyState === EventSource.CLOSED) es.close();
        });
        return es;
    },

    // ---- Table Info ----
    /** Get table metadata from pre-cached analysis result (instant, no DB query) */
    async getTableMetadata(tableName) {
        const res = await fetch(`/api/plsql/analysis/tables/${encodeURIComponent(tableName)}/metadata`);
        if (!res.ok) return null;
        return res.json();
    },

    async getTableTriggers(tableName) {
        const res = await fetch(`/api/plsql/analysis/tables/${encodeURIComponent(tableName)}/triggers`);
        if (!res.ok) return [];
        return res.json();
    },

    /** Fallback: get table info from live DB (slower) */
    async getTableInfo(tableName, schema) {
        let url = `/api/plsql/db/table-info/${encodeURIComponent(tableName)}`;
        if (schema) url += `?schema=${encodeURIComponent(schema)}`;
        const res = await fetch(url);
        if (!res.ok) return null;
        return res.json();
    },

    async executeQuery(sql, schema) {
        const res = await fetch('/api/plsql/db/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, schema })
        });
        return res.json();
    },

    // ---- Analysis Jobs ----
    async listJobs() {
        const res = await fetch('/api/plsql/analysis/jobs');
        if (!res.ok) return [];
        return res.json();
    },

    async getJob(jobId) {
        const res = await fetch(`/api/queue/${encodeURIComponent(jobId)}`);
        if (!res.ok) return null;
        const data = await res.json();
        // Queue API returns {error: "..."} for not-found instead of 404
        if (data && data.error) return null;
        return data;
    },

    async cancelJob(jobId) {
        const res = await fetch(`/api/queue/${encodeURIComponent(jobId)}/cancel`, { method: 'POST' });
        return res.json();
    },

    // ---- Config CRUD ----
    async listProjects() {
        const res = await fetch('/api/plsql/config/projects');
        if (!res.ok) return [];
        return res.json();
    },

    async createProject(project) {
        const res = await fetch('/api/plsql/config/projects', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(project)
        });
        return res.json();
    },

    async updateProject(name, project) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(name)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(project)
        });
        return res.json();
    },

    async deleteProject(name) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(name)}`, { method: 'DELETE' });
        return res.json();
    },

    async listEnvironments(projectName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments`);
        if (!res.ok) return [];
        return res.json();
    },

    async createEnvironment(projectName, env) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(env)
        });
        return res.json();
    },

    async updateEnvironment(projectName, envName, env) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(env)
        });
        return res.json();
    },

    async deleteEnvironment(projectName, envName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}`, { method: 'DELETE' });
        return res.json();
    },

    async listConnections(projectName, envName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections`);
        if (!res.ok) return [];
        return res.json();
    },

    async testConnection(conn) {
        const res = await fetch('/api/plsql/config/test-connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conn)
        });
        return res.json();
    },

    async addConnection(projectName, envName, conn) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conn)
        });
        return res.json();
    },

    async updateConnection(projectName, envName, connName, conn) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections/${encodeURIComponent(connName)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conn)
        });
        return res.json();
    },

    async deleteConnection(projectName, envName, connName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections/${encodeURIComponent(connName)}`, { method: 'DELETE' });
        return res.json();
    },

    async resolveEnvironment(project, env) {
        const res = await fetch(`/api/plsql/config/resolve?project=${encodeURIComponent(project)}&env=${encodeURIComponent(env)}`);
        if (!res.ok) return null;
        return res.json();
    },

    // ---- SSE Progress ----
    connectProgress(onProgress, onComplete, onError) {
        const es = new EventSource('/api/plsql/progress');
        let closed = false;
        let gotAnyEvent = false;
        let errorCount = 0;
        const closeOnce = () => { if (!closed) { closed = true; es.close(); } };
        es.addEventListener('progress', e => { gotAnyEvent = true; errorCount = 0; onProgress(e.data); });
        es.addEventListener('complete', e => { gotAnyEvent = true; onComplete(e.data); closeOnce(); });
        es.addEventListener('error', e => {
            errorCount++;
            // Only treat as fatal error if we got a server-sent error event with data,
            // or if we've had too many consecutive errors (SSE is truly broken)
            if (e.data) {
                closeOnce();
                onError(e.data);
            } else if (errorCount > 5) {
                // SSE is dead — but DON'T call onError, just close SSE silently.
                // Job polling will handle the rest.
                closeOnce();
                console.warn('[SSE] Too many errors, closing SSE. Job polling continues.');
            }
            // Otherwise: EventSource auto-reconnects, let it retry
        });
        return es;
    }
};
