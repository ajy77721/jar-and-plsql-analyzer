/**
 * ChatAPI — REST client for chat endpoints.
 */
const ChatAPI = {
    async createSession(name, scope, scopeContext, chatType) {
        const res = await fetch('/api/chat/sessions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, scope, scopeContext, chatType: chatType || 'classic' })
        });
        if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
        return res.json();
    },

    async listSessions(scope, chatType) {
        const params = new URLSearchParams();
        if (scope) params.set('scope', scope);
        if (chatType) params.set('chatType', chatType);
        const qs = params.toString();
        const res = await fetch('/api/chat/sessions' + (qs ? '?' + qs : ''));
        if (!res.ok) return [];
        return res.json();
    },

    async getSession(id) {
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(id));
        if (!res.ok) return null;
        return res.json();
    },

    /**
     * One backward page of a session's transcript (Q67).
     *
     * `before` is the oldest `seq` already on screen, so the next call returns the messages just
     * above it — the cursor is the server's, echoed back, never computed here.
     *
     * @param opts {before, limit, q}
     */
    async getMessages(id, opts) {
        var o = opts || {};
        var params = new URLSearchParams();
        params.set('sort', 'SEQ_DESC');
        if (o.limit) params.set('limit', o.limit);
        if (o.before !== null && o.before !== undefined) params.set('before', o.before);
        if (o.q) params.set('q', o.q);
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(id)
            + '/messages?' + params.toString());
        if (!res.ok) return null;
        // Through the shim, so a server that has not been updated (a bare array) still renders.
        return PA.normalizePage(await res.json());
    },

    async deleteSession(id) {
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(id), { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
        return res.json();
    },

    async sendMessage(sessionId, message, pageContext) {
        const payload = { message };
        if (pageContext) payload.pageContext = pageContext;
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(sessionId) + '/messages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
        return res.json();
    },

    async readFile(sessionId, path) {
        const res = await fetch('/api/chat/files/read', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId, path })
        });
        if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
        return res.json();
    },

    async writeFile(sessionId, path, content) {
        const res = await fetch('/api/chat/files/write', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId, path, content })
        });
        if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
        return res.json();
    },

    getReportUrl(sessionId) {
        return '/api/chat/sessions/' + encodeURIComponent(sessionId) + '/report';
    },

    /** Files the chatbot has written to its sandbox's ./output/ folder for THIS session — the
     *  extract/filter feature's downloadable results. Session-scoped and store-backed (so this
     *  reads correctly whichever storage mode the app runs in — local/mongodb/S3 — not raw local
     *  disk unconditionally, which was the earlier version's bug). */
    async listOutputs(sessionId) {
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(sessionId) + '/outputs');
        if (!res.ok) return [];
        return res.json();
    },

    getOutputDownloadUrl(sessionId, filename) {
        return '/api/chat/sessions/' + encodeURIComponent(sessionId) + '/outputs/' + encodeURIComponent(filename);
    },

    /** Explicit "delete this result" — the user asked for this directly: old JSON output files
     *  should be removable, ideally right after downloading them. */
    async deleteOutput(sessionId, filename) {
        const res = await fetch(
            '/api/chat/sessions/' + encodeURIComponent(sessionId) + '/outputs/' + encodeURIComponent(filename),
            { method: 'DELETE' }
        );
        return res.ok;
    },

    /** Every script the assistant has written/run for this session, newest first — the
     *  "script history" management feature (ChatController.listScripts/deleteScript). */
    async listScripts(sessionId) {
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(sessionId) + '/scripts');
        if (!res.ok) return [];
        return res.json();
    },

    getScriptDownloadUrl(sessionId, filename) {
        return '/api/chat/sessions/' + encodeURIComponent(sessionId) + '/scripts/' + encodeURIComponent(filename);
    },

    async deleteScript(sessionId, filename) {
        const res = await fetch(
            '/api/chat/sessions/' + encodeURIComponent(sessionId) + '/scripts/' + encodeURIComponent(filename),
            { method: 'DELETE' }
        );
        return res.ok;
    }
};
