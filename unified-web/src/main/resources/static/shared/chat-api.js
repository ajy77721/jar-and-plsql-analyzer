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

    async deleteSession(id) {
        const res = await fetch('/api/chat/sessions/' + encodeURIComponent(id), { method: 'DELETE' });
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
        return res.json();
    },

    async readFile(sessionId, path) {
        const res = await fetch('/api/chat/files/read', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId, path })
        });
        return res.json();
    },

    async writeFile(sessionId, path, content) {
        const res = await fetch('/api/chat/files/write', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId, path, content })
        });
        return res.json();
    },

    getReportUrl(sessionId) {
        return '/api/chat/sessions/' + encodeURIComponent(sessionId) + '/report';
    }
};
