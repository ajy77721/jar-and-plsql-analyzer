/**
 * Shared floating AI Chatbox — works on all pages (Home, PL/SQL, JAR).
 * Uses session-based ChatAPI (shared/chat-api.js).
 * Scope-aware: auto-creates a session per scope.
 */
const Chatbox = (() => {

    let _scope       = 'GLOBAL';
    let _contextFn   = () => ({});
    let _sessionId   = null;
    let _messages     = [];
    let _busy         = false;
    let _open         = false;
    let _inited       = false;

    function init(scope, contextFn) {
        if (_inited) return;
        _inited = true;
        _scope = scope || 'GLOBAL';
        _contextFn = contextFn || (() => ({}));
        _ensureFab();
    }

    function show() {
        _open = true;
        _ensurePanel();
        const panel = document.getElementById('chatbox-panel');
        if (panel) {
            panel.style.display = 'flex';
            requestAnimationFrame(() => panel.classList.add('cbx-panel-open'));
        }
        const fab = document.getElementById('chatbox-fab');
        if (fab) fab.classList.add('cbx-fab-active');
        if (!_sessionId) {
            _loadOrCreateSession().then(() => _renderMessages());
        } else {
            _renderMessages();
        }
        _focusInput();
    }

    function hide() {
        _open = false;
        const panel = document.getElementById('chatbox-panel');
        if (panel) {
            panel.classList.remove('cbx-panel-open');
            setTimeout(() => { if (!_open) panel.style.display = 'none'; }, 260);
        }
        const fab = document.getElementById('chatbox-fab');
        if (fab) fab.classList.remove('cbx-fab-active');
    }

    function toggle() { _open ? hide() : show(); }

    function destroy() {
        const fab = document.getElementById('chatbox-fab');
        const panel = document.getElementById('chatbox-panel');
        if (fab) fab.remove();
        if (panel) panel.remove();
        _inited = false;
        _open = false;
        _sessionId = null;
        _messages = [];
    }

    // --- Session ---

    async function _loadOrCreateSession() {
        try {
            const sessions = await ChatAPI.listSessions(_scope, 'new');
            if (sessions.length) {
                _sessionId = sessions[0].id;
                const full = await ChatAPI.getSession(_sessionId);
                _messages = (full && full.messages) ? full.messages : [];
                return;
            }
        } catch (e) {}

        try {
            const name = _scope + ' Chat ' + new Date().toLocaleDateString();
            const res = await ChatAPI.createSession(name, _scope, _contextFn(), 'new');
            if (res && res.id) {
                _sessionId = res.id;
                _messages = [];
            }
        } catch (e) {
            console.warn('[Chatbox] Could not create session:', e.message);
        }
    }

    // --- FAB ---

    function _ensureFab() {
        if (document.getElementById('chatbox-fab')) return;
        const fab = document.createElement('button');
        fab.id = 'chatbox-fab';
        fab.className = 'cbx-fab';
        fab.title = 'Ask AI';
        fab.setAttribute('aria-label', 'Open AI chatbox');
        fab.innerHTML = _iconChat();
        fab.addEventListener('click', toggle);
        document.body.appendChild(fab);
    }

    // --- Panel ---

    function _ensurePanel() {
        if (document.getElementById('chatbox-panel')) return;

        const panel = document.createElement('div');
        panel.id = 'chatbox-panel';
        panel.className = 'cbx-panel';
        panel.style.display = 'none';
        panel.innerHTML =
            '<div class="cbx-header">' +
                '<div class="cbx-header-left">' +
                    '<span class="cbx-header-icon">' + _iconSparkle() + '</span>' +
                    '<div>' +
                        '<div class="cbx-header-title">AI Assistant</div>' +
                        '<div class="cbx-header-sub" id="chatbox-scope-sub">' + _esc(_scope) + '</div>' +
                    '</div>' +
                '</div>' +
                '<div class="cbx-header-actions">' +
                    '<button class="cbx-icon-btn" id="chatbox-clear-btn" title="Clear conversation" onclick="Chatbox._clearChat()">' +
                        _iconTrash() +
                    '</button>' +
                    '<button class="cbx-icon-btn" title="Close" onclick="Chatbox.hide()">' +
                        _iconClose() +
                    '</button>' +
                '</div>' +
            '</div>' +
            '<div class="cbx-messages" id="chatbox-messages"></div>' +
            '<div class="cbx-input-area">' +
                '<textarea' +
                    ' id="chatbox-input"' +
                    ' class="cbx-input"' +
                    ' placeholder="Ask anything..."' +
                    ' rows="1"' +
                    ' onkeydown="Chatbox._onKeyDown(event)"' +
                    ' oninput="Chatbox._autoResize(this)"' +
                '></textarea>' +
                '<button class="cbx-send-btn" id="chatbox-send-btn" onclick="Chatbox._sendMessage()" title="Send (Enter)">' +
                    _iconSend() +
                '</button>' +
            '</div>' +
            '<div class="cbx-footer" id="chatbox-footer">Powered by Claude</div>';
        document.body.appendChild(panel);
    }

    // --- Page Context ---

    function _buildPageContext() {
        const parts = [];
        parts.push('URL: ' + window.location.href);
        parts.push('Page: ' + document.title);

        const ctx = _contextFn();
        if (ctx && typeof ctx === 'object') {
            for (const [k, v] of Object.entries(ctx)) {
                if (v) parts.push(k + ': ' + v);
            }
        }

        const hash = window.location.hash || '';
        if (hash) parts.push('Route: ' + hash);

        return parts.join('\n');
    }

    // --- Messaging ---

    function _onKeyDown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            _sendMessage();
        }
    }

    function _autoResize(el) {
        el.style.height = 'auto';
        el.style.height = Math.min(el.scrollHeight, 120) + 'px';
    }

    async function _sendMessage() {
        if (_busy) return;

        const input = document.getElementById('chatbox-input');
        if (!input) return;
        const text = input.value.trim();
        if (!text) return;

        input.value = '';
        input.style.height = 'auto';

        if (!_sessionId) {
            await _loadOrCreateSession();
            if (!_sessionId) {
                _messages.push({ role: 'assistant', content: 'Error: Could not create chat session.', error: true });
                _renderMessages();
                return;
            }
        }

        _messages.push({ role: 'user', content: text });
        _renderMessages();
        _setBusy(true);

        try {
            const pageContext = _buildPageContext();
            const res = await ChatAPI.sendMessage(_sessionId, text, pageContext);
            if (res.error) {
                _messages.push({ role: 'assistant', content: 'Error: ' + res.error, error: true });
            } else {
                _messages.push({ role: 'assistant', content: res.content || '(no response)' });
            }
        } catch (err) {
            _messages.push({ role: 'assistant', content: 'Error: ' + (err.message || 'Unknown error'), error: true });
        }

        _setBusy(false);
        _renderMessages();
        _scrollToBottom();
    }

    async function _clearChat() {
        if (_sessionId) {
            try { await ChatAPI.deleteSession(_sessionId); } catch (e) {}
        }
        _sessionId = null;
        _messages = [];
        _renderMessages();
        _focusInput();
    }

    function _setBusy(busy) {
        _busy = busy;
        const btn = document.getElementById('chatbox-send-btn');
        const input = document.getElementById('chatbox-input');
        if (btn) { btn.disabled = busy; btn.innerHTML = busy ? _iconSpinner() : _iconSend(); }
        if (input) input.disabled = busy;
    }

    // --- Render ---

    function _renderMessages() {
        const container = document.getElementById('chatbox-messages');
        if (!container) return;

        if (_messages.length === 0) {
            container.innerHTML =
                '<div class="cbx-empty">' +
                    '<div class="cbx-empty-icon">' + _iconSparkle() + '</div>' +
                    '<p>Ask me anything about your analysis.</p>' +
                    '<div class="cbx-suggestions">' +
                        '<button class="cbx-suggestion" onclick="Chatbox._insertSuggestion(this)">Summarise this analysis</button>' +
                        '<button class="cbx-suggestion" onclick="Chatbox._insertSuggestion(this)">What are the key findings?</button>' +
                        '<button class="cbx-suggestion" onclick="Chatbox._insertSuggestion(this)">Show me the riskiest items</button>' +
                    '</div>' +
                '</div>';
            return;
        }

        let html = '';
        for (const msg of _messages) {
            const isUser = msg.role === 'user';
            const bubbleCls = isUser
                ? 'cbx-bubble-user'
                : ('cbx-bubble-ai' + (msg.error ? ' cbx-bubble-error' : ''));
            const content = _formatContent(msg.content);
            html += '<div class="cbx-message ' + (isUser ? 'cbx-message-user' : 'cbx-message-ai') + '">' +
                '<div class="cbx-bubble ' + bubbleCls + '">' + content + '</div>' +
            '</div>';
        }

        if (_busy) {
            html += '<div class="cbx-message cbx-message-ai">' +
                '<div class="cbx-bubble cbx-bubble-ai cbx-bubble-typing">' +
                    '<span class="cbx-typing-dot"></span><span class="cbx-typing-dot"></span><span class="cbx-typing-dot"></span>' +
                '</div>' +
            '</div>';
        }

        container.innerHTML = html;
        _scrollToBottom();
    }

    function _formatContent(text) {
        let out = _esc(text);
        out = out.replace(/```([^`]*?)```/gs, function(_, code) {
            return '<pre class="cbx-code-block"><code>' + code.trim() + '</code></pre>';
        });
        out = out.replace(/`([^`]+?)`/g, '<code class="cbx-inline-code">$1</code>');
        out = out.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        out = out.replace(/\n/g, '<br>');
        return out;
    }

    function _scrollToBottom() {
        const c = document.getElementById('chatbox-messages');
        if (c) c.scrollTop = c.scrollHeight;
    }

    function _focusInput() {
        setTimeout(() => {
            const input = document.getElementById('chatbox-input');
            if (input) input.focus();
        }, 50);
    }

    function _insertSuggestion(btn) {
        const input = document.getElementById('chatbox-input');
        if (!input) return;
        input.value = btn.textContent;
        _autoResize(input);
        input.focus();
    }

    function _esc(s) {
        if (!s) return '';
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    // --- SVG Icons ---

    function _iconChat() {
        return '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>';
    }
    function _iconSparkle() {
        return '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 17l-6.2 4.3 2.4-7.4L2 9.4h7.6z"/></svg>';
    }
    function _iconSend() {
        return '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>';
    }
    function _iconClose() {
        return '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';
    }
    function _iconTrash() {
        return '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/></svg>';
    }
    function _iconSpinner() {
        return '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="cbx-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>';
    }

    return {
        init, show, hide, toggle, destroy,
        _sendMessage, _onKeyDown, _autoResize, _clearChat, _insertSuggestion
    };

})();
