/**
 * Floating AI Chatbox widget (new chatbox).
 * Bottom-right corner, only active when a JAR is loaded.
 * History persisted server-side. Completely independent from shared/chat.js.
 */
window.JA = window.JA || {};

JA.chatbox = (() => {

    let _jarId        = null;
    let _messages     = [];
    let _historyBytes = 0;
    let _busy         = false;
    let _open         = false;

    async function onJarChanged(jarId) {
        _jarId    = jarId;
        _messages = [];
        _historyBytes = 0;
        _busy     = false;

        if (jarId) {
            _ensureButton();
            _updateHeader();
            try {
                const data = await _apiGet('/api/jars/' + encodeURIComponent(jarId) + '/chat/history');
                _messages     = data.messages     || [];
                _historyBytes = data.historyBytes  || 0;
            } catch (e) {
                console.warn('[Chatbox] Could not load history:', e.message);
            }
            if (_open) _renderMessages();
        } else {
            _removeFab();
            _closePanel();
        }
    }

    function show() {
        if (!_jarId) return;
        _open = true;
        _ensurePanel();
        const panel = document.getElementById('chatbox-panel');
        if (panel) {
            panel.style.display = 'flex';
            requestAnimationFrame(() => panel.classList.add('chatbox-panel-open'));
        }
        const fab = document.getElementById('chatbox-fab');
        if (fab) fab.classList.add('chatbox-fab-active');
        _renderMessages();
        _focusInput();
    }

    function hide() {
        _open = false;
        const panel = document.getElementById('chatbox-panel');
        if (panel) {
            panel.classList.remove('chatbox-panel-open');
            setTimeout(() => { if (!_open) panel.style.display = 'none'; }, 260);
        }
        const fab = document.getElementById('chatbox-fab');
        if (fab) fab.classList.remove('chatbox-fab-active');
    }

    function toggle() {
        _open ? hide() : show();
    }

    // --- FAB ---

    function _ensureButton() {
        if (document.getElementById('chatbox-fab')) return;
        const fab = document.createElement('button');
        fab.id = 'chatbox-fab';
        fab.className = 'cbx-fab';
        fab.title = 'Ask AI about this analysis';
        fab.setAttribute('aria-label', 'Open AI chatbox');
        fab.innerHTML = _iconChat();
        fab.addEventListener('click', toggle);
        document.body.appendChild(fab);
    }

    function _removeFab() {
        const fab = document.getElementById('chatbox-fab');
        if (fab) fab.remove();
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
                        '<div class="cbx-header-title">AI Analysis Assistant</div>' +
                        '<div class="cbx-header-sub" id="chatbox-jar-sub">Analysing JAR</div>' +
                    '</div>' +
                '</div>' +
                '<div class="cbx-header-actions">' +
                    '<button class="cbx-icon-btn" id="chatbox-clear-btn" title="Clear conversation" onclick="JA.chatbox._clearChat()">' +
                        _iconTrash() +
                    '</button>' +
                    '<button class="cbx-icon-btn" title="Close" onclick="JA.chatbox.hide()">' +
                        _iconClose() +
                    '</button>' +
                '</div>' +
            '</div>' +
            '<div class="cbx-messages" id="chatbox-messages"></div>' +
            '<div class="cbx-input-area">' +
                '<textarea' +
                    ' id="chatbox-input"' +
                    ' class="cbx-input"' +
                    ' placeholder="Ask anything about this JAR analysis..."' +
                    ' rows="1"' +
                    ' onkeydown="JA.chatbox._onKeyDown(event)"' +
                    ' oninput="JA.chatbox._autoResize(this)"' +
                '></textarea>' +
                '<button class="cbx-send-btn" id="chatbox-send-btn" onclick="JA.chatbox._sendMessage()" title="Send (Enter)">' +
                    _iconSend() +
                '</button>' +
            '</div>' +
            '<div class="cbx-footer" id="chatbox-footer">Powered by Claude &bull; History persisted on server</div>';
        document.body.appendChild(panel);
        _updateHeader();
    }

    function _closePanel() { hide(); }

    function _updateHeader() {
        const sub = document.getElementById('chatbox-jar-sub');
        if (sub && _jarId) sub.textContent = _jarId;
        _updateFooter();
    }

    function _updateFooter() {
        const footer = document.getElementById('chatbox-footer');
        if (!footer) return;
        const mb = (_historyBytes / (1024 * 1024)).toFixed(2);
        const count = _messages.length;
        if (_historyBytes > 0) {
            footer.textContent = 'Powered by Claude \u2022 ' + count + ' messages \u2022 ' + mb + ' MB / 10 MB stored';
        } else {
            footer.textContent = 'Powered by Claude \u2022 History persisted on server';
        }
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
        if (_busy || !_jarId) return;

        const input = document.getElementById('chatbox-input');
        if (!input) return;

        const text = input.value.trim();
        if (!text) return;

        input.value = '';
        input.style.height = 'auto';

        _messages.push({ role: 'user', content: text });
        _renderMessages();
        _setBusy(true);

        try {
            const data = await _apiPost('/api/jars/' + encodeURIComponent(_jarId) + '/chat', { message: text });
            const reply = data.reply || '(no response)';
            _messages.push({ role: 'assistant', content: reply });
            _historyBytes = data.historyBytes || _historyBytes;
        } catch (err) {
            _messages.pop();
            _messages.push({ role: 'assistant', content: 'Error: ' + (err.message || 'Unknown error'), error: true });
        }

        _setBusy(false);
        _renderMessages();
        _scrollToBottom();
        _updateFooter();
    }

    async function _clearChat() {
        if (!_jarId) return;
        try {
            await _apiDelete('/api/jars/' + encodeURIComponent(_jarId) + '/chat/history');
        } catch (e) {
            if (JA.toast) JA.toast.warn('Could not clear server history: ' + e.message);
        }
        _messages = [];
        _historyBytes = 0;
        _renderMessages();
        _updateFooter();
        _focusInput();
    }

    function _setBusy(busy) {
        _busy = busy;
        const btn   = document.getElementById('chatbox-send-btn');
        const input = document.getElementById('chatbox-input');
        if (btn)   { btn.disabled = busy; btn.innerHTML = busy ? _iconSpinner() : _iconSend(); }
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
                    '<p>Ask me anything about this JAR analysis.</p>' +
                    '<div class="cbx-suggestions">' +
                        '<button class="cbx-suggestion" onclick="JA.chatbox._insertSuggestion(this)">List all endpoints</button>' +
                        '<button class="cbx-suggestion" onclick="JA.chatbox._insertSuggestion(this)">Which collections are mutated?</button>' +
                        '<button class="cbx-suggestion" onclick="JA.chatbox._insertSuggestion(this)">What are the riskiest endpoints?</button>' +
                        '<button class="cbx-suggestion" onclick="JA.chatbox._insertSuggestion(this)">Summarise the domain model</button>' +
                    '</div>' +
                '</div>';
            return;
        }

        let html = '';
        for (const msg of _messages) {
            const isUser   = msg.role === 'user';
            const bubbleCls = isUser
                ? 'cbx-bubble-user'
                : ('cbx-bubble-ai' + (msg.error ? ' cbx-bubble-error' : ''));
            const content  = _formatContent(msg.content);
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
        const esc = JA.utils.escapeHtml;
        let out = esc(text);
        out = out.replace(/```([^`]*?)```/gs, function(_, code) {
            return '<pre class="cbx-code-block"><code>' + code.trim() + '</code></pre>';
        });
        out = out.replace(/`([^`]+?)`/g, '<code class="cbx-inline-code">$1</code>');
        out = out.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        out = out.replace(/\n/g, '<br>');
        return out;
    }

    function _scrollToBottom() {
        const container = document.getElementById('chatbox-messages');
        if (container) container.scrollTop = container.scrollHeight;
    }

    function _focusInput() {
        setTimeout(function() {
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

    // --- API helpers (self-contained, no dependency on shared/chat-api.js) ---

    async function _apiGet(url) {
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    }

    async function _apiPost(url, body) {
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    }

    async function _apiDelete(url) {
        const res = await fetch(url, { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    }

    // --- SVG Icons ---

    function _iconChat() {
        return '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
            '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>' +
        '</svg>';
    }
    function _iconSparkle() {
        return '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">' +
            '<path d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 17l-6.2 4.3 2.4-7.4L2 9.4h7.6z"/>' +
        '</svg>';
    }
    function _iconSend() {
        return '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
            '<line x1="22" y1="2" x2="11" y2="13"/>' +
            '<polygon points="22 2 15 22 11 13 2 9 22 2"/>' +
        '</svg>';
    }
    function _iconClose() {
        return '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">' +
            '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>' +
        '</svg>';
    }
    function _iconTrash() {
        return '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
            '<polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/>' +
            '<path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/>' +
        '</svg>';
    }
    function _iconSpinner() {
        return '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="cbx-spin">' +
            '<path d="M21 12a9 9 0 1 1-6.219-8.56"/>' +
        '</svg>';
    }

    return {
        onJarChanged, show, hide, toggle,
        _sendMessage, _onKeyDown, _autoResize, _clearChat, _insertSuggestion
    };

})();
