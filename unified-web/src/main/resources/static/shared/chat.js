/**
 * ChatPanel — shared chat UI component.
 * Features: resizable 30% panel, search, file attach, dir read, file create w/ approval, settings.
 */
const ChatPanel = {
    _scope: 'GLOBAL',
    _contextFn: () => ({}),
    _currentSessionId: null,
    _isOpen: false,
    _sending: false,
    _attachments: [],
    _searchVisible: false,
    _searchMatches: [],
    _searchIdx: -1,
    _settings: { allowedDirs: [], autoApproveReads: false, autoApproveWrites: false, maxFileSizeKb: 500 },
    _pendingApprovals: [],
    _dockSide: 'right',
    _panelWidth: 30,
    _greeted: false,  // true once the initial context greeting has been sent for this session

    init(containerSelector, scope, contextFn) {
        this._scope = scope || 'GLOBAL';
        this._contextFn = contextFn || (() => ({}));
        const container = document.querySelector(containerSelector);
        if (!container) return;

        const saved = localStorage.getItem('chat_panel_prefs');
        if (saved) {
            try {
                const p = JSON.parse(saved);
                if (p.width) this._panelWidth = p.width;
                if (p.dock) this._dockSide = p.dock;
            } catch (e) {}
        }

        container.innerHTML = `
            <button class="chat-fab" onclick="ChatPanel.toggle()" title="Chat with Claude">&#128172;</button>
            <div class="chat-overlay" id="chatOverlay" onclick="ChatPanel.close()"></div>
            <div class="chat-panel${this._dockSide === 'left' ? ' dock-left' : ''}" id="chatPanel">
                <div class="chat-resize-handle" id="chatResizeHandle"></div>
                <div class="chat-header">
                    <span class="chat-header-title">Claude Chat</span>
                    <span class="scope-badge ${this._esc(this._scope)}">${this._esc(this._scope)}</span>
                    <div style="flex:1"></div>
                    <button class="chat-header-btn" onclick="ChatPanel.toggleDock()" title="Move to other side">&#8644;</button>
                    <button class="chat-header-btn" onclick="ChatPanel.toggleSearch()" title="Search messages">&#128269;</button>
                    <button class="chat-header-btn" onclick="ChatPanel.showSettings()" title="Session settings">&#9881;</button>
                    <button class="chat-header-btn" onclick="ChatPanel.showSessionList()" title="Sessions">&#9776;</button>
                    <button class="chat-header-btn" onclick="ChatPanel.close()" title="Close">&times;</button>
                </div>
                <div id="chatSearchBar" class="chat-search-bar" style="display:none">
                    <input type="text" id="chatSearchInput" placeholder="Search messages..." oninput="ChatPanel._doSearch(this.value)">
                    <span class="badge" id="chatSearchCount">0</span>
                    <button onclick="ChatPanel._searchPrev()">&#9650;</button>
                    <button onclick="ChatPanel._searchNext()">&#9660;</button>
                    <button onclick="ChatPanel.toggleSearch()">&times;</button>
                </div>
                <div id="chatContent" style="flex:1;display:flex;flex-direction:column;overflow:hidden"></div>
                <input type="file" id="chatFileInput" class="chat-file-input" multiple onchange="ChatPanel._onFileSelect(event)">
            </div>
        `;

        this._applyWidth();
        this._initResize();
        this.showSessionList();
    },

    toggle() { this._isOpen ? this.close() : this.open(); },

    open() {
        this._isOpen = true;
        const panel = document.getElementById('chatPanel');
        panel.classList.add('open');
        document.getElementById('chatOverlay').classList.add('open');
        this._applyWidth();
    },

    close() {
        this._isOpen = false;
        const panel = document.getElementById('chatPanel');
        panel.classList.remove('open');
        document.getElementById('chatOverlay').classList.remove('open');
        this._applyWidth();
    },

    toggleDock() {
        this._dockSide = this._dockSide === 'right' ? 'left' : 'right';
        const panel = document.getElementById('chatPanel');
        if (this._dockSide === 'left') {
            panel.classList.add('dock-left');
        } else {
            panel.classList.remove('dock-left');
        }
        this._applyWidth();
        this._savePrefs();
    },

    _applyWidth() {
        const panel = document.getElementById('chatPanel');
        if (!panel) return;
        panel.style.width = this._panelWidth + '%';
        if (this._dockSide === 'right') {
            panel.style.right = this._isOpen ? '0' : (-this._panelWidth - 5) + '%';
            panel.style.left = '';
        } else {
            panel.style.left = this._isOpen ? '0' : (-this._panelWidth - 5) + '%';
            panel.style.right = '';
        }
    },

    _initResize() {
        const handle = document.getElementById('chatResizeHandle');
        if (!handle) return;
        let startX, startW;
        const onMove = (e) => {
            const panel = document.getElementById('chatPanel');
            const vw = window.innerWidth;
            let newW;
            if (this._dockSide === 'right') {
                newW = ((vw - e.clientX) / vw) * 100;
            } else {
                newW = (e.clientX / vw) * 100;
            }
            newW = Math.max(20, Math.min(80, newW));
            this._panelWidth = Math.round(newW);
            panel.style.width = this._panelWidth + '%';
        };
        const onUp = () => {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            const panel = document.getElementById('chatPanel');
            panel.classList.remove('resizing');
            handle.classList.remove('active');
            this._savePrefs();
        };
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            const panel = document.getElementById('chatPanel');
            panel.classList.add('resizing');
            handle.classList.add('active');
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        });
    },

    _savePrefs() {
        localStorage.setItem('chat_panel_prefs', JSON.stringify({
            width: this._panelWidth,
            dock: this._dockSide
        }));
    },

    toggleSearch() {
        this._searchVisible = !this._searchVisible;
        const bar = document.getElementById('chatSearchBar');
        bar.style.display = this._searchVisible ? 'flex' : 'none';
        if (this._searchVisible) {
            document.getElementById('chatSearchInput').focus();
        } else {
            this._clearSearchHighlights();
        }
    },

    // ---- Session list ----

    async showSessionList() {
        this._currentSessionId = null;
        const el = document.getElementById('chatContent');
        el.innerHTML = '<div class="chat-sessions"><div style="text-align:center;color:var(--sidebar-muted);padding:20px">Loading...</div></div>';
        const sessions = await ChatAPI.listSessions(this._scope, 'classic');
        let html = '<div class="chat-sessions">';
        html += '<button class="chat-new-btn" onclick="ChatPanel.createSession()">+ New Chat Session</button>';
        if (!sessions.length) {
            html += '<div style="text-align:center;color:var(--sidebar-muted);padding:24px;font-size:13px">No sessions yet. Start a new chat!</div>';
        } else {
            for (const s of sessions) {
                const time = (s.updatedAt || s.createdAt || '').toString().replace('T', ' ').substring(0, 19);
                html += `<div class="chat-session-item" onclick="ChatPanel.openSession('${this._esc(s.id)}')">
                    <div class="chat-session-name">${this._esc(s.name)}</div>
                    <div class="chat-session-meta">${s.messageCount || 0} messages &middot; ${time}</div>
                    <button class="chat-session-del" onclick="event.stopPropagation();ChatPanel.deleteSession('${this._esc(s.id)}')" title="Delete">&times;</button>
                </div>`;
            }
        }
        html += '</div>';
        el.innerHTML = html;
    },

    async createSession() {
        const name = prompt('Session name:');
        if (!name) return;
        const ctx = this._contextFn();
        const res = await ChatAPI.createSession(name, this._scope, ctx, 'classic');
        if (res.error) { alert(res.error); return; }
        this._currentSessionId = res.id;
        this._greeted = false;
        this._loadSessionSettings();
        this.renderChat([]);
    },

    /** Messages per transcript request. Matches the endpoint's default (PLAN-08 Q67: 50). */
    _msgPageSize: 50,

    /**
     * Opens a session showing the newest page of its transcript (Q67 / L30).
     *
     * This used to fetch `/sessions/{id}`, whose response carries every message ever exchanged in
     * the session, to render the last screenful of it. Older messages now arrive when the reader
     * asks for them, which is also the only point at which they are wanted.
     */
    async openSession(id) {
        this._currentSessionId = id;
        this._oldestSeq = null;
        this._hasOlder = false;
        this._searchQuery = '';
        const env = await ChatAPI.getMessages(id, { limit: this._msgPageSize });
        if (!env) { alert('Session not found'); return; }
        this._greeted = false;
        this._loadSessionSettings();
        // The page is in SEQ_DESC order, as its envelope says; reading order is the reverse.
        const messages = env.items.slice().reverse();
        this._noteWindow(env, messages);
        this.renderChat(messages);
    },

    /** Records where the loaded window starts, so "load older" asks from the right place. */
    _noteWindow(env, messages) {
        this._hasOlder = !!(env.page && env.page.hasMore);
        this._oldestSeq = messages.length && messages[0].seq !== undefined
            ? messages[0].seq : null;
        // Without a seq there is nothing to page back from — an older server answering with a
        // bare array, for instance. Say there is no more rather than looping on the same page.
        if (this._oldestSeq === null) this._hasOlder = false;
    },

    /** Prepends the page of messages immediately older than the ones on screen. */
    async loadOlderMessages() {
        if (!this._currentSessionId || !this._hasOlder) return;
        const btn = document.getElementById('chatLoadOlder');
        if (btn) { btn.disabled = true; btn.textContent = 'Loading…'; }
        const env = await ChatAPI.getMessages(this._currentSessionId,
            { limit: this._msgPageSize, before: this._oldestSeq, q: this._searchQuery || null });
        if (!env) { if (btn) { btn.disabled = false; btn.textContent = 'Load older messages'; } return; }
        const older = env.items.slice().reverse();
        this._noteWindow(env, older);
        const list = document.getElementById('chatMessages');
        if (!list) return;
        // Prepended, and the scroll position held against the height the new block added, so the
        // message the reader was looking at does not jump off the screen.
        const before = list.scrollHeight;
        let html = this._olderBarHtml();
        for (const m of older) html += this._renderMessage(m);
        list.insertAdjacentHTML('afterbegin', html);
        const oldBar = list.querySelectorAll('.chat-older-bar');
        for (let i = 1; i < oldBar.length; i++) oldBar[i].remove();
        list.scrollTop += list.scrollHeight - before;
    },

    _olderBarHtml() {
        if (!this._hasOlder) return '';
        return '<div class="chat-older-bar" style="text-align:center;padding:8px">'
            + '<button class="chat-action-btn" id="chatLoadOlder" '
            + 'onclick="ChatPanel.loadOlderMessages()">Load older messages</button></div>';
    },

    async deleteSession(id) {
        if (!confirm('Delete this chat session?')) return;
        await ChatAPI.deleteSession(id);
        if (this._currentSessionId === id) this._currentSessionId = null;
        this.showSessionList();
    },

    // ---- Chat view ----

    renderChat(messages) {
        this._attachments = [];
        this._pendingApprovals = [];
        const el = document.getElementById('chatContent');
        let html = '';

        html += '<div class="chat-actions">';
        html += '<button class="chat-action-btn" onclick="ChatPanel.showSessionList()">&#8592; Sessions</button>';
        html += `<a class="chat-action-btn" href="${ChatAPI.getReportUrl(this._currentSessionId)}" target="_blank">&#8595; Report</a>`;
        html += '<button class="chat-action-btn" onclick="ChatPanel.showSettings()">&#9881; Settings</button>';
        html += '</div>';

        html += '<div class="chat-messages" id="chatMessages">';
        html += this._olderBarHtml();
        for (const m of messages) {
            html += this._renderMessage(m);
        }
        if (!messages.length) {
            html += '<div style="text-align:center;color:var(--sidebar-muted);padding:50px 24px;font-size:13px">Start the conversation by typing a message below.</div>';
        }
        html += '</div>';

        // Auto-send initial greeting for brand-new empty sessions
        if (!messages.length) {
            setTimeout(() => this._maybeSendGreeting(), 100);
        }

        html += '<div id="chatAttachArea" class="chat-attachments" style="display:none"></div>';

        html += `<div class="chat-input-area">
            <div class="chat-input-tools">
                <button class="chat-tool-btn" onclick="document.getElementById('chatFileInput').click()" title="Attach file">&#128206;</button>
                <button class="chat-tool-btn" onclick="ChatPanel._promptReadFile()" title="Read file from path">&#128193;</button>
            </div>
            <textarea class="chat-input" id="chatInput" placeholder="Type your message..." onkeydown="ChatPanel._onKeyDown(event)" rows="1" oninput="ChatPanel._autoGrow(this)"></textarea>
            <button class="chat-send-btn" id="chatSendBtn" onclick="ChatPanel.send()">Send</button>
        </div>`;

        el.innerHTML = html;
        this._scrollToBottom();
    },

    // ---- Send message ----

    async send() {
        if (this._sending || !this._currentSessionId) return;
        const input = document.getElementById('chatInput');
        const msg = input.value.trim();
        if (!msg && !this._attachments.length) return;

        this._sending = true;
        input.value = '';
        input.style.height = 'auto';
        document.getElementById('chatSendBtn').disabled = true;

        let fullMessage = msg;
        const attachNames = this._attachments.map(a => a.name);
        if (this._attachments.length) {
            fullMessage += '\n\n--- Attached Files ---\n';
            for (const att of this._attachments) {
                fullMessage += `\n**File: ${att.name}** (${att.size} bytes)\n\`\`\`\n${att.content}\n\`\`\`\n`;
            }
            this._attachments = [];
            this._updateAttachUI();
        }

        const msgEl = document.getElementById('chatMessages');
        let userBubble = this._renderMessage({ role: 'user', content: msg });
        if (attachNames.length) {
            const chips = attachNames.map(n => `<span style="display:inline-block;background:var(--bg-hover);padding:2px 8px;border-radius:var(--radius-lg);font-size:11px;margin:2px">&#128206; ${this._esc(n)}</span>`).join(' ');
            userBubble = userBubble.replace('</div>', `<div style="margin-top:6px">${chips}</div></div>`);
        }
        msgEl.innerHTML += userBubble;
        msgEl.innerHTML += '<div class="chat-typing" id="chatTyping"><span class="typing-dots">Claude is thinking</span></div>';
        this._scrollToBottom();

        try {
            const pageContext = this._buildPageContext();
            const res = await ChatAPI.sendMessage(this._currentSessionId, fullMessage, pageContext);
            const typing = document.getElementById('chatTyping');
            if (typing) typing.remove();

            if (res.error) {
                msgEl.innerHTML += '<div class="chat-msg assistant" style="color:var(--red)">Error: ' + this._esc(res.error) + '</div>';
            } else {
                const content = res.content || '';
                const approvals = this._extractApprovals(content);
                if (approvals.length) {
                    msgEl.innerHTML += this._renderMessage({ role: 'assistant', content: this._stripApprovalBlocks(content) });
                    for (const a of approvals) {
                        msgEl.innerHTML += this._renderApproval(a);
                    }
                } else {
                    msgEl.innerHTML += this._renderMessage({ role: 'assistant', content });
                }
            }
        } catch (e) {
            const typing = document.getElementById('chatTyping');
            if (typing) typing.remove();
            msgEl.innerHTML += '<div class="chat-msg assistant" style="color:var(--red)">Error: ' + this._esc(e.message) + '</div>';
        }

        this._sending = false;
        document.getElementById('chatSendBtn').disabled = false;
        this._scrollToBottom();
        input.focus();
    },

    // ---- File attachment ----

    _onFileSelect(event) {
        const files = event.target.files;
        if (!files.length) return;
        const maxSize = (this._settings.maxFileSizeKb || 500) * 1024;

        for (const file of files) {
            if (file.size > maxSize) {
                alert(`File "${file.name}" exceeds ${this._settings.maxFileSizeKb}KB limit.`);
                continue;
            }
            const reader = new FileReader();
            reader.onload = (e) => {
                this._attachments.push({ name: file.name, size: file.size, content: e.target.result });
                this._updateAttachUI();
            };
            reader.readAsText(file);
        }
        event.target.value = '';
    },

    _updateAttachUI() {
        const area = document.getElementById('chatAttachArea');
        if (!area) return;
        if (!this._attachments.length) { area.style.display = 'none'; return; }
        area.style.display = 'flex';
        area.innerHTML = this._attachments.map((a, i) =>
            `<div class="chat-attach-chip">
                <span title="${this._esc(a.name)}">${this._esc(a.name)}</span>
                <button onclick="ChatPanel._removeAttach(${i})">&times;</button>
            </div>`
        ).join('');
    },

    _removeAttach(idx) {
        this._attachments.splice(idx, 1);
        this._updateAttachUI();
    },

    // ---- Read file from path (inline bar, no alert) ----

    _promptReadFile() {
        const existing = document.getElementById('chatPathBar');
        if (existing) { existing.remove(); return; }
        const inputArea = document.querySelector('.chat-input-area');
        if (!inputArea) return;
        const bar = document.createElement('div');
        bar.id = 'chatPathBar';
        bar.className = 'chat-search-bar';
        bar.style.borderTop = '1px solid var(--sidebar-border)';
        bar.style.borderBottom = 'none';
        bar.innerHTML = `
            <span style="font-size:11px;color:var(--sidebar-muted);white-space:nowrap">&#128193; Path:</span>
            <input type="text" id="chatPathInput" placeholder="Enter file or folder path..." style="flex:1" onkeydown="if(event.key==='Enter'){ChatPanel._doReadPath();event.preventDefault();}">
            <button onclick="ChatPanel._doReadPath()" style="background:var(--accent);color:var(--text-inverse)">Read</button>
            <button onclick="document.getElementById('chatPathBar').remove()">&times;</button>
        `;
        inputArea.parentNode.insertBefore(bar, inputArea);
        document.getElementById('chatPathInput').focus();
    },

    async _doReadPath() {
        const input = document.getElementById('chatPathInput');
        if (!input) return;
        const path = input.value.trim();
        if (!path) return;

        const settings = this._settings;
        if (!settings.autoApproveReads && settings.allowedDirs && settings.allowedDirs.length) {
            const normPath = path.replace(/\\/g, '/');
            const allowed = settings.allowedDirs.some(d => normPath.startsWith(d.replace(/\\/g, '/')));
            if (!allowed) {
                const msgEl = document.getElementById('chatMessages');
                if (msgEl) {
                    msgEl.innerHTML += `<div class="chat-msg assistant" style="color:var(--orange);font-size:12px">Path "${this._esc(path)}" is outside allowed directories. Add it in Settings or enable auto-approve reads.</div>`;
                    this._scrollToBottom();
                }
                return;
            }
        }

        input.disabled = true;
        try {
            const res = await ChatAPI.readFile(this._currentSessionId, path);
            if (res.error) {
                const msgEl = document.getElementById('chatMessages');
                if (msgEl) {
                    msgEl.innerHTML += `<div class="chat-msg assistant" style="color:var(--red);font-size:12px">${this._esc(res.error)}</div>`;
                    this._scrollToBottom();
                }
            } else {
                this._attachments.push({ name: path.split(/[/\\]/).pop(), size: (res.content || '').length, content: res.content, path });
                this._updateAttachUI();
                input.value = '';
            }
        } catch (e) {
            const msgEl = document.getElementById('chatMessages');
            if (msgEl) {
                msgEl.innerHTML += `<div class="chat-msg assistant" style="color:var(--red);font-size:12px">Failed: ${this._esc(e.message)}</div>`;
                this._scrollToBottom();
            }
        }
        input.disabled = false;
        input.focus();
    },

    // ---- Approval workflow ----

    _extractApprovals(content) {
        const regex = /\[FILE_CREATE:(.+?)\]\n```[\w]*\n([\s\S]*?)```/g;
        const approvals = [];
        let m;
        while ((m = regex.exec(content)) !== null) {
            approvals.push({ path: m[1].trim(), content: m[2] });
        }
        return approvals;
    },

    _stripApprovalBlocks(content) {
        return content.replace(/\[FILE_CREATE:(.+?)\]\n```[\w]*\n[\s\S]*?```/g, '').trim();
    },

    _renderApproval(approval) {
        const id = 'approval_' + Math.random().toString(36).substring(2, 9);
        return `<div class="chat-approval-banner" id="${id}">
            <div class="approval-title">File Create Request</div>
            <div class="approval-path">${this._esc(approval.path)}</div>
            <pre>${this._esc(approval.content.substring(0, 2000))}${approval.content.length > 2000 ? '\n...(truncated)' : ''}</pre>
            <div class="chat-approval-actions">
                <button class="approve-btn" onclick="ChatPanel._handleApproval('${id}', '${this._esc(approval.path)}', true)">Approve &amp; Create</button>
                <button class="reject-btn" onclick="ChatPanel._handleApproval('${id}', '${this._esc(approval.path)}', false)">Reject</button>
            </div>
        </div>`;
    },

    async _handleApproval(elId, path, approved) {
        const el = document.getElementById(elId);
        if (!el) return;
        if (approved) {
            try {
                const pre = el.querySelector('pre');
                const content = pre ? pre.textContent : '';
                const res = await ChatAPI.writeFile(this._currentSessionId, path, content);
                if (res.error) {
                    el.innerHTML = `<div style="color:var(--red);font-size:12px">Write failed: ${this._esc(res.error)}</div>`;
                } else {
                    el.innerHTML = `<div style="color:var(--green);font-size:12px">File created: ${this._esc(path)}</div>`;
                }
            } catch (e) {
                el.innerHTML = `<div style="color:var(--red);font-size:12px">Error: ${this._esc(e.message)}</div>`;
            }
        } else {
            el.innerHTML = '<div style="color:var(--sidebar-muted);font-size:12px">File creation rejected.</div>';
        }
    },

    // ---- Settings ----

    _loadSessionSettings() {
        const key = 'chat_settings_' + (this._currentSessionId || 'global');
        try {
            const saved = localStorage.getItem(key);
            if (saved) this._settings = JSON.parse(saved);
            else this._settings = { allowedDirs: [], autoApproveReads: false, autoApproveWrites: false, maxFileSizeKb: 500 };
        } catch (e) {
            this._settings = { allowedDirs: [], autoApproveReads: false, autoApproveWrites: false, maxFileSizeKb: 500 };
        }
    },

    _saveSessionSettings() {
        const key = 'chat_settings_' + (this._currentSessionId || 'global');
        localStorage.setItem(key, JSON.stringify(this._settings));
    },

    showSettings() {
        const panel = document.getElementById('chatPanel');
        if (document.getElementById('chatSettingsOverlay')) return;

        const dirs = (this._settings.allowedDirs || []).map((d, i) =>
            `<div class="chat-settings-dir"><span>${this._esc(d)}</span><button onclick="ChatPanel._removeDir(${i})">remove</button></div>`
        ).join('');

        const overlay = document.createElement('div');
        overlay.id = 'chatSettingsOverlay';
        overlay.className = 'chat-settings-overlay';
        overlay.innerHTML = `<div class="chat-settings-box">
            <h3>Session Settings</h3>

            <div class="chat-settings-section">
                <h4>Permissions</h4>
                <label><input type="checkbox" id="csAutoReads" ${this._settings.autoApproveReads ? 'checked' : ''}> Auto-approve file reads</label>
                <label><input type="checkbox" id="csAutoWrites" ${this._settings.autoApproveWrites ? 'checked' : ''}> Auto-approve file writes</label>
            </div>

            <div class="chat-settings-section">
                <h4>Allowed Directories</h4>
                <div class="chat-settings-dirs" id="csAllowedDirs">${dirs || '<div style="color:var(--sidebar-muted);font-size:12px">No directories added</div>'}</div>
                <div style="display:flex;gap:6px">
                    <input type="text" id="csNewDir" placeholder="e.g. C:/Projects/my-app" style="flex:1">
                    <button onclick="ChatPanel._addDir()" style="background:var(--accent);color:var(--text-inverse);border:none;padding:6px 12px;border-radius:var(--radius);cursor:pointer;font-size:12px">Add</button>
                </div>
            </div>

            <div class="chat-settings-section">
                <h4>File Limits</h4>
                <label>Max attachment size (KB):
                    <input type="number" id="csMaxSize" value="${this._settings.maxFileSizeKb || 500}" style="width:100px;margin-left:8px">
                </label>
            </div>

            <div class="settings-actions">
                <button class="cancel-btn" onclick="ChatPanel._closeSettings()">Cancel</button>
                <button class="save-btn" onclick="ChatPanel._saveSettings()">Save</button>
            </div>
        </div>`;

        panel.appendChild(overlay);
    },

    _addDir() {
        const input = document.getElementById('csNewDir');
        const dir = input.value.trim();
        if (!dir) return;
        this._settings.allowedDirs = this._settings.allowedDirs || [];
        if (!this._settings.allowedDirs.includes(dir)) {
            this._settings.allowedDirs.push(dir);
        }
        input.value = '';
        const dirsEl = document.getElementById('csAllowedDirs');
        dirsEl.innerHTML = this._settings.allowedDirs.map((d, i) =>
            `<div class="chat-settings-dir"><span>${this._esc(d)}</span><button onclick="ChatPanel._removeDir(${i})">remove</button></div>`
        ).join('');
    },

    _removeDir(idx) {
        this._settings.allowedDirs.splice(idx, 1);
        const dirsEl = document.getElementById('csAllowedDirs');
        if (dirsEl) {
            dirsEl.innerHTML = this._settings.allowedDirs.length
                ? this._settings.allowedDirs.map((d, i) =>
                    `<div class="chat-settings-dir"><span>${this._esc(d)}</span><button onclick="ChatPanel._removeDir(${i})">remove</button></div>`
                ).join('')
                : '<div style="color:var(--sidebar-muted);font-size:12px">No directories added</div>';
        }
    },

    _saveSettings() {
        this._settings.autoApproveReads = document.getElementById('csAutoReads').checked;
        this._settings.autoApproveWrites = document.getElementById('csAutoWrites').checked;
        this._settings.maxFileSizeKb = parseInt(document.getElementById('csMaxSize').value) || 500;
        this._saveSessionSettings();
        this._closeSettings();
    },

    _closeSettings() {
        const overlay = document.getElementById('chatSettingsOverlay');
        if (overlay) overlay.remove();
    },

    // ---- Search ----

    /**
     * Searches the whole transcript, on the server (Q67).
     *
     * Only the newest page of a session is in the DOM, so the previous version — which swept
     * `#chatMessages .chat-msg` — searched that page and presented the result as the count for the
     * conversation. Two messages matching out of five hundred read as "2" whether the other
     * four hundred and ninety-eight had been loaded or not, and there was no way to tell.
     *
     * The matching messages are now fetched and rendered in place of the window, oldest last, with
     * the exact total from the server. Clearing the box restores the newest page.
     */
    _doSearch(query) {
        this._clearSearchHighlights();
        this._searchQuery = query || '';
        if (this._searchDebounce) clearTimeout(this._searchDebounce);
        // Debounced: one request per pause, not one per keystroke.
        this._searchDebounce = setTimeout(() => this._runSearch(this._searchQuery), 200);
    },

    async _runSearch(query) {
        const countEl = document.getElementById('chatSearchCount');
        const list = document.getElementById('chatMessages');
        if (!this._currentSessionId || !list) return;

        if (!query) {
            if (countEl) countEl.textContent = '0';
            const env = await ChatAPI.getMessages(this._currentSessionId,
                { limit: this._msgPageSize });
            if (!env) return;
            const messages = env.items.slice().reverse();
            this._noteWindow(env, messages);
            let html = this._olderBarHtml();
            for (const m of messages) html += this._renderMessage(m);
            list.innerHTML = html;
            this._scrollToBottom();
            return;
        }

        const env = await ChatAPI.getMessages(this._currentSessionId,
            { limit: this._msgPageSize, q: query });
        if (!env) return;
        const messages = env.items.slice().reverse();
        this._noteWindow(env, messages);
        const total = (env.page && env.page.totalIsExact) ? env.page.total : messages.length;
        this._searchTotal = total;
        let html = this._olderBarHtml();
        for (const m of messages) html += this._renderMessage(m);
        if (!messages.length) {
            html += '<div style="text-align:center;color:var(--sidebar-muted);padding:30px 24px;'
                + 'font-size:13px">No messages match this search.</div>';
        }
        list.innerHTML = html;

        this._searchMatches = Array.prototype.slice.call(list.querySelectorAll('.chat-msg'));
        // Shown out of the true total, so a page of matches never looks like all of them.
        if (countEl) {
            countEl.textContent = this._searchMatches.length < total
                ? this._searchMatches.length + '/' + total
                : String(total);
        }
        if (this._searchMatches.length) {
            this._searchIdx = 0;
            this._highlightMatch();
        }
    },

    _searchNext() {
        if (!this._searchMatches.length) return;
        this._searchIdx = (this._searchIdx + 1) % this._searchMatches.length;
        this._highlightMatch();
    },

    _searchPrev() {
        if (!this._searchMatches.length) return;
        this._searchIdx = (this._searchIdx - 1 + this._searchMatches.length) % this._searchMatches.length;
        this._highlightMatch();
    },

    _highlightMatch() {
        this._searchMatches.forEach(el => el.classList.remove('search-highlight'));
        const el = this._searchMatches[this._searchIdx];
        if (el) {
            el.classList.add('search-highlight');
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
        // "3/12 of 480" when the server found more matches than this page carries — the position
        // within what is loaded, and the true size of the result set, are different facts.
        const loaded = this._searchMatches.length;
        const total = this._searchTotal || loaded;
        const el2 = document.getElementById('chatSearchCount');
        if (!el2) return;
        if (!loaded) { el2.textContent = '0'; return; }
        el2.textContent = (this._searchIdx + 1) + '/' + loaded
            + (total > loaded ? ' of ' + total : '');
    },

    _clearSearchHighlights() {
        document.querySelectorAll('.search-highlight').forEach(el => el.classList.remove('search-highlight'));
        this._searchMatches = [];
        this._searchIdx = -1;
        this._searchTotal = 0;
    },

    // ---- Initial Greeting ----

    _buildGreeting() {
        const ctx = this._contextFn() || {};
        const path = window.location.pathname || '/';

        if (path.startsWith('/jar')) {
            const jar = ctx.jarName || '';
            const cls = ctx.totalClasses;
            const ep  = ctx.totalEndpoints;
            if (jar && cls != null && ep != null) {
                return "I'm looking at the JAR Analyzer. Currently analyzing: " + jar + " — " + cls + " classes, " + ep + " endpoints.";
            }
            return 'Hi';
        }

        if (path.startsWith('/parser')) {
            const proc = ctx.procName || ctx.analysisName || '';
            if (proc) {
                return "I'm looking at the PL/SQL Analyzer. Currently analyzing: " + proc;
            }
            return 'Hi';
        }

        return 'Hi';
    },

    async _maybeSendGreeting() {
        if (this._greeted || !this._currentSessionId) return;
        this._greeted = true;

        const greeting = this._buildGreeting();
        const msgEl = document.getElementById('chatMessages');
        if (!msgEl) return;

        // Show typing indicator
        msgEl.innerHTML += '<div class="chat-typing" id="chatTyping"><span class="typing-dots">Claude is thinking</span></div>';
        this._scrollToBottom();

        try {
            const pageContext = this._buildPageContext();
            const res = await ChatAPI.sendMessage(this._currentSessionId, greeting, pageContext);
            const typing = document.getElementById('chatTyping');
            if (typing) typing.remove();

            // Remove the placeholder text if still present
            const placeholder = msgEl.querySelector('div[style*="50px 24px"]');
            if (placeholder) placeholder.remove();

            if (res && !res.error) {
                const content = res.content || '';
                const approvals = this._extractApprovals(content);
                if (approvals.length) {
                    msgEl.innerHTML += this._renderMessage({ role: 'assistant', content: this._stripApprovalBlocks(content) });
                    for (const a of approvals) {
                        msgEl.innerHTML += this._renderApproval(a);
                    }
                } else {
                    msgEl.innerHTML += this._renderMessage({ role: 'assistant', content });
                }
            }
        } catch (e) {
            const typing = document.getElementById('chatTyping');
            if (typing) typing.remove();
            console.warn('[ChatPanel] Greeting failed:', e);
        }
        this._scrollToBottom();
    },

    // ---- Page Context ----

    _buildPageContext() {
        const parts = [];
        parts.push('URL: ' + window.location.href);
        parts.push('Page: ' + document.title);

        const ctx = this._contextFn();
        if (ctx && typeof ctx === 'object') {
            for (const [k, v] of Object.entries(ctx)) {
                if (v) parts.push(k + ': ' + v);
            }
        }

        const hash = window.location.hash || '';
        if (hash) parts.push('Route: ' + hash);

        return parts.join('\n');
    },

    // ---- Rendering helpers ----

    _renderMessage(m) {
        const cls = m.role === 'user' ? 'user' : 'assistant';
        let content = this._esc(m.content || '');
        if (cls === 'assistant') content = this._renderMarkdown(content);
        const ts = m.timestamp ? new Date(m.timestamp).toLocaleTimeString() : '';
        return `<div class="chat-msg ${cls}">${content}${ts ? `<span class="msg-timestamp">${ts}</span>` : ''}</div>`;
    },

    _renderMarkdown(text) {
        return text
            .replace(/```([\s\S]*?)```/g, '<pre><code>$1</code></pre>')
            .replace(/`([^`]+)`/g, '<code>$1</code>')
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/^### (.+)$/gm, '<strong style="font-size:15px;display:block;margin:8px 0 4px">$1</strong>')
            .replace(/^## (.+)$/gm, '<strong style="font-size:16px;display:block;margin:10px 0 4px">$1</strong>')
            .replace(/^# (.+)$/gm, '<strong style="font-size:17px;display:block;margin:12px 0 6px">$1</strong>')
            .replace(/^\d+\. (.+)$/gm, '<div style="padding-left:16px">&#8226; $1</div>')
            .replace(/^- (.+)$/gm, '<div style="padding-left:16px">&#8226; $1</div>')
            .replace(/\n/g, '<br>');
    },

    _onKeyDown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            this.send();
        }
    },

    _autoGrow(el) {
        el.style.height = 'auto';
        el.style.height = Math.min(el.scrollHeight, 160) + 'px';
    },

    _scrollToBottom() {
        const el = document.getElementById('chatMessages');
        if (el) setTimeout(() => { el.scrollTop = el.scrollHeight; }, 60);
    },

    _esc(s) {
        if (!s) return '';
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }
};
