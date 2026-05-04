/**
 * Chat Toggle Manager.
 * Manages switching between Classic Chat (shared/chat.js) and New Chatbox (chatbox.js).
 * Reads server config from /api/chat/config to enable/disable each independently.
 * Primary hiding is via CSS body class (chat-mode-new / chat-mode-old).
 */
window.JA = window.JA || {};

JA.chatToggle = (() => {

    const STORAGE_KEY = 'jar-chat-mode';
    let _currentMode = null;
    let _classicEnabled = true;
    let _chatboxEnabled = true;

    async function init() {
        try {
            const resp = await fetch('/api/chat/config');
            if (resp.ok) {
                const cfg = await resp.json();
                _classicEnabled = cfg.classicEnabled !== false;
                _chatboxEnabled = cfg.chatboxEnabled !== false;
            }
        } catch (e) { /* defaults: both enabled */ }

        if (!_classicEnabled && !_chatboxEnabled) {
            document.body.classList.add('chat-mode-disabled');
            return;
        }

        if (_classicEnabled && _chatboxEnabled) {
            _currentMode = localStorage.getItem(STORAGE_KEY) || 'new';
        } else if (_chatboxEnabled) {
            _currentMode = 'new';
        } else {
            _currentMode = 'old';
        }

        _applyMode(_currentMode);

        if (_classicEnabled && _chatboxEnabled) {
            _renderToggle();
        }
    }

    function getMode() { return _currentMode; }

    function setMode(mode) {
        if (mode !== 'old' && mode !== 'new') return;
        if (_currentMode === mode) return;
        if (mode === 'old' && !_classicEnabled) return;
        if (mode === 'new' && !_chatboxEnabled) return;

        _currentMode = mode;
        localStorage.setItem(STORAGE_KEY, mode);
        _applyMode(mode);
        _renderToggle();
    }

    function toggle() {
        setMode(_currentMode === 'old' ? 'new' : 'old');
    }

    function onJarChanged(jarId) {
        if (_currentMode === 'new' && JA.chatbox) {
            JA.chatbox.onJarChanged(jarId);
        }
    }

    function onJarCleared() {
        if (_currentMode === 'new' && JA.chatbox) {
            JA.chatbox.onJarChanged(null);
        }
    }

    function isClassicEnabled() { return _classicEnabled; }
    function isChatboxEnabled() { return _chatboxEnabled; }

    // --- Internal ---

    function _applyMode(mode) {
        document.body.classList.remove('chat-mode-old', 'chat-mode-new', 'chat-mode-disabled');
        document.body.classList.add('chat-mode-' + mode);

        if (mode === 'new') {
            if (typeof ChatPanel !== 'undefined' && ChatPanel._isOpen) {
                try { ChatPanel.close(); } catch (e) {}
            }
            if (JA.chatbox && JA.app && JA.app.currentJarId) {
                JA.chatbox.onJarChanged(JA.app.currentJarId);
            }
        } else {
            if (JA.chatbox) {
                try { JA.chatbox.hide(); } catch (e) {}
            }
        }
    }

    function _renderToggle() {
        let wrap = document.getElementById('chat-toggle-container');
        if (!wrap) {
            wrap = document.createElement('div');
            wrap.id = 'chat-toggle-container';
            wrap.className = 'chat-toggle-wrap';
            const topBar = document.querySelector('.content-top-bar');
            if (topBar) topBar.appendChild(wrap);
        }

        const isNew = _currentMode === 'new';
        wrap.innerHTML =
            '<span class="chat-toggle-label">Chat:</span>' +
            '<span class="chat-toggle-mode">' + (isNew ? 'New' : 'Classic') + '</span>' +
            '<button class="chat-toggle-switch' + (isNew ? ' active' : '') + '" ' +
                'onclick="JA.chatToggle.toggle()" title="Switch between Classic and New chat">' +
            '</button>';
    }

    return { init, getMode, setMode, toggle, onJarChanged, onJarCleared, isClassicEnabled, isChatboxEnabled };

})();
