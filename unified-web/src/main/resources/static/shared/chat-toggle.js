/**
 * Shared Chat Toggle — manages switching between Classic (ChatPanel) and New (Chatbox).
 * Reads config from /api/chat/config. Works on all pages (Home, PL/SQL, JAR).
 */
const SharedChatToggle = (() => {

    const STORAGE_KEY = 'chat-mode';
    let _currentMode = null;
    let _classicEnabled = true;
    let _chatboxEnabled = true;
    let _scope = 'GLOBAL';
    let _contextFn = () => ({});

    async function init(scope, contextFn) {
        _scope = scope || 'GLOBAL';
        _contextFn = contextFn || (() => ({}));

        try {
            const resp = await fetch('/api/chat/config');
            if (resp.ok) {
                const cfg = await resp.json();
                _classicEnabled = cfg.classicEnabled !== false;
                _chatboxEnabled = cfg.chatboxEnabled !== false;
            }
        } catch (e) {}

        if (!_classicEnabled && !_chatboxEnabled) {
            document.body.classList.add('chat-mode-disabled');
            return;
        }

        if (_chatboxEnabled) {
            Chatbox.init(_scope, _contextFn);
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

    function _applyMode(mode) {
        document.body.classList.remove('chat-mode-old', 'chat-mode-new', 'chat-mode-disabled');
        document.body.classList.add('chat-mode-' + mode);

        if (mode === 'new') {
            if (typeof ChatPanel !== 'undefined' && ChatPanel._isOpen) {
                try { ChatPanel.close(); } catch (e) {}
            }
        } else {
            if (typeof Chatbox !== 'undefined') {
                try { Chatbox.hide(); } catch (e) {}
            }
        }
    }

    function _renderToggle() {
        let wrap = document.getElementById('chat-toggle-container');
        if (!wrap) {
            wrap = document.createElement('div');
            wrap.id = 'chat-toggle-container';
            wrap.className = 'chat-toggle-wrap';
            const topBar = document.querySelector('.content-top-bar') || document.querySelector('.topbar-actions') || document.querySelector('.top-bar') || document.querySelector('header') || document.querySelector('nav');
            if (topBar) {
                topBar.appendChild(wrap);
            } else {
                wrap.style.position = 'fixed';
                wrap.style.top = '10px';
                wrap.style.right = '10px';
                wrap.style.zIndex = '1200';
                document.body.appendChild(wrap);
            }
        }

        const isNew = _currentMode === 'new';
        wrap.innerHTML =
            '<span class="chat-toggle-label">Chat:</span>' +
            '<span class="chat-toggle-mode">' + (isNew ? 'New' : 'Classic') + '</span>' +
            '<button class="chat-toggle-switch' + (isNew ? ' active' : '') + '" ' +
                'onclick="SharedChatToggle.toggle()" title="Switch between Classic and New chat">' +
            '</button>';
    }

    return { init, getMode, setMode, toggle };

})();
