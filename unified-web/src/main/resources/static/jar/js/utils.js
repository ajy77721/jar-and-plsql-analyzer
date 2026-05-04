/**
 * Shared utility functions — colors, formatting, DOM helpers, toast notifications.
 */
window.JA = window.JA || {};

JA.utils = {

    stereotypeColor(stereotype) {
        const colors = {
            'REST_CONTROLLER': '#3b82f6',
            'CONTROLLER': '#3b82f6',
            'SERVICE': '#22c55e',
            'REPOSITORY': '#f59e0b',
            'COMPONENT': '#a855f7',
            'CONFIGURATION': '#64748b',
            'ENTITY': '#14b8a6',
            'SPRING_DATA': '#f59e0b',
            'SPRING': '#22c55e',
            'MONGODB': '#10b981',
            'LOGGING': '#6b7280',
            'REFLECTION': '#ec4899',
            'JDBC': '#f97316',
            'HTTP': '#06b6d4',
            'JDK': '#94a3b8',
            'LIBRARY': '#8b5cf6',
            'OTHER': '#94a3b8'
        };
        return colors[stereotype] || colors.OTHER;
    },

    stereotypeBadgeClass(stereotype) {
        const map = {
            'REST_CONTROLLER': 'badge-controller',
            'CONTROLLER': 'badge-controller',
            'SERVICE': 'badge-service',
            'REPOSITORY': 'badge-repository',
            'COMPONENT': 'badge-component',
            'CONFIGURATION': 'badge-config',
            'ENTITY': 'badge-entity',
            'SPRING_DATA': 'badge-repository',
            'SPRING': 'badge-service',
            'MONGODB': 'badge-mongodb',
            'LOGGING': 'badge-logging',
            'REFLECTION': 'badge-reflection',
            'JDBC': 'badge-jdbc',
            'HTTP': 'badge-http',
            'JDK': 'badge-jdk',
            'LIBRARY': 'badge-library',
            'OTHER': 'badge-other'
        };
        return map[stereotype] || 'badge-other';
    },

    formatSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    },

    escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    truncate(str, max) {
        if (!str || str.length <= max) return str;
        return str.substring(0, max - 2) + '..';
    },

    /**
     * Styled confirmation dialog — replaces native confirm().
     * @param {object} opts
     * @param {string} opts.title - Dialog title
     * @param {string} opts.message - HTML body
     * @param {string} [opts.confirmLabel='Confirm'] - Confirm button text
     * @param {string} [opts.cancelLabel='Cancel']
     * @param {string} [opts.confirmClass='confirm-btn-danger'] - CSS class for confirm button
     * @returns {Promise<boolean>} true if confirmed
     */
    confirm({ title, message, confirmLabel, cancelLabel, confirmClass }) {
        return new Promise(resolve => {
            const overlay = document.createElement('div');
            overlay.className = 'confirm-overlay';
            overlay.innerHTML = `
                <div class="confirm-dialog">
                    <h3 class="confirm-title">${title || 'Confirm'}</h3>
                    <div class="confirm-body">${message || 'Are you sure?'}</div>
                    <div class="confirm-actions">
                        <button class="confirm-btn ${confirmClass || 'confirm-btn-danger'}" data-choice="yes">
                            ${confirmLabel || 'Confirm'}
                        </button>
                        <button class="confirm-btn confirm-btn-cancel" data-choice="no">
                            ${cancelLabel || 'Cancel'}
                        </button>
                    </div>
                </div>`;
            document.body.appendChild(overlay);

            const close = (result) => { overlay.remove(); resolve(result); };

            overlay.addEventListener('click', e => {
                const btn = e.target.closest('[data-choice]');
                if (btn) close(btn.dataset.choice === 'yes');
                else if (e.target === overlay) close(false);
            });
            overlay.addEventListener('keydown', e => {
                if (e.key === 'Escape') close(false);
            });
            // Focus the cancel button so Escape works
            overlay.querySelector('.confirm-btn-cancel').focus();
        });
    }
};

/* ========== Toast notification system ========== */

JA.toast = {

    _container: null,

    _getContainer() {
        if (!this._container) {
            this._container = document.createElement('div');
            this._container.id = 'toast-container';
            this._container.className = 'toast-container';
            document.body.appendChild(this._container);
        }
        return this._container;
    },

    /**
     * Show a toast notification.
     * @param {string} message - Text to display
     * @param {string} type - 'info' | 'success' | 'warn' | 'error' | 'loading'
     * @param {number} duration - ms before auto-dismiss (0 = persistent, must dismiss manually)
     * @returns {HTMLElement} The toast element (for manual removal)
     */
    show(message, type, duration) {
        type = type || 'info';
        duration = duration !== undefined ? duration : (type === 'loading' ? 0 : 3000);

        const container = this._getContainer();
        const toast = document.createElement('div');
        toast.className = 'toast toast-' + type;

        const icons = {
            info: '\u2139\uFE0F',
            success: '\u2705',
            warn: '\u26A0\uFE0F',
            error: '\u274C',
            loading: ''
        };

        let inner = '';
        if (type === 'loading') {
            inner = '<span class="toast-spinner"></span>';
        } else {
            inner = '<span class="toast-icon">' + (icons[type] || '') + '</span>';
        }
        inner += '<span class="toast-msg">' + message + '</span>';
        inner += '<button class="toast-close" onclick="this.parentElement.remove()">&times;</button>';

        toast.innerHTML = inner;
        container.appendChild(toast);

        // Animate in
        requestAnimationFrame(() => toast.classList.add('toast-visible'));

        if (duration > 0) {
            setTimeout(() => {
                toast.classList.remove('toast-visible');
                setTimeout(() => toast.remove(), 300);
            }, duration);
        }

        return toast;
    },

    info(msg, dur)    { return this.show(msg, 'info', dur); },
    success(msg, dur) { return this.show(msg, 'success', dur); },
    warn(msg, dur)    { return this.show(msg, 'warn', dur); },
    error(msg, dur)   { return this.show(msg, 'error', dur); },
    loading(msg)      { return this.show(msg, 'loading', 0); },

    dismiss(toastEl) {
        if (!toastEl) return;
        toastEl.classList.remove('toast-visible');
        setTimeout(() => toastEl.remove(), 300);
    }
};

/* ========== API Activity Tracker ========== */
JA.apiTracker = {

    _container: null,
    _calls: new Map(),
    _counter: 0,

    _getContainer() {
        if (!this._container) {
            const c = document.createElement('div');
            c.id = 'api-tracker';
            c.className = 'api-tracker';
            document.body.appendChild(c);
            this._container = c;
        }
        return this._container;
    },

    start(label) {
        const id = ++this._counter;
        const container = this._getContainer();
        const el = document.createElement('div');
        el.className = 'api-call loading';
        el.dataset.callId = id;
        el.innerHTML = '<span class="api-call-spinner"></span>'
            + '<span class="api-call-label">' + JA.utils.escapeHtml(label) + '</span>'
            + '<span class="api-call-time">0s</span>';
        container.appendChild(el);
        requestAnimationFrame(() => el.classList.add('visible'));

        const startTime = Date.now();
        const timer = setInterval(() => {
            const sec = Math.round((Date.now() - startTime) / 1000);
            const timeEl = el.querySelector('.api-call-time');
            if (timeEl) timeEl.textContent = sec + 's';
        }, 1000);

        this._calls.set(id, { el, timer, startTime });
        return id;
    },

    done(id, error) {
        const call = this._calls.get(id);
        if (!call) return;
        clearInterval(call.timer);

        const elapsed = Math.round((Date.now() - call.startTime) / 1000);
        const timeEl = call.el.querySelector('.api-call-time');
        if (timeEl) timeEl.textContent = elapsed + 's';

        call.el.classList.remove('loading');
        if (error) {
            call.el.classList.add('error');
            call.el.querySelector('.api-call-spinner').outerHTML = '<span class="api-call-icon">&#10007;</span>';
        } else {
            call.el.classList.add('success');
            call.el.querySelector('.api-call-spinner').outerHTML = '<span class="api-call-icon">&#10003;</span>';
        }

        setTimeout(() => {
            call.el.classList.remove('visible');
            setTimeout(() => { call.el.remove(); this._calls.delete(id); }, 300);
        }, error ? 3000 : 1500);
    },

    async track(label, promiseFn) {
        const id = this.start(label);
        try {
            const result = await promiseFn();
            this.done(id);
            return result;
        } catch (e) {
            this.done(id, true);
            throw e;
        }
    }
};
