/**
 * Shared utility functions — colors, formatting, DOM helpers, toast notifications.
 */
window.JA = window.JA || {};

/**
 * The kind of artifact this page analyses — "JAR" here, overridden to "WAR" by
 * war/js/app-overrides.js. The WAR page reuses these JAR modules wholesale, so any
 * user-facing copy that names the artifact must ask for the label instead of hardcoding
 * "JAR", or the WAR screen tells the user to upload the wrong kind of file.
 */
JA._artifactLabel = 'JAR';
JA.artifactLabel = function () { return JA._artifactLabel; };

/**
 * REST base for the per-analysis routes — "/api/jar/jars" here, overridden to "/api/war/wars"
 * by war/js/app-overrides.js. Exactly the same reason as the label above: the WAR page reuses
 * these JAR modules wholesale, and the ones that fetch by hand (rather than going through
 * JA.api, which war/js/api.js replaces) had the JAR prefix baked in. Called with a WAR id those
 * URLs address the wrong key root — WAR artifacts live under jar/war/<id>/ — so decompile,
 * Excel/ZIP export and corrections all failed on the WAR screen.
 *
 * A function, not a constant, because these modules load before the override runs; reading it
 * at call time is what makes the override take effect.
 */
JA._apiBase = '/api/jar/jars';
JA.apiBase = function () { return JA._apiBase; };

JA.utils = {

    stereotypeColor(stereotype) {
        const colors = {
            'REST_CONTROLLER': 'var(--blue)',
            'CONTROLLER': 'var(--blue)',
            'SERVICE': 'var(--green)',
            'REPOSITORY': 'var(--orange)',
            'COMPONENT': 'var(--accent)',
            // CONFIGURATION/LOGGING/JDK/OTHER used to map to var(--bg-inset) — a sunken-BACKGROUND
            // surface token, not a foreground/border/dot color and not safe to pair with a fixed
            // ink. Most call sites use this value directly as a foreground/border/dot color
            // (jar/js/codeTreeViews.js, codeTree.js, nav.js, summary-codeview.js), where --bg-inset
            // is near-invisible against the page background in both themes; one call site
            // (callGraph.js's .ep-op-num) used it as a background against a hardcoded
            // var(--text-inverse) ink, which was equally illegible. --text-muted is this
            // codebase's own convention for "a neutral, always-readable foreground" (see its own
            // doc comment above in mongodb-theme.css) and is what every other stereotype's solid
            // accent color already achieves for its own foreground uses, so reuse it here rather
            // than inventing a new token. For the one background use, route through
            // JA.utils.badgePair() (see callGraph.js:324-328's identical pattern) instead of using
            // this value as a raw background.
            'CONFIGURATION': 'var(--text-muted)',
            'ENTITY': 'var(--teal)',
            'SPRING_DATA': 'var(--orange)',
            'SPRING': 'var(--green)',
            'MONGODB': 'var(--green)',
            'LOGGING': 'var(--text-muted)',
            'REFLECTION': 'var(--pink)',
            'JDBC': 'var(--orange)',
            'HTTP': 'var(--teal)',
            'JDK': 'var(--text-muted)',
            'LIBRARY': 'var(--accent)',
            'OTHER': 'var(--text-muted)'
        };
        return colors[stereotype] || colors.OTHER;
    },

    /**
     * Map any solid status token ("var(--blue)", "var(--accent-hover)", ...) to
     * its accessible badge pair: a pale tint for the fill and a saturated ink
     * for the text, in whichever theme is active.
     *
     * Several call sites used to build the tint by string-concatenating an
     * alpha suffix onto the token — `background:${color}20` produced
     * "var(--blue)20", which is not a valid CSS colour, so the whole
     * declaration was dropped and the chip rendered with no background at all.
     * Use this instead; never concatenate onto a var() string.
     */
    badgePair(token) {
        const FAMS = ['blue', 'green', 'red', 'orange', 'yellow', 'purple', 'teal'];
        const m = /--([a-z-]+)/.exec(token || '');
        let fam = m ? m[1] : '';
        if (fam.indexOf('accent') === 0) fam = 'green';
        if (fam === 'pink') fam = 'purple';
        if (FAMS.indexOf(fam) < 0) fam = 'gray';
        return { bg: 'var(--badge-' + fam + '-bg)', ink: 'var(--badge-' + fam + ')' };
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

    /**
     * The stable identity of an endpoint — the fix for the positional-coupling hazard.
     *
     * PLAN-03 §1.5 #4: the Summary tab fetches the endpoint list from one request and each heavy
     * per-endpoint slice (externalCalls, dynamicFlows, aggregationFlows, beans) from another, then
     * merged the second into the first *by array index*. That is right only while two independent
     * passes over the analysis enumerate endpoints in identical order — which nothing enforces,
     * nothing tests, and which fails silently: the external calls of one endpoint render under
     * another and the screen still looks plausible. A Claude correction rewrites the analysis
     * between the two fetches (the slice is lazy — it arrives on tab switch, possibly minutes
     * later), and paging the list breaks it outright, because `endpointIdx` counts the whole
     * analysis while the array would hold one page.
     *
     * Prefers the server's `endpointId` and falls back to computing it from the same four fields,
     * so a response that predates the field still joins correctly. Never an index.
     */
    endpointKey(ep) {
        if (!ep) return '';
        if (ep.endpointId) return ep.endpointId;
        return (ep.httpMethod || '') + ' ' + (ep.fullPath || '') + ' '
             + (ep.controllerClass || '') + '#' + (ep.methodName || '');
    },

    formatSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    },

    /**
     * Delegated to shared/escape.js.
     *
     * The textContent/innerHTML trick it used escapes `& < >` but leaves quotes intact, so it was
     * unsafe in the attribute contexts it is used in. It also required a DOM node per call.
     */
    escapeHtml(str) {
        return Esc.html(str);
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
     * @param {string} message   - Text to display
     * @param {string} type      - 'info' | 'success' | 'warn' | 'error' | 'loading'
     * @param {number} duration  - ms before auto-dismiss (0 = persistent)
     * @param {object} opts      - { action: { label, fn }, detail: 'full text for expand modal' }
     * @returns {HTMLElement} The toast element
     */
    show(message, type, duration, opts) {
        type = type || 'info';
        duration = duration !== undefined ? duration : (type === 'loading' ? 0 : 3000);
        opts = opts || {};

        const container = this._getContainer();
        const toast = document.createElement('div');
        toast.className = 'toast toast-' + type + (opts.action ? ' toast-has-action' : '');

        const icons = { info: '\u2139\uFE0F', success: '\u2705', warn: '\u26A0\uFE0F', error: '\u274C', loading: '' };

        let inner = '';
        if (type === 'loading') {
            inner = '<span class="toast-spinner"></span>';
        } else {
            inner = '<span class="toast-icon">' + (icons[type] || '') + '</span>';
        }
        inner += '<span class="toast-msg">' + message + '</span>';

        // Action button (e.g. "View Flow")
        if (opts.action) {
            inner += '<button class="toast-action-btn" title="' + (opts.action.label || 'Open') + '">' + (opts.action.label || 'Open') + '</button>';
        }

        // Expand button \u2014 shows full message in a modal
        inner += '<button class="toast-expand-btn" title="Expand" onclick="JA.toast._expand(this.closest(\'.toast\'))">\u26F6</button>';

        // Close button
        inner += '<button class="toast-close" title="Close" onclick="JA.toast.dismiss(this.closest(\'.toast\'))">&times;</button>';

        toast.innerHTML = inner;

        // Wire action click
        if (opts.action && typeof opts.action.fn === 'function') {
            const actionBtn = toast.querySelector('.toast-action-btn');
            if (actionBtn) actionBtn.addEventListener('click', (e) => { e.stopPropagation(); opts.action.fn(); JA.toast.dismiss(toast); });
        }

        // Store full detail for expand modal
        toast._detail = opts.detail || message;

        container.appendChild(toast);
        requestAnimationFrame(() => toast.classList.add('toast-visible'));

        if (duration > 0) {
            setTimeout(() => this.dismiss(toast), duration);
        }

        return toast;
    },

    // Open expand modal showing full toast content
    _expand(toast) {
        const old = document.getElementById('toast-expand-modal');
        if (old) old.remove();
        const msg = toast._detail || toast.querySelector('.toast-msg')?.textContent || '';
        const html = `<div class="toast-expand-overlay" id="toast-expand-modal" onclick="if(event.target===this)this.remove()">
            <div class="toast-expand-panel">
                <div class="toast-expand-header">
                    <span class="toast-expand-title">Notification Detail</span>
                    <button class="toast-close" onclick="document.getElementById('toast-expand-modal').remove()">&times;</button>
                </div>
                <div class="toast-expand-body">${msg}</div>
            </div>
        </div>`;
        JA.mountOverlay(html);
    },

    info(msg, dur, opts)    { return this.show(msg, 'info', dur, opts); },
    success(msg, dur, opts) { return this.show(msg, 'success', dur, opts); },
    warn(msg, dur, opts)    { return this.show(msg, 'warn', dur, opts); },
    error(msg, dur, opts)   { return this.show(msg, 'error', dur, opts); },
    loading(msg, opts)      { return this.show(msg, 'loading', 0, opts); },

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

    /** label -> the id of the row currently showing it, so repeats share one row. */
    _byLabel: new Map(),

    start(label) {
        // Collapse concurrent identical calls into ONE row, with a count.
        //
        // This created a new row per call, unconditionally. JA.api's wrapper invokes the tracker
        // BEFORE the underlying function, so getCallTree's in-flight dedupe — which correctly
        // returns the same promise and issues no second request — could not prevent a second row.
        // Six clicks on a slow endpoint produced six "Loading Call Tree" rows ticking 295s, 265s,
        // 265s… all finishing together: one request, six observers, and a UI implying six requests.
        const existing = this._byLabel.get(label);
        if (existing != null && this._calls.has(existing)) {
            const call = this._calls.get(existing);
            call.waiters = (call.waiters || 1) + 1;
            const c = call.el.querySelector('.api-call-count');
            if (c) c.textContent = '×' + call.waiters;
            else {
                const span = document.createElement('span');
                span.className = 'api-call-count';
                span.textContent = '×' + call.waiters;
                call.el.appendChild(span);
            }
            return existing;
        }

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
        const timer = setInterval(() => {   // poll-exempt: ticks a seconds counter already on screen
            const sec = Math.round((Date.now() - startTime) / 1000);
            const timeEl = el.querySelector('.api-call-time');
            if (timeEl) timeEl.textContent = sec + 's';
        }, 1000);

        this._calls.set(id, { el, timer, startTime, label, waiters: 1 });
        this._byLabel.set(label, id);
        return id;
    },

    done(id, error) {
        const call = this._calls.get(id);
        if (!call) return;
        // Several callers may share this row. Only the last one out clears it, or the row would
        // vanish while other observers are still waiting on the same request.
        if (call.waiters && call.waiters > 1) {
            call.waiters--;
            const c = call.el.querySelector('.api-call-count');
            if (c) c.textContent = call.waiters > 1 ? '×' + call.waiters : '';
            return;
        }
        if (call.label) this._byLabel.delete(call.label);
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

/**
 * The TRUE size of a per-endpoint array, not the size of what was sent.
 *
 * The server caps large per-endpoint arrays in the summary (externalCalls/beans at 100,
 * httpCalls at 50, the flows at 200) because an uncapped summary measured 650 MB. It now also
 * emits `<field>Total` whenever it truncated. Reading `.length` therefore reports the CAP:
 * 23 endpoints showed 100 external calls where the analysis found 224, and the summary's total
 * came to 3,104 against a real 5,958.
 *
 * That matters beyond a label — externalScopeCalls feeds a sortable column and the Excel export,
 * so a capped count silently reorders the table and corrupts the exported figure.
 *
 * @param owner  the object holding the array (an endpoint report)
 * @param field  the array's field name
 * @returns the server's total when the array was truncated, else the array's own length
 */
JA.utils.trueCount = function (owner, field) {
    if (!owner) return 0;
    const total = owner[field + 'Total'];
    if (typeof total === 'number') return total;
    const arr = owner[field];
    return Array.isArray(arr) ? arr.length : 0;
};

/**
 * The fields the server caps, and the cap it applies — mirrors
 * `AbstractAnalysisDataProvider.CAPPED_ARRAY_FIELDS`.
 *
 * Kept here so a report builder can copy the matching `<field>Total` forward. `trueCount` is
 * useless on an object that never received the total, and that is precisely how the original fix
 * came to be a no-op: `summary-analyzer.js` rebuilt each endpoint into a fresh report object and
 * carried the arrays but not their totals, so `trueCount` fell through to `.length` — the cap —
 * on every call. The lookup has to survive the copy, not just exist.
 */
JA.utils.CAPPED_FIELDS = [
    'beans', 'externalCalls', 'httpCalls', 'dynamicFlows',
    'aggregationFlows', 'dynamicCollectionFlows', 'collectionLifecycleFlows'
];

/**
 * Copies every `<field>Total` the server emitted from `src` to `dst`.
 *
 * Only present-and-numeric totals are copied, so an untruncated response adds no fields and
 * `trueCount` keeps falling back to `.length` exactly as before.
 */
JA.utils.carryTotals = function (dst, src) {
    if (!dst || !src) return dst;
    for (const f of JA.utils.CAPPED_FIELDS) {
        const t = src[f + 'Total'];
        if (typeof t === 'number') dst[f + 'Total'] = t;
    }
    return dst;
};

/**
 * The house disclosure for a capped list: `— showing 100 of 225`.
 *
 * Returns '' when nothing was truncated, so it can be appended unconditionally. A cap the reader
 * cannot see is the same defect as the silent server-side truncation it comes from: the detail
 * panels headed "Cross-Module Dependencies (225)" listed 100 rows and said nothing.
 */
JA.utils.capNote = function (shown, total) {
    if (!(total > shown)) return '';
    return ' <span class="cap-note" title="The server caps this list to keep the summary '
        + 'response small. The count is the true total.">— showing '
        + shown + ' of ' + total + '</span>';
};
