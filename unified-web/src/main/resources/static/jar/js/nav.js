/**
 * Navigation module — IntelliJ-style click-to-navigate.
 * Resolves interfaces: if target is an interface with multiple implementations,
 * shows a picker popup to choose which one.
 * Works in both Endpoint Flow and Code Structure tabs.
 */
window.JA = window.JA || {};

JA.nav = {

    _implMap: null,
    _classMap: null,
    _targets: [],
    _outsideHandler: null,

    /** Build lookup maps from current analysis. Lazy — called on first navigation. */
    init() {
        this._implMap = {};
        this._classMap = {};
        this._targets = [];
        const analysis = JA.app.currentAnalysis;
        if (!analysis) return;
        const classSource = analysis.classIndex || analysis.classes || [];

        for (const cls of classSource) {
            this._classMap[cls.fullyQualifiedName] = cls;
            if (!this._classMap[cls.simpleName]) {
                this._classMap[cls.simpleName] = cls;
            }
            for (const iface of (cls.interfaces || [])) {
                (this._implMap[iface] = this._implMap[iface] || []).push(cls);
            }
            if (cls.superClass && cls.superClass !== 'java.lang.Object') {
                (this._implMap[cls.superClass] = this._implMap[cls.superClass] || []).push(cls);
            }
        }
    },

    /** Store a navigation target, return its index. Used by renderers. */
    ref(className, methodName) {
        const idx = this._targets.length;
        this._targets.push({ className: className || '', methodName: methodName || '' });
        return idx;
    },

    /** Inline onclick handler — stops propagation, then navigates. */
    click(idx, event) {
        event.stopPropagation();
        const t = this._targets[idx];
        if (t) this.goTo(t.className, t.methodName, 'structure', event);
    },

    /**
     * Navigate to a class/method. If target is interface/abstract with
     * multiple implementations, shows a picker popup.
     */
    goTo(className, methodName, targetTab, event) {
        if (!this._classMap) this.init();

        const cls = this._classMap[className] || this._classMap[(className || '').split('.').pop()];

        if (cls && (cls.isInterface || cls.isAbstract)) {
            const impls = this._implMap[cls.fullyQualifiedName] || [];
            if (impls.length === 0) {
                this._navigate(cls.simpleName, methodName, targetTab);
            } else if (impls.length === 1) {
                this._navigate(impls[0].simpleName, methodName, targetTab);
            } else {
                this._showPicker(cls, methodName, impls, targetTab, event);
            }
        } else {
            const simple = cls ? cls.simpleName : (className || '').split('.').pop();
            this._navigate(simple, methodName, targetTab);
        }
    },

    _navigate(simpleName, methodName, targetTab) {
        this.hidePicker();
        switchTab(targetTab || 'structure');
        JA.codeTree.findAndHighlight(simpleName, methodName || '');
        JA.toast.info(simpleName + (methodName ? '.' + methodName : ''), 1500);
    },

    _showPicker(interfaceCls, methodName, impls, targetTab, event) {
        this.hidePicker();
        const esc = JA.utils.escapeHtml;

        const picker = document.createElement('div');
        picker.id = 'impl-picker';
        picker.className = 'impl-picker';

        let html = '<div class="impl-picker-header">';
        html += '<span class="impl-picker-icon">&#9671;</span>';
        html += `<span class="impl-picker-title">${esc(interfaceCls.simpleName)}</span>`;
        html += `<span class="impl-picker-badge">${interfaceCls.isInterface ? 'Interface' : 'Abstract'}</span>`;
        html += '</div>';
        html += '<div class="impl-picker-subtitle">Choose Implementation</div>';

        for (const impl of impls) {
            const color = JA.utils.stereotypeColor(impl.stereotype);
            const stereo = (impl.stereotype || 'OTHER').replace('REST_', '');
            html += `<div class="impl-picker-item" data-pick-class="${esc(impl.simpleName)}" data-pick-method="${esc(methodName || '')}">`;
            html += `<span class="impl-picker-dot" style="background:${color}"></span>`;
            html += `<span class="impl-picker-name">${esc(impl.simpleName)}</span>`;
            html += `<span class="impl-picker-stereo">${esc(stereo)}</span>`;
            html += '</div>';
        }

        // Option to go to the interface itself
        html += '<div class="impl-picker-divider"></div>';
        html += `<div class="impl-picker-item impl-picker-iface" data-pick-class="${esc(interfaceCls.simpleName)}" data-pick-method="${esc(methodName || '')}">`;
        html += '<span class="impl-picker-dot" style="background:#ec4899"></span>';
        html += `<span class="impl-picker-name">${esc(interfaceCls.simpleName)}</span>`;
        html += '<span class="impl-picker-stereo">INTERFACE</span>';
        html += '</div>';

        picker.innerHTML = html;
        document.body.appendChild(picker);

        // Position near click
        if (event) {
            const x = Math.min(event.clientX + 4, window.innerWidth - 280);
            const y = Math.min(event.clientY + 4, window.innerHeight - picker.offsetHeight - 10);
            picker.style.left = x + 'px';
            picker.style.top = y + 'px';
        }

        // Click on implementation item
        picker.addEventListener('click', (e) => {
            const item = e.target.closest('.impl-picker-item');
            if (item) {
                this._navigate(item.dataset.pickClass, item.dataset.pickMethod, targetTab);
            }
        });

        // Click outside to dismiss
        setTimeout(() => {
            this._outsideHandler = (e) => {
                if (!picker.contains(e.target)) this.hidePicker();
            };
            document.addEventListener('click', this._outsideHandler);
        }, 10);
    },

    hidePicker() {
        const el = document.getElementById('impl-picker');
        if (el) el.remove();
        if (this._outsideHandler) {
            document.removeEventListener('click', this._outsideHandler);
            this._outsideHandler = null;
        }
    }
};
