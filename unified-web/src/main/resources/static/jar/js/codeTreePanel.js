/**
 * Code Structure — inline code panel (right side of split view).
 * Shows decompiled/reconstructed code for the selected class.
 * Reuses JA.summary code rendering for syntax highlighting.
 */
window.JA = window.JA || {};

JA.codeTreePanel = {

    _currentFqn: null,
    _currentMethod: null,
    _currentCls: null,
    _currentSource: null,
    _loading: false,

    show(className, methodName) {
        const panel = document.getElementById('code-panel');
        if (!panel) return;

        // Ensure classIdx is built
        if (!JA.summary._classIdx && JA.app.currentAnalysis) {
            JA.summary._classIdx = {};
            const classSource = JA.app.currentAnalysis.classIndex || JA.app.currentAnalysis.classes || [];
            for (const cls of classSource) {
                JA.summary._classIdx[cls.fullyQualifiedName] = cls;
                if (!JA.summary._classIdx[cls.simpleName]) JA.summary._classIdx[cls.simpleName] = cls;
            }
        }
        if (!JA.summary._classIdx) return;

        let cls = JA.summary._classIdx[className];
        if (!cls) {
            for (const k of Object.keys(JA.summary._classIdx)) {
                if (k.endsWith('.' + className) || k === className) {
                    cls = JA.summary._classIdx[k];
                    className = k;
                    break;
                }
            }
        }
        if (!cls) return;

        const fqn = cls.fullyQualifiedName || className;

        // Same class, different method — just re-highlight
        if (this._currentFqn === fqn && this._currentCls && !this._loading) {
            this._currentMethod = methodName;
            this._renderPanel(panel);
            return;
        }

        this._currentFqn = fqn;
        this._currentMethod = methodName;
        this._currentCls = cls;
        this._currentSource = null;
        this._loading = true;

        panel.innerHTML = '<div class="cp-loading"><div class="cm-spinner"></div><div>Decompiling...</div></div>';

        const jarId = JA.app.currentJarId || '';
        fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/decompile?class=' + encodeURIComponent(fqn))
            .then(r => r.ok ? r.json() : null)
            .then(data => {
                if (this._currentFqn !== fqn) return;
                if (data && data.classData) this._currentCls = data.classData;
                this._currentSource = (data && data.source) || null;
                this._loading = false;
                this._renderPanel(panel);
            })
            .catch(() => {
                if (this._currentFqn !== fqn) return;
                this._loading = false;
                this._renderPanel(panel);
            });
    },

    _renderPanel(panel) {
        const cls = this._currentCls;
        const source = this._currentSource;
        const method = this._currentMethod;
        const esc = s => (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

        let codeHtml, sidebarHtml, badge;

        if (source) {
            const result = JA.summary._buildDecompCode(cls, source, method, esc, '#code-panel');
            codeHtml = result.code;
            sidebarHtml = result.sidebar;
            badge = '<span class="cm-src-badge cm-badge-decomp">DECOMPILED</span>';
        } else {
            const result = JA.summary._buildFallbackCode(cls, method, esc, '#code-panel');
            codeHtml = result.code;
            sidebarHtml = result.sidebar;
            badge = '<span class="cm-src-badge cm-badge-recon">RECONSTRUCTED</span>';
        }

        panel.innerHTML = `<div class="cm-info-bar">
            <span class="cm-stereo">${esc(cls.stereotype || '')}</span>
            <span class="cm-classname">${esc(cls.simpleName || '')}</span>
            <span class="cm-fqn">${esc(cls.fullyQualifiedName || '')}</span>
            ${badge}
            <span class="cm-jar">${esc(cls.sourceJar || 'Main App')}</span>
            <button class="btn-sm cm-copy-btn" onclick="JA.codeTreePanel._copyCode()" title="Copy source to clipboard">Copy</button>
        </div>
        <div class="cm-content">
            <div class="cm-code-panel"><table class="cm-code-table">${codeHtml}</table></div>
            <div class="cm-sidebar">${sidebarHtml}</div>
        </div>`;

        // Use double rAF to ensure DOM is fully rendered before scrolling
        requestAnimationFrame(() => requestAnimationFrame(() => {
            const hlAll = panel.querySelectorAll('.cm-hl-method');
            if (hlAll.length) {
                hlAll[0].scrollIntoView({ block: 'center', behavior: 'smooth' });
                hlAll.forEach(r => r.classList.add('cm-hl-flash'));
                setTimeout(() => hlAll.forEach(r => r.classList.remove('cm-hl-flash')), 2000);
            }
            const sbHl = panel.querySelector('.cm-sb-highlight');
            if (sbHl) sbHl.scrollIntoView({ block: 'center', behavior: 'smooth' });
        }));
    },

    _copyCode() {
        const text = this._currentSource || (JA.summary._reconstructJavaFallback ? JA.summary._reconstructJavaFallback(this._currentCls).join('\n') : '');
        navigator.clipboard.writeText(text).then(
            () => JA.toast ? JA.toast.success('Copied to clipboard', 2000) : null,
            () => JA.toast ? JA.toast.warn('Copy failed') : null
        );
    },

    clear() {
        this._currentFqn = null;
        this._currentMethod = null;
        this._currentCls = null;
        this._currentSource = null;
        this._loading = false;
        const panel = document.getElementById('code-panel');
        if (panel) {
            panel.innerHTML = '<div class="cp-placeholder"><p>Select a class or method to view code</p></div>';
        }
    }
};
