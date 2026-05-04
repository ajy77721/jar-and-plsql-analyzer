/**
 * Summary codeview — in-app code modal (decompiled CFR + fallback reconstructed).
 * Replaces window.open with an overlay modal with breadcrumb navigation.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /* --- Modal state --- */
    _codeStack: [],   // [{cls, source, highlightMethod, fqn}]
    _codeIdx: -1,
    _searchState: { query: '', matches: [], current: -1, caseSensitive: false },

    showClassCode(className, highlightMethod, skipInterfaceRedirect) {
        // Route to inline panel when Code Structure tab is active
        const structTab = document.getElementById('tab-structure');
        if (structTab && structTab.style.display !== 'none' && JA.codeTreePanel) {
            JA.codeTreePanel.show(className, highlightMethod);
            return;
        }

        // Lazy-build classIdx if Summary tab hasn't rendered yet
        if (!this._classIdx && JA.app.currentAnalysis) {
            this._classIdx = {};
            const classSource = JA.app.currentAnalysis.classIndex || JA.app.currentAnalysis.classes || [];
            for (const cls of classSource) {
                this._classIdx[cls.fullyQualifiedName] = cls;
                if (!this._classIdx[cls.simpleName]) this._classIdx[cls.simpleName] = cls;
            }
        }
        if (!this._classIdx) return;
        let cls = this._classIdx[className];
        if (!cls) {
            for (const k of Object.keys(this._classIdx)) {
                if (k.endsWith('.' + className) || k === className) {
                    cls = this._classIdx[k];
                    className = k;
                    break;
                }
            }
            if (!cls) {
                if (JA.toast) JA.toast.warn('Class not in this JAR: ' + className.split('.').pop(), 3000);
                else console.warn('Class not found in analysis:', className);
                return;
            }
        }

        const fqn = cls.fullyQualifiedName || className;

        // Interface/abstract redirect — offer implementation picker unless caller opted out
        if (!skipInterfaceRedirect && (cls.isInterface || cls.isAbstract)) {
            if (JA.nav && (!JA.nav._implMap || (!Object.keys(JA.nav._implMap).length && JA.app.currentAnalysis))) JA.nav.init();
            const impls = (JA.nav && JA.nav._implMap)
                ? (JA.nav._implMap[cls.fullyQualifiedName] || JA.nav._implMap[cls.simpleName] || [])
                : [];
            if (impls.length === 1) {
                this.showClassCode(impls[0].fullyQualifiedName || impls[0].simpleName, highlightMethod);
                return;
            } else if (impls.length > 1) {
                // Show the impl picker popup AND open the interface modal (sidebar lists implementations)
                // so clicking outside the picker still shows the interface code + implementations section.
                this._showImplPickerForCode(cls, impls, highlightMethod);
                // fall through intentionally — open the interface code modal below
            }
            // 0 known implementations — fall through and show the interface itself
        }

        // Skip duplicate: if current entry is the same class+method, don't push
        const cur = this._codeStack[this._codeIdx];
        if (cur && cur.fqn === fqn && cur.highlightMethod === highlightMethod) return;

        // Same class, different method → replace highlight and re-render
        if (cur && cur.fqn === fqn && !cur.loading) {
            cur.highlightMethod = highlightMethod;
            this._renderCodeContent();
            return;
        }

        // Push to navigation stack (truncate forward history)
        this._codeIdx++;
        this._codeStack.length = this._codeIdx;
        this._codeStack.push({ cls, source: null, highlightMethod, fqn, loading: true });

        this._showCodeModal();
        this._renderCodeLoading();

        const jarId = JA.app.currentJarId || '';
        fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/decompile?class=' + encodeURIComponent(fqn))
            .then(r => r.ok ? r.json() : null)
            .then(data => {
                const entry = this._codeStack[this._codeIdx];
                if (!entry || entry.fqn !== fqn) return; // navigated away
                if (data && data.classData) entry.cls = data.classData;
                entry.source = (data && data.source) || null;
                entry.loading = false;
                this._renderCodeContent();
            })
            .catch(() => {
                const entry = this._codeStack[this._codeIdx];
                if (!entry || entry.fqn !== fqn) return;
                entry.loading = false;
                this._renderCodeContent();
            });
    },

    _showCodeModal() {
        let overlay = document.getElementById('code-modal-overlay');
        if (overlay) return; // already open
        const html = `<div class="code-modal-overlay" id="code-modal-overlay">
            <div class="code-modal-panel">
                <div class="code-modal-header" id="code-modal-header"></div>
                <div class="cm-search-bar" id="cm-search-bar" style="display:none">
                    <input type="text" class="cm-search-input" id="cm-search-input" placeholder="Find in code..."
                        oninput="JA.summary._codeSearch(this.value)"
                        onkeydown="if(event.key==='Enter'){event.shiftKey?JA.summary._codeSearchPrev():JA.summary._codeSearchNext();event.preventDefault()}if(event.key==='Escape'){JA.summary._codeSearchClose()}">
                    <span class="cm-search-count" id="cm-search-count"></span>
                    <button class="cm-search-btn" onclick="JA.summary._codeSearchPrev()" title="Previous (Shift+Enter)">&#9650;</button>
                    <button class="cm-search-btn" onclick="JA.summary._codeSearchNext()" title="Next (Enter)">&#9660;</button>
                    <label class="cm-search-case" title="Case Sensitive">
                        <input type="checkbox" id="cm-search-case" onchange="JA.summary._codeSearchToggleCase()"> Aa
                    </label>
                    <button class="cm-search-btn cm-search-close" onclick="JA.summary._codeSearchClose()" title="Close (Esc)">&#10005;</button>
                </div>
                <div class="code-modal-body" id="code-modal-body"></div>
            </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', html);
        document.getElementById('code-modal-overlay').addEventListener('click', (e) => {
            if (e.target.id === 'code-modal-overlay') JA.summary._closeCodeModal();
        });
        // Ctrl+F / Cmd+F to open search
        document.getElementById('code-modal-overlay').addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
                e.preventDefault();
                JA.summary._codeSearchOpen();
            }
        });
        // Make panel focusable for keyboard events
        document.querySelector('.code-modal-panel').setAttribute('tabindex', '0');
        document.querySelector('.code-modal-panel').focus();
    },

    _closeCodeModal() {
        const el = document.getElementById('code-modal-overlay');
        if (el) el.remove();
        const p = document.getElementById('ambi-call-picker');
        if (p) p.remove();
        this._codeStack = [];
        this._codeIdx = -1;
        this._ambiPickers = [];
    },

    _showAmbiguousCallPicker(pickerIdx, event) {
        event.stopPropagation();
        const existing = document.getElementById('ambi-call-picker');
        if (existing) { existing.remove(); return; } // second click toggles off

        const info = this._ambiPickers && this._ambiPickers[pickerIdx];
        if (!info) return;
        const esc = JA.utils.escapeHtml;

        const picker = document.createElement('div');
        picker.id = 'ambi-call-picker';
        picker.className = 'impl-picker ambi-call-picker';

        let html = '<div class="impl-picker-header">';
        html += '<span class="impl-picker-icon">&#9671;</span>';
        html += `<span class="impl-picker-title">${esc(info.iface.simpleName || '')}</span>`;
        html += `<span class="impl-picker-badge">${info.iface.isInterface ? 'Interface' : 'Abstract'}</span>`;
        html += '</div>';
        html += `<div class="impl-picker-subtitle">⚠ ${info.impls.length} possible implementation${info.impls.length !== 1 ? 's' : ''} — choose to view code</div>`;

        for (const impl of info.impls) {
            const color = JA.utils.stereotypeColor(impl.stereotype);
            const stereo = (impl.stereotype || 'CLASS').replace('REST_', '');
            const safeFqn = (impl.fullyQualifiedName || impl.simpleName || '').replace(/'/g, "\\'");
            const safeMethod = (info.inv.methodName || '').replace(/'/g, "\\'");

            // Find the field that injects this specific impl (by qualifier annotation match or type match)
            const field = info.fields.find(f => {
                const qAnn = (f.annotations || []).find(a => a.name === 'Qualifier' || a.name === 'AnalysisType');
                if (qAnn && qAnn.attributes) {
                    const vals = Object.values(qAnn.attributes).map(v => String(v).toLowerCase());
                    const implName = (impl.simpleName || '').toLowerCase();
                    return vals.some(v => implName.includes(v) || v.includes(implName.replace(/strategy|service|impl|bean/gi, '')));
                }
                return false;
            }) || info.fields[0];

            html += `<div class="ambi-picker-impl" onclick="document.getElementById('ambi-call-picker').remove();JA.summary.showClassCode('${safeFqn}','${safeMethod}')">`;
            html += '<div class="ambi-impl-header">';
            html += `<span class="impl-picker-dot" style="background:${color}"></span>`;
            html += `<span class="ambi-impl-name" style="color:${color}">${esc(impl.simpleName || '?')}</span>`;
            html += `<span class="impl-picker-stereo">${esc(stereo)}</span>`;
            html += '</div>';

            if (field) {
                html += '<div class="ambi-impl-detail">';
                html += `<span class="ambi-detail-label">var:</span> <span class="ambi-field-name">${esc(field.name || '?')}</span>`;
                html += ` <span class="ambi-field-type">${esc((field.type || '').split('.').pop())}</span>`;
                const relevantAnns = (field.annotations || []).filter(a => a.name !== 'Autowired' && a.name !== 'jakarta.inject.Inject');
                for (const ann of relevantAnns) {
                    const attrs = ann.attributes
                        ? Object.entries(ann.attributes).map(([k, v]) => (k === 'value' ? `"${v}"` : `${k}="${v}"`)).join(', ')
                        : '';
                    html += ` <span class="ambi-ann">@${esc(ann.name)}${attrs ? `(${esc(attrs)})` : ''}</span>`;
                }
                html += '</div>';
            }
            html += '</div>';
        }

        picker.innerHTML = html;
        document.body.appendChild(picker);

        // Position near click, keep inside viewport
        const x = Math.min(event.clientX + 6, window.innerWidth - 310);
        const y = Math.min(event.clientY + 6, window.innerHeight - 20);
        picker.style.left = x + 'px';
        picker.style.top  = y + 'px';
        // Adjust upward if overflows bottom
        requestAnimationFrame(() => {
            const rect = picker.getBoundingClientRect();
            if (rect.bottom > window.innerHeight - 10) {
                picker.style.top = Math.max(10, event.clientY - rect.height - 6) + 'px';
            }
        });

        setTimeout(() => {
            const handler = (e) => {
                if (!picker.contains(e.target)) { picker.remove(); document.removeEventListener('click', handler); }
            };
            document.addEventListener('click', handler);
        }, 10);
    },

    /* --- Breadcrumb chain navigation --- */
    _bcChains: [],
    _pendingChain: null,

    _registerChain(chain) {
        const idx = this._bcChains.length;
        this._bcChains.push(chain);
        return idx;
    },

    showBcCode(chainIdx, segIndex) {
        const chain = this._bcChains[chainIdx];
        if (!chain || !chain[segIndex]) return;
        const seg = chain[segIndex];
        this._pendingChain = { chain, activeIndex: segIndex };
        this.showClassCode(seg.className || seg.label.split('.')[0], seg.methodName);
    },

    _renderChainBanner(chain, activeIndex) {
        const esc = JA.utils.escapeHtml;
        let html = '<div class="cm-chain-banner">';
        html += '<span class="cm-chain-title">Call Chain</span>';
        chain.forEach((seg, i) => {
            if (i > 0) html += '<span class="cm-chain-arrow">&rarr;</span>';
            const isActive = i === activeIndex;
            const cls = isActive ? 'cm-chain-seg cm-chain-active' : 'cm-chain-seg cm-chain-link';
            const safeClass = (seg.className || seg.label.split('.')[0] || '').replace(/'/g, "\\'");
            const safeMethod = (seg.methodName || '').replace(/'/g, "\\'");
            const onclick = isActive ? '' : ` onclick="JA.summary.showClassCode('${safeClass}','${safeMethod}')"`;
            html += `<span class="cm-chain-level">L${seg.level || (i + 1)}</span>`;
            html += `<span class="${cls}"${onclick} title="${esc(seg.full || seg.label || '')}">${esc(seg.label || '?')}</span>`;
        });
        html += '</div>';
        return html;
    },

    _codeModalBack() {
        if (this._codeIdx > 0) {
            this._codeIdx--;
            this._renderCodeContent();
        }
    },

    _codeModalForward() {
        if (this._codeIdx < this._codeStack.length - 1) {
            this._codeIdx++;
            this._renderCodeContent();
        }
    },

    _codeModalGoTo(idx) {
        if (idx >= 0 && idx < this._codeStack.length) {
            this._codeIdx = idx;
            this._renderCodeContent();
        }
    },

    _renderCodeLoading() {
        const header = document.getElementById('code-modal-header');
        const body = document.getElementById('code-modal-body');
        if (!header || !body) return;
        const entry = this._codeStack[this._codeIdx];
        header.innerHTML = this._buildModalHeader();
        body.innerHTML = `<div class="cm-loading"><div class="cm-spinner"></div><div class="cm-loading-text">Decompiling ${JA.utils.escapeHtml(entry.cls.simpleName || '')}...</div></div>`;
    },

    _renderCodeContent() {
        const header = document.getElementById('code-modal-header');
        const body = document.getElementById('code-modal-body');
        if (!header || !body) return;
        const entry = this._codeStack[this._codeIdx];
        if (!entry) return;
        if (entry.loading) { this._renderCodeLoading(); return; }

        header.innerHTML = this._buildModalHeader();

        const esc = s => (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        let codeHtml, sidebarHtml, badge;

        if (entry.source) {
            const result = this._buildDecompCode(entry.cls, entry.source, entry.highlightMethod, esc);
            codeHtml = result.code;
            sidebarHtml = result.sidebar;
            badge = '<span class="cm-src-badge cm-badge-decomp">DECOMPILED</span>';
        } else {
            const result = this._buildFallbackCode(entry.cls, entry.highlightMethod, esc);
            codeHtml = result.code;
            sidebarHtml = result.sidebar;
            badge = '<span class="cm-src-badge cm-badge-recon">RECONSTRUCTED</span>';
        }

        const cls = entry.cls;
        body.innerHTML = `<div class="cm-info-bar">
            <span class="cm-stereo">${esc(cls.stereotype || '')}</span>
            <span class="cm-classname">${esc(cls.simpleName || '')}</span>
            <span class="cm-fqn">${esc(cls.fullyQualifiedName || '')}</span>
            ${badge}
            <span class="cm-jar">${esc(cls.sourceJar || 'Main App')}</span>
        </div>
        <div class="cm-content">
            <div class="cm-code-panel"><table class="cm-code-table">${codeHtml}</table></div>
            <div class="cm-sidebar">${sidebarHtml}</div>
        </div>`;

        // Scroll to highlighted method — use double rAF to ensure DOM is fully rendered
        requestAnimationFrame(() => requestAnimationFrame(() => {
            const hlAll = body.querySelectorAll('.cm-hl-method');
            if (hlAll.length) {
                hlAll[0].scrollIntoView({ block: 'center', behavior: 'smooth' });
                hlAll.forEach(r => r.classList.add('cm-hl-flash'));
                setTimeout(() => hlAll.forEach(r => r.classList.remove('cm-hl-flash')), 2000);
            }
            const sbHl = body.querySelector('.cm-sb-highlight');
            if (sbHl) {
                sbHl.scrollIntoView({ block: 'center', behavior: 'smooth' });
            }
        }));

        // Render call chain banner if navigated from breadcrumb
        if (this._pendingChain) {
            const chainData = this._pendingChain;
            this._pendingChain = null;
            const infoBar = body.querySelector('.cm-info-bar');
            if (infoBar) {
                const chainHtml = this._renderChainBanner(chainData.chain, chainData.activeIndex);
                infoBar.insertAdjacentHTML('afterend', chainHtml);
            }
        }
    },

    _buildModalHeader() {
        const esc = JA.utils.escapeHtml;
        let html = '<div class="cm-nav-btns">';
        html += `<button class="btn-sm" onclick="JA.summary._codeModalBack()" ${this._codeIdx <= 0 ? 'disabled' : ''}>&#9664; Back</button>`;
        html += `<button class="btn-sm" onclick="JA.summary._codeModalForward()" ${this._codeIdx >= this._codeStack.length - 1 ? 'disabled' : ''}>Next &#9654;</button>`;
        html += '</div>';
        html += '<div class="cm-breadcrumb">';
        this._codeStack.forEach((entry, i) => {
            if (i > 0) html += '<span class="cm-bc-sep">&#9656;</span>';
            const label = entry.highlightMethod
                ? (entry.cls.simpleName || '?') + '.' + entry.highlightMethod
                : (entry.cls.simpleName || '?');
            if (i === this._codeIdx) {
                html += `<span class="cm-bc-item cm-bc-active">${esc(label)}</span>`;
            } else {
                html += `<span class="cm-bc-item cm-bc-link" onclick="JA.summary._codeModalGoTo(${i})">${esc(label)}</span>`;
            }
        });
        html += '</div>';
        html += `<span class="cm-stack-count">${this._codeStack.length} level${this._codeStack.length !== 1 ? 's' : ''}</span>`;
        html += `<button class="btn-sm cm-copy-btn" onclick="JA.summary._copyCode()" title="Copy source to clipboard">Copy</button>`;
        html += `<button class="btn-sm cm-close-btn" onclick="JA.summary._closeCodeModal()">Close</button>`;
        return html;
    },

    /* --- Code rendering (returns {code, sidebar} HTML strings) --- */

    _buildDecompCode(cls, source, highlightMethod, esc, containerSel) {
        const lines = source.split('\n');
        if (!this._classIdx && JA.app.currentAnalysis) {
            this._classIdx = {};
            const classSource = JA.app.currentAnalysis.classIndex || JA.app.currentAnalysis.classes || [];
            for (const c of classSource) {
                this._classIdx[c.fullyQualifiedName] = c;
                if (!this._classIdx[c.simpleName]) this._classIdx[c.simpleName] = c;
            }
        }
        const classIdx = this._classIdx;

        // Collect ALL unique navigable invocations (text-based matching, not line-number)
        const navTargets = new Map(); // methodName -> {ownerClass, methodName}
        for (const m of (cls.methods || [])) {
            for (const inv of (m.invocations || [])) {
                if (!inv.ownerClass || inv.methodName === '<init>' || inv.methodName.startsWith('lambda$')) continue;
                const target = classIdx && (classIdx[inv.ownerClass] || classIdx[(inv.ownerClass || '').split('.').pop()]);
                if (target && !navTargets.has(inv.methodName)) {
                    navTargets.set(inv.methodName, inv);
                }
            }
        }

        // Build per-field dispatch narrowing — uses receiverFieldName from bytecode analysis
        // to resolve which implementation a specific field's call resolves to, rather than
        // treating every interface call as globally ambiguous.
        if (JA.nav && (!JA.nav._implMap || (!Object.keys(JA.nav._implMap).length && JA.app.currentAnalysis))) JA.nav.init();
        if (!this._ambiPickers) this._ambiPickers = [];

        const STANDARD_ANNS = new Set(['Autowired','Inject','Qualifier','Named','Resource','Value',
            'NonNull','Nullable','NotNull','NotBlank','Override','SuppressWarnings','Deprecated',
            'Component','Service','Repository','Controller','RestController','Primary','Lazy']);

        // fieldDispatch: fieldName → { target, impls[], dispatchType }
        const fieldDispatch = new Map();
        const fieldAwareMethods = new Set(); // interface methods handled via field dispatch

        // Collect interface/abstract receiver-field → methodName mappings from bytecode.
        // Only add when the owner is an interface or abstract class — concrete-class methods
        // (e.g. orchestrator.runAllStrategiesOnMetric) must stay in navTargets for direct nav.
        for (const m of (cls.methods || [])) {
            for (const inv of (m.invocations || [])) {
                if (!inv.receiverFieldName || !inv.ownerClass || !inv.methodName) continue;
                const owner = classIdx && (classIdx[inv.ownerClass] || classIdx[inv.ownerClass.split('.').pop()]);
                if (owner && (owner.isInterface || owner.isAbstract)) {
                    fieldAwareMethods.add(inv.methodName);
                }
            }
        }

        for (const f of (cls.fields || [])) {
            const fType = f.type; if (!fType) continue;
            const target = classIdx && (classIdx[fType] || classIdx[fType.split('.').pop()]);
            if (!target || (!target.isInterface && !target.isAbstract)) continue;

            const allImpls = (JA.nav && JA.nav._implMap)
                ? (JA.nav._implMap[target.fullyQualifiedName] || JA.nav._implMap[target.simpleName] || [])
                : [];
            if (allImpls.length < 1) continue;
            if (allImpls.length === 1) { fieldDispatch.set(f.name, { target, impls: allImpls, dispatchType: 'DIRECT' }); continue; }

            const anns = f.annotations || [];
            let narrowed = allImpls, dtype = 'AMBIGUOUS_IMPL';

            // Strategy 1: @Qualifier / @Named explicit value
            const qualAnn = anns.find(a => a.name === 'Qualifier' || a.name === 'Named');
            const qualVal = qualAnn && (qualAnn.attributes && qualAnn.attributes.value || qualAnn.value);
            if (qualVal) {
                const qLo = qualVal.toLowerCase();
                const m1 = allImpls.filter(impl => {
                    const bn = impl.simpleName.charAt(0).toLowerCase() + impl.simpleName.slice(1);
                    return bn === qualVal || impl.simpleName.toLowerCase().includes(qLo) || (impl.fullyQualifiedName || '').endsWith('.' + qualVal);
                });
                if (m1.length === 1) { narrowed = m1; dtype = 'QUALIFIED'; }
            }

            // Strategy 2: Custom qualifier annotation with value (e.g. @AnalysisType("statistical"))
            if (dtype === 'AMBIGUOUS_IMPL') {
                for (const ca of anns.filter(a => !STANDARD_ANNS.has(a.name) && a.attributes && a.attributes.value)) {
                    const caLo = String(ca.attributes.value).toLowerCase();
                    const m2 = allImpls.filter(impl => impl.simpleName.toLowerCase().includes(caLo));
                    if (m2.length === 1) { narrowed = m2; dtype = 'QUALIFIED'; break; }
                }
            }

            // Strategy 3: Field name heuristic — camelCase segment match
            if (dtype === 'AMBIGUOUS_IMPL') {
                const segs = [f.name.toLowerCase(),
                    ...f.name.split(/(?<=[a-z])(?=[A-Z])/).map(s => s.toLowerCase()).filter(s => s.length > 3)];
                for (const seg of segs) {
                    const m3 = allImpls.filter(impl => impl.simpleName.toLowerCase().includes(seg));
                    if (m3.length === 1) { narrowed = m3; dtype = 'HEURISTIC'; break; }
                }
            }

            fieldDispatch.set(f.name, { target, impls: narrowed, dispatchType: dtype });
        }

        // Remove field-aware methods from navTargets so they aren't double-processed
        for (const k of fieldAwareMethods) navTargets.delete(k);

        // Fall-back ambiguous targets: interface calls WITHOUT receiverFieldName info
        const ambiguousTargets = new Map();
        const ambiIdxMap = new Map();
        for (const [methodName, inv] of navTargets) {
            const target = classIdx && (classIdx[inv.ownerClass] || classIdx[(inv.ownerClass || '').split('.').pop()]);
            if (!target || (!target.isInterface && !target.isAbstract)) continue;
            const impls = (JA.nav && JA.nav._implMap)
                ? (JA.nav._implMap[target.fullyQualifiedName] || JA.nav._implMap[target.simpleName] || [])
                : [];
            if (impls.length < 1) continue;
            const matchingFields = (cls.fields || []).filter(f => {
                const fShort = (f.type || '').split('.').pop();
                return f.type === inv.ownerClass || f.type === target.fullyQualifiedName || fShort === target.simpleName;
            });
            ambiguousTargets.set(methodName, { iface: target, impls, fields: matchingFields, inv });
        }
        for (const k of ambiguousTargets.keys()) navTargets.delete(k);
        for (const [methodName, info] of ambiguousTargets) {
            ambiIdxMap.set(methodName, this._ambiPickers.length);
            this._ambiPickers.push(info);
        }

        // Find highlighted method by scanning decompiled source for method DECLARATION.
        // Bytecode line numbers don't match CFR decompiler output — always use regex.
        let hlStart = -1, hlEnd = -1;
        if (highlightMethod) {
            const safeName = highlightMethod.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const declRe = new RegExp('\\b' + safeName + '\\s*\\(');
            const accessRe = /^\s*(public|private|protected|static|final|abstract|synchronized|native|default|@\w)/;
            // Return type before method name: word/generic/array char then whitespace — avoids matching assignments
            const typeBeforeRe = new RegExp('[\\w>\\]]\\s+' + safeName + '\\s*\\(');
            // Assignment pattern: = methodName( — NOT a declaration
            const assignRe = new RegExp('=\\s*' + safeName + '\\s*\\(');
            let candidates = [];
            let braceDepth = 0, inMethod = false, currentStart = -1, seenOpenBrace = false;
            for (let i = 0; i < lines.length; i++) {
                const trimmed = lines[i].trim();
                if (!inMethod && declRe.test(trimmed) && !trimmed.startsWith('//') && !trimmed.startsWith('*')
                    && !trimmed.startsWith('/*') && !trimmed.startsWith('return ')
                    && !trimmed.startsWith('.') && !assignRe.test(trimmed)
                    && (accessRe.test(trimmed) || typeBeforeRe.test(trimmed))) {
                    currentStart = i + 1;
                    inMethod = true;
                    braceDepth = 0;
                    seenOpenBrace = false;
                }
                if (inMethod) {
                    for (const ch of lines[i]) {
                        if (ch === '{') { braceDepth++; seenOpenBrace = true; }
                        if (ch === '}') braceDepth--;
                    }
                    // Method with body: close when braces balance after opening
                    if (seenOpenBrace && braceDepth <= 0) {
                        candidates.push({ start: currentStart, end: i + 1 });
                        inMethod = false;
                        currentStart = -1;
                        seenOpenBrace = false;
                    }
                    // Abstract/interface method: no body, ends with semicolon (not inside braces)
                    else if (!seenOpenBrace && trimmed.endsWith(';')) {
                        candidates.push({ start: currentStart, end: i + 1 });
                        inMethod = false;
                        currentStart = -1;
                    }
                }
            }
            // Pick best candidate: if we have parameter types from the call node, match them
            if (candidates.length === 1) {
                hlStart = candidates[0].start;
                hlEnd = candidates[0].end;
            } else if (candidates.length > 1) {
                // Try to match by parameter types from the node data
                const nodeParams = this._pendingParamTypes || [];
                let best = candidates[0]; // fallback: first declaration
                if (nodeParams.length > 0) {
                    for (const c of candidates) {
                        const declLine = lines[c.start - 1] || '';
                        let matches = 0;
                        for (const p of nodeParams) {
                            const shortType = p.split('.').pop();
                            if (declLine.includes(shortType)) matches++;
                        }
                        if (matches > 0 && matches >= nodeParams.length) { best = c; break; }
                    }
                }
                hlStart = best.start;
                hlEnd = best.end;
            }
            this._pendingParamTypes = null;
        }

        const code = lines.map((line, i) => {
            const num = i + 1;
            const isHl = hlStart > 0 && num >= hlStart && num <= hlEnd;

            let highlighted = esc(line);
            highlighted = highlighted.replace(/\b(package|import|public|private|protected|static|final|abstract|class|interface|enum|extends|implements|return|if|else|for|while|do|switch|case|break|continue|new|try|catch|finally|throw|throws|void|this|super|instanceof|synchronized|volatile|transient|native|default)\b/g, '<span class="cm-kw">$1</span>');
            highlighted = highlighted.replace(/@(\w+)/g, '<span class="cm-ann">@$1</span>');
            highlighted = highlighted.replace(/(&quot;[^&]*?&quot;)/g, '<span class="cm-str">$1</span>');
            highlighted = highlighted.replace(/(\/\/.*)$/g, '<span class="cm-cmt">$1</span>');

            // Field-aware interface calls: match `fieldName.method(` and navigate based on
            // narrowed dispatch (QUALIFIED → direct link, AMBIGUOUS → picker, DIRECT → link).
            let isAmbi = false;
            for (const [fieldName, dispatch] of fieldDispatch) {
                const fRe = new RegExp('\\b(' + fieldName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\.([a-zA-Z_$][\\w$]*)\\s*\\(');
                if (!fRe.test(highlighted) || highlighted.includes('cm-call-link') || highlighted.includes('cm-ambi-call')) continue;
                const fm = fRe.exec(highlighted); if (!fm) continue;
                const calledMethod = fm[2];
                if (dispatch.dispatchType !== 'AMBIGUOUS_IMPL' && dispatch.impls.length >= 1) {
                    const impl = dispatch.impls[0];
                    const safeFqn = (impl.fullyQualifiedName || impl.simpleName || '').replace(/'/g, "\\'");
                    const safeM = calledMethod.replace(/'/g, "\\'");
                    const tip = (dispatch.dispatchType === 'QUALIFIED' ? '✓ QUALIFIED' : dispatch.dispatchType === 'HEURISTIC' ? '~ HEURISTIC' : '→') + ': ' + (impl.simpleName || '') + '.' + calledMethod + '()';
                    highlighted = highlighted.replace(fRe,
                        `$1.<span class="cm-call-link" onclick="JA.summary.showClassCode('${safeFqn}','${safeM}')" title="${esc(tip)}">${esc(calledMethod)}</span>(`
                    );
                } else {
                    const pickerInfo = { iface: dispatch.target, impls: dispatch.impls,
                        fields: [(cls.fields || []).find(f2 => f2.name === fieldName)].filter(Boolean), inv: null };
                    const pickerIdx = this._ambiPickers.length;
                    this._ambiPickers.push(pickerInfo);
                    highlighted = highlighted.replace(fRe,
                        `$1.<span class="cm-ambi-call" onclick="JA.summary._showAmbiguousCallPicker(${pickerIdx},event)" title="⚠ Ambiguous — ${dispatch.impls.length} impls of ${esc(dispatch.target.simpleName)} — click to choose">${esc(calledMethod)}</span>(`
                    );
                    isAmbi = true;
                }
            }

            // Fall-back: interface calls without receiver field (old ambiguous-target path)
            for (const [methodName, info] of ambiguousTargets) {
                const re = new RegExp('\\b(' + methodName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\s*\\(');
                if (re.test(highlighted) && !highlighted.includes('cm-ambi-call') && !highlighted.includes('cm-call-link')) {
                    const pickerIdx = ambiIdxMap.get(methodName);
                    const tipIface = esc(info.iface.simpleName || '');
                    const tipCount = info.impls.length;
                    highlighted = highlighted.replace(re,
                        `<span class="cm-ambi-call" onclick="JA.summary._showAmbiguousCallPicker(${pickerIdx},event)" title="⚠ Ambiguous — ${tipCount} implementation${tipCount !== 1 ? 's' : ''} of ${tipIface} — click to choose">$1</span>(`
                    );
                    isAmbi = true;
                }
            }

            // Direct navigable calls
            for (const [methodName, inv] of navTargets) {
                const re = new RegExp('\\b(' + methodName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\s*\\(');
                if (re.test(highlighted) && !highlighted.includes('cm-call-link') && !highlighted.includes('cm-ambi-call')) {
                    const safeOwner = (inv.ownerClass || '').replace(/'/g, "\\'");
                    const safeMethod = methodName.replace(/'/g, "\\'");
                    highlighted = highlighted.replace(re, `<span class="cm-call-link" onclick="JA.summary.showClassCode('${safeOwner}','${safeMethod}')" title="${esc(inv.ownerClass + '.' + methodName)} — Click to navigate">$1</span>(`);
                }
            }

            const lineClass = isHl ? ' class="cm-hl-method"' : isAmbi ? ' class="cm-ambi-line"' : '';
            return `<tr${lineClass}><td class="cm-ln">${num}</td><td class="cm-code">${highlighted}</td></tr>`;
        }).join('\n');

        const sidebar = this._buildModalSidebar(cls, classIdx, esc, containerSel, highlightMethod);
        return { code, sidebar };
    },

    _buildFallbackCode(cls, highlightMethod, esc, containerSel) {
        const codeLines = this._reconstructJavaFallback(cls);
        if (!this._classIdx && JA.app.currentAnalysis) {
            this._classIdx = {};
            const classSource = JA.app.currentAnalysis.classIndex || JA.app.currentAnalysis.classes || [];
            for (const c of classSource) {
                this._classIdx[c.fullyQualifiedName] = c;
                if (!this._classIdx[c.simpleName]) this._classIdx[c.simpleName] = c;
            }
        }
        const classIdx = this._classIdx;
        let inHighlight = false;

        // Build invocation map: collect all invocations from all methods for clickable links
        const allInvocations = [];
        for (const m of (cls.methods || [])) {
            for (const inv of (m.invocations || [])) {
                if (inv.methodName === '<init>' || inv.methodName.startsWith('lambda$')) continue;
                allInvocations.push(inv);
            }
        }

        const code = codeLines.map((line, i) => {
            const num = i + 1;
            let lineClass = '';
            if (highlightMethod && line.includes(' ' + highlightMethod + '(')) { inHighlight = true; lineClass = ' class="cm-hl-method"'; }
            else if (inHighlight && line.trim() === '}') { lineClass = ' class="cm-hl-method"'; inHighlight = false; }
            else if (inHighlight) { lineClass = ' class="cm-hl-method"'; }

            let highlighted = esc(line);
            highlighted = highlighted.replace(/\b(package|import|public|private|protected|static|final|abstract|class|interface|enum|extends|implements|return|void|this)\b/g, '<span class="cm-kw">$1</span>');
            highlighted = highlighted.replace(/@(\w+)/g, '<span class="cm-ann">@$1</span>');
            highlighted = highlighted.replace(/(&quot;[^&]*?&quot;)/g, '<span class="cm-str">$1</span>');
            highlighted = highlighted.replace(/(\/\/.*)$/g, '<span class="cm-cmt">$1</span>');

            // Make method calls clickable in reconstructed code (lines like: caller.methodName(...))
            for (const inv of allInvocations) {
                if (!inv.ownerClass || !inv.methodName) continue;
                const target = classIdx && (classIdx[inv.ownerClass] || classIdx[(inv.ownerClass || '').split('.').pop()]);
                if (!target) continue;
                const safeOwner = (inv.ownerClass || '').replace(/'/g, "\\'");
                const safeMethod = (inv.methodName || '').replace(/'/g, "\\'");
                const re = new RegExp('\\b(' + inv.methodName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\(');
                if (re.test(highlighted) && !highlighted.includes('cm-call-link')) {
                    highlighted = highlighted.replace(re, `<span class="cm-call-link" onclick="JA.summary.showClassCode('${safeOwner}','${safeMethod}')" title="${esc(inv.ownerClass + '.' + inv.methodName)} — Click to navigate">$1</span>(`);
                }
            }

            return `<tr${lineClass}><td class="cm-ln">${num}</td><td class="cm-code">${highlighted}</td></tr>`;
        }).join('\n');

        const sidebar = this._buildModalSidebar(cls, classIdx, esc, containerSel, highlightMethod);
        return { code, sidebar };
    },

    _buildModalSidebar(cls, classIdx, esc, containerSel, highlightMethod) {
        containerSel = containerSel || '#code-modal-body';
        const filterId = 'cm-sb-' + Date.now();
        let secNum = 0;
        let html = '';

        // Methods
        const methodIdx = {};
        for (const m of (cls.methods || [])) {
            if (m.name === '<init>' || m.name === '<clinit>' || m.name.startsWith('lambda$')) continue;
            methodIdx[m.name] = m;
        }
        const safeFqn = (cls.fullyQualifiedName || '').replace(/'/g, "\\'");
        const mId = filterId + '-' + (secNum++);
        html += '<div class="cm-sb-section"><div class="cm-sb-title">Methods (' + Object.keys(methodIdx).length + ')</div>';
        html += `<input type="text" class="cm-sb-filter" placeholder="Filter methods..." oninput="JA.summary._filterSbSection(this)">`;
        html += '<div class="cm-sb-scroll">';
        for (const [name, m] of Object.entries(methodIdx)) {
            const params = (m.parameters || []).map(p => this._shortType(p.type || '?')).join(', ');
            const line = m.startLine || '';
            const isHl = highlightMethod && name === highlightMethod;
            const safeMName = name.replace(/'/g, "\\'");
            const methodClick = containerSel === '#code-panel'
                ? `JA.codeTreePanel.show('${safeFqn}','${safeMName}')`
                : `JA.summary.showClassCode('${safeFqn}','${safeMName}')`;
            // Detect self-recursive invocation
            const isRecursive = (m.invocations || []).some(inv =>
                (inv.ownerClass === cls.fullyQualifiedName || inv.ownerClass === cls.simpleName) &&
                inv.methodName === m.name
            );
            html += `<div class="cm-sb-item cm-sb-click${isHl ? ' cm-sb-highlight' : ''}" onclick="${methodClick}">`;
            html += `<span class="cm-sb-method">${esc(name)}</span><span class="cm-sb-params">(${esc(params)})</span>`;
            html += `<span class="cm-sb-line">:${line}</span>`;
            if (isRecursive) html += ' <span class="cm-sb-recursive-badge" title="Recursive — calls itself">&#8635; recursive</span>';
            html += '</div>';
        }
        html += '</div></div>';

        // Fields
        const fields = (cls.fields || []).filter(f => !(f.name || '').startsWith('this$') && !['Logger', 'Log'].includes(this._shortType(f.type || '')));
        if (fields.length) {
            html += '<div class="cm-sb-section"><div class="cm-sb-title">Fields (' + fields.length + ')</div>';
            html += `<input type="text" class="cm-sb-filter" placeholder="Filter fields..." oninput="JA.summary._filterSbSection(this)">`;
            html += '<div class="cm-sb-scroll">';
            for (const f of fields) {
                html += `<div class="cm-sb-item"><span class="cm-sb-type">${esc(this._shortType(f.type || '?'))}</span> <span class="cm-sb-field">${esc(f.name || '?')}</span>`;
                if (f.constantValue) html += ` = <span class="cm-sb-const">"${esc(f.constantValue)}"</span>`;
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Collection references
        const collRefs = new Set();
        for (const m of (cls.methods || [])) {
            for (const lit of (m.stringLiterals || [])) {
                if (lit.includes('_') && this._detectDomain(lit) !== 'Other') collRefs.add(lit);
            }
        }
        for (const f of (cls.fields || [])) {
            if (f.constantValue && this._detectDomain(f.constantValue) !== 'Other') collRefs.add(f.constantValue);
        }
        if (collRefs.size) {
            html += '<div class="cm-sb-section"><div class="cm-sb-title">Collections (' + collRefs.size + ')</div>';
            html += `<input type="text" class="cm-sb-filter" placeholder="Filter collections..." oninput="JA.summary._filterSbSection(this)">`;
            html += '<div class="cm-sb-scroll">';
            for (const c of collRefs) {
                const domain = this._detectDomain(c);
                html += `<div class="cm-sb-item"><span class="cm-sb-coll">${esc(c)}</span> <span class="cm-sb-domain">${esc(domain)}</span></div>`;
            }
            html += '</div></div>';
        }

        // Referenced classes (no truncation — full list with scroll + filter)
        // Build map: referencedClass → first method in that class that we call
        const importMethodMap = {};
        for (const m of (cls.methods || [])) {
            for (const inv of (m.invocations || [])) {
                if (!inv.ownerClass || !inv.ownerClass.includes('.') || inv.ownerClass.startsWith('java.lang.')) continue;
                if (inv.methodName === '<init>' || inv.methodName === '<clinit>' || inv.methodName.startsWith('lambda$')) continue;
                if (!importMethodMap[inv.ownerClass]) {
                    importMethodMap[inv.ownerClass] = inv.methodName;
                }
            }
        }
        const imports = new Set(Object.keys(importMethodMap));
        // Also include classes referenced only via <init> / lambda (but without a target method)
        for (const m of (cls.methods || [])) {
            for (const inv of (m.invocations || [])) {
                if (inv.ownerClass && inv.ownerClass.includes('.') && !inv.ownerClass.startsWith('java.lang.'))
                    imports.add(inv.ownerClass);
            }
        }
        if (imports.size) {
            html += '<div class="cm-sb-section"><div class="cm-sb-title">Referenced Classes (' + imports.size + ')</div>';
            html += `<input type="text" class="cm-sb-filter" placeholder="Filter classes..." oninput="JA.summary._filterSbSection(this)">`;
            html += '<div class="cm-sb-scroll">';
            for (const imp of [...imports].sort()) {
                const target = classIdx && (classIdx[imp] || classIdx[imp.split('.').pop()]);
                const safeImp = imp.replace(/'/g, "\\'");
                if (target) {
                    const targetMethod = importMethodMap[imp] || '';
                    const safeMethod = targetMethod.replace(/'/g, "\\'");
                    html += `<div class="cm-sb-item cm-sb-click" onclick="JA.summary.showClassCode('${safeImp}','${safeMethod}')" title="Jump to ${esc(imp)}${targetMethod ? '.' + esc(targetMethod) + '()' : ''}"><span class="cm-sb-imp">${esc(imp)}</span>`;
                    if (targetMethod) html += `<span class="cm-sb-line">.${esc(targetMethod)}</span>`;
                    html += '</div>';
                } else {
                    html += `<div class="cm-sb-item"><span class="cm-sb-imp cm-sb-ext">${esc(imp)}</span></div>`;
                }
            }
            html += '</div></div>';
        }

        // Used By (reverse references — who calls this class within the JAR)
        html += this._buildUsedBySection(cls, classIdx, esc);

        // Implementations (for interfaces / abstract classes)
        html += this._buildImplementationsSection(cls, esc, highlightMethod);

        return html;
    },

    _buildImplementationsSection(cls, esc, highlightMethod) {
        if (!cls.isInterface && !cls.isAbstract) return '';
        if (!JA.nav) return '';
        if (!JA.nav._implMap || (!Object.keys(JA.nav._implMap).length && JA.app.currentAnalysis)) JA.nav.init();
        const fqn = cls.fullyQualifiedName || '';
        const impls = (JA.nav._implMap[fqn] || JA.nav._implMap[cls.simpleName] || []);
        if (!impls.length) return '';

        const safeMethod = (highlightMethod || '').replace(/'/g, "\\'");
        let html = '<div class="cm-sb-section cm-sb-impls-section">';
        html += `<div class="cm-sb-title" style="color:#ec4899">Implementations (${impls.length})</div>`;
        html += '<div class="cm-sb-scroll">';
        for (const impl of impls) {
            const implFqn = (impl.fullyQualifiedName || impl.simpleName || '').replace(/'/g, "\\'");
            const color = JA.utils.stereotypeColor(impl.stereotype);
            const stereo = (impl.stereotype || 'CLASS').replace('REST_', '');
            html += `<div class="cm-sb-item cm-sb-click" onclick="JA.summary.showClassCode('${implFqn}','${safeMethod}')">`;
            html += `<span class="cm-sb-impl-dot" style="background:${color};width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:5px;flex-shrink:0"></span>`;
            html += `<span class="cm-sb-imp">${esc(impl.simpleName || impl.fullyQualifiedName || '?')}</span>`;
            html += `<span class="cm-sb-domain">${esc(stereo)}</span>`;
            html += '</div>';
        }
        html += '</div></div>';
        return html;
    },

    _showImplPickerForCode(cls, impls, highlightMethod) {
        const esc = JA.utils.escapeHtml;
        const existing = document.getElementById('impl-picker-code');
        if (existing) existing.remove();

        const picker = document.createElement('div');
        picker.id = 'impl-picker-code';
        picker.className = 'impl-picker';
        picker.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:20000;min-width:260px';

        let html = '<div class="impl-picker-header">';
        html += '<span class="impl-picker-icon">&#9671;</span>';
        html += `<span class="impl-picker-title">${esc(cls.simpleName || '')}</span>`;
        html += '<span class="impl-picker-badge">Interface</span>';
        html += '</div>';
        html += '<div class="impl-picker-subtitle">Choose Implementation to View Code</div>';

        for (const impl of impls) {
            const color = JA.utils.stereotypeColor(impl.stereotype);
            const stereo = (impl.stereotype || 'CLASS').replace('REST_', '');
            const safeFqn = (impl.fullyQualifiedName || impl.simpleName || '').replace(/'/g, "\\'");
            const safeMethod = (highlightMethod || '').replace(/'/g, "\\'");
            html += `<div class="impl-picker-item" onclick="document.getElementById('impl-picker-code').remove();JA.summary.showClassCode('${safeFqn}','${safeMethod}')">`;
            html += `<span class="impl-picker-dot" style="background:${color}"></span>`;
            html += `<span class="impl-picker-name">${esc(impl.simpleName || impl.fullyQualifiedName || '?')}</span>`;
            html += `<span class="impl-picker-stereo">${esc(stereo)}</span>`;
            html += '</div>';
        }

        // Option to view the interface itself
        const ifaceFqn = (cls.fullyQualifiedName || cls.simpleName || '').replace(/'/g, "\\'");
        const safeHl  = (highlightMethod || '').replace(/'/g, "\\'");
        html += '<div class="impl-picker-divider"></div>';
        html += `<div class="impl-picker-item impl-picker-iface" onclick="document.getElementById('impl-picker-code').remove();JA.summary.showClassCode('${ifaceFqn}','${safeHl}',true)">`;
        html += '<span class="impl-picker-dot" style="background:#ec4899"></span>';
        html += `<span class="impl-picker-name">${esc(cls.simpleName || '')}</span>`;
        html += '<span class="impl-picker-stereo">INTERFACE</span>';
        html += '</div>';

        picker.innerHTML = html;
        document.body.appendChild(picker);

        setTimeout(() => {
            const handler = (e) => {
                if (!picker.contains(e.target)) { picker.remove(); document.removeEventListener('click', handler); }
            };
            document.addEventListener('click', handler);
        }, 10);
    },

    /* --- Code search (IntelliJ-style Ctrl+F) --- */

    _codeSearchOpen() {
        const bar = document.getElementById('cm-search-bar');
        if (!bar) return;
        bar.style.display = 'flex';
        const input = document.getElementById('cm-search-input');
        if (input) { input.focus(); input.select(); }
    },

    _codeSearchClose() {
        const bar = document.getElementById('cm-search-bar');
        if (bar) bar.style.display = 'none';
        this._codeSearchClear();
        // Re-focus panel for keyboard events
        const panel = document.querySelector('.code-modal-panel');
        if (panel) panel.focus();
    },

    _codeSearchToggleCase() {
        this._searchState.caseSensitive = document.getElementById('cm-search-case')?.checked || false;
        const input = document.getElementById('cm-search-input');
        if (input && input.value) this._codeSearch(input.value);
    },

    _codeSearch(query) {
        this._codeSearchClear();
        this._searchState.query = query;
        if (!query) {
            document.getElementById('cm-search-count').textContent = '';
            return;
        }

        const table = document.querySelector('#code-modal-body .cm-code-table');
        if (!table) return;

        const cs = this._searchState.caseSensitive;
        const searchQ = cs ? query : query.toLowerCase();
        const rows = table.querySelectorAll('tr');
        const matches = [];

        rows.forEach((row, rowIdx) => {
            const codeCell = row.querySelector('.cm-code');
            if (!codeCell) return;
            const text = codeCell.textContent;
            const compareText = cs ? text : text.toLowerCase();
            let startIdx = 0;
            while (true) {
                const pos = compareText.indexOf(searchQ, startIdx);
                if (pos === -1) break;
                matches.push({ row: rowIdx, cell: codeCell, pos, len: query.length });
                startIdx = pos + 1;
            }
        });

        this._searchState.matches = matches;

        // Highlight all matches in yellow
        if (matches.length > 0) {
            // Group by cell to avoid double processing
            const cellMatches = new Map();
            for (const m of matches) {
                if (!cellMatches.has(m.cell)) cellMatches.set(m.cell, []);
                cellMatches.get(m.cell).push(m);
            }
            for (const [cell, cms] of cellMatches) {
                const origHtml = cell.innerHTML;
                const text = cell.textContent;
                // Build new HTML with highlights — work on plain text positions
                // We need to find text positions in the HTML correctly
                this._highlightInCell(cell, text, searchQ, cs);
            }
            this._searchState.current = 0;
            this._codeSearchScrollTo(0);
        }

        const countEl = document.getElementById('cm-search-count');
        if (countEl) {
            countEl.textContent = matches.length > 0
                ? `1/${matches.length}`
                : 'No results';
            countEl.style.color = matches.length > 0 ? '' : '#ef4444';
        }
    },

    _highlightInCell(cell, plainText, query, caseSensitive) {
        const inner = cell.innerHTML;
        const lowerQuery = caseSensitive ? query : query.toLowerCase();
        const compareText = caseSensitive ? plainText : plainText.toLowerCase();

        // Find all match positions in plain text
        const positions = [];
        let idx = 0;
        while (true) {
            const pos = compareText.indexOf(lowerQuery, idx);
            if (pos === -1) break;
            positions.push(pos);
            idx = pos + 1;
        }
        if (!positions.length) return;

        // Map plain text positions to HTML positions
        // Walk through HTML, tracking plain text offset
        let result = '';
        let plainIdx = 0;
        let htmlIdx = 0;
        let posPtr = 0;
        let inTag = false;
        let inEntity = false;

        while (htmlIdx < inner.length && posPtr < positions.length) {
            if (inner[htmlIdx] === '<') { inTag = true; result += inner[htmlIdx]; htmlIdx++; continue; }
            if (inTag) { if (inner[htmlIdx] === '>') inTag = false; result += inner[htmlIdx]; htmlIdx++; continue; }

            if (plainIdx === positions[posPtr]) {
                // Insert highlight start — collect chars matching query length from HTML
                result += '<mark class="cm-search-hl">';
                let matchChars = 0;
                while (matchChars < query.length && htmlIdx < inner.length) {
                    if (inner[htmlIdx] === '<') {
                        // Copy tag through
                        let tagEnd = inner.indexOf('>', htmlIdx);
                        if (tagEnd === -1) tagEnd = inner.length - 1;
                        result += inner.substring(htmlIdx, tagEnd + 1);
                        htmlIdx = tagEnd + 1;
                        continue;
                    }
                    if (inner[htmlIdx] === '&') {
                        // HTML entity — counts as one char
                        let entEnd = inner.indexOf(';', htmlIdx);
                        if (entEnd === -1) entEnd = htmlIdx;
                        result += inner.substring(htmlIdx, entEnd + 1);
                        htmlIdx = entEnd + 1;
                    } else {
                        result += inner[htmlIdx];
                        htmlIdx++;
                    }
                    matchChars++;
                    plainIdx++;
                }
                result += '</mark>';
                posPtr++;
            } else {
                // Copy one char
                if (inner[htmlIdx] === '&') {
                    let entEnd = inner.indexOf(';', htmlIdx);
                    if (entEnd === -1) entEnd = htmlIdx;
                    result += inner.substring(htmlIdx, entEnd + 1);
                    htmlIdx = entEnd + 1;
                } else {
                    result += inner[htmlIdx];
                    htmlIdx++;
                }
                plainIdx++;
            }
        }
        // Copy remaining
        result += inner.substring(htmlIdx);
        cell.innerHTML = result;
    },

    _codeSearchClear() {
        // Remove all highlights
        document.querySelectorAll('#code-modal-body .cm-search-hl').forEach(el => {
            el.outerHTML = el.innerHTML;
        });
        document.querySelectorAll('#code-modal-body .cm-search-active').forEach(el => {
            el.classList.remove('cm-search-active');
        });
        this._searchState.matches = [];
        this._searchState.current = -1;
    },

    _codeSearchNext() {
        const s = this._searchState;
        if (!s.matches.length) return;
        s.current = (s.current + 1) % s.matches.length;
        this._codeSearchScrollTo(s.current);
    },

    _codeSearchPrev() {
        const s = this._searchState;
        if (!s.matches.length) return;
        s.current = (s.current - 1 + s.matches.length) % s.matches.length;
        this._codeSearchScrollTo(s.current);
    },

    _codeSearchScrollTo(idx) {
        const highlights = document.querySelectorAll('#code-modal-body .cm-search-hl');
        if (!highlights.length) return;
        // Remove previous active
        highlights.forEach(el => el.classList.remove('cm-search-active'));
        // Set new active
        if (highlights[idx]) {
            highlights[idx].classList.add('cm-search-active');
            highlights[idx].scrollIntoView({ block: 'center', behavior: 'smooth' });
        }
        const countEl = document.getElementById('cm-search-count');
        if (countEl) countEl.textContent = `${idx + 1}/${this._searchState.matches.length}`;
    },

    /** Build "Used By" sidebar section — reverse references from classes in this JAR */
    _buildUsedBySection(cls, classIdx, esc) {
        const fqn = cls.fullyQualifiedName || '';
        const simpleName = cls.simpleName || '';
        if (!fqn || !JA.app.currentAnalysis) return '';

        // Build set of all names this class can be referenced by:
        // its FQN, simpleName, interfaces it implements, superclass
        const identities = new Set([fqn, simpleName]);
        for (const iface of (cls.interfaces || [])) { identities.add(iface); identities.add(iface.split('.').pop()); }
        if (cls.superClass && cls.superClass !== 'java.lang.Object') {
            identities.add(cls.superClass);
            identities.add(cls.superClass.split('.').pop());
        }

        // Collect: callerClass → Set of method names called on this class
        const callers = new Map(); // callerFqn → { callerCls, calls: [{callerMethod, targetMethod}] }
        const allClasses = JA.app.currentAnalysis.classIndex || JA.app.currentAnalysis.classes || [];

        for (const other of allClasses) {
            if (other.fullyQualifiedName === fqn) continue;
            for (const m of (other.methods || [])) {
                for (const inv of (m.invocations || [])) {
                    if (!identities.has(inv.ownerClass) && !identities.has((inv.ownerClass || '').split('.').pop())) continue;
                    if (inv.methodName === '<init>' || inv.methodName === '<clinit>') continue;
                    if (!callers.has(other.fullyQualifiedName)) {
                        callers.set(other.fullyQualifiedName, { callerCls: other, calls: [] });
                    }
                    const entry = callers.get(other.fullyQualifiedName);
                    // Deduplicate caller.method → target.method pairs
                    const key = m.name + '→' + inv.methodName;
                    if (!entry.calls.some(c => c.callerMethod + '→' + c.targetMethod === key)) {
                        entry.calls.push({ callerMethod: m.name, targetMethod: inv.methodName });
                    }
                }
            }
        }

        if (!callers.size) return '';

        let html = '<div class="cm-sb-section"><div class="cm-sb-title cm-sb-used-by">Used By (' + callers.size + ')</div>';
        html += `<input type="text" class="cm-sb-filter" placeholder="Filter callers..." oninput="JA.summary._filterSbSection(this)">`;
        html += '<div class="cm-sb-scroll">';
        const sorted = [...callers.entries()].sort((a, b) => b[1].calls.length - a[1].calls.length);
        for (const [callerFqn, info] of sorted) {
            const callerName = info.callerCls.simpleName || callerFqn.split('.').pop();
            const safeFqn = callerFqn.replace(/'/g, "\\'");
            const callSummary = info.calls.slice(0, 5).map(c => c.callerMethod + '() → ' + c.targetMethod + '()').join('\n');
            const more = info.calls.length > 5 ? '\n+' + (info.calls.length - 5) + ' more' : '';
            const firstCall = info.calls[0];
            const safeMethod = (firstCall.callerMethod || '').replace(/'/g, "\\'");
            html += `<div class="cm-sb-item cm-sb-click cm-sb-usedby-item" onclick="JA.summary.showClassCode('${safeFqn}','${safeMethod}')" title="Jump to ${esc(callerName)}.${esc(firstCall.callerMethod)}()\n${esc(callSummary + more)}">`;
            html += `<span class="cm-sb-imp">${esc(callerName)}</span>`;
            html += `<span class="cm-sb-line">.${esc(firstCall.callerMethod)}</span>`;
            html += `<span class="cm-sb-usedby-count">${info.calls.length}</span>`;
            html += '</div>';
        }
        html += '</div></div>';
        return html;
    },

    /** Filter sidebar section items by text input */
    _filterSbSection(input) {
        const q = (input.value || '').toLowerCase().trim();
        const scroll = input.nextElementSibling;
        if (!scroll || !scroll.classList.contains('cm-sb-scroll')) return;
        scroll.querySelectorAll('.cm-sb-item').forEach(item => {
            item.style.display = !q || item.textContent.toLowerCase().includes(q) ? '' : 'none';
        });
    },

    _copyCode() {
        const entry = this._codeStack[this._codeIdx];
        if (!entry) return;
        const text = entry.source || this._reconstructJavaFallback(entry.cls).join('\n');
        navigator.clipboard.writeText(text).then(
            () => JA.toast ? JA.toast.success('Copied to clipboard', 2000) : null,
            () => JA.toast ? JA.toast.warn('Copy failed') : null
        );
    },

    _reconstructJavaFallback(cls) {
        const lines = [];
        const access = f => {
            const parts = [];
            if (f & 0x0001) parts.push('public');
            else if (f & 0x0002) parts.push('private');
            else if (f & 0x0004) parts.push('protected');
            if (f & 0x0008) parts.push('static');
            if (f & 0x0010) parts.push('final');
            if (f & 0x0400) parts.push('abstract');
            return parts.join(' ');
        };
        const fieldNameByType = {};
        for (const f of (cls.fields || [])) {
            if (f.type) { fieldNameByType[this._shortType(f.type)] = f.name; fieldNameByType[f.type] = f.name; }
        }

        if (cls.packageName) lines.push('package ' + cls.packageName + ';');
        lines.push('');
        for (const ann of (cls.annotations || [])) {
            let al = '@' + ann.name;
            if (ann.attributes && Object.keys(ann.attributes).length) al += '(' + Object.entries(ann.attributes).map(([k, v]) => k + '=' + JSON.stringify(v)).join(', ') + ')';
            lines.push(al);
        }
        const kw = cls.isInterface ? 'interface' : (cls.isEnum ? 'enum' : 'class');
        let decl = access(cls.accessFlags || 0x0001) + ' ' + kw + ' ' + (cls.simpleName || '');
        if (cls.superClass && cls.superClass !== 'java.lang.Object') decl += ' extends ' + this._shortType(cls.superClass);
        if (cls.interfaces && cls.interfaces.length) decl += ' implements ' + cls.interfaces.map(i => this._shortType(i)).join(', ');
        lines.push(decl + ' {');
        lines.push('');

        for (const f of (cls.fields || []).filter(f => !(f.name || '').startsWith('this$') && !['Logger', 'Log'].includes(this._shortType(f.type || '')))) {
            for (const ann of (f.annotations || [])) lines.push('    @' + ann.name);
            let fl = '    ' + access(f.accessFlags || 0x0002) + ' ' + this._shortType(f.type || 'Object') + ' ' + (f.name || '?');
            if (f.constantValue) fl += ' = "' + f.constantValue + '"';
            lines.push(fl + ';');
        }
        lines.push('');

        for (const m of (cls.methods || []).filter(m => m.name !== '<init>' && m.name !== '<clinit>' && !m.name.startsWith('lambda$') && !((m.accessFlags & 0x1000) !== 0))) {
            for (const ann of (m.annotations || [])) {
                let al = '    @' + ann.name;
                if (ann.attributes && Object.keys(ann.attributes).length) al += '(' + Object.entries(ann.attributes).map(([k, v]) => k + '=' + JSON.stringify(v)).join(', ') + ')';
                lines.push(al);
            }
            const params = (m.parameters || []).map(p => this._shortType(p.type || 'Object') + ' ' + (p.name || 'arg')).join(', ');
            lines.push('    ' + access(m.accessFlags || 0x0001) + ' ' + this._shortType(m.returnType || 'void') + ' ' + m.name + '(' + params + ') {');

            const invsByLine = {};
            for (const inv of (m.invocations || [])) {
                if (inv.methodName === '<init>' || inv.methodName.startsWith('lambda$')) continue;
                const line = inv.lineNumber || 0;
                (invsByLine[line] = invsByLine[line] || []).push(inv);
            }
            const sortedLines = Object.keys(invsByLine).map(Number).sort((a, b) => a - b);
            for (const lineNum of sortedLines) {
                for (const inv of invsByLine[lineNum]) {
                    const ts = (inv.ownerClass || '').split('.').pop();
                    const caller = inv.ownerClass === cls.fullyQualifiedName ? 'this' : (fieldNameByType[ts] || fieldNameByType[inv.ownerClass] || this._toCamelCase(ts));
                    const prefix = lineNum > 0 ? `/*L${lineNum}*/ ` : '';
                    lines.push('        ' + prefix + caller + '.' + inv.methodName + '(...);');
                }
            }

            if (m.fieldAccesses && m.fieldAccesses.length) {
                const fas = m.fieldAccesses.filter(fa => fa.ownerClass === cls.fullyQualifiedName);
                const unique = new Map();
                for (const fa of fas) unique.set(fa.fieldName + ':' + fa.accessType, fa);
                if (unique.size) lines.push('        // Field accesses: ' + [...unique.values()].map(fa => fa.accessType + ' ' + fa.fieldName).join(', '));
            }

            if (m.localVariables && Object.keys(m.localVariables).length) {
                lines.push('        // Local vars: ' + Object.entries(m.localVariables).map(([n, t]) => this._shortType(t) + ' ' + n).join(', '));
            }

            lines.push('    }');
            lines.push('');
        }
        lines.push('}');
        return lines;
    }
});
