/**
 * Code Structure tab — lazy-rendered tree.
 * Only package+class headers are rendered initially.
 * Class contents (fields/methods) render on expand.
 * Method invocations render on expand.
 * Keeps DOM small even with 8000+ classes.
 */
window.JA = window.JA || {};

JA.codeTree = {

    _classes: [],       // full class data (stored for lazy render)
    _history: [],
    _historyIndex: -1,
    _viewMode: 'package', // 'package' | 'project' | 'visual'

    _loaded: false,
    _resources: [],
    _resourcesLoaded: false,
    _bundledJars: [],
    _bundledJarsLoaded: false,
    _jarDepMap: {},
    _jarDepMapLoaded: false,
    _bundledContentCache: [],

    /* ---- public API ---- */

    setLazy() {
        this._classes = [];
        this._loaded = false;
        this._resources = [];
        this._resourcesLoaded = false;
        this._bundledJars = [];
        this._bundledJarsLoaded = false;
        this._jarDepMap = {};
        this._jarDepMapLoaded = false;
        this._bundledContentCache = [];
        const container = document.getElementById('class-tree');
        if (container) container.innerHTML = '<p style="padding:20px;color:var(--text-muted,#6b7280)">Switch to Code Structure tab to load classes</p>';
    },

    async loadAndRender() {
        if (this._loaded) return;
        const container = document.getElementById('class-tree');
        if (container) container.innerHTML = '<p style="padding:20px;color:var(--text-muted,#6b7280)">Loading code structure...</p>';
        try {
            const jarId = JA.app.currentJarId;
            const version = JA.app._currentVersion || undefined;
            const classTree = await JA.api.getClassTree(jarId, version);
            this.render(classTree);
            this._loaded = true;
        } catch (e) {
            console.error('Failed to load class tree:', e);
            if (container) container.innerHTML = '<p style="padding:20px;color:#ef4444">Failed to load code structure: ' + (e.message || e) + '</p>';
        }
        this._loadResources();
        this._loadBundledJars();
        this._loadJarDepMap();
    },

    async _loadResources() {
        if (this._resourcesLoaded) return;
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/resources', { cache: 'no-store' });
            if (!res.ok) return;
            this._resources = await res.json();
            this._resourcesLoaded = true;
            this._appendResourcesSection();
        } catch (e) {
            console.warn('Failed to load resource files:', e);
        }
    },

    _appendResourcesSection() {
        if (!this._resources || !this._resources.length) return;
        const container = document.getElementById('class-tree');
        if (!container) return;
        const existing = container.querySelector('.resources-section');
        if (existing) existing.remove();

        const grouped = {};
        for (const f of this._resources) {
            const ext = f.filename.includes('.') ? f.filename.slice(f.filename.lastIndexOf('.')).toLowerCase() : 'other';
            (grouped[ext] = grouped[ext] || []).push(f);
        }

        const extIcon = ext => {
            if (ext === '.properties') return '📄';
            if (ext === '.yml' || ext === '.yaml') return '📋';
            if (ext === '.json') return '📊';
            if (ext === '.xml') return '📝';
            if (ext === '.sql') return '🗄️';
            return '📃';
        };

        const esc = JA.utils.escapeHtml;
        let inner = '';
        const sortedExts = Object.keys(grouped).sort();
        for (const ext of sortedExts) {
            const files = grouped[ext];
            let fileItems = '';
            for (const f of files) {
                const safeName = f.filename.replace(/'/g, "\\'");
                fileItems += `<div class="tree-node tree-resource-file">
                    <div class="tree-leaf" style="cursor:pointer" onclick="JA.codeTree._openResourceFile('${esc(safeName)}')">
                        <span class="leaf-icon" style="margin-right:4px">${extIcon(ext)}</span>
                        <span class="resource-filename">${esc(f.filename)}</span>
                        <span class="tree-count" style="margin-left:auto">${JA.utils.formatSize ? JA.utils.formatSize(f.size) : f.size + 'B'}</span>
                    </div>
                </div>`;
            }
            const label = ext === 'other' ? 'other' : ext.slice(1);
            inner += `<div class="tree-node">
                <div class="tree-toggle expanded" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span style="margin-right:4px">${extIcon(ext)}</span>
                    <strong class="pkg-name">${esc(label)}</strong>
                    <span class="tree-count">${files.length}</span>
                </div>
                <div class="tree-children">${fileItems}</div>
            </div>`;
        }

        const section = document.createElement('div');
        section.className = 'tree-node resources-section';
        section.innerHTML = `<div class="jar-separator" style="margin-top:12px">Resources (${this._resources.length} files)</div>
            <div class="tree-node">
                <div class="tree-toggle expanded" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="jar-group-icon">&#128193;</span>
                    <strong class="jar-group-name">Resource Files</strong>
                    <span class="tree-count">${this._resources.length}</span>
                </div>
                <div class="tree-children">${inner}</div>
            </div>`;
        container.appendChild(section);
    },

    async _openResourceFile(filename) {
        const panel = document.getElementById('code-panel');
        if (!panel) return;
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        panel.innerHTML = '<div class="cp-loading"><div class="cm-spinner"></div><div>Loading...</div></div>';
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/resources/' + encodeURIComponent(filename));
            if (!res.ok) throw new Error('Not found');
            const text = await res.text();
            const esc = s => (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            panel.innerHTML = `<div class="cm-info-bar">
                <span class="cm-classname">${esc(filename)}</span>
                <span class="cm-src-badge cm-badge-recon">RESOURCE</span>
                <button class="btn-sm cm-copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('res-content-pre').textContent)" title="Copy to clipboard">Copy</button>
            </div>
            <div class="cm-content" style="padding:12px;overflow:auto;height:calc(100% - 40px);background:#1e1e2e">
                <pre id="res-content-pre" style="margin:0;white-space:pre-wrap;font-size:12px;font-family:monospace;tab-size:2;color:#cdd6f4">${esc(text)}</pre>
            </div>`;
        } catch (e) {
            panel.innerHTML = '<div style="padding:20px;color:#ef4444">Failed to load resource: ' + (e.message || e) + '</div>';
        }
    },

    render(classes) {
        this._classes = classes;
        this._loaded = true;
        this._history = [];
        this._historyIndex = -1;
        this._renderCurrentView();
        this._updateNavState();
        if (JA.codeTreePanel) JA.codeTreePanel.clear();
        if (JA.codeTreeViews) JA.codeTreeViews.initJarSourceFilter(classes);
        if (this._resourcesLoaded) this._appendResourcesSection();
        if (this._bundledJarsLoaded) this._appendBundledJarsSection();
        if (this._jarDepMapLoaded) this._appendJarDepMapSection();
    },

    setViewMode(mode) {
        this._viewMode = mode;
        document.querySelectorAll('.view-mode-btn').forEach(b => {
            b.classList.toggle('active', b.dataset.mode === mode);
        });
        const treeButtons = document.getElementById('tree-view-actions');
        if (treeButtons) treeButtons.style.display = mode === 'visual' ? 'none' : '';
        if (JA.codeTreeViews && JA.codeTreeViews._activeJarSources !== null) {
            JA.codeTreeViews._applyJarSourceFilter();
        } else {
            this._renderCurrentView();
        }
        if (this._resourcesLoaded) this._appendResourcesSection();
        if (this._bundledJarsLoaded) this._appendBundledJarsSection();
        if (this._jarDepMapLoaded) this._appendJarDepMapSection();
    },

    _renderCurrentView() {
        const container = document.getElementById('class-tree');
        if (!container || !this._classes.length) return;

        if (this._viewMode === 'project') {
            JA.codeTreeViews.renderProjectView(this._classes, container);
        } else if (this._viewMode === 'visual') {
            JA.codeTreeViews.renderVisualView(this._classes, container);
        } else {
            this._renderPackageView(container);
        }
    },

    _renderPackageView(container) {
        const { main, deps } = this._groupBySourceJar(this._classes);
        let html = '';

        // Main app classes
        if (main.length) {
            const packages = this._groupByPackage(main);
            const sorted = Object.keys(packages).sort();
            for (const pkg of sorted) {
                const pkgClasses = packages[pkg].sort((a, b) => a.simpleName.localeCompare(b.simpleName));
                html += this._renderPackage(pkg, pkgClasses);
            }
        }

        // Dependency JARs
        const jarNames = Object.keys(deps).sort();
        if (jarNames.length) {
            html += '<div class="jar-separator">Internal Dependencies (' + jarNames.length + ' JARs)</div>';
            for (const jarName of jarNames) {
                const jarClasses = deps[jarName];
                const project = JA.summary._jarToProject ? JA.summary._jarToProject(jarName) : jarName;
                const domain = JA.summary._jarToDomain ? JA.summary._jarToDomain(jarName) : '';
                const packages = this._groupByPackage(jarClasses);
                const sorted = Object.keys(packages).sort();
                let inner = '';
                for (const pkg of sorted) {
                    const pkgClasses = packages[pkg].sort((a, b) => a.simpleName.localeCompare(b.simpleName));
                    inner += this._renderPackage(pkg, pkgClasses);
                }
                html += `<div class="tree-node jar-group" data-jar="${JA.utils.escapeHtml(jarName)}">
                    <div class="tree-toggle jar-group-toggle" onclick="JA.codeTree.togglePkg(this)">
                        <span class="arrow">&#9654;</span>
                        <span class="jar-group-icon">&#128230;</span>
                        <strong class="jar-group-name">${JA.utils.escapeHtml(project)}</strong>
                        ${domain ? '<span class="sum-domain-tag" style="font-size:9px">' + JA.utils.escapeHtml(domain) + '</span>' : ''}
                        <span class="tree-count">${jarClasses.length} classes</span>
                    </div>
                    <div class="tree-children collapsed">${inner}</div>
                </div>`;
            }
        }

        container.innerHTML = html || '<p class="empty-state">No classes found</p>';
    },

    _renderFilteredPackageView(container, classes) {
        const { main, deps } = this._groupBySourceJar(classes);
        let html = '';
        if (main.length) {
            const packages = this._groupByPackage(main);
            const sorted = Object.keys(packages).sort();
            for (const pkg of sorted) {
                const pkgClasses = packages[pkg].sort((a, b) => a.simpleName.localeCompare(b.simpleName));
                html += this._renderPackage(pkg, pkgClasses);
            }
        }
        const jarNames = Object.keys(deps).sort();
        if (jarNames.length) {
            html += '<div class="jar-separator">Internal Dependencies (' + jarNames.length + ' JARs)</div>';
            for (const jarName of jarNames) {
                const jarClasses = deps[jarName];
                const project = JA.summary._jarToProject ? JA.summary._jarToProject(jarName) : jarName;
                const domain = JA.summary._jarToDomain ? JA.summary._jarToDomain(jarName) : '';
                const packages = this._groupByPackage(jarClasses);
                const sorted = Object.keys(packages).sort();
                let inner = '';
                for (const pkg of sorted) {
                    const pkgClasses = packages[pkg].sort((a, b) => a.simpleName.localeCompare(b.simpleName));
                    inner += this._renderPackage(pkg, pkgClasses);
                }
                html += `<div class="tree-node jar-group" data-jar="${JA.utils.escapeHtml(jarName)}">
                    <div class="tree-toggle jar-group-toggle" onclick="JA.codeTree.togglePkg(this)">
                        <span class="arrow">&#9654;</span>
                        <span class="jar-group-icon">&#128230;</span>
                        <strong class="jar-group-name">${JA.utils.escapeHtml(project)}</strong>
                        ${domain ? '<span class="sum-domain-tag" style="font-size:9px">' + JA.utils.escapeHtml(domain) + '</span>' : ''}
                        <span class="tree-count">${jarClasses.length} classes</span>
                    </div>
                    <div class="tree-children collapsed">${inner}</div>
                </div>`;
            }
        }
        container.innerHTML = html || '<p class="empty-state">No classes match the selected sources</p>';
    },

    _groupBySourceJar(classes) {
        const main = [];
        const deps = {};
        for (const cls of classes) {
            if (!cls.sourceJar) {
                main.push(cls);
            } else {
                (deps[cls.sourceJar] = deps[cls.sourceJar] || []).push(cls);
            }
        }
        return { main, deps };
    },

    /* ---- lazy expand ---- */

    togglePkg(el) {
        const children = el.nextElementSibling;
        if (!children) return;
        el.classList.toggle('expanded');
        children.classList.toggle('collapsed');
    },

    toggleClass(el, classIndex) {
        const children = el.nextElementSibling;
        if (!children) return;

        // Lazy render: if children are empty, render fields+methods now
        if (!children.dataset.rendered) {
            const cls = this._classes[classIndex];
            if (cls) {
                children.innerHTML = this._renderClassBody(cls, classIndex);
                children.dataset.rendered = '1';
            }
        }

        el.classList.toggle('expanded');
        children.classList.toggle('collapsed');

        // Show code in inline panel when expanding
        if (el.classList.contains('expanded') && JA.codeTreePanel) {
            const cls = this._classes[classIndex];
            if (cls) JA.codeTreePanel.show(cls.fullyQualifiedName || cls.simpleName);
        }
    },

    toggleMethod(el, classIndex, methodIndex) {
        const children = el.nextElementSibling;
        if (!children) return;

        // Lazy render invocations
        if (!children.dataset.rendered) {
            const cls = this._classes[classIndex];
            if (cls) {
                const method = cls.methods[methodIndex];
                if (method) {
                    children.innerHTML = this._renderInvocations(method);
                    children.dataset.rendered = '1';
                }
            }
        }

        el.classList.toggle('expanded');
        children.classList.toggle('collapsed');
    },

    expandAll() {
        // Only expand already-rendered nodes
        document.querySelectorAll('.class-tree .tree-toggle').forEach(t => t.classList.add('expanded'));
        document.querySelectorAll('.class-tree .tree-children').forEach(c => c.classList.remove('collapsed'));
    },

    collapseAll() {
        document.querySelectorAll('.class-tree .tree-toggle').forEach(t => t.classList.remove('expanded'));
        document.querySelectorAll('.class-tree .tree-children').forEach(c => c.classList.add('collapsed'));
    },

    filter(query) {
        if (this._viewMode === 'project') {
            JA.codeTreeViews.filterProjectView(query);
            return;
        }
        if (this._viewMode === 'visual') {
            JA.codeTreeViews.filterVisualView(query);
            return;
        }

        const q = (query || '').toLowerCase().trim();
        const tree = document.querySelector('.class-tree');
        if (!tree) return;

        // Reset visibility
        if (!q) {
            tree.querySelectorAll('.tree-node').forEach(n => n.style.display = '');
            tree.querySelectorAll('.jar-separator').forEach(n => n.style.display = '');
            return;
        }

        // Filter class nodes matching query
        const classNodes = tree.querySelectorAll('.tree-node[data-class-name]');
        const visiblePkgs = new Set();
        const visibleJarGroups = new Set();

        classNodes.forEach(cls => {
            const text = cls.getAttribute('data-search') || '';
            if (text.includes(q)) {
                cls.style.display = '';
                // Expand parent package
                let parent = cls.parentElement;
                while (parent && parent !== tree) {
                    if (parent.classList.contains('tree-children')) {
                        parent.classList.remove('collapsed');
                        const t = parent.previousElementSibling;
                        if (t && t.classList.contains('tree-toggle')) t.classList.add('expanded');
                    }
                    if (parent.classList.contains('tree-node') && !parent.hasAttribute('data-class-name')) {
                        visiblePkgs.add(parent);
                        if (parent.classList.contains('jar-group')) visibleJarGroups.add(parent);
                    }
                    parent = parent.parentElement;
                }
            } else {
                cls.style.display = 'none';
            }
        });

        // Hide packages / jar groups with no matches
        tree.querySelectorAll(':scope > .tree-node, :scope > .tree-node .tree-node:not([data-class-name])').forEach(node => {
            if (node.hasAttribute('data-class-name')) return;
            node.style.display = visiblePkgs.has(node) ? '' : 'none';
        });

        // Show jar separator only if any jar groups visible
        tree.querySelectorAll('.jar-separator').forEach(sep => {
            sep.style.display = visibleJarGroups.size > 0 ? '' : 'none';
        });
    },

    /* ---- navigation ---- */

    navigateTo(className, methodName, el) {
        if (this._historyIndex < this._history.length - 1) {
            this._history = this._history.slice(0, this._historyIndex + 1);
        }
        this._history.push({ className, methodName, element: el });
        this._historyIndex = this._history.length - 1;
        this._highlightAndScroll(el);
        this._updateNavState();
        if (JA.codeTreePanel) JA.codeTreePanel.show(className, methodName);
    },

    findAndHighlight(className, methodName) {
        setTimeout(() => {
            const tree = document.getElementById('class-tree');
            if (!tree) return;

            // First, find the class node and expand it (triggers lazy render)
            const classNodes = tree.querySelectorAll('.tree-node[data-class-name]');
            let classNode = null;
            for (const node of classNodes) {
                if (node.getAttribute('data-class-name') === className) {
                    classNode = node;
                    break;
                }
            }

            if (classNode) {
                // Expand the class to trigger lazy rendering of methods
                const toggle = classNode.querySelector(':scope > .tree-toggle');
                const children = classNode.querySelector(':scope > .tree-children');
                if (toggle && children && !children.dataset.rendered) {
                    const idx = parseInt(classNode.getAttribute('data-class-index'), 10);
                    this.toggleClass(toggle, idx);
                } else if (toggle && children) {
                    toggle.classList.add('expanded');
                    children.classList.remove('collapsed');
                }

                // Expand parent package
                let parent = classNode.parentElement;
                while (parent) {
                    if (parent.classList && parent.classList.contains('tree-children')) {
                        parent.classList.remove('collapsed');
                        const t = parent.previousElementSibling;
                        if (t && t.classList.contains('tree-toggle')) t.classList.add('expanded');
                    }
                    parent = parent.parentElement;
                }

                // Now find the method inside the expanded class
                setTimeout(() => {
                    const methods = classNode.querySelectorAll('.tree-method');
                    let found = null;
                    for (const m of methods) {
                        if (m.getAttribute('data-method') === methodName) { found = m; break; }
                    }
                    if (!found && methods.length > 0) found = methods[0];

                    if (found) {
                        const clickEl = found.querySelector('.tree-method-click');
                        this.navigateTo(className, methodName, clickEl || found);
                        JA.toast.info('Jumped to ' + className + '.' + methodName, 2000);
                    } else {
                        // Scroll to class at least
                        classNode.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        JA.toast.info('Jumped to class ' + className, 2000);
                    }
                }, 50);
            } else {
                JA.toast.warn('Class not found: ' + className, 3000);
            }
        }, 150);
    },

    jumpToInvocation(ownerClass, methodName, event) {
        JA.nav.goTo(ownerClass, methodName, 'structure', event);
    },

    goToEndpointFlow(httpMethod, path) {
        switchTab('endpoints');
        JA.toast.info('Switching to Endpoint Flow...', 1500);
        setTimeout(() => {
            const cards = document.querySelectorAll('.endpoint-card');
            for (const card of cards) {
                const methodSpan = card.querySelector('.endpoint-method');
                const pathSpan = card.querySelector('.endpoint-path');
                if (methodSpan && pathSpan) {
                    if (methodSpan.textContent.trim() === httpMethod && pathSpan.textContent.trim() === path) {
                        card.click();
                        card.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        JA.toast.success('Showing flow for ' + httpMethod + ' ' + path, 2000);
                        return;
                    }
                }
            }
            JA.toast.warn('Endpoint not found', 3000);
        }, 200);
    },

    goBack() {
        if (this._historyIndex <= 0) return;
        this._historyIndex--;
        const entry = this._history[this._historyIndex];
        this._highlightAndScroll(entry.element);
        this._updateNavState();
        if (JA.codeTreePanel) JA.codeTreePanel.show(entry.className, entry.methodName);
    },

    goForward() {
        if (this._historyIndex >= this._history.length - 1) return;
        this._historyIndex++;
        const entry = this._history[this._historyIndex];
        this._highlightAndScroll(entry.element);
        this._updateNavState();
        if (JA.codeTreePanel) JA.codeTreePanel.show(entry.className, entry.methodName);
    },

    goToHistoryEntry(index) {
        if (index < 0 || index >= this._history.length) return;
        this._historyIndex = index;
        const entry = this._history[this._historyIndex];
        this._highlightAndScroll(entry.element);
        this._updateNavState();
        this.toggleHistory();
        if (JA.codeTreePanel) JA.codeTreePanel.show(entry.className, entry.methodName);
    },

    toggleHistory() {
        const panel = document.getElementById('nav-history-panel');
        if (!panel) return;
        if (panel.style.display === 'none') {
            this._renderHistoryPanel();
            panel.style.display = '';
        } else {
            panel.style.display = 'none';
        }
    },

    _highlightAndScroll(el) {
        if (!el) return;
        document.querySelectorAll('.tree-node.nav-highlight').forEach(n => n.classList.remove('nav-highlight'));
        const node = el.closest('.tree-node');
        if (!node) return;

        let parent = node.parentElement;
        while (parent) {
            if (parent.classList && parent.classList.contains('tree-children')) {
                parent.classList.remove('collapsed');
                const t = parent.previousElementSibling;
                if (t && t.classList.contains('tree-toggle')) t.classList.add('expanded');
            }
            parent = parent.parentElement;
        }

        node.classList.add('nav-highlight');
        node.scrollIntoView({ behavior: 'smooth', block: 'center' });
    },

    _updateNavState() {
        const backBtn = document.getElementById('nav-back');
        const fwdBtn = document.getElementById('nav-forward');
        const crumb = document.getElementById('nav-breadcrumb');
        const histBtn = document.getElementById('nav-history-toggle');

        if (backBtn) backBtn.disabled = this._historyIndex <= 0;
        if (fwdBtn) fwdBtn.disabled = this._historyIndex >= this._history.length - 1;

        if (crumb) {
            if (this._historyIndex >= 0) {
                const entry = this._history[this._historyIndex];
                crumb.innerHTML = `<span class="crumb-current">${JA.utils.escapeHtml(entry.className + '.' + entry.methodName)}</span>`;
            } else {
                crumb.innerHTML = '';
            }
        }

        if (histBtn) {
            histBtn.textContent = this._history.length > 0 ? `History (${this._history.length})` : 'History';
        }
    },

    _renderHistoryPanel() {
        const panel = document.getElementById('nav-history-panel');
        if (!panel) return;

        if (this._history.length === 0) {
            panel.innerHTML = '<div class="history-empty">No navigation history yet</div>';
            return;
        }

        let html = '<div class="history-list">';
        for (let i = this._history.length - 1; i >= 0; i--) {
            const entry = this._history[i];
            const active = i === this._historyIndex ? ' history-active' : '';
            html += `<div class="history-item${active}" onclick="JA.codeTree.goToHistoryEntry(${i})">
                <span class="history-idx">${i + 1}</span>
                <span class="history-label">${JA.utils.escapeHtml(entry.className + '.' + entry.methodName)}</span>
            </div>`;
        }
        html += '</div>';
        panel.innerHTML = html;
    },

    /* ---- render helpers ---- */

    _groupByPackage(classes) {
        const map = {};
        for (const cls of classes) {
            const pkg = cls.packageName || '(default)';
            (map[pkg] = map[pkg] || []).push(cls);
        }
        return map;
    },

    /** Render package header + collapsed class headers (no method/field details yet) */
    _renderPackage(pkg, classes) {
        const esc = JA.utils.escapeHtml;
        let classHeaders = '';
        for (const cls of classes) {
            classHeaders += this._renderClassHeader(cls);
        }

        return `
            <div class="tree-node" data-search="${pkg.toLowerCase()}">
                <div class="tree-toggle expanded" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="pkg-icon">&#128230;</span>
                    <strong class="pkg-name">${esc(pkg)}</strong>
                    <span class="tree-count">${classes.length}</span>
                </div>
                <div class="tree-children">
                    ${classHeaders}
                </div>
            </div>`;
    },

    /** Render just the class toggle header — body is empty until user expands */
    _renderClassHeader(cls) {
        const esc = JA.utils.escapeHtml;
        const stereotype = cls.stereotype || 'OTHER';
        const badgeCls = JA.utils.stereotypeBadgeClass(stereotype);

        const typeBadges = [];
        if (cls.isInterface) typeBadges.push('<span class="badge badge-interface">IF</span>');
        else if (cls.isAbstract) typeBadges.push('<span class="badge badge-abstract">ABS</span>');
        if (cls.isEnum) typeBadges.push('<span class="badge badge-enum">ENUM</span>');
        typeBadges.push(`<span class="badge ${badgeCls}">${stereotype.replace('REST_', '')}</span>`);

        const annotations = cls.annotations || [];
        const stereoAnns = annotations
            .filter(a => ['RestController','Controller','Service','Repository','Component','Configuration','Entity'].includes(a.name))
            .map(a => `<span class="method-ann">@${a.name}</span>`).join(' ');

        const searchKey = [
            cls.fullyQualifiedName,
            cls.simpleName,
            annotations.map(a => a.name).join(' '),
            (cls.methods || []).map(m => m.name).join(' ')
        ].join(' ').toLowerCase();

        const classIcon = cls.isInterface ? '&#9671;' : cls.isEnum ? '&#9670;' : '&#9632;';
        const memberCount = (cls.fields || []).length + (cls.methods || []).length;
        const classIndex = this._classes.indexOf(cls);

        return `
            <div class="tree-node" data-search="${searchKey}" data-class-name="${esc(cls.simpleName)}" data-class-index="${classIndex}">
                <div class="tree-toggle" onclick="JA.codeTree.toggleClass(this, ${classIndex})">
                    <span class="arrow">&#9654;</span>
                    <span class="class-icon" style="color:${JA.utils.stereotypeColor(stereotype)}">${classIcon}</span>
                    ${stereoAnns}
                    <strong class="class-name">${esc(cls.simpleName)}</strong>
                    ${typeBadges.join('')}
                    <span class="tree-count">${memberCount}</span>
                </div>
                <div class="tree-children collapsed"></div>
            </div>`;
    },

    /** Render class body (fields + methods) — called lazily on expand */
    _renderClassBody(cls, classIndex) {
        const esc = JA.utils.escapeHtml;
        const fields = cls.fields || [];
        const methods = cls.methods || [];
        let html = '';

        // Implementations section for interfaces / abstract classes (IntelliJ Ctrl+Alt+B style)
        if (cls.isInterface || cls.isAbstract) {
            if (JA.nav && !JA.nav._implMap) JA.nav.init();
            const fqn = cls.fullyQualifiedName || '';
            const impls = (JA.nav && JA.nav._implMap)
                ? (JA.nav._implMap[fqn] || JA.nav._implMap[cls.simpleName] || [])
                : [];
            if (impls.length > 0) {
                html += '<div class="tree-section-label tree-impls-label">Implementations (' + impls.length + ')</div>';
                for (const impl of impls) {
                    const color = JA.utils.stereotypeColor(impl.stereotype);
                    const stereo = (impl.stereotype || '').replace('REST_', '');
                    const safeName = (impl.simpleName || '').replace(/'/g, "\\'");
                    html += `<div class="tree-node tree-impl-item">
                        <div class="tree-leaf" onclick="JA.codeTree.findAndHighlight('${safeName}','')" style="cursor:pointer">
                            <span class="leaf-icon" style="color:${color}">&#9658;</span>
                            <span class="impl-name" style="color:${color};font-weight:600">${esc(impl.simpleName || '?')}</span>
                            ${stereo ? `<span class="badge badge-xs" style="color:${color};border-color:${color};opacity:0.8">${esc(stereo)}</span>` : ''}
                            <span class="cg-src-btn code-view-btn" onclick="event.stopPropagation();JA.summary.showClassCode('${(impl.fullyQualifiedName||impl.simpleName||'').replace(/'/g,"\\'")}','')" title="View decompiled code">Code</span>
                        </div>
                    </div>`;
                }
            }
        }

        if (fields.length > 0) {
            html += '<div class="tree-section-label">Fields</div>';
            for (const f of fields) {
                const anns = (f.annotations || []).map(a => `<span class="method-ann">@${a.name}</span>`).join(' ');
                html += `<div class="tree-node tree-field">
                    <div class="tree-leaf">
                        <span class="leaf-icon field-icon">F</span>
                        ${anns}
                        <span class="field-type">${esc(f.type)}</span>
                        <span class="field-name">${esc(f.name)}</span>
                    </div>
                </div>`;
            }
        }

        if (methods.length > 0) {
            html += '<div class="tree-section-label">Methods</div>';
            for (let mi = 0; mi < methods.length; mi++) {
                html += this._renderMethodHeader(methods[mi], cls, classIndex, mi);
            }
        }

        return html;
    },

    /** Render method header — invocations are empty until user expands */
    _renderMethodHeader(method, cls, classIndex, methodIndex) {
        const esc = JA.utils.escapeHtml;
        const anns = (method.annotations || []).map(a => {
            let text = '@' + a.name;
            const attrs = a.attributes || {};
            const keys = Object.keys(attrs);
            if (keys.length > 0) {
                const vals = keys.map(k => k + '=' + JSON.stringify(attrs[k])).join(', ');
                text += '(' + vals + ')';
            }
            return `<span class="method-ann">${esc(text)}</span>`;
        }).join(' ');

        const params = (method.parameters || []).map(p => {
            const pAnns = (p.annotations || []).map(a => '@' + a.name + ' ').join('');
            return pAnns + p.type + ' ' + p.name;
        }).join(', ');

        const displayName = method.name === '<init>' ? 'constructor' : method.name;
        const className = cls ? cls.simpleName : '';

        const httpBadge = method.httpMethod
            ? `<span class="endpoint-method method-${method.httpMethod}" style="font-size:9px;padding:1px 5px">${method.httpMethod}</span>`
            : '';

        const fqn = cls ? (cls.fullyQualifiedName || cls.simpleName) : '';
        const safeFqn = fqn.replace(/'/g, "\\'");
        const safeDisplay = displayName.replace(/'/g, "\\'");
        const codeBtn = `<span class="cg-src-btn code-view-btn" onclick="event.stopPropagation();JA.codeTreePanel.show('${esc(safeFqn)}','${esc(safeDisplay)}')" title="View decompiled code">Code</span>`;

        const invocations = (method.invocations || []).filter(inv =>
            inv.methodName !== '<init>' && inv.methodName !== '<clinit>'
        );
        const hasInvocations = invocations.length > 0;

        return `
            <div class="tree-node tree-method" data-method="${esc(displayName)}" data-class="${esc(className)}">
                <div class="${hasInvocations ? 'tree-toggle' : 'tree-leaf'} tree-method-click"
                     onclick="${hasInvocations ? 'JA.codeTree.toggleMethod(this,' + classIndex + ',' + methodIndex + ');' : ''}JA.codeTree.navigateTo('${esc(className)}','${esc(displayName)}',this)">
                    ${hasInvocations ? '<span class="arrow">&#9654;</span>' : ''}
                    <span class="leaf-icon method-icon">M</span>
                    <span class="method-sig">
                        ${anns} ${httpBadge}
                        <span class="method-return">${esc(method.returnType)}</span>
                        <span class="method-name">${esc(displayName)}</span>(<span class="method-params">${esc(params)}</span>)
                    </span>
                    ${hasInvocations ? '<span class="tree-count">' + invocations.length + ' calls</span>' : ''}
                    ${codeBtn}
                </div>
                ${hasInvocations ? '<div class="tree-children collapsed"></div>' : ''}
            </div>`;
    },

    async _loadBundledJars() {
        if (this._bundledJarsLoaded) return;
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/bundled-jars', { cache: 'no-store' });
            if (!res.ok) return;
            this._bundledJars = await res.json();
            this._bundledJarsLoaded = true;
            this._appendBundledJarsSection();
        } catch (e) {
            console.warn('Failed to load bundled JARs:', e);
        }
    },

    async _loadJarDepMap() {
        if (this._jarDepMapLoaded) return;
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/jar-dep-map', { cache: 'no-store' });
            if (!res.ok) return;
            this._jarDepMap = await res.json();
            this._jarDepMapLoaded = true;
            this._appendJarDepMapSection();
        } catch (e) {
            console.warn('Failed to load JAR dep map:', e);
        }
    },

    _appendBundledJarsSection() {
        if (!this._bundledJars || !this._bundledJars.length) return;
        const container = document.getElementById('class-tree');
        if (!container) return;
        const existing = container.querySelector('.bundled-jars-section');
        if (existing) existing.remove();

        this._bundledContentCache = [];

        const esc = JA.utils.escapeHtml;
        const extIcon = fname => {
            const lower = fname.toLowerCase();
            if (lower.endsWith('.properties')) return '📄';
            if (lower.endsWith('.yml') || lower.endsWith('.yaml')) return '📋';
            if (lower.endsWith('.json')) return '📊';
            if (lower.endsWith('.xml')) return '📝';
            return '📃';
        };
        const formatSize = bytes => {
            if (bytes >= 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
            if (bytes >= 1024) return (bytes / 1024).toFixed(1) + ' KB';
            return bytes + ' B';
        };
        const cacheContent = (jarName, filename, content) => {
            const idx = this._bundledContentCache.length;
            this._bundledContentCache.push({ jarName, filename, content });
            return idx;
        };

        let jarNodes = '';
        for (const j of this._bundledJars) {
            let innerItems = '';
            if (j.manifest) {
                const idx = cacheContent(j.name, 'MANIFEST.MF', j.manifest);
                innerItems += `<div class="tree-node tree-resource-file">
                    <div class="tree-leaf" style="cursor:pointer" onclick="JA.codeTree._openBundledContentByIdx(${idx})">
                        <span class="leaf-icon" style="margin-right:4px">📋</span>
                        <span class="resource-filename">MANIFEST.MF</span>
                    </div>
                </div>`;
            }
            const resourceKeys = Object.keys(j.resources || {});
            for (const fname of resourceKeys) {
                const idx = cacheContent(j.name, fname, j.resources[fname]);
                innerItems += `<div class="tree-node tree-resource-file">
                    <div class="tree-leaf" style="cursor:pointer" onclick="JA.codeTree._openBundledContentByIdx(${idx})">
                        <span class="leaf-icon" style="margin-right:4px">${extIcon(fname)}</span>
                        <span class="resource-filename">${esc(fname)}</span>
                    </div>
                </div>`;
            }
            const hasChildren = j.manifest || resourceKeys.length > 0;
            jarNodes += `<div class="tree-node">
                <div class="tree-toggle" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span style="margin-right:4px">📦</span>
                    <strong class="pkg-name">${esc(j.name)}</strong>
                    <span class="tree-count">${formatSize(j.size)}</span>
                </div>
                <div class="tree-children collapsed">${hasChildren ? innerItems : '<span style="padding:4px 8px;color:var(--text-muted,#6b7280);font-size:11px">No resources</span>'}</div>
            </div>`;
        }

        const section = document.createElement('div');
        section.className = 'tree-node bundled-jars-section';
        section.innerHTML = `<div class="jar-separator" style="margin-top:12px">Bundled Libraries (${this._bundledJars.length} JARs)</div>
            <div class="tree-node">
                <div class="tree-toggle expanded" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="jar-group-icon">📦</span>
                    <strong class="jar-group-name">Bundled Libraries</strong>
                    <span class="tree-count">${this._bundledJars.length}</span>
                </div>
                <div class="tree-children">${jarNodes}</div>
            </div>`;
        container.appendChild(section);
    },

    _openBundledContentByIdx(idx) {
        const entry = this._bundledContentCache[idx];
        if (!entry) return;
        const panel = document.getElementById('code-panel');
        if (!panel) return;
        const esc = s => (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        panel.innerHTML = `<div class="cm-info-bar">
            <span class="cm-classname">${esc(entry.jarName)} / ${esc(entry.filename)}</span>
            <span class="cm-src-badge cm-badge-recon">BUNDLED</span>
            <button class="btn-sm cm-copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('bundled-content-pre').textContent)" title="Copy to clipboard">Copy</button>
        </div>
        <div class="cm-content" style="padding:12px;overflow:auto;height:calc(100% - 40px);background:#1e1e2e">
            <pre id="bundled-content-pre" style="margin:0;white-space:pre-wrap;font-size:12px;font-family:monospace;tab-size:2;color:#cdd6f4">${esc(entry.content)}</pre>
        </div>`;
    },

    _appendJarDepMapSection() {
        if (!this._jarDepMap || !Object.keys(this._jarDepMap).length) return;
        const container = document.getElementById('class-tree');
        if (!container) return;
        const existing = container.querySelector('.jar-dep-map-section');
        if (existing) existing.remove();

        const esc = JA.utils.escapeHtml;
        const depJars = Object.keys(this._jarDepMap).sort();
        const totalUsed = depJars.length;

        let jarNodes = '';
        for (const jarName of depJars) {
            const appClasses = Array.from(this._jarDepMap[jarName] || []).sort();
            let classItems = '';
            for (const cls of appClasses) {
                const safeCls = cls.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
                classItems += `<div class="tree-node tree-resource-file">
                    <div class="tree-leaf" style="cursor:pointer" onclick="JA.codeTree.findAndHighlight('${safeCls}','')">
                        <span class="leaf-icon" style="margin-right:4px;color:#6b7280">&#9632;</span>
                        <span class="resource-filename">${esc(cls)}</span>
                    </div>
                </div>`;
            }
            jarNodes += `<div class="tree-node">
                <div class="tree-toggle" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span style="margin-right:4px">📦</span>
                    <strong class="pkg-name">${esc(jarName)}</strong>
                    <span class="tree-count">${appClasses.length} classes</span>
                </div>
                <div class="tree-children collapsed">${classItems}</div>
            </div>`;
        }

        const section = document.createElement('div');
        section.className = 'tree-node jar-dep-map-section';
        section.innerHTML = `<div class="jar-separator" style="margin-top:12px">JAR Dependencies (${totalUsed} libs used)</div>
            <div class="tree-node">
                <div class="tree-toggle expanded" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="jar-group-icon">&#128279;</span>
                    <strong class="jar-group-name">JAR Dependencies</strong>
                    <span class="tree-count">${totalUsed} libs used</span>
                </div>
                <div class="tree-children">${jarNodes}</div>
            </div>`;
        container.appendChild(section);
    },

    /** Render invocations block — called lazily on expand */
    _renderInvocations(method) {
        const esc = JA.utils.escapeHtml;
        const invocations = (method.invocations || []).filter(inv =>
            inv.methodName !== '<init>' && inv.methodName !== '<clinit>'
        );
        if (invocations.length === 0) return '';

        let lineNum = 1;
        const lines = invocations.map(inv => {
            const ownerFqn = inv.ownerClass || '';
            const ownerShort = ownerFqn.split('.').pop();
            const methName = inv.methodName || '';
            const retType = inv.returnType && inv.returnType !== 'void'
                ? ` <span class="inv-return">: ${esc(inv.returnType)}</span>` : '';
            const num = lineNum++;
            const navIdx = JA.nav.ref(ownerFqn, methName);
            return `<div class="invocation-line inv-clickable" onclick="JA.nav.click(${navIdx},event)" title="Click to navigate to ${ownerShort}.${methName}">
                <span class="inv-linenum">${num}</span>
                <span class="inv-arrow">&#8594;</span>
                <span class="inv-owner">${esc(ownerShort)}</span><span class="inv-dot">.</span><span class="inv-method">${esc(methName)}</span><span class="inv-parens">()</span>${retType}
            </div>`;
        });

        return `<div class="invocations-block">
            <div class="inv-header">Method Body — ${invocations.length} calls</div>
            ${lines.join('')}
        </div>`;
    }
};
