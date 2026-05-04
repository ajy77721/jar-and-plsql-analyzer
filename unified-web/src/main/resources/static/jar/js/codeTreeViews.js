/**
 * Code Structure — additional view modes.
 * Extends JA.codeTree with Project Level and Visual Overview renderers.
 *
 * Load order: utils → codeTree → this file
 */
window.JA = window.JA || {};

JA.codeTreeViews = {

    /* ========== Project Level (IntelliJ-style folder tree) ========== */

    renderProjectView(classes, container) {
        const { main, deps } = JA.codeTree._groupBySourceJar(classes);
        let html = '';

        // Main app classes
        if (main.length) {
            const root = this._buildFolderTree(main);
            html += this._renderFolder(root, '', 0);
        }

        // Dependency JARs
        const jarNames = Object.keys(deps).sort();
        if (jarNames.length) {
            html += '<div class="jar-separator">Internal Dependencies (' + jarNames.length + ' JARs)</div>';
            for (const jarName of jarNames) {
                const esc = JA.utils.escapeHtml;
                const project = JA.summary._jarToProject ? JA.summary._jarToProject(jarName) : jarName;
                const domain = JA.summary._jarToDomain ? JA.summary._jarToDomain(jarName) : '';
                const root = this._buildFolderTree(deps[jarName]);
                const inner = this._renderFolder(root, '', 1);
                html += `<div class="tree-node jar-group" data-jar="${esc(jarName)}">
                    <div class="tree-toggle jar-group-toggle" onclick="JA.codeTree.togglePkg(this)">
                        <span class="arrow">&#9654;</span>
                        <span class="jar-group-icon">&#128230;</span>
                        <strong class="jar-group-name">${esc(project)}</strong>
                        ${domain ? '<span class="sum-domain-tag" style="font-size:9px">' + esc(domain) + '</span>' : ''}
                        <span class="tree-count">${deps[jarName].length} classes</span>
                    </div>
                    <div class="tree-children collapsed">${inner}</div>
                </div>`;
            }
        }

        container.innerHTML = html || '<p class="empty-state">No classes found</p>';
    },

    /** Build nested folder map from fully qualified names */
    _buildFolderTree(classes) {
        const root = { children: {}, classes: [] };
        for (const cls of classes) {
            const parts = (cls.packageName || '(default)').split('.');
            let node = root;
            for (const part of parts) {
                if (!node.children[part]) node.children[part] = { children: {}, classes: [] };
                node = node.children[part];
            }
            node.classes.push(cls);
        }
        return root;
    },

    /** Collapse single-child folders (com.example.app), max 3 segments */
    _collapsePath(node, label, depth) {
        depth = depth || 1;
        const childKeys = Object.keys(node.children);
        if (childKeys.length === 1 && node.classes.length === 0 && depth < 3) {
            const key = childKeys[0];
            return this._collapsePath(node.children[key], label ? label + '.' + key : key, depth + 1);
        }
        return { node, label };
    },

    _countClasses(node) {
        let count = node.classes.length;
        for (const child of Object.values(node.children)) count += this._countClasses(child);
        return count;
    },

    _renderFolder(node, path, depth) {
        const esc = JA.utils.escapeHtml;
        let html = '';
        const sortedKeys = Object.keys(node.children).sort();

        for (const key of sortedKeys) {
            const { node: collapsed, label } = this._collapsePath(node.children[key], key);
            const totalClasses = this._countClasses(collapsed);
            const expanded = depth < 2 ? ' expanded' : '';
            const collapsedCls = depth < 2 ? '' : ' collapsed';
            const fullPath = path ? path + '/' + label : label;

            html += `<div class="tree-node pv-folder">
                <div class="tree-toggle${expanded}" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="pv-folder-icon">&#128193;</span>
                    <span class="pv-folder-name">${esc(label)}</span>
                    <span class="tree-count">${totalClasses}</span>
                </div>
                <div class="tree-children${collapsedCls}">
                    ${this._renderFolder(collapsed, fullPath, depth + 1)}
                    ${this._renderFolderClasses(collapsed.classes)}
                </div>
            </div>`;
        }

        // Render classes at root level if any
        if (depth === 0 && node.classes.length > 0) {
            html += this._renderFolderClasses(node.classes);
        }

        return html;
    },

    _renderFolderClasses(classes) {
        if (!classes || classes.length === 0) return '';
        const esc = JA.utils.escapeHtml;
        const sorted = [...classes].sort((a, b) => a.simpleName.localeCompare(b.simpleName));
        let html = '';

        for (const cls of sorted) {
            const stereotype = cls.stereotype || 'OTHER';
            const badgeCls = JA.utils.stereotypeBadgeClass(stereotype);
            const color = JA.utils.stereotypeColor(stereotype);
            const icon = cls.isInterface ? '&#9671;' : cls.isEnum ? '&#9670;' : '&#9632;';
            const ext = cls.isInterface ? '' : '.java';
            const memberCount = (cls.fields || []).length + (cls.methods || []).length;
            const classIndex = JA.codeTree._classes.indexOf(cls);

            const searchKey = [
                cls.fullyQualifiedName, cls.simpleName,
                (cls.annotations || []).map(a => a.name).join(' '),
                (cls.methods || []).map(m => m.name).join(' ')
            ].join(' ').toLowerCase();

            html += `<div class="tree-node pv-file" data-search="${searchKey}" data-class-name="${esc(cls.simpleName)}" data-class-index="${classIndex}">
                <div class="tree-toggle" onclick="JA.codeTree.toggleClass(this, ${classIndex})">
                    <span class="arrow">&#9654;</span>
                    <span class="class-icon" style="color:${color}">${icon}</span>
                    <strong class="class-name">${esc(cls.simpleName)}${ext}</strong>
                    <span class="badge ${badgeCls}">${stereotype.replace('REST_', '')}</span>
                    <span class="tree-count">${memberCount}</span>
                </div>
                <div class="tree-children collapsed"></div>
            </div>`;
        }
        return html;
    },

    /* ========== Visual Overview (stereotype-grouped card grid) ========== */

    renderVisualView(classes, container) {
        const esc = JA.utils.escapeHtml;
        const groups = this._groupByStereotype(classes);
        const sortedKeys = this._stereotypeOrder().filter(s => groups[s]);
        // Add any remaining groups not in predefined order
        for (const key of Object.keys(groups).sort()) {
            if (!sortedKeys.includes(key)) sortedKeys.push(key);
        }

        if (sortedKeys.length === 0) {
            container.innerHTML = '<p class="empty-state">No classes found</p>';
            return;
        }

        // Stereotype filter toggle bar
        let html = '<div class="vo-filter-bar">';
        for (const stereo of sortedKeys) {
            const label = stereo.replace('REST_', '').replace('_', ' ');
            html += `<button class="vo-filter-toggle active" data-stereo="${stereo}" onclick="JA.codeTreeViews.toggleStereo(this)">${label} (${groups[stereo].length})</button>`;
        }
        html += '</div>';

        html += '<div class="vo-grid">';
        for (const stereo of sortedKeys) {
            html += this._renderStereotypeGroup(stereo, groups[stereo], esc);
        }
        html += '</div>';
        container.innerHTML = html;
    },

    toggleStereo(btn) {
        btn.classList.toggle('active');
        const stereo = btn.dataset.stereo;
        const groups = document.querySelectorAll('#class-tree .vo-group');
        groups.forEach(g => {
            if (g.dataset.stereo === stereo) {
                g.style.display = btn.classList.contains('active') ? '' : 'none';
            }
        });
    },

    _stereotypeOrder() {
        return [
            'REST_CONTROLLER', 'CONTROLLER', 'SERVICE', 'COMPONENT',
            'REPOSITORY', 'SPRING_DATA', 'ENTITY',
            'CONFIGURATION', 'OTHER'
        ];
    },

    _groupByStereotype(classes) {
        const map = {};
        for (const cls of classes) {
            const s = cls.stereotype || 'OTHER';
            (map[s] = map[s] || []).push(cls);
        }
        return map;
    },

    _renderStereotypeGroup(stereotype, classes, esc) {
        const badgeCls = JA.utils.stereotypeBadgeClass(stereotype);
        const color = JA.utils.stereotypeColor(stereotype);
        const sorted = [...classes].sort((a, b) => a.simpleName.localeCompare(b.simpleName));

        // Gather stats
        let totalMethods = 0, totalFields = 0;
        const packages = new Set();
        for (const cls of sorted) {
            totalMethods += (cls.methods || []).length;
            totalFields += (cls.fields || []).length;
            packages.add(cls.packageName || '(default)');
        }

        let html = `<div class="vo-group" data-stereo="${stereotype}">
            <div class="vo-group-header" style="border-left-color:${color}">
                <span class="badge ${badgeCls}" style="font-size:12px;padding:3px 10px">${stereotype.replace('REST_', '').replace('_', ' ')}</span>
                <span class="vo-group-count">${sorted.length} classes</span>
                <span class="vo-group-meta">${totalMethods} methods &middot; ${totalFields} fields &middot; ${packages.size} pkg</span>
            </div>
            <div class="vo-class-list">`;

        for (const cls of sorted) {
            const methods = cls.methods || [];
            const fields = cls.fields || [];
            const icon = cls.isInterface ? '&#9671;' : cls.isEnum ? '&#9670;' : '&#9632;';
            const classIndex = JA.codeTree._classes.indexOf(cls);

            // Key annotations
            const keyAnns = (cls.annotations || [])
                .filter(a => ['RestController','Controller','Service','Repository','Component',
                              'Configuration','Entity','Document','Scheduled','Cacheable'].includes(a.name))
                .map(a => `<span class="method-ann">@${a.name}</span>`).join(' ');

            // HTTP endpoints on this class
            const httpMethods = methods.filter(m => m.httpMethod);
            const endpointBadges = httpMethods.slice(0, 4).map(m =>
                `<span class="endpoint-method method-${m.httpMethod}" style="font-size:9px;padding:1px 4px">${m.httpMethod}</span>`
            ).join(' ');
            const moreEps = httpMethods.length > 4 ? `<span class="vo-more">+${httpMethods.length - 4}</span>` : '';

            html += `<div class="vo-card" onclick="JA.codeTreeViews.jumpToClass('${esc(cls.simpleName)}', ${classIndex})">
                <div class="vo-card-title">
                    <span style="color:${color}">${icon}</span>
                    <strong>${esc(cls.simpleName)}</strong>
                </div>
                <div class="vo-card-pkg">${esc(cls.packageName || '')}</div>
                ${keyAnns ? '<div class="vo-card-anns">' + keyAnns + '</div>' : ''}
                ${endpointBadges ? '<div class="vo-card-eps">' + endpointBadges + moreEps + '</div>' : ''}
                <div class="vo-card-stats">
                    <span>${methods.length}M</span>
                    <span>${fields.length}F</span>
                </div>
            </div>`;
        }

        html += '</div></div>';
        return html;
    },

    /** Click a card in visual view → switch to package view and navigate */
    jumpToClass(className, classIndex) {
        JA.codeTree.setViewMode('package');
        setTimeout(() => {
            JA.codeTree.findAndHighlight(className, '');
        }, 100);
    },

    /* ========== JAR Source Filter ========== */

    _activeJarSources: null, // null = all, Set = selected jars (null entry = main jar)

    initJarSourceFilter(classes) {
        const bar = document.getElementById('jarSourceFilterBar');
        if (!bar) return;
        const { main, deps } = JA.codeTree._groupBySourceJar(classes);
        const jarNames = Object.keys(deps).sort();
        if (jarNames.length === 0) { bar.style.display = 'none'; return; }

        bar.style.display = '';
        this._activeJarSources = null;
        const esc = JA.utils.escapeHtml;

        let html = '<span class="jsf-label">Source:</span>';
        html += `<button class="jar-source-pill active main-jar" data-jar="" onclick="JA.codeTreeViews.toggleJarSource(this)">Current JAR <span class="jsf-count">(${main.length})</span></button>`;
        for (const jar of jarNames) {
            const short = jar.replace(/\.jar$/i, '').replace(/-\d+\.\d+.*$/, '');
            html += `<button class="jar-source-pill active" data-jar="${esc(jar)}" onclick="JA.codeTreeViews.toggleJarSource(this)" title="${esc(jar)}">${esc(short)} <span class="jsf-count">(${deps[jar].length})</span></button>`;
        }
        html += `<button class="btn-sm" onclick="JA.codeTreeViews.resetJarSourceFilter()" style="margin-left:4px;font-size:10px">All</button>`;
        bar.innerHTML = html;
    },

    toggleJarSource(btn) {
        const allPills = document.querySelectorAll('#jarSourceFilterBar .jar-source-pill');

        if (this._activeJarSources === null) {
            // First click: select only this one
            this._activeJarSources = new Set();
            const jar = btn.dataset.jar;
            this._activeJarSources.add(jar);
            allPills.forEach(p => p.classList.toggle('active', p.dataset.jar === jar));
        } else if (this._activeJarSources.has(btn.dataset.jar)) {
            // Toggle off
            this._activeJarSources.delete(btn.dataset.jar);
            btn.classList.remove('active');
            if (this._activeJarSources.size === 0) {
                this._activeJarSources = null;
                allPills.forEach(p => p.classList.add('active'));
            }
        } else {
            // Toggle on
            this._activeJarSources.add(btn.dataset.jar);
            btn.classList.add('active');
        }
        this._applyJarSourceFilter();
    },

    resetJarSourceFilter() {
        this._activeJarSources = null;
        document.querySelectorAll('#jarSourceFilterBar .jar-source-pill').forEach(p => p.classList.add('active'));
        this._applyJarSourceFilter();
    },

    _applyJarSourceFilter() {
        const filtered = this._getFilteredClasses();
        const container = document.getElementById('class-tree');
        if (!container) return;

        const mode = JA.codeTree._viewMode;
        if (mode === 'visual') {
            this.renderVisualView(filtered, container);
        } else if (mode === 'project') {
            this.renderProjectView(filtered, container);
        } else {
            JA.codeTree._renderFilteredPackageView(container, filtered);
        }
    },

    _getFilteredClasses() {
        if (this._activeJarSources === null) return JA.codeTree._classes;
        return JA.codeTree._classes.filter(cls => {
            const jar = cls.sourceJar || '';
            return this._activeJarSources.has(jar);
        });
    },

    /* ========== Filtering for non-package views ========== */

    filterProjectView(query) {
        const q = (query || '').toLowerCase().trim();
        const tree = document.getElementById('class-tree');
        if (!tree) return;

        const allNodes = tree.querySelectorAll('.pv-folder, .pv-file, .jar-group, .jar-separator');

        if (!q) {
            allNodes.forEach(n => n.style.display = '');
            return;
        }

        // Show files that match, propagate up to folders and jar groups
        const files = tree.querySelectorAll('.pv-file');
        const visibleFolders = new Set();
        const visibleJarGroups = new Set();

        files.forEach(f => {
            const text = f.getAttribute('data-search') || '';
            if (text.includes(q)) {
                f.style.display = '';
                let parent = f.parentElement;
                while (parent && parent !== tree) {
                    if (parent.classList && parent.classList.contains('tree-children')) {
                        parent.classList.remove('collapsed');
                        const t = parent.previousElementSibling;
                        if (t && t.classList.contains('tree-toggle')) t.classList.add('expanded');
                    }
                    if (parent.classList && parent.classList.contains('pv-folder')) visibleFolders.add(parent);
                    if (parent.classList && parent.classList.contains('jar-group')) visibleJarGroups.add(parent);
                    parent = parent.parentElement;
                }
            } else {
                f.style.display = 'none';
            }
        });

        tree.querySelectorAll('.pv-folder').forEach(f => {
            f.style.display = visibleFolders.has(f) ? '' : 'none';
        });
        tree.querySelectorAll('.jar-group').forEach(g => {
            g.style.display = visibleJarGroups.has(g) ? '' : 'none';
        });
        tree.querySelectorAll('.jar-separator').forEach(sep => {
            sep.style.display = visibleJarGroups.size > 0 ? '' : 'none';
        });
    },

    filterVisualView(query) {
        const q = (query || '').toLowerCase().trim();
        const cards = document.querySelectorAll('#class-tree .vo-card');
        const groups = document.querySelectorAll('#class-tree .vo-group');

        if (!q) {
            cards.forEach(c => c.style.display = '');
            groups.forEach(g => g.style.display = '');
            return;
        }

        const visibleGroups = new Set();
        cards.forEach(c => {
            const text = (c.textContent || '').toLowerCase();
            if (text.includes(q)) {
                c.style.display = '';
                visibleGroups.add(c.closest('.vo-group'));
            } else {
                c.style.display = 'none';
            }
        });

        groups.forEach(g => {
            g.style.display = visibleGroups.has(g) ? '' : 'none';
        });
    }
};
