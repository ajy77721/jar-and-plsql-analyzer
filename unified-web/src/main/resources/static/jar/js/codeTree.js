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

    // UI-003 (§134): true once _classes genuinely holds every class — either the small-JAR eager
    // path, or after Project/Visual view (or a JAR-source-filter interaction needing them) forced
    // _ensureFullClassesLoaded. False while package view is running off _packageSummary alone.
    _classesFullyLoaded: false,
    _packageSummary: null,

    /**
     * Class object → its position in {@link _classes}.
     *
     * <p>Three render loops used `_classes.indexOf(cls)` to recover the index they pass to the
     * expand handler. That is a linear scan inside a per-class loop, so the render was O(n²): at
     * the 80,000 classes this view is specified for, ~3.2 billion comparisons before a single row
     * appears. The map makes it O(n) to build once and O(1) per row.
     *
     * <p>Rebuilt rather than maintained, because `_classes` is only ever replaced wholesale. Keyed
     * on object identity, matching what `indexOf` did, and first-write-wins so a duplicated object
     * resolves to the same index `indexOf` would have returned.
     */
    _classIndex: null,

    /**
     * Above this many classes, packages start collapsed and fill their rows on first expand.
     *
     * <p>Class *bodies* were already lazy, but every class *header* was written up front: 80,000
     * classes produced a single 52 MB `innerHTML` assignment and ~80,000 tree rows, which is far
     * past any DOM budget and past what a browser will lay out without stalling.
     *
     * <p>The threshold exists so the common case is unchanged. A 119-class JAR still opens fully
     * expanded, because that is what makes a small tree useful; the lazy path is for the corpus
     * sizes where showing everything was never going to work anyway.
     */
    EAGER_CLASS_LIMIT: 500,

    /**
     * Most class rows written into one package at once, with a "show more" control for the rest.
     *
     * <p>Needed in addition to the lazy fill: laziness bounds what is on screen at load, but a
     * single package holding 40,000 classes would still blow the budget the moment it is opened.
     */
    MAX_CLASSES_PER_PACKAGE: 200,

    /** Package key → the classes it holds, for packages whose rows have not been written yet. */
    _pendingPackages: null,
    _writtenPackages: null,
    _pendingSeq: 0,
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

    /**
     * The index a class sits at, in constant time.
     *
     * <p>Built lazily so that setting `_classes` stays a plain assignment — a rebuild hook on every
     * write is a hook that a future assignment will forget to call, and the failure mode would be
     * expand handlers silently opening the wrong class.
     *
     * @returns the index, or -1 when the class is not in the current set, exactly as `indexOf` did
     */
    classIndexOf(cls) {
        if (this._classIndex === null) {
            this._classIndex = new Map();
            for (let i = 0; i < this._classes.length; i++) {
                if (!this._classIndex.has(this._classes[i])) this._classIndex.set(this._classes[i], i);
            }
        }
        const at = this._classIndex.get(cls);
        return at === undefined ? -1 : at;
    },

    /* ---- public API ---- */

    setLazy() {
        this._classes = [];
        this._classIndex = null;
        this._classesFullyLoaded = false;
        this._packageSummary = null;
        this._pendingPackages = null;
        this._writtenPackages = null;
        this._pendingSeq = 0;
        this._loaded = false;
        this._resources = [];
        this._resourcesLoaded = false;
        this._bundledJars = [];
        this._bundledJarsLoaded = false;
        this._jarDepMap = {};
        this._jarDepMapLoaded = false;
        const container = document.getElementById('class-tree');
        if (container) container.innerHTML = '<p style="padding:20px;color:var(--text-muted,var(--text-muted))">Switch to Code Structure tab to load classes</p>';
    },

    /** Rows per class-tree request. Matches the endpoint's default (PLAN-08 §3.2: 200 for trees). */
    _pageSize: 200,

    /**
     * Loads the class tree (Q40 / L1; UI-003, §134).
     *
     * <p><b>Package view (the default) no longer fetches every class up front.</b> §118 measured
     * that the tree itself has nothing to aggregate — Project/Visual view and the JAR-source filter
     * genuinely are complete browses of every class, by design — but the *package list* (name,
     * source JAR, class count) is a real aggregate, one row per package rather than one per class.
     * Above `EAGER_CLASS_LIMIT`, this fetches that summary alone (`/classes/packages`) and renders
     * collapsed package rows from it; a package's own classes are fetched only when it is expanded
     * (`togglePkg`, below), via the same `/classes/tree` endpoint everyone else already pages, now
     * filtered to that one package.
     *
     * <p>At or under `EAGER_CLASS_LIMIT` the old eager path runs unchanged — the common case (most
     * real JARs) opens exactly as it always has, no behavioural change, no new risk.
     *
     * <p>Switching to Project or Visual view, or opening the JAR-source filter, still needs every
     * class — that is not something a package-level aggregate can serve, by definition (§118's own
     * finding). `_ensureFullClassesLoaded` fetches the whole set on that explicit, opt-in action
     * instead of unconditionally on tab open — see `setViewMode`.
     *
     * <p>The previous version of this comment claimed cross-tab navigation (`findAndHighlight`)
     * "reads the whole `_classes` array" — checked directly and that is not true: it searches the
     * rendered DOM (`.tree-node[data-class-name]`), which for a still-collapsed package was already
     * empty before this change too. `findAndHighlight` has its own fallback for that case now (see
     * its own comment) rather than depending on residency it never actually used.
     */
    async loadAndRender() {
        if (this._loaded) return;
        const container = document.getElementById('class-tree');
        if (container) container.innerHTML = '<p style="padding:20px;color:var(--text-muted,var(--text-muted))">Loading code structure...</p>';
        try {
            const jarId = JA.app.currentJarId;
            const version = JA.app._currentVersion || undefined;
            let summary = null;
            try { summary = await JA.api.getClassPackages(jarId, version); } catch (e) { summary = null; }
            const total = summary ? summary.reduce((s, r) => s + (r.classCount || 0), 0) : Infinity;

            if (!summary || total <= this.EAGER_CLASS_LIMIT) {
                // Small JAR, or the summary endpoint failed (older server / transient error):
                // unchanged eager behaviour, exactly as before this section.
                const classTree = await this._fetchAllClassPages(jarId, version, container);
                this.render(classTree);
            } else {
                this._classes = [];
                this._classIndex = null;
                this._classesFullyLoaded = false;
                this._packageSummary = summary;
                this._pendingPackages = null;
                this._writtenPackages = null;
                this._pendingSeq = 0;
                this._history = [];
                this._historyIndex = -1;
                this._loaded = true;
                this._renderPackageSummaryView(container);
                this._updateNavState();
                if (JA.codeTreePanel) JA.codeTreePanel.clear();
                if (JA.codeTreeViews) JA.codeTreeViews.initJarSourceFilterFromSummary(summary);
            }
            this._loaded = true;
        } catch (e) {
            console.error('Failed to load class tree:', e);
            if (container) container.innerHTML = '<p style="padding:20px;color:var(--red)">Failed to load code structure: ' + (e.message || e) + '</p>';
        }
        this._loadResources();
        this._loadBundledJars();
        this._loadJarDepMap();
    },

    /**
     * The whole class array, fetched once and cached — needed only by Project/Visual view and the
     * JAR-source filter's own per-class grouping, both genuine whole-corpus reads by design (§118).
     * A no-op once `_classesFullyLoaded`, and also a no-op on the small-JAR path (`loadAndRender`
     * already populated `_classes` fully there, so this flag is set true immediately in `render`).
     */
    async _ensureFullClassesLoaded() {
        if (this._classesFullyLoaded) return this._classes;
        const container = document.getElementById('class-tree');
        if (container) container.innerHTML = '<p style="padding:20px;color:var(--text-muted)">Loading full class list…</p>';
        const jarId = JA.app.currentJarId;
        const version = JA.app._currentVersion || undefined;
        const classTree = await this._fetchAllClassPages(jarId, version, container);
        this._classes = classTree;
        this._classIndex = null;
        this._classesFullyLoaded = true;
        return this._classes;
    },

    /**
     * One row per package/source-JAR pair, no classes fetched yet — the package-view entry point.
     * `summary` defaults to the full `_packageSummary`; the JAR-source filter passes a restricted
     * subset instead of re-fetching (see `codeTreeViews._applyJarSourceFilter`).
     */
    _renderPackageSummaryView(container, summary) {
        const { main, deps } = this._groupSummaryBySourceJar(summary || this._packageSummary);
        let html = '';

        if (!main.length && !Object.keys(deps).length) {
            container.innerHTML = '<p class="empty-state">No classes match the selected sources</p>';
            return;
        }

        if (main.length) {
            const sorted = main.slice().sort((a, b) => a.packageName.localeCompare(b.packageName));
            for (const row of sorted) {
                html += this._renderPackageFromSummary(row.packageName, '', row.classCount);
            }
        }

        const jarNames = Object.keys(deps).sort();
        if (jarNames.length) {
            html += '<div class="jar-separator">Internal Dependencies (' + jarNames.length + ' JARs)</div>';
            for (const jarName of jarNames) {
                const rows = deps[jarName].slice().sort((a, b) => a.packageName.localeCompare(b.packageName));
                const totalInJar = rows.reduce((s, r) => s + r.classCount, 0);
                const project = JA.summary._jarToProject ? JA.summary._jarToProject(jarName) : jarName;
                const domain = JA.summary._jarToDomain ? JA.summary._jarToDomain(jarName) : '';
                let inner = '';
                for (const row of rows) {
                    inner += this._renderPackageFromSummary(row.packageName, jarName, row.classCount);
                }
                html += `<div class="tree-node jar-group" data-jar="${JA.utils.escapeHtml(jarName)}">
                    <div class="tree-toggle jar-group-toggle" onclick="JA.codeTree.togglePkg(this)">
                        <span class="arrow">&#9654;</span>
                        <span class="jar-group-icon">&#128230;</span>
                        <strong class="jar-group-name">${JA.utils.escapeHtml(project)}</strong>
                        ${domain ? '<span class="sum-domain-tag" style="font-size:9px">' + JA.utils.escapeHtml(domain) + '</span>' : ''}
                        <span class="tree-count">${totalInJar} classes</span>
                    </div>
                    <div class="tree-children collapsed">${inner}</div>
                </div>`;
            }
        }

        container.innerHTML = html || '<p class="empty-state">No classes found</p>';
    },

    /** Same grouping `_groupBySourceJar` does for a class array, over package-summary rows instead. */
    _groupSummaryBySourceJar(summary) {
        const main = [];
        const deps = {};
        for (const row of summary) {
            if (!row.sourceJar) main.push(row);
            else (deps[row.sourceJar] = deps[row.sourceJar] || []).push(row);
        }
        return { main, deps };
    },

    /**
     * A collapsed package row backed by a summary count, not a classes array — `_pendingPackages`
     * holds `{pkg, jar}` (a plain object, not an array) as the marker that this entry's classes
     * still need fetching; `togglePkg` tells the two cases apart with `Array.isArray`, so the
     * eager path (a real classes array, set by `_renderPackage`) needs no changes at all.
     */
    _renderPackageFromSummary(pkg, jarKey, count) {
        const esc = JA.utils.escapeHtml;
        const key = 'p' + (++this._pendingSeq);
        if (this._pendingPackages === null) this._pendingPackages = new Map();
        this._pendingPackages.set(key, { pkg, jar: jarKey });

        return `
            <div class="tree-node" data-search="${pkg.toLowerCase()}" data-pkg-name="${esc(pkg)}" data-pkg-jar="${esc(jarKey)}">
                <div class="tree-toggle" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="pkg-icon">&#128230;</span>
                    <strong class="pkg-name">${esc(pkg)}</strong>
                    <span class="tree-count">${count}</span>
                </div>
                <div class="tree-children collapsed" data-pkg-key="${this._pendingSeq}">
                </div>
            </div>`;
    },

    /**
     * One package's own classes, fetched via `/classes/tree`'s new column filter
     * (`cf=0:<packageName>|1:<sourceJar>`) rather than the whole tree. Bounded to 50 pages (10,000
     * classes) the same defensive way `_fetchAllClassPages` bounds itself — a package holding more
     * than that would be its own, separate problem.
     */
    async _fetchPackageClasses(pkg, jarKey) {
        const jarId = JA.app.currentJarId;
        const version = JA.app._currentVersion || undefined;
        const jarValue = jarKey ? jarKey : JA.api.MAIN_JAR_SENTINEL;
        // Real bug, found live: unlike `_fetchAllClassPages` right below (which already guards
        // for `war/js/api.js` not defining `classTreePages` — see that method's own comment),
        // this one didn't, so expanding any package in Code Structure on the WAR page threw
        // "Cannot read properties of undefined (reading 'fetchPage')" and stayed blank. Same
        // fallback shape: filter the already-available flat class list client-side instead of
        // paging a server route WAR doesn't have.
        const canPage = !!(window.PA && PA.pageSource)
                && typeof JA.api.classTreePages === 'function';
        if (!canPage) {
            const all = await JA.api.getClassTree(jarId, version);
            return (all || []).filter(c => c.packageName === pkg
                    && (c.sourceJar || JA.api.MAIN_JAR_SENTINEL) === jarValue);
        }
        const cf = ['0:' + pkg, '1:' + jarValue];
        const source = JA.api.classTreePages(jarId, version, null, cf);
        const classes = [];
        for (let page = 0; page < 50; page++) {
            const env = await source.fetchPage(this._pageSize);
            for (const c of env.items) classes.push(c);
            if (!env.page || !env.page.hasMore) break;
        }
        return classes;
    },

    /**
     * Walks the whole cursor chain, with a progress line so a large jar does not look hung.
     *
     * <p>Falls back to the unpaged endpoint if the paged one is unavailable — the shim
     * (`PA.normalizePage`) already tolerates a bare array, so an older server answers correctly
     * without this file knowing which side of the migration it is on.
     */
    async _fetchAllClassPages(jarId, version, container) {
        // Guard the PRODUCT, not just the factory — and guard the RIGHT object.
        //
        // This checked `PA.pageSource` and then called `JA.api.classTreePages`, which is a
        // different object entirely. `war/js/api.js` does not define `classTreePages`, so on the
        // WAR page the guard passed and the call threw TypeError: the Code Structure tab was dead
        // there while the comment above explained a fallback that could never run.
        //
        // Both are checked now: the paging helper must exist AND the API must expose the method,
        // because either being absent leads to the same dead tab by a different route.
        const canPage = !!(window.PA && PA.pageSource)
                && typeof JA.api.classTreePages === 'function';
        const source = canPage ? JA.api.classTreePages(jarId, version, null) : null;
        if (!source || typeof source.fetchPage !== 'function') {
            if (source) {
                console.warn('classTreePages returned no fetchPage (stale page-envelope.js?) '
                        + '- falling back to the unpaged class tree');
            }
            return JA.api.getClassTree(jarId, version);
        }
        const classes = [];
        let total = -1;
        // Bounded, so a server that always reports hasMore cannot spin forever.
        for (let page = 0; page < 500; page++) {
            const env = await source.fetchPage(this._pageSize);
            for (const cls of env.items) classes.push(cls);
            if (total < 0 && env.page && env.page.totalIsExact) total = env.page.total;
            if (!env.page || !env.page.hasMore) break;
            if (container) {
                container.innerHTML = '<p style="padding:20px;color:var(--text-muted)">Loading code structure… '
                    + classes.length + (total >= 0 ? ' / ' + total : '') + ' classes</p>';
            }
        }
        return classes;
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
            <div class="cm-content" style="padding:12px;overflow:auto;height:calc(100% - 40px);background:var(--sidebar-bg)">
                <pre id="res-content-pre" style="margin:0;white-space:pre-wrap;font-size:12px;font-family:monospace;tab-size:2;color:var(--sidebar-text)">${esc(text)}</pre>
            </div>`;
        } catch (e) {
            panel.innerHTML = '<div style="padding:20px;color:var(--red)">Failed to load resource: ' + (e.message || e) + '</div>';
        }
    },

    render(classes) {
        this._classes = classes;
        this._classIndex = null;
        this._classesFullyLoaded = true;   // UI-003 (§134): the eager path — every class is here
        this._packageSummary = null;       // — not the lazy summary-driven one
        this._pendingPackages = null;
        this._writtenPackages = null;
        this._pendingSeq = 0;
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

    /**
     * UI-003 (§134) — Project/Visual view are genuine whole-corpus browses (§118's own finding),
     * so switching to either now ensures the full class array first, an explicit, opt-in cost, only
     * paid by a caller who actually asked to browse every class — not on every analysis load the
     * way `loadAndRender` used to pay it unconditionally.
     */
    async setViewMode(mode) {
        this._viewMode = mode;
        document.querySelectorAll('.view-mode-btn').forEach(b => {
            b.classList.toggle('active', b.dataset.mode === mode);
        });
        const treeButtons = document.getElementById('tree-view-actions');
        if (treeButtons) treeButtons.style.display = mode === 'visual' ? 'none' : '';

        // A class selected in the outgoing view's detail panel (source/method list, right-hand
        // #code-panel) is not valid for the incoming view — its DOM row and index no longer apply,
        // and staying on screen would show one view's stale detail while another view is active.
        // `render()`/the lazy branch of `loadAndRender()` already clear this on fresh load; a mode
        // switch is the same "nothing is selected yet" state and must reset the same way.
        if (JA.codeTreePanel) JA.codeTreePanel.clear();

        if (mode !== 'package' && !this._classesFullyLoaded) {
            await this._ensureFullClassesLoaded();
        }

        if (JA.codeTreeViews && JA.codeTreeViews._activeJarSources !== null) {
            await JA.codeTreeViews._applyJarSourceFilter();
        } else {
            await this._renderCurrentView();
        }
        if (this._resourcesLoaded) this._appendResourcesSection();
        if (this._bundledJarsLoaded) this._appendBundledJarsSection();
        if (this._jarDepMapLoaded) this._appendJarDepMapSection();
    },

    async _renderCurrentView() {
        const container = document.getElementById('class-tree');
        if (!container) return;

        if (this._viewMode === 'project') {
            if (!this._classesFullyLoaded) await this._ensureFullClassesLoaded();
            if (this._classes.length) JA.codeTreeViews.renderProjectView(this._classes, container);
        } else if (this._viewMode === 'visual') {
            if (!this._classesFullyLoaded) await this._ensureFullClassesLoaded();
            if (this._classes.length) JA.codeTreeViews.renderVisualView(this._classes, container);
        } else if (this._packageSummary && !this._classesFullyLoaded) {
            // Package view, still lazy: re-render from the summary rather than the (partially
            // populated, expand-order-dependent) `_classes` array — switching back to package view
            // after visiting Project/Visual starts the package list fresh, same as those two modes
            // already do on every switch.
            this._pendingPackages = null;
            this._writtenPackages = null;
            this._pendingSeq = 0;
            this._renderPackageSummaryView(container);
        } else if (this._classes.length) {
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

    /**
     * Fetches one class's members and renders them into an already-open node.
     *
     * <p>Guarded two ways. `dataset.rendered` is only set on success, so a failed load can be
     * retried by collapsing and reopening rather than leaving a permanently empty node. And the
     * class identity is re-checked on arrival: collapsing and expanding a different class while
     * this is in flight must not paint one class's members under another's row.
     */
    _loadClassMembers(cls, classIndex, children) {
        const jarId = JA.app.currentJarId || '';
        const want = cls.fullyQualifiedName || cls.simpleName;
        // Fetched BY NAME, not by index. `/classes/by-index/{idx}` indexes analysis.json's own
        // class order, while classIndex indexes this tree's PACKAGE-sorted array — the two do not
        // agree, so an index fetch would quietly return a different class and render its methods
        // under this row.
        const url = '/api/jar/jars/' + encodeURIComponent(jarId)
                + '/decompile?class=' + encodeURIComponent(want);
        fetch(url, { cache: 'no-store' })
            .then(r => r.ok ? r.json() : null)
            .then(payload => {
                const full = payload && (payload.classData || payload);
                if (!full) throw new Error('empty');
                // Belt and braces: only accept a response that is actually this class.
                const got = full.fullyQualifiedName || full.simpleName;
                if (got && got !== want && full.simpleName !== cls.simpleName) {
                    throw new Error('identity mismatch');
                }
                // Cache on the class object so a second expand costs nothing.
                if (full.methods) cls.methods = full.methods;
                if (full.methodSummaries) cls.methodSummaries = full.methodSummaries;
                if (full.fields && !cls.fields) cls.fields = full.fields;
                const still = this._classes[classIndex];
                if (!still || (still.fullyQualifiedName || still.simpleName) !== want) return;
                children.innerHTML = this._renderClassBody(cls, classIndex);
                children.dataset.rendered = '1';
            })
            .catch(() => {
                children.innerHTML = '<div class="tree-node"><div class="tree-leaf">'
                    + 'Could not load members \u2014 collapse and reopen to retry</div></div>';
            });
    },

    toggleClass(el, classIndex) {
        const children = el.nextElementSibling;
        if (!children) return;

        // Lazy render: if children are empty, render fields+methods now.
        //
        // The tree is fetched with members=false — methodSummaries and fields are 93.8% of that
        // response (11.8 MB -> ~760 KB for 1,623 classes), and nothing on screen needs them until
        // a class is opened. So the first expand of a class fetches just that class and caches the
        // members on it; every later expand is instant.
        if (!children.dataset.rendered) {
            const cls = this._classes[classIndex];
            if (cls) {
                // Test for METHODS specifically, not "any members". The paged row carries `fields`
                // but not `methodSummaries` — so a check for either would be satisfied by fields,
                // render a body with no methods, and never fetch. That is precisely how the Methods
                // section came to be permanently empty in the first place.
                const hasMethods = (cls.methodSummaries && cls.methodSummaries.length)
                        || (cls.methods && cls.methods.length);
                const expectsMethods = (cls.methodCount || 0) > 0;
                if (hasMethods || !expectsMethods) {
                    children.innerHTML = this._renderClassBody(cls, classIndex);
                    children.dataset.rendered = '1';
                } else {
                    children.innerHTML = '<div class="tree-node"><div class="tree-leaf">Loading\u2026</div></div>';
                    this._loadClassMembers(cls, classIndex, children);
                }
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

    /**
     * UI-003 (§134) — the pre-existing behaviour here, checked directly rather than trusted from
     * `loadAndRender`'s own old doc comment: this searches the rendered DOM
     * (`.tree-node[data-class-name]`), not `_classes`. For any package not yet expanded, those
     * rows were never written (`_renderPackage`'s own lazy-body branch, present before this
     * section), so a cross-tab jump to a class in a collapsed package already failed silently —
     * `else { JA.toast.warn('Class not found...') }`, below — for any JAR over
     * `EAGER_CLASS_LIMIT`, independent of this section's own package-view changes.
     *
     * <p>Fixed here as a direct beneficiary of the same lookup machinery this section needed
     * anyway (`_resolveAndExpandPackage`): a failed DOM search now resolves the class's package
     * first (from the already-loaded array if resident, one bounded server search if not) and
     * opens it before giving up.
     */
    findAndHighlight(className, methodName, _retried) {
        setTimeout(async () => {
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

            // Passed as a parameter, not an instance flag: `findAndHighlight`'s own recursive call
            // below is scheduled via `setTimeout` too, so it has not actually run yet by the time
            // control returns here — an instance flag reset immediately after starting it would
            // race the very call it exists to guard, always losing.
            if (!classNode && !_retried) {
                const opened = await this._resolveAndExpandPackage(className);
                if (opened) {
                    this.findAndHighlight(className, methodName, true);
                    return;
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

    /**
     * UI-003 (§134) — resolves which package a class lives in and opens it, so `findAndHighlight`
     * can find a class whose package row exists but whose body was never written (collapsed,
     * lazy — either this section's summary-driven package view, or the pre-existing per-package
     * lazy body any large JAR already had). Returns whether a package was found and opened.
     */
    async _resolveAndExpandPackage(className) {
        let pkg = null;
        let jar = null;

        // Cheapest first: if _classes already holds every class (eager small-JAR path, or a
        // lazy view that has since fully loaded via Project/Visual/the JAR filter), the answer
        // is already in memory — no network round trip needed.
        const existing = this._classes.find(c =>
            c.simpleName === className || c.fullyQualifiedName === className);
        if (existing) {
            pkg = existing.packageName || '(default)';
            jar = existing.sourceJar || '';
        } else if (this._packageSummary) {
            // Lazy package view: ask the server which package this class lives in — bounded to a
            // handful of rows, not the whole class array.
            try {
                const jarId = JA.app.currentJarId;
                const version = JA.app._currentVersion || undefined;
                const source = JA.api.classTreePages(jarId, version, className);
                const env = await source.fetchPage(5);
                const items = env.items || [];
                const match = items.find(c =>
                    c.simpleName === className || c.fullyQualifiedName === className) || items[0];
                if (match) {
                    pkg = match.packageName || '(default)';
                    jar = match.sourceJar || '';
                }
            } catch (e) {
                return false;
            }
        }
        if (pkg === null) return false;

        const esc = window.CSS && CSS.escape ? CSS.escape : (s => s.replace(/["\\]/g, '\\$&'));
        const pkgNode = document.querySelector(
            `.tree-node[data-pkg-name="${esc(pkg)}"][data-pkg-jar="${esc(jar)}"]`);
        if (!pkgNode) return false;
        const toggle = pkgNode.querySelector(':scope > .tree-toggle');
        const children = pkgNode.querySelector(':scope > .tree-children');
        if (!toggle || !children) return false;
        if (children.classList.contains('collapsed')) {
            await this.togglePkg(toggle);
        }
        return true;
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
            // A closing panel must not keep a query it has stopped applying (SearchQuery.resetWithin).
            if (window.SearchQuery) SearchQuery.resetWithin(panel);
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
            // Disable rather than let a click open a panel that only says "No navigation history
            // yet" — the label already told the user there's nothing to show.
            histBtn.disabled = this._history.length === 0;
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
        const eager = this._classes.length <= this.EAGER_CLASS_LIMIT;

        // Collapsed packages still hold their markup in the DOM, so collapsing alone saves nothing.
        // What bounds the tree is not writing the rows until the package is opened.
        let body;
        let toggleClass;
        if (eager) {
            body = this._renderClassRows(classes, this.MAX_CLASSES_PER_PACKAGE);
            toggleClass = 'tree-toggle expanded';
        } else {
            const key = 'p' + (++this._pendingSeq);
            if (this._pendingPackages === null) this._pendingPackages = new Map();
            this._pendingPackages.set(key, classes);
            body = '';
            toggleClass = 'tree-toggle';
        }

        // UI-003 (§134) — data-pkg-name/-jar, same attributes `_renderPackageFromSummary` carries,
        // so `_resolveAndExpandPackage` can find and open the right package node regardless of
        // which of the two render paths produced it.
        const jarKey = classes.length ? (classes[0].sourceJar || '') : '';
        return `
            <div class="tree-node" data-search="${pkg.toLowerCase()}" data-pkg-name="${esc(pkg)}" data-pkg-jar="${esc(jarKey)}">
                <div class="${toggleClass}" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span class="pkg-icon">&#128230;</span>
                    <strong class="pkg-name">${esc(pkg)}</strong>
                    <span class="tree-count">${classes.length}</span>
                </div>
                <div class="tree-children${eager ? '' : ' collapsed'}"${eager ? '' : ` data-pkg-key="${this._pendingSeq}"`}>
                    ${body}
                </div>
            </div>`;
    },

    /**
     * Class rows from the start of the package through `count`, plus a control for the rest.
     *
     * <p>Always from the start, never an append: the "show more" control lives at the end of the
     * same container, so appending would strand it above the rows it just revealed. Rewriting is
     * bounded by the same cap, so it stays cheap.
     *
     * @param count how many rows to write; clamped to the package size
     */
    _renderClassRows(classes, count) {
        const to = Math.min(classes.length, count);
        let html = '';
        for (let i = 0; i < to; i++) html += this._renderClassHeader(classes[i]);
        if (to < classes.length) {
            // Says how many remain, not just "more". A reader deciding whether to keep clicking
            // needs the number; a bare "show more" hides how deep the list actually goes.
            const next = Math.min(this.MAX_CLASSES_PER_PACKAGE, classes.length - to);
            html += `<div class="tree-leaf pkg-show-more" data-next="${to + next}"
                          onclick="JA.codeTree.showMoreClasses(this)">
                        Showing ${to} of ${classes.length} classes — show ${next} more
                     </div>`;
        }
        return html;
    },

    /**
     * Expand or collapse a package, writing its rows the first time it opens.
     *
     * <p>The key is read from the DOM rather than closed over, because this is reached from an
     * inline `onclick` on markup that was built as a string.
     *
     * <p>UI-003 (§134) — `_pendingPackages` now holds two different kinds of entry: a real classes
     * array (the pre-existing eager path, from `_renderPackage`, unchanged) or a plain `{pkg, jar}`
     * descriptor (`_renderPackageFromSummary`'s lazy path, new). `Array.isArray` tells them apart,
     * so the eager branch below is untouched — this method only grew a new branch, not a rewrite.
     */
    async togglePkg(el) {
        const children = el.nextElementSibling;
        if (!children) return;
        el.classList.toggle('expanded');
        children.classList.toggle('collapsed');

        const key = children.dataset ? children.dataset.pkgKey : null;
        if (!key || !this._pendingPackages) return;
        const entry = this._pendingPackages.get('p' + key);
        if (!entry) return;                     // already written; nothing pending for this key

        let classes;
        if (Array.isArray(entry)) {
            classes = entry;
        } else {
            // Lazy descriptor: fetch this one package's own classes, then append them into
            // `_classes` — not keep them in a side cache — so `classIndexOf`/`toggleClass`/member
            // loading all keep working completely unchanged; they already address by object
            // identity within `_classes`, not by a fetch strategy.
            children.innerHTML = '<div class="tree-leaf" style="color:var(--text-muted);font-size:11px;padding:4px 0">Loading…</div>';
            try {
                classes = await this._fetchPackageClasses(entry.pkg, entry.jar);
            } catch (e) {
                children.innerHTML = '<div class="tree-leaf" style="color:var(--red);font-size:11px;padding:4px 0">Failed to load: '
                        + JA.utils.escapeHtml(e.message || String(e)) + '</div>';
                return;
            }
            for (const c of classes) this._classes.push(c);
            this._classIndex = null; // rebuilt lazily by classIndexOf, now covering the appended range
        }

        children.innerHTML = this._renderClassRows(classes, this.MAX_CLASSES_PER_PACKAGE);
        // Kept in the map — `showMoreClasses` needs the full list to page through. Only the
        // "not yet written" fact is dropped, by moving the entry to the written set.
        this._pendingPackages.delete('p' + key);
        if (this._writtenPackages === null || this._writtenPackages === undefined) {
            this._writtenPackages = new Map();
        }
        this._writtenPackages.set('p' + key, classes);
    },

    /** Appends the next page of class rows to an already-open package. */
    showMoreClasses(el) {
        const children = el.parentNode;
        if (!children) return;
        const key = children.dataset ? children.dataset.pkgKey : null;
        const classes = key && this._writtenPackages
            ? this._writtenPackages.get('p' + key)
            : null;
        if (!classes) return;
        const next = parseInt(el.dataset ? el.dataset.next : '0', 10) || 0;
        children.innerHTML = this._renderClassRows(classes, next);
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
        const classIndex = this.classIndexOf(cls);

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
    /** Methods drawn per class before the cap engages. */
    _methodCap: 200,

    /** Fields drawn per class before the cap engages. Max observed on one class: 3,704. */
    _fieldCap: 200,

    /** Per-class override set by "show all"; keyed by class index. */
    _expandedAll: {},

    /**
     * Lifts the per-class method cap and repaints just that class's body.
     *
     * <p>Scoped to one class on purpose: opening a 2,419-method generated class should not make
     * every other class expensive for the rest of the session.
     */
    showAllMethods(classIndex, anchor) {
        this._expandedAll[classIndex] = true;
        const cls = this._classes[classIndex];
        const children = anchor && anchor.closest('.tree-children');
        if (cls && children) children.innerHTML = this._renderClassBody(cls, classIndex);
    },

    _renderClassBody(cls, classIndex) {
        const esc = JA.utils.escapeHtml;
        const fields = cls.fields || [];
        // `methodSummaries` is what /classes/tree actually sends; `methods` is the field name in
        // analysis.json. This read had only ever checked `methods`, so the Methods section of the
        // Code Structure tree rendered EMPTY for every class since the streaming provider began
        // renaming the field — 60 methods on WarAnalyzerController showed as none. Nothing failed
        // loudly: `[]` is a legitimate value for a class with no methods.
        // `methods` is kept as a fallback so a payload carrying the raw name still renders.
        const methods = cls.methodSummaries || cls.methods || [];
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
            // Fields need the same cap as methods: the maximum on this JAR is 3,704 on a single
            // class (three exceed 200), even though the 99th percentile is 18. A percentile is not
            // a bound, and the outlier is precisely the class that stalls.
            const fCap = (this._expandedAll && this._expandedAll[classIndex])
                    ? fields.length : this._fieldCap;
            const fShown = Math.min(fCap, fields.length);
            html += '<div class="tree-section-label">Fields</div>';
            for (const f of fields.slice(0, fShown)) {
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
            if (fShown < fields.length) {
                html += '<div class="tree-node"><div class="tree-leaf" style="opacity:.8">'
                    + 'Showing ' + fShown + ' of ' + fields.length + ' fields \u2014 '
                    + '<a href="#" onclick="event.preventDefault();'
                    + 'JA.codeTree.showAllMethods(' + classIndex + ',this)">show all</a>'
                    + '</div></div>';
            }
        }

        if (methods.length > 0) {
            // Capped per class, disclosed, same idiom as the endpoint list and cross-ref.
            // Generated code makes this real rather than theoretical: on this JAR, 7 classes
            // exceed 1,000 methods and PlSqlParserBaseListener has 2,419 — that many rows built
            // into one innerHTML is a visible stall when a single class is opened.
            const cap = (this._expandedAll && this._expandedAll[classIndex])
                    ? methods.length : this._methodCap;
            const shown = Math.min(cap, methods.length);
            html += '<div class="tree-section-label">Methods</div>';
            for (let mi = 0; mi < shown; mi++) {
                html += this._renderMethodHeader(methods[mi], cls, classIndex, mi);
            }
            if (shown < methods.length) {
                html += '<div class="tree-node"><div class="tree-leaf" style="opacity:.8">'
                    + 'Showing ' + shown + ' of ' + methods.length + ' methods \u2014 '
                    + '<a href="#" onclick="event.preventDefault();'
                    + 'JA.codeTree.showAllMethods(' + classIndex + ',this)">show all</a>'
                    + '</div></div>';
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

    /**
     * Bundled libraries (Q41 / L2) — one page at a time, headers only.
     *
     * <p>The unpaged response is 3.03 MB for 94 rows, of which 2.84 MB is manifest text and
     * resource file bodies that this section does not render until a leaf is clicked. Asking for
     * the projected page instead brings it to 16 KB; the body of a file is fetched by
     * `_openBundledContent` when it is actually opened.
     */
    async _loadBundledJars() {
        if (this._bundledJarsLoaded) return;
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        try {
            if (!window.PA || !PA.pageSource) {
                const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/bundled-jars', { cache: 'no-store' });
                if (!res.ok) return;
                this._bundledJars = await res.json();
            } else {
                const source = JA.api.bundledJarPages(jarId, null);
                const jars = [];
                for (let page = 0; page < 500; page++) {
                    const env = await source.fetchPage(100);
                    for (const j of env.items) jars.push(j);
                    if (!env.page || !env.page.hasMore) break;
                }
                this._bundledJars = jars;
            }
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
        // A leaf carries the JAR and file *name*, and the body is fetched when it is clicked.
        // Previously every body was cached in this array and opened by array position — a
        // positional handle into data the server may reorder, and 2.84 MB held in the tab for the
        // sake of the one file a user might open.
        const leaf = (jarName, filename, icon) => {
            const a = jarName.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
            const b = filename.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
            return `<div class="tree-node tree-resource-file">
                    <div class="tree-leaf" style="cursor:pointer" onclick="JA.codeTree._openBundledContent('${esc(a)}','${esc(b)}')">
                        <span class="leaf-icon" style="margin-right:4px">${icon}</span>
                        <span class="resource-filename">${esc(filename)}</span>
                    </div>
                </div>`;
        };

        let jarNodes = '';
        for (const j of this._bundledJars) {
            let innerItems = '';
            // Tolerates both response shapes: `hasManifest`/`resourceNames` from the paged
            // projection, `manifest`/`resources` from the legacy full array.
            const hasManifest = ('hasManifest' in j) ? !!j.hasManifest : !!j.manifest;
            const resourceKeys = j.resourceNames || Object.keys(j.resources || {});
            if (hasManifest) innerItems += leaf(j.name, 'MANIFEST.MF', '📋');
            for (const fname of resourceKeys) {
                innerItems += leaf(j.name, fname, extIcon(fname));
            }
            const hasChildren = hasManifest || resourceKeys.length > 0;
            jarNodes += `<div class="tree-node">
                <div class="tree-toggle" onclick="JA.codeTree.togglePkg(this)">
                    <span class="arrow">&#9654;</span>
                    <span style="margin-right:4px">📦</span>
                    <strong class="pkg-name">${esc(j.name)}</strong>
                    <span class="tree-count">${formatSize(j.size)}</span>
                </div>
                <div class="tree-children collapsed">${hasChildren ? innerItems : '<span style="padding:4px 8px;color:var(--text-muted,var(--text-muted));font-size:11px">No resources</span>'}</div>
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

    /** Fetches and shows one bundled file, addressed by JAR name and file name. */
    async _openBundledContent(jarName, filename) {
        // Stale-response guard (§B5). #code-panel is a singleton shared with
        // JA.codeTreePanel.show, which guards only its own _currentFqn and cannot know a
        // bundled-file write is in flight into the same node. Invalidation is made mutual:
        // codeTreePanel.show bumps this same token, and we clear its _currentFqn below.
        const isCurrent = UIBusy.seq(JA.codeTree, '_codePanelSeq');
        if (JA.codeTreePanel) JA.codeTreePanel._currentFqn = null;
        const panel = document.getElementById('code-panel');
        if (!panel) return;
        const esc = s => (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        const header = `<div class="cm-info-bar">
            <span class="cm-classname">${esc(jarName)} / ${esc(filename)}</span>
            <span class="cm-src-badge cm-badge-recon">BUNDLED</span>
            <button class="btn-sm cm-copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('bundled-content-pre').textContent)" title="Copy to clipboard">Copy</button>
        </div>`;
        panel.innerHTML = '<div class="cp-loading"><div class="cm-spinner"></div><div>Loading...</div></div>';
        let content;
        try {
            content = await JA.api.getBundledJarContent(JA.app.currentJarId, jarName, filename);
        } catch (e) {
            if (!isCurrent()) return;
            panel.innerHTML = header + '<div style="padding:20px;color:var(--red)">Failed to load: '
                + esc(e.message || String(e)) + '</div>';
            return;
        }
        if (!isCurrent()) return;
        panel.innerHTML = header + `
        <div class="cm-content" style="padding:12px;overflow:auto;height:calc(100% - 40px);background:var(--sidebar-bg)">
            <pre id="bundled-content-pre" style="margin:0;white-space:pre-wrap;font-size:12px;font-family:monospace;tab-size:2;color:var(--sidebar-text)">${esc(content)}</pre>
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
                        <span class="leaf-icon" style="margin-right:4px;color:var(--text-muted)">&#9632;</span>
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
