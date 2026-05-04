/**
 * Sidebar — renders the JAR list with duplicate grouping and select / delete actions.
 * JARs sharing the same base name (e.g. web-1.0.0-SNAPSHOT_<13-digit-ts> → web-1.0.0-SNAPSHOT)
 * are grouped; the newest (by analyzedAt) is shown as primary; older ones collapse under "+N older".
 */
window.JA = window.JA || {};

JA.sidebar = {

    _expanded: {}, // groupKey → true when expanded

    render(jars, selectedId) {
        const container = document.getElementById('jar-list');
        if (!jars || jars.length === 0) {
            container.innerHTML = '<p class="empty-state">No JARs analyzed yet</p>';
            return;
        }

        const groups = this._groupJars(jars);
        container.innerHTML = groups.map(g => this._renderGroup(g, selectedId)).join('');

        container.onclick = (e) => {
            const deleteBtn = e.target.closest('[data-delete-id]');
            if (deleteBtn) {
                e.stopPropagation();
                JA.app.deleteJar(deleteBtn.dataset.deleteId);
                return;
            }
            const toggleBtn = e.target.closest('[data-expand-key]');
            if (toggleBtn) {
                e.stopPropagation();
                const key = toggleBtn.dataset.expandKey;
                this._expanded[key] = !this._expanded[key];
                this.render(jars, selectedId);
                return;
            }
            const jarItem = e.target.closest('[data-jar-id]');
            if (jarItem) {
                JA.app.selectJar(jarItem.dataset.jarId);
            }
        };
    },

    _groupJars(jars) {
        const TS_SUFFIX = /_\d{13}$/;
        const byKey = new Map();

        for (const jar of jars) {
            const id = jar.id || '';
            const baseKey = id.replace(TS_SUFFIX, '');
            if (!byKey.has(baseKey)) byKey.set(baseKey, []);
            byKey.get(baseKey).push(jar);
        }

        return Array.from(byKey.entries()).map(([key, members]) => {
            // Sort: CORRECTED before STATIC, then newest analyzedAt first
            members.sort((a, b) => {
                const modeScore = m => (m === 'CORRECTED' ? 0 : 1);
                const md = modeScore(a.analysisMode) - modeScore(b.analysisMode);
                if (md !== 0) return md;
                return (b.analyzedAt || '').localeCompare(a.analyzedAt || '');
            });
            return { key, primary: members[0], others: members.slice(1) };
        }).sort((a, b) =>
            (b.primary.analyzedAt || '').localeCompare(a.primary.analyzedAt || '')
        );
    },

    _renderGroup(group, selectedId) {
        const { key, primary, others } = group;
        let html = this._renderItem(primary, selectedId, false);

        if (others.length > 0) {
            const expanded = !!this._expanded[key];
            html += `<div class="jar-group-toggle" data-expand-key="${JA.utils.escapeHtml(key)}">`
                  + `${expanded ? '&#9660;' : '&#9654;'} ${others.length} older version${others.length > 1 ? 's' : ''}`
                  + '</div>';
            if (expanded) {
                for (const jar of others) {
                    html += this._renderItem(jar, selectedId, true);
                }
            }
        }

        return html;
    },

    _renderItem(jar, selectedId, isOlder) {
        const active = jar.id === selectedId ? 'active' : '';
        const olderCls = isOlder ? ' jar-item-older' : '';
        const esc = JA.utils.escapeHtml;
        const name = esc(jar.projectName || jar.jarName || jar.id);
        const size = jar.jarSize ? JA.utils.formatSize(jar.jarSize) : '';
        const date = this._formatDate(jar.analyzedAt);
        const meta = jar.totalClasses + ' classes, ' + jar.totalEndpoints + ' endpoints';
        const cs = jar.claudeStatus || '';
        let claudeBadge = '';
        if (cs === 'RUNNING') claudeBadge = '<span class="jar-claude-badge jar-claude-running" title="Claude enrichment running">AI</span>';
        else if (cs === 'COMPLETE') claudeBadge = '<span class="jar-claude-badge jar-claude-complete" title="Claude enriched">AI</span>';
        else if (cs === 'FAILED') claudeBadge = '<span class="jar-claude-badge jar-claude-failed" title="Claude failed">AI</span>';
        let modeBadge = '';
        if (jar.analysisMode === 'CORRECTED') {
            const iter = jar.claudeIteration || '';
            const iterLabel = iter > 1 ? 'C(' + iter + ')' : 'C';
            modeBadge = '<span class="jar-mode-badge-sm jar-mode-corrected" title="Claude corrected' + (iter ? ', iteration ' + iter : '') + '">' + iterLabel + '</span>';
        }
        const bpTitle = jar.basePackage ? ` title="Base package: ${esc(jar.basePackage)}"` : '';
        const bpBadge = jar.basePackage
            ? `<span class="jar-bp-badge"${bpTitle}>pkg</span>`
            : '';

        return `
            <div class="jar-item ${active}${olderCls}" data-jar-id="${esc(jar.id)}">
                <div class="jar-info">
                    <div class="jar-name" title="${name}">${name}${claudeBadge}${modeBadge}${bpBadge}</div>
                    <div class="jar-meta">${meta}${size ? ' &middot; ' + size : ''}</div>
                    ${date ? '<div class="jar-date">' + date + '</div>' : ''}
                </div>
                <button class="delete-btn" data-delete-id="${esc(jar.id)}"
                        title="Delete">&times;</button>
            </div>`;
    },

    _formatDate(isoStr) {
        if (!isoStr) return '';
        try {
            const d = new Date(isoStr);
            if (isNaN(d.getTime())) return '';
            const now = new Date();
            const diffMs = now - d;
            const diffMin = Math.floor(diffMs / 60000);
            if (diffMin < 1) return 'Just now';
            if (diffMin < 60) return diffMin + 'm ago';
            const diffHr = Math.floor(diffMin / 60);
            if (diffHr < 24) return diffHr + 'h ago';
            const diffDay = Math.floor(diffHr / 24);
            if (diffDay < 7) return diffDay + 'd ago';
            return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
        } catch (e) { return ''; }
    }
};
