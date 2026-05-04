/**
 * Endpoint list panel — left side of the Endpoint Flows tab.
 */
window.JA = window.JA || {};

JA.endpointList = {

    render(endpoints) {
        const container = document.getElementById('endpoint-list');
        if (!endpoints || endpoints.length === 0) {
            container.innerHTML = '<p class="empty-state" style="padding:20px">No endpoints found in this JAR</p>';
            return;
        }

        // Stamp original index on each endpoint so sorting/filtering doesn't break call tree fetch
        endpoints.forEach((ep, i) => { if (ep._origIdx == null) ep._origIdx = i; });
        this._endpoints = endpoints;
        this._renderFiltered(endpoints, container, '');
    },

    filterEndpoints(query) {
        const container = document.getElementById('endpoint-list');
        const q = (query || '').toLowerCase().trim();
        this._renderFiltered(this._endpoints || [], container, q);
    },

    _renderFiltered(endpoints, container, query) {
        // Group by controller
        const grouped = {};
        for (const ep of endpoints) {
            const key = ep.controllerSimpleName || ep.controllerClass;
            (grouped[key] = grouped[key] || []).push(ep);
        }

        let html = `<div style="padding:0 0 12px 0">
            <input type="text" class="endpoint-search" placeholder="Filter endpoints..."
                   oninput="JA.endpointList.filterEndpoints(this.value)" value="${JA.utils.escapeHtml(query)}">
        </div>`;
        let count = 0;

        for (const [ctrl, eps] of Object.entries(grouped)) {
            const filtered = query
                ? eps.filter(ep => {
                    const text = (ep.httpMethod + ' ' + ep.fullPath + ' ' + ep.methodName + ' ' + ctrl).toLowerCase();
                    return text.includes(query);
                })
                : eps;
            if (filtered.length === 0) continue;

            html += `<div style="margin-bottom:16px">
                <div style="font-weight:600;font-size:13px;margin-bottom:6px;color:var(--text-muted)">${JA.utils.escapeHtml(ctrl)} <span style="font-weight:400;font-size:11px">(${filtered.length})</span></div>`;

            for (const ep of filtered) {
                const params = (ep.parameters || []).map(p => p.type).join(', ');
                const origIdx = ep._origIdx != null ? ep._origIdx : endpoints.indexOf(ep);
                html += `
                    <div class="endpoint-card" data-idx="${origIdx}" onclick="JA.endpointList.select(this)">
                        <div>
                            <span class="endpoint-method method-${ep.httpMethod}">${ep.httpMethod}</span>
                            <span class="endpoint-path">${JA.utils.escapeHtml(ep.fullPath || '/')}</span>
                        </div>
                        <div class="endpoint-detail">
                            ${JA.utils.escapeHtml(ep.methodName)}(${JA.utils.escapeHtml(params)})
                            &rarr; ${JA.utils.escapeHtml(ep.returnType)}
                        </div>
                    </div>`;
                count++;
            }
            html += '</div>';
        }

        if (count === 0 && query) {
            html += '<p class="empty-state" style="padding:20px">No endpoints match filter</p>';
        }

        container.innerHTML = html;
    },

    async select(el) {
        // Deselect all
        document.querySelectorAll('.endpoint-card').forEach(c => c.classList.remove('active'));
        el.classList.add('active');

        const idx = parseInt(el.dataset.idx, 10);
        const analysis = JA.app.currentAnalysis;
        if (!analysis || !analysis.endpoints[idx]) return;

        const endpoint = analysis.endpoints[idx];

        if (!endpoint.callTree) {
            document.getElementById('graph-container').innerHTML =
                '<p class="graph-placeholder">Loading call tree...</p>';
            try {
                const jarId = JA.app.currentJarId;
                const version = JA.app._currentVersion || undefined;
                endpoint.callTree = await JA.api.getCallTree(jarId, idx, version);
            } catch (e) {
                document.getElementById('graph-container').innerHTML =
                    '<p class="graph-placeholder">Failed to load call tree: ' + JA.utils.escapeHtml(e.message) + '</p>';
                return;
            }
        }

        if (endpoint.callTree) {
            JA.callGraph.render(endpoint.callTree, 'graph-container', endpoint);
        } else {
            document.getElementById('graph-container').innerHTML =
                '<p class="graph-placeholder">No call tree data for this endpoint</p>';
            const bar = document.getElementById('graph-breadcrumb-bar');
            if (bar) bar.style.display = 'none';
        }
    }
};
