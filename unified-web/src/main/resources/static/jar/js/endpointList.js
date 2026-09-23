/**
 * Endpoint list panel — left side of the Endpoint Flows tab.
 *
 * UI-025 — server-paged, flat (decision recorded 2026-09-08: dropping controller-grouping was
 * chosen over the alternatives — see the task registry). Used to hold `analysis.endpoints` whole
 * and group by controller client-side, capped at 200 cards with a "show all" escape; the group
 * headers needed the true per-controller count, which meant walking every endpoint regardless of
 * the cap. Converted to `JA.api.endpointListPages`, the same `PA.pageSource`/`shared/
 * tableFramework.js` machinery `jar/js/codeTree.js` and the bundled-JAR list already use — search
 * moves to the server (`q`), and each card carries `endpointIndex` (§55) instead of an
 * array-position `_origIdx` computed from a whole array that no longer exists client-side.
 */
window.JA = window.JA || {};

JA.endpointList = {

    _src: null,

    render() {
        const container = document.getElementById('endpoint-list');
        const jarId = JA.app.currentJarId;
        const version = JA.app._currentVersion || undefined;
        this._src = JA.api.endpointListPages(jarId, version);

        container.innerHTML = `<div style="padding:0 0 12px 0">
            <input type="text" class="endpoint-search" placeholder="Filter endpoints..."
                   oninput="JA.endpointList.onSearch(this.value)">
        </div>
        <table style="width:100%"><tbody id="endpoints-tbody"></tbody></table>
        <div class="pagination-bar" id="endpoints-pager"></div>`;

        // Loading/error/empty states are PA.tf.render's own job (id + '-tbody', matched above) —
        // not reproduced here, the same as every other converted grid.
        PA.tf.init('endpoints', this._src, 50, JA.endpointList._renderRow, {});
    },

    onSearch(value) {
        PA.tf.setSearch('endpoints', value || '');
        PA.tf.filter('endpoints');
    },

    _renderRow(ep, idx, esc) {
        const params = (ep.parameters || []).map(p => p.type).join(', ');
        return `
            <tr class="endpoint-card-row">
                <td>
                    <div class="endpoint-card" data-idx="${ep.endpointIndex}" onclick="JA.endpointList.select(this)">
                        <div>
                            <span class="endpoint-method method-${ep.httpMethod}">${ep.httpMethod}</span>
                            <span class="endpoint-path">${esc(ep.fullPath || '/')}</span>
                        </div>
                        <div class="endpoint-detail">
                            ${esc(ep.methodName)}(${esc(params)})
                            &rarr; ${esc(ep.returnType)}
                        </div>
                    </div>
                </td>
            </tr>`;
    },

    async select(el) {
        // Stale-response guard (§B5): the .active card is set synchronously but the graph is
        // painted after a fetch, so clicking card A then card B could leave B highlighted while
        // #graph-container shows A's call tree.
        const isCurrent = UIBusy.seq(JA.endpointList, '_selectSeq');

        // Deselect all
        document.querySelectorAll('.endpoint-card').forEach(c => c.classList.remove('active'));
        el.classList.add('active');

        // UI-025: the array this index used to be read out of (`analysis.endpoints`) is no longer
        // held whole client-side — `endpointIndex` (§55) is the endpoint's position there, sent by
        // the server precisely so the by-index call-tree route still has something to address.
        const idx = parseInt(el.dataset.idx, 10);
        const jarId = JA.app.currentJarId;
        const version = JA.app._currentVersion || undefined;

        document.getElementById('graph-container').innerHTML =
            '<p class="graph-placeholder">Loading call tree...</p>';
        let tree;
        try {
            tree = await JA.api.getCallTree(jarId, idx, version);
        } catch (e) {
            if (!isCurrent()) return;
            document.getElementById('graph-container').innerHTML =
                '<p class="graph-placeholder">Failed to load call tree: ' + JA.utils.escapeHtml(e.message) + '</p>';
            return;
        }
        if (!isCurrent()) return;

        if (tree) {
            // JA.callGraph.render's third argument is the endpoint object, used for the
            // breadcrumb/title bar — the row itself carries everything it reads (httpMethod,
            // fullPath, methodName, etc.), same fields the whole-array version exposed.
            const row = PA.tf.state('endpoints').filtered.find(e => e.endpointIndex === idx);
            JA.callGraph.render(tree, 'graph-container', row || {});
        } else {
            document.getElementById('graph-container').innerHTML =
                '<p class="graph-placeholder">No call tree data for this endpoint</p>';
            const bar = document.getElementById('graph-breadcrumb-bar');
            if (bar) bar.style.display = 'none';
        }
    }
};
