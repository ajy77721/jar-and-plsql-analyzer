/**
 * Summary — Lifecycle Workflows sub-tab.
 * Shows multi-step collection lifecycle patterns (e.g. a blue-green collection swap:
 * create temp -> copy -> index -> rename -> drop) detected across scattered call-tree nodes,
 * as ordered step sequences — a genuinely different shape from the flat Aggregation Flows
 * table, since this is about SEQUENCE, not independent rows.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

function _lifecycleEsc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

Object.assign(JA.summary, {

    _lifecycleExpanded: {},

    _renderLifecycleTab(epReports, esc) {
        const instances = [];
        const typeCounts = {};

        for (const r of epReports) {
            const ep = r.endpoint;
            if (!ep || !ep.collectionLifecycleFlows || !ep.collectionLifecycleFlows.length) continue;
            for (const flow of ep.collectionLifecycleFlows) {
                typeCounts[flow.workflowType] = (typeCounts[flow.workflowType] || 0) + 1;
                instances.push({
                    workflowType: flow.workflowType || 'UNKNOWN',
                    rootClassName: flow.rootClassName || '',
                    rootSimpleClassName: flow.rootSimpleClassName || '',
                    rootMethodName: flow.rootMethodName || '',
                    matchedStageCount: flow.matchedStageCount || 0,
                    totalConfiguredStages: flow.totalConfiguredStages || 0,
                    complete: !!flow.complete,
                    steps: flow.steps || [],
                    endpointName: r.endpointName || '',
                    fullPath: r.fullPath || '',
                    httpMethod: r.httpMethod || ''
                });
            }
        }

        if (!instances.length) {
            let html = '<div class="sum-section" style="padding:30px">';
            html += '<div class="sum-section-title">Lifecycle Workflows</div>';
            html += '<p class="sum-muted">No multi-step collection lifecycle patterns (e.g. collection swap: create temp -&gt; copy -&gt; index -&gt; rename -&gt; drop) detected.</p>';
            html += '</div>';
            return html;
        }

        let html = '<div class="sum-section">';
        html += '<div class="sum-section-title">Lifecycle Workflows (' + instances.length + ' instances)</div>';
        html += '<div class="sum-section-desc">Multi-step collection lifecycle patterns correlated across scattered call-tree nodes by real MongoDB driver calls (createCollection/createIndex/renameCollection/drop, $merge/$out) — not by collection name (usually dynamic/runtime-built here) or application method names (which can be renamed freely). Click a card to expand its step sequence.</div>';

        // Workflow-type filter pills
        const types = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);
        html += '<div class="agg-filter-pills" id="lifecycle-type-pills">';
        html += '<span class="agg-pill active" data-ltype="" onclick="JA.summary._lifecycleFilterType(\'\')">ALL <b>' + instances.length + '</b></span>';
        for (const [type, count] of types) {
            html += `<span class="agg-pill" data-ltype="${esc(type)}" onclick="JA.summary._lifecycleFilterType('${esc(type)}')">${esc(type)} <b>${count}</b></span>`;
        }
        html += '</div>';

        html += '<div id="lifecycle-cards"></div>';
        html += '</div>';

        this._lifecycleInstances = instances;
        this._lifecycleActiveType = '';
        setTimeout(() => this._renderLifecycleCards(), 0);
        return html;
    },

    _lifecycleActiveType: '',
    _lifecycleInstances: [],

    _lifecycleFilterType(type) {
        this._lifecycleActiveType = type;
        document.querySelectorAll('#lifecycle-type-pills .agg-pill').forEach(p => {
            p.classList.toggle('active', p.dataset.ltype === type);
        });
        this._renderLifecycleCards();
    },

    _renderLifecycleCards() {
        const container = document.getElementById('lifecycle-cards');
        if (!container) return;
        const filtered = this._lifecycleActiveType
            ? this._lifecycleInstances.filter(i => i.workflowType === this._lifecycleActiveType)
            : this._lifecycleInstances;

        let html = '';
        for (let idx = 0; idx < filtered.length; idx++) {
            html += this._renderLifecycleCard(filtered[idx], idx);
        }
        container.innerHTML = html || '<p class="sum-muted" style="padding:20px">No instances match this filter.</p>';
    },

    _renderLifecycleCard(item, idx) {
        const esc = _lifecycleEsc;
        const cardId = 'lifecycle-card-' + idx;
        const expanded = !!this._lifecycleExpanded[cardId];
        const completeness = item.complete
            ? `<span class="agg-pill" style="background:var(--badge-green-bg);color:var(--badge-green);border-color:var(--badge-green)">${item.matchedStageCount}/${item.totalConfiguredStages} stages matched</span>`
            : `<span class="agg-pill" style="background:var(--badge-orange-bg);color:var(--badge-orange);border-color:var(--badge-orange)">${item.matchedStageCount}/${item.totalConfiguredStages} stages matched</span>`;
        const rootClassMethod = (item.rootSimpleClassName || '?') + '.' + (item.rootMethodName || '?') + '()';

        let html = `<div class="agg-detail-content" style="margin-bottom:10px;border:1px solid var(--border);border-radius:var(--radius-md);overflow:hidden">`;
        html += `<div class="sum-clickable-row" style="padding:12px 14px;display:flex;align-items:center;gap:10px;cursor:pointer" onclick="JA.summary._toggleLifecycleCard('${cardId}')">`;
        html += `<span style="font-weight:600">${expanded ? '&#9660;' : '&#9654;'}</span>`;
        html += `<span class="agg-source-badge">${esc(item.workflowType)}</span>`;
        html += `<code class="agg-class-method" onclick="event.stopPropagation();JA.summary.showClassCode('${esc(item.rootClassName.replace(/'/g, "\\'"))}','${esc(item.rootMethodName.replace(/'/g, "\\'"))}')" title="${esc(item.rootClassName)}">${esc(rootClassMethod)}</code>`;
        html += completeness;
        html += `<span style="margin-left:auto"><span class="endpoint-method method-${esc(item.httpMethod)}">${esc(item.httpMethod)}</span> <span title="${esc(item.fullPath)}">${esc(item.endpointName || item.fullPath)}</span></span>`;
        html += `</div>`;

        if (expanded) {
            html += `<div style="padding:6px 14px 14px 14px">`;
            html += `<ol style="margin:0;padding-left:22px">`;
            for (const step of item.steps) {
                const opClass = 'sum-op-' + (step.operationType || '').toLowerCase();
                const stageColor = this._lifecycleStageColor(step.stageName);
                html += `<li style="margin-bottom:8px;padding-left:4px;border-left:3px solid ${stageColor.fg}">`;
                html += `<span class="agg-stage-badge" style="background:${stageColor.bg};color:${stageColor.fg}">${esc(step.stageName)}</span> `;
                html += `<code class="agg-class-method" onclick="JA.summary.showClassCode('${esc((step.className || '').replace(/'/g, "\\'"))}','${esc((step.methodName || '').replace(/'/g, "\\'"))}')" title="${esc(step.className || '')}">${esc((step.simpleClassName || '?') + '.' + (step.methodName || '?') + '()')}</code> `;
                if (step.operationType) html += `<span class="sum-op-badge ${opClass}">${esc(step.operationType)}</span> `;
                if (step.dynamicVariableHint) html += `<span class="agg-source-badge" title="Dynamic collection variable observed on this step">var: ${esc(step.dynamicVariableHint)}</span>`;
                html += `</li>`;
            }
            html += `</ol>`;
            html += `</div>`;
        }
        html += `</div>`;
        return html;
    },

    // Returns theme tokens rather than literal hex so lifecycle badges follow the active theme
    // (light/dark) like every other badge in the app. The upstream snapshot hardcoded hex and
    // built its translucent background by string-concatenating an alpha suffix ("#2563eb" + "22"),
    // which cannot work with var() — hence the explicit {fg,bg} pair using the paired
    // --badge-*/--badge-*-bg tokens already defined in shared/mongodb-theme.css.
    _lifecycleStageColor(stageName) {
        const tokens = {
            CREATE_TEMP: 'blue', COPY: 'purple', INDEX: 'teal',
            RENAME: 'orange', DROP: 'red'
        };
        const t = tokens[stageName] || 'gray';
        return { fg: `var(--badge-${t})`, bg: `var(--badge-${t}-bg)` };
    },

    _toggleLifecycleCard(cardId) {
        this._lifecycleExpanded[cardId] = !this._lifecycleExpanded[cardId];
        this._renderLifecycleCards();
    }
});
