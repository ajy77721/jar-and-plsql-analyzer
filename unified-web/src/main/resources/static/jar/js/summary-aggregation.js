/**
 * Summary — Aggregation Flows sub-tab.
 * Shows MongoDB aggregation pipelines per endpoint: collections, $lookup joins,
 * pipeline stages, operation types, with expandable detail + breadcrumb + code nav.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _renderAggregationTab(epReports, esc) {
        const allRows = [];
        const stageCounts = {};
        const opCounts = {};

        for (const r of epReports) {
            const ep = r.endpoint;
            if (!ep) continue;

            const flows = ep.aggregationFlows || [];
            if (!flows.length) continue;

            for (const f of flows) {
                const op = f.operationType || 'AGGREGATE';
                opCounts[op] = (opCounts[op] || 0) + 1;
                const stages = f.pipelineStages || [];
                for (const s of stages) stageCounts[s] = (stageCounts[s] || 0) + 1;

                allRows.push({
                    className: f.className || '',
                    simpleClassName: f.simpleClassName || '',
                    methodName: f.methodName || '',
                    operationType: op,
                    collections: f.collections || [],
                    collectionDomains: f.collectionDomains || {},
                    collectionSources: f.collectionSources || {},
                    pipelineStages: stages,
                    lookupCollections: f.lookupCollections || [],
                    stereotype: f.stereotype || '',
                    sourceJar: f.sourceJar || '',
                    breadcrumb: f.breadcrumb || [],
                    queryAnnotations: f.queryAnnotations || [],
                    endpointName: r.endpointName || '',
                    fullPath: r.fullPath || '',
                    httpMethod: r.httpMethod || '',
                    primaryDomain: r.primaryDomain || ''
                });
            }
        }

        if (!allRows.length) {
            let html = '<div class="sum-section" style="padding:30px">';
            html += '<div class="sum-section-title">Aggregation Flows</div>';
            html += '<p class="sum-muted">No MongoDB aggregation pipelines, $lookup joins, or aggregate operations detected.</p>';
            html += '</div>';
            return html;
        }

        // Build filter pills
        const allOps = Object.entries(opCounts).sort((a, b) => b[1] - a[1]);
        let html = '<div class="sum-section">';
        html += '<div class="sum-section-title">Aggregation Flows (' + allRows.length + ' operations)</div>';
        html += '<div class="sum-section-desc">MongoDB aggregation pipelines, $lookup joins, and aggregate operations across all endpoints. Click a row to see full pipeline detail.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="$lookup and $graphLookup create hard joins between collections. If those collections belong to different domains, the pipeline CANNOT be split across services without redesign.">$lookup joins block vertical splitting</span>';
        html += '<span class="sum-tip" title="Pipelines with 5+ stages are complex to maintain and test. During migration, consider whether the aggregation can be simplified or replaced with application-level logic.">Complex pipelines (5+ stages) are migration risks</span>';
        html += '<span class="sum-tip" title="DYNAMIC aggregations are built at runtime (e.g., Criteria API, string concatenation). Static analysis may not capture the full pipeline. Manual review recommended.">DYNAMIC = runtime-built, needs manual review</span>';
        html += '<span class="sum-tip" title="Use the filter icon on column headers to filter by specific collections, pipeline stages, or operation types. Multi-value columns (Collections, Stages, Lookups) match if ANY value matches your selection.">Use column header filters to drill down</span>';
        html += '</div>';

        // Operation type pills
        html += '<div class="agg-filter-pills" id="agg-op-pills">';
        html += '<span class="agg-pill active" data-aop="" onclick="JA.summary._aggFilterOp(\'\')">ALL <b>' + allRows.length + '</b></span>';
        for (const [op, count] of allOps) {
            html += `<span class="agg-pill agg-op-${op.toLowerCase()}" data-aop="${esc(op)}" onclick="JA.summary._aggFilterOp('${esc(op)}')">${esc(op)} <b>${count}</b></span>`;
        }
        html += '</div>';

        // Pipeline stage pills (secondary)
        if (Object.keys(stageCounts).length) {
            const allStages = Object.entries(stageCounts).sort((a, b) => b[1] - a[1]);
            html += '<div class="agg-stage-pills" id="agg-stage-pills">';
            html += '<span class="agg-stage-label">Pipeline stages:</span>';
            for (const [stage, count] of allStages) {
                html += `<span class="agg-stage-chip">${esc(stage)} <b>${count}</b></span>`;
            }
            html += '</div>';
        }

        // Filter bar
        html += this._buildFilterBar('sum-agg', allRows, r => r.primaryDomain);

        // Table
        html += '<div id="sum-agg-pager-top" class="sum-pager" style="margin-bottom:4px"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="JA.summary._pageSort(\'sum-agg\',0)" style="width:90px">Operation</th>';
        html += '<th data-sort-col="1" onclick="JA.summary._pageSort(\'sum-agg\',1)">Class.Method</th>';
        html += '<th data-sort-col="2" onclick="JA.summary._pageSort(\'sum-agg\',2)">Collections</th>';
        html += '<th data-sort-col="3" onclick="JA.summary._pageSort(\'sum-agg\',3)">Pipeline Stages</th>';
        html += '<th data-sort-col="4" onclick="JA.summary._pageSort(\'sum-agg\',4)">Lookups</th>';
        html += '<th data-sort-col="5" onclick="JA.summary._pageSort(\'sum-agg\',5)">Endpoint</th>';
        html += '<th data-sort-col="6" onclick="JA.summary._pageSort(\'sum-agg\',6)" style="width:90px">Domain</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-agg-tbody"></tbody>';
        html += '</table></div>';
        html += '<div id="sum-agg-pager" class="sum-pager"></div>';
        html += '</div>';

        this._aggActiveOp = '';

        this._initPage('sum-agg', allRows, 25,
            (item, idx, esc) => this._renderAggRow(item, idx, esc),
            r => r.primaryDomain,
            (item, idx, esc) => this._renderAggDetail(item, idx, esc),
            {
                sortKeys: {
                    0: { fn: r => r.operationType || '' },
                    1: { fn: r => (r.simpleClassName || '') + '.' + (r.methodName || '') },
                    2: { fn: r => (r.collections || []).join(',') },
                    3: { fn: r => (r.pipelineStages || []).join(',') },
                    4: { fn: r => (r.lookupCollections || []).join(',') },
                    5: { fn: r => r.fullPath || '' },
                    6: { fn: r => r.primaryDomain || '' }
                },
                typeFilterFn: item => item.operationType || ''
            }
        );

        setTimeout(() => {
            this._pageRender('sum-agg');
            this._initColFilters('sum-agg', {
                0: { label: 'Operation', valueFn: r => r.operationType || '' },
                2: { label: 'Collections', valueFn: r => r.collections || [] },
                3: { label: 'Pipeline Stages', valueFn: r => r.pipelineStages || [] },
                4: { label: 'Lookups', valueFn: r => r.lookupCollections || [] },
                6: { label: 'Domain', valueFn: r => r.primaryDomain || '' }
            });
        }, 0);
        return html;
    },

    _aggActiveOp: '',

    _aggFilterOp(op) {
        this._aggActiveOp = op;
        document.querySelectorAll('#agg-op-pills .agg-pill').forEach(p => {
            p.classList.toggle('active', p.dataset.aop === op);
        });
        const s = this._pageState['sum-agg'];
        if (!s) return;
        s._typeFilter = op;
        this._pageFilter('sum-agg');
    },

    _renderAggRow(item, idx, esc) {
        const opClass = 'sum-op-' + (item.operationType || '').toLowerCase();
        const classMethod = (item.simpleClassName || '?') + '.' + (item.methodName || '?') + '()';
        const collBadges = (item.collections || []).map(c => {
            const d = item.collectionDomains[c] || '';
            const src = item.collectionSources[c] || '';
            const cls = src.includes('PIPELINE') || src.includes('AGGREGATION') ? ' agg-coll-pipeline' : '';
            return `<span class="agg-coll-badge${cls}" title="${esc(d + ' | ' + src)}">${esc(c)}</span>`;
        }).join(' ');
        const stageBadges = (item.pipelineStages || []).map(s =>
            `<span class="agg-stage-badge">${esc(s)}</span>`
        ).join(' ');
        const lookupBadges = (item.lookupCollections || []).map(c =>
            `<span class="agg-lookup-badge" title="$lookup from">${esc(c)}</span>`
        ).join(' ');

        let html = `<tr class="sum-clickable-row" onclick="JA.summary.toggleDetail('agg',${idx})">`;
        html += `<td><span class="sum-op-badge ${opClass}">${esc(item.operationType || 'AGGREGATE')}</span></td>`;
        html += `<td><code class="agg-class-method" onclick="event.stopPropagation();JA.summary.showClassCode('${esc(item.className.replace(/'/g, "\\'"))}','${esc(item.methodName.replace(/'/g, "\\'"))}')" title="${esc(item.className)}">${esc(classMethod)}</code></td>`;
        html += `<td>${collBadges || '<span class="sum-muted">-</span>'}</td>`;
        html += `<td>${stageBadges || '<span class="sum-muted">-</span>'}</td>`;
        html += `<td>${lookupBadges || '<span class="sum-muted">-</span>'}</td>`;
        html += `<td><span class="endpoint-method method-${item.httpMethod}">${esc(item.httpMethod)}</span> <span title="${esc(item.fullPath)}">${esc(item.endpointName || item.fullPath)}</span></td>`;
        html += `<td><span class="sum-domain-tag">${esc(item.primaryDomain)}</span></td>`;
        html += '</tr>';
        return html;
    },

    _renderAggDetail(item, idx, esc) {
        let html = `<tr class="sum-detail-row" id="sum-agg-detail-${idx}" style="display:none"><td colspan="7"><div class="agg-detail-content">`;

        // Class + Method
        html += '<div class="agg-detail-info">';
        html += '<div class="agg-detail-field"><b>Full Class:</b> <code onclick="JA.summary.showClassCode(\'' + esc(item.className.replace(/'/g, "\\'")) + '\',\'' + esc(item.methodName.replace(/'/g, "\\'")) + '\')" class="sum-clickable">' + esc(item.className + '.' + item.methodName + '()') + '</code></div>';
        html += '<div class="agg-detail-field"><b>Operation:</b> <span class="sum-op-badge sum-op-' + (item.operationType || '').toLowerCase() + '">' + esc(item.operationType || 'AGGREGATE') + '</span>';
        if (item.isDynamic) html += ' <span class="agg-dynamic-badge">DYNAMIC</span>';
        html += '</div>';
        if (item.stereotype) html += '<div class="agg-detail-field"><b>Stereotype:</b> <code>' + esc(item.stereotype) + '</code></div>';
        if (item.sourceJar) html += '<div class="agg-detail-field"><b>Source JAR:</b> <code>' + esc(item.sourceJar) + '</code></div>';
        html += '<div class="agg-detail-field"><b>Endpoint:</b> <span class="endpoint-method method-' + item.httpMethod + '">' + esc(item.httpMethod) + '</span> ' + esc(item.fullPath) + '</div>';
        if (item.detectionSources && item.detectionSources.length) {
            html += '<div class="agg-detail-field"><b>Detected Via:</b> ';
            html += item.detectionSources.map(s => `<span class="agg-source-badge">${esc(s)}</span>`).join(' ');
            html += '</div>';
        }
        html += '</div>';

        // Collections detail
        if (item.collections.length) {
            html += '<div class="agg-detail-section"><b>Collections Accessed:</b>';
            html += '<table class="agg-detail-table"><thead><tr><th>Collection</th><th>Domain</th><th>Detected Via</th></tr></thead><tbody>';
            for (const c of item.collections) {
                const d = item.collectionDomains[c] || '-';
                const src = item.collectionSources[c] || '-';
                html += `<tr><td><code>${esc(c)}</code></td><td><span class="sum-domain-tag">${esc(d)}</span></td><td><span class="agg-source-badge">${esc(src)}</span></td></tr>`;
            }
            html += '</tbody></table></div>';
        }

        // Pipeline stages
        if (item.pipelineStages.length) {
            html += '<div class="agg-detail-section"><b>Pipeline Stages:</b> ';
            html += item.pipelineStages.map(s => `<span class="agg-stage-badge">${esc(s)}</span>`).join(' ');
            html += '</div>';
        }

        // Rich lookup targets with field detail
        const lookupTargets = item.lookupTargets || [];
        if (lookupTargets.length) {
            html += '<div class="agg-detail-section"><b>$lookup / $graphLookup Targets:</b>';
            html += '<table class="agg-detail-table"><thead><tr><th>Type</th><th>From</th><th>localField</th><th>foreignField</th><th>As</th><th>Source</th></tr></thead><tbody>';
            for (const lk of lookupTargets) {
                html += '<tr>';
                html += `<td><code>${esc(lk.type || '$lookup')}</code></td>`;
                html += `<td><span class="agg-lookup-badge">${esc(lk.from || '-')}</span></td>`;
                html += `<td><code>${esc(lk.localField || '-')}</code></td>`;
                html += `<td><code>${esc(lk.foreignField || '-')}</code></td>`;
                html += `<td><code>${esc(lk.as || '-')}</code></td>`;
                html += `<td><span class="agg-source-badge">${esc(lk.source || '-')}</span></td>`;
                html += '</tr>';
            }
            html += '</tbody></table></div>';
        } else if (item.lookupCollections.length) {
            html += '<div class="agg-detail-section"><b>$lookup Collections (from):</b> ';
            html += item.lookupCollections.map(c => `<span class="agg-lookup-badge">${esc(c)}</span>`).join(' ');
            html += '</div>';
        }

        // Match predicates
        const matchFields = item.matchFields || [];
        if (matchFields.length) {
            html += '<div class="agg-detail-section"><b>$match Fields:</b> ';
            html += matchFields.map(f => `<code class="agg-field-chip">${esc(f)}</code>`).join(' ');
            html += '</div>';
        }

        // Group fields
        const groupFields = item.groupFields || [];
        if (groupFields.length) {
            html += '<div class="agg-detail-section"><b>$group Fields:</b> ';
            html += groupFields.map(f => `<code class="agg-field-chip">${esc(f)}</code>`).join(' ');
            html += '</div>';
        }

        // Projection fields
        const projFields = item.projectionFields || [];
        if (projFields.length) {
            html += '<div class="agg-detail-section"><b>$project Fields:</b> ';
            html += projFields.map(f => `<code class="agg-field-chip">${esc(f)}</code>`).join(' ');
            html += '</div>';
        }

        // Sort fields
        const sortFields = item.sortFields || [];
        if (sortFields.length) {
            html += '<div class="agg-detail-section"><b>$sort Fields:</b> ';
            html += sortFields.map(f => `<code class="agg-field-chip">${esc(f)}</code>`).join(' ');
            html += '</div>';
        }

        // Companion class usage
        const companions = item.companionUsage || {};
        if (Object.keys(companions).length) {
            html += '<div class="agg-detail-section"><b>Companion Classes:</b>';
            for (const [type, calls] of Object.entries(companions)) {
                html += `<div class="agg-companion-group"><span class="agg-companion-type">${esc(type)}</span>: `;
                html += calls.map(c => `<code class="agg-companion-call">${esc(c)}</code>`).join(', ');
                html += '</div>';
            }
            html += '</div>';
        }

        // Pipeline source methods (cross-method tracing)
        const srcMethods = item.pipelineSourceMethods || [];
        if (srcMethods.length) {
            html += '<div class="agg-detail-section"><b>Pipeline Built In:</b> ';
            html += srcMethods.map(m => `<code class="agg-source-method">${esc(m)}</code>`).join(' &rarr; ');
            html += '</div>';
        }

        // Query annotations
        if (item.queryAnnotations && item.queryAnnotations.length) {
            html += '<div class="agg-detail-section"><b>Query Annotations:</b>';
            for (const qa of item.queryAnnotations) {
                html += '<div class="agg-query-ann"><code>@' + esc(qa.name || '') + '</code>';
                if (qa.attributes) {
                    html += '<pre class="agg-query-value">';
                    for (const [k, v] of Object.entries(qa.attributes)) {
                        html += esc(k) + ' = ' + esc(String(v)) + '\n';
                    }
                    html += '</pre>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        // Breadcrumb
        if (item.breadcrumb && item.breadcrumb.length > 0) {
            html += '<div class="agg-detail-bc"><b>Call Path:</b> <div class="sum-bc">';
            if (typeof item.breadcrumb[0] === 'string') {
                html += item.breadcrumb.map(s => '<span class="sum-bc-seg">' + esc(s) + '</span>').join('<span class="sum-bc-arrow">&rarr;</span>');
            } else {
                html += this._renderBc(item.breadcrumb, esc);
            }
            html += '</div></div>';
        }

        html += '</div></td></tr>';
        return html;
    }
});
