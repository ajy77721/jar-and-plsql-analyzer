/**
 * Summary — Dynamic Flows sub-tab.
 * Table view with top-level dispatch type filter pills, pagination,
 * expandable details with code navigation (breadcrumbs + decompile modal).
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _renderDynamicTab(epReports, esc) {
        const allRows = [];
        const typeCounts = {};

        for (const r of epReports) {
            const ep = r.endpoint;
            if (!ep) continue;

            // Use precomputed dynamicFlows if available, otherwise walk callTree
            let flows = [];
            if (ep.dynamicFlows && ep.dynamicFlows.length > 0) {
                flows = ep.dynamicFlows;
            } else if (ep.callTree) {
                const dynamics = { dispatches: [], reflections: [], dynamicSql: [] };
                this._walkDynamicNodes(ep.callTree, dynamics, []);
                for (const d of dynamics.dispatches) flows.push({ category: 'DISPATCH', dispatchType: d.dispatchType || 'UNKNOWN', className: d.className || '', simpleClassName: d.simpleClassName || '', methodName: d.methodName || '', resolvedFrom: d.resolvedFrom || '', qualifierHint: d.qualifierHint || '', breadcrumb: d.breadcrumbPath || [], pattern: '' });
                for (const ref of dynamics.reflections) flows.push({ category: 'REFLECTION', dispatchType: 'REFLECTION', className: ref.className || '', simpleClassName: ref.simpleClassName || '', methodName: ref.methodName || '', resolvedFrom: '', qualifierHint: '', breadcrumb: ref.breadcrumbPath || [], pattern: ref.pattern || '' });
                for (const q of dynamics.dynamicSql) flows.push({ category: 'DYNAMIC_QUERY', dispatchType: 'DYNAMIC_QUERY', className: q.className || '', simpleClassName: q.simpleClassName || '', methodName: q.methodName || '', resolvedFrom: '', qualifierHint: '', breadcrumb: q.breadcrumbPath || [], pattern: q.pattern || '' });
            } else {
                continue;
            }

            for (const f of flows) {
                const dtype = f.dispatchType || 'UNKNOWN';
                typeCounts[dtype] = (typeCounts[dtype] || 0) + 1;
                allRows.push({
                    category: f.category || dtype,
                    dispatchType: dtype,
                    className: f.className || '',
                    simpleClassName: f.simpleClassName || '',
                    methodName: f.methodName || '',
                    resolvedFrom: f.resolvedFrom || '',
                    qualifierHint: f.qualifierHint || '',
                    breadcrumb: f.breadcrumb || [],
                    pattern: f.pattern || '',
                    endpointName: r.endpointName || '',
                    fullPath: r.fullPath || '',
                    httpMethod: r.httpMethod || '',
                    primaryDomain: r.primaryDomain || ''
                });
            }
        }

        if (!allRows.length) {
            let html = '<div class="sum-section" style="padding:30px">';
            html += '<div class="sum-section-title">Dynamic Flows</div>';
            html += '<p class="sum-muted">No dynamic dispatch, reflection, or dynamic SQL patterns detected.</p>';
            html += '</div>';
            return html;
        }

        // Build dispatch type pills
        const allTypes = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);
        let html = '<div class="sum-section">';
        html += '<div class="sum-section-title">Dynamic Flows (' + allRows.length + ' items)</div>';
        html += '<div class="sum-section-desc">Non-direct method dispatch, reflection, and dynamic queries. Click a row to see the call path.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Spring injects the implementation at runtime based on @Qualifier, @Primary, or profile. The static analyzer resolved the most likely target, but the actual implementation may differ in production.">INTERFACE_DISPATCH: resolved by Spring injection, verify @Qualifier</span>';
        html += '<span class="sum-tip" title="Method.invoke(), Class.forName(), or proxy-based calls. Completely opaque to static analysis — the target class and method are determined at runtime. Always needs manual review.">REFLECTION: fully opaque, manual review required</span>';
        html += '<span class="sum-tip" title="MongoDB queries built with Criteria API, Aggregation framework, or string concatenation. The collections and operations depend on runtime values. These may access collections not captured in other tabs.">DYNAMIC_QUERY: runtime-built DB queries, may hide collection access</span>';
        html += '<span class="sum-tip" title="Strategy/factory patterns where the implementation is selected based on configuration or runtime conditions. Check which implementations exist and which are actually used in production.">STRATEGY_DISPATCH: check all implementations, not just the resolved one</span>';
        html += '</div>';

        // Type filter pills
        html += '<div class="dyn-filter-pills" id="dyn-type-pills">';
        html += '<span class="dyn-pill active" data-dtype="" onclick="JA.summary._dynFilterType(\'\')">ALL <b>' + allRows.length + '</b></span>';
        for (const [dtype, count] of allTypes) {
            const label = dtype.replace(/_/g, ' ');
            html += `<span class="dyn-pill dyn-type-${dtype.toLowerCase().replace(/_/g, '-')}" data-dtype="${esc(dtype)}" onclick="JA.summary._dynFilterType('${esc(dtype)}')">${esc(label)} <b>${count}</b></span>`;
        }
        html += '</div>';

        // Filter bar (text + domain)
        html += this._buildFilterBar('sum-dyn', allRows, r => r.primaryDomain);

        // Table
        html += '<div id="sum-dyn-pager-top" class="sum-pager" style="margin-bottom:4px"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="JA.summary._pageSort(\'sum-dyn\',0)" style="width:130px">Type</th>';
        html += '<th data-sort-col="1" onclick="JA.summary._pageSort(\'sum-dyn\',1)">Class.Method</th>';
        html += '<th data-sort-col="2" onclick="JA.summary._pageSort(\'sum-dyn\',2)">Resolved From</th>';
        html += '<th data-sort-col="3" onclick="JA.summary._pageSort(\'sum-dyn\',3)">Endpoint</th>';
        html += '<th data-sort-col="4" onclick="JA.summary._pageSort(\'sum-dyn\',4)" style="width:90px">Domain</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-dyn-tbody"></tbody>';
        html += '</table></div>';

        html += '<div id="sum-dyn-pager" class="sum-pager"></div>';
        html += '</div>';

        // Store type filter state
        this._dynActiveType = '';

        // Initialize pagination
        this._initPage('sum-dyn', allRows, 25,
            (item, idx, esc) => this._renderDynRow(item, idx, esc),
            r => r.primaryDomain,
            (item, idx, esc) => this._renderDynDetail(item, idx, esc),
            {
                sortKeys: {
                    0: { fn: r => r.dispatchType || '' },
                    1: { fn: r => (r.simpleClassName || '') + '.' + (r.methodName || '') },
                    2: { fn: r => r.resolvedFrom || r.pattern || '' },
                    3: { fn: r => r.fullPath || '' },
                    4: { fn: r => r.primaryDomain || '' }
                },
                typeFilterFn: item => item.dispatchType || ''
            }
        );

        setTimeout(() => {
            this._pageRender('sum-dyn');
            this._initColFilters('sum-dyn', {
                0: { label: 'Type', valueFn: r => r.dispatchType || '' },
                2: { label: 'Resolved From', valueFn: r => r.resolvedFrom || r.pattern || '' },
                4: { label: 'Domain', valueFn: r => r.primaryDomain || '' }
            });
        }, 0);
        return html;
    },

    _dynActiveType: '',

    _dynFilterType(dtype) {
        this._dynActiveType = dtype;
        // Update pill active states
        document.querySelectorAll('#dyn-type-pills .dyn-pill').forEach(p => {
            p.classList.toggle('active', p.dataset.dtype === dtype);
        });
        // Apply type filter via the page state
        const s = this._pageState['sum-dyn'];
        if (!s) return;
        s._typeFilter = dtype;
        this._pageFilter('sum-dyn');
    },

    _renderDynRow(item, idx, esc) {
        const typeLabel = (item.dispatchType || '').replace(/_/g, ' ');
        const typeClass = 'dyn-type-' + (item.dispatchType || '').toLowerCase().replace(/_/g, '-');
        const classMethod = (item.simpleClassName || '?') + '.' + (item.methodName || '?') + '()';

        let resolvedCol = '';
        if (item.resolvedFrom) {
            const shortIface = item.resolvedFrom.split('.').pop();
            resolvedCol = '<span class="dyn-from-iface">from <code>' + esc(shortIface) + '</code></span>';
            if (item.qualifierHint) resolvedCol += ' <span class="dyn-qualifier">@' + esc(item.qualifierHint) + '</span>';
        } else if (item.pattern) {
            resolvedCol = '<code class="dyn-pattern">' + esc(item.pattern) + '</code>';
        }

        let html = `<tr class="sum-clickable-row" onclick="JA.summary.toggleDetail('dyn',${idx})">`;
        html += `<td><span class="dyn-type-pill ${typeClass}">${esc(typeLabel)}</span></td>`;
        html += `<td><code class="dyn-class-method" onclick="event.stopPropagation();JA.summary.showClassCode('${esc(item.className.replace(/'/g, "\\'"))}','${esc(item.methodName.replace(/'/g, "\\'"))}')" title="${esc(item.className)}">${esc(classMethod)}</code></td>`;
        html += `<td>${resolvedCol}</td>`;
        html += `<td><span class="endpoint-method method-${item.httpMethod}">${esc(item.httpMethod)}</span> <span title="${esc(item.fullPath)}">${esc(item.endpointName || item.fullPath)}</span></td>`;
        html += `<td><span class="sum-domain-tag">${esc(item.primaryDomain)}</span></td>`;
        html += '</tr>';
        return html;
    },

    _renderDynDetail(item, idx, esc) {
        let html = `<tr class="sum-detail-row" id="sum-dyn-detail-${idx}" style="display:none"><td colspan="5"><div class="dyn-detail-content">`;

        // Dispatch info section
        html += '<div class="dyn-detail-info">';
        html += '<div class="dyn-detail-field"><b>Full Class:</b> <code onclick="JA.summary.showClassCode(\'' + esc(item.className.replace(/'/g, "\\'")) + '\',\'' + esc(item.methodName.replace(/'/g, "\\'")) + '\')" class="sum-clickable">' + esc(item.className + '.' + item.methodName + '()') + '</code></div>';
        if (item.resolvedFrom) {
            html += '<div class="dyn-detail-field"><b>Interface:</b> <code onclick="JA.summary.showClassCode(\'' + esc(item.resolvedFrom.replace(/'/g, "\\'")) + '\')" class="sum-clickable">' + esc(item.resolvedFrom) + '</code></div>';
        }
        if (item.qualifierHint) {
            html += '<div class="dyn-detail-field"><b>Qualifier:</b> <code>@Qualifier("' + esc(item.qualifierHint) + '")</code></div>';
        }
        if (item.pattern) {
            html += '<div class="dyn-detail-field"><b>Pattern:</b> <code>' + esc(item.pattern) + '</code></div>';
        }
        html += '<div class="dyn-detail-field"><b>Endpoint:</b> <span class="endpoint-method method-' + item.httpMethod + '">' + esc(item.httpMethod) + '</span> ' + esc(item.fullPath) + '</div>';
        html += '</div>';

        // Breadcrumb / call path
        if (item.breadcrumb && item.breadcrumb.length > 0) {
            html += '<div class="dyn-detail-bc"><b>Call Path:</b> <div class="sum-bc">';
            html += this._renderBc(item.breadcrumb, esc);
            html += '</div></div>';
        }

        html += '</div></td></tr>';
        return html;
    },

    _walkDynamicNodes(node, out, path) {
        if (!node) return;

        const cur = [...path, node];
        const bcPath = this._buildBreadcrumb(cur, 6);

        // Non-direct dispatch
        if (node.dispatchType && node.dispatchType !== 'DIRECT' && node.dispatchType !== 'SPRING_DATA_DERIVED') {
            out.dispatches.push({
                simpleClassName: node.simpleClassName || node.className || '',
                className: node.className || node.simpleClassName || '',
                methodName: node.methodName || '',
                dispatchType: node.dispatchType,
                resolvedFrom: node.resolvedFrom || null,
                qualifierHint: node.qualifierHint || null,
                breadcrumbPath: bcPath
            });
        }

        // Reflection patterns in string literals
        const reflPatterns = ['Class.forName', 'Method.invoke', 'getMethod', 'getDeclaredMethod', 'newInstance'];
        if (node.stringLiterals) {
            for (const lit of node.stringLiterals) {
                for (const pat of reflPatterns) {
                    if (lit.includes(pat)) {
                        out.reflections.push({
                            simpleClassName: node.simpleClassName || '',
                            className: node.className || node.simpleClassName || '',
                            methodName: node.methodName || '',
                            pattern: pat + ': ' + lit,
                            breadcrumbPath: bcPath
                        });
                    }
                }
            }
        }

        // Dynamic query patterns
        if (node.annotations) {
            for (const ann of node.annotations) {
                const annName = typeof ann === 'string' ? ann : (ann.name || ann);
                if (annName === 'Query' && node.annotationDetails) {
                    for (const ad of node.annotationDetails) {
                        if (ad.name && ad.name.includes('Query') && ad.attributes) {
                            const queryVal = ad.attributes.value || '';
                            if (typeof queryVal === 'string' && (queryVal.includes('?') || queryVal.includes('#{'))) {
                                out.dynamicSql.push({
                                    simpleClassName: node.simpleClassName || '',
                                    className: node.className || node.simpleClassName || '',
                                    methodName: node.methodName || '',
                                    pattern: 'Parameterized @Query',
                                    breadcrumbPath: bcPath
                                });
                            }
                        }
                    }
                }
            }
        }

        if (node.children) {
            for (const child of node.children) {
                this._walkDynamicNodes(child, out, cur);
            }
        }
    }
});
