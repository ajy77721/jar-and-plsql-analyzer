/**
 * Summary analyzer — endpoint analysis + report builders.
 * Produces: epReports, vertReport, extReport, distReport, batchReport, viewsReport.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _jarModuleMap: {},

    _analyzeEndpoint(ep, classIdx, entityCollMap, repoCollMap) {
        if (!ep.callTree) {
            const collections = {};
            if (ep.aggregatedCollections && ep.aggregatedCollections.length > 0) {
                const verifMap = ep.aggregatedCollectionVerification || {};
                const srcMap = ep.aggregatedCollectionSources || {};
                for (const coll of ep.aggregatedCollections) {
                    const op = (ep.aggregatedOperations && ep.aggregatedOperations[coll]) || ep.derivedOperationType || 'READ';
                    const domain = (ep.aggregatedCollectionDomains && ep.aggregatedCollectionDomains[coll]) || this._detectDomain(coll);
                    const detectedVia = srcMap[coll] || null;
                    const verification = verifMap[coll] || null;
                    this._addColl(collections, coll, op, ep.controllerSimpleName, domain, detectedVia, verification, ep.methodName);
                }
            }
            const collKeys = Object.keys(collections);
            const firstColl = (ep.aggregatedCollections && ep.aggregatedCollections[0]) || '';

            // Use server-precomputed aggregates when callTree is stripped
            const extCalls = (ep.externalCalls || []).map(e => ({
                className: e.className || '', simpleClassName: e.simpleClassName || '',
                methodName: e.methodName || '', stereotype: e.stereotype || 'EXTERNAL',
                sourceJar: e.sourceJar || 'main', module: e.module || 'External',
                domain: e.domain || 'External',
                breadcrumb: Array.isArray(e.breadcrumb) ? e.breadcrumb : []
            }));
            const hCalls = (ep.httpCalls || []).map(h => ({
                className: h.className || '', simpleClassName: h.simpleClassName || '',
                methodName: h.methodName || '', operationType: h.operationType || 'READ',
                url: h.url || '', allUrls: h.url ? [h.url] : [],
                breadcrumb: Array.isArray(h.breadcrumb) ? h.breadcrumb : []
            }));
            const beanList = (ep.beans || []).map(b => ({
                className: b.className || '', simpleClassName: b.simpleClassName || '',
                stereotype: b.stereotype || '', sourceJar: b.sourceJar || null
            }));
            const moduleNames = (ep.modules && ep.modules.length) ? ep.modules : ['main'];
            for (const e of extCalls) {
                if (e.sourceJar) { const mk = e.sourceJar; if (!this._jarModuleMap[mk]) this._jarModuleMap[mk] = e.module; }
            }
            const jarDomains = new Set();
            for (const m of moduleNames) jarDomains.add(this._jarToDomain(m));
            const domainList = [...jarDomains].sort();
            const primaryDomain = domainList[0] || this._detectDomain(firstColl) || 'Core';

            const collDomainGroups = {};
            for (const [name, c] of Object.entries(collections)) {
                (collDomainGroups[c.domain] = collDomainGroups[c.domain] || []).push(name);
            }
            const crossDomainCount = Object.keys(collDomainGroups).filter(d => d !== primaryDomain && d !== 'Other').length;

            const writeColls = collKeys.filter(c => ['WRITE','UPDATE','DELETE','CALL'].includes(ep.aggregatedOperations && ep.aggregatedOperations[c]));
            const readColls = collKeys.filter(c => ['READ','COUNT'].includes(ep.aggregatedOperations && ep.aggregatedOperations[c]));
            const aggColls = collKeys.filter(c => (ep.aggregatedOperations && ep.aggregatedOperations[c]) === 'AGGREGATE');
            const views = collKeys.filter(c => collections[c] && collections[c].type === 'VIEW');

            const procEntries = ep.procNames || [];
            const allPN = procEntries.map(p => p.procName || '').filter(Boolean);
            const procNameSources = {};
            for (const p of procEntries) {
                if (p.procName && !procNameSources[p.procName]) {
                    procNameSources[p.procName] = { simpleClassName: p.simpleClassName || '', methodName: p.methodName || '', className: p.className || '' };
                }
            }

            return {
                endpoint: ep,
                endpointName: (ep.controllerSimpleName || '') + '.' + (ep.methodName || ''),
                httpMethod: ep.httpMethod || 'ALL',
                fullPath: ep.fullPath || '/',
                procName: allPN[0] || '', allProcNames: allPN, procNameSources,
                endpointUrl: ep.fullPath || '',
                typeOfEndpoint: this._classifyEndpointType(ep),
                primaryDomain,
                beans: beanList, externalCalls: extCalls, httpCalls: hCalls, collections,
                collDomainGroups, domains: domainList,
                crossDomainCount,
                totalMethods: ep.totalMethods || 0, totalLoc: ep.totalLoc || 0,
                totalCollections: collKeys.length,
                totalViews: views.length,
                totalDbOperations: ep.totalDbOperations || 0,
                operationTypes: (ep.operationTypesList && ep.operationTypesList.length) ? ep.operationTypesList : (ep.derivedOperationType ? [ep.derivedOperationType] : []),
                writeCollections: writeColls.sort(),
                readCollections: readColls.sort(),
                aggregateCollections: aggColls.sort(),
                viewsUsed: views.sort(),
                modules: moduleNames.map(m => this._jarToProject(m)),
                inScopeCalls: ep.inScopeCalls || 0,
                externalScopeCalls: ep.externalScopeCalls || 0,
                collBreadcrumbs: this._buildSyntheticBreadcrumbs(ep, collections),
                serviceClasses: beanList.filter(b => b.stereotype === 'SERVICE').map(b => b.simpleClassName),
                sizeCategory: this._sizeCategory(ep.totalMethods || 0),
                performanceImplication: this._perfImpl(ep.totalDbOperations || 0)
            };
        }
        const controllerJar = ep.callTree.sourceJar || null;
        const beans = [], externalCalls = [], httpCalls = [];
        const collections = {}, modules = new Set(), operations = new Set();
        let totalMethods = 0, totalLoc = 0, dbOpCount = 0, inScopeCalls = 0, externalScopeCalls = 0;
        const seenBeans = new Set(), seenExt = new Set();
        const collBreadcrumbs = {};
        const allProcNames = new Set();
        const procNameSources = {}; // procName → { simpleClassName, methodName, className }

        if (controllerJar) modules.add(controllerJar);
        modules.add('main');

        this._walkWithPath(ep.callTree, (node, path) => {
            totalMethods++;
            if (node.lineCount > 0) totalLoc += node.lineCount;
            else if (node.sourceCode) totalLoc += (node.sourceCode.match(/\n/g) || []).length + 1;

            // Extract procName from annotationDetails or annotations — track source method
            if (node.annotationDetails) {
                for (const ad of node.annotationDetails) {
                    if (ad.name && ad.name.includes('LogParameters')) {
                        const attrs = ad.attributes || {};
                        const pn = attrs.procedureName || attrs.value || attrs.procName;
                        if (pn) {
                            allProcNames.add(pn);
                            if (!procNameSources[pn]) {
                                procNameSources[pn] = { simpleClassName: node.simpleClassName || '', methodName: node.methodName || '', className: node.className || '' };
                            }
                        }
                    }
                }
            }
            if (node.annotations) {
                for (const ann of (Array.isArray(node.annotations) ? node.annotations : [])) {
                    const annName = typeof ann === 'string' ? ann : (ann.name || '');
                    if (annName.includes('LogParameters') && !allProcNames.size) {
                        const attrs = typeof ann === 'object' ? (ann.attributes || {}) : {};
                        const pn = attrs.procedureName || attrs.value || attrs.procName;
                        if (pn) {
                            allProcNames.add(pn);
                            if (!procNameSources[pn]) {
                                procNameSources[pn] = { simpleClassName: node.simpleClassName || '', methodName: node.methodName || '', className: node.className || '' };
                            }
                        }
                    }
                }
            }

            // Backend-provided collectionsAccessed
            if (node.collectionsAccessed && node.collectionsAccessed.length) {
                const collSrcs = node.collectionSources || {};
                const collVerif = node.collectionVerification || {};
                const ops = node.operationTypes && node.operationTypes.length
                    ? node.operationTypes
                    : [node.operationType || this._inferOp(node.methodName, node.stereotype) || 'READ'];
                dbOpCount++;
                for (const coll of node.collectionsAccessed) {
                    const domain = (node.collectionDomains && node.collectionDomains[coll]) || this._detectDomain(coll);
                    const detectedVia = collSrcs[coll] || null;
                    const verification = collVerif[coll] || null;
                    for (const op of ops) {
                        this._addColl(collections, coll, op, node.simpleClassName, domain, detectedVia, verification, node.methodName);
                    }
                    if (collections[coll]) collections[coll].domain = domain;
                    this._addCollBc(collBreadcrumbs, coll, path);
                }
            }

            // HTTP client calls (RestTemplate, WebClient, FeignClient)
            if (node.callType === 'HTTP_CALL' || (node.id && node.id.startsWith('http:'))) {
                externalScopeCalls++;
                const hk = (node.className || '') + '#' + (node.methodName || '');
                if (!seenExt.has(hk)) {
                    seenExt.add(hk);
                    const urls = (node.stringLiterals || []).filter(s => s.startsWith('http') || s.startsWith('/'));
                    httpCalls.push({
                        className: node.className, simpleClassName: node.simpleClassName,
                        methodName: node.methodName, operationType: node.operationType || 'READ',
                        url: urls[0] || '', allUrls: urls,
                        breadcrumb: this._buildBreadcrumb(path, 5)
                    });
                }
                return;
            }

            if (node.id && node.id.startsWith('ext:')) {
                externalScopeCalls++;
                const ek = (node.className || '') + '#' + (node.methodName || '');
                if (!seenExt.has(ek)) {
                    seenExt.add(ek);
                    externalCalls.push({
                        className: node.className, simpleClassName: node.simpleClassName,
                        methodName: node.methodName, stereotype: node.stereotype || 'EXTERNAL',
                        sourceJar: 'unknown', module: node.simpleClassName || 'External',
                        domain: 'External', breadcrumb: this._buildBreadcrumb(path, 5)
                    });
                }
                return;
            }

            const nodeJar = node.sourceJar || null;
            // Populate global jar→module map from backend-provided module names
            if (node.module) {
                const mapKey = nodeJar || 'main';
                if (!this._jarModuleMap[mapKey]) {
                    this._jarModuleMap[mapKey] = node.module;
                }
            }
            const isCross = node.crossModule !== undefined ? node.crossModule : (nodeJar !== controllerJar);
            if (isCross) {
                externalScopeCalls++;
                if (nodeJar) modules.add(nodeJar);
                const ek = (nodeJar || 'main') + ':' + node.className + '#' + node.methodName;
                if (!seenExt.has(ek)) {
                    seenExt.add(ek);
                    externalCalls.push({
                        className: node.className, simpleClassName: node.simpleClassName,
                        methodName: node.methodName, stereotype: node.stereotype,
                        sourceJar: nodeJar || 'main',
                        module: node.module || this._jarToProject(nodeJar || 'main'),
                        domain: node.domain || this._jarToDomain(nodeJar || 'main'),
                        breadcrumb: this._buildBreadcrumb(path, 5)
                    });
                }
            } else { inScopeCalls++; }

            const bk = node.className || node.simpleClassName;
            if (bk && !seenBeans.has(bk)) {
                seenBeans.add(bk);
                beans.push({ className: node.className, simpleClassName: node.simpleClassName, stereotype: node.stereotype, sourceJar: nodeJar });
            }

            const op = this._inferOp(node.methodName, node.stereotype);
            if (op) operations.add(op);

            if (node.stereotype === 'REPOSITORY' || node.stereotype === 'SPRING_DATA') {
                const dbOp = this._inferOp(node.methodName, node.stereotype) || 'READ';
                dbOpCount++;
                const repoKey = node.className || node.simpleClassName;
                const directColl = repoCollMap[repoKey] || repoCollMap[node.simpleClassName];
                if (directColl) { this._addColl(collections, directColl, dbOp, node.simpleClassName, this._detectDomain(directColl) || 'Other', 'REPOSITORY_MAPPING', null, node.methodName); this._addCollBc(collBreadcrumbs, directColl, path); }
                const cls = classIdx[repoKey] || classIdx[node.simpleClassName];
                if (cls) {
                    for (const m of (cls.methods || [])) {
                        if (m.name !== node.methodName) continue;
                        for (const ann of (m.annotations || [])) {
                            for (const val of Object.values(ann.attributes || {})) {
                                const strs = Array.isArray(val) ? val.flat(3).map(String) : [String(val)];
                                for (const s of strs) {
                                    for (const ref of this._extractCollRefs(s)) {
                                        this._addColl(collections, ref, dbOp, node.simpleClassName, undefined, 'QUERY_ANNOTATION', null, node.methodName);
                                        this._addCollBc(collBreadcrumbs, ref, path);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Supplemental class index extraction — only when backend didn't provide collections.
            // Backend CollectionResolver applies NON_COLLECTION_METHODS, fieldNameExclusions,
            // and mongoCatalog filtering that the frontend can't replicate.
            // When backend data exists (collectionsAccessed populated), trust it as source of truth.
            const cls = classIdx[bk] || classIdx[node.simpleClassName];
            if (cls && !(node.collectionsAccessed && node.collectionsAccessed.length)) {
                // Only extract from class index for nodes without backend collection data
                // Infer operation from method name; default to READ — if we detected a collection
                // access, the minimum assumption is that something is reading it
                const nodeOp = this._inferOp(node.methodName, node.stereotype) || 'READ';
                for (const ann of (cls.annotations || [])) {
                    if (ann.name === 'Document' && ann.attributes) {
                        const cn = ann.attributes.collection || ann.attributes.value;
                        // Skip @Document collection on data class nodes (ENTITY/OTHER/null) — bean accessors
                        const st = node.stereotype;
                        if (cn && st && st !== 'ENTITY' && st !== 'OTHER') {
                            this._addColl(collections, cn, nodeOp, node.simpleClassName, this._detectDomain(cn) || 'Other', 'DOCUMENT_ANNOTATION', null, node.methodName);
                            this._addCollBc(collBreadcrumbs, cn, path);
                        }
                    }
                }
                // Only scan the MATCHING method — not all methods in the class.
                // Scanning all methods leaks collections from unrelated methods in shared
                // service classes (e.g., ClaimApiImpl has 194 methods; scanning all of them
                // would attach every collection to every endpoint that calls any method).
                const matchedMethods = (cls.methods || []).filter(m => m.name === node.methodName);
                for (const m of matchedMethods) {
                    const methOp = this._inferOp(m.name, cls.stereotype) || nodeOp;
                    for (const ann of (m.annotations || [])) {
                        for (const val of Object.values(ann.attributes || {})) {
                            const strs = Array.isArray(val) ? val.flat(3).map(String) : [String(val)];
                            for (const s of strs) {
                                for (const ref of this._extractCollRefs(s)) {
                                    this._addColl(collections, ref, methOp, cls.simpleName || node.simpleClassName, undefined, 'QUERY_ANNOTATION', null, node.methodName);
                                    this._addCollBc(collBreadcrumbs, ref, path);
                                }
                            }
                        }
                    }
                    for (const lit of (m.stringLiterals || [])) {
                        for (const ref of this._extractCollRefs(lit)) {
                            this._addColl(collections, ref, methOp, cls.simpleName || node.simpleClassName, undefined, 'STRING_LITERAL', null, m.name);
                            this._addCollBc(collBreadcrumbs, ref, path);
                        }
                    }
                }
                // Field constants: only for REPOSITORY/SPRING_DATA classes, matching
                // the backend CollectionResolver guard. SERVICE/COMPONENT classes often
                // have field constants (e.g., collection name strings for MongoTemplate
                // calls) that would leak to every method in the class as false positives.
                const st = cls.stereotype || node.stereotype;
                if (st === 'REPOSITORY' || st === 'SPRING_DATA') {
                    for (const f of (cls.fields || [])) {
                        if (f.constantValue) {
                            for (const ref of this._extractCollRefs(f.constantValue)) {
                                this._addColl(collections, ref, nodeOp, cls.simpleName || node.simpleClassName, undefined, 'FIELD_CONSTANT');
                            }
                        }
                    }
                }
            }
        });

        // Collection domain grouping
        const collDomainGroups = {};
        for (const [name, c] of Object.entries(collections)) {
            (collDomainGroups[c.domain] = collDomainGroups[c.domain] || []).push(name);
        }

        const writeColls = [], readColls = [], aggColls = [];
        for (const [name, c] of Object.entries(collections)) {
            if (c.operations.has('WRITE') || c.operations.has('UPDATE') || c.operations.has('CALL')) writeColls.push(name);
            if (c.operations.has('READ') || c.operations.has('COUNT')) readColls.push(name);
            if (c.operations.has('AGGREGATE')) aggColls.push(name);
        }
        const views = Object.entries(collections).filter(([_, c]) => c.type === 'VIEW').map(([n]) => n);

        const jarDomains = new Set();
        for (const m of modules) jarDomains.add(this._jarToDomain(m));
        const domainList = [...jarDomains].sort();
        const moduleNames = [...modules].map(m => this._jarToProject(m));
        const primaryDomain = domainList[0] || 'Core';
        const crossDomainCount = Object.keys(collDomainGroups).filter(d => d !== primaryDomain && d !== 'Other').length;

        return {
            endpoint: ep,
            endpointName: (ep.controllerSimpleName || '') + '.' + (ep.methodName || ''),
            httpMethod: ep.httpMethod || 'ALL',
            fullPath: ep.fullPath || '/',
            procName: [...allProcNames][0] || '',
            allProcNames: [...allProcNames],
            procNameSources,
            endpointUrl: ep.fullPath || '',
            typeOfEndpoint: this._classifyEndpointType(ep),
            primaryDomain,
            beans, collections, collDomainGroups,
            domains: domainList,
            crossDomainCount,
            totalMethods, totalLoc,
            totalCollections: Object.keys(collections).length,
            totalViews: views.length,
            totalDbOperations: dbOpCount,
            operationTypes: [...operations].sort(),
            writeCollections: writeColls.sort(),
            readCollections: readColls.sort(),
            aggregateCollections: aggColls.sort(),
            viewsUsed: views.sort(),
            externalCalls, httpCalls,
            modules: moduleNames,
            inScopeCalls, externalScopeCalls,
            collBreadcrumbs,
            serviceClasses: beans.filter(b => b.stereotype === 'SERVICE').map(b => b.simpleClassName),
            sizeCategory: this._sizeCategory(totalMethods),
            performanceImplication: this._perfImpl(dbOpCount)
        };
    },

    /* ===== Distributed Transactions ===== */
    _buildDistributedTransactions(epReports) {
        const results = [];
        for (const r of epReports) {
            const domainWrites = {}, domainReads = {}, domainAggs = {};
            for (const [name, c] of Object.entries(r.collections)) {
                const d = c.domain;
                if (c.operations.has('WRITE') || c.operations.has('UPDATE') || c.operations.has('DELETE')) {
                    (domainWrites[d] = domainWrites[d] || []).push(name);
                }
                if (c.operations.has('READ') || c.operations.has('COUNT')) {
                    (domainReads[d] = domainReads[d] || []).push(name);
                }
                if (c.operations.has('AGGREGATE')) {
                    (domainAggs[d] = domainAggs[d] || []).push(name);
                }
            }
            const writeDomains = Object.keys(domainWrites);
            const txnReq = writeDomains.length > 1
                ? 'REQUIRED - cross-domain writes detected (' + writeDomains.join(', ') + ')'
                : writeDomains.length === 1 ? 'SINGLE-DOMAIN' : 'NOT REQUIRED';

            const crossDeps = {};
            const allDomains = new Set([...Object.keys(domainWrites), ...Object.keys(domainReads), ...Object.keys(domainAggs)]);
            for (const d of allDomains) {
                crossDeps[d] = {
                    read: domainReads[d] || [],
                    write: domainWrites[d] || [],
                    aggregate: domainAggs[d] || []
                };
            }

            results.push({
                endpointName: r.endpointName, procName: r.procName, allProcNames: r.allProcNames,
                httpMethod: r.httpMethod, fullPath: r.fullPath, typeOfEndpoint: r.typeOfEndpoint,
                primaryDomain: r.primaryDomain, transactionRequirement: txnReq,
                crossDomainDependencies: crossDeps, totalCollections: r.totalCollections,
                totalLoc: r.totalLoc, performanceImplication: r.performanceImplication
            });
        }
        return results;
    },

    /* ===== Batch Job Analysis ===== */
    _buildBatchAnalysis(epReports) {
        const results = [];
        for (const r of epReports) {
            if (!this._isBatchEndpoint(r)) continue;
            const extDeps = {};
            for (const [name, c] of Object.entries(r.collections)) {
                const d = c.domain;
                if (!extDeps[d]) extDeps[d] = [];
                extDeps[d].push({ collection: name, objectType: c.type, usageTypes: [...c.operations] });
            }
            results.push({
                batchName: r.endpointName, procName: r.procName, allProcNames: r.allProcNames,
                endpointUrl: r.fullPath, primaryDomain: r.primaryDomain, domains: r.domains,
                totalCollections: r.totalCollections, totalMethods: r.totalMethods,
                totalLoc: r.totalLoc,
                externalDependencies: extDeps, sizeCategory: r.sizeCategory,
                performanceImplication: r.performanceImplication
            });
        }
        return results;
    },

    _isBatchEndpoint(r) {
        const text = ((r.fullPath || '') + ' ' + (r.endpointName || '')).toLowerCase();
        for (const kw of (this._batchKeywords || [])) {
            if (text.includes(kw)) return true;
        }
        return false;
    },

    /* ===== Views Analysis ===== */
    _buildViewsAnalysis(vertReport, epReports) {
        return vertReport.filter(c => c.type === 'VIEW').map(c => ({
            viewName: c.name, domain: c.domain, dependencyOfOtherViews: [],
            viewDefinition: [...c.operations].map(op => op + ' via repository'),
            fields: [], recursive: false,
            possibleAlternative: c.usageCount <= 1 ? 'Evaluate: direct query on base collection may suffice' : '',
            usedByEndpoints: [...c.endpoints], usageCount: c.usageCount,
            complexity: this._calcComplexity(c)
        }));
    },

    /* ===== Verticalisation ===== */
    _buildVerticalisation(epReports) {
        const READ_OPS = new Set(['READ', 'COUNT', 'EXISTS']);
        const WRITE_OPS = new Set(['WRITE', 'UPDATE', 'DELETE']);
        const AGG_OPS = new Set(['AGGREGATE']);
        const collMap = {};
        for (const r of epReports) {
            for (const [name, c] of Object.entries(r.collections)) {
                if (!collMap[name]) {
                    collMap[name] = { name, type: c.type, domain: c.domain, endpoints: new Set(), operations: new Set(), sources: new Set(), detectedVia: new Set(), usageCount: 0, breadcrumbs: [], procNames: new Set(), verification: null, readOps: new Set(), writeOps: new Set(), aggOps: new Set(), domainSet: new Set(), opCounts: {} };
                }
                collMap[name].endpoints.add(r.endpointName);
                for (const op of c.operations) {
                    collMap[name].operations.add(op);
                    if (READ_OPS.has(op)) collMap[name].readOps.add(op);
                    if (WRITE_OPS.has(op)) collMap[name].writeOps.add(op);
                    if (AGG_OPS.has(op)) collMap[name].aggOps.add(op);
                    collMap[name].opCounts[op] = (collMap[name].opCounts[op] || 0) + 1;
                }
                for (const s of c.sources) collMap[name].sources.add(s);
                if (c.detectedVia) { for (const d of c.detectedVia) collMap[name].detectedVia.add(d); }
                // Propagate verification: prefer VERIFIED/CLAUDE_VERIFIED over null/NOT_IN_DB
                if (c.verification) {
                    const cur = collMap[name].verification;
                    const isUpgrade = !cur || cur === 'NO_CATALOG'
                        || (cur === 'NOT_IN_DB' && (c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED'));
                    if (isUpgrade) collMap[name].verification = c.verification;
                }
                collMap[name].usageCount++;
                if (r.procName) collMap[name].procNames.add(r.procName);
                if (r.primaryDomain) collMap[name].domainSet.add(r.primaryDomain);
                if (r.collBreadcrumbs[name]) {
                    for (const bc of r.collBreadcrumbs[name]) {
                        if (collMap[name].breadcrumbs.length < 5) collMap[name].breadcrumbs.push(bc);
                    }
                }
            }
        }
        return Object.values(collMap).sort((a, b) => a.domain !== b.domain ? a.domain.localeCompare(b.domain) : a.type !== b.type ? (a.type === 'VIEW' ? 1 : -1) : a.name.localeCompare(b.name));
    },

    /* ===== External Dependencies ===== */
    _buildExternalDeps(epReports) {
        const moduleMap = {};
        for (const r of epReports) {
            for (const ext of r.externalCalls) {
                const jar = ext.sourceJar || 'main';
                if (!moduleMap[jar]) {
                    moduleMap[jar] = { jar, domain: this._jarToDomain(jar), project: this._jarToProject(jar), classes: new Set(), endpoints: new Set(), methods: new Set(), count: 0, breadcrumbs: [] };
                }
                moduleMap[jar].classes.add(ext.simpleClassName);
                moduleMap[jar].endpoints.add(r.endpointName);
                moduleMap[jar].methods.add(ext.simpleClassName + '.' + ext.methodName);
                moduleMap[jar].count++;
                if (ext.breadcrumb && moduleMap[jar].breadcrumbs.length < 5) moduleMap[jar].breadcrumbs.push(ext.breadcrumb);
            }
        }
        return { crossModule: Object.values(moduleMap).sort((a, b) => b.count - a.count) };
    },

    _buildSyntheticBreadcrumbs(ep, collections) {
        const result = {};
        const ctrl = ep.controllerSimpleName || '';
        const meth = ep.methodName || '';
        for (const [collName, cInfo] of Object.entries(collections)) {
            const segs = [];
            if (ctrl) segs.push({ label: ctrl + '.' + meth + '()', full: ep.controllerClass || ctrl, className: ctrl, methodName: meth, level: 0 });
            if (cInfo.sourceMethodMap) {
                for (const [src, srcMeth] of Object.entries(cInfo.sourceMethodMap)) {
                    segs.push({ label: src + '.' + (srcMeth || '?') + '()', full: src, className: src, methodName: srcMeth || '', level: 1 });
                }
            } else if (cInfo.sources && cInfo.sources.size) {
                for (const src of cInfo.sources) {
                    segs.push({ label: src + '()', full: src, className: src, methodName: '', level: 1 });
                }
            }
            if (segs.length > 0) result[collName] = [segs];
        }
        return result;
    }
});
