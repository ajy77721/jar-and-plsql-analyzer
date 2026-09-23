package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * UI-026a — server-side port of {@code jar/js/summary-analyzer.js:_analyzeEndpoint}'s
 * <b>no-call-tree branch</b> (the {@code if (!ep.callTree)} arm), for one endpoint from the
 * <i>summary</i> document ({@code _summary_static.json}) — the same document
 * {@link SummaryStats#compute} already reads, and the branch its own javadoc names as the one this
 * project's summary writer selects by stripping {@code callTree}.
 *
 * <p>Real data checked, not assumed: a summary document's endpoint entries carry
 * {@code aggregatedCollections}, {@code aggregatedCollectionDomains}, {@code aggregatedOperations},
 * {@code externalCalls}, {@code httpCalls}, {@code beans}, {@code modules}, {@code procNames} in
 * full — none of the fields this branch reads are stripped from the source file (only from the
 * {@code /summary/headers} HTTP response, which is a separate, later filtering step).
 *
 * <h2>What this covers, and what it deliberately does not yet</h2>
 *
 * <p>The per-endpoint, order-independent core: collection aggregation (mirroring {@code _addColl}
 * field for field, including its confidence rule), operation classification, external/HTTP call
 * mapping, view/read/write/aggregate collection sets, {@code procName} extraction, the
 * config-driven scoring ({@code typeOfEndpoint}/{@code sizeCategory}/{@code performanceImplication}),
 * {@code collDomainGroups}, {@code serviceClasses}, and {@code collBreadcrumbs}
 * ({@code _buildSyntheticBreadcrumbs}) — the last three were first assumed to need the same
 * stateful map the next paragraph describes, by association with the fields next to them in the
 * JS source, and moved here once rereading the actual code showed they do not.
 *
 * <p>{@code primaryDomain}, {@code modules}, {@code domains}, and {@code crossDomainCount} (which
 * compares against {@code primaryDomain}) depend on {@code _jarToProject}/{@code _jarToDomain},
 * which read a client-side global, {@code _jarModuleMap} — a map built up <b>across every endpoint
 * processed so far in the render pass</b> (first {@code sourceJar} write wins, in processing
 * order), not a pure function of one endpoint. The overload of {@link #build} that takes a
 * {@code jarModuleMap} parameter ports these four faithfully by making that same map an explicit,
 * caller-owned argument instead of hidden global state: a caller processing a whole batch passes
 * the same mutable map to every {@code build} call, in the same order the endpoints appear in the
 * document, exactly mirroring the single {@code JA.summary._jarModuleMap} instance the browser
 * mutates across one render pass. The no-map overload below remains for a caller that only needs
 * the order-independent core and does not want to opt into this ordering contract.
 */
public final class EndpointReportBuilder {

    private EndpointReportBuilder() {
    }

    private static final Set<String> WRITE_OPS = Set.of("WRITE", "UPDATE", "DELETE", "CALL");
    private static final Set<String> READ_OPS = Set.of("READ", "COUNT");

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? "" : n.asText("");
    }

    /** One collection's accumulated state — {@code _addColl}'s object, transcribed. */
    private static final class Coll {
        String type;
        String domain;
        final Set<String> operations = new LinkedHashSet<>();
        final Set<String> sources = new LinkedHashSet<>();
        final Map<String, String> sourceMethodMap = new LinkedHashMap<>();
        final Set<String> detectedVia = new LinkedHashSet<>();
        String verification;
    }

    /** {@code _classifyColl}, transcribed: view detection over the same config lists
     *  {@link SummaryStats.Config} already carries for the headline chips. */
    private static String classifyColl(String name, SummaryStats.Config cfg) {
        if (name == null || name.isEmpty()) return "COLLECTION";
        for (String s : cfg.viewContains()) if (name.contains(s)) return "VIEW";
        for (String s : cfg.viewStartsWith()) if (name.startsWith(s)) return "VIEW";
        return "COLLECTION";
    }

    /**
     * {@code _addColl}, transcribed. In the no-call-tree branch {@code forceDomain} is always
     * supplied (the caller resolves it as {@code aggregatedCollectionDomains[c] || detectDomain(c)}
     * before calling), so the "drop a low-confidence, unforced Other" branch never fires here —
     * matching {@link SummaryStats}'s own note that every entry counts on this path.
     */
    private static void addColl(Map<String, Coll> collections, String name, String operation,
                                String source, String forceDomain, String detectedVia,
                                String verification, String methodName, SummaryStats.Config cfg) {
        if (name == null || name.isEmpty()) return;
        Coll c = collections.get(name);
        if (c == null) {
            c = new Coll();
            c.type = classifyColl(name, cfg);
            c.domain = (forceDomain == null || forceDomain.isEmpty())
                    ? SummaryStats.detectDomain(name, cfg) : forceDomain;
            collections.put(name, c);
        }
        if (operation != null && !operation.isEmpty()) c.operations.add(operation);
        if (source != null && !source.isEmpty()) c.sources.add(source);
        if (source != null && !source.isEmpty() && methodName != null && !methodName.isEmpty()
                && !c.sourceMethodMap.containsKey(source)) {
            c.sourceMethodMap.put(source, methodName);
        }
        if (detectedVia != null && !detectedVia.isEmpty()) c.detectedVia.add(detectedVia);
        if (verification != null && !verification.isEmpty() && !"NO_CATALOG".equals(verification)) {
            c.verification = verification;
        }
    }

    /** {@code _classifyEndpointType}, transcribed. */
    static String classifyEndpointType(JsonNode ep, SummaryStats.Config cfg) {
        String haystack = (text(ep.get("fullPath")) + ' ' + text(ep.get("methodName")))
                .toLowerCase(Locale.ROOT);
        for (String kw : cfg.batchKeywords()) {
            if (!kw.isEmpty() && haystack.contains(kw)) return "BATCH";
        }
        return "REST";
    }

    /** {@code _sizeCategory}, transcribed, with the JS's own hard-coded fallback thresholds. */
    static String sizeCategory(long methods, Map<String, Object> sizeThresholds) {
        long s = threshold(sizeThresholds, "S", 5);
        long m = threshold(sizeThresholds, "M", 20);
        long l = threshold(sizeThresholds, "L", 50);
        if (methods <= s) return "S";
        if (methods <= m) return "M";
        if (methods <= l) return "L";
        return "XL";
    }

    private static long threshold(Map<String, Object> t, String key, long fallback) {
        if (t == null) return fallback;
        Object v = t.get(key);
        return v instanceof Number n ? n.longValue() : fallback;
    }

    /** {@code _perfImpl}, transcribed: first threshold whose {@code maxDbOps} the count clears. */
    @SuppressWarnings("unchecked")
    static String perfImpl(long dbOps, List<Object> perfThresholds) {
        if (perfThresholds == null || perfThresholds.isEmpty()) return "Unknown";
        String last = "Unknown";
        for (Object o : perfThresholds) {
            if (!(o instanceof Map<?, ?> t)) continue;
            Object maxOpsObj = t.get("maxDbOps");
            Object label = t.get("label");
            last = label != null ? String.valueOf(label) : last;
            if (maxOpsObj instanceof Number n && dbOps <= n.longValue()) {
                return label != null ? String.valueOf(label) : "Unknown";
            }
        }
        return last;
    }

    /**
     * Builds one endpoint's report, without the batch-order-dependent fields ({@code
     * primaryDomain}/{@code modules}/{@code domains}/{@code crossDomainCount} are computed as
     * though this were the only endpoint in the run — no prior {@code _jarModuleMap} contribution
     * from another endpoint is possible). For the ordering-faithful version, see {@link
     * #build(ObjectMapper, JsonNode, SummaryStats.Config, Map, List, Map, String)}.
     */
    public static ObjectNode build(ObjectMapper mapper, JsonNode ep, SummaryStats.Config cfg,
                                   Map<String, Object> sizeThresholds, List<Object> perfThresholds) {
        return build(mapper, ep, cfg, sizeThresholds, perfThresholds, new LinkedHashMap<>(), null);
    }

    /**
     * Builds one endpoint's report. {@code ep} must lack {@code callTree} — the caller's job to
     * check, the same precondition the JS branches on, since this class only transcribes the one
     * arm.
     *
     * @param jarModuleMap the same mutable map passed to every {@code build} call in a batch, in
     *                     document order — {@code JA.summary._jarModuleMap}'s server-side
     *                     equivalent. Updated in place from this endpoint's own
     *                     {@code externalCalls} (first {@code sourceJar} write wins), then
     *                     consulted for {@code primaryDomain}/{@code modules}/{@code domains}/
     *                     {@code crossDomainCount} — matching the JS's own within-endpoint order
     *                     (the map is updated from this endpoint's calls before being read for
     *                     this same endpoint's domain list).
     * @param uploadedJarName the analysis's uploaded JAR file name ({@code JA.app.currentAnalysis
     *                        .jarName}), used to derive the {@code "main"} module label when the
     *                        map has no {@code "main"} entry yet
     */
    public static ObjectNode build(ObjectMapper mapper, JsonNode ep, SummaryStats.Config cfg,
                                   Map<String, Object> sizeThresholds, List<Object> perfThresholds,
                                   Map<String, String> jarModuleMap, String uploadedJarName) {
        Map<String, Coll> collections = new LinkedHashMap<>();
        String firstColl = "";
        JsonNode aggColls = ep.get("aggregatedCollections");
        if (aggColls != null && aggColls.isArray() && !aggColls.isEmpty()) {
            firstColl = text(aggColls.get(0));
            JsonNode domainMap = ep.get("aggregatedCollectionDomains");
            JsonNode opMap = ep.get("aggregatedOperations");
            JsonNode verifMap = ep.get("aggregatedCollectionVerification");
            JsonNode srcMap = ep.get("aggregatedCollectionSources");
            String derivedOp = text(ep.get("derivedOperationType"));
            String controllerSimpleName = text(ep.get("controllerSimpleName"));
            String methodName = text(ep.get("methodName"));
            for (JsonNode cn : aggColls) {
                String coll = text(cn);
                if (coll.isEmpty()) continue;
                String op = opMap != null ? text(opMap.get(coll)) : "";
                if (op.isEmpty()) op = derivedOp;
                if (op.isEmpty()) op = "READ";
                String domain = domainMap != null ? text(domainMap.get(coll)) : "";
                if (domain.isEmpty()) domain = SummaryStats.detectDomain(coll, cfg);
                String detectedVia = srcMap != null ? text(srcMap.get(coll)) : null;
                String verification = verifMap != null ? text(verifMap.get(coll)) : null;
                addColl(collections, coll, op, controllerSimpleName, domain, detectedVia,
                        verification, methodName, cfg);
            }
        }

        List<String> collKeys = new ArrayList<>(collections.keySet());
        List<String> writeColls = new ArrayList<>();
        List<String> readColls = new ArrayList<>();
        List<String> aggColls2 = new ArrayList<>();
        List<String> views = new ArrayList<>();
        JsonNode rawOpMap = ep.get("aggregatedOperations");
        for (String c : collKeys) {
            Coll info = collections.get(c);
            // The JS reads `ep.aggregatedOperations[c]` RAW here — not the resolved-with-fallback
            // op `_addColl` stored — so a collection with no entry in that map classifies into
            // none of write/read/agg, even though `_addColl` itself defaulted it to READ. Matched
            // exactly: using the resolved op here would silently reclassify such collections.
            String rawOp = rawOpMap != null ? text(rawOpMap.get(c)) : "";
            if (WRITE_OPS.contains(rawOp)) writeColls.add(c);
            if (READ_OPS.contains(rawOp)) readColls.add(c);
            if ("AGGREGATE".equals(rawOp)) aggColls2.add(c);
            if ("VIEW".equals(info.type)) views.add(c);
        }
        writeColls.sort(null);
        readColls.sort(null);
        aggColls2.sort(null);
        views.sort(null);

        ArrayNode beanList = mapper.createArrayNode();
        JsonNode beansSrc = ep.get("beans");
        if (beansSrc != null && beansSrc.isArray()) {
            for (JsonNode b : beansSrc) {
                ObjectNode o = mapper.createObjectNode();
                o.put("className", text(b.get("className")));
                o.put("simpleClassName", text(b.get("simpleClassName")));
                o.put("stereotype", text(b.get("stereotype")));
                JsonNode sj = b.get("sourceJar");
                if (sj == null || sj.isNull()) o.putNull("sourceJar"); else o.put("sourceJar", text(sj));
                beanList.add(o);
            }
        }

        ArrayNode extCalls = mapper.createArrayNode();
        JsonNode extSrc = ep.get("externalCalls");
        if (extSrc != null && extSrc.isArray()) {
            for (JsonNode e : extSrc) {
                ObjectNode o = mapper.createObjectNode();
                o.put("className", text(e.get("className")));
                o.put("simpleClassName", text(e.get("simpleClassName")));
                o.put("methodName", text(e.get("methodName")));
                String stereotype = text(e.get("stereotype"));
                o.put("stereotype", stereotype.isEmpty() ? "EXTERNAL" : stereotype);
                String sourceJar = text(e.get("sourceJar"));
                o.put("sourceJar", sourceJar.isEmpty() ? "main" : sourceJar);
                String module = text(e.get("module"));
                o.put("module", module.isEmpty() ? "External" : module);
                String domain = text(e.get("domain"));
                o.put("domain", domain.isEmpty() ? "External" : domain);
                ArrayNode bc = mapper.createArrayNode();
                JsonNode bcSrc = e.get("breadcrumb");
                if (bcSrc != null && bcSrc.isArray()) for (JsonNode b : bcSrc) bc.add(b);
                o.set("breadcrumb", bc);
                extCalls.add(o);
            }
        }

        ArrayNode hCalls = mapper.createArrayNode();
        JsonNode httpSrc = ep.get("httpCalls");
        if (httpSrc != null && httpSrc.isArray()) {
            for (JsonNode h : httpSrc) {
                ObjectNode o = mapper.createObjectNode();
                o.put("className", text(h.get("className")));
                o.put("simpleClassName", text(h.get("simpleClassName")));
                o.put("methodName", text(h.get("methodName")));
                String opType = text(h.get("operationType"));
                o.put("operationType", opType.isEmpty() ? "READ" : opType);
                String url = text(h.get("url"));
                o.put("url", url);
                ArrayNode allUrls = mapper.createArrayNode();
                if (!url.isEmpty()) allUrls.add(url);
                o.set("allUrls", allUrls);
                ArrayNode bc = mapper.createArrayNode();
                JsonNode bcSrc = h.get("breadcrumb");
                if (bcSrc != null && bcSrc.isArray()) for (JsonNode b : bcSrc) bc.add(b);
                o.set("breadcrumb", bc);
                hCalls.add(o);
            }
        }

        List<String> allPN = new ArrayList<>();
        Map<String, ObjectNode> procNameSources = new LinkedHashMap<>();
        JsonNode procNamesSrc = ep.get("procNames");
        if (procNamesSrc != null && procNamesSrc.isArray()) {
            for (JsonNode p : procNamesSrc) {
                String pn = text(p.get("procName"));
                if (pn.isEmpty()) continue;
                allPN.add(pn);
                if (!procNameSources.containsKey(pn)) {
                    ObjectNode src = mapper.createObjectNode();
                    src.put("simpleClassName", text(p.get("simpleClassName")));
                    src.put("methodName", text(p.get("methodName")));
                    src.put("className", text(p.get("className")));
                    procNameSources.put(pn, src);
                }
            }
        }

        String controllerSimpleName = text(ep.get("controllerSimpleName"));
        String methodName = text(ep.get("methodName"));
        String fullPath = text(ep.get("fullPath"));

        ObjectNode collectionsOut = mapper.createObjectNode();
        for (var e : collections.entrySet()) {
            Coll c = e.getValue();
            ObjectNode co = mapper.createObjectNode();
            co.put("name", e.getKey());
            co.put("type", c.type);
            co.put("domain", c.domain);
            ArrayNode ops = co.putArray("operations");
            for (String o : c.operations) ops.add(o);
            ArrayNode srcs = co.putArray("sources");
            for (String s : c.sources) srcs.add(s);
            ObjectNode smm = co.putObject("sourceMethodMap");
            c.sourceMethodMap.forEach(smm::put);
            ArrayNode dv = co.putArray("detectedVia");
            for (String d : c.detectedVia) dv.add(d);
            if (c.verification != null) co.put("verification", c.verification); else co.putNull("verification");
            collectionsOut.set(e.getKey(), co);
        }

        ObjectNode report = mapper.createObjectNode();
        report.put("endpointName", controllerSimpleName + "." + methodName);
        // TASK-REGISTRY §196 — `jar/js/summary-tables.js`'s `_renderEpCard` (the Summary tab's own
        // endpoint report cards) unconditionally reads `r.endpoint.controllerClass`/`.methodName`
        // for its "View decompiled code" link and nav index — a field this builder never wrote at
        // all, on any row, ever. Confirmed on the real `unified-web.jar` corpus: all 286 rows
        // missing it, not an edge case. `controllerClass` (fully qualified, unlike
        // `controllerSimpleName` above) is already read from the same raw `ep` node further down
        // in this method for the call-tree segment labels — reusing that exact field here instead
        // of introducing a second, possibly-inconsistent source for it.
        ObjectNode endpointOut = report.putObject("endpoint");
        endpointOut.put("controllerClass", text(ep.get("controllerClass")));
        endpointOut.put("methodName", methodName);
        String httpMethod = text(ep.get("httpMethod"));
        report.put("httpMethod", httpMethod.isEmpty() ? "ALL" : httpMethod);
        report.put("fullPath", fullPath.isEmpty() ? "/" : fullPath);
        report.put("procName", allPN.isEmpty() ? "" : allPN.get(0));
        ArrayNode allProcNamesOut = report.putArray("allProcNames");
        for (String pn : allPN) allProcNamesOut.add(pn);
        ObjectNode pns = report.putObject("procNameSources");
        procNameSources.forEach(pns::set);
        report.put("endpointUrl", fullPath.isEmpty() ? "/" : fullPath);
        report.put("typeOfEndpoint", classifyEndpointType(ep, cfg));

        // `(ep.modules && ep.modules.length) ? ep.modules : ['main']`
        List<String> moduleNames = new ArrayList<>();
        JsonNode modulesSrc = ep.get("modules");
        if (modulesSrc != null && modulesSrc.isArray() && !modulesSrc.isEmpty()) {
            for (JsonNode m : modulesSrc) moduleNames.add(text(m));
        } else {
            moduleNames.add("main");
        }
        // First-write-wins map update from THIS endpoint's own external calls, before that same
        // map is consulted below for THIS endpoint's own domain list — the JS does the update and
        // the read in that order, within one `_analyzeEndpoint` call, not just across endpoints.
        for (JsonNode e : extCalls) {
            String sourceJar = text(e.get("sourceJar"));
            if (!sourceJar.isEmpty() && !jarModuleMap.containsKey(sourceJar)) {
                jarModuleMap.put(sourceJar, text(e.get("module")));
            }
        }
        Set<String> jarDomains = new LinkedHashSet<>();
        for (String m : moduleNames) jarDomains.add(SummaryStats.jarToDomain(m, jarModuleMap, cfg));
        List<String> domainList = new ArrayList<>(jarDomains);
        domainList.sort(null);
        String primaryDomain = !domainList.isEmpty() ? domainList.get(0)
                : (!firstColl.isEmpty() ? SummaryStats.detectDomain(firstColl, cfg) : null);
        if (primaryDomain == null || primaryDomain.isEmpty()) primaryDomain = "Core";
        report.put("primaryDomain", primaryDomain);
        ArrayNode domainsOut = report.putArray("domains");
        for (String d : domainList) domainsOut.add(d);

        report.set("beans", beanList);
        report.set("externalCalls", extCalls);
        report.set("httpCalls", hCalls);
        report.set("collections", collectionsOut);
        report.put("totalMethods", ep.path("totalMethods").asLong(0));
        report.put("totalLoc", ep.path("totalLoc").asLong(0));
        report.put("totalCollections", collKeys.size());
        report.put("totalViews", views.size());
        report.put("totalDbOperations", ep.path("totalDbOperations").asLong(0));
        ArrayNode operationTypesOut = report.putArray("operationTypes");
        JsonNode opTypesList = ep.get("operationTypesList");
        if (opTypesList != null && opTypesList.isArray() && !opTypesList.isEmpty()) {
            for (JsonNode o : opTypesList) operationTypesOut.add(o);
        } else {
            String derived = text(ep.get("derivedOperationType"));
            if (!derived.isEmpty()) operationTypesOut.add(derived);
        }
        ArrayNode writeOut = report.putArray("writeCollections");
        for (String c : writeColls) writeOut.add(c);
        ArrayNode readOut = report.putArray("readCollections");
        for (String c : readColls) readOut.add(c);
        ArrayNode aggOut = report.putArray("aggregateCollections");
        for (String c : aggColls2) aggOut.add(c);
        ArrayNode viewsOut = report.putArray("viewsUsed");
        for (String c : views) viewsOut.add(c);
        ArrayNode modulesOut = report.putArray("modules");
        for (String m : moduleNames) modulesOut.add(SummaryStats.jarToProject(m, uploadedJarName, jarModuleMap));
        report.put("inScopeCalls", ep.path("inScopeCalls").asLong(0));
        report.put("externalScopeCalls", ep.path("externalScopeCalls").asLong(0));
        report.put("sizeCategory", sizeCategory(ep.path("totalMethods").asLong(0), sizeThresholds));
        report.put("performanceImplication",
                perfImpl(ep.path("totalDbOperations").asLong(0), perfThresholds));

        // `collDomainGroups`, transcribed: groups collections by their own resolved `.domain`,
        // not by anything jar/module related — no _jarModuleMap dependency despite living next to
        // `crossDomainCount` (which does need it, via `primaryDomain`) in the JS source.
        ObjectNode collDomainGroups = mapper.createObjectNode();
        for (var e : collections.entrySet()) {
            String domain = e.getValue().domain;
            ArrayNode group = (ArrayNode) collDomainGroups.get(domain);
            if (group == null) { group = mapper.createArrayNode(); collDomainGroups.set(domain, group); }
            group.add(e.getKey());
        }
        report.set("collDomainGroups", collDomainGroups);

        // `crossDomainCount`: how many of collDomainGroups' own keys are neither `primaryDomain`
        // nor `"Other"` — a count over the per-collection domains just grouped above, compared
        // against the per-module `primaryDomain` computed earlier.
        String primaryDomainFinal = primaryDomain;
        long crossDomainCount = 0;
        var groupNames = collDomainGroups.fieldNames();
        while (groupNames.hasNext()) {
            String d = groupNames.next();
            if (!d.equals(primaryDomainFinal) && !"Other".equals(d)) crossDomainCount++;
        }
        report.put("crossDomainCount", crossDomainCount);

        // `serviceClasses`, transcribed: a pure filter over the bean list already built above.
        ArrayNode serviceClasses = report.putArray("serviceClasses");
        for (JsonNode b : beanList) {
            if ("SERVICE".equals(text(b.get("stereotype")))) serviceClasses.add(text(b.get("simpleClassName")));
        }

        // `_buildSyntheticBreadcrumbs`, transcribed: one synthetic path per collection, built from
        // the endpoint's own controller identity and each collection's recorded source methods —
        // no _jarModuleMap involvement either.
        ObjectNode collBreadcrumbs = report.putObject("collBreadcrumbs");
        String controllerClass = text(ep.get("controllerClass"));
        for (var e : collections.entrySet()) {
            Coll c = e.getValue();
            ArrayNode segs = mapper.createArrayNode();
            if (!controllerSimpleName.isEmpty()) {
                ObjectNode seg = mapper.createObjectNode();
                seg.put("label", controllerSimpleName + "." + methodName + "()");
                seg.put("full", controllerClass.isEmpty() ? controllerSimpleName : controllerClass);
                seg.put("className", controllerSimpleName);
                seg.put("methodName", methodName);
                seg.put("level", 0);
                segs.add(seg);
            }
            if (!c.sourceMethodMap.isEmpty()) {
                for (var sm : c.sourceMethodMap.entrySet()) {
                    ObjectNode seg = mapper.createObjectNode();
                    String srcMeth = sm.getValue() == null || sm.getValue().isEmpty() ? "?" : sm.getValue();
                    seg.put("label", sm.getKey() + "." + srcMeth + "()");
                    seg.put("full", sm.getKey());
                    seg.put("className", sm.getKey());
                    seg.put("methodName", sm.getValue() == null ? "" : sm.getValue());
                    seg.put("level", 1);
                    segs.add(seg);
                }
            } else if (!c.sources.isEmpty()) {
                for (String src : c.sources) {
                    ObjectNode seg = mapper.createObjectNode();
                    seg.put("label", src + "()");
                    seg.put("full", src);
                    seg.put("className", src);
                    seg.put("methodName", "");
                    seg.put("level", 1);
                    segs.add(seg);
                }
            }
            if (!segs.isEmpty()) {
                ArrayNode outer = mapper.createArrayNode();
                outer.add(segs);
                collBreadcrumbs.set(e.getKey(), outer);
            }
        }

        return report;
    }
}
