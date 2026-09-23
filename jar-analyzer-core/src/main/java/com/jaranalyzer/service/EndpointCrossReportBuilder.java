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
 * UI-026e — server-side port of {@code jar/js/summary-analyzer.js}'s five whole-{@code epReports}
 * aggregation builders: {@code _buildDistributedTransactions}, {@code _buildBatchAnalysis},
 * {@code _buildViewsAnalysis}, {@code _buildVerticalisation}, {@code _buildExternalDeps}. Each runs
 * once, after every endpoint has its own {@link EndpointReportBuilder} report, over the whole
 * array — branch-agnostic (they read the assembled report shape, not {@code ep.callTree}), and
 * confirmed live in §67/§68: all four call sites (`summary.js`, `summary-export.js`,
 * `summary-export-modal.js`, `summary-popups.js`) feed them {@code this._epReports}.
 *
 * <p>{@code _calcComplexity} was first misclassified as dead code in §67 (it sits in the same file,
 * shortly after the dead call-tree-walking branch, and was grouped with that branch's line numbers
 * without checking it individually) — corrected in §68. It is a real, separate, config-driven
 * scorer ({@code summary-helpers.js:169}, over {@code domain-config.json}'s {@code
 * complexity_rules}) needed by {@link #buildViewsAnalysis}.
 */
public final class EndpointCrossReportBuilder {

    private EndpointCrossReportBuilder() {
    }

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? "" : n.asText("");
    }

    private static Set<String> asStringSet(JsonNode arr) {
        Set<String> out = new LinkedHashSet<>();
        if (arr != null && arr.isArray()) for (JsonNode n : arr) out.add(text(n));
        return out;
    }

    /**
     * {@code complexity_rules} from {@code domain-config.json}, transcribed as a value object —
     * same rationale as {@link SummaryStats.Config}: reproducible from a config file alone.
     */
    public record ComplexityRules(List<Factor> factors, double lowMax, double mediumMax) {

        public record Factor(String id, double weight) {
        }

        @SuppressWarnings("unchecked")
        public static ComplexityRules fromDomainConfig(Map<String, Object> config) {
            if (config == null || !(config.get("complexity_rules") instanceof Map<?, ?> cr)) {
                return null; // `!rules || !rules.factors` — no config means the JS's own fallback applies
            }
            List<Factor> factors = new ArrayList<>();
            if (cr.get("factors") instanceof List<?> list) {
                for (Object o : list) {
                    if (o instanceof Map<?, ?> f) {
                        Object id = f.get("id");
                        Object w = f.get("weight");
                        if (id != null) {
                            factors.add(new Factor(String.valueOf(id),
                                    w instanceof Number n ? n.doubleValue() : 0));
                        }
                    }
                }
            }
            double lowMax = 4, mediumMax = 10;
            if (cr.get("thresholds") instanceof Map<?, ?> th) {
                if (th.get("low_max") instanceof Number n) lowMax = n.doubleValue();
                if (th.get("medium_max") instanceof Number n) mediumMax = n.doubleValue();
            }
            return new ComplexityRules(factors, lowMax, mediumMax);
        }
    }

    /** {@code _isBatchEndpoint}, transcribed for the already-assembled report shape (`fullPath` +
     *  `endpointName`, not the raw endpoint's `controllerSimpleName`/`methodName` pair). */
    private static boolean isBatchEndpoint(JsonNode report, List<String> batchKeywords) {
        String haystack = (text(report.get("fullPath")) + ' ' + text(report.get("endpointName")))
                .toLowerCase(Locale.ROOT);
        for (String kw : batchKeywords) {
            if (!kw.isEmpty() && haystack.contains(kw)) return true;
        }
        return false;
    }

    /** {@code _buildDistributedTransactions}, transcribed. */
    public static ArrayNode buildDistributedTransactions(ObjectMapper mapper, List<JsonNode> epReports) {
        ArrayNode results = mapper.createArrayNode();
        for (JsonNode r : epReports) {
            Map<String, List<String>> writes = new LinkedHashMap<>();
            Map<String, List<String>> reads = new LinkedHashMap<>();
            Map<String, List<String>> aggs = new LinkedHashMap<>();
            JsonNode collections = r.path("collections");
            var fields = collections.fields();
            while (fields.hasNext()) {
                var e = fields.next();
                String name = e.getKey();
                JsonNode c = e.getValue();
                String domain = text(c.get("domain"));
                Set<String> ops = asStringSet(c.get("operations"));
                if (ops.contains("WRITE") || ops.contains("UPDATE") || ops.contains("DELETE")) {
                    writes.computeIfAbsent(domain, d -> new ArrayList<>()).add(name);
                }
                if (ops.contains("READ") || ops.contains("COUNT")) {
                    reads.computeIfAbsent(domain, d -> new ArrayList<>()).add(name);
                }
                if (ops.contains("AGGREGATE")) {
                    aggs.computeIfAbsent(domain, d -> new ArrayList<>()).add(name);
                }
            }
            List<String> writeDomains = new ArrayList<>(writes.keySet());
            String txnReq;
            if (writeDomains.size() > 1) {
                txnReq = "REQUIRED - cross-domain writes detected (" + String.join(", ", writeDomains) + ")";
            } else if (writeDomains.size() == 1) {
                txnReq = "SINGLE-DOMAIN";
            } else {
                txnReq = "NOT REQUIRED";
            }

            Set<String> allDomains = new LinkedHashSet<>();
            allDomains.addAll(writes.keySet());
            allDomains.addAll(reads.keySet());
            allDomains.addAll(aggs.keySet());

            ObjectNode crossDeps = mapper.createObjectNode();
            for (String d : allDomains) {
                ObjectNode dep = mapper.createObjectNode();
                ArrayNode readArr = dep.putArray("read");
                for (String c : reads.getOrDefault(d, List.of())) readArr.add(c);
                ArrayNode writeArr = dep.putArray("write");
                for (String c : writes.getOrDefault(d, List.of())) writeArr.add(c);
                ArrayNode aggArr = dep.putArray("aggregate");
                for (String c : aggs.getOrDefault(d, List.of())) aggArr.add(c);
                crossDeps.set(d, dep);
            }

            ObjectNode out = mapper.createObjectNode();
            out.put("endpointName", text(r.get("endpointName")));
            out.put("procName", text(r.get("procName")));
            out.set("allProcNames", r.path("allProcNames"));
            out.put("httpMethod", text(r.get("httpMethod")));
            out.put("fullPath", text(r.get("fullPath")));
            out.put("typeOfEndpoint", text(r.get("typeOfEndpoint")));
            out.put("primaryDomain", text(r.get("primaryDomain")));
            out.put("transactionRequirement", txnReq);
            out.set("crossDomainDependencies", crossDeps);
            out.put("totalCollections", r.path("totalCollections").asLong(0));
            out.put("totalLoc", r.path("totalLoc").asLong(0));
            out.put("performanceImplication", text(r.get("performanceImplication")));
            results.add(out);
        }
        return results;
    }

    /** {@code _buildBatchAnalysis}, transcribed. */
    public static ArrayNode buildBatchAnalysis(ObjectMapper mapper, List<JsonNode> epReports,
                                               List<String> batchKeywords) {
        ArrayNode results = mapper.createArrayNode();
        for (JsonNode r : epReports) {
            if (!isBatchEndpoint(r, batchKeywords)) continue;
            ObjectNode extDeps = mapper.createObjectNode();
            JsonNode collections = r.path("collections");
            var fields = collections.fields();
            while (fields.hasNext()) {
                var e = fields.next();
                String name = e.getKey();
                JsonNode c = e.getValue();
                String domain = text(c.get("domain"));
                ArrayNode list = (ArrayNode) extDeps.get(domain);
                if (list == null) { list = mapper.createArrayNode(); extDeps.set(domain, list); }
                ObjectNode dep = mapper.createObjectNode();
                dep.put("collection", name);
                dep.put("objectType", text(c.get("type")));
                ArrayNode usageTypes = dep.putArray("usageTypes");
                for (String op : asStringSet(c.get("operations"))) usageTypes.add(op);
                list.add(dep);
            }
            ObjectNode out = mapper.createObjectNode();
            out.put("batchName", text(r.get("endpointName")));
            out.put("procName", text(r.get("procName")));
            out.set("allProcNames", r.path("allProcNames"));
            out.put("endpointUrl", text(r.get("fullPath")));
            out.put("primaryDomain", text(r.get("primaryDomain")));
            out.set("domains", r.path("domains"));
            out.put("totalCollections", r.path("totalCollections").asLong(0));
            out.put("totalMethods", r.path("totalMethods").asLong(0));
            out.put("totalLoc", r.path("totalLoc").asLong(0));
            out.set("externalDependencies", extDeps);
            out.put("sizeCategory", text(r.get("sizeCategory")));
            out.put("performanceImplication", text(r.get("performanceImplication")));
            results.add(out);
        }
        return results;
    }

    /** {@code _calcComplexity}, transcribed — the JS's own no-config fallback (usage-count buckets)
     *  when {@code rules} is null, matching {@code !rules || !rules.factors}. */
    static String calcComplexity(ObjectNode collEntry, ComplexityRules rules) {
        long usageCount = collEntry.path("usageCount").asLong(0);
        if (rules == null || rules.factors().isEmpty()) {
            if (usageCount <= 2) return "Low";
            if (usageCount <= 5) return "Medium";
            return "High";
        }
        boolean hasPipeline = false;
        for (JsonNode d : collEntry.path("detectedVia")) {
            String v = d.asText("");
            if ("PIPELINE_RUNTIME".equals(v) || "AGGREGATION_API".equals(v) || "TEMPLATE_AGGREGATE".equals(v)) {
                hasPipeline = true;
                break;
            }
        }
        Map<String, Double> vals = new LinkedHashMap<>();
        vals.put("endpoints", (double) collEntry.path("endpoints").size());
        vals.put("read_ops", (double) collEntry.path("readOps").size());
        vals.put("write_ops", (double) collEntry.path("writeOps").size());
        boolean hasAggregate = false;
        for (JsonNode o : collEntry.path("operations")) if ("AGGREGATE".equals(o.asText(""))) hasAggregate = true;
        vals.put("aggregate_ops", hasAggregate ? 1.0 : 0.0);
        int domainSetSize = collEntry.path("domainSet").size();
        vals.put("cross_domain", domainSetSize > 1 ? (double) (domainSetSize - 1) : 0.0);
        vals.put("pipeline_usage", hasPipeline ? 1.0 : 0.0);
        vals.put("usage_count", (double) usageCount);

        double score = 0;
        for (ComplexityRules.Factor f : rules.factors()) {
            score += vals.getOrDefault(f.id(), 0.0) * f.weight();
        }
        collEntry.put("_complexityScore", Math.round(score * 10) / 10.0);
        if (score <= rules.lowMax()) return "Low";
        if (score <= rules.mediumMax()) return "Medium";
        return "High";
    }

    /**
     * {@code _buildVerticalisation}, transcribed: one row per collection across the whole analysis,
     * folding every endpoint that touches it.
     */
    public static ArrayNode buildVerticalisation(ObjectMapper mapper, List<JsonNode> epReports) {
        Set<String> readOps = Set.of("READ", "COUNT", "EXISTS");
        Set<String> writeOps = Set.of("WRITE", "UPDATE", "DELETE");
        Set<String> aggOps = Set.of("AGGREGATE");

        Map<String, ObjectNode> collMap = new LinkedHashMap<>();
        Map<String, Set<String>> operations = new LinkedHashMap<>();
        Map<String, Set<String>> readOpsSeen = new LinkedHashMap<>();
        Map<String, Set<String>> writeOpsSeen = new LinkedHashMap<>();
        Map<String, Set<String>> aggOpsSeen = new LinkedHashMap<>();
        Map<String, Set<String>> endpointsSeen = new LinkedHashMap<>();
        Map<String, Set<String>> sourcesSeen = new LinkedHashMap<>();
        Map<String, Set<String>> detectedViaSeen = new LinkedHashMap<>();
        Map<String, Set<String>> procNamesSeen = new LinkedHashMap<>();
        Map<String, Set<String>> domainSetSeen = new LinkedHashMap<>();

        for (JsonNode r : epReports) {
            String endpointName = text(r.get("endpointName"));
            String procName = text(r.get("procName"));
            String primaryDomain = text(r.get("primaryDomain"));
            JsonNode collections = r.path("collections");
            JsonNode collBreadcrumbs = r.path("collBreadcrumbs");
            var fields = collections.fields();
            while (fields.hasNext()) {
                var e = fields.next();
                String name = e.getKey();
                JsonNode c = e.getValue();

                ObjectNode entry = collMap.get(name);
                if (entry == null) {
                    entry = mapper.createObjectNode();
                    entry.put("name", name);
                    entry.put("type", text(c.get("type")));
                    entry.put("domain", text(c.get("domain")));
                    entry.putArray("endpoints");
                    entry.putArray("operations");
                    entry.putArray("sources");
                    entry.putArray("detectedVia");
                    entry.put("usageCount", 0L);
                    entry.putArray("breadcrumbs");
                    entry.putArray("procNames");
                    entry.putNull("verification");
                    entry.putArray("readOps");
                    entry.putArray("writeOps");
                    entry.putArray("aggOps");
                    entry.putArray("domainSet");
                    collMap.put(name, entry);
                    operations.put(name, new LinkedHashSet<>());
                    readOpsSeen.put(name, new LinkedHashSet<>());
                    writeOpsSeen.put(name, new LinkedHashSet<>());
                    aggOpsSeen.put(name, new LinkedHashSet<>());
                    endpointsSeen.put(name, new LinkedHashSet<>());
                    sourcesSeen.put(name, new LinkedHashSet<>());
                    detectedViaSeen.put(name, new LinkedHashSet<>());
                    procNamesSeen.put(name, new LinkedHashSet<>());
                    domainSetSeen.put(name, new LinkedHashSet<>());
                }

                endpointsSeen.get(name).add(endpointName);
                for (String op : asStringSet(c.get("operations"))) {
                    operations.get(name).add(op);
                    if (readOps.contains(op)) readOpsSeen.get(name).add(op);
                    if (writeOps.contains(op)) writeOpsSeen.get(name).add(op);
                    if (aggOps.contains(op)) aggOpsSeen.get(name).add(op);
                }
                sourcesSeen.get(name).addAll(asStringSet(c.get("sources")));
                detectedViaSeen.get(name).addAll(asStringSet(c.get("detectedVia")));

                String verification = c.hasNonNull("verification") ? text(c.get("verification")) : "";
                if (!verification.isEmpty()) {
                    String cur = entry.path("verification").isNull() ? null : entry.path("verification").asText();
                    boolean isUpgrade = cur == null || "NO_CATALOG".equals(cur)
                            || ("NOT_IN_DB".equals(cur) && ("VERIFIED".equals(verification) || "CLAUDE_VERIFIED".equals(verification)));
                    if (isUpgrade) entry.put("verification", verification);
                }

                entry.put("usageCount", entry.path("usageCount").asLong(0) + 1);
                if (!procName.isEmpty()) procNamesSeen.get(name).add(procName);
                if (!primaryDomain.isEmpty()) domainSetSeen.get(name).add(primaryDomain);

                JsonNode bcList = collBreadcrumbs.get(name);
                if (bcList != null && bcList.isArray()) {
                    ArrayNode breadcrumbs = (ArrayNode) entry.get("breadcrumbs");
                    for (JsonNode bc : bcList) {
                        if (breadcrumbs.size() < 5) breadcrumbs.add(bc);
                    }
                }
            }
        }

        for (var e : collMap.entrySet()) {
            String name = e.getKey();
            ObjectNode entry = e.getValue();
            ArrayNode endpointsArr = entry.putArray("endpoints");
            for (String s : endpointsSeen.get(name)) endpointsArr.add(s);
            ArrayNode opsArr = entry.putArray("operations");
            for (String s : operations.get(name)) opsArr.add(s);
            ArrayNode srcArr = entry.putArray("sources");
            for (String s : sourcesSeen.get(name)) srcArr.add(s);
            ArrayNode dvArr = entry.putArray("detectedVia");
            for (String s : detectedViaSeen.get(name)) dvArr.add(s);
            ArrayNode pnArr = entry.putArray("procNames");
            for (String s : procNamesSeen.get(name)) pnArr.add(s);
            ArrayNode roArr = entry.putArray("readOps");
            for (String s : readOpsSeen.get(name)) roArr.add(s);
            ArrayNode woArr = entry.putArray("writeOps");
            for (String s : writeOpsSeen.get(name)) woArr.add(s);
            ArrayNode aoArr = entry.putArray("aggOps");
            for (String s : aggOpsSeen.get(name)) aoArr.add(s);
            ArrayNode dsArr = entry.putArray("domainSet");
            for (String s : domainSetSeen.get(name)) dsArr.add(s);
        }

        List<ObjectNode> sorted = new ArrayList<>(collMap.values());
        sorted.sort((a, b) -> {
            String da = text(a.get("domain")), db = text(b.get("domain"));
            if (!da.equals(db)) return da.compareTo(db);
            String ta = text(a.get("type")), tb = text(b.get("type"));
            if (!ta.equals(tb)) return "VIEW".equals(ta) ? 1 : -1;
            return text(a.get("name")).compareTo(text(b.get("name")));
        });

        ArrayNode out = mapper.createArrayNode();
        for (ObjectNode o : sorted) out.add(o);
        return out;
    }

    /** {@code _buildViewsAnalysis}, transcribed. Mutates each view entry with {@code _complexityScore}
     *  as a side effect, matching {@code _calcComplexity}'s own behavior. */
    public static ArrayNode buildViewsAnalysis(ObjectMapper mapper, ArrayNode vertReport, ComplexityRules rules) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode node : vertReport) {
            if (!"VIEW".equals(text(node.get("type")))) continue;
            ObjectNode c = (ObjectNode) node;
            ObjectNode view = mapper.createObjectNode();
            view.put("viewName", text(c.get("name")));
            view.put("domain", text(c.get("domain")));
            view.putArray("dependencyOfOtherViews");
            ArrayNode viewDef = view.putArray("viewDefinition");
            for (JsonNode op : c.path("operations")) viewDef.add(op.asText("") + " via repository");
            view.putArray("fields");
            view.put("recursive", false);
            long usageCount = c.path("usageCount").asLong(0);
            view.put("possibleAlternative",
                    usageCount <= 1 ? "Evaluate: direct query on base collection may suffice" : "");
            ArrayNode usedBy = view.putArray("usedByEndpoints");
            for (JsonNode ep : c.path("endpoints")) usedBy.add(ep);
            view.put("usageCount", usageCount);
            view.put("complexity", calcComplexity(c, rules));
            out.add(view);
        }
        return out;
    }

    /** {@code _buildExternalDeps}, transcribed, reusing the same {@code jarModuleMap}/{@code
     *  SummaryStats.jarToDomain}/{@code jarToProject} machinery UI-026a's closing increment (§59)
     *  already built. */
    public static ObjectNode buildExternalDeps(ObjectMapper mapper, List<JsonNode> epReports,
                                               SummaryStats.Config cfg, Map<String, String> jarModuleMap,
                                               String uploadedJarName) {
        Map<String, ObjectNode> moduleMap = new LinkedHashMap<>();
        Map<String, Set<String>> classesSeen = new LinkedHashMap<>();
        Map<String, Set<String>> endpointsSeen = new LinkedHashMap<>();
        Map<String, Set<String>> methodsSeen = new LinkedHashMap<>();

        for (JsonNode r : epReports) {
            String endpointName = text(r.get("endpointName"));
            for (JsonNode ext : r.path("externalCalls")) {
                String sourceJar = text(ext.get("sourceJar"));
                String jar = sourceJar.isEmpty() ? "main" : sourceJar;

                ObjectNode entry = moduleMap.get(jar);
                if (entry == null) {
                    entry = mapper.createObjectNode();
                    entry.put("jar", jar);
                    entry.put("domain", SummaryStats.jarToDomain(jar, jarModuleMap, cfg));
                    entry.put("project", SummaryStats.jarToProject(jar, uploadedJarName, jarModuleMap));
                    entry.put("count", 0L);
                    entry.putArray("breadcrumbs");
                    moduleMap.put(jar, entry);
                    classesSeen.put(jar, new LinkedHashSet<>());
                    endpointsSeen.put(jar, new LinkedHashSet<>());
                    methodsSeen.put(jar, new LinkedHashSet<>());
                }

                String simpleClassName = text(ext.get("simpleClassName"));
                classesSeen.get(jar).add(simpleClassName);
                endpointsSeen.get(jar).add(endpointName);
                methodsSeen.get(jar).add(simpleClassName + "." + text(ext.get("methodName")));
                entry.put("count", entry.path("count").asLong(0) + 1);

                JsonNode bc = ext.get("breadcrumb");
                if (bc != null && bc.isArray() && !bc.isEmpty()) {
                    ArrayNode breadcrumbs = (ArrayNode) entry.get("breadcrumbs");
                    if (breadcrumbs.size() < 5) breadcrumbs.add(bc);
                }
            }
        }

        for (var e : moduleMap.entrySet()) {
            String jar = e.getKey();
            ObjectNode entry = e.getValue();
            ArrayNode classesArr = entry.putArray("classes");
            for (String s : classesSeen.get(jar)) classesArr.add(s);
            ArrayNode endpointsArr = entry.putArray("endpoints");
            for (String s : endpointsSeen.get(jar)) endpointsArr.add(s);
            ArrayNode methodsArr = entry.putArray("methods");
            for (String s : methodsSeen.get(jar)) methodsArr.add(s);
        }

        List<ObjectNode> sorted = new ArrayList<>(moduleMap.values());
        sorted.sort((a, b) -> Long.compare(b.path("count").asLong(0), a.path("count").asLong(0)));

        ObjectNode out = mapper.createObjectNode();
        ArrayNode crossModule = out.putArray("crossModule");
        for (ObjectNode o : sorted) crossModule.add(o);
        return out;
    }
}
