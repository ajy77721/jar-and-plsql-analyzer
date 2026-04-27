package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.*;

// Put callTree last so streamSummary sees pre-computed aggregates before callTree.
// This ordering is critical for the streaming OOM fix in AbstractAnalysisDataProvider.
@JsonPropertyOrder({"httpMethod","fullPath","controllerClass","controllerSimpleName",
    "methodName","returnType","parameters",
    "aggregatedCollections","aggregatedCollectionDomains","aggregatedOperations",
    "derivedOperationType","totalDbOperations",
    "totalMethods","totalLoc","inScopeCalls","externalScopeCalls",
    "modules","externalCalls","httpCalls","beans","operationTypesList","procNames",
    "dynamicFlows","aggregationFlows",
    "aggregatedCollectionVerification","aggregatedCollectionSources",
    "callTree"})
public class EndpointInfo {
    private String httpMethod;
    private String fullPath;
    private String controllerClass;
    private String controllerSimpleName;
    private String methodName;
    private String returnType;
    private List<ParameterInfo> parameters = new ArrayList<>();
    private CallNode callTree;

    // Aggregated from entire call tree (computed after build + after corrections)
    private List<String> aggregatedCollections = new ArrayList<>();
    private Map<String, String> aggregatedCollectionDomains = new LinkedHashMap<>();
    private Map<String, String> aggregatedOperations = new LinkedHashMap<>();  // collection -> highest-priority op
    private String derivedOperationType;  // highest-priority operation across entire tree
    private int totalDbOperations;        // count of nodes with collections

    // Precomputed aggregates so frontend doesn't need to walk callTree
    private int totalMethods;
    private int totalLoc;
    private int inScopeCalls;
    private int externalScopeCalls;
    private List<String> modules = new ArrayList<>();
    private List<Map<String, String>> externalCalls = new ArrayList<>();
    private List<Map<String, String>> httpCalls = new ArrayList<>();
    private List<Map<String, String>> beans = new ArrayList<>();
    private List<String> operationTypesList = new ArrayList<>();
    private List<Map<String, Object>> procNames = new ArrayList<>();
    private List<Map<String, Object>> dynamicFlows = new ArrayList<>();
    private List<Map<String, Object>> aggregationFlows = new ArrayList<>();

    public EndpointInfo() {}

    public String getHttpMethod() { return httpMethod; }
    public void setHttpMethod(String httpMethod) { this.httpMethod = httpMethod; }
    public String getFullPath() { return fullPath; }
    public void setFullPath(String fullPath) { this.fullPath = fullPath; }
    public String getControllerClass() { return controllerClass; }
    public void setControllerClass(String controllerClass) { this.controllerClass = controllerClass; }
    public String getControllerSimpleName() { return controllerSimpleName; }
    public void setControllerSimpleName(String controllerSimpleName) { this.controllerSimpleName = controllerSimpleName; }
    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }
    public String getReturnType() { return returnType; }
    public void setReturnType(String returnType) { this.returnType = returnType; }
    public List<ParameterInfo> getParameters() { return parameters; }
    public void setParameters(List<ParameterInfo> parameters) { this.parameters = parameters; }
    public CallNode getCallTree() { return callTree; }
    public void setCallTree(CallNode callTree) { this.callTree = callTree; }

    public List<String> getAggregatedCollections() { return aggregatedCollections; }
    public void setAggregatedCollections(List<String> aggregatedCollections) { this.aggregatedCollections = aggregatedCollections; }
    public Map<String, String> getAggregatedCollectionDomains() { return aggregatedCollectionDomains; }
    public void setAggregatedCollectionDomains(Map<String, String> aggregatedCollectionDomains) { this.aggregatedCollectionDomains = aggregatedCollectionDomains; }
    public Map<String, String> getAggregatedOperations() { return aggregatedOperations; }
    public void setAggregatedOperations(Map<String, String> aggregatedOperations) { this.aggregatedOperations = aggregatedOperations; }
    public String getDerivedOperationType() { return derivedOperationType; }
    public void setDerivedOperationType(String derivedOperationType) { this.derivedOperationType = derivedOperationType; }
    public int getTotalDbOperations() { return totalDbOperations; }
    public void setTotalDbOperations(int totalDbOperations) { this.totalDbOperations = totalDbOperations; }

    public int getTotalMethods() { return totalMethods; }
    public void setTotalMethods(int totalMethods) { this.totalMethods = totalMethods; }
    public int getTotalLoc() { return totalLoc; }
    public void setTotalLoc(int totalLoc) { this.totalLoc = totalLoc; }
    public int getInScopeCalls() { return inScopeCalls; }
    public void setInScopeCalls(int inScopeCalls) { this.inScopeCalls = inScopeCalls; }
    public int getExternalScopeCalls() { return externalScopeCalls; }
    public void setExternalScopeCalls(int externalScopeCalls) { this.externalScopeCalls = externalScopeCalls; }
    public List<String> getModules() { return modules; }
    public void setModules(List<String> modules) { this.modules = modules; }
    public List<Map<String, String>> getExternalCalls() { return externalCalls; }
    public void setExternalCalls(List<Map<String, String>> externalCalls) { this.externalCalls = externalCalls; }
    public List<Map<String, String>> getHttpCalls() { return httpCalls; }
    public void setHttpCalls(List<Map<String, String>> httpCalls) { this.httpCalls = httpCalls; }
    public List<Map<String, String>> getBeans() { return beans; }
    public void setBeans(List<Map<String, String>> beans) { this.beans = beans; }
    public List<String> getOperationTypesList() { return operationTypesList; }
    public void setOperationTypesList(List<String> operationTypesList) { this.operationTypesList = operationTypesList; }
    public List<Map<String, Object>> getProcNames() { return procNames; }
    public void setProcNames(List<Map<String, Object>> procNames) { this.procNames = procNames; }
    public List<Map<String, Object>> getDynamicFlows() { return dynamicFlows; }
    public void setDynamicFlows(List<Map<String, Object>> dynamicFlows) { this.dynamicFlows = dynamicFlows; }
    public List<Map<String, Object>> getAggregationFlows() { return aggregationFlows; }
    public void setAggregationFlows(List<Map<String, Object>> aggregationFlows) { this.aggregationFlows = aggregationFlows; }

    /**
     * Walk entire call tree and compute aggregated endpoint-level summaries.
     * Call after tree build AND after every correction/enrichment pass.
     */
    public void computeAggregates() {
        Set<String> colls = new LinkedHashSet<>();
        Map<String, String> collDomains = new LinkedHashMap<>();
        Map<String, String> collOps = new LinkedHashMap<>();
        int[] dbOps = {0};
        String[] bestOp = {null};

        int[] methodCount = {0};
        int[] locCount = {0};
        int[] inScope = {0};
        int[] extScope = {0};
        Set<String> moduleSet = new LinkedHashSet<>();
        Set<String> opSet = new LinkedHashSet<>();
        Set<String> seenBeans = new LinkedHashSet<>();
        Set<String> seenExt = new LinkedHashSet<>();
        Set<String> seenHttp = new LinkedHashSet<>();
        List<Map<String, String>> extCallList = new ArrayList<>();
        List<Map<String, String>> httpCallList = new ArrayList<>();
        List<Map<String, String>> beanList = new ArrayList<>();
        Set<String> allProcNames = new LinkedHashSet<>();
        List<Map<String, Object>> procList = new ArrayList<>();
        List<Map<String, Object>> dynFlows = new ArrayList<>();
        List<Map<String, Object>> aggFlows = new ArrayList<>();

        String controllerJar = callTree != null ? callTree.getSourceJar() : null;
        moduleSet.add("main");

        Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        walkForAggregation(callTree, colls, collDomains, collOps, dbOps, bestOp, visited,
                methodCount, locCount, inScope, extScope, moduleSet, opSet,
                seenBeans, seenExt, seenHttp, extCallList, httpCallList, beanList,
                allProcNames, procList, dynFlows, aggFlows, new ArrayList<>(), controllerJar);

        this.aggregatedCollections = new ArrayList<>(colls);
        this.aggregatedCollectionDomains = collDomains;
        this.aggregatedOperations = collOps;
        this.totalDbOperations = dbOps[0];
        this.derivedOperationType = bestOp[0];

        this.totalMethods = methodCount[0];
        this.totalLoc = locCount[0];
        this.inScopeCalls = inScope[0];
        this.externalScopeCalls = extScope[0];
        this.modules = new ArrayList<>(moduleSet);
        this.externalCalls = extCallList;
        this.httpCalls = httpCallList;
        this.beans = beanList;
        this.operationTypesList = new ArrayList<>(opSet);
        this.procNames = procList;
        this.dynamicFlows = dynFlows;
        this.aggregationFlows = aggFlows;
    }

    private void walkForAggregation(CallNode node, Set<String> colls,
            Map<String, String> domains, Map<String, String> ops,
            int[] dbOps, String[] bestOp, Set<CallNode> visited,
            int[] methodCount, int[] locCount, int[] inScope, int[] extScope,
            Set<String> moduleSet, Set<String> opSet,
            Set<String> seenBeans, Set<String> seenExt, Set<String> seenHttp,
            List<Map<String, String>> extCallList, List<Map<String, String>> httpCallList,
            List<Map<String, String>> beanList,
            Set<String> allProcNames, List<Map<String, Object>> procList,
            List<Map<String, Object>> dynFlows, List<Map<String, Object>> aggFlows,
            List<String> breadcrumbPath,
            String controllerJar) {
        if (node == null || !visited.add(node)) return;

        methodCount[0]++;
        if (node.getLineCount() > 0) locCount[0] += node.getLineCount();

        // Extract procName from annotationDetails
        if (node.getAnnotationDetails() != null) {
            for (Map<String, Object> ad : node.getAnnotationDetails()) {
                String annName = ad.get("name") != null ? ad.get("name").toString() : "";
                if (annName.contains("LogParameters")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> attrs = (Map<String, Object>) ad.get("attributes");
                    if (attrs != null) {
                        String pn = getStr(attrs, "procedureName");
                        if (pn == null) pn = getStr(attrs, "value");
                        if (pn == null) pn = getStr(attrs, "procName");
                        if (pn != null && allProcNames.add(pn)) {
                            Map<String, Object> entry = new LinkedHashMap<>();
                            entry.put("procName", pn);
                            entry.put("simpleClassName", node.getSimpleClassName());
                            entry.put("methodName", node.getMethodName());
                            entry.put("className", node.getClassName());
                            procList.add(entry);
                        }
                    }
                }
            }
        }

        // Dynamic flow detection (non-direct dispatch, reflection, dynamic queries)
        List<String> bcPath = new ArrayList<>(breadcrumbPath);
        bcPath.add(safe(node.getSimpleClassName()) + "." + safe(node.getMethodName()) + "()");
        if (bcPath.size() > 6) bcPath = bcPath.subList(bcPath.size() - 6, bcPath.size());

        String dt = node.getDispatchType();
        if (dt != null && !"DIRECT".equals(dt) && !"SPRING_DATA_DERIVED".equals(dt)) {
            Map<String, Object> df = new LinkedHashMap<>();
            df.put("category", "DISPATCH");
            df.put("dispatchType", dt);
            df.put("className", safe(node.getClassName()));
            df.put("simpleClassName", safe(node.getSimpleClassName()));
            df.put("methodName", safe(node.getMethodName()));
            df.put("resolvedFrom", node.getResolvedFrom());
            df.put("qualifierHint", node.getQualifierHint());
            df.put("breadcrumb", bcPath);
            dynFlows.add(df);
        }

        List<String> reflPatterns = List.of("Class.forName", "Method.invoke", "getMethod", "getDeclaredMethod", "newInstance");
        if (node.getStringLiterals() != null) {
            for (String lit : node.getStringLiterals()) {
                for (String pat : reflPatterns) {
                    if (lit.contains(pat)) {
                        Map<String, Object> df = new LinkedHashMap<>();
                        df.put("category", "REFLECTION");
                        df.put("dispatchType", "REFLECTION");
                        df.put("className", safe(node.getClassName()));
                        df.put("simpleClassName", safe(node.getSimpleClassName()));
                        df.put("methodName", safe(node.getMethodName()));
                        df.put("pattern", pat + ": " + lit);
                        df.put("breadcrumb", bcPath);
                        dynFlows.add(df);
                    }
                }
            }
        }

        if (node.getAnnotationDetails() != null) {
            for (Map<String, Object> ad : node.getAnnotationDetails()) {
                String annName = ad.get("name") != null ? ad.get("name").toString() : "";
                if (annName.contains("Query")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> attrs = (Map<String, Object>) ad.get("attributes");
                    if (attrs != null) {
                        String qv = getStr(attrs, "value");
                        if (qv != null && (qv.contains("?") || qv.contains("#{"))) {
                            Map<String, Object> df = new LinkedHashMap<>();
                            df.put("category", "DYNAMIC_QUERY");
                            df.put("dispatchType", "DYNAMIC_QUERY");
                            df.put("className", safe(node.getClassName()));
                            df.put("simpleClassName", safe(node.getSimpleClassName()));
                            df.put("methodName", safe(node.getMethodName()));
                            df.put("pattern", "Parameterized @Query");
                            df.put("breadcrumb", bcPath);
                            dynFlows.add(df);
                        }
                    }
                }
            }
        }

        // Aggregation flow detection: nodes with aggregate operations, lookups, pipelines
        String nodeOp = node.getOperationType();
        boolean isAggNode = "AGGREGATE".equals(nodeOp) || "COUNT".equals(nodeOp);
        boolean hasPipelineSource = false;
        if (node.getCollectionSources() != null) {
            for (String src : node.getCollectionSources().values()) {
                if (src != null && (src.contains("PIPELINE") || src.contains("AGGREGATION")
                        || src.contains("TEMPLATE_AGGREGATE"))) {
                    hasPipelineSource = true;
                    break;
                }
            }
        }
        boolean hasRichPipeline = node.getAggregationPipeline() != null && !node.getAggregationPipeline().isEmpty();
        List<String> lookupColls = new ArrayList<>();
        List<String> pipelineStages = new ArrayList<>();
        if (hasRichPipeline) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> stages = (List<Map<String, Object>>) node.getAggregationPipeline().get("stages");
            if (stages != null) {
                for (Map<String, Object> s : stages) {
                    Object stg = s.get("stage");
                    if (stg != null) pipelineStages.add(stg.toString());
                }
            }
            @SuppressWarnings("unchecked")
            List<Map<String, String>> lookups = (List<Map<String, String>>) node.getAggregationPipeline().get("lookupTargets");
            if (lookups != null) {
                for (Map<String, String> lk : lookups) {
                    String from = lk.get("from");
                    if (from != null) lookupColls.add(from);
                }
            }
        }
        if (pipelineStages.isEmpty() && node.getStringLiterals() != null) {
            for (String lit : node.getStringLiterals()) {
                if (lit.contains("$lookup")) pipelineStages.add("$lookup");
                if (lit.contains("$graphLookup")) pipelineStages.add("$graphLookup");
                if (lit.contains("$unwind")) pipelineStages.add("$unwind");
                if (lit.contains("$match")) pipelineStages.add("$match");
                if (lit.contains("$group")) pipelineStages.add("$group");
                if (lit.contains("$project")) pipelineStages.add("$project");
                if (lit.contains("$sort")) pipelineStages.add("$sort");
                if (lit.contains("$limit")) pipelineStages.add("$limit");
                if (lit.contains("$skip")) pipelineStages.add("$skip");
                if (lit.contains("$out")) pipelineStages.add("$out");
                if (lit.contains("$merge")) pipelineStages.add("$merge");
                if (lit.contains("$unionWith")) pipelineStages.add("$unionWith");
                if (lit.contains("$count")) pipelineStages.add("$count");
                if (lit.contains("$addFields")) pipelineStages.add("$addFields");
                if (lit.contains("$replaceRoot")) pipelineStages.add("$replaceRoot");
                if (lit.contains("$facet")) pipelineStages.add("$facet");
                java.util.regex.Matcher lm = java.util.regex.Pattern
                        .compile("\\$(lookup|graphLookup)[^}]*[\"']from[\"']\\s*:\\s*[\"']([A-Z][A-Z0-9_]+)[\"']")
                        .matcher(lit);
                while (lm.find()) lookupColls.add(lm.group(2));
            }
        }
        if (isAggNode || hasPipelineSource || !pipelineStages.isEmpty() || hasRichPipeline) {
            List<String> nodeCollections = node.getCollectionsAccessed() != null
                    ? node.getCollectionsAccessed() : List.of();
            if (!nodeCollections.isEmpty() || !pipelineStages.isEmpty() || isAggNode || hasRichPipeline) {
                Map<String, Object> af = new LinkedHashMap<>();
                af.put("className", safe(node.getClassName()));
                af.put("simpleClassName", safe(node.getSimpleClassName()));
                af.put("methodName", safe(node.getMethodName()));
                af.put("operationType", nodeOp);
                af.put("collections", nodeCollections);
                af.put("collectionDomains", node.getCollectionDomains() != null
                        ? new LinkedHashMap<>(node.getCollectionDomains()) : Map.of());
                af.put("collectionSources", node.getCollectionSources() != null
                        ? new LinkedHashMap<>(node.getCollectionSources()) : Map.of());
                af.put("pipelineStages", new ArrayList<>(new LinkedHashSet<>(pipelineStages)));
                af.put("lookupCollections", lookupColls);
                af.put("stereotype", safe(node.getStereotype()));
                af.put("sourceJar", safe(node.getSourceJar()));
                af.put("breadcrumb", bcPath);
                if (node.getAnnotationDetails() != null) {
                    List<Map<String, Object>> queryAnns = new ArrayList<>();
                    for (Map<String, Object> ad : node.getAnnotationDetails()) {
                        String ann = ad.get("name") != null ? ad.get("name").toString() : "";
                        if (ann.contains("Query") || ann.contains("Aggregation")) {
                            queryAnns.add(ad);
                        }
                    }
                    if (!queryAnns.isEmpty()) af.put("queryAnnotations", queryAnns);
                }
                if (hasRichPipeline) {
                    Map<String, Object> pipe = node.getAggregationPipeline();
                    if (pipe.get("lookupTargets") != null) af.put("lookupTargets", pipe.get("lookupTargets"));
                    if (pipe.get("matchFields") != null) af.put("matchFields", pipe.get("matchFields"));
                    if (pipe.get("groupFields") != null) af.put("groupFields", pipe.get("groupFields"));
                    if (pipe.get("projectionFields") != null) af.put("projectionFields", pipe.get("projectionFields"));
                    if (pipe.get("sortFields") != null) af.put("sortFields", pipe.get("sortFields"));
                    if (pipe.get("companionUsage") != null) af.put("companionUsage", pipe.get("companionUsage"));
                    if (pipe.get("detectionSources") != null) af.put("detectionSources", pipe.get("detectionSources"));
                    if (Boolean.TRUE.equals(pipe.get("isDynamic"))) af.put("isDynamic", true);
                    if (pipe.get("pipelineSourceMethods") != null) af.put("pipelineSourceMethods", pipe.get("pipelineSourceMethods"));
                }
                aggFlows.add(af);
            }
        }

        // Collection aggregation
        List<String> nodeColls = node.getCollectionsAccessed();
        if (nodeColls != null && !nodeColls.isEmpty()) {
            dbOps[0]++;
            for (String c : nodeColls) {
                colls.add(c);
                if (node.getCollectionDomains() != null && node.getCollectionDomains().containsKey(c)) {
                    domains.putIfAbsent(c, node.getCollectionDomains().get(c));
                }
                String existing = ops.get(c);
                if (nodeOp != null && opPri(nodeOp) > opPri(existing)) {
                    ops.put(c, nodeOp);
                }
            }
            if (nodeOp != null && opPri(nodeOp) > opPri(bestOp[0])) {
                bestOp[0] = nodeOp;
            }
        } else if (node.isHasDbInteraction() && node.getOperationType() != null) {
            dbOps[0]++;
            if (opPri(node.getOperationType()) > opPri(bestOp[0])) {
                bestOp[0] = node.getOperationType();
            }
        }

        String nodeId = node.getId();

        // HTTP calls
        if ("HTTP_CALL".equals(node.getCallType()) || (nodeId != null && nodeId.startsWith("http:"))) {
            extScope[0]++;
            String hk = safe(node.getClassName()) + "#" + safe(node.getMethodName());
            if (seenHttp.add(hk)) {
                Map<String, String> hc = new LinkedHashMap<>();
                hc.put("className", safe(node.getClassName()));
                hc.put("simpleClassName", safe(node.getSimpleClassName()));
                hc.put("methodName", safe(node.getMethodName()));
                hc.put("operationType", node.getOperationType() != null ? node.getOperationType() : "READ");
                if (node.getStringLiterals() != null) {
                    for (String s : node.getStringLiterals()) {
                        if (s.startsWith("http") || s.startsWith("/")) { hc.put("url", s); break; }
                    }
                }
                httpCallList.add(hc);
            }
        }
        // External (ext:) calls
        else if (nodeId != null && nodeId.startsWith("ext:")) {
            extScope[0]++;
            String ek = safe(node.getClassName()) + "#" + safe(node.getMethodName());
            if (seenExt.add(ek)) {
                Map<String, String> ec = new LinkedHashMap<>();
                ec.put("className", safe(node.getClassName()));
                ec.put("simpleClassName", safe(node.getSimpleClassName()));
                ec.put("methodName", safe(node.getMethodName()));
                ec.put("stereotype", node.getStereotype() != null ? node.getStereotype() : "EXTERNAL");
                ec.put("sourceJar", "unknown");
                ec.put("module", safe(node.getSimpleClassName()));
                ec.put("domain", "External");
                extCallList.add(ec);
            }
        }
        // Cross-module calls
        else {
            String nodeJar = node.getSourceJar();
            boolean isCross = node.isCrossModule() || (nodeJar != null && !Objects.equals(nodeJar, controllerJar));
            if (isCross) {
                extScope[0]++;
                if (nodeJar != null) moduleSet.add(nodeJar);
                String ek = safe(nodeJar) + ":" + safe(node.getClassName()) + "#" + safe(node.getMethodName());
                if (seenExt.add(ek)) {
                    Map<String, String> ec = new LinkedHashMap<>();
                    ec.put("className", safe(node.getClassName()));
                    ec.put("simpleClassName", safe(node.getSimpleClassName()));
                    ec.put("methodName", safe(node.getMethodName()));
                    ec.put("stereotype", node.getStereotype() != null ? node.getStereotype() : "");
                    ec.put("sourceJar", nodeJar != null ? nodeJar : "main");
                    ec.put("module", node.getModule() != null ? node.getModule() : safe(nodeJar));
                    ec.put("domain", node.getDomain() != null ? node.getDomain() : "");
                    extCallList.add(ec);
                }
            } else {
                inScope[0]++;
            }
        }

        // Beans
        String bk = node.getClassName() != null ? node.getClassName() : node.getSimpleClassName();
        if (bk != null && seenBeans.add(bk)) {
            Map<String, String> bean = new LinkedHashMap<>();
            bean.put("className", safe(node.getClassName()));
            bean.put("simpleClassName", safe(node.getSimpleClassName()));
            bean.put("stereotype", node.getStereotype() != null ? node.getStereotype() : "");
            bean.put("sourceJar", node.getSourceJar() != null ? node.getSourceJar() : "");
            beanList.add(bean);
        }

        // Operations
        if (node.getOperationType() != null) opSet.add(node.getOperationType());

        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                walkForAggregation(child, colls, domains, ops, dbOps, bestOp, visited,
                        methodCount, locCount, inScope, extScope, moduleSet, opSet,
                        seenBeans, seenExt, seenHttp, extCallList, httpCallList, beanList,
                        allProcNames, procList, dynFlows, aggFlows, bcPath, controllerJar);
            }
        }
    }

    private static String safe(String s) { return s != null ? s : ""; }
    private static String getStr(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v != null ? v.toString() : null;
    }

    private static int opPri(String op) {
        if (op == null) return 0;
        return switch (op) {
            case "COUNT" -> 1;
            case "AGGREGATE" -> 2;
            case "READ" -> 3;
            case "DELETE" -> 4;
            case "UPDATE" -> 5;
            case "WRITE" -> 6;
            case "CALL" -> 7;
            default -> 0;
        };
    }
}
