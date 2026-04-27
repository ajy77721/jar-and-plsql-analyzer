package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public abstract class AbstractAnalysisDataProvider implements AnalysisDataProvider {

    // Cap large per-endpoint arrays in the summary to prevent multi-hundred-MB responses.
    // ext-policy has 1173 endpoints with 10K+ nodes each — beans/externalCalls/aggregationFlows
    // can have thousands of entries, bloating the summary from ~10MB to 650MB.
    private static final Map<String, Integer> CAPPED_ARRAY_FIELDS = Map.of(
            "beans", 100,
            "externalCalls", 100,
            "httpCalls", 50,
            "dynamicFlows", 200,
            "aggregationFlows", 200
    );

    protected final Path filePath;
    protected final ObjectMapper objectMapper;
    protected final JarDataPaths jarDataPaths;
    protected final String jarName;

    protected AbstractAnalysisDataProvider(Path filePath, ObjectMapper objectMapper,
                                           JarDataPaths jarDataPaths, String jarName) {
        this.filePath = filePath;
        this.objectMapper = objectMapper;
        this.jarDataPaths = jarDataPaths;
        this.jarName = jarName;
    }

    @Override
    public Path getFilePath() { return filePath; }

    @Override
    public void streamAnalysis(JsonParser parser, JsonGenerator gen) throws IOException {
        boolean skipNext = false;
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (skipNext) {
                if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT)
                    parser.skipChildren();
                skipNext = false;
                continue;
            }
            if (token == JsonToken.FIELD_NAME) {
                String field = parser.currentName();
                if ("descriptor".equals(field) || "opcode".equals(field)
                        || "fieldAccesses".equals(field) || "localVariables".equals(field)
                        || "sourceCode".equals(field) || "callTree".equals(field)
                        || "entityFieldMappings".equals(field)) {
                    skipNext = true;
                    continue;
                }
            }
            switch (token) {
                case START_OBJECT -> gen.writeStartObject();
                case END_OBJECT -> gen.writeEndObject();
                case START_ARRAY -> gen.writeStartArray();
                case END_ARRAY -> gen.writeEndArray();
                case FIELD_NAME -> gen.writeFieldName(parser.currentName());
                case VALUE_STRING -> gen.writeString(parser.getText());
                case VALUE_NUMBER_INT -> gen.writeNumber(parser.getLongValue());
                case VALUE_NUMBER_FLOAT -> gen.writeNumber(parser.getDoubleValue());
                case VALUE_TRUE -> gen.writeBoolean(true);
                case VALUE_FALSE -> gen.writeBoolean(false);
                case VALUE_NULL -> gen.writeNull();
                default -> {}
            }
        }
    }

    @Override
    public void streamCallTree(JsonParser parser, JsonGenerator gen, int targetIdx) throws IOException {
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.FIELD_NAME
                    && "endpoints".equals(parser.currentName())) {
                parser.nextToken();
                break;
            }
        }
        if (parser.currentToken() != JsonToken.START_ARRAY) { gen.writeNull(); return; }
        int current = 0;
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.END_ARRAY) { gen.writeNull(); return; }
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;
            if (current < targetIdx) { parser.skipChildren(); current++; continue; }
            while (parser.nextToken() != null) {
                var tok = parser.currentToken();
                if (tok == JsonToken.END_OBJECT) { gen.writeNull(); return; }
                if (tok == JsonToken.FIELD_NAME && "callTree".equals(parser.currentName())) {
                    parser.nextToken();
                    if (parser.currentToken() == JsonToken.VALUE_NULL) gen.writeNull();
                    else copyValue(parser, gen);
                    return;
                }
                if (tok == JsonToken.FIELD_NAME) {
                    parser.nextToken();
                    if (parser.currentToken() == JsonToken.START_OBJECT
                            || parser.currentToken() == JsonToken.START_ARRAY)
                        parser.skipChildren();
                }
            }
            return;
        }
        gen.writeNull();
    }

    @Override
    public void streamClassTree(JsonParser parser, JsonGenerator gen) throws IOException {
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.FIELD_NAME
                    && "classes".equals(parser.currentName())) {
                parser.nextToken();
                break;
            }
        }
        if (parser.currentToken() != JsonToken.START_ARRAY) { gen.writeStartArray(); gen.writeEndArray(); return; }

        Set<String> classKeep = Set.of("fullyQualifiedName", "packageName", "simpleName", "stereotype",
                "sourceJar", "superClass", "isInterface", "isAbstract", "isEnum", "accessFlags",
                "interfaces", "annotations", "repositoryEntityType");
        Set<String> classSkip = Set.of("sourceCode", "entityFieldMappings");
        Set<String> methodSkip = Set.of("invocations", "fieldAccesses", "localVariables", "descriptor", "opcode");
        Set<String> methodKeep = Set.of("name", "returnType", "annotations", "parameters",
                "startLine", "endLine", "accessFlags", "httpMethod", "path", "stringLiterals");

        gen.writeStartArray();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;
            gen.writeStartObject();
            int methodCount = 0;
            boolean wroteMethodSummaries = false;
            boolean wroteFields = false;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) continue;
                String fname = parser.currentName();

                if (classKeep.contains(fname)) {
                    gen.writeFieldName(fname);
                    parser.nextToken();
                    copyValue(parser, gen);
                } else if ("fields".equals(fname)) {
                    parser.nextToken();
                    List<JsonNode> fieldNodes = new ArrayList<>();
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        fieldNodes.add(objectMapper.readTree(parser));
                    }
                    gen.writeNumberField("fieldCount", fieldNodes.size());
                    gen.writeFieldName("fields");
                    gen.writeStartArray();
                    for (var f : fieldNodes) {
                        gen.writeStartObject();
                        writeStringField(gen, "name", textOf(f, "name"));
                        writeStringField(gen, "type", textOf(f, "type"));
                        if (f.has("annotations") && f.get("annotations").isArray() && f.get("annotations").size() > 0) {
                            gen.writeFieldName("annotations");
                            objectMapper.writeValue(gen, f.get("annotations"));
                        }
                        if (f.has("constantValue") && !f.path("constantValue").isNull()) {
                            writeStringField(gen, "constantValue", f.path("constantValue").asText());
                        }
                        if (f.has("mongoFieldName")) writeStringField(gen, "mongoFieldName", textOf(f, "mongoFieldName"));
                        gen.writeNumberField("accessFlags", f.path("accessFlags").asInt(0));
                        gen.writeEndObject();
                    }
                    gen.writeEndArray();
                    wroteFields = true;
                } else if ("methods".equals(fname)) {
                    parser.nextToken();
                    gen.writeFieldName("methodSummaries");
                    gen.writeStartArray();
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                        methodCount++;
                        gen.writeStartObject();
                        while (parser.nextToken() != JsonToken.END_OBJECT) {
                            if (parser.currentToken() != JsonToken.FIELD_NAME) continue;
                            String mf = parser.currentName();
                            if (methodSkip.contains(mf)) {
                                parser.nextToken();
                                if (parser.currentToken() == JsonToken.START_OBJECT
                                        || parser.currentToken() == JsonToken.START_ARRAY)
                                    parser.skipChildren();
                            } else if (methodKeep.contains(mf)) {
                                gen.writeFieldName(mf);
                                parser.nextToken();
                                copyValue(parser, gen);
                            } else {
                                parser.nextToken();
                                if (parser.currentToken() == JsonToken.START_OBJECT
                                        || parser.currentToken() == JsonToken.START_ARRAY)
                                    parser.skipChildren();
                            }
                        }
                        gen.writeEndObject();
                    }
                    gen.writeEndArray();
                    gen.writeNumberField("methodCount", methodCount);
                    wroteMethodSummaries = true;
                } else if (classSkip.contains(fname)) {
                    parser.nextToken();
                    if (parser.currentToken() == JsonToken.START_OBJECT
                            || parser.currentToken() == JsonToken.START_ARRAY)
                        parser.skipChildren();
                } else {
                    parser.nextToken();
                    if (parser.currentToken() == JsonToken.START_OBJECT
                            || parser.currentToken() == JsonToken.START_ARRAY)
                        parser.skipChildren();
                }
            }
            if (!wroteFields) { gen.writeNumberField("fieldCount", 0); gen.writeFieldName("fields"); gen.writeStartArray(); gen.writeEndArray(); }
            if (!wroteMethodSummaries) { gen.writeFieldName("methodSummaries"); gen.writeStartArray(); gen.writeEndArray(); gen.writeNumberField("methodCount", 0); }
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    @Override
    public void streamClassByIndex(JsonParser parser, JsonGenerator gen, int targetIdx) throws IOException {
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.FIELD_NAME
                    && "classes".equals(parser.currentName())) {
                parser.nextToken();
                break;
            }
        }
        if (parser.currentToken() != JsonToken.START_ARRAY) { gen.writeNull(); return; }
        int current = 0;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;
            if (current < targetIdx) {
                parser.skipChildren();
                current++;
                continue;
            }
            copyValue(parser, gen);
            return;
        }
        gen.writeNull();
    }

    @Override
    public void streamSummary(JsonParser parser, JsonGenerator gen) throws IOException {
        // Pre-scan: detect whether endpoints have pre-computed aggregates (externalCalls etc.)
        // before starting the main streaming pass. In existing analysis files, callTree is
        // serialized BEFORE aggregate fields, so we cannot tell at callTree-encounter time.
        // This single pass reads through the first endpoint's callTree using skipChildren()
        // (O(1) memory, no JsonNode allocation) then checks for externalCalls.
        boolean hasPrecomputedAggregates = checkHasPrecomputedAggregates();

        Map<String, String> entityCollMap = new LinkedHashMap<>();
        Map<String, String> repoCollMap = new LinkedHashMap<>();
        List<Map<String, Object>> scheduledJobs = new ArrayList<>();
        List<String> enableSchedulingClasses = new ArrayList<>();
        List<Map<String, Object>> classIndex = new ArrayList<>();
        int scheduledCount = 0;

        gen.writeStartObject();

        parser.nextToken(); // START_OBJECT
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String fieldName = parser.currentName();

            if ("classes".equals(fieldName)) {
                parser.nextToken(); // START_ARRAY
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                    JsonNode classNode = objectMapper.readTree(parser);

                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("fullyQualifiedName", textOf(classNode, "fullyQualifiedName"));
                    entry.put("simpleName", textOf(classNode, "simpleName"));
                    entry.put("packageName", textOf(classNode, "packageName"));
                    entry.put("stereotype", textOf(classNode, "stereotype"));
                    entry.put("sourceJar", textOf(classNode, "sourceJar"));
                    entry.put("isInterface", classNode.path("isInterface").asBoolean(false));
                    entry.put("isAbstract", classNode.path("isAbstract").asBoolean(false));
                    entry.put("isEnum", classNode.path("isEnum").asBoolean(false));

                    var annArr = classNode.path("annotations");
                    List<Map<String, Object>> annList = new ArrayList<>();
                    if (annArr.isArray()) {
                        for (var ann : annArr) {
                            Map<String, Object> a = new LinkedHashMap<>();
                            a.put("name", textOf(ann, "name"));
                            if ("Document".equals(textOf(ann, "name")) && ann.has("attributes")) {
                                a.put("attributes", objectMapper.convertValue(ann.get("attributes"), Map.class));
                            }
                            annList.add(a);
                        }
                    }
                    entry.put("annotations", annList);

                    var ifArr = classNode.path("interfaces");
                    List<String> interfaces = new ArrayList<>();
                    if (ifArr.isArray()) { for (var i : ifArr) interfaces.add(i.asText()); }
                    entry.put("interfaces", interfaces);
                    entry.put("superClass", textOf(classNode, "superClass"));

                    classIndex.add(entry);

                    String fqn = textOf(classNode, "fullyQualifiedName");
                    String simple = textOf(classNode, "simpleName");
                    for (var ann : annArr) {
                        if ("Document".equals(textOf(ann, "name")) && ann.has("attributes")) {
                            String coll = ann.path("attributes").path("collection").asText(null);
                            if (coll == null) coll = ann.path("attributes").path("value").asText(null);
                            if (coll != null) {
                                if (fqn != null) entityCollMap.put(fqn, coll);
                                if (simple != null) entityCollMap.put(simple, coll);
                            }
                        }
                    }

                    String stereotype = textOf(classNode, "stereotype");
                    if ("REPOSITORY".equals(stereotype) || "SPRING_DATA".equals(stereotype)) {
                        var superIfaces = classNode.path("interfaces");
                        if (superIfaces.isArray()) {
                            for (var si : superIfaces) {
                                // Placeholder for generic type extraction
                            }
                        }
                    }

                    var methods = classNode.path("methods");
                    if (methods.isArray()) {
                        for (var method : methods) {
                            var mAnns = method.path("annotations");
                            if (mAnns.isArray()) {
                                for (var mAnn : mAnns) {
                                    String annName = textOf(mAnn, "name");
                                    if ("Scheduled".equals(annName)) {
                                        scheduledCount++;
                                        Map<String, Object> job = new LinkedHashMap<>();
                                        job.put("className", fqn);
                                        job.put("simpleName", simple);
                                        job.put("methodName", textOf(method, "name"));
                                        job.put("stereotype", stereotype);
                                        job.put("sourceJar", textOf(classNode, "sourceJar"));
                                        if (mAnn.has("attributes")) {
                                            job.put("attributes", objectMapper.convertValue(mAnn.get("attributes"), Map.class));
                                        }
                                        scheduledJobs.add(job);
                                    }
                                    if ("EnableScheduling".equals(annName)) {
                                        if (simple != null) enableSchedulingClasses.add(simple);
                                    }
                                }
                            }
                        }
                    }
                    for (var ann : annArr) {
                        if ("EnableScheduling".equals(textOf(ann, "name"))) {
                            if (simple != null && !enableSchedulingClasses.contains(simple))
                                enableSchedulingClasses.add(simple);
                        }
                    }
                }

                gen.writeFieldName("classIndex");
                objectMapper.writeValue(gen, classIndex);
                gen.writeFieldName("entityCollMap");
                objectMapper.writeValue(gen, entityCollMap);
                gen.writeFieldName("repoCollMap");
                objectMapper.writeValue(gen, repoCollMap);
                gen.writeFieldName("scheduledJobs");
                objectMapper.writeValue(gen, scheduledJobs);
                gen.writeNumberField("scheduledCount", scheduledCount);
                gen.writeFieldName("enableSchedulingClasses");
                objectMapper.writeValue(gen, enableSchedulingClasses);

            } else if ("endpoints".equals(fieldName)) {
                gen.writeFieldName("endpoints");
                parser.nextToken(); // START_ARRAY
                gen.writeStartArray();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() != JsonToken.START_OBJECT) continue;

                    // Stream through the endpoint without loading callTree as JsonNode.
                    //
                    // Root cause: in existing analysis files, callTree is serialized BEFORE the
                    // pre-computed aggregate fields (externalCalls, dynamicFlows, etc.). We cannot
                    // know at callTree-encounter time whether aggregates are present.
                    //
                    // Fix: pre-scan the file once (hasPrecomputedAggregates flag set before this loop)
                    // to detect new-format files. For new format: skipChildren() — O(1) memory.
                    // For old format (small trees): readTree → compute aggregates.
                    Map<String, JsonNode> epSmallFields = new LinkedHashMap<>();
                    boolean hasCallTreeToken = false;
                    JsonNode callTreeNode = null;

                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.currentToken() != JsonToken.FIELD_NAME) continue;
                        String fk = parser.currentName();
                        parser.nextToken(); // advance to value
                        if ("sourceCode".equals(fk)) {
                            parser.skipChildren();
                        } else if ("callTree".equals(fk)) {
                            hasCallTreeToken = true;
                            if (hasPrecomputedAggregates) {
                                // New format: aggregates are pre-computed; skip potentially huge tree.
                                // skipChildren() reads tokens using an internal depth counter — O(1) memory.
                                parser.skipChildren();
                            } else {
                                // Old format (small files only): load tree to compute aggregates.
                                if (parser.currentToken() != JsonToken.VALUE_NULL) {
                                    callTreeNode = objectMapper.readTree(parser);
                                }
                            }
                        } else {
                            epSmallFields.put(fk, objectMapper.readTree(parser));
                        }
                    }

                    boolean hasCallTree = hasCallTreeToken;
                    boolean needsAggregates = !epSmallFields.containsKey("externalCalls") && hasCallTree;
                    boolean needsDynamicFlows = (!epSmallFields.containsKey("dynamicFlows")
                            || !epSmallFields.containsKey("aggregationFlows")) && hasCallTree;
                    boolean needsVerification = shouldComputeVerification()
                            && !epSmallFields.containsKey("aggregatedCollectionVerification") && hasCallTree;
                    Map<String, Object> computed = null;
                    if ((needsAggregates || needsDynamicFlows) && callTreeNode != null) {
                        computed = computeAggregatesFromJson(callTreeNode);
                        if (!needsAggregates) {
                            computed.keySet().retainAll(Set.of("dynamicFlows", "aggregationFlows"));
                        }
                    } else if (needsVerification && callTreeNode != null) {
                        computed = computeAggregatesFromJson(callTreeNode);
                        computed.keySet().retainAll(Set.of("aggregatedCollectionVerification", "aggregatedCollectionSources"));
                    }

                    gen.writeStartObject();
                    Set<String> writtenFields = new HashSet<>();
                    for (var entry : epSmallFields.entrySet()) {
                        String key = entry.getKey();
                        writtenFields.add(key);
                        gen.writeFieldName(key);
                        JsonNode val = entry.getValue();
                        // Cap large per-endpoint arrays to prevent huge summaries
                        if (CAPPED_ARRAY_FIELDS.containsKey(key) && val.isArray()
                                && val.size() > CAPPED_ARRAY_FIELDS.get(key)) {
                            int cap = CAPPED_ARRAY_FIELDS.get(key);
                            gen.writeStartArray();
                            int i = 0;
                            for (var item : val) {
                                if (i++ >= cap) break;
                                objectMapper.writeValue(gen, item);
                            }
                            gen.writeEndArray();
                        } else {
                            objectMapper.writeValue(gen, val);
                        }
                    }
                    if (computed != null) {
                        for (var ce : computed.entrySet()) {
                            if (writtenFields.contains(ce.getKey())) continue;
                            gen.writeFieldName(ce.getKey());
                            Integer cap = CAPPED_ARRAY_FIELDS.get(ce.getKey());
                            if (cap != null && ce.getValue() instanceof List<?> list && list.size() > cap) {
                                objectMapper.writeValue(gen, list.subList(0, cap));
                            } else {
                                objectMapper.writeValue(gen, ce.getValue());
                            }
                        }
                    }
                    gen.writeEndObject();
                }
                gen.writeEndArray();

            } else {
                gen.writeFieldName(fieldName);
                parser.nextToken();
                copyValue(parser, gen);
            }
        }

        gen.writeEndObject();
    }

    // ==================== Aggregate Computation ====================

    protected Map<String, Object> computeAggregatesFromJson(JsonNode tree) {
        int[] methodCount = {0}, locCount = {0}, inScope = {0}, extScope = {0}, dbOps = {0};
        Set<String> moduleSet = new LinkedHashSet<>(List.of("main"));
        Set<String> opSet = new LinkedHashSet<>();
        Set<String> seenBeans = new LinkedHashSet<>(), seenExt = new LinkedHashSet<>(), seenHttp = new LinkedHashSet<>();
        List<Map<String, Object>> extCallList = new ArrayList<>(), httpCallList = new ArrayList<>();
        List<Map<String, String>> beanList = new ArrayList<>();
        List<Map<String, Object>> procList = new ArrayList<>();
        Set<String> allProcNames = new LinkedHashSet<>();
        List<Map<String, Object>> dynFlows = new ArrayList<>();
        List<Map<String, Object>> aggFlows = new ArrayList<>();
        Set<String> collSet = new LinkedHashSet<>();
        Map<String, String> collOps = new LinkedHashMap<>();
        Map<String, String> collDomains = new LinkedHashMap<>();
        Map<String, String> collVerification = new LinkedHashMap<>();
        Map<String, String> collSources = new LinkedHashMap<>();
        String controllerJar = textOf(tree, "sourceJar");
        Set<JsonNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());

        Deque<Map<String, String>> path = new ArrayDeque<>();
        walkCallTreeJson(tree, controllerJar, visited, path, methodCount, locCount, inScope, extScope, dbOps,
                moduleSet, opSet, seenBeans, seenExt, seenHttp, extCallList, httpCallList, beanList, procList, allProcNames,
                dynFlows, aggFlows, collSet, collOps, collDomains, collVerification, collSources);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalMethods", methodCount[0]);
        result.put("totalLoc", locCount[0]);
        result.put("totalDbOperations", dbOps[0]);
        result.put("inScopeCalls", inScope[0]);
        result.put("externalScopeCalls", extScope[0]);
        result.put("modules", new ArrayList<>(moduleSet));
        result.put("externalCalls", extCallList);
        result.put("httpCalls", httpCallList);
        result.put("beans", beanList);
        result.put("operationTypesList", new ArrayList<>(opSet));
        result.put("procNames", procList);
        if (!dynFlows.isEmpty()) result.put("dynamicFlows", dynFlows);
        if (!aggFlows.isEmpty()) result.put("aggregationFlows", aggFlows);
        if (!collSet.isEmpty()) {
            result.put("aggregatedCollections", new ArrayList<>(collSet));
            result.put("aggregatedOperations", collOps);
            result.put("aggregatedCollectionDomains", collDomains);
            if (!collVerification.isEmpty()) result.put("aggregatedCollectionVerification", collVerification);
            if (!collSources.isEmpty()) result.put("aggregatedCollectionSources", collSources);
        }
        return result;
    }

    protected void walkCallTreeJson(JsonNode node, String controllerJar,
            Set<JsonNode> visited, Deque<Map<String, String>> path,
            int[] methodCount, int[] locCount, int[] inScope, int[] extScope, int[] dbOps,
            Set<String> moduleSet, Set<String> opSet,
            Set<String> seenBeans, Set<String> seenExt, Set<String> seenHttp,
            List<Map<String, Object>> extCallList, List<Map<String, Object>> httpCallList,
            List<Map<String, String>> beanList,
            List<Map<String, Object>> procList, Set<String> allProcNames,
            List<Map<String, Object>> dynFlows, List<Map<String, Object>> aggFlows,
            Set<String> collSet, Map<String, String> collOps, Map<String, String> collDomains,
            Map<String, String> collVerification, Map<String, String> collSources) {
        if (node == null || node.isNull() || !visited.add(node)) return;

        Map<String, String> seg = Map.of(
                "simpleClassName", safe(textOf(node, "simpleClassName")),
                "methodName", safe(textOf(node, "methodName")),
                "className", safe(textOf(node, "className")),
                "sourceJar", safe(textOf(node, "sourceJar")));
        path.addLast(seg);

        methodCount[0]++;
        int lc = node.path("lineCount").asInt(0);
        if (lc > 0) locCount[0] += lc;

        String opType = textOf(node, "operationType");
        if (opType != null) opSet.add(opType);

        var collsArr = node.path("collectionsAccessed");
        if (collsArr.isArray() && collsArr.size() > 0) {
            dbOps[0]++;
            var collDomainsNode = node.path("collectionDomains");
            var collVerifNode = node.path("collectionVerification");
            var collSrcNode = node.path("collectionSources");
            for (var coll : collsArr) {
                String collName = coll.asText(null);
                if (collName != null) {
                    collSet.add(collName);
                    if (opType != null && !collOps.containsKey(collName)) collOps.put(collName, opType);
                    if (collDomainsNode.has(collName) && !collDomains.containsKey(collName)) {
                        collDomains.put(collName, collDomainsNode.path(collName).asText(""));
                    }
                    if (collVerifNode.has(collName)) {
                        String v = collVerifNode.path(collName).asText(null);
                        if (v != null) {
                            String cur = collVerification.get(collName);
                            if (cur == null || "NO_CATALOG".equals(cur) || "NOT_IN_DB".equals(cur)
                                    || (v.startsWith("CLAUDE_") && !"CLAUDE_REMOVED".equals(v))) {
                                collVerification.put(collName, v);
                            }
                        }
                    }
                    if (collSrcNode.has(collName) && !collSources.containsKey(collName)) {
                        collSources.put(collName, collSrcNode.path(collName).asText(""));
                    }
                }
            }
        }
        else if (node.path("hasDbInteraction").asBoolean(false) && opType != null) dbOps[0]++;

        String nodeId = textOf(node, "id");
        String callType = textOf(node, "callType");

        if ("HTTP_CALL".equals(callType) || (nodeId != null && nodeId.startsWith("http:"))) {
            extScope[0]++;
            String hk = safe(textOf(node, "className")) + "#" + safe(textOf(node, "methodName"));
            if (seenHttp.add(hk)) {
                Map<String, Object> hc = new LinkedHashMap<>();
                hc.put("className", safe(textOf(node, "className")));
                hc.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                hc.put("methodName", safe(textOf(node, "methodName")));
                hc.put("operationType", opType != null ? opType : "READ");
                var strLiterals = node.path("stringLiterals");
                if (strLiterals.isArray()) {
                    for (var lit : strLiterals) {
                        String s = lit.asText("");
                        if (s.startsWith("http://") || s.startsWith("https://") || s.startsWith("/")) {
                            hc.put("url", s);
                            break;
                        }
                    }
                }
                hc.put("breadcrumb", buildSimpleBreadcrumb(path, 5));
                httpCallList.add(hc);
            }
        }
        else if (nodeId != null && nodeId.startsWith("ext:")) {
            extScope[0]++;
            String ek = safe(textOf(node, "className")) + "#" + safe(textOf(node, "methodName"));
            if (seenExt.add(ek)) {
                Map<String, Object> ec = new LinkedHashMap<>();
                ec.put("className", safe(textOf(node, "className")));
                ec.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                ec.put("methodName", safe(textOf(node, "methodName")));
                ec.put("stereotype", safe(textOf(node, "stereotype")));
                ec.put("sourceJar", "unknown");
                ec.put("module", safe(textOf(node, "simpleClassName")));
                ec.put("domain", "External");
                ec.put("breadcrumb", buildSimpleBreadcrumb(path, 5));
                extCallList.add(ec);
            }
        }
        else {
            String nodeJar = textOf(node, "sourceJar");
            boolean isCross = node.path("crossModule").asBoolean(false)
                    || (nodeJar != null && !Objects.equals(nodeJar, controllerJar));
            if (isCross) {
                extScope[0]++;
                // Skip well-known third-party library JARs from the external-calls list.
                // They're JDBC connection pools, utilities, framework internals — not meaningful
                // cross-service dependencies. Filter by both JAR name and class package prefix.
                String nodeClass = textOf(node, "className");
                if (!isKnownLibraryNode(nodeJar, nodeClass)) {
                    if (nodeJar != null) moduleSet.add(nodeJar);
                    String ek = safe(nodeJar) + ":" + safe(nodeClass) + "#" + safe(textOf(node, "methodName"));
                    if (seenExt.add(ek)) {
                        Map<String, Object> ec = new LinkedHashMap<>();
                        ec.put("className", safe(nodeClass));
                        ec.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                        ec.put("methodName", safe(textOf(node, "methodName")));
                        ec.put("stereotype", safe(textOf(node, "stereotype")));
                        ec.put("sourceJar", nodeJar != null ? nodeJar : "main");
                        ec.put("module", textOf(node, "module") != null ? textOf(node, "module") : safe(nodeJar));
                        ec.put("domain", textOf(node, "domain") != null ? textOf(node, "domain") : "");
                        ec.put("breadcrumb", buildSimpleBreadcrumb(path, 5));
                        extCallList.add(ec);
                    }
                }
            } else {
                inScope[0]++;
            }
        }

        String bk = textOf(node, "className") != null ? textOf(node, "className") : textOf(node, "simpleClassName");
        if (bk != null && seenBeans.add(bk)) {
            Map<String, String> bean = new LinkedHashMap<>();
            bean.put("className", safe(textOf(node, "className")));
            bean.put("simpleClassName", safe(textOf(node, "simpleClassName")));
            bean.put("stereotype", safe(textOf(node, "stereotype")));
            bean.put("sourceJar", safe(textOf(node, "sourceJar")));
            beanList.add(bean);
        }

        var annDetails = node.path("annotationDetails");
        if (annDetails.isArray()) {
            for (var ad : annDetails) {
                if (textOf(ad, "name") != null && textOf(ad, "name").contains("LogParameters")) {
                    var attrs = ad.path("attributes");
                    String pn = textOf(attrs, "procedureName");
                    if (pn == null) pn = textOf(attrs, "value");
                    if (pn == null) pn = textOf(attrs, "procName");
                    if (pn != null && allProcNames.add(pn)) {
                        Map<String, Object> entry = new LinkedHashMap<>();
                        entry.put("procName", pn);
                        entry.put("simpleClassName", textOf(node, "simpleClassName"));
                        entry.put("methodName", textOf(node, "methodName"));
                        entry.put("className", textOf(node, "className"));
                        procList.add(entry);
                    }
                }
            }
        }

        // Dynamic flows: non-direct dispatch, reflection, dynamic queries
        String dt = textOf(node, "dispatchType");
        if (dt != null && !"DIRECT".equals(dt) && !"SPRING_DATA_DERIVED".equals(dt)) {
            Map<String, Object> df = new LinkedHashMap<>();
            df.put("category", "DISPATCH");
            df.put("dispatchType", dt);
            df.put("className", safe(textOf(node, "className")));
            df.put("simpleClassName", safe(textOf(node, "simpleClassName")));
            df.put("methodName", safe(textOf(node, "methodName")));
            df.put("resolvedFrom", textOf(node, "resolvedFrom"));
            df.put("qualifierHint", textOf(node, "qualifierHint"));
            df.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
            dynFlows.add(df);
        }

        var strLiterals = node.path("stringLiterals");
        if (strLiterals.isArray()) {
            for (var lit : strLiterals) {
                String s = lit.asText("");
                for (String pat : List.of("Class.forName", "Method.invoke", "getMethod", "getDeclaredMethod", "newInstance")) {
                    if (s.contains(pat)) {
                        Map<String, Object> df = new LinkedHashMap<>();
                        df.put("category", "REFLECTION");
                        df.put("dispatchType", "REFLECTION");
                        df.put("className", safe(textOf(node, "className")));
                        df.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                        df.put("methodName", safe(textOf(node, "methodName")));
                        df.put("pattern", pat + ": " + s);
                        df.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
                        dynFlows.add(df);
                    }
                }
            }
        }

        if (annDetails.isArray()) {
            for (var ad2 : annDetails) {
                String adn = textOf(ad2, "name");
                if (adn != null && adn.contains("Query")) {
                    var qAttrs = ad2.path("attributes");
                    String qv = textOf(qAttrs, "value");
                    if (qv != null && (qv.contains("?") || qv.contains("#{"))) {
                        Map<String, Object> df = new LinkedHashMap<>();
                        df.put("category", "DYNAMIC_QUERY");
                        df.put("dispatchType", "DYNAMIC_QUERY");
                        df.put("className", safe(textOf(node, "className")));
                        df.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                        df.put("methodName", safe(textOf(node, "methodName")));
                        df.put("pattern", "Parameterized @Query");
                        df.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
                        dynFlows.add(df);
                    }
                }
            }
        }

        // Aggregation flow detection
        String opType2 = textOf(node, "operationType");
        boolean isAggNode = "AGGREGATE".equals(opType2) || "COUNT".equals(opType2);
        boolean hasPipelineSrc = false;
        var collSrcNode2 = node.path("collectionSources");
        if (collSrcNode2.isObject()) {
            var srcIt = collSrcNode2.fields();
            while (srcIt.hasNext()) {
                String sv = srcIt.next().getValue().asText("");
                if (sv.contains("PIPELINE") || sv.contains("AGGREGATION") || sv.contains("TEMPLATE_AGGREGATE")) {
                    hasPipelineSrc = true; break;
                }
            }
        }
        List<String> pipeStages = new ArrayList<>();
        List<String> lookupColls2 = new ArrayList<>();
        var strLitsAgg = node.path("stringLiterals");
        if (strLitsAgg.isArray()) {
            for (var lit : strLitsAgg) {
                String s = lit.asText("");
                if (s.contains("$lookup")) pipeStages.add("$lookup");
                if (s.contains("$graphLookup")) pipeStages.add("$graphLookup");
                if (s.contains("$unwind")) pipeStages.add("$unwind");
                if (s.contains("$match")) pipeStages.add("$match");
                if (s.contains("$group")) pipeStages.add("$group");
                if (s.contains("$project")) pipeStages.add("$project");
                if (s.contains("$sort")) pipeStages.add("$sort");
                if (s.contains("$out")) pipeStages.add("$out");
                if (s.contains("$merge")) pipeStages.add("$merge");
                if (s.contains("$unionWith")) pipeStages.add("$unionWith");
                if (s.contains("$count")) pipeStages.add("$count");
                if (s.contains("$addFields")) pipeStages.add("$addFields");
                if (s.contains("$replaceRoot")) pipeStages.add("$replaceRoot");
                if (s.contains("$facet")) pipeStages.add("$facet");
                java.util.regex.Matcher lm = java.util.regex.Pattern
                        .compile("\\$(lookup|graphLookup)[^}]*[\"']from[\"']\\s*:\\s*[\"']([A-Z][A-Z0-9_]+)[\"']")
                        .matcher(s);
                while (lm.find()) lookupColls2.add(lm.group(2));
            }
        }
        boolean hasRichPipe = node.has("aggregationPipeline") && node.path("aggregationPipeline").isObject()
                && !node.path("aggregationPipeline").isEmpty();
        if (isAggNode || hasPipelineSrc || !pipeStages.isEmpty() || hasRichPipe) {
            var collsArr2 = node.path("collectionsAccessed");
            List<String> nodeCollsList = new ArrayList<>();
            if (collsArr2.isArray()) { for (var c : collsArr2) { String cn = c.asText(null); if (cn != null) nodeCollsList.add(cn); } }
            if (!nodeCollsList.isEmpty() || !pipeStages.isEmpty() || isAggNode || hasRichPipe) {
                Map<String, Object> af = new LinkedHashMap<>();
                af.put("className", safe(textOf(node, "className")));
                af.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                af.put("methodName", safe(textOf(node, "methodName")));
                af.put("operationType", opType2);
                af.put("collections", nodeCollsList);
                Map<String, String> cDoms = new LinkedHashMap<>();
                var cdNode = node.path("collectionDomains");
                if (cdNode.isObject()) { var it = cdNode.fields(); while (it.hasNext()) { var e = it.next(); cDoms.put(e.getKey(), e.getValue().asText("")); } }
                af.put("collectionDomains", cDoms);
                Map<String, String> cSrcs = new LinkedHashMap<>();
                if (collSrcNode2.isObject()) { var it = collSrcNode2.fields(); while (it.hasNext()) { var e = it.next(); cSrcs.put(e.getKey(), e.getValue().asText("")); } }
                af.put("collectionSources", cSrcs);
                af.put("pipelineStages", new ArrayList<>(new LinkedHashSet<>(pipeStages)));
                af.put("lookupCollections", lookupColls2);
                af.put("stereotype", safe(textOf(node, "stereotype")));
                af.put("sourceJar", safe(textOf(node, "sourceJar")));
                af.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
                if (annDetails.isArray()) {
                    List<Map<String, Object>> qAnns = new ArrayList<>();
                    for (var ad3 : annDetails) {
                        String an3 = textOf(ad3, "name");
                        if (an3 != null && (an3.contains("Query") || an3.contains("Aggregation"))) {
                            Map<String, Object> annMap = new LinkedHashMap<>();
                            annMap.put("name", an3);
                            var attrs3 = ad3.path("attributes");
                            if (attrs3.isObject()) {
                                Map<String, String> am = new LinkedHashMap<>();
                                var ai = attrs3.fields(); while (ai.hasNext()) { var ae = ai.next(); am.put(ae.getKey(), ae.getValue().asText("")); }
                                annMap.put("attributes", am);
                            }
                            qAnns.add(annMap);
                        }
                    }
                    if (!qAnns.isEmpty()) af.put("queryAnnotations", qAnns);
                }
                // Extract rich pipeline detail from aggregationPipeline JSON field (if present)
                var pipeNode = node.path("aggregationPipeline");
                if (pipeNode.isObject()) {
                    var stagesArr = pipeNode.path("stages");
                    if (stagesArr.isArray() && !stagesArr.isEmpty()) {
                        // Override pipelineStages with rich data
                        List<String> richStages = new ArrayList<>();
                        for (var s : stagesArr) {
                            String stg = s.has("stage") ? s.path("stage").asText("") : "";
                            if (!stg.isEmpty()) richStages.add(stg);
                        }
                        if (!richStages.isEmpty()) af.put("pipelineStages", new ArrayList<>(new LinkedHashSet<>(richStages)));
                    }
                    var lkArr = pipeNode.path("lookupTargets");
                    if (lkArr.isArray() && !lkArr.isEmpty()) {
                        List<Map<String, String>> lkList = new ArrayList<>();
                        for (var lk : lkArr) {
                            Map<String, String> lkMap = new LinkedHashMap<>();
                            for (String k : List.of("type", "from", "localField", "foreignField", "as", "source")) {
                                String v = lk.has(k) ? lk.path(k).asText("") : "";
                                if (!v.isEmpty()) lkMap.put(k, v);
                            }
                            lkList.add(lkMap);
                        }
                        af.put("lookupTargets", lkList);
                    }
                    for (String fld : List.of("matchFields", "groupFields", "projectionFields", "sortFields", "detectionSources", "pipelineSourceMethods")) {
                        var arr = pipeNode.path(fld);
                        if (arr.isArray() && !arr.isEmpty()) {
                            List<String> vals = new ArrayList<>();
                            for (var v : arr) vals.add(v.asText(""));
                            af.put(fld, vals);
                        }
                    }
                    var compNode = pipeNode.path("companionUsage");
                    if (compNode.isObject() && !compNode.isEmpty()) {
                        Map<String, List<String>> compMap = new LinkedHashMap<>();
                        var ci = compNode.fields();
                        while (ci.hasNext()) {
                            var ce = ci.next();
                            List<String> calls = new ArrayList<>();
                            if (ce.getValue().isArray()) { for (var cv : ce.getValue()) calls.add(cv.asText("")); }
                            compMap.put(ce.getKey(), calls);
                        }
                        af.put("companionUsage", compMap);
                    }
                    if (pipeNode.path("isDynamic").asBoolean(false)) af.put("isDynamic", true);
                }
                aggFlows.add(af);
            }
        }

        var children = node.path("children");
        if (children.isArray()) {
            for (var child : children) {
                walkCallTreeJson(child, controllerJar, visited, path, methodCount, locCount, inScope, extScope, dbOps,
                        moduleSet, opSet, seenBeans, seenExt, seenHttp, extCallList, httpCallList, beanList, procList, allProcNames,
                        dynFlows, aggFlows, collSet, collOps, collDomains, collVerification, collSources);
            }
        }
        path.removeLast();
    }

    private List<Map<String, String>> buildBreadcrumbFromPath(Deque<Map<String, String>> path, int maxDepth) {
        List<Map<String, String>> result = new ArrayList<>();
        int skip = Math.max(0, path.size() - maxDepth);
        int i = 0;
        Map<String, String> prev = null;
        for (Map<String, String> seg : path) {
            if (i++ < skip) continue;
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("label", seg.get("simpleClassName") + "." + seg.get("methodName"));
            entry.put("full", seg.get("className") + "." + seg.get("methodName"));
            entry.put("className", seg.get("className"));
            entry.put("methodName", seg.get("methodName"));
            entry.put("jar", seg.getOrDefault("sourceJar", "main"));
            entry.put("isExternal", String.valueOf(prev != null && !Objects.equals(
                    seg.getOrDefault("sourceJar", ""), prev.getOrDefault("sourceJar", ""))));
            entry.put("level", String.valueOf(i));
            result.add(entry);
            prev = seg;
        }
        return result;
    }

    /**
     * Returns true if a node in the call tree belongs to a well-known third-party library.
     * Such nodes are excluded from the external-calls report since they're implementation details
     * of JDBC pools, collections frameworks, ORM, etc. — not meaningful cross-service dependencies.
     * Filters by both the sourceJar filename and the class package prefix.
     */
    private static boolean isKnownLibraryNode(String sourceJar, String className) {
        if (sourceJar != null) {
            String jar = sourceJar.toLowerCase();
            if (jar.startsWith("hikaricp") || jar.startsWith("c3p0") || jar.startsWith("dbcp")
                    || jar.startsWith("guava") || jar.startsWith("commons-")
                    || jar.startsWith("spring-") || jar.startsWith("hibernate-")
                    || jar.startsWith("jackson-") || jar.startsWith("netty-")
                    || jar.startsWith("reactor-") || jar.startsWith("byte-buddy")
                    || jar.startsWith("javassist") || jar.startsWith("asm-")
                    || jar.startsWith("ojdbc") || jar.startsWith("orai18n")
                    || jar.startsWith("micrometer") || jar.startsWith("slf4j")
                    || jar.startsWith("logback") || jar.startsWith("log4j")
                    || jar.startsWith("tomcat-") || jar.startsWith("catalina")
                    || jar.startsWith("druid")) {
                return true;
            }
        }
        if (className != null) {
            return className.startsWith("com.zaxxer.hikari.") || className.startsWith("com.google.common.")
                || className.startsWith("com.google.guava.") || className.startsWith("com.google.gson.")
                || className.startsWith("org.apache.commons.") || className.startsWith("org.apache.tomcat.")
                || className.startsWith("org.apache.catalina.") || className.startsWith("org.apache.http.")
                || className.startsWith("org.springframework.") || className.startsWith("org.hibernate.")
                || className.startsWith("com.fasterxml.jackson.") || className.startsWith("io.netty.")
                || className.startsWith("reactor.") || className.startsWith("net.bytebuddy.")
                || className.startsWith("javassist.") || className.startsWith("org.objectweb.asm.")
                || className.startsWith("io.micrometer.") || className.startsWith("ch.qos.logback.")
                || className.startsWith("org.slf4j.") || className.startsWith("com.mchange.")
                || className.startsWith("oracle.jdbc.driver.") || className.startsWith("oracle.jdbc.proxy.");
        }
        return false;
    }

    private List<String> buildSimpleBreadcrumb(Deque<Map<String, String>> path, int maxDepth) {
        List<String> result = new ArrayList<>();
        int skip = Math.max(0, path.size() - maxDepth);
        int i = 0;
        for (Map<String, String> seg : path) {
            if (i++ < skip) continue;
            result.add(seg.getOrDefault("simpleClassName", "") + "." + seg.getOrDefault("methodName", "") + "()");
        }
        return result;
    }

    // ==================== Summary Slice Streaming ====================

    // Heavy per-endpoint fields excluded from headers-only mode
    private static final Set<String> HEAVY_ENDPOINT_FIELDS = Set.of(
            "beans", "externalCalls", "httpCalls", "dynamicFlows", "aggregationFlows", "parameters"
    );

    @Override
    public void streamSummarySlice(JsonParser summaryParser, JsonGenerator gen,
                                   String mode, Set<String> fields) throws IOException {
        if ("headers".equals(mode)) {
            streamSummaryHeaders(summaryParser, gen);
        } else if ("slice".equals(mode) && fields != null && !fields.isEmpty()) {
            streamEndpointFieldSlice(summaryParser, gen, fields);
        }
    }

    // Fields skipped entirely in headers mode (too heavy for initial load)
    private static final Set<String> HEADERS_SKIP_FIELDS = Set.of(
            "callTree", "sourceCode", "beans", "externalCalls", "httpCalls",
            "dynamicFlows", "aggregationFlows", "parameters"
    );

    private void streamSummaryHeaders(JsonParser parser, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        parser.nextToken(); // START_OBJECT
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String fieldName = parser.currentName();

            if ("endpoints".equals(fieldName)) {
                gen.writeFieldName("endpoints");
                parser.nextToken(); // START_ARRAY
                gen.writeStartArray();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                    // Stream field-by-field — skip heavy fields without loading them into memory
                    gen.writeStartObject();
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.currentToken() != JsonToken.FIELD_NAME) continue;
                        String epField = parser.currentName();
                        parser.nextToken(); // advance to value token
                        if (HEADERS_SKIP_FIELDS.contains(epField)) {
                            parser.skipChildren(); // skip heavy/unwanted fields without materializing
                        } else {
                            gen.writeFieldName(epField);
                            copyValue(parser, gen);
                        }
                    }
                    gen.writeEndObject();
                }
                gen.writeEndArray();
            } else {
                gen.writeFieldName(fieldName);
                parser.nextToken();
                copyValue(parser, gen);
            }
        }
        gen.writeEndObject();
    }

    private void streamEndpointFieldSlice(JsonParser parser, JsonGenerator gen,
                                          Set<String> targetFields) throws IOException {
        // Skip to "endpoints" array in the summary JSON, extract only targeted fields
        gen.writeStartArray();
        parser.nextToken(); // START_OBJECT
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String fieldName = parser.currentName();

            if ("endpoints".equals(fieldName)) {
                parser.nextToken(); // START_ARRAY
                int idx = 0;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                    JsonNode epNode = objectMapper.readTree(parser);

                    // Check if this endpoint has any of the target fields with non-empty values
                    boolean hasData = false;
                    for (String tf : targetFields) {
                        JsonNode val = epNode.get(tf);
                        if (val != null && !val.isNull() && !(val.isArray() && val.isEmpty())
                                && !(val.isObject() && val.isEmpty())) {
                            hasData = true;
                            break;
                        }
                    }
                    if (hasData) {
                        gen.writeStartObject();
                        gen.writeNumberField("endpointIdx", idx);
                        // Include endpoint identity for merging
                        if (epNode.has("fullPath"))
                            gen.writeStringField("fullPath", epNode.get("fullPath").asText(""));
                        if (epNode.has("httpMethod"))
                            gen.writeStringField("httpMethod", epNode.get("httpMethod").asText(""));
                        for (String tf : targetFields) {
                            JsonNode val = epNode.get(tf);
                            if (val != null && !val.isNull()) {
                                gen.writeFieldName(tf);
                                objectMapper.writeValue(gen, val);
                            }
                        }
                        gen.writeEndObject();
                    }
                    idx++;
                }
            } else {
                // Skip non-endpoints fields
                parser.nextToken();
                parser.skipChildren();
            }
        }
        gen.writeEndArray();
    }

    // ==================== Helpers ====================

    /**
     * Pre-scan the analysis file to detect whether endpoints have pre-computed aggregate fields
     * (externalCalls, dynamicFlows, aggregationFlows). In existing analysis files, callTree is
     * written BEFORE these fields, so we need to skip the callTree (using non-recursive
     * skipChildren()) and then check for externalCalls in the remaining fields.
     *
     * Returns true if aggregates are pre-computed → caller should use skipChildren() on callTree.
     * Returns false if old format → caller loads callTree via readTree() for aggregate computation.
     */
    private boolean checkHasPrecomputedAggregates() {
        try (var scanner = objectMapper.getFactory().createParser(filePath.toFile())) {
            scanner.nextToken(); // consume START_OBJECT (top-level)

            // Scan top-level fields, skipping each value until we reach "endpoints"
            while (scanner.nextToken() != null) {
                if (scanner.currentToken() != JsonToken.FIELD_NAME) continue;
                String topField = scanner.currentName();
                scanner.nextToken(); // advance to the value of this top-level field
                if ("endpoints".equals(topField)) {
                    break; // scanner.currentToken() is now START_ARRAY for endpoints
                }
                // Skip this field's value (e.g., the multi-GB "classes" array) without allocating
                if (scanner.currentToken() == JsonToken.START_OBJECT
                        || scanner.currentToken() == JsonToken.START_ARRAY) {
                    scanner.skipChildren();
                }
            }
            if (scanner.currentToken() != JsonToken.START_ARRAY) return false;

            // Read the first endpoint object
            if (scanner.nextToken() != JsonToken.START_OBJECT) return false;

            // Stream through the first endpoint's fields, skipping each value
            while (scanner.nextToken() != JsonToken.END_OBJECT) {
                if (scanner.currentToken() != JsonToken.FIELD_NAME) continue;
                String fk = scanner.currentName();
                scanner.nextToken(); // advance to value
                if ("externalCalls".equals(fk)) return true; // new format confirmed
                if (scanner.currentToken() == JsonToken.START_OBJECT
                        || scanner.currentToken() == JsonToken.START_ARRAY) {
                    scanner.skipChildren();
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    protected void copyValue(JsonParser parser, JsonGenerator gen) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT -> {
                gen.writeStartObject();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    gen.writeFieldName(parser.currentName()); parser.nextToken(); copyValue(parser, gen);
                }
                gen.writeEndObject();
            }
            case START_ARRAY -> {
                gen.writeStartArray();
                while (parser.nextToken() != JsonToken.END_ARRAY) copyValue(parser, gen);
                gen.writeEndArray();
            }
            case VALUE_STRING -> gen.writeString(parser.getText());
            case VALUE_NUMBER_INT -> gen.writeNumber(parser.getLongValue());
            case VALUE_NUMBER_FLOAT -> gen.writeNumber(parser.getDoubleValue());
            case VALUE_TRUE -> gen.writeBoolean(true);
            case VALUE_FALSE -> gen.writeBoolean(false);
            case VALUE_NULL -> gen.writeNull();
            default -> {}
        }
    }

    protected static String safe(String s) { return s != null ? s : ""; }

    protected static String textOf(JsonNode node, String field) {
        var v = node.path(field);
        return v.isNull() || v.isMissingNode() ? null : v.asText();
    }

    protected static void writeStringField(JsonGenerator gen, String name, String value) throws IOException {
        if (value != null) gen.writeStringField(name, value);
        else gen.writeNullField(name);
    }
}
