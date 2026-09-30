package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
            "aggregationFlows", 200,
            "dynamicCollectionFlows", 200,
            "collectionLifecycleFlows", 100
    );

    protected final com.analyzer.storage.ArtifactKey documentKey;
    protected final ObjectMapper objectMapper;
    protected final JarDataPaths jarDataPaths;
    protected final String jarName;

    /**
     * @param documentKey the analysis document's storage key. Taken directly rather than derived
     *                    from a {@code Path} via {@code keyForPath}: the key IS the identity of the
     *                    document, and a provider built from a path outside the data root used to
     *                    be representable (key silently null) — it no longer is.
     */
    protected AbstractAnalysisDataProvider(com.analyzer.storage.ArtifactKey documentKey,
                                           ObjectMapper objectMapper,
                                           JarDataPaths jarDataPaths, String jarName) {
        this.objectMapper = objectMapper;
        this.jarDataPaths = jarDataPaths;
        this.jarName = jarName;
        this.documentKey = documentKey;
    }

    /** The analysis document's storage key. */
    protected com.analyzer.storage.ArtifactKey documentKey() {
        if (documentKey == null) {
            throw new IllegalStateException(
                    "Analysis provider for '" + jarName + "' was built without a document key.");
        }
        return documentKey;
    }

    @Override
    public java.io.InputStream openStream() throws IOException {
        // Buffered: the streaming parsers below read token-by-token, and an unbuffered
        // per-byte read against a multi-GB document is pathologically slow.
        return new java.io.BufferedInputStream(
                jarDataPaths.store().read(documentKey()), 256 * 1024);
    }

    @Override
    public long lastModifiedMillis() {
        return jarDataPaths.store().lastModified(documentKey());
    }

    @Override
    public boolean exists() {
        return jarDataPaths.store().exists(documentKey());
    }

    @Override
    public com.analyzer.storage.ArtifactKey getDocumentKey() { return documentKey(); }

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
                        || "fragments".equals(field)
                        // Flat-format twins of callTree/fragments plus the dictionary itself.
                        // /analysis has never carried the call tree, so dropping these keeps the
                        // response byte-identical for documents written in either format.
                        || CallTreeFlattener.ROOT_ID_FIELD.equals(field)
                        || CallTreeFlattener.FRAGMENT_IDS_FIELD.equals(field)
                        || CallTreeFlattener.NODES_FIELD.equals(field)
                        || CallTreeFlattener.FORMAT_FIELD.equals(field)
                        // Same treatment for the endpoint pool: internal storage detail, and its
                        // marker leaked into /summary/header until this was added.
                        || EndpointArrayPool.POOL_FIELD.equals(field)
                        || InvocationPool.POOL_FIELD.equals(field)
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

    /**
     * The call tree as the flat DAG, rather than expanded.
     *
     * <p>Same forward pass as {@link #streamCallTree}; only the final step differs — the reachable
     * dictionary is written instead of the materialised tree. Measured on endpoint 271 of
     * {@code unified-web-1.0.0-SNAPSHOT}: 2,282 nodes / 7.4 MB here against 300,167,238 nodes
     * expanded.
     *
     * <p>A legacy document with no node dictionary has nothing to send in this form, so it answers
     * {@code null} and the caller falls back to the expanded endpoint rather than getting an empty
     * object that looks like a tree with no nodes.
     */
    public void streamCallTreeDag(JsonParser parser, JsonGenerator gen, int targetIdx) throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) { gen.writeNull(); return; }
        boolean found = false;
        CallTreeInflater inflater = CallTreeInflater.empty();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String name = parser.currentName();
            parser.nextToken();
            if ("endpoints".equals(name)) { found = true; break; }
            if (CallTreeFlattener.NODES_FIELD.equals(name)) {
                inflater = CallTreeInflater.read(parser, objectMapper);
                continue;
            }
            if (parser.currentToken() == JsonToken.START_ARRAY
                    || parser.currentToken() == JsonToken.START_OBJECT)
                parser.skipChildren();
        }
        if (!found || parser.currentToken() != JsonToken.START_ARRAY) { gen.writeNull(); return; }
        if (!inflater.isFlat()) { gen.writeNull(); return; }   // legacy: no dictionary to send

        int current = 0;
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.END_ARRAY) { gen.writeNull(); return; }
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;
            if (current < targetIdx) { parser.skipChildren(); current++; continue; }
            while (parser.nextToken() != null) {
                var tok = parser.currentToken();
                if (tok == JsonToken.END_OBJECT) { gen.writeNull(); return; }
                if (tok == JsonToken.FIELD_NAME
                        && CallTreeFlattener.ROOT_ID_FIELD.equals(parser.currentName())) {
                    parser.nextToken();
                    inflater.writeReachable(
                            parser.currentToken() == JsonToken.VALUE_NULL ? null : parser.getText(), gen);
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
    public void streamCallTree(JsonParser parser, JsonGenerator gen, int targetIdx) throws IOException {
        // Seek to top-level "endpoints" array only — scanning all tokens naively would
        // match nested fields also named "endpoints" (e.g. inside localVariables maps).
        if (parser.nextToken() != JsonToken.START_OBJECT) { gen.writeNull(); return; }
        boolean found = false;
        // Flat-format documents carry the shared node dictionary ahead of "endpoints" precisely
        // so this single forward pass can pick it up before it needs it. Legacy documents have
        // no such field and this stays empty, leaving the old inline path below untouched.
        CallTreeInflater inflater = CallTreeInflater.empty();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String name = parser.currentName();
            parser.nextToken(); // advance to value
            if ("endpoints".equals(name)) { found = true; break; }
            if (CallTreeFlattener.NODES_FIELD.equals(name)) {
                inflater = CallTreeInflater.read(parser, objectMapper);
                continue;
            }
            // skip value of any other top-level field (e.g. the large "classes" array)
            if (parser.currentToken() == JsonToken.START_ARRAY
                    || parser.currentToken() == JsonToken.START_OBJECT)
                parser.skipChildren();
        }
        if (!found || parser.currentToken() != JsonToken.START_ARRAY) { gen.writeNull(); return; }
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
                if (tok == JsonToken.FIELD_NAME
                        && CallTreeFlattener.ROOT_ID_FIELD.equals(parser.currentName())) {
                    parser.nextToken();
                    // Re-expands the DAG into the nested tree this endpoint has always returned.
                    inflater.expand(parser.currentToken() == JsonToken.VALUE_NULL
                            ? null : parser.getText(), gen);
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
    public void streamFragment(JsonParser parser, JsonGenerator gen, int targetIdx, String fragmentId)
            throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) { gen.writeNull(); return; }
        boolean found = false;
        CallTreeInflater inflater = CallTreeInflater.empty();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String name = parser.currentName();
            parser.nextToken();
            if ("endpoints".equals(name)) { found = true; break; }
            if (CallTreeFlattener.NODES_FIELD.equals(name)) {
                inflater = CallTreeInflater.read(parser, objectMapper);
                continue;
            }
            if (parser.currentToken() == JsonToken.START_ARRAY
                    || parser.currentToken() == JsonToken.START_OBJECT)
                parser.skipChildren();
        }
        if (!found || parser.currentToken() != JsonToken.START_ARRAY) { gen.writeNull(); return; }
        int current = 0;
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.END_ARRAY) { gen.writeNull(); return; }
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;
            if (current < targetIdx) { parser.skipChildren(); current++; continue; }
            // Positioned on the target endpoint object — find its "fragments" field, then the
            // specific fragmentId entry within it.
            while (parser.nextToken() != null) {
                var tok = parser.currentToken();
                if (tok == JsonToken.END_OBJECT) { gen.writeNull(); return; }
                if (tok == JsonToken.FIELD_NAME
                        && CallTreeFlattener.FRAGMENT_IDS_FIELD.equals(parser.currentName())) {
                    parser.nextToken(); // START_OBJECT (map of fragmentId -> [nodeId, ...])
                    if (parser.currentToken() != JsonToken.START_OBJECT) { gen.writeNull(); return; }
                    JsonNode fragMap = objectMapper.readTree(parser);
                    JsonNode ids = fragMap.get(fragmentId);
                    if (ids == null) gen.writeNull(); else inflater.expandAll(ids, gen);
                    return;
                }
                if (tok == JsonToken.FIELD_NAME && "fragments".equals(parser.currentName())) {
                    parser.nextToken(); // START_OBJECT (map) or VALUE_NULL
                    if (parser.currentToken() != JsonToken.START_OBJECT) { gen.writeNull(); return; }
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.currentToken() != JsonToken.FIELD_NAME) continue;
                        if (fragmentId.equals(parser.currentName())) {
                            parser.nextToken();
                            copyValue(parser, gen);
                            return;
                        }
                        parser.nextToken();
                        if (parser.currentToken() == JsonToken.START_ARRAY
                                || parser.currentToken() == JsonToken.START_OBJECT)
                            parser.skipChildren();
                    }
                    gen.writeNull();
                    return;
                }
                if (tok == JsonToken.FIELD_NAME) {
                    parser.nextToken();
                    if (parser.currentToken() == JsonToken.START_OBJECT
                            || parser.currentToken() == JsonToken.START_ARRAY)
                        parser.skipChildren();
                }
            }
            gen.writeNull();
            return;
        }
        gen.writeNull();
    }

    @Override
    public void streamClassTree(JsonParser parser, JsonGenerator gen) throws IOException {
        streamClassTree(parser, gen, true);
    }

    @Override
    public void streamClassTree(JsonParser parser, JsonGenerator gen, boolean includeMembers)
            throws IOException {
        // Captures the invocation pool on the way to `classes`. This response does not emit
        // invocations, but it READS them to set hasOutboundRestCall — so skipping the pool would
        // leave that flag false for every method, with nothing failing.
        InvocationPool.Inflater invPool = InvocationPool.Inflater.seekClasses(parser, objectMapper);
        if (parser.currentToken() != JsonToken.START_ARRAY) { gen.writeStartArray(); gen.writeEndArray(); return; }

        Set<String> classKeep = Set.of("fullyQualifiedName", "packageName", "simpleName", "stereotype",
                "sourceJar", "superClass", "isInterface", "isAbstract", "isEnum", "accessFlags",
                "interfaces", "annotations", "repositoryEntityType");
        Set<String> classSkip = Set.of("sourceCode", "entityFieldMappings");
        Set<String> methodSkip = Set.of("fieldAccesses", "localVariables", "descriptor", "opcode");
        Set<String> methodKeep = Set.of("name", "returnType", "annotations", "parameters",
                "startLine", "endLine", "accessFlags", "httpMethod", "path", "stringLiterals");

        // HTTP client class prefixes — mirrors domain-config.json "http_client_classes"
        // Used to detect methods that make outbound REST calls via their invocations list.
        List<String> httpClientPrefixes = List.of(
                "org.springframework.web.client.RestTemplate",
                "org.springframework.web.reactive.function.client.WebClient",
                "org.springframework.cloud.openfeign",
                "feign.Client",
                "feign.",
                "org.apache.http.client.HttpClient",
                "java.net.http.HttpClient"
        );

        gen.writeStartArray();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;

            // Buffer the whole class object so we can write all fields after reading.
            com.fasterxml.jackson.databind.node.ObjectNode classNode =
                    (com.fasterxml.jackson.databind.node.ObjectNode) objectMapper.readTree(parser);
            invPool.inflateClass(classNode, objectMapper);

            // Reconstruct from buffered node using the original field-filtering logic
            gen.writeStartObject();
            int methodCount = 0;
            boolean wroteMethodSummaries = false;
            boolean wroteFields = false;

            // Iterate fields from the buffered ObjectNode
            java.util.Iterator<java.util.Map.Entry<String, com.fasterxml.jackson.databind.JsonNode>> nodeFields
                    = classNode.fields();
            while (nodeFields.hasNext()) {
                java.util.Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> entry = nodeFields.next();
                String fname = entry.getKey();
                com.fasterxml.jackson.databind.JsonNode fval = entry.getValue();

                if (classKeep.contains(fname)) {
                    gen.writeFieldName(fname);
                    objectMapper.writeValue(gen, fval);
                } else if ("fields".equals(fname)) {
                    int fieldCount = fval.isArray() ? fval.size() : 0;
                    // The count is emitted either way: a lean response must never turn into a
                    // wrong number on the row. Only the array itself is skipped.
                    gen.writeNumberField("fieldCount", fieldCount);
                    if (!includeMembers) { wroteFields = true; continue; }
                    gen.writeFieldName("fields");
                    gen.writeStartArray();
                    if (fval.isArray()) {
                        for (var f : fval) {
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
                    }
                    gen.writeEndArray();
                    wroteFields = true;
                } else if ("methods".equals(fname)) {
                    // In lean mode the array is skipped but still counted, so the row keeps its
                    // "N methods" figure. Members arrive from /classes/by-index/{idx} on expand.
                    if (!includeMembers) {
                        gen.writeNumberField("methodCount", fval.isArray() ? fval.size() : 0);
                        wroteMethodSummaries = true;
                        continue;
                    }
                    gen.writeFieldName("methodSummaries");
                    gen.writeStartArray();
                    if (fval.isArray()) {
                        for (var methodNode : fval) {
                            methodCount++;
                            gen.writeStartObject();

                            // Detect outbound REST calls: scan invocations for HTTP client classes.
                            // The invocations list is in analysis.json but normally skipped for the tree.
                            // We read it here only to set hasOutboundRestCall, then discard it.
                            boolean hasOutboundRestCall = false;
                            JsonNode invocations = methodNode.get("invocations");
                            if (invocations != null && invocations.isArray()) {
                                for (JsonNode inv : invocations) {
                                    String owner = textOf(inv, "ownerClass");
                                    if (owner != null) {
                                        for (String prefix : httpClientPrefixes) {
                                            if (owner.startsWith(prefix)) {
                                                hasOutboundRestCall = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (hasOutboundRestCall) break;
                                }
                            }

                            var mFields = methodNode.fields();
                            while (mFields.hasNext()) {
                                var me = mFields.next();
                                String mf = me.getKey();
                                JsonNode mv = me.getValue();
                                if (!methodSkip.contains(mf) && methodKeep.contains(mf)) {
                                    gen.writeFieldName(mf);
                                    objectMapper.writeValue(gen, mv);
                                }
                                // invocations: skip (not in methodKeep, used above only)
                            }
                            if (hasOutboundRestCall) gen.writeBooleanField("hasOutboundRestCall", true);
                            gen.writeEndObject();
                        }
                    }
                    gen.writeEndArray();
                    gen.writeNumberField("methodCount", methodCount);
                    wroteMethodSummaries = true;
                } else if (classSkip.contains(fname)) {
                    // skip — already buffered, just don't write
                } else {
                    // unknown field — skip
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
        InvocationPool.Inflater invPool = InvocationPool.Inflater.seekClasses(parser, objectMapper);
        if (parser.currentToken() != JsonToken.START_ARRAY) { gen.writeNull(); return; }
        int current = 0;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() != JsonToken.START_OBJECT) continue;
            if (current < targetIdx) {
                parser.skipChildren();
                current++;
                continue;
            }
            // Buffered rather than token-copied: a raw copy would emit the pooled
            // `invocationRefs` ordinals to the client instead of `invocations`. One class at a
            // time, so this does not reintroduce whole-document buffering.
            if (invPool.isEmpty()) {
                copyValue(parser, gen);
            } else {
                objectMapper.writeTree(gen,
                        invPool.inflateClass(objectMapper.readTree(parser), objectMapper));
            }
            return;
        }
        gen.writeNull();
    }

    /* ═══════════════ Offset-seek serving (plan 2a adoption) ═══════════════ */

    /**
     * The offsets sidecar that indexes this provider's document, or {@code null} when the document
     * has none.
     *
     * <p>{@code analysis.json} and {@code analysis_corrected.json} each get their own sidecar,
     * written in the same pass as the document (plan 2a). The {@code previous} correction twin has
     * no sidecar yet, so it answers {@code null} and keeps the legacy scan.
     */
    protected com.analyzer.storage.ArtifactKey offsetsKey() {
        if (documentKey == null) return null;
        return switch (documentKey.leaf()) {
            case "analysis.json" -> jarDataPaths.analysisOffsetsKey(jarName);
            case "analysis_corrected.json" -> jarDataPaths.correctedOffsetsKey(jarName);
            default -> null;
        };
    }

    /**
     * One class by ordinal, read from the offsets sidecar instead of a full-document scan.
     *
     * <h2>What the slice alone is not enough for</h2>
     *
     * <p>{@code analysis.json} pools each distinct {@code MethodCallInfo} once (see {@link
     * InvocationPool}); a class whose methods make calls stores {@code invocationRefs} ordinals, not
     * the calls. Serving that class byte-for-byte therefore needs the document-level {@code
     * invocationPool} as well as the class slice. The pool is a known fixed-position section — the
     * writer emits it immediately before {@code classes} — so it is read from the document prefix
     * bounded by the first class's recorded offset, never from a scan of the arrays themselves. A
     * class that carries no {@code invocationRefs} needs no pool at all and is served from its slice
     * alone (the legacy token copy).
     *
     * <h2>Reads happen before any write</h2>
     *
     * <p>Every failure — absent sidecar, out-of-range offset, truncated slice, unreadable pool —
     * returns {@code false} <b>before a single byte reaches the generator</b>, so the caller can
     * replay the legacy scan onto the same output. Correctness over speed: an offset that cannot be
     * trusted degrades to the old path rather than risk a wrong record.
     */
    @Override
    public boolean streamClassByIndexSeek(JsonGenerator gen, int idx) throws IOException {
        com.analyzer.storage.ArtifactKey offsetsKey = offsetsKey();
        if (offsetsKey == null) return false;
        com.analyzer.storage.ArtifactStore store = jarDataPaths.store();

        AnalysisOffsetIndexReader.Segment seg;
        JsonNode cls = null;
        byte[] slice = null;
        InvocationPool.Inflater pool = null;
        try {
            seg = new AnalysisOffsetIndexReader().loadSegment(
                    store, offsetsKey, AnalysisOffsets.SEGMENT_CLASSES);
            if (seg == null) return false;
            if (seg.has(idx)) {
                slice = readBytes(store, documentKey(), seg.offsets()[idx], seg.lengths()[idx]);
                cls = objectMapper.readTree(slice);
                if (cls != null && hasInvocationRefs(cls)) {
                    pool = readInvocationPool(store, seg.offsets()[0]);
                }
            }
        } catch (IOException | RuntimeException e) {
            return false;   // stale/corrupt index -> legacy scan; nothing written yet
        }

        // All reads succeeded; only now touch the output.
        if (cls == null) {
            gen.writeNull();
            return true;
        }
        if (pool != null && !pool.isEmpty()) {
            // Identical to the legacy pooled path: inflate, then serialise the class node.
            objectMapper.writeTree(gen, pool.inflateClass(cls, objectMapper));
        } else {
            // No pooled references: reproduce the legacy no-pool token copy exactly.
            try (JsonParser p = objectMapper.getFactory().createParser(slice)) {
                p.nextToken();   // START_OBJECT
                copyValue(p, gen);
            }
        }
        return true;
    }

    /** True when any method of the class carries pooled {@code invocationRefs}. */
    private static boolean hasInvocationRefs(JsonNode cls) {
        JsonNode methods = cls.get("methods");
        if (methods == null || !methods.isArray()) return false;
        for (JsonNode method : methods) {
            if (method.has(InvocationPool.REFS_FIELD)) return true;
        }
        return false;
    }

    /**
     * Reads the document-level {@code invocationPool} from the prefix ending at the first class.
     *
     * <p>{@code prefixEnd} is {@code classes.offsets[0]}, so the stream is cut immediately after
     * {@code "classes":[} — the loop stops on that field name rather than running off the truncated
     * end. Nodes, the endpoint pool and advice (which precede the invocation pool) are skipped
     * without materialising them.
     */
    private InvocationPool.Inflater readInvocationPool(com.analyzer.storage.ArtifactStore store,
                                                       long prefixEnd) throws IOException {
        try (java.io.InputStream in = store.read(documentKey(), 0, prefixEnd);
             JsonParser p = objectMapper.getFactory().createParser(in)) {
            if (p.nextToken() != JsonToken.START_OBJECT) return InvocationPool.Inflater.passthrough();
            while (p.nextToken() != null) {
                if (p.currentToken() == JsonToken.END_OBJECT) break;
                if (p.currentToken() != JsonToken.FIELD_NAME) continue;
                String name = p.currentName();
                if ("classes".equals(name)) break;      // reached the arrays; no pool field seen
                p.nextToken();                          // value token
                if (InvocationPool.POOL_FIELD.equals(name)) {
                    return InvocationPool.Inflater.of(objectMapper.readTree(p));
                }
                if (p.currentToken() == JsonToken.START_OBJECT
                        || p.currentToken() == JsonToken.START_ARRAY) {
                    p.skipChildren();
                }
            }
        }
        return InvocationPool.Inflater.passthrough();
    }

    private static byte[] readBytes(com.analyzer.storage.ArtifactStore store,
                                    com.analyzer.storage.ArtifactKey key, long offset, long length)
            throws IOException {
        try (java.io.InputStream in = store.read(key, offset, length)) {
            return in.readAllBytes();
        }
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

        // Flat-format node dictionary, picked up before "endpoints" (where the writer puts it).
        // Never echoed into the summary — the summary has never contained call-tree nodes.
        CallTreeInflater inflater = CallTreeInflater.empty();

        // Pooled endpoint arrays, likewise written before "endpoints" so this one forward pass can
        // pick them up. Passthrough for a document written before pooling existed.
        EndpointArrayPool.Inflater epPool = EndpointArrayPool.Inflater.passthrough();

        parser.nextToken(); // START_OBJECT
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String fieldName = parser.currentName();

            if (CallTreeFlattener.NODES_FIELD.equals(fieldName)) {
                parser.nextToken();
                inflater = CallTreeInflater.read(parser, objectMapper);

            } else if (CallTreeFlattener.FORMAT_FIELD.equals(fieldName)) {
                parser.nextToken(); // internal marker, not part of the summary contract

            } else if (InvocationPool.POOL_FIELD.equals(fieldName)) {
                // This response needs no invocations, but the pool MUST be skipped as a unit.
                // The loop above breaks on any END_OBJECT, so walking into the array would end
                // the whole pass at the first pooled invocation's closing brace and return a
                // truncated summary — with no error.
                parser.nextToken();
                parser.skipChildren();

            } else if (EndpointArrayPool.POOL_FIELD.equals(fieldName)) {
                // Written before "endpoints" precisely so this single forward pass can pick it up.
                parser.nextToken();
                epPool = EndpointArrayPool.Inflater.of(objectMapper.readTree(parser));

            } else if ("classes".equals(fieldName)) {
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
                    entry.put("jarName", textOf(classNode, "jarName"));
                    entry.put("sourcePath", textOf(classNode, "sourcePath"));
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

                    // ANNOTATED methods only, and only their name and annotations.
                    //
                    // Three consumers — callGraph.js:184, summary-analyzer.js:249/291 and
                    // summary-scheduled.js:34 — iterate `cls.methods` on this payload looking for
                    // @Scheduled, @Document and mapping annotations. The field was never emitted,
                    // so all three loops have always been no-ops: a silently missing feature, not
                    // an error.
                    //
                    // Carrying every method would bloat a payload the browser fetches on every
                    // analysis open. It is not needed: on unified-web only 343 of 33,250 methods
                    // (1.0%) carry an annotation, and annotations are the only thing these three
                    // read — so the whole subset costs 0.04 MB. Attributes are kept because that is
                    // what they inspect (a cron expression, a collection name).
                    //
                    // Omitted entirely when a class has no annotated methods, which is 99% of them,
                    // so the rest of the summary is unchanged.
                    List<Map<String, Object>> annotatedMethods = new ArrayList<>();
                    for (var m : classNode.path("methods")) {
                        var mAnns = m.path("annotations");
                        if (!mAnns.isArray() || mAnns.isEmpty()) continue;
                        List<Map<String, Object>> methodAnnList = new ArrayList<>();
                        for (var a : mAnns) {
                            Map<String, Object> one = new LinkedHashMap<>();
                            one.put("name", textOf(a, "name"));
                            if (a.has("attributes")) {
                                one.put("attributes",
                                        objectMapper.convertValue(a.get("attributes"), Map.class));
                            }
                            methodAnnList.add(one);
                        }
                        Map<String, Object> mEntry = new LinkedHashMap<>();
                        mEntry.put("name", textOf(m, "name"));
                        mEntry.put("annotations", methodAnnList);
                        annotatedMethods.add(mEntry);
                    }
                    if (!annotatedMethods.isEmpty()) entry.put("methods", annotatedMethods);

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
                                        job.put("jarName", textOf(classNode, "jarName"));
                                        job.put("sourcePath", textOf(classNode, "sourcePath"));
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
                        if ("sourceCode".equals(fk) || "fragments".equals(fk)
                                || CallTreeFlattener.FRAGMENT_IDS_FIELD.equals(fk)) {
                            parser.skipChildren();
                        } else if (CallTreeFlattener.ROOT_ID_FIELD.equals(fk)) {
                            // Flat format. Same contract as "callTree": the summary never emits
                            // the tree, it only needs to know one exists so the aggregate
                            // fallback below can fire for documents that lack pre-computed
                            // aggregates. Expanding is deferred to that branch and normally
                            // never happens.
                            hasCallTreeToken = parser.currentToken() != JsonToken.VALUE_NULL;
                            if (hasCallTreeToken && !hasPrecomputedAggregates) {
                                callTreeNode = inflater.toTree(parser.getText(), objectMapper);
                            }
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
                            // THE single inflation point for pooled endpoint arrays. The summary
                            // cache is what the slice routes and the collections/domains views
                            // read, so inflating here makes every one of them correct without any
                            // of them knowing that pooling exists.
                            //
                            // The field is restored under its ORIGINAL name, not the *Refs name it
                            // was read under. That is not cosmetic: HEADERS_SKIP_FIELDS drops
                            // externalCalls/beans/dynamicFlows from /summary/headers by name, and
                            // storing them as "externalCallsRefs" meant the skip never matched and
                            // the inflated arrays were emitted — /summary/headers grew 6.3x
                            // (928,083 -> 5,892,425 bytes).
                            String original = EndpointArrayPool.Inflater.originalFieldFor(fk);
                            epSmallFields.put(original != null ? original : fk,
                                    epPool.inflateField(fk, objectMapper.readTree(parser), objectMapper));
                        }
                    }

                    boolean hasCallTree = hasCallTreeToken;
                    boolean needsAggregates = !epSmallFields.containsKey("externalCalls") && hasCallTree;
                    // Deliberately does NOT include "collectionLifecycleFlows" here — that
                    // feature requires CollectionLifecycleDetector.aggregate()'s ancestor-
                    // tracking tree walk (Spring-injected, IdentityHashMap-keyed grouping),
                    // which is not replicated over streamed JsonNodes for v1 (would duplicate
                    // ~150 lines of logic with guaranteed drift risk, same tradeoff already
                    // accepted for aggregationFlows/dynamicFlows's dual implementation). Old
                    // cached files missing this field simply won't get it until their next full
                    // re-analysis via the in-memory EndpointInfo.computeAggregates() path.
                    boolean needsDynamicFlows = (!epSmallFields.containsKey("dynamicFlows")
                            || !epSmallFields.containsKey("aggregationFlows")
                            || !epSmallFields.containsKey("dynamicCollectionFlows")) && hasCallTree;
                    boolean needsVerification = shouldComputeVerification()
                            && !epSmallFields.containsKey("aggregatedCollectionVerification") && hasCallTree;
                    Map<String, Object> computed = null;
                    if ((needsAggregates || needsDynamicFlows) && callTreeNode != null) {
                        computed = computeAggregatesFromJson(callTreeNode);
                        if (!needsAggregates) {
                            computed.keySet().retainAll(Set.of("dynamicFlows", "aggregationFlows", "dynamicCollectionFlows"));
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
                            // The truncation used to be SILENT, so anything counting the array got
                            // the cap instead of the truth: 24 endpoints reported 100 external
                            // calls where the analysis found 224, and the summary's total came to
                            // 3,104 against a stored 5,958. The array still has to be capped (an
                            // uncapped summary measured 650 MB), but the real size is now stated so
                            // a reader can say "100 of 224" instead of quietly saying "100".
                            // Written only when truncation actually happened, so untruncated
                            // responses are byte-for-byte unchanged.
                            gen.writeNumberField(key + "Total", val.size());
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
                                gen.writeNumberField(ce.getKey() + "Total", list.size());
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
        List<Map<String, Object>> dynCollFlows = new ArrayList<>();
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
                dynFlows, aggFlows, dynCollFlows, collSet, collOps, collDomains, collVerification, collSources);

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
        if (!dynCollFlows.isEmpty()) result.put("dynamicCollectionFlows", dynCollFlows);
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
            List<Map<String, Object>> dynCollFlows,
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

        // Use the node's full operationTypes array (not just the single primary operationType
        // field) when picking the best op to attribute to each collection this node carries —
        // see EndpointInfo.walkForAggregation's matching fix for why: the primary field can be
        // null on a pure orchestrator whose own direct call didn't resolve a collection, while
        // the real DB verb for a collection it carries lives in a child's operationType, already
        // unioned into this node's own operationTypes by CallTreeBuilder.
        String bestNodeOpType = opType;
        var opTypesArr = node.path("operationTypes");
        if (opTypesArr.isArray()) {
            for (var opNode : opTypesArr) {
                String op = opNode.asText(null);
                if (op != null && opPri(op) > opPri(bestNodeOpType)) bestNodeOpType = op;
            }
        }

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
                    String existingOp = collOps.get(collName);
                    if (bestNodeOpType != null && opPri(bestNodeOpType) > opPri(existingOp)) {
                        collOps.put(collName, bestNodeOpType);
                    }
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
                hc.put("sourceJar", safe(textOf(node, "sourceJar")));
                hc.put("jarName", safe(textOf(node, "jarName")));
                hc.put("sourcePath", safe(textOf(node, "sourcePath")));
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
                ec.put("jarName", safe(textOf(node, "jarName")));
                ec.put("sourcePath", safe(textOf(node, "sourcePath")));
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
                        ec.put("jarName", safe(textOf(node, "jarName")));
                        ec.put("sourcePath", safe(textOf(node, "sourcePath")));
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
            bean.put("jarName", safe(textOf(node, "jarName")));
            bean.put("sourcePath", safe(textOf(node, "sourcePath")));
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
            df.put("sourceJar", safe(textOf(node, "sourceJar")));
            df.put("jarName", safe(textOf(node, "jarName")));
            df.put("sourcePath", safe(textOf(node, "sourcePath")));
            df.put("resolvedFrom", textOf(node, "resolvedFrom"));
            df.put("qualifierHint", textOf(node, "qualifierHint"));
            df.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
            dynFlows.add(df);
        }

        // Real reflective invocations — CallNode.reflectiveCalls, populated by CallTreeBuilder from
        // actual resolved bytecode calls (owner class + method name), NOT a string-literal
        // substring scan. The old scan below both missed real reflection with no matching literal
        // in the same method, and falsely flagged unrelated string literals (e.g. a log message)
        // that merely happened to contain matching text.
        var reflectiveCalls = node.path("reflectiveCalls");
        if (reflectiveCalls.isArray()) {
            for (var rc : reflectiveCalls) {
                String tag = rc.asText("");
                if (tag.isEmpty()) continue;
                Map<String, Object> df = new LinkedHashMap<>();
                df.put("category", "REFLECTION");
                df.put("dispatchType", "REFLECTION");
                df.put("className", safe(textOf(node, "className")));
                df.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                df.put("methodName", safe(textOf(node, "methodName")));
                df.put("sourceJar", safe(textOf(node, "sourceJar")));
                df.put("jarName", safe(textOf(node, "jarName")));
                df.put("sourcePath", safe(textOf(node, "sourcePath")));
                df.put("pattern", tag);
                df.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
                dynFlows.add(df);
            }
        }

        // Real third-party library dependencies — CallNode.thirdPartyDependencies (audit #10):
        // the existing crossModule/httpTarget "External Dependency" facet above only ever covers
        // intra-repo cross-module calls and outbound HTTP; this is the actual third-party-library
        // signal that facet's name implies but never captured, added as its own category rather
        // than folded into DISPATCH so existing consumers of that category are unaffected.
        var thirdPartyDeps = node.path("thirdPartyDependencies");
        if (thirdPartyDeps.isArray()) {
            for (var tpd : thirdPartyDeps) {
                String cls = tpd.asText("");
                if (cls.isEmpty()) continue;
                Map<String, Object> df = new LinkedHashMap<>();
                df.put("category", "THIRD_PARTY_DEPENDENCY");
                df.put("dependencyClass", cls);
                df.put("className", safe(textOf(node, "className")));
                df.put("simpleClassName", safe(textOf(node, "simpleClassName")));
                df.put("methodName", safe(textOf(node, "methodName")));
                df.put("sourceJar", safe(textOf(node, "sourceJar")));
                df.put("jarName", safe(textOf(node, "jarName")));
                df.put("sourcePath", safe(textOf(node, "sourcePath")));
                df.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
                dynFlows.add(df);
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
                        df.put("sourceJar", safe(textOf(node, "sourceJar")));
                        df.put("jarName", safe(textOf(node, "jarName")));
                        df.put("sourcePath", safe(textOf(node, "sourcePath")));
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
                af.put("jarName", safe(textOf(node, "jarName")));
                af.put("sourcePath", safe(textOf(node, "sourcePath")));
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

        // Dynamic collection reference flow — mirrors EndpointInfo.walkForAggregation's
        // in-memory equivalent. Never folded into aggFlows/collSet/collOps.
        var dynCollRefsArr = node.path("dynamicCollectionRefs");
        if (dynCollRefsArr.isArray() && !dynCollRefsArr.isEmpty()) {
            List<Map<String, Object>> refs = new ArrayList<>();
            for (var r : dynCollRefsArr) {
                Map<String, Object> refMap = new LinkedHashMap<>();
                for (String k : List.of("variableName", "varKind", "pattern", "source", "operationType")) {
                    if (r.has(k)) refMap.put(k, r.path(k).asText(""));
                }
                refs.add(refMap);
            }
            Map<String, Object> dcf = new LinkedHashMap<>();
            dcf.put("className", safe(textOf(node, "className")));
            dcf.put("simpleClassName", safe(textOf(node, "simpleClassName")));
            dcf.put("methodName", safe(textOf(node, "methodName")));
            dcf.put("dynamicRefs", refs);
            dcf.put("sourceJar", safe(textOf(node, "sourceJar")));
            dcf.put("jarName", safe(textOf(node, "jarName")));
            dcf.put("sourcePath", safe(textOf(node, "sourcePath")));
            dcf.put("breadcrumb", buildSimpleBreadcrumb(path, 6));
            dynCollFlows.add(dcf);
        }

        var children = node.path("children");
        if (children.isArray()) {
            for (var child : children) {
                walkCallTreeJson(child, controllerJar, visited, path, methodCount, locCount, inScope, extScope, dbOps,
                        moduleSet, opSet, seenBeans, seenExt, seenHttp, extCallList, httpCallList, beanList, procList, allProcNames,
                        dynFlows, aggFlows, dynCollFlows, collSet, collOps, collDomains, collVerification, collSources);
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
            "beans", "externalCalls", "httpCalls", "dynamicFlows", "aggregationFlows",
            "dynamicCollectionFlows", "collectionLifecycleFlows", "parameters"
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

    /**
     * Q21 — the header scalars, and nothing whose size depends on the analysis.
     *
     * <p>PLAN-03 §1.2 I2: rendering the title bar of a JAR cost a 430 KB document (measured
     * {@code _summary_static.json}, and 120 KB even after the {@code /summary/headers} mitigation),
     * because the eleven scalars the bar shows were fields of the same object as the endpoint list
     * and the class index. This copies only the top-level values that are scalars, so the response
     * is a fixed few hundred bytes whatever the JAR contains.
     *
     * <p>The selection rule is "every top-level scalar", not a hardcoded field list, deliberately:
     * a new scalar added to the summary appears here automatically, and — more importantly — a new
     * <em>array</em> cannot accidentally be dragged into the header response by someone adding it
     * to the writer. The size of this response is bounded by construction rather than by review.
     *
     * <p>Streamed: {@code skipChildren()} walks past the endpoint list and the class index without
     * materialising either, so serving the header never allocates the document it exists to avoid.
     */
    @Override
    public void streamSummaryHeader(JsonParser parser, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        parser.nextToken();     // START_OBJECT
        boolean wroteBasePackage = false;
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String fieldName = parser.currentName();
            JsonToken value = parser.nextToken();
            if (value == JsonToken.START_OBJECT || value == JsonToken.START_ARRAY) {
                parser.skipChildren();          // endpoints, classIndex, the collection maps
                continue;
            }
            // "every top-level scalar" is the rule here, but the storage-format marker is an
            // internal detail of the document, not part of the header contract — emitting it
            // would put a new field in a response the UI already consumes.
            if (CallTreeFlattener.FORMAT_FIELD.equals(fieldName)) continue;
            if ("basePackage".equals(fieldName)) wroteBasePackage = true;
            gen.writeFieldName(fieldName);
            copyValue(parser, gen);
        }
        if (!wroteBasePackage) writeBasePackageFromSidecar(gen);
        gen.writeEndObject();
    }

    /**
     * Q21b — one page of the endpoint list, in the same shape {@code /summary/headers} gives it.
     *
     * <p>Items are identical to the endpoint objects in headers mode, {@code endpointId} included,
     * so a screen can move from the whole array to a page without touching its rendering code.
     *
     * @param afterId  resume strictly after the endpoint with this {@code endpointId}; null starts
     *                 at the beginning. A natural key, never an ordinal — the analysis is rewritten
     *                 in place by a Claude correction and ordinals do not survive that.
     * @param limit    maximum endpoints to write, or -1 for all of them (the legacy shape)
     * @param query    case-insensitive substring that must appear in the method, path, controller
     *                 or handler name. Search moves to the server with pagination: once only one
     *                 page is in memory a client-side filter would silently search that page alone.
     * @return the {@code endpointId} of the last endpoint written and whether more matched, so the
     *         caller can mint a cursor without a second pass
     */
    @Override
    public EndpointPageMark streamSummaryEndpointPage(JsonParser parser, JsonGenerator gen,
                                                      String afterId, int limit, String query)
            throws IOException {
        String needle = (query == null || query.isBlank())
                ? null : query.toLowerCase(java.util.Locale.ROOT);
        boolean seeking = afterId != null && !afterId.isBlank();
        boolean past = !seeking;
        int written = 0;
        boolean more = false;
        String lastId = null;
        long matched = 0;

        // This route streams analysis.json directly rather than the summary cache, so it has to
        // resolve the endpoint pool itself. Without this the raw ordinal arrays were emitted:
        // externalCallsRefs/beansRefs/dynamicFlowsRefs appeared in the response and it grew by
        // 51,029 bytes of numbers the UI cannot use.
        EndpointArrayPool.Inflater epPool = EndpointArrayPool.Inflater.passthrough();

        gen.writeStartArray();
        parser.nextToken();     // START_OBJECT
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            if (EndpointArrayPool.POOL_FIELD.equals(parser.currentName())) {
                parser.nextToken();
                epPool = EndpointArrayPool.Inflater.of(objectMapper.readTree(parser));
                continue;
            }
            if (!"endpoints".equals(parser.currentName())) {
                parser.nextToken();
                parser.skipChildren();
                continue;
            }
            parser.nextToken();     // START_ARRAY
            int ordinal = -1;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                // Tracked regardless of match/cursor state: this is the endpoint's position in
                // analysis.endpoints[], the identity /endpoints/by-index/{idx}/call-tree needs.
                // UI-025: the endpoint list used to hold the whole array and read each card's
                // index straight off it (`_origIdx`); paged, the card never sees the array, so the
                // index has to travel with the row instead.
                ordinal++;
                // Read the endpoint as a tree: it is one endpoint, not the document, and the
                // identity has to be computed from four fields that may arrive in any order.
                JsonNode ep = objectMapper.readTree(parser);
                if (ep instanceof com.fasterxml.jackson.databind.node.ObjectNode epObj) {
                    epPool.inflateEndpoint(epObj, objectMapper);
                }
                String id = endpointId(ep);
                boolean isMatch = needle == null || endpointMatches(ep, needle);

                // Counted over the whole array, not just the part after the cursor: `total` has to
                // mean the same thing on page 3 as on page 1, or the pager's "of 40" changes as the
                // user walks the list. It is free here — the pass happens anyway.
                if (isMatch) matched++;

                // Seeking is by identity alone, deliberately independent of the filter. If the skip
                // depended on `q`, a cursor minted under one search and replayed under another
                // would never find its anchor and would answer with an empty page — which the
                // client reads as "end of list".
                if (!past) {
                    if (id.equals(afterId)) past = true;
                    continue;
                }
                if (!isMatch) continue;
                if (limit >= 0 && written >= limit) { more = true; continue; }
                writeEndpointHeader(ep, id, ordinal, gen);
                lastId = id;
                written++;
            }
        }
        gen.writeEndArray();
        // `past` is still false when the cursor's endpoint is not in the document at all — the
        // analysis was rewritten under the reader. That is a 409, not an empty page (PLAN-08 §3.4).
        return new EndpointPageMark(lastId, more, matched, !seeking || past);
    }

    /**
     * What {@link #streamSummaryEndpointPage} discovered while streaming.
     *
     * @param lastId  {@code endpointId} of the last item written — the cursor's tiebreaker
     * @param hasMore whether at least one further endpoint matched
     * @param matched total endpoints matching the filter, over the whole array rather than the
     *                part after the cursor. Exact and cheap here because the pass over the endpoint
     *                array happens anyway; the count is a side effect of it, not a second query.
     * @param cursorResolved false when a cursor was supplied and its endpoint no longer exists —
     *                the analysis was rewritten mid-scroll. The caller must answer 409, not an
     *                empty page: PLAN-08 §3.4 is explicit that this is the case offset paging would
     *                have silently corrupted, and a client cannot tell an empty page from the end
     *                of the list.
     */
    public record EndpointPageMark(String lastId, boolean hasMore, long matched,
                                   boolean cursorResolved) {}

    /**
     * One page of the endpoint list, served from the {@code endpoints[]} offsets rather than a full
     * parse of the document per page.
     *
     * <h2>Byte-identity with the legacy scan</h2>
     *
     * <p>The output is the same items, in the same order, with the same {@code endpointId} and
     * {@code endpointIndex}, and the same mark. The only structural difference is that the document
     * pools some heavy endpoint arrays ({@link EndpointArrayPool}); the legacy scan inflates a
     * {@code externalCallsRefs} back to {@code externalCalls} and then drops it as a header-skipped
     * field. Skipping the refs field directly is the same omission in the same position, so no byte
     * of the response depends on the pool's contents — the page never reads it.
     *
     * <h2>Which elements are read</h2>
     *
     * <ul>
     *   <li><b>Unfiltered first page</b> ({@code afterId} absent, no {@code q}): only the page's own
     *       elements are sought, and the exact total is the sidecar's {@code rowCount} — no element
     *       beyond the page is touched.</li>
     *   <li><b>Filtered or resumed</b>: the total counts matches over the whole array and a cursor
     *       is resolved by identity, so every endpoint must be seen. They are read as one ranged
     *       read of the contiguous {@code endpoints[]} region — never the classes array, the pools
     *       or the call-tree dictionary.</li>
     * </ul>
     *
     * <p>Returns {@code null} — possibly after writing nothing, so a buffering caller can discard
     * and replay the legacy path — when the sidecar is absent or unreadable.
     */
    @Override
    public EndpointPageMark streamSummaryEndpointPageSeek(JsonGenerator gen, String afterId,
                                                          int limit, String query) throws IOException {
        com.analyzer.storage.ArtifactKey offsetsKey = offsetsKey();
        if (offsetsKey == null) return null;
        com.analyzer.storage.ArtifactStore store = jarDataPaths.store();
        AnalysisOffsetIndexReader.Segment seg;
        try {
            seg = new AnalysisOffsetIndexReader().loadSegment(
                    store, offsetsKey, AnalysisOffsets.SEGMENT_ENDPOINTS);
        } catch (IOException | RuntimeException e) {
            return null;
        }
        if (seg == null) return null;

        String needle = (query == null || query.isBlank())
                ? null : query.toLowerCase(java.util.Locale.ROOT);
        boolean seeking = afterId != null && !afterId.isBlank();

        if (!seeking && needle == null && limit >= 0) {
            return seekEndpointPageFast(gen, store, seg, limit);
        }
        return scanEndpointPageSeek(gen, store, seg, afterId, seeking, limit, needle);
    }

    /**
     * The unfiltered first page: seek exactly the page's elements; the total is the row count.
     *
     * <p>{@code hasMore} is "there is an element after the last one written", which with no filter
     * and no cursor is exactly {@code rowCount > written} — the same condition the legacy scan
     * discovers by continuing past the limit.
     */
    private EndpointPageMark seekEndpointPageFast(JsonGenerator gen,
                                                  com.analyzer.storage.ArtifactStore store,
                                                  AnalysisOffsetIndexReader.Segment seg, int limit)
            throws IOException {
        gen.writeStartArray();
        int written = 0;
        String lastId = null;
        AnalysisOffsetIndexReader reader = new AnalysisOffsetIndexReader();
        long end = Math.min((long) seg.rowCount(), (long) limit);
        for (int i = 0; i < end; i++) {
            JsonNode ep = reader.readElement(store, documentKey(), seg, i);
            if (ep == null) break;   // cannot happen while the offsets are valid
            String id = endpointId(ep);
            writeEndpointHeaderSkippingPoolRefs(ep, id, i, gen);
            lastId = id;
            written++;
        }
        gen.writeEndArray();
        return new EndpointPageMark(lastId, seg.rowCount() > written, seg.rowCount(), true);
    }

    /**
     * The filtered/resumed page: the whole {@code endpoints[]} region in one ranged read, streamed
     * through the same per-endpoint logic the legacy document scan uses. The region is the array's
     * contiguous bytes (elements and their separating commas), wrapped in synthetic {@code [} and
     * {@code ]} bytes so a forward-only parser can walk it as one array without the region having
     * to carry the document's own brackets.
     */
    private EndpointPageMark scanEndpointPageSeek(JsonGenerator gen,
                                                  com.analyzer.storage.ArtifactStore store,
                                                  AnalysisOffsetIndexReader.Segment seg,
                                                  String afterId, boolean seeking, int limit,
                                                  String needle) throws IOException {
        int n = seg.rowCount();
        long start = seg.offsets()[0];
        long end = seg.offsets()[n - 1] + seg.lengths()[n - 1];
        boolean past = !seeking;
        int written = 0;
        boolean more = false;
        String lastId = null;
        long matched = 0;

        gen.writeStartArray();
        try (java.io.InputStream body = store.read(documentKey(), start, end - start);
             java.io.InputStream in = new java.io.SequenceInputStream(
                     new java.io.ByteArrayInputStream(new byte[]{'['}),
                     new java.io.SequenceInputStream(body,
                             new java.io.ByteArrayInputStream(new byte[]{']'})));
             JsonParser p = objectMapper.getFactory().createParser(in)) {
            p.nextToken();   // synthetic START_ARRAY
            int ordinal = -1;
            while (p.nextToken() != null) {
                if (p.currentToken() == JsonToken.END_ARRAY) break;
                if (p.currentToken() != JsonToken.START_OBJECT) continue;
                ordinal++;
                JsonNode ep = objectMapper.readTree(p);
                String id = endpointId(ep);
                boolean isMatch = needle == null || endpointMatches(ep, needle);
                if (isMatch) matched++;
                if (!past) {
                    if (id.equals(afterId)) past = true;
                    continue;
                }
                if (!isMatch) continue;
                if (limit >= 0 && written >= limit) { more = true; continue; }
                writeEndpointHeaderSkippingPoolRefs(ep, id, ordinal, gen);
                lastId = id;
                written++;
            }
        }
        gen.writeEndArray();
        return new EndpointPageMark(lastId, more, matched, !seeking || past);
    }

    /**
     * {@link #writeEndpointHeader} plus the pooled-refs omission: a {@code *Refs} key is skipped
     * where the legacy scan would have inflated it to its original name (which is itself a skipped
     * header field). Same bytes out; the pool is never read.
     */
    private void writeEndpointHeaderSkippingPoolRefs(JsonNode ep, String id, int ordinal,
                                                     JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("endpointId", id);
        gen.writeNumberField("endpointIndex", ordinal);
        var it = ep.fields();
        while (it.hasNext()) {
            var e = it.next();
            if (HEADERS_SKIP_FIELDS.contains(e.getKey())) continue;
            if (EndpointArrayPool.Inflater.originalFieldFor(e.getKey()) != null) continue;
            gen.writeFieldName(e.getKey());
            objectMapper.writeValue(gen, e.getValue());
        }
        gen.writeEndObject();
    }

    /** The four columns the endpoint list is searched by in the browser today. */
    private static boolean endpointMatches(JsonNode ep, String lowerNeedle) {
        return contains(ep, "fullPath", lowerNeedle)
                || contains(ep, "httpMethod", lowerNeedle)
                || contains(ep, "controllerSimpleName", lowerNeedle)
                || contains(ep, "controllerClass", lowerNeedle)
                || contains(ep, "methodName", lowerNeedle);
    }

    private static boolean contains(JsonNode ep, String field, String lowerNeedle) {
        JsonNode v = ep.get(field);
        return v != null && v.isTextual()
                && v.asText().toLowerCase(java.util.Locale.ROOT).contains(lowerNeedle);
    }

    /**
     * The stable identity of an endpoint — <b>the positional-coupling fix</b>.
     *
     * <p>PLAN-03 §1.5 #4 recorded this as the highest-severity latent bug in the UI. The Summary
     * tab fetches the endpoint list from one request and each of the four heavy per-endpoint slices
     * ({@code externalCalls}, {@code dynamicFlows}, {@code aggregationFlows}, {@code beans}) from
     * another, then merged the second into the first <b>by array index</b>
     * ({@code summary.js:_mergeSliceData}, {@code eps[item.endpointIdx]}). That is correct only
     * while two independent passes over the analysis enumerate endpoints in exactly the same order
     * — an invariant nothing enforces, nothing tests, and no error surfaces when it breaks: the
     * external calls of endpoint 7 are simply rendered under endpoint 6, and the screen looks
     * entirely plausible.
     *
     * <p>Two things make that invariant fail. A Claude correction rewrites the analysis and can add,
     * drop or reorder endpoints, and the two requests can straddle it (the slice is fetched lazily,
     * on tab switch, minutes after the list). And paging the endpoint list at all breaks it
     * immediately and unconditionally: {@code endpointIdx} is a position in the whole analysis,
     * while {@code eps} would hold one page, so index 40 of the slice would land on row 40 of
     * page 2.
     *
     * <p>The identity is the endpoint's natural key: HTTP method, path, declaring controller and
     * handler method. Not an ordinal — PLAN-00 finding #5 is that a re-analysis renumbers, so any
     * ordinal is only meaningful within the run that produced it. Not a hash either: a readable id
     * makes a mismatch diagnosable in a network log instead of opaque.
     *
     * <p><b>Where it is emitted.</b> On the four slice responses and on {@code /summary/endpoints}
     * (Q21b), which are the two sides of the join. Deliberately <em>not</em> on the pre-existing
     * {@code /summary/headers}: adding a ~120-byte field to each of its endpoints grew a response
     * that is fetched on every JAR click by 8%, and that endpoint has to stay as it is for the
     * conversion to be revertible on its own. While a screen still reads its endpoint list from
     * there, it derives the same identity from the same four fields with
     * {@code JA.utils.endpointKey} — one formula, written twice, with the client copy retired as
     * each screen moves to {@code /summary/endpoints}.
     *
     * <p>All four fields are included because the first two are not guaranteed unique on their own
     * — the same path and verb can be declared by two handlers with different {@code consumes}. In
     * the measured analyses the pair is already unique (84/84 endpoints); the full tuple is what
     * makes that a guarantee rather than an observation.
     */
    protected static String endpointId(JsonNode ep) {
        return text(ep, "httpMethod") + ' ' + text(ep, "fullPath") + ' '
                + text(ep, "controllerClass") + '#' + text(ep, "methodName");
    }

    private static String text(JsonNode ep, String field) {
        JsonNode v = ep == null ? null : ep.get(field);
        return v == null || v.isNull() ? "" : v.asText("");
    }

    /**
     * {@code basePackage} for analyses written before the summary carried it.
     *
     * <p>Extracted so the header endpoint and the headers endpoint cannot drift on it — the badge
     * disappearing on old analyses in one of the two would look like a data problem.
     */
    private void writeBasePackageFromSidecar(JsonGenerator gen) {
        try {
            var sidecar = jarDataPaths.headerKey(jarName);
            var store = jarDataPaths.store();
            if (!store.exists(sidecar)) return;
            try (var in = store.read(sidecar)) {
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> h = objectMapper.readValue(in, java.util.Map.class);
                Object bp = h.get("basePackage");
                if (bp instanceof String s && !s.isBlank()) gen.writeStringField("basePackage", s);
            }
        } catch (Exception ignore) { /* an absent sidecar is normal, not an error */ }
    }

    /**
     * Writes one endpoint in headers mode — heavy fields dropped, {@code endpointId} added, plus
     * {@code endpointIndex} (UI-025): the position in {@code analysis.endpoints[]}, which
     * {@code /endpoints/by-index/{idx}/call-tree} needs and a paged caller has no other way to
     * know once it stops holding the whole array.
     */
    private void writeEndpointHeader(JsonNode ep, String id, int ordinal, JsonGenerator gen)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField("endpointId", id);
        gen.writeNumberField("endpointIndex", ordinal);
        var it = ep.fields();
        while (it.hasNext()) {
            var e = it.next();
            if (HEADERS_SKIP_FIELDS.contains(e.getKey())) continue;
            gen.writeFieldName(e.getKey());
            objectMapper.writeValue(gen, e.getValue());
        }
        gen.writeEndObject();
    }

    // Fields skipped entirely in headers mode (too heavy for initial load)
    private static final Set<String> HEADERS_SKIP_FIELDS = Set.of(
            "callTree", "fragments", "callTreeRootId", "fragmentChildIds",
            "sourceCode", "beans", "externalCalls", "httpCalls",
            "dynamicFlows", "aggregationFlows", "dynamicCollectionFlows", "collectionLifecycleFlows", "parameters"
    );

    private void streamSummaryHeaders(JsonParser parser, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        parser.nextToken(); // START_OBJECT
        boolean wroteBasePackage = false;
        while (parser.nextToken() != null) {
            var token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String fieldName = parser.currentName();
            if ("basePackage".equals(fieldName)) wroteBasePackage = true;

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
        // Inject basePackage from sidecar for analyses written before this field was added
        if (!wroteBasePackage) writeBasePackageFromSidecar(gen);
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

            if (InvocationPool.POOL_FIELD.equals(fieldName)) {
                // The summary cache should not carry the pool, but this loop breaks on any
                // END_OBJECT, so if one ever reaches it the slice would silently return a
                // fraction of the endpoints. Skipping it as a unit costs nothing.
                parser.nextToken();
                parser.skipChildren();
                continue;
            }

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
                        // The stable identity the client merges on. `endpointIdx` stays beside it
                        // so an unconverted caller is unaffected, but it is no longer what the join
                        // is made of: see endpointId(JsonNode) for what merging by position cost.
                        gen.writeStringField("endpointId", endpointId(epNode));
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
        // Second independent read of the document: this pre-scan runs to completion (and its
        // parser is closed) before streamSummary opens its own stream, so the two never share a
        // position. Streaming throughout — it skipChildren()s past the multi-GB "classes" array.
        try (var in = openStream();
             var scanner = objectMapper.getFactory().createParser(in)) {
            scanner.nextToken(); // consume START_OBJECT (top-level)

            // Scan top-level fields, skipping each value until we reach "endpoints"
            while (scanner.nextToken() != null) {
                if (scanner.currentToken() != JsonToken.FIELD_NAME) continue;
                String topField = scanner.currentName();
                scanner.nextToken(); // advance to the value of this top-level field
                // A pooled document is still "new format": its endpoints carry the aggregates as
                // ordinals under the same keys. Without this, the check below never sees a literal
                // array of externalCalls OBJECTS, returns false, and the summary silently falls
                // back to recomputing aggregates from the call tree — no error, but different
                // collections and domains in the UI.
                if (EndpointArrayPool.POOL_FIELD.equals(topField)) return true;
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

    /** Operation-type priority order, mirrors EndpointInfo.opPri: WRITE > UPDATE > DELETE > READ > AGGREGATE > COUNT. */
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

    protected static String textOf(JsonNode node, String field) {
        var v = node.path(field);
        return v.isNull() || v.isMissingNode() ? null : v.asText();
    }

    protected static void writeStringField(JsonGenerator gen, String name, String value) throws IOException {
        if (value != null) gen.writeStringField(name, value);
        else gen.writeNullField(name);
    }
}
