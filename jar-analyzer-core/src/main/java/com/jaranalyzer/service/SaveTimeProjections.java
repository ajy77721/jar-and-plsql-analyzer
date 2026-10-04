package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the summary and class-tree sidecars <b>during</b> the streaming document write, so the
 * save never re-reads the document it just wrote.
 *
 * <h2>Why this exists</h2>
 * {@code writeAnalysisStreaming} used to write {@code analysis.json}, then call
 * {@code preGenerateSummaryCache}/{@code preGenerateClassTreeCache}, each of which opened the
 * freshly written document again and streamed it. On a 33 MB analysis that is two extra full reads
 * (measured ~34s of the save) to produce sidecars whose inputs had <i>just been in memory</i>. The
 * document is written from a classes JSONL and an in-memory endpoint list; both projections are
 * derivable from exactly those elements as they stream past. This class is the accumulator that
 * does it in the same pass.
 *
 * <h2>Byte-identical by construction</h2>
 * It does not re-implement the projections. It calls the same shared methods the read-time builders
 * use ({@link AbstractAnalysisDataProvider#summarizeClass},
 * {@link AbstractAnalysisDataProvider#writeSummaryEndpoint},
 * {@link AbstractAnalysisDataProvider#writeClassTreeRow}) with the same element each builder would
 * have read back from the document:
 * <ul>
 *   <li>classes: the raw JSONL line, which carries {@code invocations} inline — the one field
 *       pooling rewrites, and the only field the class-tree row reads but does not emit. Pooling
 *       is a storage representation only, so the projection is identical.</li>
 *   <li>endpoints: the un-pooled {@code EndpointInfo} node, whose arrays are already inline — the
 *       same node the document writer pools and the reader inflates back.</li>
 * </ul>
 *
 * <h2>Correctness first</h2>
 * Every projection step is isolated: a failure marks that sidecar failed and disables it, but never
 * fails the document save. {@link PersistenceService} then falls back to the lazy re-read path for
 * the failed sidecar only. Output is written to staging keys and moved into place only after the
 * document has committed, so a failed or partial projection can never be served.
 */
final class SaveTimeProjections {

    private static final Logger log = LoggerFactory.getLogger(SaveTimeProjections.class);

    /** Suffix for every staged artifact; moved away in {@link #finish()}, deleted in {@link #abort()}. */
    static final String STAGE = ".savepartial";

    /** HTTP client class prefixes — mirrors domain-config.json "http_client_classes". */
    private static final List<String> HTTP_CLIENT_PREFIXES = List.of(
            "org.springframework.web.client.RestTemplate",
            "org.springframework.web.reactive.function.client.WebClient",
            "org.springframework.cloud.openfeign",
            "feign.Client",
            "feign.",
            "org.apache.http.client.HttpClient",
            "java.net.http.HttpClient"
    );

    private final ArtifactStore store;
    private final ObjectMapper objectMapper;
    private final String jarName;
    private final boolean isStatic;

    private final ArtifactKey summaryKey;
    private final ArtifactKey summaryGzKey;
    private final ArtifactKey classTreeKey;
    private final ArtifactKey chunksDir;
    private final ArtifactKey chunkIndexKey;
    private final ArtifactKey summaryStage;
    private final ArtifactKey summaryGzStage;
    private final ArtifactKey classTreeStage;
    private final ArtifactKey chunkIndexStage;

    // summary output
    private OutputStream summaryPlain;
    private OutputStream summaryGz;
    private JsonGenerator summaryGen;
    private boolean summaryOpen;
    private boolean summaryFailed;

    // class-tree output (raw array; the rows are serialised individually so the same bytes feed
    // both the monolithic array and the chunker)
    private OutputStream classTreeOut;
    private boolean classTreeOpen;
    private boolean classTreeFailed;
    private ClassTreeChunks.Accumulator chunkAcc;

    // class-derived accumulators — bounded by annotations, not by document size
    private final Map<String, String> entityCollMap = new LinkedHashMap<>();
    private final Map<String, String> repoCollMap = new LinkedHashMap<>();
    private final List<Map<String, Object>> scheduledJobs = new ArrayList<>();
    private final List<String> enableSchedulingClasses = new ArrayList<>();
    private final int[] scheduledCount = {0};

    private int classRows;
    private int endpointRows;

    SaveTimeProjections(ArtifactStore store, ObjectMapper objectMapper, JarDataPaths paths,
                        String jarName, boolean isStatic) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.jarName = jarName;
        this.isStatic = isStatic;
        this.summaryKey = isStatic
                ? paths.summaryCacheStaticKey(jarName) : paths.summaryCacheKey(jarName);
        this.summaryGzKey = summaryKey.parent().resolve(summaryKey.leaf() + ".gz");
        this.classTreeKey = paths.classTreeCacheKey(jarName, null);
        this.chunksDir = paths.classTreeChunksDirKey(jarName);
        this.chunkIndexKey = paths.classTreeChunkIndexKey(jarName);
        this.summaryStage = stage(summaryKey);
        this.summaryGzStage = summaryKey.parent().resolve(summaryKey.leaf() + ".gz" + STAGE);
        this.classTreeStage = stage(classTreeKey);
        this.chunkIndexStage = stage(chunkIndexKey);
    }

    private static ArtifactKey stage(ArtifactKey key) {
        return key.parent().resolve(key.leaf() + STAGE);
    }

    boolean summaryFailed() { return summaryFailed; }

    boolean classTreeFailed() { return classTreeFailed; }

    private boolean summaryUsable() { return summaryOpen && !summaryFailed; }

    private boolean classTreeUsable() { return classTreeOpen && !classTreeFailed; }

    /* ─────────────────────────── summary ─────────────────────────── */

    /**
     * Opens the summary output and writes the top-level header, mirroring the document header
     * field-for-field (the read-time builder copies exactly these fields and skips the internal
     * ones: {@code callTreeFormat}, {@code callTreeNodes}, the pools).
     */
    void beginSummary(String projectName, long jarSize, String analyzedAt, int totalClasses,
                      int totalEndpoints, String analysisMode, String basePackage,
                      String scopeTarget) {
        try {
            summaryPlain = store.write(summaryStage);
            summaryGz = new java.util.zip.GZIPOutputStream(
                    new BufferedOutputStream(store.write(summaryGzStage), 64 * 1024));
            summaryGen = objectMapper.getFactory().createGenerator(new TeeOutputStream(summaryPlain, summaryGz));
            summaryOpen = true;
            summaryGen.writeStartObject();
            summaryGen.writeStringField("jarName", jarName);
            if (projectName != null) summaryGen.writeStringField("projectName", projectName);
            summaryGen.writeNumberField("jarSize", jarSize);
            summaryGen.writeStringField("analyzedAt", analyzedAt);
            summaryGen.writeNumberField("totalClasses", totalClasses);
            summaryGen.writeNumberField("totalEndpoints", totalEndpoints);
            summaryGen.writeStringField("analysisMode", analysisMode != null ? analysisMode : "STATIC");
            if (basePackage != null && !basePackage.isBlank()) {
                summaryGen.writeStringField("basePackage", basePackage);
            }
            if (scopeTarget != null && !scopeTarget.isBlank()) {
                summaryGen.writeBooleanField("scoped", true);
                summaryGen.writeStringField("scopeTarget", scopeTarget);
            }
        } catch (Exception e) {
            failSummary("header", e);
        }
    }

    /** The {@code advice} field, emitted only when non-empty exactly as the document does. */
    void writeAdvice(List<?> advice) {
        if (!summaryUsable()) return;
        try {
            if (advice != null && !advice.isEmpty()) {
                summaryGen.writeFieldName("advice");
                objectMapper.writeValue(summaryGen, advice);
            }
        } catch (Exception e) {
            failSummary("advice", e);
        }
    }

    /** Opens the class-tree array and the summary's {@code classIndex} array. */
    void beginClasses() {
        if (summaryUsable()) {
            try {
                summaryGen.writeFieldName("classIndex");
                summaryGen.writeStartArray();
            } catch (Exception e) {
                failSummary("classIndex open", e);
            }
        }
        try {
            classTreeOut = new BufferedOutputStream(store.write(classTreeStage), 128 * 1024);
            classTreeOut.write('[');
            chunkAcc = new ClassTreeChunks.Accumulator(store, chunksDir, chunkIndexStage, STAGE);
            classTreeOpen = true;
        } catch (Exception e) {
            failClassTree("begin", e);
        }
    }

    /**
     * Feeds one raw JSONL class line to both projections, parsing it here. A malformed line fails
     * this sidecar only — the document write is never affected (correctness first).
     */
    void addClassLine(String line) {
        try {
            addClass(objectMapper.readTree(line));
        } catch (Exception e) {
            failSummary("class parse", e);
            failClassTree("class parse", e);
        }
    }

    /**
     * Feeds one class to both projections. {@code classNode} is the raw JSONL class (invocations
     * inline), parsed once by the writer and shared, so the document's own parse is the only one.
     */
    void addClass(JsonNode classNode) {
        if (classNode == null) return;
        if (summaryUsable()) {
            try {
                Map<String, Object> entry = AbstractAnalysisDataProvider.summarizeClass(
                        classNode, objectMapper, entityCollMap, scheduledJobs, scheduledCount,
                        enableSchedulingClasses);
                objectMapper.writeValue(summaryGen, entry);
            } catch (Exception e) {
                failSummary("classIndex row", e);
            }
        }
        if (classTreeUsable()) {
            try {
                // One serialisation: the bytes go into the monolithic array verbatim, and the
                // parsed node feeds the chunker. The monolithic array is written raw with commas
                // here, exactly as a generator writing `[row,row]` would.
                ByteArrayOutputStream buf = new ByteArrayOutputStream(1024);
                try (JsonGenerator rowGen = objectMapper.getFactory().createGenerator(buf)) {
                    AbstractAnalysisDataProvider.writeClassTreeRow(
                            rowGen, objectMapper, classNode, HTTP_CLIENT_PREFIXES, true);
                }
                byte[] rowBytes = buf.toByteArray();
                if (classRows > 0) classTreeOut.write(',');
                classTreeOut.write(rowBytes);
                chunkAcc.add(objectMapper.readTree(rowBytes));
            } catch (Exception e) {
                failClassTree("row", e);
            }
        }
        classRows++;
    }

    /** Closes the {@code classIndex} array and emits the class-derived sidecars after it. */
    void endClasses() {
        if (!summaryUsable()) return;
        try {
            summaryGen.writeEndArray();
            summaryGen.writeFieldName("entityCollMap");
            objectMapper.writeValue(summaryGen, entityCollMap);
            summaryGen.writeFieldName("repoCollMap");
            objectMapper.writeValue(summaryGen, repoCollMap);
            summaryGen.writeFieldName("scheduledJobs");
            objectMapper.writeValue(summaryGen, scheduledJobs);
            summaryGen.writeNumberField("scheduledCount", scheduledCount[0]);
            summaryGen.writeFieldName("enableSchedulingClasses");
            objectMapper.writeValue(summaryGen, enableSchedulingClasses);
        } catch (Exception e) {
            failSummary("class sidecars", e);
        }
    }

    /** Opens the summary's {@code endpoints} array. */
    void beginEndpoints() {
        if (!summaryUsable()) return;
        try {
            summaryGen.writeFieldName("endpoints");
            summaryGen.writeStartArray();
        } catch (Exception e) {
            failSummary("endpoints open", e);
        }
    }

    /** Projects one endpoint from its un-pooled node, capped exactly as the read-time builder. */
    void addEndpoint(JsonNode endpointNode) {
        endpointRows++;
        if (endpointNode == null || !summaryUsable()) return;
        try {
            AbstractAnalysisDataProvider.writeSummaryEndpoint(
                    summaryGen, objectMapper, summaryEndpointFields(endpointNode), null);
        } catch (Exception e) {
            failSummary("endpoint row", e);
        }
    }

    void endEndpoints() {
        if (!summaryUsable()) return;
        try {
            summaryGen.writeEndArray();
            summaryGen.writeEndObject();
            summaryGen.flush();
        } catch (Exception e) {
            failSummary("endpoints close", e);
        }
    }

    /**
     * The endpoint's fields in document order, inflated and with the tree fields dropped — the
     * same map the read-time builder assembles by streaming the document endpoint.
     */
    private static Map<String, JsonNode> summaryEndpointFields(JsonNode endpointNode) {
        Map<String, JsonNode> fields = new LinkedHashMap<>();
        var it = endpointNode.fields();
        while (it.hasNext()) {
            var e = it.next();
            String key = e.getKey();
            if ("sourceCode".equals(key) || "callTree".equals(key) || "fragments".equals(key)
                    || CallTreeFlattener.ROOT_ID_FIELD.equals(key)
                    || CallTreeFlattener.FRAGMENT_IDS_FIELD.equals(key)) {
                continue;
            }
            fields.put(key, e.getValue());
        }
        return fields;
    }

    /* ─────────────────────────── commit / abort ─────────────────────────── */

    /**
     * Closes both outputs and publishes them, then signals which sidecars (if any) failed so the
     * caller can fall back to the lazy re-read path for exactly those. Never throws.
     */
    void finish() {
        if (summaryOpen && !summaryFailed) {
            try {
                summaryGen.close();
            } catch (Exception e) {
                failSummary("close", e);
            }
            summaryOpen = false;
        }
        if (classTreeOpen && !classTreeFailed) {
            try {
                classTreeOut.write(']');
                classTreeOut.close();
            } catch (Exception e) {
                failClassTree("close", e);
            }
            classTreeOpen = false;
        }
        if (!classTreeFailed && chunkAcc != null) {
            try {
                ClassTreeChunks.Index idx = chunkAcc.finish();
                log.info("Class-tree chunked during write: {} chunks / {} rows",
                        idx.chunkCount(), idx.rowCount());
            } catch (Exception e) {
                failClassTree("chunk index", e);
            }
        }

        if (!summaryFailed) {
            try {
                store.move(summaryStage, summaryKey);
                store.move(summaryGzStage, summaryGzKey);
                log.info("Summary built during write: {} ({} classes, {} endpoints)",
                        summaryKey, classRows, endpointRows);
            } catch (Exception e) {
                failSummary("publish", e);
            }
        }
        if (!classTreeFailed) {
            try {
                moveStagedChunks();
                store.move(classTreeStage, classTreeKey);
                log.info("Class tree built during write: {} ({} classes)", classTreeKey, classRows);
            } catch (Exception e) {
                failClassTree("publish", e);
            }
        }
        if (summaryFailed) cleanupSummaryStaging();
        if (classTreeFailed) cleanupClassTreeStaging();
    }

    /** Closes and deletes everything staged. Called when the document write itself failed. */
    void abort() {
        summaryFailed = true;
        classTreeFailed = true;
        closeQuietly(summaryGen);
        closeQuietly(summaryPlain);
        closeQuietly(summaryGz);
        closeQuietly(classTreeOut);
        cleanupSummaryStaging();
        cleanupClassTreeStaging();
    }

    private void moveStagedChunks() throws IOException {
        List<ArtifactKey> staged = new ArrayList<>();
        try (var s = store.list(chunksDir, false)) {
            s.forEach(e -> {
                if (e.key().leaf().endsWith(STAGE)) staged.add(e.key());
            });
        }
        for (ArtifactKey from : staged) {
            String leaf = from.leaf();
            store.move(from, from.parent().resolve(leaf.substring(0, leaf.length() - STAGE.length())));
        }
        store.move(chunkIndexStage, chunkIndexKey);
    }

    private void cleanupSummaryStaging() {
        deleteQuietly(summaryStage);
        deleteQuietly(summaryGzStage);
    }

    private void cleanupClassTreeStaging() {
        deleteQuietly(classTreeStage);
        deleteQuietly(chunkIndexStage);
        try (var s = store.list(chunksDir, false)) {
            s.filter(e -> e.key().leaf().endsWith(STAGE))
                    .forEach(e -> deleteQuietly(e.key()));
        } catch (Exception ignored) {
            // best effort — a stale staging chunk is inert (no index points at it)
        }
    }

    private void failSummary(String where, Exception e) {
        if (!summaryFailed) {
            log.warn("Save-time summary projection failed at {}: {} — will fall back to re-read",
                    where, e.getMessage());
        }
        summaryFailed = true;
    }

    private void failClassTree(String where, Exception e) {
        if (!classTreeFailed) {
            log.warn("Save-time class-tree projection failed at {}: {} — will fall back to re-read",
                    where, e.getMessage());
        }
        classTreeFailed = true;
    }

    private void closeQuietly(AutoCloseable c) {
        if (c == null) return;
        try { c.close(); } catch (Exception ignored) { /* best effort */ }
    }

    private void deleteQuietly(ArtifactKey key) {
        try { store.delete(key); } catch (Exception ignored) { /* best effort */ }
    }

    /** Writes every byte to two streams, so the summary can be produced once and stored twice. */
    private static final class TeeOutputStream extends OutputStream {
        private final OutputStream a;
        private final OutputStream b;

        TeeOutputStream(OutputStream a, OutputStream b) {
            this.a = a;
            this.b = b;
        }

        @Override public void write(int x) throws IOException { a.write(x); b.write(x); }

        @Override public void write(byte[] buf, int off, int len) throws IOException {
            a.write(buf, off, len);
            b.write(buf, off, len);
        }

        @Override public void flush() throws IOException { a.flush(); b.flush(); }

        @Override public void close() throws IOException {
            try { a.close(); } finally { b.close(); }
        }
    }
}
