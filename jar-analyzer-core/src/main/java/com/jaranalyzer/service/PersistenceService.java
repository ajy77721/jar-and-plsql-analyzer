package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import com.jaranalyzer.model.StreamableNodeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

@Primary
@Service("jarPersistenceService")
public class PersistenceService {

    private static final Logger log = LoggerFactory.getLogger(PersistenceService.class);

    private final ObjectMapper objectMapper;
    private final JarDataPaths paths;

    private final com.analyzer.storage.ArtifactStore store;

    public PersistenceService(JarDataPaths paths,
                              @Qualifier("jarArtifactStore") com.analyzer.storage.ArtifactStore store) {
        this.objectMapper = new ObjectMapper();
        this.paths = paths;
        this.store = store;
    }
    /** Every write in this class goes through the store; the key decides where it lands. */
    private java.io.OutputStream openWrite(com.analyzer.storage.ArtifactKey key) throws IOException {
        return store.write(key);
    }

    /** Reads via the store. Callers must close. */
    /**
     * Length-aware variant. Backends that must declare a size up front (a single-shot S3 PUT
     * rather than a multipart upload) use it; local ignores it.
     */
    private java.io.OutputStream openWrite(com.analyzer.storage.ArtifactKey key, long contentLength)
            throws IOException {
        return store.write(key, contentLength);
    }

    private InputStream openRead(com.analyzer.storage.ArtifactKey key) throws IOException {
        return store.read(key);
    }

    public JarDataPaths getPaths() { return paths; }

    /* ---- JAR file storage ---- */

    public Path storeJar(Path tempJar, String jarName) throws IOException {
        paths.ensureJarRoot(jarName);
        var key = paths.storedJarKey(jarName);
        try (InputStream in = Files.newInputStream(tempJar); OutputStream out = openWrite(key)) {
            in.transferTo(out);
        }
        log.info("Stored JAR: {} ({} MB)", key, store.size(key) / (1024 * 1024));
        return paths.storedJarFile(jarName);
    }

    public Path getJarFilePath(String id) {
        return store.exists(paths.storedJarKey(id)) ? paths.storedJarFile(id) : null;
    }

    /* ---- Per-JAR domain config storage ---- */

    public Path storeDomainConfig(Path tempConfig, String jarName) throws IOException {
        paths.ensureJarRoot(jarName);
        var key = paths.domainConfigKey(jarName);
        try (InputStream in = Files.newInputStream(tempConfig); OutputStream out = openWrite(key)) {
            in.transferTo(out);
        }
        log.info("Stored per-JAR domain config: {}", key);
        return paths.domainConfigFile(jarName);
    }

    public Path getDomainConfigPath(String jarName) {
        return store.exists(paths.domainConfigKey(jarName)) ? paths.domainConfigFile(jarName) : null;
    }

    /* ---- Connection info storage ---- */

    public void storeConnections(String jarName, Map<String, Object> info) {
        try {
            paths.ensureJarRoot(jarName);
            Path dest = paths.connectionsFile(jarName);
            try (java.io.OutputStream out = openWrite(paths.connectionsKey(jarName))) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, info);
            }
            log.info("Stored connections.json for {}", jarName);
        } catch (IOException e) {
            log.warn("Failed to store connections.json for {}: {}", jarName, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> loadConnections(String jarName) {
        var key = paths.connectionsKey(jarName);
        if (!store.exists(key)) return null;
        try (InputStream in = openRead(key)) {
            return objectMapper.readValue(in, Map.class);
        } catch (Exception e) {
            log.warn("Failed to load connections.json for {}: {}", jarName, e.getMessage());
            return null;
        }
    }

    /* ---- Streaming write (upload time — always writes STATIC) ---- */

    public void writeAnalysisStreaming(String jarName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints) throws IOException {
        writeAnalysisStreaming(jarName, null, jarSize, analyzedAt, totalClasses, totalEndpoints, classesJsonlFile, endpoints);
    }

    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints) throws IOException {
        writeAnalysisStreaming(jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints, classesJsonlFile, endpoints, "STATIC");
    }

    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints, String analysisMode) throws IOException {
        writeAnalysisStreaming(jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints,
                classesJsonlFile, endpoints, analysisMode, null);
    }

    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints, String analysisMode,
                                       String basePackage) throws IOException {
        writeAnalysisStreaming(jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints,
                classesJsonlFile, endpoints, analysisMode, basePackage, null);
    }

    /**
     * @param scopeTarget non-null only when this analysis was scoped to a single endpoint/method
     *                    (see CallGraphService.buildScopedEndpointFromIndex) rather than a full
     *                    scan of every discovered endpoint. Persisted so the UI can show a
     *                    "partial analysis" banner instead of implying full coverage.
     */
    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints, String analysisMode,
                                       String basePackage, String scopeTarget) throws IOException {
        paths.ensureJarRoot(jarName);
        // Invalidate FIRST: the rewrite below is about to replace every derived sidecar, and doing
        // this after it would delete the offsets/manifest just written. save()/correction paths also
        // call this, which is how a document written without offsets (the POJO save) drops a stale
        // index that would otherwise point into the new bytes.
        invalidateSummaryCache(jarName);
        WriteStats stats = writeAnalysisStreamingTo(paths.analysisFile(jarName), jarName, projectName, jarSize, analyzedAt,
                totalClasses, totalEndpoints, classesJsonlFile, endpoints, analysisMode, basePackage, scopeTarget);
        // Save-time precompute (plan 2b): the summary and the class tree are the first-hit penalties
        // a fresh analysis used to make its first visitor pay. Both are built here, from the same
        // builder code the lazy read paths use, so the first request is a small read.
        preGenerateSummaryCache(jarName, true);
        preGenerateClassTreeCache(jarName, true);
        writeManifest(jarName, true, stats);
    }

    public void writeAnalysisStreamingCorrected(String jarName, String projectName, long jarSize, String analyzedAt,
                                                 int totalClasses, int totalEndpoints,
                                                 Path classesJsonlFile,
                                                 List<EndpointInfo> endpoints, String analysisMode) throws IOException {
        paths.ensureJarRoot(jarName);
        invalidateSummaryCache(jarName);
        WriteStats stats = writeAnalysisStreamingTo(paths.correctedFile(jarName), jarName, projectName, jarSize, analyzedAt,
                totalClasses, totalEndpoints, classesJsonlFile, endpoints, analysisMode, null, null);
        preGenerateSummaryCache(jarName, false);
        preGenerateClassTreeCache(jarName, false);
        writeManifest(jarName, false, stats);
    }

    private WriteStats writeAnalysisStreamingTo(Path outPath, String jarName, String projectName, long jarSize,
                                           String analyzedAt, int totalClasses, int totalEndpoints,
                                           Path classesJsonlFile, List<EndpointInfo> endpoints,
                                           String analysisMode, String basePackage,
                                           String scopeTarget) throws IOException {
        log.info("Streaming analysis to {} ...", outPath);

        // The analysis document is the largest artifact by far (1 GB observed in production), so
        // it is streamed rather than buffered — openWrite returns an S3 multipart stream in s3
        // mode and a buffered file stream locally, and the generator below cannot tell.
        com.analyzer.storage.ArtifactKey outKey = outPath.equals(paths.correctedFile(jarName))
                ? paths.correctedKey(jarName) : paths.analysisKey(jarName);

        // Filled by writeDocumentTo in the SAME pass that writes the document: byte offsets for the
        // two ordinal-addressed arrays (plan 2a) and the counts the manifest needs (plan 2c).
        AnalysisOffsets offsets = new AnalysisOffsets();
        WriteStats stats = new WriteStats();

        // Flatten every endpoint's call tree into a content-addressed node dictionary BEFORE
        // opening the output. In memory a call tree is a DAG — a method reached by N paths is one
        // CallNode referenced N times — but JSON cannot say "this node again", so serialising it
        // nested re-emits each shared subtree once per path and the file grows with the number of
        // PATHS rather than the number of METHODS. That is what produced a 69 GB analysis.json for
        // unified-web that never finished writing. See CallTreeFlattener for the id scheme.
        // Scratch space for the dictionary while it is being built, so peak memory stays flat
        // regardless of how many distinct nodes the JAR has. Goes through the store like every
        // other byte this service writes, and is removed below.
        com.analyzer.storage.ArtifactKey spillKey =
                paths.jarRootKey(jarName).resolve("_calltree-nodes.spill.jsonl");
        try (CallTreeFlattener flattener =
                     new CallTreeFlattener(objectMapper, openWrite(spillKey), () -> openRead(spillKey))) {
            List<String> rootIds = new ArrayList<>(endpoints.size());
            List<com.fasterxml.jackson.databind.node.ObjectNode> fragIds = new ArrayList<>(endpoints.size());
            for (EndpointInfo ep : endpoints) {
                rootIds.add(flattener.flatten(ep.getCallTree()));
                fragIds.add(flattener.flattenFragments(ep.getFragments()));
            }
            log.info("  call tree flattened to {} distinct nodes across {} endpoints",
                    flattener.nodeCount(), endpoints.size());

            writeDocument(outKey, outPath, jarName, projectName, jarSize, analyzedAt, totalClasses,
                    totalEndpoints, classesJsonlFile, endpoints, analysisMode, basePackage, scopeTarget,
                    flattener, rootIds, fragIds, offsets, stats);
        } finally {
            try {
                store.delete(spillKey);
            } catch (Exception ignored) {
                // scratch only — a leftover file must never fail an otherwise-complete analysis
            }
        }

        // Ask the STORE for the size, not the filesystem: in s3 mode the bytes went to an object
        // and no local file exists, so Files.size() threw NoSuchFileException and failed the whole
        // analysis — from a logging line. Size is informational, so a failure to obtain it must
        // never propagate.
        long sizeBytes = -1;
        try {
            sizeBytes = store.size(outKey);
        } catch (Exception ignored) {
            // not worth failing an otherwise-successful analysis over
        }
        log.info("Analysis saved: {} ({})", outPath.getFileName(),
                sizeBytes >= 0 ? (sizeBytes / (1024 * 1024)) + " MB" : "size unknown");

        // Offset index (plan 2a), written right after the document it indexes. Its absence is a
        // supported state (old analyses), so a failure here only logs — it must never fail a save.
        try {
            com.analyzer.storage.ArtifactKey offsetsKey = outKey.equals(paths.correctedKey(jarName))
                    ? paths.correctedOffsetsKey(jarName) : paths.analysisOffsetsKey(jarName);
            offsets.writeTo(store, offsetsKey);
        } catch (Exception e) {
            log.warn("Failed to write analysis offsets sidecar for {}: {}", jarName, e.getMessage());
        }

        // Write lightweight header sidecar for fast listing
        writeHeaderSidecar(jarName, projectName, jarSize, analyzedAt,
                totalClasses, totalEndpoints, analysisMode, basePackage, scopeTarget);
        return stats;
    }

    /**
     * Turns {@code methods[].invocationRefs} back into {@code invocations} and drops the pool.
     *
     * <p>After this, the POJO graph is indistinguishable from one loaded from a pre-pooling
     * document, so the correction pipeline, the collections view and everything else downstream of
     * {@code load()} need no knowledge of pooling. A document with no pool is left alone.
     */
    private void resolveInvocationPool(JarAnalysis analysis) {
        var poolNode = analysis.getInvocationPool();
        if (poolNode == null || !poolNode.isArray() || poolNode.isEmpty()) return;

        // Deliberately kept as nodes and converted PER REFERENCE rather than converted once and
        // shared. Before pooling, every method owned distinct MethodCallInfo instances; sharing one
        // instance across the methods that reference it would make a mutation by any downstream
        // consumer — the Claude correction pipeline mutates this graph — visible in every other
        // method that happens to make the same call. The pool is a storage optimisation, and it must
        // not change object identity semantics in memory.
        List<com.fasterxml.jackson.databind.JsonNode> pool = new ArrayList<>(poolNode.size());
        poolNode.forEach(pool::add);

        long resolved = 0;
        for (var cls : analysis.getClasses()) {
            for (var m : cls.getMethods()) {
                var refs = m.getInvocationRefs();
                if (refs == null || refs.isEmpty()) continue;
                List<com.jaranalyzer.model.MethodCallInfo> invs = new ArrayList<>(refs.size());
                for (Integer ref : refs) {
                    // A ref outside the pool loses that one call rather than failing the load;
                    // a truncated document should still open.
                    if (ref != null && ref >= 0 && ref < pool.size()) {
                        invs.add(objectMapper.convertValue(
                                pool.get(ref), com.jaranalyzer.model.MethodCallInfo.class));
                    }
                }
                m.setInvocations(invs);
                m.setInvocationRefs(new ArrayList<>());
                resolved += invs.size();
            }
        }
        analysis.setInvocationPool(null);
        log.info("  resolved {} invocation references from a pool of {}", resolved, pool.size());
    }

    /**
     * Turns the {@code *Refs} ordinals on each endpoint back into their objects and drops the pool.
     *
     * <p>After this the POJO graph is indistinguishable from one loaded from a pre-pooling
     * document, so the correction pipeline and everything else downstream of {@code load()} needs
     * no knowledge of pooling. A document with no pool is left alone.
     */
    private void resolveEndpointPool(JarAnalysis analysis) {
        var poolNode = analysis.getEndpointPool();
        if (poolNode == null || !poolNode.isObject() || poolNode.isEmpty()) return;

        long resolved = 0;
        for (EndpointInfo ep : analysis.getEndpoints()) {
            resolved += resolveOne(poolNode, "externalCalls", ep.getExternalCallsRefs(),
                    new TypeReference<Map<String, String>>() {}, ep::setExternalCalls);
            ep.setExternalCallsRefs(new ArrayList<>());
            resolved += resolveOne(poolNode, "beans", ep.getBeansRefs(),
                    new TypeReference<Map<String, String>>() {}, ep::setBeans);
            ep.setBeansRefs(new ArrayList<>());
            resolved += resolveOne(poolNode, "dynamicFlows", ep.getDynamicFlowsRefs(),
                    new TypeReference<Map<String, Object>>() {}, ep::setDynamicFlows);
            ep.setDynamicFlowsRefs(new ArrayList<>());
        }
        analysis.setEndpointPool(null);
        log.info("  resolved {} endpoint aggregate references from the pool", resolved);
    }

    /**
     * Resolves one field's ordinals.
     *
     * <p>Converted PER REFERENCE rather than once and shared: before pooling every endpoint owned
     * distinct maps, and sharing one instance across the endpoints referencing it would make a
     * mutation by any downstream consumer visible in all of them. The pool is a storage
     * optimisation and must not change in-memory identity semantics.
     */
    private <T> long resolveOne(com.fasterxml.jackson.databind.JsonNode poolNode, String field,
                                List<Integer> refs, TypeReference<T> type,
                                java.util.function.Consumer<List<T>> setter) {
        if (refs == null || refs.isEmpty()) return 0;
        var pool = poolNode.get(field);
        if (pool == null || !pool.isArray()) return 0;
        List<T> out = new ArrayList<>(refs.size());
        for (Integer ref : refs) {
            // A ref outside the pool loses that one entry rather than failing the load.
            if (ref != null && ref >= 0 && ref < pool.size()) {
                out.add(objectMapper.convertValue(pool.get(ref), type));
            }
        }
        setter.accept(out);
        return out.size();
    }

    /**
     * The single owner of reads on the classes JSONL scratch file.
     *
     * <p>This file is pipeline-local scratch, not an artifact: it is produced and consumed inside
     * one analysis and never served, so it stays on the filesystem rather than going through
     * {@code ArtifactStore} — which is why it is a budgeted exception in the storage gate. Both
     * passes over it (advice detection, then the classes array) come through here so that
     * exception has one site rather than one per pass.
     */
    private static BufferedReader openClassesJsonl(Path classesJsonlFile) throws IOException {
        return Files.newBufferedReader(classesJsonlFile);
    }

    /**
     * Emits the analysis document. The node dictionary is written BEFORE {@code classes} and
     * {@code endpoints} on purpose: every reader is a single forward-only streaming pass, so the
     * dictionary has to arrive before the endpoints that reference it.
     */
    private void writeDocument(com.analyzer.storage.ArtifactKey outKey, Path outPath, String jarName,
                               String projectName, long jarSize, String analyzedAt, int totalClasses,
                               int totalEndpoints, Path classesJsonlFile, List<EndpointInfo> endpoints,
                               String analysisMode, String basePackage, String scopeTarget,
                               CallTreeFlattener flattener, List<String> rootIds,
                               List<com.fasterxml.jackson.databind.node.ObjectNode> fragIds,
                               AnalysisOffsets offsets, WriteStats stats)
            throws IOException {
        commitAtomically(outKey, staging ->
                writeDocumentTo(staging, jarName, projectName, jarSize, analyzedAt, totalClasses,
                        totalEndpoints, classesJsonlFile, endpoints, analysisMode, basePackage,
                        scopeTarget, flattener, rootIds, fragIds, offsets, stats));
    }

    /**
     * Counts collected in the same pass that writes the document, so the layout manifest needs no
     * second read of {@code analysis.json} (plan 2c).
     */
    static final class WriteStats {
        int classCount;
        int endpointCount;
        int packageCount;
        long docLength;
    }

    /**
     * A growable {@code (offset, length)} pair of arrays. The number of classes is not known before
     * the JSONL is read, and a {@code long[]} per element would box; this doubles as it fills.
     */
    private static final class GrowableOffsets {
        private long[] offsets = new long[256];
        private long[] lengths = new long[256];
        private int size;

        void add(long offset, long length) {
            if (size == offsets.length) {
                int n = offsets.length * 2;
                offsets = java.util.Arrays.copyOf(offsets, n);
                lengths = java.util.Arrays.copyOf(lengths, n);
            }
            offsets[size] = offset;
            lengths[size] = length;
            size++;
        }

        long[] offsets() { return java.util.Arrays.copyOf(offsets, size); }
        long[] lengths() { return java.util.Arrays.copyOf(lengths, size); }
    }

    /**
     * The {@code packageName} of a class JSONL line, or {@code null}.
     *
     * <p>A targeted scan, not a parse: {@code ClassInfo} declares {@code fullyQualifiedName} then
     * {@code packageName} and no nested object has that field, so the first occurrence is the
     * class's own. Counting distinct values while the classes are written is what gives the manifest
     * its {@code packages} count without a second pass.
     */
    private static String packageOf(String classLine) {
        final String marker = "\"packageName\":\"";
        int i = classLine.indexOf(marker);
        if (i < 0) return null;
        int p = i + marker.length();
        StringBuilder sb = new StringBuilder(32);
        while (p < classLine.length()) {
            char c = classLine.charAt(p);
            if (c == '\\' && p + 1 < classLine.length()) { sb.append(classLine.charAt(++p)); p++; continue; }
            if (c == '"') break;
            sb.append(c);
            p++;
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /** What to write, given the staging key to write it to. */
    @FunctionalInterface
    private interface StagedWrite {
        void writeTo(com.analyzer.storage.ArtifactKey staging) throws IOException;
    }

    /**
     * Writes via a staging key and commits with a single {@code move}, so a failure mid-write
     * cannot damage the analysis that is already stored.
     *
     * <p>This is not hypothetical. An encoder error thrown 23.7 MB into a 53 MB document left
     * <b>three</b> analyses truncated ({@code Unexpected end-of-input}), and because
     * {@code reanalyze-all} reads its metadata out of the very file that had just been destroyed,
     * it then found <b>0</b> JARs to re-queue — the failure took out both the data and the means of
     * rebuilding it. A complete old analysis is worth far more than a partial new one.
     *
     * <p>{@code move} is part of the frozen {@code ArtifactStore} SPI, so this needs no interface
     * change. It is a rename locally and a copy+delete on S3; the only guarantee relied on here is
     * that the destination never observes a partial body, which holds for all three backends.
     */
    private void commitAtomically(com.analyzer.storage.ArtifactKey target, StagedWrite write)
            throws IOException {
        com.analyzer.storage.ArtifactKey staging =
                target.parent().resolve(target.leaf() + ".partial");
        try {
            write.writeTo(staging);
        } catch (IOException | RuntimeException e) {
            try {
                store.delete(staging);
            } catch (Exception ignored) {
                // A leftover staging object is inert — nothing reads it. Losing the real failure
                // behind a cleanup error would be worse.
            }
            throw e;
        }
        store.move(staging, target);
    }

    /** The document body itself. Always writes to {@code target}, which is a staging key. */
    private void writeDocumentTo(com.analyzer.storage.ArtifactKey outKey, String jarName,
                                 String projectName, long jarSize, String analyzedAt, int totalClasses,
                                 int totalEndpoints, Path classesJsonlFile, List<EndpointInfo> endpoints,
                                 String analysisMode, String basePackage, String scopeTarget,
                                 CallTreeFlattener flattener, List<String> rootIds,
                                 List<com.fasterxml.jackson.databind.node.ObjectNode> fragIds,
                                 AnalysisOffsets offsets, WriteStats stats)
            throws IOException {
        try (OutputStream os = openWrite(outKey);
             CountingOutputStream counting = new CountingOutputStream(os);
             JsonGenerator gen = objectMapper.getFactory().createGenerator(counting)) {

            gen.writeStartObject();
            gen.writeStringField("jarName", jarName);
            if (projectName != null) gen.writeStringField("projectName", projectName);
            gen.writeNumberField("jarSize", jarSize);
            gen.writeStringField("analyzedAt", analyzedAt);
            gen.writeNumberField("totalClasses", totalClasses);
            gen.writeNumberField("totalEndpoints", totalEndpoints);
            gen.writeStringField("analysisMode", analysisMode != null ? analysisMode : "STATIC");
            if (basePackage != null && !basePackage.isBlank()) gen.writeStringField("basePackage", basePackage);
            if (scopeTarget != null && !scopeTarget.isBlank()) {
                gen.writeBooleanField("scoped", true);
                gen.writeStringField("scopeTarget", scopeTarget);
            }

            gen.writeStringField(CallTreeFlattener.FORMAT_FIELD, CallTreeFlattener.FORMAT);
            flattener.streamNodes(gen);

            // Cross-cutting advice: aspects, interceptors and filters that appear in NO call tree,
            // because interception happens through a proxy and leaves no static call edge. Written
            // before classes/endpoints so a forward-only reader has it first, and omitted entirely
            // when the JAR has none, so no existing consumer sees a new field.
            // Pool the duplicated per-endpoint arrays. Built before `endpoints` for the same
            // forward-only-reader reason as every other pool here. externalCalls is 4.6% unique:
            // the same external call reachable from many endpoints was written once per endpoint.
            EndpointArrayPool endpointPool = new EndpointArrayPool(objectMapper);
            for (EndpointInfo ep : endpoints) {
                endpointPool.scan(endpointNode(ep));
            }
            if (!endpointPool.isEmpty()) {
                endpointPool.streamPool(gen);
            }

            List<com.jaranalyzer.model.AdviceInfo> advice;
            try (BufferedReader adviceReader = openClassesJsonl(classesJsonlFile)) {
                advice = AdviceDetector.detect(adviceReader, objectMapper);
            }
            if (!advice.isEmpty()) {
                gen.writeFieldName("advice");
                objectMapper.writeValue(gen, advice);
            }

            // Pool distinct invocations — the largest field in the document, and 42.6% of its
            // entries are repeats of another. The pool must precede `classes`, but is only known
            // after every class has been seen, hence the separate scan pass. Same ordering
            // constraint and same solution as the call-tree node dictionary above.
            InvocationPool invocationPool = new InvocationPool(objectMapper);
            try (BufferedReader poolScan = openClassesJsonl(classesJsonlFile)) {
                invocationPool.scan(poolScan);
            }
            if (!invocationPool.isEmpty()) {
                invocationPool.streamPool(gen);
            }

            // ── classes (plan 2a) ───────────────────────────────────────────────────────────
            // The elements are written as raw bytes rather than through the generator, so the
            // counting stream's position is exactly the byte offset of each one — the offsets cost
            // nothing extra because the bytes are serialised either way. The generator is flushed
            // first so its buffer holds no bytes that have not reached the counter; the comma
            // separator is written here (not by the generator) and the offset recorded after it, so
            // a slice is a complete JSON value. The generator's array context is untouched: it wrote
            // `[`, we write the elements, it writes `]`.
            gen.writeFieldName("classes");
            gen.writeStartArray();
            gen.flush();
            int classCount = 0;
            GrowableOffsets classOffsets = new GrowableOffsets();
            Set<String> packages = new java.util.HashSet<>();
            try (BufferedReader reader = openClassesJsonl(classesJsonlFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) continue;
                    // Unchanged classes keep their original bytes; a pooled class is serialised by
                    // the mapper so its string escaping matches what produced the JSONL (a lone
                    // surrogate written raw fails the UTF-8 encoder).
                    var pooled = invocationPool.rewrite(line);
                    byte[] row = pooled == null
                            ? line.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                            : objectMapper.writeValueAsBytes(pooled);
                    if (classCount > 0) counting.write(',');
                    classOffsets.add(counting.count(), row.length);
                    counting.write(row);
                    String pkg = packageOf(line);
                    if (pkg != null) packages.add(pkg);
                    classCount++;
                    if (classCount % 500 == 0) {
                        gen.flush();
                        log.info("  ... {} classes written to JSON", classCount);
                    }
                }
            }
            gen.writeEndArray();
            log.info("  {} classes written", classCount);

            // ── endpoints (plan 2a) ─────────────────────────────────────────────────────────
            gen.writeFieldName("endpoints");
            gen.writeStartArray();
            gen.flush();
            int epCount = 0;
            GrowableOffsets endpointOffsets = new GrowableOffsets();
            for (EndpointInfo ep : endpoints) {
                byte[] row = endpointBytes(ep, rootIds.get(epCount), fragIds.get(epCount), endpointPool);
                if (epCount > 0) counting.write(',');
                endpointOffsets.add(counting.count(), row.length);
                counting.write(row);
                epCount++;
            }
            gen.writeEndArray();
            log.info("  {} endpoints written", epCount);

            gen.writeEndObject();
            gen.flush();

            offsets.setClasses(classOffsets.offsets(), classOffsets.lengths());
            offsets.setEndpoints(endpointOffsets.offsets(), endpointOffsets.lengths());
            stats.classCount = classCount;
            stats.endpointCount = epCount;
            stats.packageCount = packages.size();
            stats.docLength = counting.count();
        }
    }

    /**
     * One endpoint as bytes, with its call tree replaced by dictionary references.
     *
     * <p>Built as an {@code ObjectNode} and serialised whole rather than streamed through the
     * document's generator, so the writer can record its exact byte offset (plan 2a). The field
     * order, the ref-field slot and the compact serialisation are identical to the streamed form
     * this replaced, so the document's bytes are unchanged.
     *
     * <p>{@code callTree} and {@code fragments} are nulled for the duration of the conversion rather
     * than suppressed with a mixin, so the reference fields land in the exact slots the tree fields
     * occupied.
     */
    private byte[] endpointBytes(EndpointInfo ep, String rootId,
                                 com.fasterxml.jackson.databind.node.ObjectNode fragIds,
                                 EndpointArrayPool endpointPool) throws IOException {
        com.fasterxml.jackson.databind.node.ObjectNode on = endpointNode(ep);
        // Ordinals replace the objects under the SAME keys, so field order — and therefore the
        // bytes of an inflated endpoint — are unchanged.
        endpointPool.rewrite(on);

        com.fasterxml.jackson.databind.node.ObjectNode out = objectMapper.createObjectNode();
        boolean placed = false;
        var fields = on.fields();
        while (fields.hasNext()) {
            var e = fields.next();
            if ("callTree".equals(e.getKey())) {
                addRefFields(out, rootId, fragIds);
                placed = true;
            } else if ("fragments".equals(e.getKey())
                    || CallTreeFlattener.ROOT_ID_FIELD.equals(e.getKey())
                    || CallTreeFlattener.FRAGMENT_IDS_FIELD.equals(e.getKey())) {
                // dropped — the reference fields are written by addRefFields, in callTree's slot
            } else {
                out.set(e.getKey(), e.getValue());
            }
        }
        if (!placed) addRefFields(out, rootId, fragIds);
        return objectMapper.writeValueAsBytes(out);
    }

    private void addRefFields(com.fasterxml.jackson.databind.node.ObjectNode out, String rootId,
                              com.fasterxml.jackson.databind.node.ObjectNode fragIds) {
        if (rootId == null) out.putNull(CallTreeFlattener.ROOT_ID_FIELD);
        else out.put(CallTreeFlattener.ROOT_ID_FIELD, rootId);
        if (fragIds != null && fragIds.size() > 0) {
            out.set(CallTreeFlattener.FRAGMENT_IDS_FIELD, fragIds);
        }
    }

    /**
     * One endpoint as a tree, with the call tree and fragments detached.
     *
     * <p>Shared by the {@link EndpointArrayPool} pre-pass and {@link #endpointBytes} so both see
     * exactly the same node — if they disagreed, a pooled entry would not match its interned key
     * and the field would silently stay inline.
     *
     * <p>The detach-and-restore is why this is not simply {@code valueToTree}: the tree is written
     * separately as a shared node dictionary, and serialising it per endpoint is what produced a
     * 69 GB document.
     */
    private com.fasterxml.jackson.databind.node.ObjectNode endpointNode(EndpointInfo ep) {
        var callTree = ep.getCallTree();
        var fragments = ep.getFragments();
        try {
            ep.setCallTree(null);
            ep.setFragments(null);
            return objectMapper.valueToTree(ep);
        } finally {
            ep.setCallTree(callTree);
            ep.setFragments(fragments);
        }
    }

    // NOTE: the leading `Path jarRoot` these overloads used to take was dead — the sidecar is
    // written through paths.headerKey(jarName), never relative to that Path. It is gone rather
    // than kept "for context": a Path on a migrated signature invites the next caller to resolve
    // against it and silently drop back to the local filesystem.
    private void writeHeaderSidecar(String jarName, String projectName,
                                     long jarSize, String analyzedAt,
                                     int totalClasses, int totalEndpoints, String analysisMode,
                                     String basePackage) {
        writeHeaderSidecar(jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints, analysisMode, basePackage, null);
    }

    private void writeHeaderSidecar(String jarName, String projectName,
                                     long jarSize, String analyzedAt,
                                     int totalClasses, int totalEndpoints, String analysisMode,
                                     String basePackage, String scopeTarget) {
        try {
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("jarName", jarName);
            if (projectName != null) header.put("projectName", projectName);
            header.put("jarSize", jarSize);
            header.put("analyzedAt", analyzedAt);
            header.put("totalClasses", totalClasses);
            header.put("totalEndpoints", totalEndpoints);
            header.put("analysisMode", analysisMode != null ? analysisMode : "STATIC");
            if (basePackage != null && !basePackage.isBlank()) header.put("basePackage", basePackage);
            if (scopeTarget != null && !scopeTarget.isBlank()) {
                header.put("scoped", true);
                header.put("scopeTarget", scopeTarget);
            }
            try (java.io.OutputStream out = openWrite(paths.headerKey(jarName))) {
                objectMapper.writeValue(out, header);
            }
        } catch (IOException e) {
            log.debug("Failed to write header sidecar: {}", e.getMessage());
        }
    }

    /* ---- Load / Save ---- */

    public void save(JarAnalysis analysis) throws IOException {
        paths.ensureJarRoot(analysis.getJarName());
        var key = paths.analysisKey(analysis.getJarName());
        log.info("Saving static analysis to {} ...", key);
        // Staged and committed, like the streaming path: a failure here would otherwise replace a
        // complete analysis with a truncated one.
        commitAtomically(key, staging -> {
            try (OutputStream out = openWrite(staging)) {
                writeFlattened(out, analysis);
            }
        });
        log.info("Saved {} ({} MB)", key, store.size(key) / (1024 * 1024));
        // This path writes the document WITHOUT the offset index, so any index left from a prior
        // streaming save would point into different bytes. Drop it rather than serve a wrong record.
        store.delete(paths.analysisOffsetsKey(analysis.getJarName()));
        writeHeaderSidecar(analysis.getJarName(), analysis.getProjectName(),
                analysis.getJarSize(), analysis.getAnalyzedAt(),
                analysis.getTotalClasses(), analysis.getTotalEndpoints(), analysis.getAnalysisMode(),
                analysis.getBasePackage());
    }

    public void saveCorrected(JarAnalysis analysis) throws IOException {
        paths.ensureJarRoot(analysis.getJarName());
        var key = paths.correctedKey(analysis.getJarName());
        log.info("Saving corrected analysis to {} ...", key);
        // The most damaging of the three to lose: this overwrites the corrected document that the
        // Claude pipeline has already invested work in producing.
        commitAtomically(key, staging -> {
            try (OutputStream out = openWrite(staging)) {
                writeFlattened(out, analysis);
            }
        });
        log.info("Saved corrected {} ({} MB)", key, store.size(key) / (1024 * 1024));
        store.delete(paths.correctedOffsetsKey(analysis.getJarName()));
        writeHeaderSidecar(analysis.getJarName(), analysis.getProjectName(),
                analysis.getJarSize(), analysis.getAnalyzedAt(),
                analysis.getTotalClasses(), analysis.getTotalEndpoints(), analysis.getAnalysisMode(),
                analysis.getBasePackage());
        invalidateSummaryCache(analysis.getJarName());
    }

    /**
     * The document {@link #load} would open, as a stream rather than a materialised
     * {@code JarAnalysis} — for callers that only need a fraction of it (MID-101/§30:
     * {@code ChatboxService} was loading the whole object graph, up to the 912 MB measured
     * elsewhere in this codebase, to build a context capped at 40,000 characters).
     *
     * <p>Same key preference as {@link #load}: corrected first, then the original. A caller
     * streaming this must apply that preference itself, which is the whole reason this exists
     * rather than exposing {@code paths} and {@code store} directly — the preference is a rule
     * about which document is current, and duplicating it at each call site is how the two
     * accessors would drift.
     *
     * @return null if neither document exists
     */
    public InputStream openDocumentStream(String jarName) throws IOException {
        for (var key : List.of(paths.correctedKey(jarName), paths.analysisKey(jarName))) {
            if (store.exists(key)) return openRead(key);
        }
        return null;
    }

    /**
     * The header sidecar — {@code jarName}, {@code projectName}, {@code jarSize},
     * {@code analyzedAt}, {@code totalClasses}, {@code totalEndpoints}, {@code analysisMode} —
     * without opening {@code analysis.json} at all. Already used for the jar listing; exposed
     * publicly so a caller building a small context (a chat prompt, a summary) never has to load
     * the full document merely to answer "which jar is this and how big is it".
     */
    public Map<String, Object> readHeader(String jarName) throws IOException {
        // Guarded the same way the two existing callers guard it, rather than relying on
        // readHeaderOnly to handle a missing key gracefully — it was never asked to before.
        var key = paths.headerKey(jarName);
        return store.exists(key) ? readHeaderOnly(key) : null;
    }

    public JarAnalysis load(String jarName) throws IOException {
        for (var key : List.of(paths.correctedKey(jarName), paths.analysisKey(jarName))) {
            if (!store.exists(key)) continue;
            log.info("Loading analysis from {} ({} MB)...", key, store.size(key) / (1024 * 1024));
            try (InputStream in = openRead(key)) {
                return rehydrate(objectMapper.readValue(in, JarAnalysis.class));
            }
        }
        return null;
    }

    public JarAnalysis loadStatic(String jarName) throws IOException {
        var key = paths.analysisKey(jarName);
        if (store.exists(key)) {
            log.info("Loading static analysis from {} ({} MB)...", key, store.size(key) / (1024 * 1024));
            try (InputStream in = openRead(key)) {
                return rehydrate(objectMapper.readValue(in, JarAnalysis.class));
            }
        }
        return null;
    }

    /**
     * Expands a flat-format document back into ordinary nested {@code CallNode} trees.
     *
     * <p>Everything downstream of {@code load()} — the Claude correction pipeline, the collections
     * view, the chat context — works on {@code EndpointInfo.getCallTree()}. Rehydrating here means
     * none of it has to know the storage format changed, and a load/modify/{@code saveCorrected}
     * round trip keeps the trees it started with. Legacy documents already carry inline trees and
     * pass through untouched.
     */
    private JarAnalysis rehydrate(JarAnalysis analysis) throws IOException {
        if (analysis == null) return analysis;

        // Invocations first, and independently of the call-tree format: a document can carry a
        // pool without flattened trees, and returning early on a null callTreeNodes (as this used
        // to) would leave every method holding ordinals instead of invocations.
        resolveInvocationPool(analysis);
        resolveEndpointPool(analysis);

        if (analysis.getCallTreeNodes() == null) return analysis;

        var parser = objectMapper.treeAsTokens(analysis.getCallTreeNodes());
        parser.nextToken();
        CallTreeInflater inflater = CallTreeInflater.read(parser, objectMapper);

        for (EndpointInfo ep : analysis.getEndpoints()) {
            var tree = inflater.toTree(ep.getCallTreeRootId(), objectMapper);
            ep.setCallTree(tree == null ? null : objectMapper.treeToValue(tree, CallNode.class));
            Map<String, List<CallNode>> frags = new LinkedHashMap<>();
            if (ep.getFragmentChildIds() != null) {
                for (var e : ep.getFragmentChildIds().entrySet()) {
                    List<CallNode> kids = new ArrayList<>();
                    for (String id : e.getValue()) {
                        var t = inflater.toTree(id, objectMapper);
                        if (t != null) kids.add(objectMapper.treeToValue(t, CallNode.class));
                    }
                    frags.put(e.getKey(), kids);
                }
            }
            ep.setFragments(frags);
            ep.setCallTreeRootId(null);
            ep.setFragmentChildIds(null);
        }
        // Cleared so the rehydrated object is indistinguishable from one loaded from a legacy
        // document, and so a later save() rebuilds the dictionary from the (possibly corrected)
        // trees rather than re-emitting a stale one.
        analysis.setCallTreeNodes(null);
        analysis.setCallTreeFormat(null);
        return analysis;
    }

    /**
     * POJO counterpart of {@link #writeAnalysisStreaming}: flattens the trees into the shared
     * dictionary, writes, then restores the object so the caller can keep using it.
     *
     * <p>MID-103 — the dictionary itself is no longer materialised in memory at all. It used to be
     * built as one {@code ObjectNode} (`dict`), fed from a per-endpoint {@code
     * CallTreeFlattener.sidecar()} call whose own {@code ObjectNode} was merged in and discarded —
     * real work (a spilling {@link CallTreeFlattener}, already built and already used by {@link
     * #writeAnalysisStreamingTo} for the crawl-time path) existed to avoid exactly this, just not
     * wired into this POJO-input path. {@code flattener.flatten(...)} now spills each node straight
     * to a scratch file as it is discovered, and {@link StreamableNodeSerializer} (see its own
     * javadoc) redirects {@code JarAnalysis.callTreeNodes}'s normal serialization to {@link
     * CallTreeFlattener#streamNodesValue} for this one write — so the dictionary goes from the
     * spill file straight to {@code out}, one node at a time, never resident as a Java object.
     *
     * <p>What this does <b>not</b> change: {@code savedTrees}/{@code savedFragments} still hold the
     * original nested {@code CallNode} trees for the whole write, so they can be restored in the
     * {@code finally} block — {@code analysis} is a live object the caller keeps using afterward,
     * and mutating it permanently would be a correctness bug, not a memory optimisation. Removing
     * that residency too would mean giving up the mutate-then-restore strategy entirely (e.g.
     * flattening a read-only snapshot instead of the live tree), which is real, separate, larger
     * work — not attempted here.
     */
    private void writeFlattened(OutputStream out, JarAnalysis analysis) throws IOException {
        List<CallNode> savedTrees = new ArrayList<>();
        List<Map<String, List<CallNode>>> savedFragments = new ArrayList<>();

        com.analyzer.storage.ArtifactKey spillKey = paths.jarRootKey(analysis.getJarName())
                .resolve("_calltree-nodes-flat.spill." + System.nanoTime() + ".jsonl");
        try (CallTreeFlattener flattener =
                     new CallTreeFlattener(objectMapper, openWrite(spillKey), () -> openRead(spillKey))) {
            try {
                for (EndpointInfo ep : analysis.getEndpoints()) {
                    savedTrees.add(ep.getCallTree());
                    savedFragments.add(ep.getFragments());
                    String rootId = flattener.flatten(ep.getCallTree());
                    ep.setCallTreeRootId(rootId);

                    if (ep.getFragments() != null && !ep.getFragments().isEmpty()) {
                        Map<String, List<String>> fragIds = new LinkedHashMap<>();
                        for (var e : ep.getFragments().entrySet()) {
                            fragIds.put(e.getKey(), flattener.flattenAll(e.getValue()));
                        }
                        ep.setFragmentChildIds(fragIds);
                    }
                    ep.setCallTree(null);
                    ep.setFragments(null);
                }
                analysis.setCallTreeFormat(CallTreeFlattener.FORMAT);
                analysis.setCallTreeNodes(null);
                objectMapper.writer()
                        .withAttribute(StreamableNodeSerializer.STREAM_ATTR,
                                (StreamableNodeSerializer.StreamWriter) flattener::streamNodesValue)
                        .writeValue(out, analysis);
            } finally {
                analysis.setCallTreeFormat(null);
                analysis.setCallTreeNodes(null);
                for (int i = 0; i < analysis.getEndpoints().size() && i < savedTrees.size(); i++) {
                    EndpointInfo ep = analysis.getEndpoints().get(i);
                    ep.setCallTree(savedTrees.get(i));
                    ep.setFragments(savedFragments.get(i));
                    ep.setCallTreeRootId(null);
                    ep.setFragmentChildIds(null);
                }
            }
        } finally {
            try {
                store.delete(spillKey);
            } catch (Exception ignored) {
                // scratch only — a leftover file must never fail an otherwise-complete write
            }
        }
    }

    public void invalidateSummaryCache(String jarName) {
        try {
            var root = paths.jarRootKey(jarName);
            store.delete(paths.summaryCacheKey(jarName));
            store.delete(root.resolve("_summary.json.gz"));
            store.delete(paths.summaryCacheStaticKey(jarName));
            store.delete(root.resolve("_summary_static.json.gz"));
            store.delete(paths.classTreeCacheKey(jarName, null));
            store.delete(root.resolve("_class-tree.json.tmp"));
            // Versioned class-tree caches too — deleting only the default left the "corrected"
            // variant behind, so after applying corrections the UI could still be served the
            // pre-correction class tree. List rather than enumerate: the version suffix is
            // caller-supplied (see classTreeCacheKey(jarName, version)), so there is no fixed list.
            try (var s = store.list(root, false)) {
                for (var entry : s.filter(e -> e.key().leaf().startsWith("_class-tree_")).toList()) {
                    store.delete(entry.key());
                }
            }
            // Precomputed view summaries (the summary header, the collections aggregate) summarise
            // the document that was just rewritten, so they are stale the moment it changes. Deleted
            // whole: a re-analysis or a correction must never be shadowed by a pre-change view.
            store.deletePrefix(paths.viewsDirKey(jarName));
            // Chunked class tree (plan 2d): the directory does not match the "_class-tree_" prefix
            // above (its leaf is "_class-tree"), so it is deleted explicitly. A stale chunk index
            // would otherwise point the serving path at pre-rewrite rows.
            store.deletePrefix(paths.classTreeChunksDirKey(jarName));
            store.delete(paths.classTreeChunkIndexKey(jarName));
            // The manifest describes the artifacts of the document that was just replaced; the
            // streaming save rewrites it, and a POJO save simply leaves it absent (readers treat a
            // missing manifest as "no index", never as an error).
            store.delete(paths.manifestKey(jarName));
        } catch (IOException e) { log.debug("Failed to invalidate summary cache: {}", e.getMessage()); }
    }

    /**
     * Pre-generate summary cache from the analysis file so first UI open is instant.
     * Called after saving analysis.json — adds ~20-30s to upload but saves that on every first open.
     */
    public void preGenerateSummaryCache(String jarName, boolean isStatic) {
        var analysisKey = isStatic ? paths.analysisKey(jarName) : paths.correctedKey(jarName);
        var cacheKey = isStatic ? paths.summaryCacheStaticKey(jarName) : paths.summaryCacheKey(jarName);
        var gzKey = cacheKey.parent().resolve(cacheKey.leaf() + ".gz");
        if (!store.exists(analysisKey)) return;
        try {
            log.info("Pre-generating summary cache for {} ...", jarName);
            long start = System.currentTimeMillis();
            AnalysisDataProvider provider = isStatic
                    ? new StaticAnalysisProvider(analysisKey, objectMapper, paths, jarName)
                    : new ClaudeAnalysisProvider(analysisKey, objectMapper, paths, jarName, "claude");
            // Streamed to BOTH outputs in one pass rather than buffered.
            //
            // This used to build the whole summary in a ByteArrayOutputStream so it could be written
            // twice — plain and gzipped. The summary grows with class and endpoint count (430 KB
            // measured at 119 classes, ~35 MB projected at 1,597), so on a very large analysis that
            // buffer is the process's peak memory and eventually an OutOfMemoryError. A tee writes
            // both copies as the generator produces bytes, so memory is constant regardless of how
            // large the analysis is — which is the property the rest of this class already has.
            long written;
            try (OutputStream plain = openWrite(cacheKey);
                 OutputStream gz = new java.util.zip.GZIPOutputStream(
                         new java.io.BufferedOutputStream(openWrite(gzKey), 64 * 1024));
                 CountingOutputStream counting = new CountingOutputStream(new TeeOutputStream(plain, gz));
                 InputStream src = openRead(analysisKey);
                 var parser = objectMapper.getFactory().createParser(src);
                 var gen = objectMapper.getFactory().createGenerator(counting)) {
                provider.streamSummary(parser, gen);
                gen.flush();
                written = counting.count();
            }
            long ms = System.currentTimeMillis() - start;
            log.info("Summary cache pre-generated: {} ({} MB, {}ms)", cacheKey,
                    written / (1024 * 1024), ms);
        } catch (Exception e) {
            log.warn("Failed to pre-generate summary cache: {}", e.getMessage());
        }
    }

    /**
     * Pre-generate the class-tree cache at save time (plan 2b), so the first Code Structure request
     * is a small read instead of a full pass over the (up to hundreds of MB) document.
     *
     * <p>Calls the same builder the lazy read path uses ({@code ArtifactScanRepository} streams
     * {@code provider.streamClassTree} into {@code _class-tree.json} on a cache miss); the lazy path
     * stays as the fallback for analyses written before this, detected by the cache's absence or a
     * trailing byte that is not {@code ]}.
     *
     * <p>The chunked form (plan 2d) is split from the just-written monolith — a pass over the
     * class-tree sidecar, never over {@code analysis.json} — so the serving path can prefer chunks.
     */
    public void preGenerateClassTreeCache(String jarName, boolean isStatic) {
        var analysisKey = isStatic ? paths.analysisKey(jarName) : paths.correctedKey(jarName);
        var cacheKey = paths.classTreeCacheKey(jarName, null);
        if (!store.exists(analysisKey)) return;
        try {
            log.info("Pre-generating class-tree cache for {} ...", jarName);
            long start = System.currentTimeMillis();
            AnalysisDataProvider provider = isStatic
                    ? new StaticAnalysisProvider(analysisKey, objectMapper, paths, jarName)
                    : new ClaudeAnalysisProvider(analysisKey, objectMapper, paths, jarName, "claude");
            long written;
            try (InputStream src = openRead(analysisKey);
                 CountingOutputStream counting = new CountingOutputStream(
                         new java.io.BufferedOutputStream(openWrite(cacheKey), 128 * 1024));
                 var parser = objectMapper.getFactory().createParser(src);
                 var gen = objectMapper.getFactory().createGenerator(counting)) {
                provider.streamClassTree(parser, gen);
                gen.flush();
                written = counting.count();
            }
            log.info("Class-tree cache pre-generated: {} ({} KB, {}ms)", cacheKey,
                    written / 1024, System.currentTimeMillis() - start);
            // Chunks (plan 2d). A failure here is not fatal: the serving path falls back to the
            // monolith, which was just written above.
            try {
                ClassTreeChunks.Index idx = ClassTreeChunks.split(store, cacheKey,
                        paths.classTreeChunksDirKey(jarName), paths.classTreeChunkIndexKey(jarName));
                log.info("Class-tree chunked: {} chunks / {} rows", idx.chunkCount(), idx.rowCount());
            } catch (Exception e) {
                log.warn("Failed to chunk class-tree cache: {}", e.getMessage());
            }
        } catch (Exception e) {
            // A partial write must not be served: delete it so the lazy path rebuilds cleanly.
            try { store.delete(cacheKey); } catch (Exception ignored) { /* best effort */ }
            log.warn("Failed to pre-generate class-tree cache: {}", e.getMessage());
        }
    }

    /**
     * Writes {@code jar/<id>/manifest.json} (plan 2c) — the artifact inventory, counts and sizes a
     * reader can use to route an API with one small read. Built entirely from values known at save
     * time (see {@link WriteStats} and {@link AnalysisOffsets}); never a pass over the document.
     */
    private void writeManifest(String jarName, boolean isStatic, WriteStats stats) {
        try {
            var analysisKey = isStatic ? paths.analysisKey(jarName) : paths.correctedKey(jarName);
            var offsetsKey = isStatic ? paths.analysisOffsetsKey(jarName) : paths.correctedOffsetsKey(jarName);
            JarLayoutManifest m = new JarLayoutManifest(jarName);
            m.setCount("classes", stats.classCount);
            m.setCount("endpoints", stats.endpointCount);
            m.setCount("packages", stats.packageCount);

            // ONE recursive listing of the jar root answers every presence/size/mtime question the
            // manifest asks. The previous form issued a HEAD (exists + size) per artifact — ~14
            // round trips on S3 for a manifest whose whole purpose is to save the reader round
            // trips — plus a second listing for the endpoints aggregate. The listing drains any
            // pending write-behind uploads first, so it sees the summary/class-tree/chunk artifacts
            // written moments earlier in this same save. Its payload is the endpoints subtree either
            // way, because the endpoints aggregate already listed it.
            Map<ArtifactKey, ArtifactEntry> tree = store.statPrefix(paths.jarRootKey(jarName));

            recordIfPresent(m, tree, analysisKey, (long) stats.classCount);
            recordIfPresent(m, tree, offsetsKey, (long) stats.classCount + stats.endpointCount);
            recordIfPresent(m, tree, paths.headerKey(jarName), null);

            ArtifactKey summaryKey = isStatic
                    ? paths.summaryCacheStaticKey(jarName) : paths.summaryCacheKey(jarName);
            recordIfPresent(m, tree, summaryKey, null);
            recordIfPresent(m, tree, summaryKey.parent().resolve(summaryKey.leaf() + ".gz"), null);
            recordIfPresent(m, tree, paths.classTreeCacheKey(jarName, null), (long) stats.classCount);
            recordIfPresent(m, tree, paths.storedJarKey(jarName), null);

            // Offset refs: which key holds the index, and how many rows it addresses.
            Map<String, Object> offsets = new LinkedHashMap<>();
            offsets.put(AnalysisOffsets.SEGMENT_CLASSES, offsetRef(offsetsKey, stats.classCount));
            offsets.put(AnalysisOffsets.SEGMENT_ENDPOINTS, offsetRef(offsetsKey, stats.endpointCount));
            m.setSection("offsets", offsets);

            // Chunked class tree (plan 2d), when it was produced.
            ClassTreeChunks.Index idx = ClassTreeChunks.loadIndex(store, paths.classTreeChunkIndexKey(jarName));
            if (idx != null) {
                Map<String, Object> chunks = new LinkedHashMap<>();
                chunks.put("dir", paths.classTreeChunksDirKey(jarName).toString());
                chunks.put("chunkSize", idx.chunkSize());
                chunks.put("chunkCount", idx.chunkCount());
                chunks.put("rowCount", idx.rowCount());
                m.setSection("classTreeChunks", chunks);
            }

            // Per-endpoint output folders, collapsed to one aggregate — from the SAME listing.
            m.recordPrefix(tree, paths.endpointsDirKey(jarName));

            m.writeTo(store, paths.manifestKey(jarName));
        } catch (Exception e) {
            log.warn("Failed to write jar manifest for {}: {}", jarName, e.getMessage());
        }
    }

    /**
     * Records {@code key} in the manifest when the (already-taken) listing saw it, with the size and
     * mtime that listing carried — no per-key HEAD. Absent keys are skipped, exactly as the previous
     * {@code exists}-guarded form skipped them.
     */
    private static void recordIfPresent(JarLayoutManifest m, Map<ArtifactKey, ArtifactEntry> tree,
                                        ArtifactKey key, Long rows) {
        ArtifactEntry entry = tree.get(key);
        if (entry == null) return;
        m.recordArtifact(key, entry.size(), rows, entry.lastModified());
    }

    private static Map<String, Object> offsetRef(com.analyzer.storage.ArtifactKey key, int rowCount) {
        Map<String, Object> ref = new LinkedHashMap<>();
        ref.put("key", key.toString());
        ref.put("rowCount", rowCount);
        return ref;
    }

    /**
     * Writes every byte to two streams, so a document can be produced once and stored in two forms.
     *
     * <p>Both are closed even if the first close throws — otherwise a failure writing the plain copy
     * would leak the gzip stream and, with a store whose writes publish on close, leave a half-written
     * artifact visible.
     */
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

    /** Counts bytes passing through, so a streamed artifact can still report its size. */
    private static final class CountingOutputStream extends java.io.FilterOutputStream {
        private long count;

        CountingOutputStream(OutputStream out) { super(out); }

        long count() { return count; }

        @Override public void write(int x) throws IOException { out.write(x); count++; }

        @Override public void write(byte[] buf, int off, int len) throws IOException {
            // FilterOutputStream's default writes byte-at-a-time through write(int); overriding is
            // what makes this usable on a multi-GB stream.
            out.write(buf, off, len);
            count += len;
        }
    }

    /* ---- File path getters for streaming responses ---- */

    public Path getFilePath(String jarName) {
        if (store.exists(paths.correctedKey(jarName))) return paths.correctedFile(jarName);
        return store.exists(paths.analysisKey(jarName)) ? paths.analysisFile(jarName) : null;
    }

    public Path getStaticFilePath(String jarName) {
        return store.exists(paths.analysisKey(jarName)) ? paths.analysisFile(jarName) : null;
    }

    public Path getCorrectedFilePath(String jarName) {
        return store.exists(paths.correctedKey(jarName)) ? paths.correctedFile(jarName) : null;
    }

    public Path getCorrectedPrevFilePath(String jarName) {
        return store.exists(paths.correctedPrevKey(jarName)) ? paths.correctedPrevFile(jarName) : null;
    }

    /* ---- Key twins of the getters above ----
     *
     * The Path getters are still used by callers that hand a file name to a response header. The
     * analysis PROVIDERS take a key, so they use these: no keyForPath() round-trip, and therefore
     * no way for a provider to be built from a path outside the data root. Same existence
     * semantics as the Path twins — null when the artifact is absent. */

    /** Best available document: corrected first, then static. Null if neither exists. */
    public com.analyzer.storage.ArtifactKey getDocumentKey(String jarName) {
        if (store.exists(paths.correctedKey(jarName))) return paths.correctedKey(jarName);
        return store.exists(paths.analysisKey(jarName)) ? paths.analysisKey(jarName) : null;
    }

    public com.analyzer.storage.ArtifactKey getStaticDocumentKey(String jarName) {
        return store.exists(paths.analysisKey(jarName)) ? paths.analysisKey(jarName) : null;
    }

    public com.analyzer.storage.ArtifactKey getCorrectedDocumentKey(String jarName) {
        return store.exists(paths.correctedKey(jarName)) ? paths.correctedKey(jarName) : null;
    }

    public com.analyzer.storage.ArtifactKey getCorrectedPrevDocumentKey(String jarName) {
        return store.exists(paths.correctedPrevKey(jarName)) ? paths.correctedPrevKey(jarName) : null;
    }

    /* ---- Version management ---- */

    public void rotateCorrected(String jarName) throws IOException {
        var current = paths.correctedKey(jarName);
        if (!store.exists(current)) return;
        var prev = paths.correctedPrevKey(jarName);
        store.move(current, prev);
        log.info("Rotated corrected -> prev: {}", prev);
    }

    public void revertCorrected(String jarName) throws IOException {
        var prev = paths.correctedPrevKey(jarName);
        if (!store.exists(prev)) {
            throw new IOException("No previous corrected version to revert to");
        }
        var current = paths.correctedKey(jarName);
        store.move(prev, current);
        log.info("Reverted: prev -> corrected: {}", current);
    }

    public Map<String, Object> getVersionInfo(String jarName) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("hasStatic", store.exists(paths.analysisKey(jarName)));
        info.put("hasCorrected", store.exists(paths.correctedKey(jarName)));
        info.put("hasCorrectedPrev", store.exists(paths.correctedPrevKey(jarName)));
        var correctedKey = paths.correctedKey(jarName);
        if (store.exists(correctedKey)) {
            try {
                Map<String, Object> header = readHeaderOnly(correctedKey);
                if (header != null) {
                    if (header.containsKey("claudeIteration"))
                        info.put("claudeIteration", header.get("claudeIteration"));
                    if (header.containsKey("correctionAppliedAt"))
                        info.put("correctionAppliedAt", header.get("correctionAppliedAt"));
                    if (header.containsKey("correctionCount"))
                        info.put("correctionCount", header.get("correctionCount"));
                }
            } catch (IOException e) { /* defaults */ }
        }
        info.putIfAbsent("claudeIteration", 0);
        return info;
    }

    /* ---- Listing ---- */

    /**
     * Storage-backed listing, used when an ArtifactStore is wired.
     *
     * <p>Needed because the write path now targets the store: with s3 mode the header sidecars
     * are objects, not files, so the filesystem walk below finds nothing and the dashboard shows
     * an empty list even though every analysis uploaded correctly.
     *
     * <p><b>The scan is shallow and the reads are concurrent.</b> One listing per candidate
     * directory under {@code keyRoot()} finds every analysis root, and the {@code _header.json}
     * sidecar each row needs is read on {@code ShallowTree}'s pool rather than one after another.
     * The recursive {@code statPrefix} this replaced answered the same question by returning every
     * object in the tree — endpoint documents, class trees and sources included — so its payload
     * grew with what each analysis <em>produced</em>, not with how many analyses exist. Walking
     * per-JAR with exists()/size() is still the wrong shape for the opposite reason: 9 metadata
     * round trips each, ~12.5 s at 50 analyses over S3.
     */
    private List<Map<String, Object>> listJarsFromStore() {
        try {
            return scanJarsFromStore();
        } catch (Exception e) {
            // A storage failure is not "no analyses": the caller (and the operator reading the log)
            // must be able to tell them apart. The lenient entry point stays empty-on-error for the
            // internal callers that prefer a degraded screen; the HTTP listing uses listJarsStrict.
            log.warn("Store-backed listJars failed: {}", e.toString());
            return new ArrayList<>();
        }
    }

    /**
     * The scan {@link #listJarsFromStore()} wraps: shallow discovery plus a concurrent header read
     * per analysis. Throws when the store cannot be reached, so a caller that must not confuse
     * "unreachable" with "empty" can say so.
     *
     * <p>The store's request scope is captured on this thread by {@code ShallowTree} before its
     * workers start, so each pool thread routes as the requesting user rather than falling through
     * to the shared default target — which on a pod with no ambient credentials failed with an
     * {@code SdkClientException} and produced an empty list for a user whose data was right there.
     */
    private List<Map<String, Object>> scanJarsFromStore() throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();

        // Real bug, found live: this used to hardcode `ArtifactKey.of("jar")` and always group
        // by `segs.get(1)` — correct ONLY for the plain JAR bean, whose keyRoot() is a single
        // segment ("jar"). WarDataPaths overrides keyRoot() to `{"jar","war"}` (WAR artifacts
        // live at jar/war/<warName>/..., 4 segments deep, per its own doc comment: "Without
        // this, WAR artifacts land in the JAR namespace"). Scanning "jar" and grouping by
        // index 1 therefore collapsed every WAR analysis into one bogus group keyed by the
        // literal string "war", and the real warName (segs.get(2)) was never read — the WAR
        // list screen always showed empty ("No JARs analyzed yet") even though every upload
        // completed and was directly readable by id. `listJarIds()` (below) already derives
        // this correctly from `paths.keyRoot()`; mirrored here instead of re-deriving it.
        String[] root = paths.keyRoot();
        com.analyzer.storage.ArtifactKey jarsRoot = com.analyzer.storage.ArtifactKey.of(root);

        // Shallow: one listing per candidate directory directly under keyRoot(), never a scan of
        // everything each analysis wrote. An analysis root is recognised by its own
        // analysis.json / analysis_corrected.json, so the walk descends at most one level and
        // stops — endpoint documents, class trees and sources are never enumerated. Same set of
        // ids and same order (key order) as the grouping this replaced.
        //
        // Real bug, found live alongside the routing fault: a directory WITHOUT an analysis.json
        // used to be SKIPped. That is every directory at the root level — an analysis lives at
        // jar/<id>/analysis.json, one level below the `jar` walk root — so the walk pruned the very
        // directories it needed to look inside and listed nothing even against a reachable store.
        // The rule must be RECORD at an analysis root and DESCEND otherwise; the depth bound (1)
        // is what stops the descent, so a non-analysis directory costs one listing and no more.
        List<com.analyzer.storage.ShallowTree.Found> found = com.analyzer.storage.ShallowTree
                .findTargets(store, jarsRoot, 1, (dir, children) ->
                        com.analyzer.storage.ShallowTree.hasFile(children, "analysis.json")
                                || com.analyzer.storage.ShallowTree.hasFile(children, "analysis_corrected.json")
                                ? com.analyzer.storage.ShallowTree.Decision.RECORD
                                : com.analyzer.storage.ShallowTree.Decision.DESCEND);

        // The per-analysis header read is an independent round trip each; run them together, on the
        // requesting user's routing (the store is passed so ShallowTree captures the scope).
        List<Map<String, Object>> rows = com.analyzer.storage.ShallowTree.parallelMap(store, found,
                f -> listOneJar(f.dir().leaf(), f));
        for (Map<String, Object> row : rows) {
            if (row != null) result.add(row);
        }
        return result;
    }

    /**
     * One JAR's dashboard row, built from its own shallow listing.
     *
     * <p>{@code found} carries the direct children of {@code <keyRoot>/<id>/}, which is where every
     * key this row needs lives — the {@code _header.json} sidecar, the two analysis documents and the
     * per-JAR config — so deciding what is present costs no round trip beyond the listing the walk
     * already made.
     *
     * @return the row, or null when the directory holds nothing listable
     */
    private Map<String, Object> listOneJar(String id, com.analyzer.storage.ShallowTree.Found found) {
        com.analyzer.storage.ArtifactKey headerKey = paths.headerKey(id);
        Map<String, Object> entry = null;
        if (found.has("_header.json")) {
            try (InputStream in = store.read(headerKey)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> h = objectMapper.readValue(in, LinkedHashMap.class);
                entry = h;
            } catch (Exception ex) {
                log.debug("Unreadable header for {}: {}", id, ex.getMessage());
            }
        }
        if (entry == null) {
            // No sidecar (analysis written by an older build, or the sidecar was lost).
            // Recover it by streaming the header fields out of the analysis file and
            // writing the sidecar back, so this costs the full parse only once. Skipping
            // instead would silently drop the JAR from the dashboard.
            for (var src : List.of(paths.correctedKey(id), paths.analysisKey(id))) {
                if (!found.has(src.leaf())) continue;
                try {
                    entry = readHeaderOnly(src);
                } catch (IOException ex) {
                    log.debug("Unreadable analysis for {}: {}", id, ex.getMessage());
                }
                if (entry != null) break;
            }
            if (entry == null) return null;            // nothing listable here
            try (OutputStream out = openWrite(headerKey)) {
                objectMapper.writeValue(out, entry);
            } catch (IOException ignore) { /* cache miss next time, not fatal */ }
        }
        entry.putIfAbsent("analysisMode",
                found.has("analysis_corrected.json") ? "CORRECTED" : "STATIC");
        entry.put("id", id);
        entry.putIfAbsent("jarName", id + (paths instanceof WarDataPaths ? ".war" : ".jar"));
        entry.put("hasPerJarConfig", found.has("domain-config.json"));
        entry.put("hasConnections", found.has("connections.json"));
        entry.putIfAbsent("claudeStatus", "IDLE");
        return entry;
    }

    /**
     * Every JAR the store knows about. Backed entirely by {@link #listJarsFromStore()}: one prefix
     * listing rather than a directory walk plus a stat per candidate file, which is what made this
     * slow — and what made it wrong once writes stopped landing on the local disk.
     */
    public List<Map<String, Object>> listJars() {
        return listJarsFromStore();
    }

    /**
     * The same listing, but a storage failure <b>propagates</b> instead of answering empty.
     *
     * <p>Used by the {@code GET /api/jar/jars} and {@code GET /api/war/wars} endpoints so a browser
     * can never mistake "the store could not be read" for "you have no analyses": the failure is
     * raised as {@link StorageListingException} and mapped to a non-2xx body by the web layer. The
     * lenient {@link #listJars()} remains for internal callers that want a degraded screen.
     */
    public List<Map<String, Object>> listJarsStrict() {
        try {
            return scanJarsFromStore();
        } catch (StorageListingException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Store-backed listJars failed: {}", e.toString());
            throw new StorageListingException(
                    "Listing analyses from the artifact store failed: " + e, e);
        }
    }

    private Map<String, Object> readHeaderOnly(com.analyzer.storage.ArtifactKey key)
            throws IOException {
        try (InputStream in = openRead(key);
             com.fasterxml.jackson.core.JsonParser parser =
                     objectMapper.getFactory().createParser(in)) {
            Map<String, Object> entry = new LinkedHashMap<>();
            if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) return null;

            while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();

                switch (field) {
                    case "jarName" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("jarName", parser.getText());
                    }
                    case "projectName" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("projectName", parser.getText());
                    }
                    case "jarSize" -> entry.put("jarSize", parser.getLongValue());
                    case "analyzedAt" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("analyzedAt", parser.getText());
                    }
                    case "totalClasses" -> entry.put("totalClasses", parser.getIntValue());
                    case "totalEndpoints" -> entry.put("totalEndpoints", parser.getIntValue());
                    case "analysisMode" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("analysisMode", parser.getText());
                    }
                    case "correctionAppliedAt" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("correctionAppliedAt", parser.getText());
                    }
                    case "correctionCount" -> entry.put("correctionCount", parser.getIntValue());
                    case "claudeIteration" -> entry.put("claudeIteration", parser.getIntValue());
                    case "classes", "endpoints" -> {
                        parser.skipChildren();
                        if (entry.containsKey("jarName") && entry.containsKey("totalEndpoints")) return entry;
                    }
                    default -> parser.skipChildren();
                }
            }
            return entry.isEmpty() ? null : entry;
        }
    }

    /* ---- Resource files ---- */

    public void storeResourceFiles(String jarId, Map<String, String> resources) {
        if (resources == null || resources.isEmpty()) return;
        try {
            var dir = paths.resourcesDirKey(jarId);
            for (Map.Entry<String, String> entry : resources.entrySet()) {
                // Names come from inside the JAR, so they are untrusted: keep the traversal guard.
                if (entry.getKey().contains("..")) continue;
                byte[] body = entry.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                try (OutputStream out = openWrite(dir.resolve(entry.getKey()), body.length)) {
                    out.write(body);
                }
            }
            log.info("Stored {} resource file(s) for {}", resources.size(), jarId);
        } catch (IOException e) {
            log.warn("Failed to store resource files for {}: {}", jarId, e.getMessage());
        }
    }

    public List<String> listResourceFiles(String jarId) {
        var dir = paths.resourcesDirKey(jarId);
        try (var stream = store.list(dir, false)) {
            return stream.filter(e -> !e.directory())
                    .map(e -> e.key().leaf())
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
        } catch (IOException e) {
            log.warn("Failed to list resource files for {}: {}", jarId, e.getMessage());
            return Collections.emptyList();
        }
    }

    public String loadResourceFile(String jarId, String filename) throws IOException {
        if (filename.contains("..")) return null;
        var key = paths.resourcesDirKey(jarId).resolve(filename);
        if (!store.exists(key)) return null;
        try (InputStream in = openRead(key)) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /* ---- Bundled JAR info ---- */

    public void storeBundledJarInfo(String jarId, List<JarParserService.BundledJarInfo> jars) {
        if (jars == null || jars.isEmpty()) return;
        try {
            paths.ensureJarRoot(jarId);
            Path dest = paths.jarRoot(jarId).resolve("bundled-jars.json");
            try (java.io.OutputStream out = openWrite(paths.bundledJarsKey(jarId))) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, jars);
            }
            log.info("Stored bundled-jars.json for {} ({} JARs)", jarId, jars.size());
        } catch (IOException e) {
            log.warn("Failed to store bundled-jars.json for {}: {}", jarId, e.getMessage());
        }
    }

    public List<JarParserService.BundledJarInfo> loadBundledJarInfo(String jarId) {
        var key = paths.bundledJarsKey(jarId);
        if (!store.exists(key)) return Collections.emptyList();
        try (InputStream in = openRead(key)) {
            return objectMapper.readValue(in,
                    new TypeReference<List<JarParserService.BundledJarInfo>>() {});
        } catch (Exception e) {
            log.warn("Failed to load bundled-jars.json for {}: {}", jarId, e.getMessage());
            return Collections.emptyList();
        }
    }

    /* ---- JAR dependency map ---- */

    public void storeJarDepsMap(String jarId, Map<String, Set<String>> depMap) {
        if (depMap == null || depMap.isEmpty()) return;
        try {
            paths.ensureJarRoot(jarId);
            try (java.io.OutputStream out = openWrite(paths.jarDepMapKey(jarId))) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, depMap);
            }
            log.info("Stored jar-dep-map.json for {} ({} entries)", jarId, depMap.size());
        } catch (IOException e) {
            log.warn("Failed to store jar-dep-map.json for {}: {}", jarId, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Set<String>> loadJarDepsMap(String jarId) {
        var key = paths.jarDepMapKey(jarId);
        if (!store.exists(key)) return Collections.emptyMap();
        try (InputStream in = openRead(key)) {
            return objectMapper.readValue(in,
                    new TypeReference<Map<String, Set<String>>>() {});
        } catch (Exception e) {
            log.warn("Failed to load jar-dep-map.json for {}: {}", jarId, e.getMessage());
            return Collections.emptyMap();
        }
    }

    /* ---- Delete ---- */

    public boolean delete(String jarName) throws IOException {
        var root = paths.jarRootKey(jarName);
        return store.deletePrefix(root) > 0;
    }
}
