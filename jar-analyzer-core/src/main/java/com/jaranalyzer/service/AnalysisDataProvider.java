package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public interface AnalysisDataProvider {

    /**
     * Opens the backing analysis document for streaming.
     *
     * <p>This is the storage-neutral way to read the analysis. A caller that holds a stream works
     * unchanged whether the bytes come from a local file or a remote object store; a caller that
     * holds a {@code Path} does not — which is why no {@code Path} accessor exists here any more.
     *
     * <p>The caller owns the stream and must close it. Passing it to
     * {@code JsonFactory.createParser(InputStream)} transfers ownership to the parser, so
     * try-with-resources on the parser is sufficient.
     *
     * <p>Never buffers the document — it can be far larger than heap.
     */
    InputStream openStream() throws IOException;

    /** Last-modified time of the backing document in millis since epoch, or -1 if absent. */
    long lastModifiedMillis();

    /** True if the backing document exists. */
    boolean exists();

    /** Storage key of the backing analysis document. The document's identity. */
    com.analyzer.storage.ArtifactKey getDocumentKey();

    String getProviderType();

    /** Storage key of this provider's pre-generated summary cache. */
    com.analyzer.storage.ArtifactKey getSummaryCacheKey();

    boolean shouldComputeVerification();

    void streamSummary(JsonParser parser, JsonGenerator gen) throws IOException;

    void streamAnalysis(JsonParser parser, JsonGenerator gen) throws IOException;

    void streamCallTree(JsonParser parser, JsonGenerator gen, int endpointIdx) throws IOException;

    /**
     * The same call tree as the flat DAG rather than the expanded nested tree.
     *
     * <p>Expansion is exponential in shared subtrees. Measured on endpoint 271 of
     * {@code unified-web-1.0.0-SNAPSHOT}: 300,167,238 nodes expanded against <b>2,282</b> reachable
     * DAG nodes (7.4 MB). A node budget can only decide how much of an unusable answer to send;
     * this changes the shape of the answer.
     *
     * <p>Defaults to {@code null} so a provider that has no node dictionary — a legacy document —
     * tells the caller to use the expanded endpoint instead of returning an empty tree.
     */
    default void streamCallTreeDag(JsonParser parser, JsonGenerator gen, int endpointIdx)
            throws IOException {
        gen.writeNull();
    }

    /**
     * Stream the overflow children for one fragment of one endpoint's call tree — the "expand N
     * more calls" affordance's data source. Writes {@code null} if the endpoint/fragment isn't
     * found. See {@link com.jaranalyzer.model.CallNode#getFragmentRef()}.
     */
    void streamFragment(JsonParser parser, JsonGenerator gen, int endpointIdx, String fragmentId) throws IOException;

    void streamClassTree(JsonParser parser, JsonGenerator gen) throws IOException;

    /**
     * Class tree with member arrays optionally omitted.
     *
     * <p>{@code methodSummaries} and {@code fields} are <b>93.8%</b> of this response — measured
     * 8,174,421 B and 3,551,050 B of 11,834,160 B for 1,623 classes — and the initial tree paints
     * only class-level rows; members are rendered when a class is expanded. Omitting them takes the
     * payload to ~760 KB.
     *
     * <p>{@code methodCount} and {@code fieldCount} are still emitted, so the counts on each row
     * stay correct: a lean response must not turn into a wrong number, which is the mistake the
     * capped arrays made (PLAN-12 §4.6).
     *
     * @param includeMembers false to omit the member arrays; true reproduces the full response
     *                       byte-for-byte
     */
    default void streamClassTree(JsonParser parser, JsonGenerator gen, boolean includeMembers)
            throws IOException {
        streamClassTree(parser, gen);
    }

    void streamClassByIndex(JsonParser parser, JsonGenerator gen, int idx) throws IOException;

    /**
     * One class by ordinal, served by seeking the offsets sidecar (plan 2a adoption) instead of
     * streaming the whole document to reach position {@code idx}.
     *
     * <p>Writes exactly what {@link #streamClassByIndex} writes — including a JSON {@code null} for
     * an out-of-range index — and returns {@code true} once it has written it. Returns {@code false}
     * <b>without writing anything</b> when there is no usable offsets sidecar (an older analysis, a
     * partial write, a corrupt index), so the caller can fall back to the legacy scan on the same
     * generator. A read failure is the same contract: nothing is written, {@code false} is returned.
     *
     * <p>Kept a default so a provider implementation that predates the sidecar is untouched.
     */
    default boolean streamClassByIndexSeek(JsonGenerator gen, int idx) throws IOException {
        return false;
    }

    /**
     * One page of the endpoint list, served from the {@code endpoints[]} offsets (plan 2a adoption)
     * rather than a full parse of the document per page.
     *
     * @return the same mark {@link #streamSummaryEndpointPage} returns, or {@code null} when there
     *         is no usable offsets sidecar and the caller must fall back to the legacy scan. On
     *         {@code null} the method may have written nothing; a caller that buffers can discard
     *         its buffer and replay the legacy path.
     */
    default AbstractAnalysisDataProvider.EndpointPageMark streamSummaryEndpointPageSeek(
            JsonGenerator gen, String afterId, int limit, String query) throws IOException {
        return null;
    }

    /**
     * Stream a filtered slice of the summary: either headers-only (excluding heavy fields)
     * or a specific field extracted from endpoints.
     *
     * @param summaryParser parser positioned at the cached summary JSON
     * @param gen output generator
     * @param mode "headers" = full summary minus heavy endpoint fields;
     *             "slice" = extract only the specified fields from endpoints
     * @param fields for "slice" mode: the endpoint field names to extract (e.g., "externalCalls", "dynamicFlows")
     */
    void streamSummarySlice(JsonParser summaryParser, JsonGenerator gen,
                            String mode, Set<String> fields) throws IOException;

    /**
     * Q21 — the summary's top-level scalars only, for the JAR header bar.
     *
     * <p>A fixed few hundred bytes whatever the JAR contains: every value whose size depends on the
     * analysis (the endpoint list, the class index) is skipped without being materialised. The
     * header used to cost the whole summary — 430 KB measured, 120 KB through
     * {@code /summary/headers} — to render eleven numbers and strings.
     */
    void streamSummaryHeader(JsonParser summaryParser, JsonGenerator gen) throws IOException;

    /**
     * Q21b — one page of the endpoint list, in the same item shape as headers mode.
     *
     * @param afterId resume after this {@code endpointId}; null starts at the beginning
     * @param limit   maximum items, or -1 for the whole array (the legacy shape)
     * @param query   case-insensitive search across method, path, controller and handler name
     */
    AbstractAnalysisDataProvider.EndpointPageMark streamSummaryEndpointPage(
            JsonParser summaryParser, JsonGenerator gen, String afterId, int limit, String query)
            throws IOException;
}
