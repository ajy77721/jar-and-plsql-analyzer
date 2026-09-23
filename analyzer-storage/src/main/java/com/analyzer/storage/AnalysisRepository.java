package com.analyzer.storage;

import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Queries over analysis entities. <b>Additive</b> — {@link ArtifactStore} is unchanged and every
 * existing caller keeps working without touching this. A caller that wants bytes uses the store;
 * a caller that wants an answer uses this.
 *
 * <p>PLAN-09 Increment C. It is an <b>extraction</b>, not a construction: Increment A already
 * built the scan-based query path (streaming filter/order/page with a bounded heap, exact totals,
 * a cursor contract) and spread it across controllers and readers with no interface over it. This
 * is that interface. {@code local} and {@code s3} are served by one implementation written against
 * {@link ArtifactStore} alone, so it cannot tell them apart — the same property that made the S3
 * backend a single class. {@code mongodb} gets a second implementation in Increment D.
 *
 * <h2>Why the rows are {@link JsonNode} and not typed records</h2>
 *
 * PLAN-07 §2.2 sketched ~20 methods over ~15 new record types ({@code EndpointSummary},
 * {@code FlowNodeDetail}, {@code JoinRef}, …). They are deliberately <b>not</b> here, for two
 * reasons that only became visible once Increment A shipped:
 *
 * <ol>
 *   <li><b>Increment C's acceptance criterion is that no endpoint's response changes, byte for
 *       byte.</b> Today's responses <em>are</em> these JSON documents. Mapping them through
 *       records and back is precisely where a field order, a numeric widening or an absent-vs-null
 *       distinction changes — so records would put the one thing this increment must guarantee at
 *       risk in exchange for compile-time names.</li>
 *   <li>They would be designed before any consumer needs them and before the Mongo implementation
 *       exists to disagree with them. An interface shaped by exactly one implementation is wrong
 *       in ways nobody notices until the second one arrives.</li>
 * </ol>
 *
 * Typed rows become worth having in Increment D, where Mongo produces rows that were never a JSON
 * document, and are cheap to add then because every method here is already paged.
 *
 * <h2>Where the paging went</h2>
 *
 * Not here. Filtering, ordering and page selection are pure functions of a request and a row
 * sequence, and Increment A already implements them once, correctly, over a bounded heap
 * ({@code JsonFacetPager}). Duplicating that behind this interface would create a second
 * implementation of the cursor contract, and a resume comparison that can disagree with the
 * ordering that produced it is exactly how a paged list starts skipping rows on page 2. So this
 * interface answers <b>where the rows come from</b>; the pager stays the single owner of
 * <b>which rows the caller gets</b>. {@link #rows} is streaming precisely so the pager keeps its
 * bounded-heap guarantee — a real {@code analysis.json} reached 912 MB.
 *
 * <h2>Methods deliberately NOT on this interface</h2>
 *
 * Each one from PLAN-07 §2.2 that has no consumer today, with the reason. <b>Add when a consumer
 * exists</b> — the trimmed list is as much a part of the design as the kept one:
 *
 * <ul>
 *   <li>{@code boolean anySessionRunning(Scope)} — <b>no storage behind it.</b> PLAN-07 assumed the
 *       session list was an artifact. {@code GlobalSessionsController#anyRunning} already reduced
 *       the 30-second poll to a boolean over the in-memory session managers, so the waste this
 *       method was designed to remove is gone, and routing a process-local fact through a storage
 *       repository would add indirection over nothing. Mongo cannot answer it either.</li>
 *   <li>{@code Page<LogLine> logLines(LogScope, LogFilter, PageRequest)} — the live application log
 *       is local by design ({@code /storage-check}'s {@code logging} category: rolled logs are
 *       shipped to the store, the live file is not), and the conversion job log/live streams are an
 *       in-memory {@code CopyOnWriteArrayList} in the controller. {@code LogTailReader} and
 *       {@code LineBufferTail} are already the extracted seam for both, already in this module,
 *       already returning {@link com.analyzer.storage.page.Page}. The one genuinely
 *       artifact-backed append-only stream, {@code claude/_call_log.jsonl}, is likewise already
 *       read through {@code ArtifactStore} into a {@code Page} by {@code ClaudeCallLogger}, as is
 *       {@code claude-chatbot/*.json} by {@code ChatTranscriptReader}. Folding them in would add
 *       row DTOs without changing where a single byte comes from.</li>
 *   <li>{@code Page<EndpointSummary> endpoints(...)} / {@code Optional<EndpointSummary> endpoint} —
 *       the live paged endpoint list is produced by a single Jackson pass that computes the page,
 *       the exact match count and cursor <em>resolution</em> inside one state machine
 *       ({@code AbstractAnalysisDataProvider#streamSummaryEndpointPage} /
 *       {@code EndpointPageMark}); the 409 {@code RESULT_SET_REPLACED} contract depends on that.
 *       Expressing it here would mean either returning that one-consumer mark type or buffering the
 *       endpoint array, which is the allocation the streaming pass exists to avoid.</li>
 *   <li>{@code callTreeNodes}, {@code flowNodes}, {@code flowNode}, {@code callees},
 *       {@code callers}, {@code tables}, {@code tableUsages}, {@code endpointsUsingTable},
 *       {@code nodesUsingTable}, {@code facet(AnalysisId, FacetKind, PageRequest)} —
 *       <b>no paged consumer.</b> Their endpoints exist and are unconverted: they return whole
 *       documents and are read whole. PLAN-08 owns converting them, and each conversion is the
 *       moment to add its method, with a screen to validate it against.</li>
 *   <li>{@code Optional<ArtifactKey> blobRef(BlobRef)} — pure key arithmetic with no I/O.
 *       {@code JarDataPaths} and {@code AnalysisPaths} already do exactly this, already return
 *       {@link ArtifactKey}, and are already the only place those keys are built. A second front
 *       door to the same arithmetic invites the two to drift, which is the failure PLAN-09 §8 R2
 *       records as having silently broken WAR storage once already.</li>
 * </ul>
 *
 * <p>Every method here maps to a live paged endpoint. {@link #indexed()} is the honest signal of
 * which implementation answered.
 */
public interface AnalysisRepository {

    /**
     * True when this implementation answers from an index, false when it answers by scanning
     * artifacts.
     *
     * <p>Not decoration: a scan of every {@code api/nodes/*.json} is seconds, an index lookup is
     * milliseconds, and a screen that wants to say "this may take a moment" has no other way to
     * know. Also the one assertion that distinguishes the two implementations in a conformance
     * test that otherwise requires them to agree exactly.
     */
    boolean indexed();

    /**
     * Every analysis of one kind, newest first, as a name plus the key of its index document.
     *
     * <p>Returns references rather than summaries on purpose. Building a summary means reading and
     * interpreting each index document, and the interpretation is the caller's domain model, not
     * storage's — see the {@link JsonNode} note above. The reference is the part only storage can
     * answer, and it is the part Mongo replaces with a collection scan.
     *
     * <p>Bounded by the number of analyses, not by their size, so it is deliberately not paged:
     * the artifacts are unbounded but the analysis count is a directory listing, and today's
     * picker renders all of them.
     */
    List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) throws IOException;

    /**
     * Whether the artifact backing this query exists at all.
     *
     * <p>Separate from an empty result because they are different answers: "this analysis recorded
     * no bundled JARs" is a 204 and "the filter matched nothing" is an empty page, and the tree
     * renders them differently.
     */
    boolean exists(AnalysisQuery query);

    /**
     * The whole facet document for a query, in exactly the shape it has on the wire today.
     *
     * <p>For the facets whose unpaged response is still served — the four node-scan facets and the
     * two table-detail lists — the document IS the legacy response, so it has to be available
     * whole. It is bounded: these are per-analysis facet documents, the largest observed being
     * ~307 KB ({@code api/tables/index.json}). It is NOT a way to read {@code analysis.json};
     * nothing here returns a whole analysis, and {@code LazyAnalysisResult}'s reason for existing
     * (PLAN-03 §4.1) survives untouched.
     *
     * @return the document, or a null/missing node when the analysis has no such facet
     * @throws IOException if the artifact exists but cannot be read
     */
    JsonNode document(AnalysisQuery query) throws IOException;

    /**
     * The rows a query matches, streamed one at a time. <b>The caller must close the stream.</b>
     *
     * <p>The counterpart to {@link #document} for the facets that are too large to hold even once:
     * the class tree projected out of a multi-megabyte {@code analysis.json}, and the 3.03 MB
     * bundled-JAR list whose bodies the tree does not render. Peak heap is one row plus the pager's
     * bounded heap, so ten thousand rows cost what ninety-four do.
     *
     * @param projection applied to each row as it is read, {@code null} for the row as-is. The
     *                   caller owns it, not this interface: which fields a screen needs is a
     *                   presentation decision, and pushing it in here is what keeps the row
     *                   definitions ({@code JarFacetSpecs}) next to the endpoint that declares
     *                   the sorts and filters over the very same fields. Returning {@code null}
     *                   from the projection drops the row, so a caller can filter during the scan
     *                   rather than build a node it is about to discard.
     */
    RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection) throws IOException;

    /* ═══════════════════════ PLAN-09 Increment E — entity queries ═══════════════════════ */

    /**
     * Every flow node that uses a table, with the operation it uses it for. <b>PLAN-08 Q43b / L4.</b>
     *
     * <p><b>This is the method the database exists for.</b> Everything above answers
     * "give me the document behind this screen"; an object store answers those too, and cheaper.
     * This one cannot be answered by a key lookup at all — the fact is spread across every
     * {@code api/nodes/*.json} of the analysis — so the two implementations differ in kind rather
     * than in speed: {@code artifact-scan} reads all 44 node documents of a 69-node analysis and
     * filters, and the Mongo one seeks an index on {@code (analysis, tableKey)}. {@link #indexed()}
     * is how a caller tells which it got, and PLAN-09 Increment E's report measures both.
     *
     * <p>Returned as a list, not a {@link com.analyzer.storage.page.Page}, and deliberately: the
     * result is bounded by the number of nodes that touch <em>one</em> table (the observed maximum
     * over this corpus is well inside a page), and PLAN-08 Q43b's screen renders it whole. Adding
     * paging before a consumer needs it would mean designing a cursor contract against no screen —
     * the mistake {@link AnalysisRepository}'s own "methods deliberately NOT on this interface" list
     * exists to avoid. It becomes paged when a table with thousands of users appears.
     *
     * <h3>Default: refuse, do not scan</h3>
     *
     * <p>The default throws rather than falling back to a scan. An implementation that cannot answer
     * this from entities must say so, because a silent scan behind an "indexed" repository is
     * exactly the shape of PLAN-09 §8 R1 — the operator believes a cutover happened and every query
     * still reads JSON. Both shipped implementations override it.
     *
     * @param analysis the {@code '~'}-joined PL/SQL analysis identity, as {@link AnalysisQuery}
     * @param table    a table name, with or without a schema prefix, in any case. Matched on
     *                 {@link com.analyzer.storage.entity.TableUsageRow#keyOf} — the corpus writes
     *                 the same table as {@code ANALYZERDB.ERP_AUDIT_LOG} in {@code tables.json} and
     *                 as {@code erp_audit_log} inside a node, so a case-sensitive match would answer
     *                 one spelling and miss the other
     * @param usage    {@link com.analyzer.storage.entity.TableUsageRow#DIRECT} for the nodes whose
     *                 own statements touch the table, {@code SUBTREE} for everything that reaches
     *                 it transitively, {@code null} for both. Not a boolean: they are different
     *                 questions and a screen asks one of them
     */
    default List<com.analyzer.storage.entity.TableUsageRow> nodesUsingTable(
            String analysis, String table, String usage) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not implement the PLAN-04 entity model; "
                + "nodesUsingTable cannot be answered from opaque payloads");
    }

    /**
     * One flow node's detail row. <b>PLAN-08 Q58 / L21.</b>
     *
     * <p>Keyed on {@code (analysis, nodeId)} and never on {@code depth} or an ordinal: PLAN-11
     * rule 5, the crawl is non-deterministic and re-running renumbers both.
     */
    default java.util.Optional<com.analyzer.storage.entity.FlowNodeRow> flowNode(
            String analysis, String nodeId) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not implement the PLAN-04 entity model");
    }

    /**
     * The calls out of one node. <b>PLAN-08 Q60 / L23</b>, the server side of a join the browser
     * does today.
     *
     * <p>Sourced from {@code calls[]} rather than {@code edges/*.json} because only {@code calls[]}
     * carries {@code type} — see {@link com.analyzer.storage.entity.FlowEdgeRow}.
     */
    default List<com.analyzer.storage.entity.FlowEdgeRow> edgesFrom(
            String analysis, String nodeId) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not implement the PLAN-04 entity model");
    }
}
