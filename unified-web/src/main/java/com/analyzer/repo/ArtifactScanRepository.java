package com.analyzer.repo;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.AnalysisDataProvider;
import com.jaranalyzer.service.AnalysisDataProviderFactory;
import com.jaranalyzer.service.JarDataPaths;
import com.plsqlanalyzer.web.page.JsonArrayRows;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.AnalysisReaderFactory;
import com.plsqlanalyzer.web.parser.service.StaticAnalysisReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@code local} / {@code s3} {@link AnalysisRepository}: today's JSON artifacts, read through
 * the frozen {@link ArtifactStore} and nothing else.
 *
 * <p><b>One implementation serves both modes</b> because it is written against {@code ArtifactStore}
 * alone and therefore cannot tell a local file from an S3 object — the same property that made the
 * S3 backend a single class. There is no {@code if (local)} anywhere here, and there must never be
 * one: the moment this class can distinguish them, {@code s3} mode starts diverging from the mode
 * that gets tested.
 *
 * <p><b>It is an extraction, not a rewrite.</b> Every method below performs the same reads, in the
 * same order, through the same readers as the controller that used to call them directly. That is
 * deliberate and it is the acceptance criterion: PLAN-09 Increment C changes no endpoint's response
 * by a single byte, so a "while we're here" improvement to a read path belongs in a different
 * commit where a difference in output is the point rather than a defect. {@link #indexed()} returns
 * {@code false} to say plainly that these answers come from scanning.
 *
 * <p><b>Jackson stays streaming.</b> {@link #rows} never materialises a document —
 * {@code JsonArrayRows} walks the array element by element off a {@code JsonParser} over a store
 * stream, so peak heap is one row. A real {@code analysis.json} reached 912 MB; the class tree is
 * projected out of it without ever holding it. {@link #document} is only used for the facets whose
 * unpaged response is still served whole and which are bounded per analysis (largest observed
 * ~307 KB).
 */
public class ArtifactScanRepository implements AnalysisRepository {

    private static final Logger log = LoggerFactory.getLogger(ArtifactScanRepository.class);

    /**
     * PL/SQL-side reads ({@code PARSE_ROOT}-rooted keys: {@link #analyses}, {@link #scanNodes}).
     * This repository answers BOTH PL/SQL and JAR dataset queries (see {@link #exists}/{@link
     * #rows} below) through the one scanning implementation, so — unlike most consumers — it
     * genuinely needs both {@code ArtifactStore} beans rather than picking one.
     */
    private final ArtifactStore parserStore;
    /** JAR-side reads (bundled-jars key, class-tree cache — both under {@code jarPaths}' root). */
    private final ArtifactStore jarStore;
    private final ObjectMapper mapper;

    /** PL/SQL facets: the very readers the controllers called, so the bytes cannot differ. */
    private final AnalysisReaderFactory readerFactory;
    private final StaticAnalysisReader staticReader;

    /** JAR side. */
    private final AnalysisDataProviderFactory providerFactory;
    private final JarDataPaths jarPaths;

    public ArtifactScanRepository(ArtifactStore parserStore,
                                  ArtifactStore jarStore,
                                  ObjectMapper mapper,
                                  AnalysisReaderFactory readerFactory,
                                  StaticAnalysisReader staticReader,
                                  AnalysisDataProviderFactory providerFactory,
                                  JarDataPaths jarPaths) {
        this.parserStore = parserStore;
        this.jarStore = jarStore;
        this.mapper = mapper;
        this.readerFactory = readerFactory;
        this.staticReader = staticReader;
        this.providerFactory = providerFactory;
        this.jarPaths = jarPaths;
    }

    @Override
    public boolean indexed() {
        return false;
    }

    /* ─────────────────────────── analyses ─────────────────────────── */

    /**
     * Every PL/SQL analysis in the store, newest first.
     *
     * <p>ONE {@code statPrefix} call for the whole tree, then an mtime sort. Both halves are moved
     * verbatim from {@code AnalysisService}: the directory walk they replaced probed
     * {@code Files.isRegularFile} per candidate directory, ~10 µs locally but ~25 ms per HEAD on
     * object storage — minutes for a few thousand analyses.
     *
     * <p>An unreadable listing is logged and answered with an empty list rather than thrown,
     * because that is what the picker did before and a history screen that 500s because one
     * analysis is mid-write is worse than one that shows the rest.
     */
    @Override
    public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) throws IOException {
        if (dataset != AnalysisQuery.Dataset.PLSQL_ANALYSES) {
            throw new IllegalArgumentException("analyses() does not serve " + dataset);
        }
        Map<String, ArtifactKey> byName = new LinkedHashMap<>();
        try {
            for (ArtifactKey k : parserStore.statPrefix(ArtifactKey.of(AnalysisPaths.PARSE_ROOT)).keySet()) {
                List<String> segs = k.segments();
                int n = segs.size();
                if (n >= 4 && "index.json".equals(segs.get(n - 1)) && "api".equals(segs.get(n - 2))) {
                    byName.put(AnalysisPaths.nameOfKey(k.parent().parent()), k);
                }
            }
        } catch (IOException e) {
            log.warn("[repo] listing PL/SQL analyses failed: {}", e.getMessage());
            return List.of();
        }

        List<AnalysisRef> refs = new ArrayList<>(byName.size());
        for (Map.Entry<String, ArtifactKey> e : byName.entrySet()) {
            refs.add(new AnalysisRef(e.getKey(), e.getValue(), parserStore.lastModified(e.getValue())));
        }
        // Newest first. The timestamp used to be the directory mtime; object stores have no
        // directories, so the index.json mtime stands in — it is written last in every run, so the
        // ordering is the same.
        refs.sort(Comparator.comparingLong(AnalysisRef::lastModified).reversed());
        return refs;
    }

    /* ─────────────────────────── exists ─────────────────────────── */

    @Override
    public boolean exists(AnalysisQuery query) {
        return switch (query.dataset()) {
            case JAR_BUNDLED_JARS -> jarStore.exists(jarPaths.bundledJarsKey(query.analysis()));
            // The PL/SQL facets are synthesized from a node scan rather than stored as a document,
            // so "does the artifact exist" is not a question with an answer for them; their
            // absent case is the reader returning a null/empty node, which is what their endpoints
            // already branch on. Answering `true` here would be a lie the caller could act on.
            default -> throw new UnsupportedOperationException(
                    "exists() is not defined for " + query.dataset()
                    + "; this facet is synthesized, not stored");
        };
    }

    /* ─────────────────────────── documents ─────────────────────────── */

    /**
     * Delegates to the reader the controller used to call, chosen the same way.
     *
     * <p>The four node-scan facets go through {@code AnalysisReaderFactory}, so an analysis switched
     * to the Claude view keeps answering from the Claude reader. The two table-detail facets are
     * bound to the static reader instead — deliberately, and for the reason
     * {@code AnalysisService#getTableJoins} states: these rows come from the parsed node documents
     * and the Claude reader's only contribution would be to re-merge documents this call exists to
     * avoid reading. Preserving that asymmetry is preserving the response.
     */
    @Override
    public JsonNode document(AnalysisQuery query) throws IOException {
        String name = query.analysis();
        return switch (query.dataset()) {
            case PLSQL_JOINS -> readerFactory.getReader(name).getJoins(name);
            case PLSQL_CURSORS -> readerFactory.getReader(name).getCursors(name);
            case PLSQL_SEQUENCES -> readerFactory.getReader(name).getSequences(name);
            case PLSQL_DYNAMIC_SQL -> readerFactory.getReader(name).getDynamicSql(name);
            case PLSQL_PROCEDURES -> readerFactory.getReader(name).getProcedures(name);
            // Static reader, matching getTables' own binding: the tables index is a crawler output,
            // not a node scan, and the Claude reader has nothing to add to it.
            case PLSQL_TABLES -> staticReader.getTables(name);
            case PLSQL_TABLE_JOINS -> staticReader.getTableJoins(name, query.table());
            case PLSQL_TABLE_COLUMNS -> staticReader.getTableColumnRefs(name, query.table());
            case PLSQL_TABLE_COLUMN_DETAIL -> staticReader.getTableColumnDetail(name, query.table());
            default -> throw new UnsupportedOperationException(
                    "document() is not defined for " + query.dataset() + "; use rows()");
        };
    }

    /* ─────────────────────────── rows ─────────────────────────── */

    @Override
    public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection)
            throws IOException {
        return switch (query.dataset()) {
            case JAR_CLASS_TREE -> classTreeRows(query, projection);
            case JAR_BUNDLED_JARS -> arrayRows(jarPaths.bundledJarsKey(query.analysis()),
                                               null, projection);
            default -> throw new UnsupportedOperationException(
                    "rows() is not defined for " + query.dataset() + "; use document()");
        };
    }

    /**
     * The class tree, paged out of a sidecar cache rather than out of {@code analysis.json}.
     *
     * <p><b>The cache is this implementation's business, not the interface's.</b> Without it page 2
     * would re-stream the whole multi-megabyte analysis document to reach the cursor; with it every
     * page after the first is a scan of a small projected array. An indexed implementation needs
     * none of this, which is exactly why it lives here and not in the contract.
     *
     * <p>The cache is invalidated on re-analysis by {@code PersistenceService.invalidateSummaryCache}
     * and validated here by its last byte. A cursor survives a rebuild anyway, because it is keyed
     * on the class's package and name rather than on an array position.
     */
    private RowStream classTreeRows(AnalysisQuery query, Function<JsonNode, JsonNode> projection)
            throws IOException {
        AnalysisDataProvider provider = providerFactory.resolve(query.analysis(), query.version());
        ArtifactKey cache = jarPaths.classTreeCacheKey(query.analysis(), query.version());
        if (!isCompleteJsonArray(cache)) {
            // Streamed into the store, never buffered: ArtifactStore.write is atomic on close, so a
            // concurrent reader sees the previous complete cache or the new one, never a partial.
            try (var parser = mapper.getFactory().createParser(provider.openStream());
                 var out = new java.io.BufferedOutputStream(jarStore.write(cache), 128 * 1024);
                 var gen = mapper.getFactory().createGenerator(out)) {
                provider.streamClassTree(parser, gen);
                gen.flush();
            }
        }
        return arrayRows(cache, null, projection);
    }

    /**
     * Opens a stored JSON array as a lazy row stream.
     *
     * <p>The parser is closed by the returned stream, which closes the underlying store stream in
     * turn — so an abandoned page still releases the S3 connection.
     */
    private RowStream arrayRows(ArtifactKey key, String arrayField,
                                Function<JsonNode, JsonNode> projection) throws IOException {
        // Both call sites below address JAR-side keys (bundled-jars, class-tree cache).
        InputStream in = jarStore.read(key);
        try {
            var parser = mapper.getFactory().createParser(in);
            JsonArrayRows rows = JsonArrayRows.of(parser, mapper, arrayField, projection);
            return RowStream.of(rows, rows);
        } catch (IOException | RuntimeException e) {
            // The parser never took ownership, so this method owns closing the stream.
            try {
                in.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /* ═════════════ PLAN-09 Increment E — the same entity questions, by scanning ═════════════ */

    /**
     * PLAN-08 Q43b, answered the only way an artifact store can answer it: <b>read every node
     * document of the analysis</b>.
     *
     * <p>This exists to be the honest baseline. Increment E's claim is that entities plus an index
     * beat a scan, and a claim like that is worth nothing without the scan actually implemented and
     * measured — six plan steps have already been rejected on measurement. So this is not a stub and
     * not deliberately slow: it uses the same streaming mapper the Mongo ingest uses, over the same
     * bytes, through the same {@link ArtifactStore}. The only difference is that it does it on every
     * call and Mongo does it once.
     *
     * <p>It is also the correctness oracle. The two implementations must return the same rows for
     * the same question, which is checkable precisely because they share
     * {@link com.analyzer.storage.entity.PlsqlEntityMapper} — a second parser over the same bytes
     * would be a second thing to be wrong.
     *
     * <p>{@link #indexed()} stays {@code false}, so a caller already knows this answer cost a scan.
     */
    @Override
    public java.util.List<com.analyzer.storage.entity.TableUsageRow> nodesUsingTable(
            String analysis, String table, String usage) throws IOException {
        String key = com.analyzer.storage.entity.TableUsageRow.keyOf(null, table);
        if (key == null) return java.util.List.of();
        boolean qualified = key.indexOf('.') >= 0;
        String suffix = "." + key;
        java.util.List<com.analyzer.storage.entity.TableUsageRow> out = new ArrayList<>();
        scanNodes(analysis, com.analyzer.storage.entity.PlsqlEntityMapper.sinkOf(null, null, row -> {
            if (usage != null && !usage.isBlank() && !usage.equals(row.usage())) return;
            String k = row.tableKey();
            if (qualified ? k.equals(key) : (k.equals(key) || k.endsWith(suffix))) out.add(row);
        }));
        out.sort(Comparator.comparing(com.analyzer.storage.entity.TableUsageRow::nodeId));
        return out;
    }

    /** PLAN-08 Q58, by scanning. Stops at the first match — the node id is the file name, so this
     *  is usually one read, which is why the entity index wins far less here than on Q43b. */
    @Override
    public java.util.Optional<com.analyzer.storage.entity.FlowNodeRow> flowNode(
            String analysis, String nodeId) throws IOException {
        java.util.List<com.analyzer.storage.entity.FlowNodeRow> found = new ArrayList<>();
        scanNodes(analysis, com.analyzer.storage.entity.PlsqlEntityMapper.sinkOf(
                row -> { if (row.nodeId().equals(nodeId)) found.add(row); }, null, null));
        return found.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(found.get(0));
    }

    /** PLAN-08 Q60, by scanning. */
    @Override
    public java.util.List<com.analyzer.storage.entity.FlowEdgeRow> edgesFrom(
            String analysis, String nodeId) throws IOException {
        java.util.List<com.analyzer.storage.entity.FlowEdgeRow> out = new ArrayList<>();
        scanNodes(analysis, com.analyzer.storage.entity.PlsqlEntityMapper.sinkOf(null,
                row -> { if (row.fromNodeId().equals(nodeId)) out.add(row); }, null));
        out.sort(Comparator.comparing(com.analyzer.storage.entity.FlowEdgeRow::toNodeId));
        return out;
    }

    /**
     * Streams every {@code api/nodes/*.json} of an analysis through the entity mapper.
     *
     * <p>One document open at a time and one JSON element at a time inside it, so peak heap is a
     * table entry rather than an analysis. That matters even in the baseline: the point of the
     * measurement is index-seek versus scan, and a scan that also blew the heap would be a straw
     * man.
     */
    private void scanNodes(String analysis,
                           com.analyzer.storage.entity.PlsqlEntityMapper.RowSink sink)
            throws IOException {
        var entityMapper = new com.analyzer.storage.entity.PlsqlEntityMapper(mapper);
        ArtifactKey nodesDir = ArtifactKey.of(AnalysisPaths.PARSE_ROOT)
                .resolve(analysis, "api", "nodes");
        List<ArtifactKey> keys = new ArrayList<>();
        try (var s = parserStore.list(nodesDir, false)) {
            s.filter(e -> !e.directory())
             .filter(e -> e.key().toString().endsWith(".json"))
             .forEach(e -> keys.add(e.key()));
        }
        for (ArtifactKey key : keys) {
            try (InputStream in = parserStore.read(key)) {
                entityMapper.map(analysis, in, sink);
            }
        }
    }

    /**
     * A stored JSON array is complete when its last byte is {@code ]}.
     *
     * <p>One ranged read, not a {@code RandomAccessFile}: one byte is transferred either way and it
     * works against an object store (an HTTP Range request) as well as a local file. Same check the
     * summary cache makes for its trailing {@code &#125;}. Only ever called against the (JAR-side)
     * class-tree cache key.
     */
    private boolean isCompleteJsonArray(ArtifactKey key) {
        try {
            long size = jarStore.size(key);
            if (size < 2) return false;
            try (InputStream in = jarStore.read(key, size - 1, 1)) {
                return in.read() == ']';
            }
        } catch (IOException e) {
            return false;
        }
    }
}
