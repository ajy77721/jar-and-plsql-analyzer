package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.plsql.NodeDetailMapper;
import com.codeanalyzer.ingest.mongo.map.jar.AnalysisJsonMapper;
import com.codeanalyzer.ingest.mongo.map.jar.DagAndEndpointMapper;
import com.codeanalyzer.ingest.mongo.map.jar.BundledJarMapper;
import com.codeanalyzer.ingest.mongo.map.jar.JarDependencyMapper;
import com.codeanalyzer.ingest.mongo.map.plsql.CallGraphMapper;
import com.codeanalyzer.ingest.mongo.map.plsql.ChunkStatementMapper;
import com.codeanalyzer.ingest.mongo.map.plsql.EdgesMapper;
import com.codeanalyzer.ingest.mongo.map.plsql.StatementsMapper;
import com.codeanalyzer.ingest.mongo.map.plsql.TablesIndexMapper;
import com.codeanalyzer.ingest.mongo.map.plsql.TablesJsonMapper;
import com.codeanalyzer.ingest.mongo.schema.SchemaInstaller;
import com.codeanalyzer.ingest.mongo.schema.SchemaMeta;
import com.codeanalyzer.ingest.mongo.write.BulkWriter;
import com.codeanalyzer.ingest.mongo.write.EndpointReachability;
import com.codeanalyzer.ingest.mongo.write.FacetCounter;
import com.codeanalyzer.ingest.mongo.write.RefAssertion;
import com.codeanalyzer.ingest.mongo.write.RunMarker;
import com.codeanalyzer.ingest.mongo.write.SourceDigest;
import com.codeanalyzer.ingest.mongo.write.WriteStats;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Ingests one analysis into MongoDB.
 *
 * <h2>The ordering, and why there is only one constraint</h2>
 *
 * <p>Every reference is a computed string, so children do not need their parents to exist first —
 * a child can be written before, after, or concurrently with its parent and the link is still
 * correct. The single ordering requirement is that the <b>run marker is written first</b>, because
 * it is what makes everything else invisible until the run completes.
 *
 * <p>Within a run, mappers run in declaration order only so that {@code TablesIndexMapper} can
 * populate the object lookup that {@code TablesJsonMapper} reads. That is an optimisation, not a
 * correctness requirement: the second mapper falls back to computing the same id itself.
 */
public final class MongoIngestService {

    private static final Logger log = LoggerFactory.getLogger(MongoIngestService.class);

    /** Opens an artifact by its path relative to the analysis root. Null if absent. */
    public interface ArtifactSource {
        InputStream open(String artifactPath) throws IOException;

        /**
         * Lists artifacts under a directory, relative to the analysis root.
         *
         * <p>Needed because {@code api/nodes/} holds one file per node — 356 across the corpus —
         * rather than a single document, so the set cannot be known in advance.
         */
        default List<String> list(String directory) throws IOException {
            return List.of();
        }
    }

    private final MongoDatabase db;
    private final RunMarker marker;

    /** M1 scope. Each later milestone appends its mappers here; nothing else changes. */
    private final List<ArtifactMapper> mappers = List.of(
            new TablesIndexMapper(),
            new TablesJsonMapper());

    /**
     * Runs after the node files, because a statement's {@code nodeRef} and {@code objectRef} are
     * resolved from the lookups the earlier mappers fill. Neither is required for correctness —
     * both fall back to computing the key — but resolving them locally is what keeps the write
     * path free of read-backs.
     */
    private final ArtifactMapper statementsMapper = new StatementsMapper();

    /** Edges run after nodes for the same reason statements do: node refs resolve from the lookup. */
    private final List<ArtifactMapper> postNodeMappers = List.of(
            new StatementsMapper(), new EdgesMapper(), new CallGraphMapper());

    /** Node files are per-node, so they are driven by listing rather than by a fixed path. */
    private static final String NODES_DIR = "api/nodes";
    private final NodeDetailMapper nodeMapper = new NodeDetailMapper();

    /**
     * Chunks are per-procedure files like the node files, so they are listed rather than opened by
     * a fixed path. Run after the nodes so each chunk's {@code nodeRef} resolves from the lookup.
     */
    private static final String CHUNKS_DIR = ChunkStatementMapper.DIRECTORY;
    private final ChunkStatementMapper chunkMapper = new ChunkStatementMapper();

    public MongoIngestService(MongoDatabase db) {
        this.db = db;
        this.marker = new RunMarker(db);
    }

    /** Creates collections, validators and indexes. Idempotent; safe on every start. */
    public void installSchema() {
        new SchemaInstaller(db).install();
    }

    /**
     * @param runId       the run that holds this content — the existing one when skipped
     * @param stats       what was written; all zeroes when skipped
     * @param skipped     true when an identical run was already published and nothing was written
     */
    /**
     * Checks structural references before the run is published, and fails it if any resolve to
     * nothing.
     *
     * <p>Between the last write and {@code complete()} on purpose. The run is still
     * {@code IN_PROGRESS}, so a failure leaves documents no reader can see — recoverable at leisure
     * — rather than a published run with links into nothing.
     */
    private void assertReferencesResolve(String runId) {
        try {
            new RefAssertion(db).assertAllResolve(runId);
        } catch (RuntimeException e) {
            marker.fail(runId, e.toString());
            throw e;
        }
    }

    public record Result(String runId, WriteStats stats, boolean skipped) {
        public Result(String runId, WriteStats stats) {
            this(runId, stats, false);
        }
    }

    /**
     * The published run holding this exact source, if there is one.
     *
     * <p>Only {@code COMPLETE} runs count. A match against an {@code IN_PROGRESS} run would skip an
     * ingest on the strength of one that has not finished and may never finish, leaving the content
     * permanently invisible with nothing scheduled to write it again.
     */
    private Document publishedRunWithDigest(String entryPoint, String digest) {
        return db.getCollection(com.codeanalyzer.ingest.mongo.schema.Collections.RUN)
                .find(new Document("entryPoint", entryPoint)
                        .append("status", RunMarker.COMPLETE)
                        .append(SourceDigest.FIELD, digest)
                        // Same source is not enough. A mapper fix changes what the same bytes
                        // produce, so a run written by an older schema must not satisfy a newer
                        // build — otherwise the fix silently never reaches the database.
                        .append("schemaVersion", SchemaMeta.CURRENT_VERSION))
                .first();
    }

    /**
     * Ingests one analysis.
     *
     * @param dialect  ORACLE / MYSQL / DB2 / SQLSERVER / POSTGRES
     * @param source   opens artifacts by relative path
     */
    /**
     * Ingests a JAR analysis.
     *
     * <p>Deliberately does <b>not</b> read the 321 per-endpoint directories: they were verified to
     * be a strict subset of the global pools {@code analysis.json} already contains, so reading
     * them would turn 49 MB of input into 1.17 GB for nothing.
     *
     * <p>{@code bundled-jars.json} is a different case, read since ING-116: it carries data
     * {@code analysis.json} does not — the analysed application's bundled dependencies and their
     * resource files — and was simply never opened, not excluded on evidence the way the
     * per-endpoint directories were.
     */
    public Result ingestJar(String jarName, ArtifactSource source) {
        return ingestJar(jarName, source, false);
    }

    /** @param force re-ingest even when the source digest matches a published run; see {@link
     *  #ingest(String, String, String, String, ArtifactSource, boolean)} for why this exists. */
    public Result ingestJar(String jarName, ArtifactSource source, boolean force) {
        // Digest before allocating a version. Allocating first would burn a version number on every
        // no-op re-ingest, so the sequence would climb without any run to show for it.
        String digest = SourceDigest.of(source,
                List.of(AnalysisJsonMapper.ARTIFACT, DagAndEndpointMapper.ARTIFACT,
                        BundledJarMapper.ARTIFACT, JarDependencyMapper.ARTIFACT));
        Document existing = force ? null : publishedRunWithDigest(jarName, digest);
        if (existing != null) {
            log.info("jar {}: source unchanged (digest {}), reusing run {}",
                    jarName, digest.substring(0, 12), existing.getString("_id"));
            return new Result(existing.getString("_id"), new WriteStats(), true);
        }

        int version = marker.nextVersion("JAR", jarName, "LOCAL", jarName);
        String runId = IdCodec.run("JAR", jarName, "LOCAL", jarName, version).id();
        Document run = new Document("_id", runId)
                .append("kind", "JAR")
                .append("project", jarName)
                .append("environment", "LOCAL")
                .append("entryPoint", jarName)
                .append("version", version)
                .append(SourceDigest.FIELD, digest)
                .append("schemaVersion", SchemaMeta.CURRENT_VERSION)
                .append("isCurrent", false);
        marker.begin(run);

        WriteStats stats = new WriteStats();
        IngestContext ctx = new IngestContext(runId, "JAR");
        try (BulkWriter writer = new BulkWriter(db, stats)) {
            // Two passes over the same file rather than one mapper doing everything: the Java
            // model and the call tree are independent subjects, and a single class handling both
            // would be the "one big mapper" the extension point exists to avoid. The file is
            // streamed either way, so the cost is I/O the OS has already cached.
            for (ArtifactMapper m : List.of(new AnalysisJsonMapper(), new DagAndEndpointMapper(),
                    new BundledJarMapper(), new JarDependencyMapper())) {
                try (InputStream in = source.open(m.artifact())) {
                    // bundled-jars.json is optional: older analyses were produced before this
                    // mapper existed and have no such file. Absence is not an ingest failure —
                    // the same reasoning that already applies to per-endpoint directories.
                    if (in == null) continue;
                    m.map(ctx, in, writer::upsert);
                    writer.flushAll();
                } catch (IOException e) {
                    throw new IllegalStateException("reading " + m.artifact() + " for " + runId, e);
                }
            }
        } catch (RuntimeException e) {
            marker.fail(runId, e.toString());
            throw e;
        }
        assertReferencesResolve(runId);
        // After the edges exist and before publishing, for the same reason as the facet counts.
        new EndpointReachability(db).computeFor(runId);
        new FacetCounter(db).computeFor(runId);
        marker.complete(runId, "JAR", jarName, "LOCAL", jarName);
        log.info("jar run {} complete: {}", runId, stats);
        return new Result(runId, stats);
    }

    /**
     * Every artifact the PL/SQL path would read, including the per-node files.
     *
     * <p>The node files have to be listed, not assumed: they are the bulk of the input and a run
     * that added or removed one while leaving the indexes byte-identical would otherwise digest the
     * same as its predecessor and be skipped.
     */
    private List<String> plsqlArtifactPaths(ArtifactSource source) {
        List<String> paths = new ArrayList<>();
        for (ArtifactMapper m : mappers) paths.add(m.artifact());
        for (ArtifactMapper m : postNodeMappers) paths.add(m.artifact());
        try {
            paths.addAll(source.list(NODES_DIR));
            paths.addAll(source.list(CHUNKS_DIR));
        } catch (IOException e) {
            // Cannot enumerate the nodes, so cannot claim the source is unchanged.
            paths.add("unlistable:" + NODES_DIR + ":" + e);
        }
        return paths;
    }

    public Result ingest(String dialect, String project, String env, String entryPoint,
                         ArtifactSource source) {
        return ingest(dialect, project, env, entryPoint, source, false);
    }

    /**
     * @param force re-ingest even when the source digest matches a published run
     *
     * <p>The escape hatch is not optional. A digest describes the <em>source</em>, and identical
     * source can still need re-ingesting: after a mapper fix, after a schema change, or after a
     * partial cleanup left the database missing rows the digest knows nothing about. Without this,
     * the only way to recover would be to edit the run document or drop the database.
     *
     * <p>It is also how the dedup property stays testable. Dedup is proven by writing the same
     * entities twice and seeing one copy with two membership rows — which the short-circuit would
     * otherwise make unreachable.
     */
    public Result ingest(String dialect, String project, String env, String entryPoint,
                         ArtifactSource source, boolean force) {
        String digest = SourceDigest.of(source, plsqlArtifactPaths(source));
        Document existing = force ? null : publishedRunWithDigest(entryPoint, digest);
        if (existing != null) {
            log.info("{}: source unchanged (digest {}), reusing run {}",
                    entryPoint, digest.substring(0, Math.min(12, digest.length())),
                    existing.getString("_id"));
            return new Result(existing.getString("_id"), new WriteStats(), true);
        }

        int version = marker.nextVersion(dialect, project, env, entryPoint);
        String runId = IdCodec.run(dialect, project, env, entryPoint, version).id();

        Document run = new Document("_id", runId)
                .append("kind", "PLSQL")
                .append("dialect", dialect)
                .append("project", project)
                .append("environment", env)
                .append("entryPoint", entryPoint)
                .append("version", version)
                .append(SourceDigest.FIELD, digest)
                .append("schemaVersion", SchemaMeta.CURRENT_VERSION)
                .append("isCurrent", false);
        marker.begin(run);

        WriteStats stats = new WriteStats();
        IngestContext ctx = new IngestContext(runId, dialect);
        // Real diagnostic gap found live: this whole method previously logged only at the very
        // start (implicitly) and the very end ("run {} complete") — every artifact/node/chunk
        // mapper in between wrote real documents to Mongo (a genuine network round trip per
        // flushAll(), and against a remote/Atlas cluster this can take minutes) with nothing in
        // between to show it was still making progress rather than stuck. A user watching a job
        // sit at "[3/3] Analysis complete" for 5+ minutes had no way to tell entity-ingest was
        // even running, let alone how far through it was. Log per-mapper and per-N-files instead.
        long ingestStart = System.currentTimeMillis();
        log.info("run {}: entity-ingest starting ({} artifact mappers, {} post-node mappers)",
                runId, mappers.size(), postNodeMappers.size());
        try (BulkWriter writer = new BulkWriter(db, stats)) {
            for (ArtifactMapper m : mappers) {
                long t0 = System.currentTimeMillis();
                try (InputStream in = source.open(m.artifact())) {
                    if (in == null) {
                        log.info("run {}: artifact {} absent, skipping", runId, m.artifact());
                        continue;
                    }
                    m.map(ctx, in, writer::upsert);
                    // Flush between artifacts so a later failure cannot roll back earlier work
                    // that is already correct — with no transactions, partial progress is normal
                    // and is made harmless by the run marker, not by holding writes back.
                    writer.flushAll();
                    log.info("run {}: artifact {} mapped+flushed in {}ms", runId, m.artifact(),
                            System.currentTimeMillis() - t0);
                } catch (IOException e) {
                    throw new IllegalStateException("reading " + m.artifact() + " for " + runId, e);
                }
            }
            // Nodes last: TablesIndexMapper has by now filled the object lookup, so each table
            // access a node records resolves to a real objectRef without a database round-trip.
            try {
                long tNodes = System.currentTimeMillis();
                int nodeCount = 0;
                for (String nodePath : source.list(NODES_DIR)) {
                    try (InputStream in = source.open(nodePath)) {
                        if (in == null) continue;
                        nodeMapper.map(ctx, in, writer::upsert);
                        nodeCount++;
                        if (nodeCount % 500 == 0) {
                            log.info("run {}: {} node files mapped so far ({}ms elapsed)",
                                    runId, nodeCount, System.currentTimeMillis() - tNodes);
                        }
                    }
                }
                writer.flushAll();
                log.info("run {}: {} node files mapped+flushed in {}ms", runId, nodeCount,
                        System.currentTimeMillis() - tNodes);

                long tChunks = System.currentTimeMillis();
                int chunkCount = 0;
                for (String chunkPath : source.list(CHUNKS_DIR)) {
                    try (InputStream in = source.open(chunkPath)) {
                        if (in == null) continue;
                        chunkMapper.map(ctx, in, writer::upsert);
                        chunkCount++;
                        if (chunkCount % 500 == 0) {
                            log.info("run {}: {} chunk files mapped so far ({}ms elapsed)",
                                    runId, chunkCount, System.currentTimeMillis() - tChunks);
                        }
                    }
                }
                writer.flushAll();
                log.info("run {}: {} chunk files mapped+flushed in {}ms", runId, chunkCount,
                        System.currentTimeMillis() - tChunks);

                for (ArtifactMapper m : postNodeMappers) {
                    long t0 = System.currentTimeMillis();
                    try (InputStream in = source.open(m.artifact())) {
                        if (in == null) {
                            log.info("run {}: artifact {} absent, skipping", runId, m.artifact());
                            continue;
                        }
                        m.map(ctx, in, writer::upsert);
                        writer.flushAll();
                        log.info("run {}: post-node artifact {} mapped+flushed in {}ms", runId,
                                m.artifact(), System.currentTimeMillis() - t0);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("reading node files for " + runId, e);
            }
        } catch (RuntimeException e) {
            marker.fail(runId, e.toString());
            throw e;
        }

        long tFacets = System.currentTimeMillis();
        assertReferencesResolve(runId);
        // After the writes and before publishing: the rows are resident, and a run that never
        // publishes should not leave facet rows behind for a reader to find.
        new FacetCounter(db).computeFor(runId);
        marker.complete(runId, dialect, project, env, entryPoint);
        log.info("run {}: reference-check + facet-count complete in {}ms", runId,
                System.currentTimeMillis() - tFacets);
        log.info("run {} complete in {}ms total: {}", runId,
                System.currentTimeMillis() - ingestStart, stats);
        return new Result(runId, stats);
    }

    /** Convenience for callers holding a path-to-stream function. */
    public static ArtifactSource sourceOf(Function<String, InputStream> f) {
        return f::apply;
    }
}
