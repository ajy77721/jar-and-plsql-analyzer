package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.StorageFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.FlowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for flow analysis mode.
 *
 * <p>All output is addressed by {@link ArtifactKey} and written through the supplied
 * {@link ArtifactStore} — there is no {@code --output-dir} filesystem path any more. The one
 * remaining local path is {@link Options#configPath}, which names {@code plsql-config.yaml};
 * that file holds plaintext database credentials and must never enter the store.
 *
 * <p>Called in-process from the web layer via {@link #run(ArtifactStore, Options)}, and from the
 * CLI via {@link #parseArgs(String[])} + the same method.
 */
public class FlowAnalysisMain {

    private static final Logger log = LoggerFactory.getLogger(FlowAnalysisMain.class);

    /** Everything the flow analysis needs, in storage-neutral terms. */
    public static final class Options {

        /** Local filesystem path to plsql-config.yaml. Stays local: it holds DB credentials. */
        public String configPath;

        public final List<String> entryPoints = new ArrayList<>();

        /**
         * PARENT prefix the analysis folder is created under — e.g.
         * {@code plsql-parse/<DBTYPE>/<PROJECT>/<ENV>}. Null means "print JSON to stdout".
         */
        public ArtifactKey outputKey;

        /** Leaf folder name under {@link #outputKey}; null derives it from the entry point. */
        public String outputName;

        /** Shared prefix for ParseResultCache/NodeParseCache. Null falls back to outputKey. */
        public ArtifactKey cacheKey;

        /** Per-connection scope for the schema-resolver cache, e.g. "DB2~Db2Demo~local". */
        public String cacheScope;

        /** Base prefix for the schema-resolver cache. Null means {@code cache/}. */
        public ArtifactKey cacheRootKey;

        public int maxDepth = -1;
        public boolean pretty = false;
        public boolean clearCache = false;

        /**
         * Bypass the analysis-level idempotency registry so every reachable object is re-fetched
         * and re-analysed even when unchanged. {@code --force-refresh} on the CLI; the analyze
         * endpoint's {@code forceRefresh} form field. See
         * {@link DependencyCrawler#setForceRefresh(boolean)}.
         */
        public boolean forceRefresh = false;

        /**
         * Fast re-run: reuse the persistent source/parse caches and the analysis registry to skip
         * objects unchanged since the last run. {@code false} (the default) is a complete FRESH
         * crawl — no cross-run cache reads, no registry skips. Wired from the analyze form's
         * "Fast re-run (skip unchanged)" checkbox via
         * {@link DependencyCrawler#setFastReRun(boolean)}; {@code --fast-re-run} on the CLI.
         */
        public boolean fastReRun = false;

        /**
         * W3 DB-impact safety knobs. Defaults are deliberately gentle; the web layer wires them
         * from {@code app.crawl.db.qps} / {@code app.crawl.db.timeout-seconds} /
         * {@code app.crawl.db.circuit-breaker-threshold}.
         */
        /** Catalog/source query QPS cap (token bucket). {@code <=0} disables throttling. */
        public double dbQps = 20.0;
        /** Per-statement JDBC queryTimeout in seconds. {@code <=0} leaves the driver default. */
        public int dbTimeoutSeconds = 30;
        /** Consecutive failures that abort the crawl to protect the DB. {@code <=0} disables it. */
        public int dbCircuitBreakerThreshold = 5;

        /**
         * When false (the default) a crawl that produces zero nodes throws instead of writing an
         * empty analysis over the output key. Wired from {@code app.parser.allow-empty-analysis}
         * by the web layer; {@code --allow-empty-analysis} on the CLI. See
         * {@link DependencyCrawler#setAllowEmptyAnalysis(boolean)}.
         */
        public boolean allowEmptyAnalysis = false;

        /**
         * The object type the caller asked for (e.g. {@code PROCEDURE}, {@code PACKAGE}), or null
         * when unspecified. Advisory only — it feeds the depth-0 entry lookup's package fallback and
         * its error message; the downloader already probes every relevant type. See
         * {@link DependencyCrawler#setEntryObjectType(String)}.
         */
        public String entryObjectType;

        /** CLI only: local directory the LOCAL store is rooted at. Ignored by in-process callers. */
        public String dataRoot = "data";

        /**
         * Upper bound on concurrent database work fanned out per BFS level (catalog lookups and
         * source fetches). Default 8; lower it to match a database's connection limit.
         * See {@link DependencyCrawler#setCatalogParallelism(int)} /
         * {@link DependencyCrawler#setSourceParallelism(int)}.
         */
        public int parallelism = 8;
    }

    /** Parses a key from its '/'-joined form. Empty segments are dropped. */
    public static ArtifactKey key(String slashJoined) {
        List<String> segs = new ArrayList<>();
        for (String s : slashJoined.split("/")) {
            if (!s.isEmpty()) segs.add(s);
        }
        if (segs.isEmpty()) throw new IllegalArgumentException("Empty artifact key: " + slashJoined);
        return ArtifactKey.of(segs.toArray(new String[0]));
    }

    /**
     * Parses CLI arguments into {@link Options}. Throws on a usage error; never exits the JVM.
     *
     * @param args remaining arguments after --flow has been consumed
     * @throws IllegalArgumentException on a usage error
     */
    public static Options parseArgs(String[] args) {
        Options o = new Options();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config":
                    if (i + 1 < args.length) o.configPath = args[++i];
                    else fail("--config requires a path argument");
                    break;
                case "--entry":
                    if (i + 1 < args.length) o.entryPoints.add(args[++i]);
                    else fail("--entry requires an entry point argument");
                    break;
                case "--output-key":
                    if (i + 1 < args.length) o.outputKey = key(args[++i]);
                    else fail("--output-key requires a key argument");
                    break;
                case "--output-name":
                    if (i + 1 < args.length) o.outputName = args[++i];
                    else fail("--output-name requires an argument");
                    break;
                case "--max-depth":
                    if (i + 1 < args.length) {
                        try {
                            o.maxDepth = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            fail("--max-depth requires a numeric argument");
                        }
                    } else {
                        fail("--max-depth requires a numeric argument");
                    }
                    break;
                case "--pretty":
                    o.pretty = true;
                    break;
                case "--flow":
                    break;   // skip if passed through
                case "--cache-key":
                    if (i + 1 < args.length) o.cacheKey = key(args[++i]);
                    else fail("--cache-key requires a key argument");
                    break;
                case "--cache-scope":
                    if (i + 1 < args.length) o.cacheScope = args[++i];
                    else fail("--cache-scope requires an argument");
                    break;
                case "--cache-root-key":
                    if (i + 1 < args.length) o.cacheRootKey = key(args[++i]);
                    else fail("--cache-root-key requires a key argument");
                    break;
                case "--data-root":
                    if (i + 1 < args.length) o.dataRoot = args[++i];
                    else fail("--data-root requires a path argument");
                    break;
                case "--parallelism":
                    if (i + 1 < args.length) {
                        try {
                            o.parallelism = Math.max(1, Integer.parseInt(args[++i]));
                        } catch (NumberFormatException e) {
                            fail("--parallelism requires a numeric argument");
                        }
                    } else {
                        fail("--parallelism requires a numeric argument");
                    }
                    break;
                case "--clear-cache":
                    o.clearCache = true;
                    break;
                case "--force-refresh":
                    o.forceRefresh = true;
                    break;
                case "--fast-re-run":
                    o.fastReRun = true;
                    break;
                case "--db-qps":
                    if (i + 1 < args.length) {
                        try {
                            o.dbQps = Double.parseDouble(args[++i]);
                        } catch (NumberFormatException e) {
                            fail("--db-qps requires a numeric argument");
                        }
                    } else {
                        fail("--db-qps requires a numeric argument");
                    }
                    break;
                case "--db-timeout-seconds":
                    if (i + 1 < args.length) {
                        try {
                            o.dbTimeoutSeconds = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            fail("--db-timeout-seconds requires a numeric argument");
                        }
                    } else {
                        fail("--db-timeout-seconds requires a numeric argument");
                    }
                    break;
                case "--allow-empty-analysis":
                    o.allowEmptyAnalysis = true;
                    break;
                default:
                    if (args[i].startsWith("-")) fail("Unknown flow option: " + args[i]);
                    break;
            }
        }

        if (o.configPath == null) fail("--config is required for flow analysis mode");
        if (o.entryPoints.isEmpty()) fail("at least one --entry is required");
        return o;
    }

    /**
     * Reports a usage error by <em>throwing</em>, never by exiting the JVM.
     *
     * <p>This used to call {@code System.exit(1)}. {@link #parseArgs} is public and documented as
     * callable in-process, so an exit here was a latent whole-server kill: one bad argument string
     * passed to an embedded caller would have taken the application down. {@link #runCli} catches
     * this, prints usage and returns a non-zero exit code; the only place that terminates the
     * process is {@link com.plsql.parser.PlSqlParserMain#main(String[])}.
     */
    private static void fail(String message) {
        throw new IllegalArgumentException(message);
    }

    /**
     * CLI shim: parse arguments, create the store for {@code --data-root}, run.
     *
     * <p>Returns an exit code instead of calling {@link System#exit}, so this method — and the
     * {@link #parseArgs} it calls — stay safe to invoke in-process (e.g. from an embedding server
     * or from {@link com.plsql.parser.PlSqlParserMain#runCli}). Only the true process entry point,
     * {@link com.plsql.parser.PlSqlParserMain#main(String[])}, turns the returned code into a JVM exit.
     *
     * <p>This is the ONE place in this module that names a filesystem path for artifact data,
     * and only because a command line has nowhere else to say where the data root is. Everything
     * downstream is key-based; pointing the CLI at a remote backend is a change to
     * {@code StorageFactory} alone.
     *
     * @return 0 on success, 1 on a usage or runtime failure
     */
    public static int runCli(String[] args) {
        Options opts;
        try {
            opts = parseArgs(args);
        } catch (IllegalArgumentException usageError) {
            System.err.println("Error: " + usageError.getMessage());
            printUsage();
            return 1;
        }
        try {
            // Resolve the mode rather than assuming local, so a CLI run honours app.storage.mode
            // exactly as the web app does. Hardcoding local here would write to disk while the
            // application wrote to the bucket.
            run(StorageFactory.create(
                    StorageFactory.settingsFor(System.getProperty("app.storage.mode"),
                                               Path.of(opts.dataRoot))), opts);
            return 0;
        } catch (RuntimeException e) {
            // run() no longer calls System.exit itself (see its own doc comment) — it rethrows so
            // the web app's queue worker can mark the job FAILED. Only the true standalone CLI
            // entry point (PlSqlParserMain.main) turns a non-zero return into a process exit.
            System.err.println("Fatal error in flow analysis: " + e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        }
    }

    /**
     * Run the flow analysis. Every artifact is written through {@code store}.
     *
     * @param store the artifact store all output and caches go through
     * @param opts  analysis parameters; see {@link Options}
     */
    public static void run(ArtifactStore store, Options opts) {
        if (store == null) throw new IllegalArgumentException("ArtifactStore is required");
        if (opts == null || opts.configPath == null) {
            throw new IllegalArgumentException("Options.configPath is required");
        }
        if (opts.entryPoints.isEmpty()) {
            throw new IllegalArgumentException("At least one entry point is required");
        }

        long totalStart = System.currentTimeMillis();

        try (DbConnectionManager connManager = new DbConnectionManager(opts.configPath)) {
            // W3 DB-impact safety: rate-limit catalog/source queries, bound every statement with a
            // JDBC queryTimeout, and abort the crawl if the database stops responding. The analyzed
            // database is only ever SELECTed (see DbConnectionManager's statement creators).
            connManager.setRateLimitQps(opts.dbQps);
            connManager.setStatementTimeoutSeconds(opts.dbTimeoutSeconds);
            connManager.setGlobalCircuitThreshold(opts.dbCircuitBreakerThreshold);
            System.err.println("=== PL/SQL Flow Analysis ===");
            System.err.println("Config: " + opts.configPath);
            System.err.println("Schemas: " + connManager.getAvailableSchemas());
            System.err.println("Entry points: " + opts.entryPoints);
            System.err.println("Max depth: " + (opts.maxDepth < 0 ? "UNLIMITED" : opts.maxDepth));
            System.err.println();

            // Clear the schema-resolver cache if requested (scoped to this connection when a
            // scope is given). The key comes from SchemaResolver so the two cannot disagree.
            if (opts.clearCache) {
                ArtifactKey resolverCache =
                        SchemaResolver.resolverCacheKey(opts.cacheRootKey, opts.cacheScope);
                int removed = store.deletePrefix(resolverCache);
                if (removed > 0) System.err.println("Cleared schema resolver cache: " + resolverCache);
            }

            System.err.println("Initializing schema resolver for: " + opts.entryPoints);
            SchemaResolver schemaResolver = new SchemaResolver(
                    connManager, opts.entryPoints, opts.cacheScope, store, opts.cacheRootKey);

            SourceDownloader downloader = new SourceDownloader(connManager);
            PlSqlParserEngine engine = new PlSqlParserEngine();

            DependencyCrawler crawler = new DependencyCrawler(engine, downloader, schemaResolver);
            crawler.setMaxDepth(opts.maxDepth);
            crawler.setAllowEmptyAnalysis(opts.allowEmptyAnalysis);
            crawler.setForceRefresh(opts.forceRefresh);
            // Fresh by default: only an explicit fast re-run reads the cross-run caches/registry.
            crawler.setFastReRun(opts.fastReRun);
            // Advisory: lets the depth-0 lookup fall back from e.g. PROCEDURE to PACKAGE and name
            // what it tried when it still finds nothing (Fix C).
            crawler.setEntryObjectType(opts.entryObjectType);
            // Bound the per-level parallel catalog + source fan-out (default 8).
            crawler.setCatalogParallelism(opts.parallelism);
            crawler.setSourceParallelism(opts.parallelism);
            if (opts.outputKey != null) {
                crawler.setOutputDir(store, opts.outputKey);
                if (opts.outputName != null && !opts.outputName.isBlank()) {
                    crawler.setOutputFolderName(opts.outputName);
                }
                // A shared cacheKey lets all procs in a package reuse the same ParseResultCache
                // and NodeParseCache, so package bodies are parsed by ANTLR only once.
                ArtifactKey cacheKey = opts.cacheKey != null ? opts.cacheKey : opts.outputKey;
                // Node-level parse cache: skip ANTLR for nodes already analysed in any prior run
                try {
                    crawler.setNodeCache(new NodeParseCache(store, cacheKey));
                } catch (java.io.IOException e) {
                    System.err.println("[NodeCache] Could not init cache: " + e.getMessage());
                }
                // Source-level parse cache: share parse result across subprograms from the same
                // package body
                try {
                    crawler.setParseResultCache(new ParseResultCache(store, cacheKey));
                } catch (java.io.IOException e) {
                    System.err.println("[ParseCache] Could not init cache: " + e.getMessage());
                }
            }

            ObjectMapper mapper = new ObjectMapper();
            if (opts.pretty) {
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
            }

            // No output directory to create: the store materialises parents on first write.

            for (String entry : opts.entryPoints) {
                System.err.println();
                System.err.println("========================================");
                System.err.println("Crawling entry point: " + entry);
                System.err.println("========================================");

                FlowResult result = crawler.crawl(entry);

                System.err.println();
                System.err.println("Completed: " + entry);
                System.err.println("  Objects crawled: " + result.getTotalObjectsCrawled());
                System.err.println("  Max depth reached: " + result.getMaxDepthReached());
                System.err.println("  Tables found: " + result.getAllTables().size());
                System.err.println("  Call graph edges: " + result.getCallGraph().size());
                System.err.println("  Errors: " + result.getErrors().size());
                System.err.println("  Time: " + result.getCrawlTimeMs() + "ms");
                // DependencyCrawler.crawl() already logs its own "[timing] <entry>: parse=...
                // discover=... save=... total=..." summary line via log.info (SLF4J), which is
                // what lands in the real application log for both CLI and in-process web usage.
                // Log it here too via SLF4J (not System.err) so it's grep-able from wherever
                // this run() call is driven, matching DependencyCrawler's own log.info pattern.
                log.info("[timing] {}: crawl total={}ms (see DependencyCrawler for parse/discover/save breakdown)",
                        entry, result.getCrawlTimeMs());

                if (opts.outputKey != null) {
                    // ChunkedFlowWriter already wrote flow.json, chunks/, sources/, progress.json
                    String safeName = (opts.outputName != null && !opts.outputName.isBlank())
                            ? opts.outputName
                            : entry.toUpperCase().replace(".", "_")
                                    .replace("/", "_").replace("\\", "_");
                    System.err.println("  Output: " + opts.outputKey.resolve(safeName));
                } else {
                    System.out.println(mapper.writeValueAsString(result));
                }
            }

            long totalTime = System.currentTimeMillis() - totalStart;
            System.err.println();
            System.err.println("=== Flow Analysis Complete ===");
            System.err.println("Total entry points: " + opts.entryPoints.size());
            System.err.println("Total time: " + totalTime + "ms");

        } catch (Exception e) {
            // Real bug found live: this used to call System.exit(1) here — but run() is called
            // BOTH from the CLI (runCli, where terminating the process is correct) AND in-process
            // from the web app's own queue worker thread (AnalysisService.runAnalysis, per this
            // class's own doc comment). A single bad request (e.g. an unresolvable DB host) hit
            // this catch block on the worker thread and took down the ENTIRE running server with
            // it — every other user/job died along with the one bad analysis. Rethrow instead;
            // runCli() turns this into a non-zero return code, and only PlSqlParserMain.main —
            // the true process entry point — exits the JVM.
            System.err.println("Fatal error in flow analysis: " + e.getMessage());
            e.printStackTrace(System.err);
            // The empty-crawl guard is an IllegalStateException by contract (see DependencyCrawler).
            // Rethrow it unwrapped so the queue/web layer sees the exact type and message rather
            // than a generic "Fatal error in flow analysis: ..." wrapper.
            if (e instanceof IllegalStateException ise) {
                throw ise;
            }
            throw new RuntimeException("Fatal error in flow analysis: " + e.getMessage(), e);
        }
    }

    private static void printUsage() {
        System.err.println();
        System.err.println("PL/SQL Flow Analysis - Recursive dependency analysis");
        System.err.println();
        System.err.println("Usage:");
        System.err.println("  java -jar plsql-parser.jar --flow [options]");
        System.err.println();
        System.err.println("Required options:");
        System.err.println("  --config <path>       Path to plsql-config.yaml (a LOCAL file: it");
        System.err.println("                        holds DB credentials and never enters storage)");
        System.err.println("  --entry <name>        Entry point (e.g. PKG_NAME.PROC_NAME or FUNC_NAME)");
        System.err.println("                        Can be specified multiple times");
        System.err.println();
        System.err.println("Optional:");
        System.err.println("  --data-root <path>    Local root the artifact store is created over");
        System.err.println("                        (default: ./data)");
        System.err.println("  --output-key <key>    Storage key of the PARENT prefix for output,");
        System.err.println("                        e.g. plsql-parse/ORACLE/PROJ/DEV");
        System.err.println("  --output-name <name>  Leaf folder under --output-key");
        System.err.println("  --cache-key <key>     Shared prefix for ParseResultCache/NodeParseCache");
        System.err.println("                        (defaults to --output-key)");
        System.err.println("  --cache-scope <id>    Per-connection scope for the schema-resolver cache");
        System.err.println("                        (isolates object→schema mappings across databases)");
        System.err.println("  --cache-root-key <k>  Base prefix for the schema-resolver cache");
        System.err.println("                        (default: cache)");
        System.err.println("  --clear-cache         Delete the schema-resolver cache before running");
        System.err.println("  --allow-empty-analysis  Permit writing a 0-node analysis (default: fail loud)");
        System.err.println("  --parallelism <N>     Max concurrent DB lookups/source fetches per BFS");
        System.err.println("                        level (default: 8; lower to match a DB conn limit)");
        System.err.println("  --max-depth <N>       Maximum recursion depth (default: -1 = unlimited)");
        System.err.println("  --pretty              Pretty-print the JSON output");
        System.err.println();
        System.err.println("Examples:");
        System.err.println("  java -jar plsql-parser.jar --flow --config config.yaml \\");
        System.err.println("    --entry \"PG_AC_EINVOICE.PC_IRBM_SUBMISSION\" --pretty \\");
        System.err.println("    --data-root ./data --output-key plsql-parse/ORACLE/PROJ/DEV");
    }
}
