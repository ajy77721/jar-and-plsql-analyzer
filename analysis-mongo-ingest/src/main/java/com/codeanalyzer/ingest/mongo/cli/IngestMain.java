package com.codeanalyzer.ingest.mongo.cli;

import com.codeanalyzer.ingest.mongo.MongoIngestService;
import com.codeanalyzer.ingest.mongo.verify.IntegrityChecker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.StorageFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * The command-line entry point.
 *
 * <h2>Exit codes are the contract</h2>
 *
 * <p>This runs unattended, so what it prints matters less than what it returns. A verification pass
 * that finds problems must not exit 0: a script that ignores the output and trusts the code would
 * then treat a broken ingest as a good one, which is precisely the failure the verifier exists to
 * prevent.
 *
 * <pre>
 *   0  success
 *   1  usage error
 *   2  the source is missing or unreadable
 *   3  ingest failed
 *   4  ingest succeeded but verification found problems
 * </pre>
 */
public final class IngestMain {

    private static final String USAGE = """
            usage: ingest <command> [options]

              plsql --source <dir> --dialect <D> --project <P> --env <E> --entry <N>
                    ingest one PL/SQL analysis directory

              jar   --source <dir> --name <jarName>
                    ingest one JAR analysis directory

              verify --run <runId>
                    re-check an already-ingested run

            options:
              --mongo <uri>   default mongodb://localhost:27017 (where entities are written)
              --db <name>     default analyzer

            --source's own storage mode (where the analysis being ingested was written) is read
            from -Dapp.storage.mode / APP_STORAGE_MODE — unset means local. For s3/mongodb, the
            usual APP_STORAGE_S3_*/APP_STORAGE_MONGODB_* system properties or env vars apply, same
            as the web app's own boot path; --source itself is then ignored except in local mode.
            """;

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * The real entry point, returning the exit code instead of calling {@code System.exit}.
     *
     * <p>Public so the exit codes can be asserted. They are the contract this class exists to
     * uphold, and a contract nothing tests is a comment.
     */
    public static int run(String[] args) {
        if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            System.out.println(USAGE);
            return args.length == 0 ? 1 : 0;
        }
        String command = args[0];
        String mongo = opt(args, "--mongo", "mongodb://localhost:27017");
        String dbName = opt(args, "--db", "analyzer");

        try (MongoClient client = MongoClients.create(mongo)) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoIngestService svc = new MongoIngestService(db);
            svc.installSchema();

            String runId;
            switch (command) {
                case "plsql" -> {
                    MongoIngestService.ArtifactSource src = storeSource(required(args, "--source"));
                    try (InputStream probe = src.open("tables.json")) {
                        if (probe == null) {
                            System.err.println("not an analysis directory (no tables.json): "
                                    + required(args, "--source"));
                            return 2;
                        }
                    } catch (IOException e) {
                        System.err.println("cannot read source: " + e.getMessage());
                        return 2;
                    }
                    runId = svc.ingest(required(args, "--dialect"), required(args, "--project"),
                            opt(args, "--env", "local"), required(args, "--entry"), src).runId();
                }
                case "jar" -> {
                    MongoIngestService.ArtifactSource src = storeSource(required(args, "--source"));
                    try (InputStream probe = src.open("analysis.json")) {
                        if (probe == null) {
                            System.err.println("no readable analysis.json under: "
                                    + required(args, "--source"));
                            return 2;
                        }
                    } catch (IOException e) {
                        System.err.println("cannot read source: " + e.getMessage());
                        return 2;
                    }
                    runId = svc.ingestJar(required(args, "--name"), src).runId();
                }
                case "verify" -> runId = required(args, "--run");
                default -> {
                    System.err.println("unknown command: " + command);
                    System.err.println(USAGE);
                    return 1;
                }
            }

            List<IntegrityChecker.Finding> findings = new IntegrityChecker(db).check(runId);
            System.out.println("run " + runId);
            if (findings.isEmpty()) {
                System.out.println("  integrity: clean");
                return 0;
            }
            System.out.println("  integrity findings:");
            findings.forEach(f -> System.out.println("    " + f));
            return 4;
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(USAGE);
            return 1;
        } catch (RuntimeException e) {
            System.err.println("ingest failed: " + e);
            return 3;
        }
    }

    /**
     * Reads artifacts through {@link ArtifactStore}, never through {@code java.nio.file} directly.
     *
     * <p>Not a stylistic preference. {@code DirectFileIoGateTest} fails the build when a class
     * outside {@code analyzer-storage} touches the filesystem, and it caught this class doing
     * exactly that — eight operations. The rule exists so the whole product can run against S3 or
     * MongoDB-backed storage with nothing on local disk; a CLI that reads files directly would
     * work only in the one deployment that happens to have them.
     *
     * <p>{@code LocalArtifactStore} is the local implementation of that interface, so this keeps
     * the convenience of a directory argument without the module owning any file I/O of its own.
     * The plan required this module to add zero allowlist entries, and it adds none.
     *
     * <p><b>Mode-aware, not hardcoded to local.</b> This used to always build a local store
     * ({@code Settings.localAt(root)}) regardless of what mode the analysis being ingested was
     * actually written in, so an analysis run in {@code mongodb} mode (or {@code s3}) could never
     * be ingested at all — the "source" directory it was told to read simply did not exist on
     * local disk. Resolved through {@link StorageFactory#settingsFor}, the same entry point
     * {@code FlowAnalysisMain}/{@code PocRunner} already use for exactly this reason —
     * {@code app.storage.mode} (system property, falling back to the {@code APP_STORAGE_MODE} env
     * var) decides which backend {@code --source} is read through; unset means {@code local}, so
     * every existing invocation keeps working unchanged.
     *
     * <p><b>What {@code root} means shifts with the mode, and the key-building below has to shift
     * with it</b> — caught before shipping, not after. In local mode {@code settingsFor} builds a
     * {@code LocalArtifactStore} rooted exactly at {@code root} (identical to the old
     * {@code localAt(root)}), so a bare key like {@code "tables.json"} already resolves correctly
     * with no prefix. In {@code s3}/{@code mongodb} mode, {@code settingsFor} builds a store rooted
     * at the storage-wide bucket/database prefix — {@code root} plays no part in that store's own
     * construction — so the same bare key would resolve at the *storage root* instead of under the
     * analysis's own subdirectory, silently returning nothing rather than erroring. {@code root} is
     * therefore joined onto every key here, unconditionally: a no-op in local mode (the store is
     * already scoped there, so the join is redundant but harmless — {@code ArtifactKey} normalises
     * the segments the same way either order produces them), and the fix in {@code s3}/{@code
     * mongodb} mode. {@code --source} in non-local mode is the analysis's own key prefix (e.g.
     * {@code plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE/api}), not a filesystem path.
     */
    private static MongoIngestService.ArtifactSource storeSource(String root) {
        // StorageFactory rather than `new LocalArtifactStore(...)`: the factory wraps every store
        // in CredentialGuardStore, which blocks the config files holding plaintext production
        // database passwords. Constructing the backend directly would quietly opt out of that.
        String modeProp = System.getProperty("app.storage.mode");
        ArtifactStore store = StorageFactory.create(
                StorageFactory.settingsFor(modeProp, Path.of(root)));
        // Local mode's store is already rooted at `root` (see the method doc above) — joining it
        // again here would look for e.g. "<root>/<root>/tables.json". Every other mode's store is
        // rooted at the storage-wide prefix, so `root` has to be joined onto each key by hand.
        boolean localMode = com.analyzer.storage.StorageMode.parse(modeProp)
                == com.analyzer.storage.StorageMode.LOCAL;
        return storeSource(store, localMode ? new String[0] : root.split("/"));
    }

    /**
     * The in-process entry point — an already-built {@link ArtifactStore} and the analysis's own
     * key prefix within it, rather than a filesystem path plus system properties.
     *
     * <p>Exists for {@code unified-web}'s own post-analyze entity-ingest trigger, which already has
     * the exact {@code ArtifactStore} bean the analysis was just written through (see {@code
     * StorageConfig#artifactStore}) and must reuse it rather than reconstruct one from {@code
     * System.getProperty}, the only source the CLI has. A second, independently-built store risks
     * config drift the moment the two diverge — a wrong bucket, a stale URI — silently reading zero
     * rows rather than failing loudly. Always prefix-mode (the s3/mongodb shape below): the app-wide
     * store is never scoped down to one analysis's own directory the way the CLI's {@code
     * localAt(root)} legacy path is, so treating {@code rootSegments} as always-needs-joining is
     * correct here regardless of which backend the store itself happens to be.
     */
    public static MongoIngestService.ArtifactSource storeSource(ArtifactStore store, String rootPrefix) {
        return storeSource(store, rootPrefix.isBlank() ? new String[0] : rootPrefix.split("/"));
    }

    private static MongoIngestService.ArtifactSource storeSource(ArtifactStore store, String[] rootSegments) {
        return new MongoIngestService.ArtifactSource() {
            @Override public InputStream open(String artifactPath) throws IOException {
                ArtifactKey key = ArtifactKey.of(join(rootSegments, artifactPath.split("/")));
                return store.exists(key) ? store.read(key) : null;
            }
            @Override public List<String> list(String directory) throws IOException {
                ArtifactKey prefix = ArtifactKey.of(join(rootSegments, directory.split("/")));
                // No exists(prefix) pre-check: exists() looks up an exact document _id, and a
                // "directory" key is never one — every real document lives *under* it, not at it.
                // That check happened to be harmless in local mode (Files.isDirectory legitimately
                // answers it) but silently zeroed every listing in s3/mongodb mode, since neither
                // backend has anything at that exact key either. Found by running this for real
                // against mongodb mode with the fix in place and getting 0 am_proc_node rows
                // despite 52 real node artifacts sitting right there. Both ArtifactStore.list()
                // implementations already return an empty stream for "nothing here" on their own
                // (LocalArtifactStore: `!Files.isDirectory(dir)`; MongoArtifactStore: an empty
                // query result) — the pre-check was always redundant for local, just never
                // exercised anywhere else until this fix made s3/mongodb mode reachable at all.
                try (var s = store.list(prefix, false)) {
                    // Strip rootSegments back off before returning: callers (MongoIngestService)
                    // pass these strings straight back into open() above, which re-adds the same
                    // prefix — a caller-visible key must stay root-relative in every mode, exactly
                    // as it already was in local mode, or a round trip through list() then open()
                    // would double the prefix and read nothing.
                    return s.map(ArtifactEntry::key)
                            .map(ArtifactKey::segments)
                            .map(segs -> String.join("/", segs.subList(
                                    Math.min(rootSegments.length, segs.size()), segs.size())))
                            .filter(k -> k.endsWith(".json"))
                            .sorted().toList();
                }
            }
        };
    }

    /** Concatenates two path-segment arrays, dropping blank segments a trailing/leading slash
     *  in {@code root} would otherwise introduce. */
    private static String[] join(String[] prefix, String[] rest) {
        if (prefix.length == 0) return rest;
        List<String> out = new java.util.ArrayList<>(prefix.length + rest.length);
        for (String s : prefix) if (!s.isBlank()) out.add(s);
        for (String s : rest) if (!s.isBlank()) out.add(s);
        return out.toArray(new String[0]);
    }

    private static String opt(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) if (args[i].equals(name)) return args[i + 1];
        return fallback;
    }

    private static String required(String[] args, String name) {
        String v = opt(args, name, null);
        if (v == null) throw new IllegalArgumentException("missing required option " + name);
        return v;
    }
}
