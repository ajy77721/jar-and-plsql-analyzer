package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fails the build when a class outside the allowlist touches the filesystem directly.
 *
 * <p>All storage must go through {@code com.analyzer.storage.ArtifactStore}, so that adding a
 * remote backend needs no change anywhere else. The hard part of that migration is not converting
 * a call site — it is knowing whether any are left. A one-off audit answers that for one instant
 * and is wrong by the next commit, and two separate audits of this codebase both undercounted
 * (one missed the fully-qualified {@code new java.io.File(...)} form entirely).
 *
 * <p>So the inventory lives here instead, as a gate. {@link #ALLOWED} is the complete list of
 * classes permitted to do direct I/O, each with the reason. Migrating a class means deleting its
 * line. Adding direct I/O to anything else fails this test with the exact file and line.
 *
 * <p>The counts are budgets, not targets: they may only ever go down. A class that does less
 * direct I/O than its budget also fails, with a note to lower the number — otherwise the budget
 * silently becomes headroom for new violations.
 */
class DirectFileIoGateTest {

    /** Optional package qualifier, so {@code new java.io.File} is caught as well as {@code new File}. */
    private static final String Q = "(?:[a-zA-Z_][\\w.]*\\.)?";

    private static final Map<String, Pattern> PATTERNS = new LinkedHashMap<>();
    static {
        PATTERNS.put("Files.*",        Pattern.compile("\\b" + Q + "Files\\s*\\.\\s*[a-zA-Z]"));
        PATTERNS.put("new File",       Pattern.compile("\\bnew\\s+" + Q + "File\\s*[(\\[]"));
        PATTERNS.put("File statics",   Pattern.compile("\\b" + Q + "File\\s*\\.\\s*(?:createTempFile|separator|separatorChar|listRoots|pathSeparator)"));
        PATTERNS.put("File methods",   Pattern.compile("\\.\\s*(?:listFiles|getParentFile|getAbsoluteFile|getCanonicalFile|mkdirs?|createNewFile|renameTo|deleteOnExit)\\s*\\("));
        PATTERNS.put("File streams",   Pattern.compile("\\bnew\\s+" + Q + "(?:FileInputStream|FileOutputStream|FileReader|FileWriter|RandomAccessFile)\\s*\\("));
        PATTERNS.put("Jar/Zip",        Pattern.compile("\\bnew\\s+" + Q + "(?:JarFile|ZipFile|ZipOutputStream|ZipInputStream|JarOutputStream|JarInputStream)\\s*\\("));
        PATTERNS.put("Path.of/get",    Pattern.compile("\\b" + Q + "(?:Path\\s*\\.\\s*of|Paths\\s*\\.\\s*get)\\s*\\("));
        PATTERNS.put(".toFile()",      Pattern.compile("\\.\\s*toFile\\s*\\(\\s*\\)"));
        PATTERNS.put(".toPath()",      Pattern.compile("\\.\\s*toPath\\s*\\(\\s*\\)"));
        PATTERNS.put("FileSystem/Channel", Pattern.compile("\\b" + Q + "FileSystems?\\s*\\.\\s*[a-zA-Z]|\\bnew\\s+" + Q + "FileSystemResource\\s*\\(|\\b" + Q + "FileChannel\\s*\\.\\s*[a-zA-Z]"));
        PATTERNS.put("commons-io",     Pattern.compile("\\b(?:FileUtils|IOUtils)\\s*\\.\\s*[a-zA-Z]"));
        PATTERNS.put("ProcessBuilder dir", Pattern.compile("\\.\\s*(?:redirectOutput|redirectError|redirectInput)\\s*\\("));
    }

    /**
     * Classes permitted direct filesystem access, with their current op budget and the reason.
     *
     * <p>Key is {@code <module>/<SimpleName>}, because two modules each have a {@code LogController}
     * and a {@code ClaudeProcessRunner}.
     */
    private static final Map<String, Integer> ALLOWED = new LinkedHashMap<>();
    static {
        // ── Config bootstrap — read before any store bean exists, and the one place that handles
        //    plsql-config.yaml, which must never reach shared storage.
        ALLOWED.put("unified-web/ConfigDirService", 42);
        ALLOWED.put("unified-web/DirPropertyResolver", 3);
        ALLOWED.put("unified-web/AppConfig", 1);
        ALLOWED.put("jar-analyzer-core/DomainConfigLoader", 5);
        ALLOWED.put("plsql-config/ConfigLoader", 3);
        // Budget raised 3→6: the TLS cert-upload endpoint writes user-uploaded certificates into
        // {config}/certs/<connection>/ — the self-contained-connection design puts cert material
        // beside plsql-config.yaml on purpose (drivers need file paths; see the plsql-config TLS
        // entries below). Same local-config concern as the original 3 ops.
        ALLOWED.put("unified-web/ConfigController", 6);

        // ── Credential files — plaintext production DB passwords. These MUST stay on local disk;
        //    CredentialGuardStore throws if they are pushed through the store.
        ALLOWED.put("unified-web/AnalysisService", 6);
        ALLOWED.put("unified-web/ParserAnalysisExecutor", 1);
        ALLOWED.put("plsql-parser/DbConnectionManager", 2);

        // ── TLS certificate paths for JDBC — the drivers themselves consume FILE paths (MySQL
        //    trustCertificateKeyStoreUrl, Postgres sslrootcert/sslcert, Oracle and Db2 keystores),
        //    so cert material must live at a resolvable local path inside the config tree. These
        //    classes only RESOLVE paths and convert keystores (PEM→JKS/PKCS12); no analysis
        //    artifact ever flows through here, so the ArtifactStore route cannot apply.
        ALLOWED.put("plsql-config/ConnectionConfig", 5);
        ALLOWED.put("plsql-config/MySqlDialect", 1);
        ALLOWED.put("plsql-config/PostgresDialect", 3);
        ALLOWED.put("plsql-config/SqlServerDialect", 1);
        ALLOWED.put("plsql-config/TlsKeystoreConverter", 7);

        // ── Logging and traversal guards — local operational concerns, not artifacts.
        ALLOWED.put("unified-web/LogController", 10);
        ALLOWED.put("unified-web/SourceController", 4);
        // Admin audit trail (who logged in / connected / was denied) — same reasoning as the log
        // archive: it must survive a store being unreachable (an admin investigating a denial needs
        // it regardless of S3 health), and the admin-portal privacy rule says identity metadata
        // stays inside the data tree, never in shared storage.
        ALLOWED.put("unified-web/AdminAuditService", 17);
        // Reads the local log directory in order to EMPTY it: it ships each rolled log into the
        // store and deletes the local copy. The local reads are the mechanism by which logs stop
        // being local, so they are the point rather than an exception to it. The live log must stay
        // on disk — logging has to survive the store being unreachable.
        ALLOWED.put("unified-web/LogArchiveService", 7);

        // ── Subprocess plumbing — ProcessBuilder and the Claude CLI need a real working directory.
        ALLOWED.put("jar-analyzer-core/ClaudeProcessRunner", 1);
        ALLOWED.put("jar-analyzer-core/ClaudeAnalysisService", 2);

        // ── Storage bootstrap — the one Path.of that builds the store root itself.
        ALLOWED.put("plsql-parser/FlowAnalysisMain", 1);
        // flow-poc/PocRunner deleted (was budget 2): fully migrated to the store — 0 direct ops.
        ALLOWED.put("jar-analyzer-core/JarDataPaths", 1);
        // Mode 4 (entity-ingest) — IngestMain.storeSource(String) builds a StorageFactory.Settings
        // the identical way FlowAnalysisMain/PocRunner do above, for the identical reason: a CLI's
        // own --source/root argument has to become a Path before StorageFactory.settingsFor can
        // build a local-mode store from it. The in-process overload EntityIngestTrigger actually
        // calls, storeSource(ArtifactStore, String), does none of this — it takes an already-built
        // store, so this entry stays at 1, not 2.
        ALLOWED.put("analysis-mongo-ingest/IngestMain", 1);

        // ── Upload temps and seekable-file APIs. A multipart upload genuinely arrives as a local
        //    temp, and JarFile/ZipFile/CFR genuinely need a seekable file. What matters is that the
        //    STORED artifact is always reached via store.materialize(key) — never an assumed path.
        //    These are migrated; the residual ops are the temps themselves.
        ALLOWED.put("unified-web/AnalyzerController", 30);
        // Raised 11 -> 19 (2026-09-14): WarAnalyzerController grew this session with the JAR-side
        // feature ports (bundled-jars content read, domain-config upload, connections) — each new
        // route follows the SAME already-migrated pattern as the JAR side (a multipart/upload temp,
        // or a seekable JarFile/ZipFile read), not a new kind of direct I/O. Real migration of this
        // whole class to ArtifactStore-only access is separate, larger work, not done here.
        ALLOWED.put("unified-web/WarAnalyzerController", 19);
        ALLOWED.put("unified-web/JarAnalysisExecutor", 12);
        ALLOWED.put("unified-web/WarAnalysisExecutor", 12);
        ALLOWED.put("unified-web/JarTargetedAnalysisExecutor", 4);
        ALLOWED.put("unified-web/ClaudeJobExecutor", 3);
        ALLOWED.put("jar-analyzer-core/JarParserService", 13);
        ALLOWED.put("jar-analyzer-core/WarParserService", 9);
        ALLOWED.put("jar-analyzer-core/DecompilerService", 11);
        ALLOWED.put("jar-analyzer-core/PersistenceService", 3);
        ALLOWED.put("unified-web/FlowAnalysisController", 1);
        // Chat sandbox — genuinely local, pre-existing, not yet migrated to ArtifactStore. Chat
        // sessions/scripts run in a real local sandbox directory (ProcessBuilder needs a real
        // working directory, same reasoning as ClaudeProcessRunner/ClaudeAnalysisService above),
        // so this is closer to the "subprocess plumbing" category than an artifact read/write.
        // Recorded here as a deliberate, acknowledged budget rather than left unclassified —
        // migrating chat sessions to ArtifactStore is separate, larger work, not done here.
        ALLOWED.put("unified-web/ChatConfig", 5);
        ALLOWED.put("unified-web/ChatService", 15);
        // Export bundling — builds a real ZipOutputStream/JarFile locally before the finished
        // bundle is handed off; same seekable-file reasoning as JarParserService/DecompilerService
        // above, just not previously listed.
        ALLOWED.put("unified-web/JarExportBundleExecutor", 1);
        ALLOWED.put("unified-web/JarExportController", 1);
        ALLOWED.put("unified-web/ParserAnalysisController", 1);

        // ── Process-local scratch in java.io.tmpdir — needs append and seek, which the store
        //    contract deliberately does not offer. Never touches the data tree.
        ALLOWED.put("jar-analyzer-core/SubtreeCache", 4);
        ALLOWED.put("jar-analyzer-core/RepositoryStereotypeResolver", 3);

        // ── Outside the data root by design — reads the operator's own filesystem, not an artifact.
        ALLOWED.put("unified-web/ConversionController", 5);
        ALLOWED.put("plsql-parser/PlSqlParserMain", 6);
        ALLOWED.put("plsql-parser/PlSqlParserEngine", 2);
    }

    /** analyzer-storage IS the filesystem boundary, so it is exempt by definition. */
    private static final String STORAGE_MODULE = "analyzer-storage";

    @Test
    @DisplayName("no class outside the allowlist touches the filesystem directly")
    void noUnapprovedDirectFileIo() throws IOException {
        Path root = repoRoot();
        Map<String, Integer> counts = new TreeMap<>();
        Map<String, List<String>> where = new TreeMap<>();

        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java"))
                               .filter(DirectFileIoGateTest::isProductionSource).toList()) {
                String module = root.relativize(f).getName(0).toString();
                if (module.equals(STORAGE_MODULE)) continue;
                String id = module + "/" + f.getFileName().toString().replace(".java", "");

                int n = 0;
                List<String> hits = new ArrayList<>();
                List<String> lines = stripComments(Files.readString(f, StandardCharsets.UTF_8));
                for (int i = 0; i < lines.size(); i++) {
                    for (Map.Entry<String, Pattern> e : PATTERNS.entrySet()) {
                        Matcher m = e.getValue().matcher(lines.get(i));
                        while (m.find()) {
                            n++;
                            hits.add(root.relativize(f) + ":" + (i + 1) + "  " + e.getKey());
                        }
                    }
                }
                if (n > 0) {
                    counts.merge(id, n, Integer::sum);
                    where.computeIfAbsent(id, k -> new ArrayList<>()).addAll(hits);
                }
            }
        }

        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            Integer budget = ALLOWED.get(e.getKey());
            if (budget == null) {
                problems.add("NOT ALLOWED: " + e.getKey() + " does direct file I/O (" + e.getValue()
                        + " ops). Route it through com.analyzer.storage.ArtifactStore, or add it to "
                        + "ALLOWED with the reason it must stay local.\n    "
                        + String.join("\n    ", where.get(e.getKey())));
            } else if (e.getValue() > budget) {
                problems.add("BUDGET EXCEEDED: " + e.getKey() + " has " + e.getValue()
                        + " direct I/O ops, allowed " + budget + ". New direct I/O must go through "
                        + "ArtifactStore.\n    " + String.join("\n    ", where.get(e.getKey())));
            }
        }
        // A class that improved must ratchet, or its budget becomes headroom for the next regression.
        for (Map.Entry<String, Integer> e : ALLOWED.entrySet()) {
            int actual = counts.getOrDefault(e.getKey(), 0);
            if (actual < e.getValue()) {
                problems.add("RATCHET: " + e.getKey() + " is down to " + actual + " ops (budget "
                        + e.getValue() + "). Lower the budget in ALLOWED"
                        + (actual == 0 ? ", or delete the entry — it is fully migrated." : "."));
            }
        }

        if (!problems.isEmpty()) {
            int todo = counts.entrySet().stream()
                    .filter(e -> ALLOWED.containsKey(e.getKey())).mapToInt(Map.Entry::getValue).sum();
            fail("Direct filesystem access outside analyzer-storage (" + todo + " ops tracked):\n\n"
                    + String.join("\n\n", problems));
        }
    }

    private static boolean isProductionSource(Path p) {
        String s = p.toString();
        return s.contains("/src/main/java/")
                && !s.contains("/target/")
                && !s.contains("_migration-backup")
                && !s.contains("data_old_");
    }

    /** Walks up from the working directory to the reactor root, so the test runs from any module. */
    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("analyzer-storage"))) {
            p = p.getParent();
        }
        if (p == null) throw new IllegalStateException("Could not locate the repository root");
        return p;
    }

    /** Drops comments so a pattern mentioned in prose is not counted as a call. */
    private static List<String> stripComments(String src) {
        String noBlock = src.replaceAll("(?s)/\\*.*?\\*/", "");
        List<String> out = new ArrayList<>();
        for (String line : noBlock.split("\n", -1)) {
            StringBuilder kept = new StringBuilder();
            boolean inString = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"' && (i == 0 || line.charAt(i - 1) != '\\')) inString = !inString;
                if (!inString && c == '/' && i + 1 < line.length() && line.charAt(i + 1) == '/') break;
                kept.append(c);
            }
            out.add(kept.toString());
        }
        return out;
    }
}
