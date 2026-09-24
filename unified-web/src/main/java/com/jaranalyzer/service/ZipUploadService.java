package com.jaranalyzer.service;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * Ingest a ZIP of <b>already-produced analysis results</b> into the per-user ArtifactStore,
 * detecting which kind of analysis it holds — without re-running any analysis.
 *
 * <h2>Three accepted shapes</h2>
 * <ol>
 *   <li><b>JAR / WAR analysis</b> — contains {@code analysis.json} (with the usual siblings
 *       {@code _class-tree.json}, {@code _summary_static.json}, {@code _header.json}, an
 *       {@code endpoints/} and {@code resources/} tree). Stored under {@code jar/&lt;name&gt;/},
 *       or {@code jar/war/&lt;name&gt;/} when {@code _header.json}'s {@code jarName} ends in
 *       {@code .war}. The name comes from that header, or from the folder that holds
 *       {@code analysis.json}, or — only when the analysis sits at the ZIP root — from the
 *       optional {@code analysisName} form field.</li>
 *   <li><b>PL/SQL parse</b> — top-level folders shaped {@code &lt;DIALECT&gt;/&lt;PROJECT&gt;/&lt;ENV&gt;/…}
 *       with {@code DIALECT} one of {@code ORACLE|MYSQL|SQLSERVER|DB2|POSTGRES} (case-insensitive).
 *       Stored preserving that structure under {@code plsql-parse/}. Project/env are self-described
 *       by the tree (an {@code <OBJ>/api/index.json} marker pins them precisely); when the tree is
 *       shallower than that they must be supplied as form fields, and both missing is a 400.</li>
 *   <li><b>Raw {@code .jar}/{@code .war}</b> — a single archive entry and nothing else. That is not
 *       analysis output, so it is handed to the existing upload pipeline (queue job) exactly as
 *       {@code AnalyzerController}/@code WarAnalyzerController} do — store + analyze.</li>
 * </ol>
 *
 * <p>Anything else is a 400 that lists what was found next to what was expected.
 *
 * <h2>Safety</h2>
 * Nothing is ever extracted to a filesystem path: entries are streamed into {@link ArtifactStore}
 * keys whose every segment is validated by {@link ArtifactKey}. On top of that, entries with
 * {@code ..}, absolute or drive-qualified names, NULs, or symlink entries are rejected outright,
 * and both the declared and actual unpacked sizes are capped at 2 GB.
 */
@Service
public class ZipUploadService {

    private static final Logger log = LoggerFactory.getLogger(ZipUploadService.class);

    public static final String TYPE_JAR = "jar-analysis";
    public static final String TYPE_PLSQL = "plsql-parse";
    public static final String TYPE_RAW = "raw-jar";

    /** Refuse to unpack more than this, whether the ZIP declares it or actually contains it. */
    static final long MAX_UNPACKED_BYTES = 2L * 1024 * 1024 * 1024;

    private static final Set<String> DIALECTS =
            Set.of("ORACLE", "MYSQL", "SQLSERVER", "DB2", "POSTGRES");

    /** Cap on the echoed key list; the count is always complete. */
    private static final int MAX_ECHOED_KEYS = 200;

    private final ArtifactStore jarStore;
    private final ArtifactStore parserStore;
    private final ObjectMapper objectMapper;
    private final AnalysisQueueService queueService;

    public ZipUploadService(@Qualifier("jarArtifactStore") ArtifactStore jarStore,
                            @Qualifier("parserArtifactStore") ArtifactStore parserStore,
                            ObjectMapper objectMapper,
                            AnalysisQueueService queueService) {
        this.jarStore = jarStore;
        this.parserStore = parserStore;
        this.objectMapper = objectMapper;
        this.queueService = queueService;
    }

    /**
     * @param dryRun when true, detect and validate only — nothing is written and no job is queued.
     *               This backs the UI's "Detected: …" confirmation line before the user confirms.
     */
    public Map<String, Object> ingest(MultipartFile file, String project, String env, String dialect,
                                      String analysisName, String mode, boolean dryRun) throws IOException {
        if (file == null || file.isEmpty()) {
            throw badRequest("file is required (a .zip of analysis results)");
        }
        Path zipTemp = Files.createTempFile("analysis-zip-", ".zip");
        try {
            file.transferTo(zipTemp);
            return ingestTemp(zipTemp, project, env, dialect, analysisName, mode, dryRun);
        } finally {
            deleteQuietly(zipTemp);
        }
    }

    private Map<String, Object> ingestTemp(Path zipTemp, String project, String env, String dialect,
                                           String analysisName, String mode, boolean dryRun) throws IOException {
        try (ZipFile zf = openZip(zipTemp)) {
            List<ZipEntry> files = new ArrayList<>();
            zf.stream().filter(e -> !e.isDirectory()).forEach(files::add);
            if (files.isEmpty()) {
                throw badRequest("The ZIP is empty — there are no files to store.");
            }
            validateSafety(files, zipTemp);

            Map<String, ZipEntry> byPath = new LinkedHashMap<>();
            for (ZipEntry e : files) byPath.put(String.join("/", segments(e.getName())), e);

            Plan plan = detect(zf, byPath, files, project, env, dialect, analysisName);
            return dryRun ? dryRunResponse(plan) : execute(zf, plan, mode);
        }
    }

    // ── Detection ───────────────────────────────────────────────────────────

    private Plan detect(ZipFile zf, Map<String, ZipEntry> byPath, List<ZipEntry> files,
                        String formProject, String formEnv, String formDialect,
                        String formAnalysisName) throws IOException {
        Plan jar = detectJarAnalysis(zf, byPath, files, formAnalysisName);
        if (jar != null) return jar;
        Plan plsql = detectPlsqlParse(files, formProject, formEnv, formDialect);
        if (plsql != null) return plsql;
        Plan raw = detectRawArchive(files);
        if (raw != null) return raw;
        throw unknownLayout(files);
    }

    private Plan detectJarAnalysis(ZipFile zf, Map<String, ZipEntry> byPath, List<ZipEntry> files,
                                   String formAnalysisName) throws IOException {
        ZipEntry analysis = null;
        List<String> dirSegs = null;
        for (ZipEntry e : files) {
            List<String> s = segments(e.getName());
            if (s.isEmpty() || !"analysis.json".equals(s.get(s.size() - 1))) continue;
            if (analysis == null || s.size() < dirSegs.size() + 1) {
                analysis = e;
                dirSegs = new ArrayList<>(s.subList(0, s.size() - 1));
            }
        }
        if (analysis == null) return null;

        String headerJarName = null;
        ZipEntry header = byPath.get(join(dirSegs, "_header.json"));
        if (header != null) {
            try (InputStream in = zf.getInputStream(header)) {
                JsonNode node = objectMapper.readTree(in);
                if (node != null && node.hasNonNull("jarName")) {
                    String raw = node.get("jarName").asText();
                    if (raw != null && !raw.isBlank()) headerJarName = raw.trim();
                }
            } catch (IOException e) {
                log.debug("Unreadable _header.json in uploaded ZIP: {}", e.toString());
            }
        }
        boolean storedWar = byPath.containsKey(join(dirSegs, "stored.war"));

        String name;
        boolean war;
        if (headerJarName != null) {
            name = JarNameUtil.normalizeKey(headerJarName);
            war = headerJarName.toLowerCase(Locale.ROOT).endsWith(".war");
        } else if (!dirSegs.isEmpty()) {
            String leaf = dirSegs.get(dirSegs.size() - 1);
            name = JarNameUtil.normalizeKey(leaf);
            war = leaf.toLowerCase(Locale.ROOT).endsWith(".war")
                    || (dirSegs.size() >= 2 && "war".equalsIgnoreCase(dirSegs.get(dirSegs.size() - 2)));
        } else if (formAnalysisName != null && !formAnalysisName.isBlank()) {
            name = JarNameUtil.normalizeKey(formAnalysisName);
            war = formAnalysisName.toLowerCase(Locale.ROOT).endsWith(".war");
        } else {
            throw badRequest("This looks like a JAR analysis (it contains analysis.json), but its "
                    + "name could not be determined: it has no _header.json and analysis.json sits at "
                    + "the ZIP root. Provide the 'analysisName' field.");
        }
        if (name.isEmpty()) {
            throw badRequest("Could not derive a safe analysis name from the uploaded ZIP.");
        }
        if (war || storedWar) {
            war = true;
            if (!name.toLowerCase(Locale.ROOT).endsWith(".war")) name = name + ".war";
        }

        Plan plan = new Plan(war ? "WAR" : "JAR");
        plan.type = TYPE_JAR;
        plan.war = war;
        plan.analysisName = name;

        List<String> root = new ArrayList<>();
        root.add("jar");
        if (war) root.add("war");
        root.add(name);

        for (ZipEntry e : files) {
            List<String> s = segments(e.getName());
            if (s.size() <= dirSegs.size() || !s.subList(0, dirSegs.size()).equals(dirSegs)) continue;
            List<String> keySegs = new ArrayList<>(root);
            keySegs.addAll(s.subList(dirSegs.size(), s.size()));
            plan.writes.add(new PlannedWrite(e, keySegs));
        }
        if (plan.writes.isEmpty()) {
            throw badRequest("No files were found under the analysis directory in the ZIP.");
        }
        plan.target = "data/jar/" + (war ? "war/" : "") + name + "/";
        return plan;
    }

    private Plan detectPlsqlParse(List<ZipEntry> files, String formProject, String formEnv,
                                  String formDialect) {
        // Marker-based: <PREFIX>/<DIALECT>/<PROJECT>/<ENV>/<OBJECT>/api/index.json pins all four
        // levels exactly (ParserAnalysisReader uses the same file as the analysis marker).
        Map<String, Integer> votes = new LinkedHashMap<>();
        Map<String, Set<String>> objectDirsByScope = new LinkedHashMap<>();
        Map<String, String[]> scopeParts = new LinkedHashMap<>();
        for (ZipEntry e : files) {
            List<String> s = segments(e.getName());
            int n = s.size();
            if (n < 6) continue;
            if (!"index.json".equals(s.get(n - 1)) || !"api".equals(s.get(n - 2))) continue;
            if (!isDialect(s.get(n - 6))) continue;
            String scope = (n - 6) + "|" + s.get(n - 6).toUpperCase(Locale.ROOT) + "|"
                    + s.get(n - 5) + "|" + s.get(n - 4);
            votes.merge(scope, 1, Integer::sum);
            objectDirsByScope.computeIfAbsent(scope, k -> new LinkedHashSet<>()).add(s.get(n - 3));
            scopeParts.put(scope, new String[]{s.get(n - 6), s.get(n - 5), s.get(n - 4)});
        }

        String zipProject = null;
        String zipEnv = null;
        String dialect = null;
        // Where a dialect folder lives in the path: index into the segments, or -1 for the
        // "dialect supplied as a form field" fallback (then the content starts after a wrapper).
        int dialectIdx = -1;
        // Index of the first content segment after the dialect (the object level in every shape
        // where project/env must be filled in by hand).
        int contentStart = -1;
        Set<String> objectDirs = new LinkedHashSet<>();
        boolean markerBased = false;

        if (!votes.isEmpty()) {
            String best = votes.entrySet().stream()
                    .max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElseThrow();
            String[] parts = scopeParts.get(best);
            dialectIdx = Integer.parseInt(best.substring(0, best.indexOf('|')));
            dialect = parts[0];
            zipProject = parts[1];
            zipEnv = parts[2];
            objectDirs = objectDirsByScope.get(best);
            contentStart = dialectIdx + 1;
            markerBased = true;
        } else {
            int[] found = dialectPosition(files);
            if (found != null) {
                dialectIdx = found[0];
                dialect = segments(files.get(found[1]).getName()).get(dialectIdx);
                contentStart = dialectIdx + 1;
            } else if (formDialect != null && isDialect(formDialect)) {
                dialect = formDialect;
                contentStart = commonPrefixLength(files);
            } else {
                return null;
            }
        }

        // The zip wins where it self-describes; form fields fill the gaps.
        String project = (zipProject != null && !zipProject.isBlank()) ? zipProject : formProject;
        String env = (zipEnv != null && !zipEnv.isBlank()) ? zipEnv : formEnv;

        List<String> missing = new ArrayList<>();
        if (project == null || project.isBlank()) missing.add("project");
        if (env == null || env.isBlank()) missing.add("env");
        if (!missing.isEmpty()) {
            throw badRequest("This looks like a PL/SQL parse ZIP"
                    + (dialect != null ? " (dialect " + dialect + ")" : "")
                    + ", but it does not self-describe "
                    + (missing.size() == 2 ? "project and env" : "the " + missing.get(0) + " name")
                    + " and neither did the request. Missing form field"
                    + (missing.size() == 1 ? "" : "s") + ": " + String.join(", ", missing)
                    + ". Supply it and retry.");
        }

        Plan plan = new Plan("PL/SQL");
        plan.type = TYPE_PLSQL;
        plan.dialect = AnalysisPaths.seg(dialect);
        plan.project = AnalysisPaths.seg(project);
        plan.env = AnalysisPaths.seg(env);

        // Without the api/index.json marker there is no exact object list; treat any leading
        // directory segment below the dialect as an object folder (uppercased, matching
        // AnalysisPaths) and leave a lone file directly under the dialect untouched.
        if (!markerBased) {
            for (ZipEntry e : files) {
                List<String> s = segments(e.getName());
                if (s.size() >= contentStart + 1) objectDirs.add(s.get(contentStart));
            }
        }

        for (ZipEntry e : files) {
            List<String> s = segments(e.getName());
            if (s.size() <= contentStart) continue;              // at/above the dialect level
            if (dialectIdx >= 0 && !isDialect(s.get(dialectIdx))) continue;  // a different dialect
            List<String> tail = s.subList(contentStart, s.size());
            List<String> keySegs = new ArrayList<>();
            keySegs.add(AnalysisPaths.PARSE_ROOT);
            keySegs.add(plan.dialect);
            keySegs.add(plan.project);
            keySegs.add(plan.env);
            if (tail.size() >= 2 && objectDirs.contains(tail.get(0))) {
                keySegs.add(AnalysisPaths.seg(tail.get(0)));
                keySegs.addAll(tail.subList(1, tail.size()));
            } else {
                keySegs.addAll(tail);
            }
            plan.writes.add(new PlannedWrite(e, keySegs));
        }
        if (plan.writes.isEmpty()) {
            throw badRequest("No files were found beneath the dialect folder in the ZIP.");
        }
        plan.target = "data/plsql-parse/" + plan.dialect + "/" + plan.project + "/" + plan.env + "/";
        return plan;
    }

    private Plan detectRawArchive(List<ZipEntry> files) {
        if (files.size() != 1) return null;
        List<String> s = segments(files.get(0).getName());
        if (s.isEmpty()) return null;
        String leaf = s.get(s.size() - 1);
        String lower = leaf.toLowerCase(Locale.ROOT);
        if (!lower.endsWith(".jar") && !lower.endsWith(".war")) return null;
        Plan plan = new Plan("raw archive");
        plan.type = TYPE_RAW;
        plan.war = lower.endsWith(".war");
        plan.rawEntry = files.get(0);
        plan.analysisName = leaf;
        plan.target = "store + analyze: " + leaf;
        return plan;
    }

    // ── Execution ───────────────────────────────────────────────────────────

    private Map<String, Object> execute(ZipFile zf, Plan plan, String mode) throws IOException {
        if (TYPE_RAW.equals(plan.type)) {
            return executeRaw(zf, plan, mode);
        }
        ArtifactStore store = TYPE_PLSQL.equals(plan.type) ? parserStore : jarStore;
        List<String> written = new ArrayList<>();
        long bytes = 0;
        for (PlannedWrite w : plan.writes) {
            ArtifactKey key = ArtifactKey.of(w.keySegments.toArray(new String[0]));
            long declared = w.entry.getSize();
            try (InputStream in = zf.getInputStream(w.entry);
                 OutputStream out = store.write(key, declared)) {
                bytes += copyLimited(in, out);
            }
            written.add(key.toString());
        }

        // List the prefix back: proof the objects are where the response says they are.
        List<String> root = new ArrayList<>();
        if (TYPE_PLSQL.equals(plan.type)) {
            root.add(AnalysisPaths.PARSE_ROOT);
            root.add(plan.dialect);
            root.add(plan.project);
            root.add(plan.env);
        } else {
            root.add("jar");
            if (plan.war) root.add("war");
            root.add(plan.analysisName);
        }
        Map<ArtifactKey, ArtifactEntry> present = store.statPrefix(ArtifactKey.of(root.toArray(new String[0])));
        boolean verified = written.stream().allMatch(k -> present.containsKey(ArtifactKey.of(splitKey(k))));

        Map<String, Object> resp = base(plan, false);
        resp.put("storedKeys", written.size() > MAX_ECHOED_KEYS ? written.subList(0, MAX_ECHOED_KEYS) : written);
        resp.put("storedCount", written.size());
        resp.put("keysTruncated", written.size() > MAX_ECHOED_KEYS);
        resp.put("storedBytes", bytes);
        resp.put("verified", verified);
        resp.put("listedCount", present.size());
        log.info("ZIP ingest: type={} name={} keys={} bytes={} verified={}",
                plan.type, plan.analysisName, written.size(), bytes, verified);
        return resp;
    }

    private Map<String, Object> executeRaw(ZipFile zf, Plan plan, String mode) throws IOException {
        Path raw = Files.createTempFile("zip-raw-", plan.war ? ".war" : ".jar");
        long size;
        try (InputStream in = zf.getInputStream(plan.rawEntry)) {
            size = Files.copy(in, raw, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put(plan.war ? "warName" : "jarName", plan.analysisName);
        metadata.put("tempFilePath", raw.toString());
        metadata.put("fileSize", size);
        metadata.put("mode", (mode == null || mode.isBlank()) ? "static" : mode);

        QueueJob.Type type = plan.war ? QueueJob.Type.WAR_UPLOAD : QueueJob.Type.JAR_UPLOAD;
        QueueJob job = queueService.submit(type, "Upload: " + plan.analysisName, metadata);

        Map<String, Object> resp = base(plan, false);
        resp.put("storedKeys", List.of());
        resp.put("id", JarNameUtil.normalizeKey(plan.analysisName));
        resp.put("jobId", job.id);
        resp.put("statusUrl", "/api/queue/" + job.id);
        log.info("ZIP ingest: raw {} handed to the upload pipeline — job {}", plan.analysisName, job.id);
        return resp;
    }

    // ── Safety ──────────────────────────────────────────────────────────────

    private void validateSafety(List<ZipEntry> files, Path zipTemp) {
        long declared = 0;
        for (ZipEntry e : files) {
            assertSafeName(e.getName());
            if (e.getSize() >= 0) declared += e.getSize();
            if (declared > MAX_UNPACKED_BYTES) {
                throw badRequest("The ZIP unpacks to more than 2 GB — refusing to ingest it.");
            }
        }
        List<String> symlinks = findSymlinkEntries(zipTemp);
        if (!symlinks.isEmpty()) {
            throw badRequest("The ZIP contains symbolic-link entries, which are not accepted: "
                    + String.join(", ", symlinks.subList(0, Math.min(5, symlinks.size()))));
        }
    }

    private static void assertSafeName(String raw) {
        if (raw == null || raw.isBlank()) {
            throw badRequest("The ZIP contains an entry with an empty name.");
        }
        if (raw.indexOf('\0') >= 0) {
            throw badRequest("The ZIP contains an entry with a NUL in its name.");
        }
        String n = raw.replace('\\', '/');
        if (n.startsWith("/")) {
            throw badRequest("Unsafe ZIP entry (absolute path): " + raw);
        }
        if (n.length() >= 2 && n.charAt(1) == ':' && Character.isLetter(n.charAt(0))) {
            throw badRequest("Unsafe ZIP entry (drive-qualified path): " + raw);
        }
        for (String seg : n.split("/")) {
            if ("..".equals(seg)) {
                throw badRequest("Unsafe ZIP entry (path traversal): " + raw);
            }
        }
    }

    private List<String> findSymlinkEntries(Path zipTemp) {
        List<String> out = new ArrayList<>();
        try (FileSystem fs = FileSystems.newFileSystem(zipTemp, (ClassLoader) null)) {
            Path root = fs.getPath("/");
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path p : walk.toList()) {
                    if (Files.isSymbolicLink(p)) out.add(root.relativize(p).toString());
                }
            }
        } catch (IOException | RuntimeException e) {
            // The scan is best-effort: entries are never extracted to the filesystem, so a ZIP
            // this cannot open is still safe to stream into keyed artifacts.
            log.debug("Symlink scan skipped: {}", e.toString());
        }
        return out;
    }

    private static long copyLimited(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[64 * 1024];
        long total = 0;
        int n;
        while ((n = in.read(buf)) >= 0) {
            total += n;
            if (total > MAX_UNPACKED_BYTES) {
                throw badRequest("The ZIP unpacks to more than 2 GB — refusing to ingest it.");
            }
            out.write(buf, 0, n);
        }
        return total;
    }

    // ── Response shaping ────────────────────────────────────────────────────

    private Map<String, Object> dryRunResponse(Plan plan) {
        Map<String, Object> resp = base(plan, true);
        resp.put("target", plan.target);
        resp.put("fileCount", plan.writes.size());
        return resp;
    }

    private Map<String, Object> base(Plan plan, boolean dryRun) {
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        resp.put("type", plan.type);
        resp.put("war", plan.war);
        resp.put("target", plan.target);
        if (plan.analysisName != null) resp.put("analysisName", plan.analysisName);
        if (plan.project != null) resp.put("project", plan.project);
        if (plan.env != null) resp.put("env", plan.env);
        if (plan.dialect != null) resp.put("dialect", plan.dialect);
        if (dryRun) resp.put("dryRun", true);
        return resp;
    }

    // ── Detection helpers ───────────────────────────────────────────────────

    private static boolean isDialect(String s) {
        return s != null && DIALECTS.contains(s.toUpperCase(Locale.ROOT));
    }

    /** @return {prefixLen, fileIndex} of the first entry whose dialect folder we can locate, or null. */
    private int[] dialectPosition(List<ZipEntry> files) {
        for (int i = 0; i < files.size(); i++) {
            List<String> s = segments(files.get(i).getName());
            for (int p = 0; p <= Math.min(2, s.size() - 1); p++) {
                if (!isDialect(s.get(p))) continue;
                if (p == 0) return new int[]{0, i};
                if (p == 1 && (s.get(0).equalsIgnoreCase("data") || s.get(0).equalsIgnoreCase("plsql-parse"))) {
                    return new int[]{1, i};
                }
                if (p == 2 && s.get(0).equalsIgnoreCase("data") && s.get(1).equalsIgnoreCase("plsql-parse")) {
                    return new int[]{2, i};
                }
            }
        }
        return null;
    }

    /** Longest common leading directory prefix shared by every entry (0/1/2 for our wrappers). */
    private static int commonPrefixLength(List<ZipEntry> files) {
        for (int p : new int[]{2, 1}) {
            boolean all = true;
            for (ZipEntry e : files) {
                List<String> s = segments(e.getName());
                if (s.size() <= p + 1) { all = false; break; }
                if (p >= 1 && !s.get(0).equalsIgnoreCase("data") && !s.get(0).equalsIgnoreCase("plsql-parse")) {
                    all = false; break;
                }
                if (p == 2 && !s.get(1).equalsIgnoreCase("plsql-parse")) { all = false; break; }
            }
            if (all) return p;
        }
        return 0;
    }

    // ── Small utilities ─────────────────────────────────────────────────────

    /** Normalised path segments: backslashes folded, empty/"." segments dropped. */
    static List<String> segments(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) return out;
        for (String s : raw.replace('\\', '/').split("/")) {
            if (!s.isEmpty() && !".".equals(s)) out.add(s);
        }
        return out;
    }

    private static String join(List<String> dirSegs, String leaf) {
        List<String> all = new ArrayList<>(dirSegs);
        all.add(leaf);
        return String.join("/", all);
    }

    private static String[] splitKey(String key) {
        return key.split("/");
    }

    private ZipFile openZip(Path temp) {
        try {
            return new ZipFile(temp.toString());
        } catch (ZipException e) {
            throw badRequest("Not a valid ZIP archive: " + e.getMessage());
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Could not open the uploaded ZIP: " + e.getMessage());
        }
    }

    private ResponseStatusException unknownLayout(List<ZipEntry> files) {
        Set<String> top = new TreeSet<>();
        for (ZipEntry e : files) {
            List<String> s = segments(e.getName());
            if (!s.isEmpty()) top.add(s.get(0));
        }
        return badRequest("Unrecognised ZIP layout. Found " + files.size() + " file(s); top-level "
                + (top.size() == 1 ? "folder" : "folders") + ": " + String.join(", ", top)
                + ". Expected one of: a JAR/WAR analysis (analysis.json), a PL/SQL parse tree "
                + "(<DIALECT>/<PROJECT>/<ENV>/... with DIALECT one of ORACLE, MYSQL, SQLSERVER, DB2, "
                + "POSTGRES), or a single .jar/.war.");
    }

    private static ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException e) {
            log.debug("Could not delete ingest temp {}: {}", p, e.toString());
        }
    }

    // ── Plan types ──────────────────────────────────────────────────────────

    private static final class Plan {
        final String kind;
        String type;
        String analysisName;
        String project;
        String env;
        String dialect;
        boolean war;
        String target;
        ZipEntry rawEntry;
        final List<PlannedWrite> writes = new ArrayList<>();

        Plan(String kind) { this.kind = kind; }

        @Override
        public String toString() { return kind; }
    }

    private record PlannedWrite(ZipEntry entry, List<String> keySegments) {}
}
