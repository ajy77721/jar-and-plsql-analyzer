package com.jaranalyzer.service;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Compiles the committed scratch-fixture sample apps at test time, replacing the pre-June-2026
 * arrangement those tests originally used: sample apps hand-compiled into a per-session
 * {@code /private/tmp/claude-502/<workspace-hash>/scratchpad/...} — paths that died the moment the
 * workspace moved and tmp was cleaned (44 tests failing on one NoSuchFile). The sources now live in
 * {@code src/test/resources/scratch/<fixture>/<app>/*.java} (committed); this helper compiles them,
 * with {@code -g} like the originals, into a fresh temp dir and hands back the classes dir.
 *
 * <p>Each fixture dir may contain an {@code _stubs/} subdir — minimal same-FQN stub types for
 * libraries NOT on this module's test classpath (e.g. spring-data-elasticsearch's
 * {@code ElasticsearchOperations}; the detectors match owner-class simple names, so a same-FQN
 * stub detects identically — the §204 precedent). Stubs compile once into a shared dir that sits
 * on the classpath of every app in the fixture, but each app's OWN classes land in separate
 * output dirs, so a test walking one app's dir sees only that app's classes.
 *
 * <p>The compile classpath is the runtime classpath of the test JVM itself (built from the code
 * sources of the marker types the fixtures touch) — the same trick the original sessions used
 * ("compiled against this repo's own spring-web/mongodb-driver-sync deps"), minus the /tmp.
 */
final class ScratchCompiler {

    private ScratchCompiler() {}

    private static Path compiledRoot;   // memo: one temp root per JVM

    /**
     * Compiles every {@code .java} under {@code /scratch/<fixture>/<app>/} (plus the fixture's
     * shared {@code _stubs/}) and returns the directory holding that app's compiled classes.
     */
    static synchronized Path compile(String fixture, String app) throws IOException {
        Path resources = fixtureResources(fixture);
        Path stubsSrc = resources.resolve("_stubs");
        Path root = compiledRoot();
        Path stubsOut = root.resolve(fixture + "/_stubs");
        if (Files.isDirectory(stubsSrc) && !Files.exists(stubsOut.resolve("_done"))) {
            compileAll(sourcesIn(stubsSrc), classpath(), stubsOut);
            Files.createFile(stubsOut.resolve("_done"));
        }
        Path appSrc = resources.resolve(app);
        if (!Files.isDirectory(appSrc)) {
            throw new IOException("no such scratch fixture app: " + fixture + "/" + app);
        }
        Path appOut = root.resolve(fixture + "/" + app);
        if (!Files.exists(appOut.resolve("_done"))) {
            List<Path> sources = sourcesIn(appSrc);
            List<File> cp = new ArrayList<>(classpath());
            if (Files.isDirectory(stubsOut)) cp.add(stubsOut.toFile());
            compileAll(sources, cp, appOut);
            Files.createFile(appOut.resolve("_done"));
        }
        return appOut;
    }

    private static Path compiledRoot() throws IOException {
        if (compiledRoot == null) {
            compiledRoot = Files.createTempDirectory("scratch-fixtures");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try (Stream<Path> walk = Files.walk(compiledRoot)) {
                    walk.sorted(java.util.Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
                } catch (IOException ignored) {}
            }));
        }
        return compiledRoot;
    }

    private static Path fixtureResources(String fixture) throws IOException {
        var url = ScratchCompiler.class.getResource("/scratch/" + fixture);
        if (url == null) {
            throw new IOException("no such scratch fixture: " + fixture
                + " (expected src/test/resources/scratch/" + fixture + "/ committed in the repo)");
        }
        try {
            return Path.of(url.toURI());
        } catch (java.net.URISyntaxException e) {
            throw new IOException("bad fixture resource URI: " + url, e);
        }
    }

    private static List<Path> sourcesIn(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(p -> p.toString().endsWith(".java")).toList();
        }
    }

    private static void compileAll(List<Path> sources, List<File> classpath, Path out) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("tests need a JDK (ToolProvider.getSystemJavaCompiler); "
                + "surefire must run under a JDK, not a JRE");
        }
        Files.createDirectories(out);
        List<String> args = new ArrayList<>();
        args.add("-g");                       // full debug info — the fixtures' whole point (§204)
        args.add("-nowarn");
        args.add("-cp");
        args.add(join(classpath));
        args.add("-d");
        args.add(out.toString());
        try (var fm = compiler.getStandardFileManager(null, null, null)) {
            var units = fm.getJavaFileObjectsFromFiles(sources.stream().map(Path::toFile).toList());
            var diagnostics = new javax.tools.DiagnosticCollector<javax.tools.JavaFileObject>();
            var task = compiler.getTask(null, fm, diagnostics, args, null, units);
            if (!task.call()) {
                String details = diagnostics.getDiagnostics().stream()
                        .map(d -> d.getLineNumber() + ": " + d.getMessage(null))
                        .reduce((a, b) -> a + "\n" + b).orElse("(no diagnostics)");
                throw new IOException("scratch fixture failed to compile:\n" + details
                        + "\nsources: " + sources);
            }
        }
    }

    private static String join(List<File> cp) {
        StringBuilder sb = new StringBuilder();
        for (File f : cp) {
            if (sb.length() > 0) sb.append(File.pathSeparator);
            sb.append(f.getAbsolutePath());
        }
        return sb.toString();
    }

    /** The runtime classpath, as the code sources of marker types the fixtures bind against. */
    private static List<File> classpath() {
        List<File> cp = new ArrayList<>();
        // the module itself + its test classes
        cp.add(codeSourceOf(ScratchCompiler.class));
        // spring-web annotations (RestController/GetMapping) — a real test dependency
        cp.add(codeSourceOf(org.springframework.web.bind.annotation.GetMapping.class));
        // mongodb driver (Document) — a real test dependency
        cp.add(codeSourceOf(org.bson.Document.class));
        // mongodb driver-sync (MongoDatabase/MongoCollection) — the native-driver fixtures
        cp.add(codeSourceOf(com.mongodb.client.MongoDatabase.class));
        // spring-core — spring-web types (RestTemplate) extend NestedRuntimeException's hierarchy
        cp.add(codeSourceOf(org.springframework.core.NestedRuntimeException.class));
        return cp.stream().distinct().filter(java.util.Objects::nonNull).toList();
    }

    private static File codeSourceOf(Class<?> type) {
        CodeSource cs = type.getProtectionDomain().getCodeSource();
        if (cs == null || cs.getLocation() == null) return null;
        try {
            return new File(cs.getLocation().toURI());
        } catch (IllegalArgumentException | java.net.URISyntaxException e) {
            return null;
        }
    }
}
