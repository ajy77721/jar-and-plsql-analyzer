package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.repo.ArtifactScanRepository;
import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.s3.S3ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.AnalysisReaderFactory;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import com.plsqlanalyzer.web.parser.service.AnalysisViewStore;
import com.plsqlanalyzer.web.parser.service.ClaudeAnalysisReader;
import com.plsqlanalyzer.web.parser.service.ClaudePersistenceService;
import com.plsqlanalyzer.web.parser.service.ComplexityScoringService;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import com.plsqlanalyzer.web.parser.service.ProfilingArtifactStore;
import com.plsqlanalyzer.web.parser.service.StaticAnalysisReader;
import com.plsqlanalyzer.web.parser.service.SummaryStatsService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

/**
 * TEST-SCOPE profiling harness. NOT an assertion suite: it drives the REAL endpoint code path
 * ({@code ParserAnalysisController}) over the REAL staging S3 analysis and prints PROFILE tables.
 *
 * <p>Read-only against production: the profiling store denies every mutation, and the cold
 * scenario hides the {@code views/} sidecar prefix so the legacy node-scan runs without deleting
 * anything.
 *
 * <p>Run explicitly: {@code mvn -pl unified-web test -Dtest=ProfilingHarnessTest}.
 */
@org.junit.jupiter.api.Disabled("MANUAL TOOL, not a test: profiles the real S3 analysis (needs AWS SSO live) — run explicitly via -Dtest=ProfilingHarnessTest when profiling")
class ProfilingHarnessTest {

    private static final ObjectMapper M = new ObjectMapper();
    /** The real staging analysis: s3://amp-analyzer-parse-jar/data/plsql-parse/POSTGRES/... */
    private static final String NAME = "POSTGRES~POSTGRESDEMO~LOCAL~ERP_USP_RUN_CYCLE";
    private static final String BUCKET = "amp-analyzer-parse-jar";
    private static final String PREFIX = "data";

    private static final StringBuilder SUMMARY = new StringBuilder();

    private record Profile(String view, String mode, int http, long wallMs,
                           long ops, long gets, long heads, long lists, long bytes) {}

    /* ───────────────────────────── wiring ───────────────────────────── */

    private static ParserAnalysisController buildController(ArtifactStore store) {
        AnalysisViewStore views = new AnalysisViewStore(store);
        StaticAnalysisReader staticReader = new StaticAnalysisReader(store);
        ClaudeAnalysisReader claude = new ClaudeAnalysisReader(staticReader,
                new ClaudePersistenceService(store));
        AnalysisReaderFactory factory = new AnalysisReaderFactory(staticReader, claude);
        ArtifactScanRepository repository = new ArtifactScanRepository(store, store, M, factory,
                staticReader, views, null, new com.jaranalyzer.service.JarDataPaths("data/jar", store));
        AnalysisService service = new AnalysisService("data", factory, staticReader, views,
                mock(com.plsqlanalyzer.config.PlsqlConfig.class),
                new ClaudePersistenceService(store), store, repository,
                mock(com.plsqlanalyzer.web.controller.detail.ObjectResolutionService.class),
                null, mock(com.analyzer.usercfg.UserConnectionStore.class));
        ParserScopeResolver scopes = new ParserScopeResolver(service);
        return buildController(service, views, repository, scopes);
    }

    private static ParserAnalysisController buildController(AnalysisService service,
                                                            AnalysisViewStore views,
                                                            AnalysisRepository repository,
                                                            ParserScopeResolver scopes) {
        try {
            Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
            Parameter[] params = ctor.getParameters();
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                Class<?> type = params[i].getType();
                if (ObjectMapper.class.equals(type)) args[i] = M;
                else if (AnalysisService.class.equals(type)) args[i] = service;
                else if (AnalysisViewStore.class.equals(type)) args[i] = views;
                else if (AnalysisRepository.class.equals(type)) args[i] = repository;
                else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(M, 10000);
                else if (ParserScopeResolver.class.equals(type)) args[i] = scopes;
                else if (SummaryStatsService.class.equals(type)) args[i] = new SummaryStatsService(service, scopes, M);
                else if (ComplexityScoringService.class.equals(type)) args[i] = new ComplexityScoringService(service, M);
                else if (JoinComplexityConfig.class.equals(type)) args[i] = new JoinComplexityConfig();
                else if (type.isPrimitive()) args[i] = (type == boolean.class) ? false : 0;
                else args[i] = mock(type);
            }
            return (ParserAnalysisController) ctor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* ───────────────────────────── driver ───────────────────────────── */

    private static Profile runOne(String view, String mode, boolean hideSidecar,
                                  Function<ParserAnalysisController, ResponseEntity<?>> call) {
        S3Client client = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        try {
            String mask = hideSidecar ? AnalysisPaths.resolveKey(NAME, "views").toString() : null;
            ProfilingArtifactStore profiler = new ProfilingArtifactStore(new S3ArtifactStore(client, BUCKET, PREFIX), true, mask);
            ParserAnalysisController controller = buildController(profiler);

            long t0 = System.nanoTime();
            ResponseEntity<?> resp = call.apply(controller);
            long wallMs = (System.nanoTime() - t0) / 1_000_000;
            int http = resp != null ? resp.getStatusCode().value() : -1;

            String label = view + " [" + mode + "]  wall=" + wallMs + " ms  http=" + http;
            System.out.println(profiler.report(label));
            System.out.println("duplicate-key touches:\n" + profiler.duplicates());

            long gets = profiler.getOps(), heads = profiler.headOps(), lists = profiler.listOps();
            Profile p = new Profile(view, mode, http, wallMs, profiler.totalOps(),
                    gets, heads, lists, profiler.bytesRead());
            SUMMARY.append(String.format(java.util.Locale.ROOT,
                    "%-14s %-24s wall=%6d ms  http=%d  ops=%5d  GET=%4d  HEAD=%5d  LIST=%2d  bytes=%9d%n",
                    p.view(), p.mode(), p.wallMs(), p.http(), p.ops(), p.gets(), p.heads(), p.lists(), p.bytes()));
            return p;
        } finally {
            client.close();
        }
    }

    @Test
    void profileSequencesJoinsCursors() {
        runOne("sequences", "COLD legacy (no sidecar)", true,
                c -> c.getSequences(NAME, null, null, null, null, null, null, null, null, null, null));
        runOne("sequences", "WARM sidecar present", false,
                c -> c.getSequences(NAME, null, null, null, null, null, null, null, null, null, null));
        runOne("joins-summary", "COLD legacy (no sidecar)", true,
                c -> c.getJoinsSummary(NAME, null, null));
        runOne("joins-summary", "WARM sidecar present", false,
                c -> c.getJoinsSummary(NAME, null, null));
        runOne("cursors", "COLD legacy (no sidecar)", true,
                c -> c.getCursors(NAME, null, null, null, null, null, null, null, null, null, null, null, null));
        runOne("cursors", "WARM sidecar present", false,
                c -> c.getCursors(NAME, null, null, null, null, null, null, null, null, null, null, null, null));
        System.out.println("\n===== SUMMARY =====\n" + SUMMARY);
    }

    /**
     * Measures the parallelisation ceiling directly: read every {@code api/nodes/*.json} of the
     * real analysis sequentially, then with 8 and 16 worker threads, over the same S3 client. This
     * is the measured basis for the "parallel per-node reads" saving — no code change to the app.
     */
    @Test
    void parallelReadBenchmark() throws Exception {
        S3Client client = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        try {
            ArtifactStore store = new S3ArtifactStore(client, BUCKET, PREFIX);
            com.analyzer.storage.ArtifactKey nodesDir = AnalysisPaths.resolveKey(NAME, "api/nodes");
            java.util.List<com.analyzer.storage.ArtifactKey> keys = new java.util.ArrayList<>();
            try (var s = store.list(nodesDir, false)) {
                s.filter(e -> !e.directory()).filter(e -> e.key().leaf().endsWith(".json"))
                        .forEach(e -> keys.add(e.key()));
            }
            System.out.println("\n===== PARALLEL READ BENCHMARK (" + keys.size() + " node GETs) =====");

            long t = System.nanoTime();
            long bytes = 0;
            for (var k : keys) {
                try (var in = store.read(k)) {
                    byte[] buf = new byte[65536];
                    int n;
                    while ((n = in.read(buf)) > 0) bytes += n;
                }
            }
            long seq = (System.nanoTime() - t) / 1_000_000;
            System.out.printf("sequential (1 thread) : %,6d ms  %,d bytes%n", seq, bytes);

            for (int threads : new int[]{8, 16}) {
                var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
                t = System.nanoTime();
                var futures = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
                for (var k : keys) {
                    futures.add(pool.submit(() -> {
                        long b = 0;
                        try (var in = store.read(k)) {
                            byte[] buf = new byte[65536];
                            int n;
                            while ((n = in.read(buf)) > 0) b += n;
                        }
                        return b;
                    }));
                }
                long total = 0;
                for (var f : futures) total += f.get();
                long par = (System.nanoTime() - t) / 1_000_000;
                pool.shutdown();
                System.out.printf("parallel (%2d threads): %,6d ms  speedup %.1fx  (%,d bytes)%n",
                        threads, par, seq / (double) par, total);
            }
        } finally {
            client.close();
        }
    }
}
