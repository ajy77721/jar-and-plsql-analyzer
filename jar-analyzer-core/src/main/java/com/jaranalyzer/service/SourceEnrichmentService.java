package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedSsoContext;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SourceEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(SourceEnrichmentService.class);

    private final DecompilerService decompilerService;
    private final ProgressService progressService;

    /**
     * The store the decompile cache reads/writes through. Held here (not only inside
     * {@link DecompilerService}) so the enrich pool can capture the caller's routing scope and
     * re-install it per task. The same {@code jarArtifactStore} bean the decompiler injects.
     */
    private final ArtifactStore store;

    public SourceEnrichmentService(DecompilerService decompilerService,
                                   ProgressService progressService,
                                   @org.springframework.beans.factory.annotation.Qualifier("jarArtifactStore") ArtifactStore store) {
        this.decompilerService = decompilerService;
        this.progressService = progressService;
        this.store = store;
    }

    /**
     * Walk all endpoint call trees and inject actual decompiled Java source code
     * into every CallNode. Decompiles each class once (cached per class), then
     * extracts individual methods. This ensures every level of the flow has real code.
     */
    /**
     * Enrich against a stored jar <b>key</b>. Preferred over {@link #enrichCallTreesWithSource(List,
     * Path)}: the key is the artifact's stable identity, so decompilation goes through
     * {@link DecompilerService#decompile(com.analyzer.storage.ArtifactKey, String)} — correct for
     * the decompile cache, and immune to the remote-storage trap where {@code materialize()} hands
     * back a temp path ({@code /tmp/artifact-*-stored.jar}) that the data-root containment check
     * cannot key and so refuses for every class.
     */
    public int enrichCallTreesWithSource(List<EndpointInfo> endpoints,
                                         com.analyzer.storage.ArtifactKey jarKey) {
        return enrich(endpoints, jarKey, null, null);
    }

    /**
     * As {@link #enrichCallTreesWithSource(List, com.analyzer.storage.ArtifactKey)} but handed the
     * artifact's <b>already-resolved local jar file</b> (the queue executors materialize it once and
     * hold it open for the rest of the job). Every class then decompiles from that one open file
     * instead of re-reading it per class; {@code jarKey} remains the decompile-cache identity.
     * Pass {@code null} to have this service resolve the file once itself.
     */
    public int enrichCallTreesWithSource(List<EndpointInfo> endpoints,
                                         com.analyzer.storage.ArtifactKey jarKey, Path localJar) {
        return enrich(endpoints, jarKey, localJar, null);
    }

    /** Path-based variant, retained for callers that genuinely only have a local file. */
    public int enrichCallTreesWithSource(List<EndpointInfo> endpoints, Path jarPath) {
        return enrich(endpoints, null, null, jarPath);
    }

    private int enrich(List<EndpointInfo> endpoints,
                       com.analyzer.storage.ArtifactKey jarKey, Path localJar, Path jarPath) {
        // Per-analysis rate limit: one bad path must not emit one identical WARN per class.
        decompilerService.resetGuardWarnCounter();
        final int cacheLimit = 1500;
        ConcurrentHashMap<String, String> classSourceCache = new ConcurrentHashMap<>(1024);
        ConcurrentLinkedQueue<String> accessOrder = new ConcurrentLinkedQueue<>();

        // The caller is a queue worker whose installed job context makes the submitter's stores and
        // credentials resolvable; the [source-enrich] pool threads below have neither. Capture HERE —
        // before any task is submitted, on the thread that owns the answer — and re-install per task,
        // or every decompile-cache read/write and jar read routes/signs as the shared default target
        // (a pod with no ambient credentials then fails every class on a ~2s IMDS timeout). Capture
        // both halves: the job snapshot (credentials + stores) and the routing scope (store
        // session). Null-safe: nothing captured means install is a no-op, preserving legacy/
        // no-SSO behaviour exactly.
        final CapturedSsoContext jobContext = CapturedSsoContext.capture(null);
        final RequestScopedArtifactStore.Scope routingScope = store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;

        // Resolve the jar's local file ONCE for the whole job: every class then decompiles from the
        // same open JarFile rather than re-materializing the artifact per class. Prefer the file the
        // caller already holds (the queue executors materialize it once and keep it open); otherwise
        // pin it here. Null means "no local file resolvable" — the per-class key/path path is kept.
        DecompilerService.PinnedJar pinned = null;
        Path effectiveLocalJar = localJar;
        if (effectiveLocalJar == null && jarKey != null) {
            pinned = decompilerService.pinJar(jarKey);
            if (pinned != null) effectiveLocalJar = pinned.path();
        }
        final Path localJarFile = effectiveLocalJar;

        AtomicInteger methodCount = new AtomicInteger();
        AtomicInteger nodeCount = new AtomicInteger();
        AtomicInteger newClassCount = new AtomicInteger();
        AtomicInteger failedClassCount = new AtomicInteger();
        AtomicInteger doneCount = new AtomicInteger();
        int total = endpoints.size();
        long startTime = System.currentTimeMillis();

        int threads = Math.min(4, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "source-enrich");
            t.setDaemon(true);
            return t;
        });

        try {
            List<Future<?>> futures = new ArrayList<>(total);
            for (int i = 0; i < total; i++) {
                final int idx = i;
                final EndpointInfo ep = endpoints.get(i);
                // Explicit Callable: the try-with-resources close() can throw, which a Runnable cannot.
                futures.add(pool.submit((Callable<Void>) () -> {
                    int beforeMethods = methodCount.get();
                    int beforeNodes = nodeCount.get();
                    int beforeCache = classSourceCache.size();
                    long epStart = System.currentTimeMillis();

                    try (AutoCloseable routedCtx = routingScope.install();
                         AutoCloseable ssoCtx = jobContext == null ? null : jobContext.install()) {
                        try {
                            Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
                            enrichNodeWithSource(ep.getCallTree(), jarKey, localJarFile, jarPath,
                                    classSourceCache, accessOrder, cacheLimit,
                                    methodCount, nodeCount, newClassCount, failedClassCount, visited,
                                    ep.getFragments());
                        } catch (Throwable t) {
                            log.warn("[5.6] ({}/{}) {} — FAILED: {}", idx + 1, total,
                                    ep.getHttpMethod() + " " + ep.getFullPath(), t.getMessage());
                            return null;
                        }
                    }

                    int methodsThisEp = methodCount.get() - beforeMethods;
                    int nodesThisEp = nodeCount.get() - beforeNodes;
                    int newClasses = classSourceCache.size() - beforeCache;
                    long epMs = System.currentTimeMillis() - epStart;
                    int done = doneCount.incrementAndGet();
                    String label = ep.getHttpMethod() + " " + ep.getFullPath();

                    log.info("[5.6] ({}/{}) {} — {} nodes, {} methods decompiled, {} new classes ({}ms)",
                            done, total, label, nodesThisEp, methodsThisEp, newClasses, epMs);
                    if (done % 10 == 0 || done == total) {
                        progressService.detail("[5.6] (" + done + "/" + total + ") "
                                + methodCount.get() + " methods | " + classSourceCache.size() + " classes cached"
                                + " | " + failedClassCount.get() + " failed");
                    }
                    return null;
                }));
            }

            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    log.warn("[5.6] Endpoint enrichment failed: {}", e.getMessage());
                }
            }
        } finally {
            pool.shutdown();
            if (pinned != null) pinned.close();
        }

        long totalMs = System.currentTimeMillis() - startTime;
        String summary = "Decompiled " + methodCount.get() + " methods across " + nodeCount.get() + " nodes"
                + " | " + classSourceCache.size() + " classes (" + failedClassCount.get() + " failed)"
                + " | " + (totalMs / 1000) + "s total";
        log.info("[5.6] DONE: {}", summary);
        progressService.detail("[5.6] DONE: " + summary);

        return methodCount.get();
    }

    private void enrichNodeWithSource(CallNode node,
                                      com.analyzer.storage.ArtifactKey jarKey, Path localJar, Path jarPath,
                                      ConcurrentHashMap<String, String> classSourceCache,
                                      ConcurrentLinkedQueue<String> accessOrder, int cacheLimit,
                                      AtomicInteger methodCount, AtomicInteger nodeCount,
                                      AtomicInteger newClassCount, AtomicInteger failedClassCount,
                                      Set<CallNode> visited, Map<String, List<CallNode>> fragments) {
        if (node == null || !visited.add(node)) return;
        nodeCount.incrementAndGet();

        String className = node.getClassName();
        String methodName = node.getMethodName();

        if (className != null && methodName != null && node.getSourceCode() == null
                && !isFrameworkClass(className)) {
            try {
                boolean[] wasNew = {false};
                String fullSource = classSourceCache.computeIfAbsent(className, cls -> {
                    wasNew[0] = true;
                    // Prefer the one local file resolved for the whole job (key still gives the
                    // cache identity); fall back, null-safe, to the per-class key/path behaviour.
                    String src;
                    if (jarKey != null && localJar != null) {
                        src = decompilerService.decompile(localJar, jarKey, cls);
                    } else if (jarKey != null) {
                        src = decompilerService.decompile(jarKey, cls);
                    } else {
                        src = decompilerService.decompile(jarPath, cls);
                    }
                    if (src == null) {
                        failedClassCount.incrementAndGet();
                    }
                    return src != null ? src : "";
                });
                if (wasNew[0]) {
                    accessOrder.add(className);
                    while (classSourceCache.size() > cacheLimit) {
                        String oldest = accessOrder.poll();
                        if (oldest != null) classSourceCache.remove(oldest);
                    }
                    if (!fullSource.isEmpty()) {
                        newClassCount.incrementAndGet();
                    }
                }

                if (!fullSource.isEmpty()) {
                    String methodSource = decompilerService.extractMethod(
                            fullSource, methodName, node.getParameterTypes());
                    if (methodSource != null) {
                        if (methodSource.length() > 8192) {
                            methodSource = methodSource.substring(0, 8192) + "\n// ... truncated ...";
                        }
                        node.setSourceCode(methodSource);
                        if (node.getLineCount() == 0) {
                            node.setLineCount((int) methodSource.lines().count());
                        }
                        methodCount.incrementAndGet();
                    }
                }
            } catch (Throwable t) {
                log.warn("Decompile failed for {}.{}: {}", className, methodName, t.getMessage());
            }
        }

        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                enrichNodeWithSource(child, jarKey, localJar, jarPath, classSourceCache,
                        accessOrder, cacheLimit,
                        methodCount, nodeCount, newClassCount, failedClassCount, visited, fragments);
            }
        }
        // Fragment overflow — same node's children that were moved out of the inline tree by
        // the max-children-per-node split. They still need source decompiled.
        if (node.getFragmentRef() != null && fragments != null) {
            List<CallNode> overflow = fragments.get(node.getFragmentRef());
            if (overflow != null) {
                for (CallNode child : overflow) {
                    enrichNodeWithSource(child, jarKey, localJar, jarPath, classSourceCache,
                            accessOrder, cacheLimit,
                            methodCount, nodeCount, newClassCount, failedClassCount, visited, fragments);
                }
            }
        }
    }

    private static boolean isFrameworkClass(String className) {
        return className.startsWith("org.springframework.")
                || className.startsWith("org.apache.")
                || className.startsWith("com.fasterxml.")
                || className.startsWith("org.hibernate.")
                || className.startsWith("org.slf4j.")
                || className.startsWith("ch.qos.logback.")
                || className.startsWith("com.mongodb.internal.")
                || className.startsWith("com.mongodb.client.")
                || className.startsWith("io.netty.")
                || className.startsWith("reactor.")
                || className.startsWith("java.")
                || className.startsWith("javax.")
                || className.startsWith("jakarta.")
                || className.startsWith("com.sun.")
                || className.startsWith("sun.")
                || className.startsWith("jdk.")
                || className.startsWith("org.bouncycastle.")
                || className.startsWith("com.google.")
                || className.startsWith("io.swagger.")
                || className.startsWith("org.springdoc.")
                || className.startsWith("io.micrometer.")
                || className.startsWith("org.aspectj.");
    }
}
