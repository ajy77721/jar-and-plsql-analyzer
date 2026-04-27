package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SourceEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(SourceEnrichmentService.class);

    private final DecompilerService decompilerService;
    private final ProgressService progressService;

    public SourceEnrichmentService(DecompilerService decompilerService,
                                   ProgressService progressService) {
        this.decompilerService = decompilerService;
        this.progressService = progressService;
    }

    /**
     * Walk all endpoint call trees and inject actual decompiled Java source code
     * into every CallNode. Decompiles each class once (cached per class), then
     * extracts individual methods. This ensures every level of the flow has real code.
     */
    public int enrichCallTreesWithSource(List<EndpointInfo> endpoints, Path jarPath) {
        final int cacheLimit = 1500;
        ConcurrentHashMap<String, String> classSourceCache = new ConcurrentHashMap<>(1024);
        ConcurrentLinkedQueue<String> accessOrder = new ConcurrentLinkedQueue<>();

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

        List<Future<?>> futures = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            final int idx = i;
            final EndpointInfo ep = endpoints.get(i);
            futures.add(pool.submit(() -> {
                int beforeMethods = methodCount.get();
                int beforeNodes = nodeCount.get();
                int beforeCache = classSourceCache.size();
                long epStart = System.currentTimeMillis();

                try {
                    Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
                    enrichNodeWithSource(ep.getCallTree(), jarPath, classSourceCache,
                            accessOrder, cacheLimit,
                            methodCount, nodeCount, newClassCount, failedClassCount, visited);
                } catch (Throwable t) {
                    log.warn("[5.6] ({}/{}) {} — FAILED: {}", idx + 1, total,
                            ep.getHttpMethod() + " " + ep.getFullPath(), t.getMessage());
                    return;
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
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                log.warn("[5.6] Endpoint enrichment failed: {}", e.getMessage());
            }
        }
        pool.shutdown();

        long totalMs = System.currentTimeMillis() - startTime;
        String summary = "Decompiled " + methodCount.get() + " methods across " + nodeCount.get() + " nodes"
                + " | " + classSourceCache.size() + " classes (" + failedClassCount.get() + " failed)"
                + " | " + (totalMs / 1000) + "s total";
        log.info("[5.6] DONE: {}", summary);
        progressService.detail("[5.6] DONE: " + summary);

        return methodCount.get();
    }

    private void enrichNodeWithSource(CallNode node, Path jarPath,
                                      ConcurrentHashMap<String, String> classSourceCache,
                                      ConcurrentLinkedQueue<String> accessOrder, int cacheLimit,
                                      AtomicInteger methodCount, AtomicInteger nodeCount,
                                      AtomicInteger newClassCount, AtomicInteger failedClassCount,
                                      Set<CallNode> visited) {
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
                    String src = decompilerService.decompile(jarPath, cls);
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
                enrichNodeWithSource(child, jarPath, classSourceCache,
                        accessOrder, cacheLimit,
                        methodCount, nodeCount, newClassCount, failedClassCount, visited);
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
