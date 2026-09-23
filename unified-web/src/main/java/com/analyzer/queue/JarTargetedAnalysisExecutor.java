package com.analyzer.queue;

import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.service.CallGraphService;
import com.jaranalyzer.service.CallGraphService.MethodRef;
import com.jaranalyzer.service.JarParserService;
import com.jaranalyzer.service.PersistenceService;
import com.jaranalyzer.service.RepositoryStereotypeResolver;
import com.jaranalyzer.service.SourceEnrichmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Targeted "developer analysis": builds COMPLETE call trees for ONLY the supplied entry
 * methods (not the whole project) and persists them under a "_targeted" id so they list
 * as a separate analysis. Stages 1–3 mirror {@link JarAnalysisExecutor} (parse → index →
 * stream classes); the difference is {@link CallGraphService#buildTreesForRoots} in place
 * of {@code buildEndpointsFromIndex}.
 */
@Component
public class JarTargetedAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(JarTargetedAnalysisExecutor.class);

    private final JarParserService parserService;
    private final CallGraphService callGraphService;
    private final PersistenceService persistenceService;
    private final SourceEnrichmentService sourceEnrichmentService;
    private final RepositoryStereotypeResolver repositoryStereotypeResolver;
    private final ArtifactStore store;

    public JarTargetedAnalysisExecutor(JarParserService parserService,
                                       CallGraphService callGraphService,
                                       PersistenceService persistenceService,
                                       SourceEnrichmentService sourceEnrichmentService,
                                       RepositoryStereotypeResolver repositoryStereotypeResolver,
                                       @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.store = store;
        this.parserService = parserService;
        this.callGraphService = callGraphService;
        this.persistenceService = persistenceService;
        this.sourceEnrichmentService = sourceEnrichmentService;
        this.repositoryStereotypeResolver = repositoryStereotypeResolver;
    }

    @SuppressWarnings("unchecked")
    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        String tempFilePath = (String) job.metadata.get("tempFilePath");
        long fileSize = ((Number) job.metadata.get("fileSize")).longValue();
        List<Map<String, Object>> rawEntries = (List<Map<String, Object>>) job.metadata.get("entries");

        List<MethodRef> roots = new ArrayList<>();
        for (Map<String, Object> e : rawEntries) {
            String className = (String) e.get("className");
            String methodName = (String) e.get("methodName");
            List<String> paramTypes = (List<String>) e.get("paramTypes");
            roots.add(new MethodRef(className, methodName, paramTypes));
        }

        Path tempJar = Path.of(tempFilePath);
        Path classesFile = null;
        try {
            progress(job, broadcast, "[1/4] Parsing JAR bytecode...");
            JarParserService.ParseResult parseResult = parserService.parseJarToFile(tempJar.toFile(), null, jarName);
            classesFile = parseResult.classesFile();
            classesFile = repositoryStereotypeResolver.resolveTransitiveRepositoryStereotypes(classesFile);
            checkCancelled(job);

            progress(job, broadcast, "[2/4] Building call-graph index...");
            callGraphService.buildIndex();
            callGraphService.setJarArtifactMap(parseResult.jarArtifactMap());
            parserService.streamClasses(classesFile, cls -> callGraphService.addToIndex(cls));
            progress(job, broadcast, "Index built from " + parseResult.totalClasses() + " classes");
            checkCancelled(job);

            progress(job, broadcast, "[3/4] Building targeted call trees for " + roots.size() + " root(s)...");
            List<EndpointInfo> endpoints = callGraphService.buildTreesForRoots(roots);
            System.gc();
            checkCancelled(job);

            progress(job, broadcast, "[4/4] Writing analysis to disk...");
            persistenceService.storeJar(tempJar, jarName);
            // The decompiler needs a REAL local file (it opens a JarFile and seeks), so this is
            // ArtifactStore's documented escape hatch rather than a stream. Locally it is a
            // zero-copy handle; remotely it downloads to a temp that close() removes.
            var storedJarKey = persistenceService.getPaths().storedJarKey(jarName);
            if (store.exists(storedJarKey)) {
                try (LocalHandle storedJar = store.materialize(storedJarKey)) {
                    sourceEnrichmentService.enrichCallTreesWithSource(endpoints, storedJar.path());
                } catch (Exception ex) {
                    log.warn("Source enrichment failed (non-fatal): {}", ex.getMessage());
                }
            }
            System.gc();

            String analyzedAt = LocalDateTime.now().toString();
            persistenceService.writeAnalysisStreaming(
                    jarName, null, fileSize, analyzedAt,
                    parseResult.totalClasses(), endpoints.size(),
                    classesFile, endpoints, "TARGETED", null
            );

            job.resultName = jarName;
            progress(job, broadcast, "Targeted analysis complete: " + endpoints.size()
                    + " call trees from " + roots.size() + " requested root(s)");
        } finally {
            // LOCAL scratch, deliberately not store keys: tempJar is the multipart upload temp and
            // classesFile the parser's intermediate JSONL. Neither is an artifact; both are
            // consumed by APIs that take a java.io File and deleted here.
            Files.deleteIfExists(tempJar);
            if (classesFile != null) Files.deleteIfExists(classesFile);
        }
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String message) {
        job.updateProgress(message);
        broadcast.accept(job, "job-progress");
    }

    private void checkCancelled(QueueJob job) {
        if (job.status == QueueJob.Status.CANCELLED) {
            throw new RuntimeException("Job cancelled");
        }
    }
}
