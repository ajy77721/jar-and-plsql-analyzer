package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CorrectionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.*;

@Component
public class CorrectionPersistence {

    private static final Logger log = LoggerFactory.getLogger(CorrectionPersistence.class);
    private static final String SUFFIX = "_correction.json";

    private final JarDataPaths jarDataPaths;
    private final ArtifactStore store;
    private final ObjectMapper objectMapper = new ObjectMapper();

    CorrectionPersistence(JarDataPaths jarDataPaths, @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.jarDataPaths = jarDataPaths;
        this.store = store;
    }

    /**
     * @deprecated local-filesystem view of {@link #getWorkKey}; only a real directory in local
     *             mode. Use the key.
     */
    @Deprecated
    public Path getWorkDir(String jarName) {
        return jarDataPaths.correctionsDir(jarName);
    }

    /** Storage prefix all correction artifacts for {@code jarName} live under. */
    public ArtifactKey getWorkKey(String jarName) {
        return jarDataPaths.correctionsDirKey(jarName);
    }

    public void saveCorrection(String jarName, String endpointKey, CorrectionResult result) {
        try (OutputStream out = store.write(getWorkKey(jarName).resolve(sanitize(endpointKey) + SUFFIX))) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, result);
        } catch (IOException e) {
            log.warn("Failed to save correction for {}: {}", endpointKey, e.getMessage());
        }
    }

    public CorrectionResult loadCorrection(String jarName, String endpointKey) {
        ArtifactKey key = getWorkKey(jarName).resolve(sanitize(endpointKey) + SUFFIX);
        if (!store.exists(key)) return null;
        try (InputStream in = store.read(key)) {
            return objectMapper.readValue(in, CorrectionResult.class);
        } catch (IOException e) {
            log.warn("Failed to load correction for {}: {}", endpointKey, e.getMessage());
            return null;
        }
    }

    public Map<String, CorrectionResult> loadAllCorrections(String jarName) {
        Map<String, CorrectionResult> results = new LinkedHashMap<>();
        try (var entries = store.list(getWorkKey(jarName), false)) {
            for (var e : entries.filter(en -> !en.directory())
                                .filter(en -> en.key().leaf().endsWith(SUFFIX)).toList()) {
                try (InputStream in = store.read(e.key())) {
                    CorrectionResult cr = objectMapper.readValue(in, CorrectionResult.class);
                    if (cr.getEndpointName() != null) results.put(cr.getEndpointName(), cr);
                } catch (IOException ex) {
                    log.debug("Skipping malformed correction artifact: {}", e.key());
                }
            }
        } catch (IOException e) {
            log.warn("Failed to list corrections for {}: {}", jarName, e.getMessage());
        }
        return results;
    }

    void saveSummary(String jarName, Map<String, Object> summary) {
        try (OutputStream out = store.write(getWorkKey(jarName).resolve("_summary.json"))) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, summary);
        } catch (IOException e) {
            log.warn("Failed to save correction summary: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> loadSummary(String jarName) {
        ArtifactKey key = getWorkKey(jarName).resolve("_summary.json");
        if (!store.exists(key)) return null;
        try (InputStream in = store.read(key)) {
            return objectMapper.readValue(in, Map.class);
        } catch (IOException e) {
            log.debug("No correction summary for {}", jarName);
            return null;
        }
    }

    boolean hasData(String jarName) {
        try (var entries = store.list(getWorkKey(jarName), false)) {
            return entries.anyMatch(e -> !e.directory() && e.key().leaf().endsWith(SUFFIX));
        } catch (IOException e) {
            return false;
        }
    }

    public void deleteAll(String jarName) {
        try {
            if (store.deletePrefix(getWorkKey(jarName)) > 0) {
                log.info("Deleted correction data for {}", jarName);
            }
        } catch (IOException e) {
            log.warn("Failed to delete correction data for {}: {}", jarName, e.getMessage());
        }
    }

    private String sanitize(String name) {
        return JarNameUtil.sanitize(name);
    }
}
