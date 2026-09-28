package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;

/**
 * Dual-mode persistence for plsql-parser analysis data.
 *
 * Static data stays in flow-output/{name}/ (immutable, written by ChunkedFlowWriter).
 * Claude enrichment data goes to data/plsql/{name}/ (mutable, versioned).
 *
 * Version rotation:
 *   analysis_claude.json      = latest Claude-enriched result
 *   analysis_claude_prev.json = previous version (rotated on save)
 *   metadata.json             = tracks mode, iteration count, timestamp
 */
@Service("parserClaudePersistenceService")
public class ClaudePersistenceService {

    private static final Logger log = LoggerFactory.getLogger(ClaudePersistenceService.class);

    private final ObjectMapper mapper;
    private final ArtifactStore store;

    public ClaudePersistenceService(@Qualifier("parserArtifactStore") ArtifactStore store) {
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.store = store;
    }

    /** Every read in this class goes through the store; the key decides where it comes from. */
    private InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    /** Every write goes through the store. close() is the commit point. */
    private OutputStream openWrite(ArtifactKey key) throws IOException { return store.write(key); }

    /**
     * Length-aware variant. Backends that must declare a size up front (a single-shot S3 PUT
     * rather than a multipart upload) use it; local ignores it.
     */
    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return store.write(key, contentLength);
    }

    /**
     * Drops the analysis's precomputed view summaries.
     *
     * <p>A correction can change what a facet should report without moving {@code api/index.json}'s
     * mtime (a Claude apply writes {@code claude/tables_merged.json}), so {@code AnalysisViewStore}'s
     * mtime freshness check would not catch it. Deleting the {@code views/} folder here is the
     * simplest correct answer: the next read recomputes and rewrites from the corrected data.
     */
    private void invalidateViews(String analysisName) {
        try {
            store.deletePrefix(AnalysisPaths.resolveKey(analysisName, "views"));
        } catch (Exception e) {
            log.debug("view invalidate for '{}' failed: {}", analysisName, e.getMessage());
        }
    }

    /**
     * Save a Claude verification result with version rotation.
     * Current analysis_claude.json is moved to analysis_claude_prev.json before writing new.
     */
    public void saveClaudeResult(String analysisName,
                                 VerificationResult result) {
        try {
            // No createDirectories: the store creates parents on write.
            // Rotate: current -> prev
            ArtifactKey claudeFile = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude.json");
            ArtifactKey claudePrev = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude_prev.json");
            if (store.exists(claudeFile)) {
                store.move(claudeFile, claudePrev);
                log.info("Rotated claude -> prev for '{}'", analysisName);
            }

            // Write new Claude result
            try (OutputStream out = openWrite(claudeFile)) {
                mapper.writeValue(out, result);
            }

            // A prior "apply" merge is now stale — drop it so the Claude view reflects
            // the fresh verification (live overlay) until the user applies again.
            store.delete(AnalysisPaths.resolveKey(analysisName, "claude/tables_merged.json"));
            // The precomputed views are now stale too.
            invalidateViews(analysisName);

            // Update metadata
            updateMetadata(analysisName, result);

            log.info("Saved Claude result for '{}' ({} tables)", analysisName,
                    result.tables != null ? result.tables.size() : 0);
        } catch (IOException e) {
            log.error("Failed to save Claude result for '{}': {}", analysisName, e.getMessage());
        }
    }

    /**
     * Update a Claude result in-place (no version rotation).
     * Used for user review decisions that shouldn't create new versions.
     */
    public void updateClaudeResult(String analysisName, VerificationResult result) {
        try {
            ArtifactKey claudeFile = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude.json");
            try (OutputStream out = openWrite(claudeFile)) {
                mapper.writeValue(out, result);
            }
            invalidateViews(analysisName);
            log.info("Updated Claude result in-place for '{}'", analysisName);
        } catch (IOException e) {
            log.error("Failed to update Claude result for '{}': {}", analysisName, e.getMessage());
        }
    }

    /**
     * Load the applied/merged Claude tables index (written by applyAcceptedChanges),
     * or {@code null} if the user has not applied any changes yet. This is the
     * Claude-enhanced tables view; the static tables index is never modified.
     */
    public JsonNode loadMergedTables(String analysisName) {
        ArtifactKey merged = AnalysisPaths.resolveKey(analysisName, "claude/tables_merged.json");
        if (!store.exists(merged)) return null;
        try (InputStream in = openRead(merged)) {
            return mapper.readTree(in);
        } catch (IOException e) {
            log.error("Failed to load merged Claude tables for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Load the latest Claude verification result for an analysis.
     */
    public VerificationResult loadClaudeResult(String analysisName) {
        ArtifactKey file = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude.json");
        if (!store.exists(file)) return null;
        try (InputStream in = openRead(file)) {
            return mapper.readValue(in, VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load Claude result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Load the previous Claude verification result (for revert).
     */
    public VerificationResult loadClaudePrev(String analysisName) {
        ArtifactKey file = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude_prev.json");
        if (!store.exists(file)) return null;
        try (InputStream in = openRead(file)) {
            return mapper.readValue(in, VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load prev Claude result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Revert: swap analysis_claude_prev.json -> analysis_claude.json.
     */
    public boolean revertClaude(String analysisName) {
        ArtifactKey claudeFile = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude.json");
        ArtifactKey claudePrev = AnalysisPaths.claudeFileKey(analysisName, "analysis_claude_prev.json");
        if (!store.exists(claudePrev)) return false;
        try {
            store.move(claudePrev, claudeFile);
            invalidateViews(analysisName);
            log.info("Reverted Claude enrichment for '{}'", analysisName);
            return true;
        } catch (IOException e) {
            log.error("Failed to revert Claude for '{}': {}", analysisName, e.getMessage());
            return false;
        }
    }

    /**
     * Get version info for an analysis: what files exist, iteration count, mode.
     */
    public Map<String, Object> getVersionInfo(String analysisName) {
        ArtifactKey claudeFolder = AnalysisPaths.claudeDirKey(analysisName);

        // ONE listing covers existence AND size for every file below — the alternative is six
        // separate exists()/size() calls, six S3 HEADs, for a single row of the UI.
        Map<ArtifactKey, ArtifactEntry> stat;
        try {
            stat = store.statPrefix(claudeFolder);
        } catch (IOException e) {
            stat = Collections.emptyMap();
        }

        ArtifactKey claudeFile = claudeFolder.resolve("analysis_claude.json");
        ArtifactKey claudePrev = claudeFolder.resolve("analysis_claude_prev.json");
        ArtifactKey metaFile   = claudeFolder.resolve("metadata.json");

        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", analysisName);
        info.put("hasStatic", hasAnyArtifact(AnalysisPaths.dirKey(analysisName)));
        info.put("hasClaude", stat.containsKey(claudeFile));
        info.put("hasClaudePrev", stat.containsKey(claudePrev));
        // Object stores have no directories, so "is there a claude/ folder" becomes
        // "is there any artifact under claude/".
        ArtifactKey verificationDir = claudeFolder.resolve("claude");
        info.put("hasVerificationData",
                stat.keySet().stream().anyMatch(k -> startsWith(k, verificationDir)));

        // Load metadata if present
        if (stat.containsKey(metaFile)) {
            try (InputStream in = openRead(metaFile)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> meta = mapper.readValue(in, LinkedHashMap.class);
                info.put("mode", meta.getOrDefault("mode", "static"));
                info.put("claudeIteration", meta.getOrDefault("claudeIteration", 0));
                info.put("lastUpdated", meta.get("lastUpdated"));
            } catch (IOException e) { /* ignore */ }
        } else {
            info.put("mode", "static");
            info.put("claudeIteration", 0);
        }

        // File sizes — already in hand from the listing above.
        addFileSize(info, "claudeSizeKb", stat.get(claudeFile));
        addFileSize(info, "claudePrevSizeKb", stat.get(claudePrev));

        return info;
    }

    /** True when {@code prefix} contains at least one artifact (the store-native "is a folder"). */
    private boolean hasAnyArtifact(ArtifactKey prefix) {
        try (Stream<ArtifactEntry> s = store.list(prefix, true)) {
            return s.anyMatch(e -> !e.directory());
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean startsWith(ArtifactKey key, ArtifactKey prefix) {
        List<String> k = key.segments(), p = prefix.segments();
        if (k.size() <= p.size()) return false;
        return k.subList(0, p.size()).equals(p);
    }

    /**
     * List all analyses that have Claude enrichment data.
     */
    public List<Map<String, Object>> listClaudeEnriched() {
        ArtifactKey root = ArtifactKey.of(AnalysisPaths.CLAUDE_ROOT);
        List<Map<String, Object>> result = new ArrayList<>();
        // Claude sidecar data is stored flat, keyed by the (context-rich) identity name.
        // ONE recursive listing instead of a directory scan plus two exists() per candidate.
        try {
            Set<String> names = new LinkedHashSet<>();
            for (ArtifactKey k : store.statPrefix(root).keySet()) {
                List<String> segs = k.segments();
                if (segs.size() != 3) continue;                 // plsql/<name>/<file>
                String leaf = segs.get(2);
                if (leaf.equals("metadata.json") || leaf.equals("analysis_claude.json")) {
                    names.add(segs.get(1));
                }
            }
            for (String name : names) result.add(getVersionInfo(name));
        } catch (IOException e) {
            log.error("Error listing Claude enriched analyses: {}", e.getMessage());
        }

        // Sort by lastUpdated descending
        result.sort((a, b) -> {
            String ta = (String) a.getOrDefault("lastUpdated", "");
            String tb = (String) b.getOrDefault("lastUpdated", "");
            return tb.compareTo(ta);
        });
        return result;
    }

    /**
     * Copy Claude data from one analysis name to another (used when archiving a run).
     * Source data/plsql/{fromName}/ is copied to data/plsql/{toName}/.
     */
    public boolean archiveClaudeData(String fromName, String toName) {
        ArtifactKey src = AnalysisPaths.claudeDirKey(fromName);
        ArtifactKey dst = AnalysisPaths.claudeDirKey(toName);
        try {
            List<ArtifactKey> files;
            try (Stream<ArtifactEntry> walk = store.list(src, true)) {
                files = walk.filter(e -> !e.directory()).map(ArtifactEntry::key).toList();
            }
            if (files.isEmpty()) return false;   // nothing to archive (was: source folder missing)
            int srcDepth = src.segments().size();
            for (ArtifactKey f : files) {
                ArtifactKey target = dst;
                for (String seg : f.segments().subList(srcDepth, f.segments().size())) {
                    target = target.resolve(seg);
                }
                // copy(), not read+write: the backend can do this server-side where supported.
                store.copy(f, target);
            }
            log.info("Archived Claude data '{}' -> '{}'", fromName, toName);
            return true;
        } catch (IOException e) {
            log.error("Failed to archive Claude data '{}' -> '{}': {}", fromName, toName, e.getMessage());
            return false;
        }
    }

    /**
     * Delete all Claude data for an analysis.
     */
    public boolean deleteClaudeData(String analysisName) {
        try {
            if (store.deletePrefix(AnalysisPaths.claudeDirKey(analysisName)) == 0) return false;
            invalidateViews(analysisName);
            log.info("Deleted Claude data for '{}'", analysisName);
            return true;
        } catch (IOException e) {
            log.error("Failed to delete Claude data for '{}': {}", analysisName, e.getMessage());
            return false;
        }
    }

    /**
     * Apply accepted Claude verification changes to the static analysis tables.
     *
     * For REMOVED + userDecision=ACCEPTED: remove that operation/line from the static data.
     * For NEW + userDecision=ACCEPTED: add that operation/line to the static data.
     *
     * The static tables/index.json is never modified; the merged result is written
     * to claude/tables_merged.json and served only in the Claude-enhanced view.
     *
     * @return map with "removed" and "added" counts, or null on error
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> applyAcceptedChanges(String analysisName) {
        VerificationResult claudeResult = loadClaudeResult(analysisName);
        if (claudeResult == null || claudeResult.tables == null) {
            log.warn("No Claude result for '{}' to apply", analysisName);
            return null;
        }

        ArtifactKey tablesFile = AnalysisPaths.resolveKey(analysisName, "api/tables/index.json");
        if (!store.exists(tablesFile)) {
            log.warn("No tables index file for '{}': {}", analysisName, tablesFile);
            return null;
        }

        try {
            // Load a COPY of static tables data — never modify the original
            Map<String, Object> tablesData;
            try (InputStream in = openRead(tablesFile)) {
                tablesData = mapper.readValue(in, LinkedHashMap.class);
            }
            List<Map<String, Object>> tables = (List<Map<String, Object>>) tablesData.get("tables");
            if (tables == null) {
                log.warn("No 'tables' array in tables index for '{}'", analysisName);
                return null;
            }

            int removedCount = 0;
            int addedCount = 0;

            for (var claudeTable : claudeResult.tables) {
                if (claudeTable.claudeVerifications == null) continue;

                String claudeTableFull = claudeTable.tableName != null ? claudeTable.tableName : "";
                String claudeSchema = "";
                String claudeBareName = claudeTableFull;
                int dotIdx = claudeTableFull.indexOf('.');
                if (dotIdx > 0) {
                    claudeSchema = claudeTableFull.substring(0, dotIdx);
                    claudeBareName = claudeTableFull.substring(dotIdx + 1);
                }

                Map<String, Object> staticTable = findStaticTable(tables, claudeSchema, claudeBareName);

                for (var v : claudeTable.claudeVerifications) {
                    if (!"ACCEPTED".equalsIgnoreCase(v.userDecision)) continue;

                    String status = v.status != null ? v.status.toUpperCase() : "";

                    if ("REMOVED".equals(status)) {
                        if (staticTable != null) {
                            boolean didRemove = removeOperationFromTable(
                                    staticTable, v.operation, v.procedureName, v.lineNumber);
                            if (didRemove) removedCount++;
                        }
                    } else if ("NEW".equals(status)) {
                        if (staticTable == null) {
                            staticTable = createStaticTableEntry(claudeSchema, claudeBareName);
                            tables.add(staticTable);
                        }
                        boolean didAdd = addOperationToTable(
                                staticTable, v.operation, v.procedureName, v.lineNumber);
                        if (didAdd) addedCount++;
                    }
                }

                if (staticTable != null) {
                    cleanupTable(staticTable);
                }
            }

            tables.removeIf(t -> {
                List<?> usedBy = (List<?>) t.get("usedBy");
                return usedBy == null || usedBy.isEmpty();
            });

            recalculateSummary(tablesData, tables);

            // Write to claude/ subfolder — static analysis remains untouched.
            // No createDirectories: the store creates parents on write.
            ArtifactKey mergedFile =
                    AnalysisPaths.resolveKey(analysisName, "claude/tables_merged.json");
            try (OutputStream out = openWrite(mergedFile)) {
                mapper.writeValue(out, tablesData);
            }
            log.info("Applied Claude changes for '{}' -> {}: {} removed, {} added",
                    analysisName, mergedFile, removedCount, addedCount);
            // The corrected data must not be shadowed by a pre-apply view summary.
            invalidateViews(analysisName);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("removed", removedCount);
            result.put("added", addedCount);
            result.put("analysisName", analysisName);
            return result;

        } catch (IOException e) {
            log.error("Failed to apply Claude changes for '{}': {}", analysisName, e.getMessage(), e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> findStaticTable(List<Map<String, Object>> tables,
                                                 String schema, String bareName) {
        String upperSchema = schema.toUpperCase();
        String upperName = bareName.toUpperCase();
        for (Map<String, Object> t : tables) {
            String tSchema = ((String) t.getOrDefault("schema", "")).toUpperCase();
            String tName = ((String) t.getOrDefault("name", "")).toUpperCase();
            if (tName.equals(upperName) && (tSchema.equals(upperSchema)
                    || upperSchema.isEmpty() || tSchema.isEmpty())) {
                return t;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private boolean removeOperationFromTable(Map<String, Object> staticTable,
                                              String operation, String procedureName, int lineNumber) {
        List<Map<String, Object>> usedBy = (List<Map<String, Object>>) staticTable.get("usedBy");
        if (usedBy == null) return false;

        String opUpper = operation != null ? operation.toUpperCase() : "";
        boolean removed = false;

        for (Map<String, Object> usage : usedBy) {
            // Match by procedure name if available
            String nodeName = (String) usage.getOrDefault("nodeName", "");
            if (procedureName != null && !procedureName.isEmpty()) {
                if (!nameMatches(nodeName, procedureName)) continue;
            }

            Map<String, Object> operations = (Map<String, Object>) usage.get("operations");
            if (operations == null) continue;

            List<Number> lines = (List<Number>) operations.get(opUpper);
            if (lines == null) continue;

            if (lineNumber > 0) {
                boolean lineRemoved = lines.removeIf(l -> l.intValue() == lineNumber);
                if (lineRemoved) removed = true;
                if (lines.isEmpty()) {
                    operations.remove(opUpper);
                }
            } else {
                // No specific line - remove the entire operation for this procedure
                operations.remove(opUpper);
                removed = true;
            }
        }
        return removed;
    }

    @SuppressWarnings("unchecked")
    private boolean addOperationToTable(Map<String, Object> staticTable,
                                         String operation, String procedureName, int lineNumber) {
        List<Map<String, Object>> usedBy = (List<Map<String, Object>>) staticTable.get("usedBy");
        if (usedBy == null) {
            usedBy = new ArrayList<>();
            staticTable.put("usedBy", usedBy);
        }

        String opUpper = operation != null ? operation.toUpperCase() : "SELECT";

        // Find existing usedBy entry for this procedure
        Map<String, Object> targetUsage = null;
        if (procedureName != null && !procedureName.isEmpty()) {
            for (Map<String, Object> usage : usedBy) {
                String nodeName = (String) usage.getOrDefault("nodeName", "");
                if (nameMatches(nodeName, procedureName)) {
                    targetUsage = usage;
                    break;
                }
            }
        }

        if (targetUsage == null) {
            // Create a new usedBy entry
            targetUsage = new LinkedHashMap<>();
            String nodeId = procedureName != null ? procedureName.replace('.', '$') : "";
            targetUsage.put("nodeId", nodeId);
            targetUsage.put("nodeName", procedureName != null ? procedureName : "");
            targetUsage.put("nodeDepth", 0);
            targetUsage.put("operations", new LinkedHashMap<>());
            usedBy.add(targetUsage);
        }

        Map<String, Object> operations = (Map<String, Object>) targetUsage.get("operations");
        if (operations == null) {
            operations = new LinkedHashMap<>();
            targetUsage.put("operations", operations);
        }

        List<Number> lines = (List<Number>) operations.get(opUpper);
        if (lines == null) {
            lines = new ArrayList<>();
            operations.put(opUpper, lines);
        }

        // Add line if not already present
        int lineFinal = lineNumber > 0 ? lineNumber : 0;
        if (lineFinal > 0) {
            boolean exists = lines.stream().anyMatch(l -> l.intValue() == lineFinal);
            if (!exists) {
                lines.add(lineFinal);
                Collections.sort(lines, Comparator.comparingInt(Number::intValue));
            }
        }

        return true;
    }

    private Map<String, Object> createStaticTableEntry(String schema, String bareName) {
        Map<String, Object> entry = new LinkedHashMap<>();
        if (!schema.isEmpty()) {
            entry.put("schema", schema);
        }
        entry.put("name", bareName);
        entry.put("objectType", "TABLE");
        entry.put("allOperations", new ArrayList<String>());
        entry.put("usedBy", new ArrayList<Map<String, Object>>());
        return entry;
    }

    @SuppressWarnings("unchecked")
    private void cleanupTable(Map<String, Object> staticTable) {
        List<Map<String, Object>> usedBy = (List<Map<String, Object>>) staticTable.get("usedBy");
        if (usedBy != null) {
            usedBy.removeIf(usage -> {
                Map<String, Object> ops = (Map<String, Object>) usage.get("operations");
                return ops == null || ops.isEmpty();
            });
        }

        // Recalculate allOperations from remaining usedBy entries
        Set<String> allOps = new LinkedHashSet<>();
        if (usedBy != null) {
            for (Map<String, Object> usage : usedBy) {
                Map<String, Object> ops = (Map<String, Object>) usage.get("operations");
                if (ops != null) {
                    allOps.addAll(ops.keySet());
                }
            }
        }
        staticTable.put("allOperations", new ArrayList<>(allOps));
    }

    @SuppressWarnings("unchecked")
    private void recalculateSummary(Map<String, Object> tablesData,
                                     List<Map<String, Object>> tables) {
        tablesData.put("totalTables", tables.size());
        Map<String, Integer> opSummary = new LinkedHashMap<>();
        for (Map<String, Object> t : tables) {
            List<String> allOps = (List<String>) t.get("allOperations");
            if (allOps != null) {
                for (String op : allOps) {
                    opSummary.merge(op, 1, Integer::sum);
                }
            }
        }
        tablesData.put("operationSummary", opSummary);
    }

    private boolean nameMatches(String staticName, String claudeName) {
        if (staticName == null || claudeName == null) return false;
        String a = staticName.toUpperCase();
        String b = claudeName.toUpperCase();
        return a.equals(b) || a.endsWith("." + b) || b.endsWith("." + a);
    }

    // ---- Internal ----

    private void updateMetadata(String analysisName, VerificationResult result) {
        ArtifactKey metaFile = AnalysisPaths.claudeFileKey(analysisName, "metadata.json");
        Map<String, Object> meta;

        if (store.exists(metaFile)) {
            try (InputStream in = openRead(metaFile)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> existing = mapper.readValue(in, LinkedHashMap.class);
                meta = existing;
            } catch (IOException e) {
                meta = new LinkedHashMap<>();
            }
        } else {
            meta = new LinkedHashMap<>();
        }

        meta.put("name", analysisName);
        meta.put("mode", "claude");
        int iteration = meta.containsKey("claudeIteration")
                ? ((Number) meta.get("claudeIteration")).intValue() + 1 : 1;
        meta.put("claudeIteration", iteration);
        meta.put("lastUpdated", LocalDateTime.now().toString());
        meta.put("confirmedCount", result.confirmedCount);
        meta.put("removedCount", result.removedCount);
        meta.put("newCount", result.newCount);
        meta.put("totalTables", result.tables != null ? result.tables.size() : 0);
        meta.put("totalChunks", result.totalChunks);
        meta.put("errorChunks", result.errorChunks);

        try (OutputStream out = openWrite(metaFile)) {
            mapper.writeValue(out, meta);
        } catch (IOException e) {
            log.error("Failed to write metadata for '{}': {}", analysisName, e.getMessage());
        }
    }

    /** @param entry may be null, meaning the artifact does not exist */
    private void addFileSize(Map<String, Object> info, String key, ArtifactEntry entry) {
        if (entry != null) info.put(key, entry.size() / 1024);
    }
}
