package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
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

    @Value("${plsql.data-dir:data/plsql}")
    private String dataBaseDir;

    private final String flowOutputDir;

    public ClaudePersistenceService(@Value("${app.data-dir.base:data}") String dataDir) {
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.flowOutputDir = Path.of(dataDir, "plsql-parse").toString();
    }

    /**
     * Save a Claude verification result with version rotation.
     * Current analysis_claude.json is moved to analysis_claude_prev.json before writing new.
     */
    public void saveClaudeResult(String analysisName,
                                 VerificationResult result) {
        Path folder = Paths.get(dataBaseDir).resolve(analysisName);
        try {
            Files.createDirectories(folder);

            // Rotate: current -> prev
            Path claudeFile = folder.resolve("analysis_claude.json");
            Path claudePrev = folder.resolve("analysis_claude_prev.json");
            if (Files.exists(claudeFile)) {
                Files.move(claudeFile, claudePrev, StandardCopyOption.REPLACE_EXISTING);
                log.info("Rotated claude -> prev for '{}'", analysisName);
            }

            // Write new Claude result
            mapper.writeValue(claudeFile.toFile(), result);

            // Update metadata
            updateMetadata(folder, analysisName, result);

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
        Path folder = Paths.get(dataBaseDir).resolve(analysisName);
        try {
            Files.createDirectories(folder);
            Path claudeFile = folder.resolve("analysis_claude.json");
            mapper.writeValue(claudeFile.toFile(), result);
            log.info("Updated Claude result in-place for '{}'", analysisName);
        } catch (IOException e) {
            log.error("Failed to update Claude result for '{}': {}", analysisName, e.getMessage());
        }
    }

    /**
     * Load the latest Claude verification result for an analysis.
     */
    public VerificationResult loadClaudeResult(String analysisName) {
        Path file = Paths.get(dataBaseDir).resolve(analysisName).resolve("analysis_claude.json");
        if (!Files.exists(file)) return null;
        try {
            return mapper.readValue(file.toFile(), VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load Claude result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Load the previous Claude verification result (for revert).
     */
    public VerificationResult loadClaudePrev(String analysisName) {
        Path file = Paths.get(dataBaseDir).resolve(analysisName).resolve("analysis_claude_prev.json");
        if (!Files.exists(file)) return null;
        try {
            return mapper.readValue(file.toFile(), VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load prev Claude result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Revert: swap analysis_claude_prev.json -> analysis_claude.json.
     */
    public boolean revertClaude(String analysisName) {
        Path folder = Paths.get(dataBaseDir).resolve(analysisName);
        Path claudeFile = folder.resolve("analysis_claude.json");
        Path claudePrev = folder.resolve("analysis_claude_prev.json");
        if (!Files.exists(claudePrev)) return false;
        try {
            Files.move(claudePrev, claudeFile, StandardCopyOption.REPLACE_EXISTING);
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
        Path claudeFolder = Paths.get(dataBaseDir).resolve(analysisName);
        Path staticFolder = Paths.get(flowOutputDir).resolve(analysisName);

        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", analysisName);
        info.put("hasStatic", Files.isDirectory(staticFolder));
        info.put("hasClaude", Files.exists(claudeFolder.resolve("analysis_claude.json")));
        info.put("hasClaudePrev", Files.exists(claudeFolder.resolve("analysis_claude_prev.json")));
        info.put("hasVerificationData", Files.isDirectory(claudeFolder.resolve("claude")));

        // Load metadata if present
        Path metaFile = claudeFolder.resolve("metadata.json");
        if (Files.exists(metaFile)) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> meta = mapper.readValue(metaFile.toFile(), LinkedHashMap.class);
                info.put("mode", meta.getOrDefault("mode", "static"));
                info.put("claudeIteration", meta.getOrDefault("claudeIteration", 0));
                info.put("lastUpdated", meta.get("lastUpdated"));
            } catch (IOException e) { /* ignore */ }
        } else {
            info.put("mode", "static");
            info.put("claudeIteration", 0);
        }

        // File sizes
        addFileSize(info, "claudeSizeKb", claudeFolder.resolve("analysis_claude.json"));
        addFileSize(info, "claudePrevSizeKb", claudeFolder.resolve("analysis_claude_prev.json"));

        return info;
    }

    /**
     * List all analyses that have Claude enrichment data.
     */
    public List<Map<String, Object>> listClaudeEnriched() {
        Path root = Paths.get(dataBaseDir);
        if (!Files.isDirectory(root)) return Collections.emptyList();

        List<Map<String, Object>> result = new ArrayList<>();
        try (Stream<Path> dirs = Files.list(root)) {
            dirs.filter(Files::isDirectory).forEach(dir -> {
                String name = dir.getFileName().toString();
                Path metaFile = dir.resolve("metadata.json");
                if (Files.exists(metaFile) || Files.exists(dir.resolve("analysis_claude.json"))) {
                    result.add(getVersionInfo(name));
                }
            });
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
     * Delete all Claude data for an analysis.
     */
    public boolean deleteClaudeData(String analysisName) {
        Path folder = Paths.get(dataBaseDir).resolve(analysisName);
        if (!Files.exists(folder)) return false;
        try {
            try (Stream<Path> walk = Files.walk(folder)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
            }
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
     * Backs up the original tables/index.json to tables/index_pre_claude.json before modifying.
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

        Path tablesFile = Path.of(flowOutputDir, analysisName, "api/tables/index.json");
        if (!Files.exists(tablesFile)) {
            log.warn("No tables index file for '{}': {}", analysisName, tablesFile);
            return null;
        }

        try {
            // Load a COPY of static tables data — never modify the original
            Map<String, Object> tablesData = mapper.readValue(tablesFile.toFile(), LinkedHashMap.class);
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

            // Write to claude/ subfolder — static analysis remains untouched
            Path claudeDir = Path.of(flowOutputDir, analysisName, "claude");
            Files.createDirectories(claudeDir);
            Path mergedFile = claudeDir.resolve("tables_merged.json");
            mapper.writeValue(mergedFile.toFile(), tablesData);
            log.info("Applied Claude changes for '{}' -> {}: {} removed, {} added",
                    analysisName, mergedFile, removedCount, addedCount);

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

    private void updateMetadata(Path folder, String analysisName,
                                VerificationResult result) {
        Path metaFile = folder.resolve("metadata.json");
        Map<String, Object> meta;

        if (Files.exists(metaFile)) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> existing = mapper.readValue(metaFile.toFile(), LinkedHashMap.class);
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

        try {
            mapper.writeValue(metaFile.toFile(), meta);
        } catch (IOException e) {
            log.error("Failed to write metadata for '{}': {}", analysisName, e.getMessage());
        }
    }

    private void addFileSize(Map<String, Object> info, String key, Path file) {
        if (Files.exists(file)) {
            try {
                info.put(key, Files.size(file) / 1024);
            } catch (IOException e) { /* ignore */ }
        }
    }
}
