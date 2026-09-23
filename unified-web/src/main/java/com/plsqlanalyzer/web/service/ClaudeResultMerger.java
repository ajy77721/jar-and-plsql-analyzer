package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.OperationVerification;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.TableVerificationResult;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.VerificationResult;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Merges per-chunk verification results into a single VerificationResult and resolves
 * procedure IDs. Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeResultMerger {

    VerificationResult mergeResults(AnalysisResult staticResult, List<ChunkResult> chunkResults) {
        VerificationResult vr = new VerificationResult();
        vr.analysisName = staticResult.getName();
        vr.timestamp = LocalDateTime.now();
        vr.tables = new ArrayList<>();

        // Build a lookup of all static tables
        Map<String, TableOperationSummary> staticTables = staticResult.getTableOperations();

        // Build procedure ID lookup: bare name → list of fully qualified IDs (handles duplicates + overloads)
        Map<String, List<String>> bareToIds = new HashMap<>();
        CallGraph graph = staticResult.getCallGraph();
        if (graph != null) {
            for (var node : graph.getAllNodes()) {
                String nodeId = node.getId();
                // Use baseId for bare-name extraction (strips overload suffix like /3IN_2OUT)
                String baseId = node.getBaseId() != null ? node.getBaseId() : nodeId;
                String[] p = baseId.split("\\.");
                String bare = p[p.length - 1].toUpperCase();
                bareToIds.computeIfAbsent(bare, k -> new ArrayList<>()).add(nodeId);
            }
        }

        String entrySchema = staticResult.getEntrySchema();

        // Merge chunk results by table name
        Map<String, List<OperationVerification>> mergedOps = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.tableVerifications == null) continue;

            // Build the set of qualified IDs from this chunk's procedure list for disambiguation
            Set<String> chunkProcUpper = new HashSet<>();
            if (cr.procedureIds != null) {
                for (String pid : cr.procedureIds) chunkProcUpper.add(pid.toUpperCase());
            }

            for (TableVerification tv : cr.tableVerifications) {
                if (tv.tableName == null) continue;
                String key = tv.tableName.toUpperCase();
                for (OperationVerification ov : tv.operations) {
                    resolveOperationIds(ov, bareToIds, chunkProcUpper, entrySchema);
                }
                mergedOps.computeIfAbsent(key, k -> new ArrayList<>()).addAll(tv.operations);
            }
        }

        // Build final table verification list
        Set<String> allTableNames = new LinkedHashSet<>();
        allTableNames.addAll(staticTables.keySet());
        allTableNames.addAll(mergedOps.keySet());

        int confirmed = 0, removed = 0, newOps = 0;

        for (String tableName : allTableNames) {
            TableVerificationResult tvr = new TableVerificationResult();
            tvr.tableName = tableName;
            tvr.staticOperations = new ArrayList<>();
            tvr.claudeVerifications = new ArrayList<>();

            // Static operations
            TableOperationSummary staticSummary = staticTables.get(tableName);
            if (staticSummary != null) {
                tvr.schemaName = staticSummary.getSchemaName();
                for (Object op : staticSummary.getOperations()) {
                    tvr.staticOperations.add(op.toString());
                }
                tvr.staticAccessCount = staticSummary.getAccessCount();
                tvr.isExternal = staticSummary.isExternal();
            }

            // Claude verifications
            List<OperationVerification> claudeOps = mergedOps.get(tableName);
            if (claudeOps != null) {
                // Deduplicate by operation + procedureName
                Map<String, OperationVerification> deduped = new LinkedHashMap<>();
                for (OperationVerification ov : claudeOps) {
                    String dedupKey = (ov.operation != null ? ov.operation : "?") + "|"
                            + (ov.procedureName != null ? ov.procedureName : "?");
                    deduped.putIfAbsent(dedupKey, ov);
                }
                tvr.claudeVerifications.addAll(deduped.values());

                for (OperationVerification ov : deduped.values()) {
                    String status = ov.status != null ? ov.status.toUpperCase() : "CONFIRMED";
                    switch (status) {
                        case "CONFIRMED" -> confirmed++;
                        case "REMOVED" -> removed++;
                        case "NEW" -> newOps++;
                    }
                }
            }

            // Determine overall status for this table
            if (claudeOps == null || claudeOps.isEmpty()) {
                tvr.overallStatus = "UNVERIFIED";
            } else {
                boolean hasRemoved = claudeOps.stream().anyMatch(o -> "REMOVED".equalsIgnoreCase(o.status));
                boolean hasNew = claudeOps.stream().anyMatch(o -> "NEW".equalsIgnoreCase(o.status));
                if (hasRemoved && hasNew) tvr.overallStatus = "MODIFIED";
                else if (hasRemoved) tvr.overallStatus = "PARTIAL";
                else if (hasNew) tvr.overallStatus = "EXPANDED";
                else tvr.overallStatus = "CONFIRMED";
            }

            vr.tables.add(tvr);
        }

        vr.confirmedCount = confirmed;
        vr.removedCount = removed;
        vr.newCount = newOps;
        vr.totalChunks = chunkResults.size();
        vr.errorChunks = (int) chunkResults.stream().filter(c -> c.error != null).count();

        return vr;
    }

    private void resolveOperationIds(OperationVerification ov,
                                     Map<String, List<String>> bareToIds,
                                     Set<String> chunkProcIds,
                                     String entrySchema) {
        if (ov.procedureName == null || ov.procedureId != null) return;

        String bare = ov.procedureName.toUpperCase();

        // Already qualified — extract sourceFile from it
        if (bare.contains(".")) {
            ov.procedureId = ov.procedureName;
            String[] sp = bare.split("\\.");
            if (sp.length >= 2) ov.sourceFile = sp[0] + "." + sp[1];
            return;
        }

        List<String> candidates = bareToIds.get(bare);
        if (candidates == null || candidates.isEmpty()) {
            // Not in call graph — construct best-effort ID from entry schema
            if (entrySchema != null && !entrySchema.isEmpty()) {
                ov.procedureId = entrySchema + "." + ov.procedureName;
                ov.sourceFile = entrySchema + "." + ov.procedureName;
            }
            return;
        }

        if (candidates.size() == 1) {
            // Unique match — use it directly
            String nodeId = candidates.get(0);
            ov.procedureId = nodeId;
            ov.sourceFile = buildSourceFile(nodeId, entrySchema);
            return;
        }

        // Multiple candidates — disambiguate using chunk's procedure context
        String resolved = null;
        if (!chunkProcIds.isEmpty()) {
            for (String candidate : candidates) {
                // Check if this candidate (or its parent package) is in the chunk's procedure list
                String candidateUpper = candidate.toUpperCase();
                if (chunkProcIds.contains(candidateUpper)) {
                    resolved = candidate;
                    break;
                }
                // Also check if any chunk proc shares the same package as this candidate
                String[] cp = candidateUpper.split("\\.");
                if (cp.length >= 2) {
                    String candidatePkg = cp.length >= 3 ? cp[0] + "." + cp[1] : cp[0];
                    for (String chunkProc : chunkProcIds) {
                        if (chunkProc.startsWith(candidatePkg + ".")) {
                            resolved = candidate;
                            break;
                        }
                    }
                    if (resolved != null) break;
                }
            }
        }

        // Fallback: prefer 3-part IDs (SCHEMA.PACKAGE.PROC) over shorter ones
        if (resolved == null) {
            for (String candidate : candidates) {
                if (candidate.split("\\.").length >= 3) {
                    resolved = candidate;
                    break;
                }
            }
        }

        // Last resort: take first candidate
        if (resolved == null) {
            resolved = candidates.get(0);
        }

        ov.procedureId = resolved;
        ov.sourceFile = buildSourceFile(resolved, entrySchema);
    }

    private String buildSourceFile(String nodeId, String entrySchema) {
        // Strip overload suffix (e.g., "CUSTOMER.PKG.PROC/3IN_2OUT" → "CUSTOMER.PKG.PROC")
        String base = nodeId.contains("/") ? nodeId.substring(0, nodeId.indexOf('/')) : nodeId;
        String[] parts = base.split("\\.");
        if (parts.length >= 2) {
            return parts[0] + "." + parts[1];
        }
        // Standalone proc (bare ID) — use SCHEMA.PROC as sourceFile
        if (entrySchema != null && !entrySchema.isEmpty()) {
            return entrySchema + "." + parts[0];
        }
        return null;
    }
}
