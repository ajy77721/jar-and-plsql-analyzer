package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.OperationVerification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal (package-private) data classes shared by ClaudeVerificationService and its
 * collaborator helpers for chunking and per-chunk parsing.
 */
final class ClaudeVerificationModels {
    private ClaudeVerificationModels() {}
}

class VerificationChunk {
    String id;
    String name;
    List<String> procedureIds = new ArrayList<>();
    Map<String, TableOperationSummary> tables = new LinkedHashMap<>();
    /** For large-proc source window chunks: which line range of the proc body this chunk covers */
    int sourceWindowStart = 0; // 0 means "include full proc"
    int sourceWindowEnd = 0;
}

class ChunkResult {
    String chunkId;
    List<String> procedureIds;
    List<TableVerification> tableVerifications;
    String summary;
    String error;

    static ChunkResult error(String chunkId, String error) {
        ChunkResult cr = new ChunkResult();
        cr.chunkId = chunkId;
        cr.error = error;
        cr.tableVerifications = Collections.emptyList();
        return cr;
    }
}

class TableVerification {
    String tableName;
    List<OperationVerification> operations = new ArrayList<>();
}
