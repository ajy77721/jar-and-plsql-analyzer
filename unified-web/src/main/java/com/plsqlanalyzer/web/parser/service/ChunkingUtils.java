package com.plsqlanalyzer.web.parser.service;

import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.TableOp;

import java.util.*;

/**
 * Shared utility methods for chunking table operations.
 */
final class ChunkingUtils {

    private ChunkingUtils() {}

    /** Group table ops by qualified table name (schema.table or just table). */
    static Map<String, List<TableOp>> groupByTable(List<TableOp> ops) {
        Map<String, List<TableOp>> map = new LinkedHashMap<>();
        for (TableOp op : ops) {
            String key = (op.schema != null ? op.schema + "." : "") + op.tableName;
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(op);
        }
        return map;
    }

    /** Count distinct table names in a list of table ops. */
    static int countDistinctTables(List<TableOp> ops) {
        Set<String> seen = new HashSet<>();
        for (TableOp op : ops) {
            seen.add((op.schema != null ? op.schema + "." : "") + op.tableName);
        }
        return seen.size();
    }
}
