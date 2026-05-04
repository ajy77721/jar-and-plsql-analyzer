package com.jaranalyzer.util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts table names from SQL strings found in bytecode (JPQL, HQL, native SQL).
 * Handles SELECT FROM, INSERT INTO, UPDATE, DELETE FROM, JOIN, and subquery patterns.
 */
public final class SqlStatementParser {

    private SqlStatementParser() {}

    private static final Pattern FROM_PATTERN =
            Pattern.compile("\\bFROM\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern JOIN_PATTERN =
            Pattern.compile("\\bJOIN\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_PATTERN =
            Pattern.compile("\\bINSERT\\s+INTO\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_PATTERN =
            Pattern.compile("\\bUPDATE\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_PATTERN =
            Pattern.compile("\\bDELETE\\s+FROM\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern MERGE_PATTERN =
            Pattern.compile("\\bMERGE\\s+INTO\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRUNCATE_PATTERN =
            Pattern.compile("\\bTRUNCATE\\s+(?:TABLE\\s+)?([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);

    // Callable statement: {call PROC(...)}, {? = call FUNC(...)}
    private static final Pattern JDBC_CALL_PATTERN =
            Pattern.compile("\\{\\s*(?:\\?\\s*=\\s*)?call\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    // Bare CALL statement: CALL PROC_NAME(...)
    private static final Pattern CALL_PATTERN =
            Pattern.compile("\\bCALL\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    // EXEC/EXECUTE statement: EXEC PROC_NAME
    private static final Pattern EXEC_PATTERN =
            Pattern.compile("\\b(?:EXEC|EXECUTE)\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);
    // PL/SQL anonymous block: BEGIN PROC_NAME(...); END;
    private static final Pattern PLSQL_BEGIN_PATTERN =
            Pattern.compile("\\bBEGIN\\s+([A-Za-z_][A-Za-z0-9_.]+)", Pattern.CASE_INSENSITIVE);

    private static final Set<String> SQL_KEYWORDS = Set.of(
            "select", "from", "where", "and", "or", "not", "in", "on", "as",
            "set", "values", "into", "join", "inner", "outer", "left", "right",
            "cross", "full", "group", "order", "having", "limit", "offset",
            "union", "all", "distinct", "exists", "between", "like", "is",
            "null", "true", "false", "case", "when", "then", "else", "end",
            "begin", "call", "exec", "execute",
            "create", "alter", "drop", "table", "index", "view", "schema",
            "insert", "update", "delete", "merge", "truncate", "with",
            "fetch", "next", "rows", "only", "dual", "count", "sum", "avg",
            "min", "max", "asc", "desc", "cascade", "restrict", "constraint",
            "primary", "key", "foreign", "references", "unique", "check",
            "default", "auto_increment", "serial", "sequence", "returning"
    );

    /** Extract all table names from a SQL/JPQL/HQL string. */
    public static Set<String> extractTableNames(String sql) {
        if (sql == null || sql.isBlank()) return Set.of();
        Set<String> tables = new LinkedHashSet<>();

        extractAll(tables, sql, FROM_PATTERN);
        extractAll(tables, sql, JOIN_PATTERN);
        extractAll(tables, sql, INSERT_PATTERN);
        extractAll(tables, sql, UPDATE_PATTERN);
        extractAll(tables, sql, DELETE_PATTERN);
        extractAll(tables, sql, MERGE_PATTERN);
        extractAll(tables, sql, TRUNCATE_PATTERN);

        return tables;
    }

    /** Infer the primary operation type from a SQL string. */
    public static String inferOperationType(String sql) {
        if (sql == null) return null;
        String trimmed = sql.trim().toUpperCase();
        if (trimmed.startsWith("SELECT") || trimmed.startsWith("WITH")) return "READ";
        if (trimmed.startsWith("INSERT")) return "WRITE";
        if (trimmed.startsWith("UPDATE")) return "UPDATE";
        if (trimmed.startsWith("DELETE")) return "DELETE";
        if (trimmed.startsWith("MERGE")) return "WRITE";
        if (trimmed.startsWith("TRUNCATE")) return "DELETE";
        if (trimmed.startsWith("CALL") || trimmed.startsWith("EXEC")) return "CALL";
        if (trimmed.startsWith("{CALL") || trimmed.startsWith("{?") || trimmed.startsWith("{? =")) return "CALL";
        if (trimmed.startsWith("BEGIN")) return "CALL";
        return null;
    }

    /**
     * Extract stored procedure/function names from callable SQL strings.
     * Handles {call PROC}, {? = call FUNC}, CALL PROC, EXEC PROC, BEGIN PROC patterns.
     */
    public static Set<String> extractProcedureNames(String sql) {
        if (sql == null || sql.isBlank()) return Set.of();
        Set<String> procs = new LinkedHashSet<>();
        extractAllProcs(procs, sql, JDBC_CALL_PATTERN);
        extractAllProcs(procs, sql, CALL_PATTERN);
        extractAllProcs(procs, sql, EXEC_PATTERN);
        extractAllProcs(procs, sql, PLSQL_BEGIN_PATTERN);
        return procs;
    }

    private static void extractAllProcs(Set<String> procs, String sql, Pattern pattern) {
        Matcher m = pattern.matcher(sql);
        while (m.find()) {
            String name = m.group(1);
            String simpleName = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
            if (!SQL_KEYWORDS.contains(simpleName.toLowerCase()) && simpleName.length() >= 2) {
                procs.add(name);
            }
        }
    }

    private static void extractAll(Set<String> tables, String sql, Pattern pattern) {
        Matcher m = pattern.matcher(sql);
        while (m.find()) {
            String name = m.group(1);
            // Strip schema prefix (schema.table → table) for matching but keep full for display
            String simpleName = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
            if (!SQL_KEYWORDS.contains(simpleName.toLowerCase()) && simpleName.length() >= 2) {
                tables.add(name);
            }
        }
    }
}
