package com.analyzer.storage.entity;

import java.util.List;

/**
 * One (node, table, operation) usage — PLAN-04's {@code table_usage}, and <b>the row that justifies
 * the database</b>.
 *
 * <p>"Which nodes touch table X" is the question the opaque-payload repository cannot answer at all:
 * it can return {@code document(key)} and nothing else, so the only way to answer is to read every
 * {@code api/nodes/*.json} of the analysis and look. This row, with an index on
 * {@code (analysis, tableKey)}, turns that scan into a seek.
 *
 * <h2>Why {@code tableKey} exists as a separate field</h2>
 *
 * The corpus disagrees with itself about case: {@code tables.json} keys tables as
 * {@code ANALYZERDB.ERP_AUDIT_LOG} while the same table inside a node document is
 * {@code schema=ANALYZERDB, name=erp_audit_log}. An index on {@code name} would therefore answer
 * a query typed one way and miss the same table typed the other. {@code tableKey} is the upper-cased
 * {@code schema.name} (or the bare upper-cased name when the schema is absent) and is the only field
 * the hot query filters on; {@code schema} and {@code name} are kept verbatim beside it so a
 * response can still render what the analyser actually wrote.
 *
 * @param usage {@code DIRECT} for {@code tables[]}/{@code directTables[]} — this node's own
 *              statements — or {@code SUBTREE} for {@code subtreeTables[]}, which is everything
 *              reachable below it. Kept as one collection with a discriminator rather than two,
 *              because every consumer wants one or the other by name and none wants a union
 * @param lines the lines the operation appears on, verbatim from {@code operations.<TYPE>}, which
 *              the corpus observes as an array of integers in every one of 40,748 table entries
 */
public record TableUsageRow(String analysis,
                            String nodeId,
                            String nodeName,
                            String tableKey,
                            String tableSchema,
                            String tableName,
                            String objectType,
                            String operation,
                            String usage,
                            Integer totalUsageCount,
                            Integer directUsageCount,
                            List<Integer> lines) {

    public static final String DIRECT = "DIRECT";
    public static final String SUBTREE = "SUBTREE";

    /**
     * The indexed lookup key for a table, normalised for case.
     *
     * @return {@code null} when there is no name — PLAN-11 rule 4, emit nothing for null. 68 of the
     *         40,748 observed table entries have a null {@code name}, and a row keyed on
     *         {@code "ANALYZERDB.null"} would be a table that does not exist appearing in results.
     */
    public static String keyOf(String schema, String name) {
        if (name == null || name.isBlank()) return null;
        String n = name.toUpperCase(java.util.Locale.ROOT);
        if (schema == null || schema.isBlank()) return n;
        return schema.toUpperCase(java.util.Locale.ROOT) + "." + n;
    }
}
