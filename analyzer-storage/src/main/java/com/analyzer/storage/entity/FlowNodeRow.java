package com.analyzer.storage.entity;

import java.util.List;

/**
 * One PL/SQL flow node — PLAN-04's {@code flow_node}, PLAN-05's spine, PLAN-11 §3's highest-fanout
 * row.
 *
 * <p>PLAN-11 measured {@code nodeId} appearing in 827 files and {@code schema}, {@code objectType},
 * {@code lineStart}, {@code lineEnd}, {@code linesOfCode} in 739. Those 827 duplicated copies of the
 * same six columns are ~8.7 MB of every 14.3 MB PL/SQL analysis. <b>One row per node replaces all of
 * them.</b>
 *
 * <p>{@code depth} is a column and <b>not part of any key</b>: PLAN-11 §3 and rule 5 — the crawl is
 * non-deterministic, so re-running renumbers depth and node ordinals. The key is
 * {@code (analysis, nodeId)}.
 *
 * @param parameters PLAN-11 §3: {@code parameters[].{name,dataType,direction,line}}, a child table
 *                   in the relational model, an ordinal-tagged embedded array here (see
 *                   {@link PlsqlEntityMapper} for why that is the same thing in MongoDB)
 * @param statements {@code statementSummary} — <b>never an enum.</b> PLAN-11 §3 says the key set is
 *                   11–28 dialect-specific tokens; this corpus has 43. A closed enum would drop the
 *                   ones it did not know about, silently
 */
public record FlowNodeRow(String analysis,
                          String nodeId,
                          String name,
                          String schema,
                          String objectType,
                          Integer depth,
                          Integer lineStart,
                          Integer lineEnd,
                          Integer linesOfCode,
                          String sourceFile,
                          List<Parameter> parameters,
                          List<StatementCount> statements) {

    /** {@code flow_node_parameter} — keyed by {@code (nodeId, ordinal)}, never by position alone. */
    public record Parameter(int ordinal, String name, String dataType, String direction,
                            Integer line) {
    }

    /** {@code flow_node_statement(node_id, statement_type, count)}. An open token set, not an enum. */
    public record StatementCount(String statementType, int count) {
    }
}
