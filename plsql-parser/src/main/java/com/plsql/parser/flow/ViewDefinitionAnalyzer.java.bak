package com.plsql.parser.flow;

import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.JoinInfo;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.TableOperationInfo;
import com.plsql.parser.multidb.StoredProcedureParser;
import com.plsql.parser.multidb.StoredProcedureParserFactory;
import com.plsqlanalyzer.config.DbType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs a view/materialized-view's catalog-fetched definition text through the same
 * dialect-specific FROM/JOIN extractor a real {@code CREATE VIEW} statement already goes
 * through when it appears inline in a parsed script — so the tables a view selects from, and
 * how it joins them, become visible without hand-rolling a second SELECT-list walker.
 *
 * <p>{@link DependencyCrawler#isNonParsableType} still refuses to feed VIEW/MATERIALIZED_VIEW
 * source through the main crawl queue; this class is the separate, targeted path each table
 * enricher calls once it already has the definition text from the catalog.
 *
 * <p>The catalog text is, depending on dialect/catalog column, either the bare SELECT body
 * (Oracle {@code ALL_VIEWS.TEXT}/{@code ALL_MVIEWS.QUERY}, MySQL
 * {@code INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION}, Postgres
 * {@code INFORMATION_SCHEMA.VIEWS.view_definition}/{@code pg_matviews.definition}) or the full
 * {@code CREATE VIEW} statement (SQL Server {@code sys.sql_modules.definition}, DB2
 * {@code SYSCAT.VIEWS.TEXT}). Either shape is accepted: a bare SELECT body is wrapped in a
 * synthetic {@code CREATE VIEW}/{@code CREATE MATERIALIZED VIEW} so it can reach the dialect's
 * normal entry point; text that already starts with {@code CREATE} is parsed unchanged.
 */
public final class ViewDefinitionAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(ViewDefinitionAnalyzer.class);

    private ViewDefinitionAnalyzer() {}

    /** Table operations + joins pulled out of a view/MV's own SELECT body. Never null; may be empty. */
    public static final class ViewDefinitionResult {
        public final List<TableOperationInfo> tableOperations;
        public final List<JoinInfo> joins;

        ViewDefinitionResult(List<TableOperationInfo> tableOperations, List<JoinInfo> joins) {
            this.tableOperations = tableOperations;
            this.joins = joins;
        }

        static ViewDefinitionResult empty() {
            return new ViewDefinitionResult(Collections.emptyList(), Collections.emptyList());
        }

        public boolean isEmpty() { return tableOperations.isEmpty() && joins.isEmpty(); }
    }

    /**
     * @param dialect         the view's own dialect (null treated as Oracle)
     * @param viewName        bare (unqualified) view/MV name, used only as the synthetic DDL's
     *                        object name — never persisted
     * @param definitionText  the view's SQL body/definition as fetched from the catalog
     * @param isMaterialized  true for a materialized view / MQT; only changes the wrapper DDL
     *                        keyword for Oracle (the other dialects have no native MV DDL)
     * @return table operations and joins found in the definition; empty (never null) on any
     *         parse failure — a malformed or truncated definition must not break the caller
     */
    public static ViewDefinitionResult analyze(DbType dialect, String viewName, String definitionText,
                                               boolean isMaterialized) {
        if (definitionText == null || definitionText.isBlank()) return ViewDefinitionResult.empty();
        String name = (viewName == null || viewName.isBlank()) ? "V" : viewName;

        String wrapped;
        try {
            wrapped = wrap(dialect, name, definitionText, isMaterialized);
        } catch (Exception e) {
            log.debug("[ViewDefinitionAnalyzer] could not build DDL wrapper for {}: {}", viewName, e.getMessage());
            return ViewDefinitionResult.empty();
        }

        try {
            List<TableOperationInfo> ops;
            if (dialect == null || dialect == DbType.ORACLE) {
                ops = collectOps(new PlSqlParserEngine().parseContent(wrapped, name + ".sql").getObjects());
            } else {
                StoredProcedureParser parser = StoredProcedureParserFactory.get(dialect);
                if (parser == null) return ViewDefinitionResult.empty();
                ops = collectOps(parser.parse(wrapped, name, null).getObjects());
            }
            if (ops.isEmpty()) return ViewDefinitionResult.empty();

            List<JoinInfo> joins = new ArrayList<>();
            for (TableOperationInfo op : ops) {
                if (op.getJoins() != null) joins.addAll(op.getJoins());
            }
            return new ViewDefinitionResult(ops, joins);
        } catch (Exception e) {
            // Malformed/truncated definition text (e.g. Oracle's 32000-char cap can cut a
            // statement mid-token) must not fail the enricher that called this — the raw
            // viewDefinition is still useful even when it cannot be parsed for table ops.
            log.debug("[ViewDefinitionAnalyzer] parse failed for {} ({}): {}", viewName, dialect, e.getMessage());
            return ViewDefinitionResult.empty();
        }
    }

    private static List<TableOperationInfo> collectOps(List<ParsedObject> objects) {
        List<TableOperationInfo> out = new ArrayList<>();
        if (objects == null) return out;
        for (ParsedObject obj : objects) {
            if (obj.getTableOperations() != null) out.addAll(obj.getTableOperations());
        }
        return out;
    }

    /**
     * Convenience for the table enrichers: parses {@code t}'s already-set {@code viewDefinition}
     * field (a no-op when it is absent or blank) and, when any table operations or joins were
     * found, sets {@code viewTableOperations}/{@code viewJoins} on the same node — the exact
     * pattern every enricher already uses for {@code viewDefinition} itself, so callers don't
     * each re-derive "is there a definition to parse" and "how do I serialize the result".
     */
    public static void attachTo(com.fasterxml.jackson.databind.node.ObjectNode t,
                                com.fasterxml.jackson.databind.ObjectMapper mapper,
                                DbType dialect, String viewName, boolean isMaterialized) {
        if (t == null || !t.hasNonNull("viewDefinition")) return;
        String def = t.get("viewDefinition").asText(null);
        if (def == null || def.isBlank()) return;
        ViewDefinitionResult vr = analyze(dialect, viewName, def, isMaterialized);
        if (vr.isEmpty()) return;
        t.set("viewTableOperations", mapper.valueToTree(vr.tableOperations));
        t.set("viewJoins", mapper.valueToTree(vr.joins));
    }

    /**
     * Builds parsable DDL from catalog text. Text that already looks like a full CREATE
     * statement (SQL Server/DB2 store the whole {@code CREATE VIEW}, not just the SELECT) is
     * passed through untouched; a bare SELECT body is wrapped so it reaches the dialect's
     * view-definition visitor. Oracle materialized views get {@code CREATE MATERIALIZED VIEW}
     * so the text hits {@code visitCreate_materialized_view} instead of {@code visitCreate_view}
     * — the other four dialects have no native MV DDL, so they always get {@code CREATE VIEW}
     * even when {@code isMaterialized} is true; the goal is only to get the SELECT body through
     * the existing FROM/JOIN walker, not to produce semantically perfect DDL.
     */
    private static String wrap(DbType dialect, String name, String definitionText, boolean isMaterialized) {
        String trimmed = definitionText.trim();
        if (trimmed.regionMatches(true, 0, "CREATE", 0, 6)) return trimmed;
        if (dialect == DbType.ORACLE && isMaterialized) {
            return "CREATE MATERIALIZED VIEW " + name + " AS " + trimmed;
        }
        return "CREATE OR REPLACE VIEW " + name + " AS " + trimmed;
    }
}
