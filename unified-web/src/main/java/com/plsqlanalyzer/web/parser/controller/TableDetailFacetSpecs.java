package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The paging contract of the two Table Detail sub-lists — PLAN-08 Q59 and Q59b.
 *
 * <p>Kept out of {@link ParserFacetSpecs} only to keep the two conversions independently
 * revertible; the declarations follow the same rules.
 *
 * <p><b>{@code SCAN} is the default sort in both.</b> These lists never had a sort applied before
 * conversion — the browser rendered them in the order its node scan produced. {@code SCAN} is that
 * order, made explicit as the row's {@code seq}, so page 1 after conversion shows the same rows in
 * the same order as the unpaged list. Any other default would silently reorder the table on the
 * day it was converted.
 *
 * <p>Each {@code id} is the de-duplication key the scan already merged rows under, so it is unique
 * by construction. It is deliberately not {@code seq}: re-running an analysis renumbers the scan,
 * and a cursor minted before the re-run would resume at a row that has moved.
 */
final class TableDetailFacetSpecs {

    private TableDetailFacetSpecs() {
    }

    private static String lower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }

    private static String upper(JsonNode row, String field) {
        return row.path(field).asText("").toUpperCase(Locale.ROOT);
    }

    /**
     * The procedure a flat row belongs to, for the Direct/Subtree/SubFlow selector. One id per
     * row, because these rows are per-procedure already — unlike the merged usage facets, which
     * carry a list in {@code accessDetails}.
     */
    private static List<String> rowProcedure(JsonNode row) {
        String pid = row.path("procedureId").asText("");
        if (pid.isEmpty()) pid = row.path("procedureName").asText("");
        return pid.isEmpty() ? List.of() : List.of(pid.toUpperCase(Locale.ROOT));
    }

    /** Q59 — joins on one table. Column indices are the modal's own {@code data-sort-col}s. */
    static final FacetSpec TABLE_JOINS = new FacetSpec(
            "joins",
            FacetSort.byName(
                    FacetSort.number("SCAN", "seq", false),
                    FacetSort.text("TYPE_ASC", "joinType", false),
                    FacetSort.text("TYPE_DESC", "joinType", true),
                    FacetSort.text("TABLE_ASC", "joinedTable", false),
                    FacetSort.text("TABLE_DESC", "joinedTable", true),
                    FacetSort.text("PROCEDURE_ASC", "procedureName", false),
                    FacetSort.text("PROCEDURE_DESC", "procedureName", true),
                    FacetSort.number("LINE_ASC", "line", false),
                    FacetSort.number("LINE_DESC", "line", true)),
            "SCAN",
            50,
            row -> row.path("direction").asText("") + "|" + upper(row, "joinType") + "|"
                    + upper(row, "joinedTable") + "|" + upper(row, "procedureId"),
            // Mirrors the modal's searchFn: join type, joined table, procedure, condition.
            row -> lower(row.path("joinType").asText("") + " "
                    + row.path("joinedTable").asText("") + " "
                    + row.path("procedureName").asText("") + " "
                    + row.path("condition").asText("")),
            // No filter pills on this table; the pills in the modal header filter Accesses.
            null,
            TableDetailFacetSpecs::rowProcedure,
            Map.of("0", FacetSpec.scalarColumn("joinType", "JOIN"),
                   "1", FacetSpec.scalarColumn("joinedTable", null),
                   "2", FacetSpec.scalarColumn("joinedTableAlias", "-"),
                   "4", FacetSpec.scalarColumn("procedureName", null)));

    /** Every value in an array field of a row, upper-cased — for pills and column filters. */
    private static List<String> arrayValues(JsonNode row, String field) {
        JsonNode arr = row.path(field);
        if (!arr.isArray() || arr.isEmpty()) return List.of();
        List<String> out = new java.util.ArrayList<>(arr.size());
        arr.forEach(n -> out.add(n.asText("").toUpperCase(Locale.ROOT)));
        return out;
    }

    private static String joined(JsonNode row, String field) {
        return String.join(" ", arrayValues(row, field));
    }

    /**
     * B3 — one table's folded column detail.
     *
     * <p>Default sort is {@code SCAN} for the same reason as the other two: it is the order the
     * fold produced, so page 1 is the head of the list a caller would have got unpaged. The id is
     * the column name, which is unique by construction — the fold is one row per column.
     *
     * <p>The pills are {@code usages} ({@code SELECT}/{@code SET}/{@code INSERT}), because "which
     * columns does anything write" is the question this data exists to answer and it is not
     * answerable by sorting. {@code valueSources} is a filter column rather than a pill: a column
     * can have several, so as a pill the counts would sum to more than the row count.
     */
    static final FacetSpec TABLE_COLUMN_DETAIL = new FacetSpec(
            "column detail",
            FacetSort.byName(
                    FacetSort.number("SCAN", "seq", false),
                    FacetSort.text("COLUMN_ASC", "columnName", false),
                    FacetSort.text("COLUMN_DESC", "columnName", true),
                    FacetSort.number("LINE_ASC", "firstLine", false),
                    FacetSort.number("LINE_DESC", "firstLine", true),
                    FacetSort.text("EXPRESSION_ASC", "sampleExpression", false),
                    FacetSort.text("EXPRESSION_DESC", "sampleExpression", true)),
            "SCAN",
            100,
            row -> upper(row, "columnName"),
            row -> lower(row.path("columnName").asText("") + " "
                    + row.path("sampleExpression").asText("") + " "
                    + joined(row, "usages") + " " + joined(row, "valueSources") + " "
                    + joined(row, "intoVariables") + " " + joined(row, "referencedBy")),
            row -> arrayValues(row, "usages"),
            row -> arrayValues(row, "referencedBy"),
            Map.of("0", FacetSpec.scalarColumn("columnName", null),
                   "1", FacetSpec.arrayColumn("usages"),
                   "2", FacetSpec.arrayColumn("valueSources")));

    /** Q59b — column references to one table. */
    static final FacetSpec TABLE_COLUMNS = new FacetSpec(
            "column references",
            FacetSort.byName(
                    FacetSort.number("SCAN", "seq", false),
                    FacetSort.text("COLUMN_ASC", "refField", false),
                    FacetSort.text("COLUMN_DESC", "refField", true),
                    FacetSort.text("VARIABLE_ASC", "varName", false),
                    FacetSort.text("VARIABLE_DESC", "varName", true),
                    FacetSort.text("PROCEDURE_ASC", "procedureName", false),
                    FacetSort.text("PROCEDURE_DESC", "procedureName", true),
                    FacetSort.number("LINE_ASC", "line", false),
                    FacetSort.number("LINE_DESC", "line", true)),
            "SCAN",
            100,
            row -> upper(row, "refType") + "|" + upper(row, "procedureId") + "|"
                    + upper(row, "varName") + "|" + upper(row, "refField") + "|"
                    + row.path("line").asLong(0),
            row -> lower(row.path("refField").asText("") + " " + row.path("varName").asText("")
                    + " " + row.path("dataType").asText("") + " "
                    + row.path("procedureName").asText("")),
            row -> List.of(row.path("refType").asText("TYPE_REF")),
            TableDetailFacetSpecs::rowProcedure,
            Map.of("1", FacetSpec.scalarColumn("refField", null),
                   "2", FacetSpec.scalarColumn("refType", null)));
}
