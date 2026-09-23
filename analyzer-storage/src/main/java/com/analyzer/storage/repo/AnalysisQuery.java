package com.analyzer.storage.repo;

import java.util.Objects;

/**
 * What to query, and for which analysis.
 *
 * <p>A closed {@link Dataset} enum rather than a free-form name, for the same reason
 * {@code FacetSort} is a closed set: an open string here would be a way to ask for an artifact
 * nobody modelled, and the whole value of this type is that the mapping from "a screen's list" to
 * "the artifact behind it" is written down in exactly one place. Increment D adds Mongo collections
 * to that one place; it does not add a second table.
 *
 * <p>Not every component applies to every dataset, and that is checked at construction rather than
 * discovered as a null later. A table-scoped query without a table would otherwise silently become
 * "the whole analysis", which is the failure {@code TableDetailController} answers with a stated
 * 400.
 *
 * @param dataset  which list is being asked for
 * @param analysis the analysis identity — the {@code '~'}-joined PL/SQL identity, or the JAR id.
 *                 Never an ordinal: re-running an analysis renumbers its nodes (PLAN-00 finding 5)
 * @param version  JAR only: {@code null} for best-available, or {@code static}/{@code previous}.
 *                 Part of the query because the class tree of the corrected analysis and of the
 *                 static one are different lists cached under different keys
 * @param table    table-scoped datasets only: the table whose rows are wanted
 */
public record AnalysisQuery(Dataset dataset, String analysis, String version, String table) {

    /**
     * The lists this interface can answer, and the artifact family behind each.
     *
     * <p>One entry per <b>live paged endpoint</b>. Datasets are not added speculatively: an entry
     * with no consumer is an untested branch in every implementation, and in the scan
     * implementation an untested branch means an artifact whose shape nobody checked.
     */
    public enum Dataset {

        /** PL/SQL analysis history: one {@code api/index.json} per analysis. Use {@code analyses}. */
        PLSQL_ANALYSES(false, false),

        /** Joins, from a scan of every {@code api/nodes/*.json}. Memoised as today. */
        PLSQL_JOINS(true, false),
        /** Cursors, same scan. */
        PLSQL_CURSORS(true, false),
        /** Sequences, same scan. */
        PLSQL_SEQUENCES(true, false),
        /** Dynamic SQL, same scan. */
        PLSQL_DYNAMIC_SQL(true, false),

        /**
         * The procedure list — {@code api/index.json}'s {@code nodes} array. UI-029: added so
         * {@code /procedures} can take {@code scope}/{@code procId} the same way the other four do.
         */
        PLSQL_PROCEDURES(true, false),

        /**
         * Table operations, from {@code api/tables/index.json}.
         *
         * <p>Unlike the four above, this document is read whole rather than scanned out of the node
         * files. Its size is driven by {@code usedBy[].operations} — one entry per access line — so
         * it is the facet most in need of paging and the last one still returning the whole array.
         */
        PLSQL_TABLES(true, false),

        /**
         * Joins on one table. Replaces a browser-side {@code Promise.all} over every node document
         * in the open call tree (measured: 64 documents, 3.6 MB, per modal open).
         */
        PLSQL_TABLE_JOINS(true, true),
        /** Column references to one table. Replaces the second fan-out of the same modal. */
        PLSQL_TABLE_COLUMNS(true, true),
        /**
         * One table's column-level statement detail and in-procedure DDL — which columns each
         * statement reads or writes and, for a write, where the value came from.
         *
         * <p>Distinct from {@link #PLSQL_TABLE_COLUMNS} despite the name: that one is
         * {@code %TYPE}/{@code %ROWTYPE} variable declarations scanned out of the node documents,
         * this one is folded per column by the crawler into {@code tables.json}.
         */
        PLSQL_TABLE_COLUMN_DETAIL(true, true),

        /** JAR class tree, projected out of {@code analysis.json}. Use {@code rows}. */
        JAR_CLASS_TREE(true, false),
        /** JAR bundled-library list, {@code bundled-jars.json}. Use {@code rows}. */
        JAR_BUNDLED_JARS(true, false);

        private final boolean needsAnalysis;
        private final boolean needsTable;

        Dataset(boolean needsAnalysis, boolean needsTable) {
            this.needsAnalysis = needsAnalysis;
            this.needsTable = needsTable;
        }

        /** True for the datasets scoped to one table. */
        public boolean tableScoped() {
            return needsTable;
        }
    }

    public AnalysisQuery {
        Objects.requireNonNull(dataset, "dataset");
        if (dataset.needsAnalysis && (analysis == null || analysis.isBlank())) {
            throw new IllegalArgumentException(dataset + " requires an analysis");
        }
        if (dataset.needsTable && (table == null || table.isBlank())) {
            throw new IllegalArgumentException(dataset + " requires a table");
        }
        if (!dataset.needsTable && table != null) {
            throw new IllegalArgumentException(dataset + " is not table-scoped");
        }
    }

    /** A whole-analysis facet: the four node-scan facets, or the JAR bundled-JAR list. */
    public static AnalysisQuery of(Dataset dataset, String analysis) {
        return new AnalysisQuery(dataset, analysis, null, null);
    }

    /** A table-scoped facet — the two Table Detail sub-lists. */
    public static AnalysisQuery ofTable(Dataset dataset, String analysis, String table) {
        return new AnalysisQuery(dataset, analysis, null, table);
    }

    /** A JAR dataset at a specific version; {@code null} version means best-available. */
    public static AnalysisQuery ofJar(Dataset dataset, String jarId, String version) {
        return new AnalysisQuery(dataset, jarId, version, null);
    }

    /** The whole-store datasets, which name no analysis. */
    public static AnalysisQuery all(Dataset dataset) {
        return new AnalysisQuery(dataset, null, null, null);
    }
}
