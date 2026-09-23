package com.plsql.parser.service;

import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.TableOperationInfo;
import com.plsqlanalyzer.parser.model.SqlAnalysisResult;
import com.plsqlanalyzer.parser.service.SqlAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Adds column-level detail to table operations, for every datasource.
 *
 * <h2>Why this is shared rather than per-dialect</h2>
 * There are five datasources and two parser families. Oracle routes its SQL through
 * {@link SqlAnalyzer}; DB2, MySQL, Postgres and SQL Server each have their own ANTLR grammar
 * engine and never touch it. Implementing "which columns does this statement write, and where do
 * the values come from" four more times would mean four separate things to keep correct.
 *
 * <p>Instead each dialect contributes one thing it already has — the statement's original text, via
 * {@link TableOperationInfo#getSqlText()} — and this class does the analysis once. JSqlParser
 * handles ANSI DML across dialects, so the same code serves all five.
 *
 * <h2>What it does not do</h2>
 * It does not guess. A statement JSqlParser cannot parse simply gets no column detail; the table
 * operation itself is untouched and every existing field keeps its value. That matters because
 * dialect SQL contains plenty that ANSI parsing will reject, and a wrong column list would be far
 * worse than an absent one — it would appear on a table's page as fact.
 *
 * <p>{@link #coverage} exists so that "how much of each dialect does this actually cover" is a
 * measurement rather than an assumption.
 */
public final class TableOperationEnricher {

    private static final Logger log = LoggerFactory.getLogger(TableOperationEnricher.class);

    private final SqlAnalyzer analyzer = new SqlAnalyzer();

    /** Parsed vs attempted, so per-dialect coverage can be reported instead of assumed. */
    public record Coverage(int attempted, int parsed, int withColumns) {
        public double parsedPct() { return attempted == 0 ? 0 : (parsed * 100.0) / attempted; }

        @Override
        public String toString() {
            return String.format("%d/%d parsed (%.1f%%), %d with column detail",
                    parsed, attempted, parsedPct(), withColumns);
        }
    }

    /**
     * A dialect-specific spelling that JSqlParser rejects, rewritten to the ANSI form.
     *
     * <p>Only spellings whose rewrite cannot change the answer belong here. Rewriting a table or
     * column name would be inventing data; rewriting the <em>notation</em> of a literal is not.
     */
    public enum Dialect {
        /** No normalisation — the behaviour every caller had before this existed. */
        GENERIC,
        /**
         * T-SQL. Three notations that JSqlParser rejects or misreads, each rewritten to its ANSI
         * equivalent. All three were measured against the 57 real T-SQL sources in this repo, not
         * guessed:
         *
         * <ul>
         *   <li>{@code N'x'} -> {@code 'x'} — the national-string prefix made JSqlParser return
         *       no operation type at all, so one {@code N} anywhere discarded that statement's
         *       whole column list. The prefix carries no provenance: the value is a literal
         *       either way.</li>
         *   <li>{@code @var} in an expression -> {@code :var} — a T-SQL local variable is a bound
         *       value. Left as {@code @var} it was classified {@code COLUMN}, which is a wrong
         *       provenance, not merely a missing one.</li>
         *   <li>{@code @tab} in a table position -> a plain identifier — a table variable.
         *       JSqlParser rejects {@code INSERT INTO @tab(...)} outright and lost the column
         *       list. The rewritten name is never used: this class reads only the column usages
         *       and DDL out of the analysis, and the table each belongs to is already known from
         *       the operation itself.</li>
         * </ul>
         */
        SQLSERVER;

        /**
         * The rewrites to try, in order, stopping at the first that parses.
         *
         * <p>There is more than one because {@code @outer} becomes {@code :outer} and
         * <em>{@code OUTER} is a reserved word</em> — JSqlParser then rejects the whole statement.
         * That was the single remaining unparsed pattern on the real T-SQL sources after the other
         * fixes. Rather than guess at JSqlParser's reserved-word list, the second candidate
         * prefixes every parameter so a collision is impossible, and the third is the untouched
         * original so normalisation can never make a statement worse than it was.
         */
        List<String> candidates(String sql) {
            if (this != SQLSERVER) return List.of(sql);
            // Sep05-12: table hints are removed before the other rewrites. `FROM t WITH (NOEXPAND)`
            // is rejected outright by JSqlParser, and it was the whole of the remaining SQL Server
            // gap — 12 statements, all of the form
            // `SELECT @n = @n + COUNT(*) FROM erp.vw_ProductSales WITH (NOEXPAND)`.
            //
            // A hint is an instruction to the query planner, not part of what the statement reads,
            // so dropping it cannot change any column, predicate or table this class extracts.
            String hintless = stripTableHints(sql);
            String plain = normaliseTsql(hintless, "");
            String prefixed = normaliseTsql(hintless, "v_");
            List<String> out = new java.util.ArrayList<>(4);
            out.add(plain);
            if (!prefixed.equals(plain)) out.add(prefixed);
            // The untouched original stays last, as before: normalisation must never be able to
            // make a statement parse worse than it did before being rewritten.
            if (!sql.equals(plain)) out.add(sql);
            return out;
        }
    }

    /**
     * A T-SQL table hint: {@code WITH (NOLOCK)}, {@code WITH (NOEXPAND, INDEX(ix_a))}.
     *
     * <p>The {@code (?=\s*\()} lookahead is what separates a hint from a CTE. A CTE is
     * {@code WITH name AS (…)} — a name always stands between {@code WITH} and the parenthesis —
     * whereas a hint opens its parenthesis immediately. Without that distinction this pattern would
     * eat the head of every CTE in the codebase and turn working statements into broken ones, which
     * is a far larger loss than the 12 statements it is meant to recover.
     */
    private static final java.util.regex.Pattern TSQL_TABLE_HINT = java.util.regex.Pattern.compile(
            "(?i)\\bWITH\\s*(?=\\()\\((?:[^()]|\\([^()]*\\))*\\)");

    /**
     * Removes T-SQL table hints, leaving everything else untouched.
     *
     * <p>Applied only for {@link Dialect#SQLSERVER}. The inner alternation tolerates one level of
     * nesting so {@code WITH (INDEX(ix_orders))} is removed whole rather than leaving a stray
     * {@code )} behind — a half-removed hint would be worse than the hint itself.
     */
    static String stripTableHints(String sql) {
        if (sql == null) return null;
        // Cheap guard: most statements have no hint, and this runs on every statement analysed.
        if (sql.indexOf('(') < 0) return sql;
        String out = TSQL_TABLE_HINT.matcher(sql).replaceAll(" ");
        return out;
    }

    /** Table-position keywords: an {@code @name} straight after one of these is a table variable. */
    private static final java.util.Set<String> TABLE_POSITION_KEYWORDS =
            java.util.Set.of("INTO", "FROM", "JOIN", "UPDATE", "TABLE", "APPLY", "MERGE");

    /**
     * Rewrites T-SQL notation to its ANSI equivalent, outside string literals, bracket
     * identifiers and comments — so {@code 'ends in N'}, {@code 'a@b'} and {@code [N]} are
     * untouched. See {@link Dialect#SQLSERVER} for what is rewritten and why each rewrite cannot
     * change the answer.
     */
    public static String normaliseTsql(String sql) {
        return normaliseTsql(sql, "");
    }

    /** @param paramPrefix prepended to every rewritten parameter name, to dodge reserved words. */
    public static String normaliseTsql(String sql, String paramPrefix) {
        if (sql == null) return null;
        if (sql.indexOf('\'') < 0 && sql.indexOf('@') < 0) return sql;
        StringBuilder out = new StringBuilder(sql.length());
        String lastWord = "";
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {                                  // literal — copy verbatim
                out.append(c); i++;
                while (i < n) {
                    char d = sql.charAt(i);
                    out.append(d); i++;
                    if (d == '\'') {
                        if (i < n && sql.charAt(i) == '\'') { out.append('\''); i++; continue; }
                        break;
                    }
                }
                lastWord = "";
                continue;
            }
            if (c == '[') {                                   // bracket identifier
                while (i < n) { out.append(sql.charAt(i)); if (sql.charAt(i) == ']') { i++; break; } i++; }
                lastWord = "";
                continue;
            }
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                while (i < n && sql.charAt(i) != '\n') { out.append(sql.charAt(i)); i++; }
                continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                out.append("/*"); i += 2;
                while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) { out.append(sql.charAt(i)); i++; }
                if (i + 1 < n) { out.append("*/"); i += 2; } else { while (i < n) { out.append(sql.charAt(i)); i++; } }
                continue;
            }
            if ((c == 'N' || c == 'n') && i + 1 < n && sql.charAt(i + 1) == '\''
                    && (i == 0 || !isIdentChar(sql.charAt(i - 1)))) {
                i++;                                          // drop the prefix, keep the literal
                continue;
            }
            if (c == '@') {
                if (i + 1 < n && sql.charAt(i + 1) == '@') {  // @@ROWCOUNT etc — a system function
                    out.append("@@"); i += 2;
                    while (i < n && isWordChar(sql.charAt(i))) { out.append(sql.charAt(i)); i++; }
                    lastWord = "";
                    continue;
                }
                int j = i + 1;
                while (j < n && isWordChar(sql.charAt(j))) j++;
                if (j == i + 1) { out.append(c); i++; continue; }   // a bare @, leave it
                String name = sql.substring(i + 1, j);
                out.append(TABLE_POSITION_KEYWORDS.contains(lastWord)
                        ? "tsqlvar_" + name : ":" + paramPrefix + name);
                lastWord = "";
                i = j;
                continue;
            }
            if (isWordChar(c)) {
                int j = i;
                while (j < n && isWordChar(sql.charAt(j))) j++;
                String word = sql.substring(i, j);
                out.append(word);
                lastWord = word.toUpperCase();
                i = j;
                continue;
            }
            out.append(c); i++;
            if (!Character.isWhitespace(c)) lastWord = "";
        }
        return out.toString();
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '#' || c == '$';
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#' || c == '.';
    }

    /**
     * The subset of a statement's column usages that belong to <em>this</em> operation's table.
     *
     * <h3>The defect this fixes</h3>
     * {@code INSERT INTO audit(a, b) SELECT x, y FROM product} analyses to five usages: three
     * {@code INSERT} on {@code audit} and two {@code SELECT} which are columns of
     * {@code product}. Every one of them was being attached to the {@code audit} operation, and
     * the fold step keys column detail by the operation's table name — so {@code product}'s
     * columns were appearing on {@code audit}'s page, stated as fact. The source table gets its
     * own {@code SELECT} operation with its own usages, so nothing is lost by dropping them here.
     *
     * <p>An operation whose type is not one of the three (MERGE, DELETE, DDL, or unknown) keeps
     * every usage, as before: there is no mapping to apply and guessing would be worse.
     */
    private static List<com.plsqlanalyzer.parser.model.ColumnUsage> ownedBy(
            TableOperationInfo op, List<com.plsqlanalyzer.parser.model.ColumnUsage> all) {
        String owned = switch (op.getOperation() == null ? "" : op.getOperation().toUpperCase()) {
            case "INSERT" -> "INSERT";
            case "UPDATE" -> "SET";
            case "SELECT" -> "SELECT";
            default -> null;
        };
        if (owned == null) return all;
        List<com.plsqlanalyzer.parser.model.ColumnUsage> mine = new java.util.ArrayList<>();
        for (com.plsqlanalyzer.parser.model.ColumnUsage u : all) {
            if (owned.equals(u.getUsage())) mine.add(u);
        }
        return mine;
    }

    /**
     * ING-024: does this join have {@code tableName} on either side? Compared on the last
     * {@code schema.}-qualified segment, case-insensitively — the same tolerant form the analyser's
     * own table references and this class's callers already use elsewhere (e.g. the search
     * screen's fuzzy id matching), since a join's own table names and an op's {@code tableName} are
     * not guaranteed to agree on schema-qualification.
     */
    private static boolean joinTouchesTable(com.plsqlanalyzer.parser.model.JoinInfo j, String tableName) {
        if (tableName == null || tableName.isBlank()) return false;
        String want = lastSegment(tableName);
        return want.equals(lastSegment(j.getLeftTable())) || want.equals(lastSegment(j.getRightTable()));
    }

    /**
     * ING-024: identifies "the same statement" across the several per-table ops it produces.
     *
     * <p>{@code sqlText} alone, not {@code line} — measured directly (a debug dump of a real
     * 3-join cursor) that sibling ops sharing one statement carry *different* line numbers (each
     * one the line of that op's own table reference within the statement) despite sharing the
     * exact same {@code sqlText} content. Including {@code line} in the key made every sibling
     * look like a distinct statement, defeating the dedupe entirely.
     */
    private static String statementKey(TableOperationInfo op) {
        return op.getSqlText();
    }

    private static String lastSegment(String qualified) {
        if (qualified == null) return "";
        int dot = qualified.lastIndexOf('.');
        return (dot >= 0 ? qualified.substring(dot + 1) : qualified).toUpperCase(java.util.Locale.ROOT);
    }

    /**
     * Analyses one statement, trying each of the dialect's rewrites until one parses.
     *
     * <p>Returns the first result whose operation type is non-null, or the last attempt if none
     * parsed — so an unparseable statement still yields an empty result rather than an exception.
     */
    private SqlAnalysisResult analyseWithFallback(String sql, Dialect dialect, TableOperationInfo op) {
        SqlAnalysisResult last = null;
        for (String candidate : dialect.candidates(sql)) {
            try {
                last = analyzer.analyze(candidate, op.getLine());
            } catch (Exception e) {
                // SqlAnalyzer already swallows parse failures; this guards the unexpected.
                log.debug("enrichment failed for {} line {}: {}",
                        op.getTableName(), op.getLine(), e.toString());
                continue;
            }
            if (last.getOperationType() != null) return last;
        }
        return last;
    }

    /** Enriches every table operation on every object. Returns what it managed to cover. */
    public Coverage enrich(List<ParsedObject> objects) {
        return enrich(objects, Dialect.GENERIC);
    }

    /**
     * ING-024 (fixed; found while wiring ING-019's dynamic-SQL DDL through this same path) — a
     * standalone {@code CREATE PROCEDURE}'s operations land on {@code ParsedObject
     * .tableOperations} (the ANTLR visitor's {@code currentSubprogram()} is null at the top level),
     * but a procedure declared inside a {@code PACKAGE BODY} pushes a {@code SubprogramInfo}
     * context, so its operations land on {@code SubprogramInfo.tableOperations} instead. Both are
     * walked now, via {@link #enrichOps}.
     *
     * <p>The reason this wasn't a one-line addition the first time: a multi-table statement (a
     * JOIN chain) produces one {@code TableOperationInfo} per table it touches, all sharing the
     * same {@code sqlText}. The join-carry step below used to attach the analyser's <em>entire</em>
     * per-statement join list to every op sharing that text with no native joins of its own —
     * harmless while only one op per statement was ever reachable (the object-level path), but the
     * moment subprogram-level statements (where a multi-table cursor query actually lives) were
     * also enriched, a real 3-join query measured as 12. Fixed by scoping join-carry to the op's
     * own table (see {@link #joinTouchesTable}), the same criterion {@link #ownedBy} already
     * applies to column usages — not by walking fewer operations.
     */
    public Coverage enrich(List<ParsedObject> objects, Dialect dialect) {
        int[] counts = new int[3]; // attempted, parsed, withCols
        for (ParsedObject obj : objects) {
            if (obj == null) continue;
            enrichOps(obj.getTableOperations(), dialect, counts);
            for (var sub : obj.getSubprograms()) {
                if (sub == null) continue;
                enrichOps(sub.getTableOperations(), dialect, counts);
            }
        }
        Coverage c = new Coverage(counts[0], counts[1], counts[2]);
        if (counts[0] > 0) log.info("TableOperationEnricher[{}]: {}", dialect, c);
        return c;
    }

    private void enrichOps(List<TableOperationInfo> ops, Dialect dialect, int[] counts) {
        if (ops == null) return;
        // ING-024: a multi-table statement produces one op per table it touches, all sharing the
        // same sqlText+line — and the visitor's own native population already puts the statement's
        // full, correct join list on exactly one of them. If the join-carry fallback below ran for
        // every *other* op sharing that statement too (each individually empty, so each individually
        // eligible), the same 3 real joins would be re-derived and attached three more times, once
        // per table — a real 3-join query measured as 6, not the 12 a per-op-unscoped carry gave
        // before, but still wrong. Recorded once per statement, before the loop mutates anything, so
        // the carry step below can tell "no op for this statement has native joins yet" (a real
        // dialect gap, safe to fill in, scoped per table) from "a sibling op already has them" (fully
        // answered already, adding more from the same statement's text would only duplicate).
        java.util.Set<String> statementsWithNativeJoins = new java.util.HashSet<>();
        for (TableOperationInfo op : ops) {
            if (!op.getJoins().isEmpty()) statementsWithNativeJoins.add(statementKey(op));
        }
        for (TableOperationInfo op : ops) {
            String sql = op.getSqlText();
            if (sql == null || sql.isBlank()) continue;
            counts[0]++;
            SqlAnalysisResult r = analyseWithFallback(sql, dialect, op);
            if (r == null) { op.setColumnsParsed(false); continue; }
            boolean ok = r.getOperationType() != null;
            // Recorded per statement, so an empty column list downstream can be read as
            // "nothing to report" rather than "we could not parse this".
            op.setColumnsParsed(ok);
            if (ok) counts[1]++;
            List<com.plsqlanalyzer.parser.model.ColumnUsage> mine = ownedBy(op, r.getColumnUsages());
            if (!mine.isEmpty()) {
                op.getColumnUsages().addAll(mine);
                counts[2]++;
            }
            if (!r.getDdl().isEmpty()) op.getDdl().addAll(r.getDdl());

            // Sep05-02: carry the joins.
            //
            // TableOperationInfo.joins is filled by the per-dialect visitors, so join detail was
            // inconsistent between dialects rather than absent — and the shared analyser's own
            // joinInfos were computed and dropped alongside the predicates. Added only when the
            // visitor found none, so a dialect that already resolves aliases properly keeps its
            // better answer instead of being overwritten by the generic one.
            //
            // ING-024: scoped to *this op's own table*, the same reasoning `ownedBy` already
            // applies to column usages. A single multi-table statement (a 3-way JOIN chain, say)
            // produces one `TableOperationInfo` per table it touches, all sharing the same
            // `sqlText` — `SqlAnalyzer.analyze()` finds every join in that text regardless of
            // which op asked, so attaching the *whole* list to every op sharing the statement
            // multiplied 3 real joins into 12 the moment more than one op per statement was ever
            // enriched (previously masked: only object-level, usually-one-op-per-statement code
            // paths were ever enriched; subprogram-level statements, where a multi-table cursor
            // query actually lives, were not). One join belongs to an op if that op's own table is
            // either side of it — exactly the criterion the query itself expresses.
            if (op.getJoins().isEmpty() && !r.getJoinInfos().isEmpty()
                    && !statementsWithNativeJoins.contains(statementKey(op))) {
                // Two different JoinInfo types: the analyser's carries leftTable/rightTable/
                // onPredicate, the model's (which the UI sees) carries joinedTable/condition
                // plus aliases and a queryId. Converted rather than swapped, because the
                // model's extra fields are what let a screen disambiguate two joins on the
                // same pair of tables — they are simply unknown on this path.
                for (com.plsqlanalyzer.parser.model.JoinInfo j : r.getJoinInfos()) {
                    if (!joinTouchesTable(j, op.getTableName())) continue;
                    com.plsql.parser.model.JoinInfo m = new com.plsql.parser.model.JoinInfo();
                    m.setJoinType(j.getJoinType());
                    m.setLeftTable(j.getLeftTable());
                    m.setJoinedTable(j.getRightTable());
                    m.setCondition(j.getOnPredicate());
                    m.setLine(j.getLineNumber());
                    op.getJoins().add(m);
                }
            }

            // Sep05-01: carry the predicate. It was computed on every statement and dropped
            // here, which is where the empty column cells came from — a predicate column is
            // never a ColumnUsage, so for a table that is mostly filtered or joined on, this
            // collection held all of it.
            if (!r.getWhereFilters().isEmpty()) {
                op.getWhereFilters().addAll(r.getWhereFilters());
                // Exact only when the statement named one table; WhereFilter has no qualifier,
                // so on a join a filter cannot be attributed to one side. Recorded rather than
                // assumed, so the screen can say which it is.
                op.setPredicateScope(r.getTableReferences() != null
                        && r.getTableReferences().size() == 1 ? "table" : "statement");
            }
        }
    }

    /** Measures without mutating — for reporting coverage per dialect. */
    public Coverage coverage(List<ParsedObject> objects) {
        return coverage(objects, Dialect.GENERIC);
    }

    /**
     * Measures without mutating — for reporting coverage per dialect.
     *
     * <p>ING-024: walks {@code obj.getSubprograms()}'s own operations too, same as {@link #enrich},
     * so a package-body-heavy corpus doesn't silently under-report coverage relative to a
     * standalone-procedure one — this method never mutates, so there is no join-duplication risk
     * to guard against here the way {@link #enrichOps} has to.
     */
    public Coverage coverage(List<ParsedObject> objects, Dialect dialect) {
        int[] counts = new int[3];
        for (ParsedObject obj : objects) {
            if (obj == null) continue;
            coverageOps(obj.getTableOperations(), dialect, counts);
            for (var sub : obj.getSubprograms()) {
                if (sub == null) continue;
                coverageOps(sub.getTableOperations(), dialect, counts);
            }
        }
        return new Coverage(counts[0], counts[1], counts[2]);
    }

    private void coverageOps(List<TableOperationInfo> ops, Dialect dialect, int[] counts) {
        if (ops == null) return;
        for (TableOperationInfo op : ops) {
            String sql = op.getSqlText();
            if (sql == null || sql.isBlank()) continue;
            counts[0]++;
            try {
                SqlAnalysisResult r = analyseWithFallback(sql, dialect, op);
                if (r == null) continue;
                if (r.getOperationType() != null) counts[1]++;
                if (!ownedBy(op, r.getColumnUsages()).isEmpty()) counts[2]++;
            } catch (Exception ignore) {
                // counted as attempted-but-unparsed
            }
        }
    }
}
