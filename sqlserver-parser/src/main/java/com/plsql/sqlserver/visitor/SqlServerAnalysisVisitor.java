package com.plsql.sqlserver.visitor;

import com.plsql.sqlserver.grammar.SqlServerLexer;
import com.plsql.sqlserver.grammar.SqlServerParser;
import com.plsql.sqlserver.grammar.SqlServerParserBaseVisitor;
import com.plsql.sqlserver.model.*;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ANTLR4 visitor over the SqlServerParser parse tree.
 *
 * Extracts:
 *   visitRoutineDefinition  → routine identity (name, schema, type, line range)
 *   visitParam              → parameter list
 *   visitDeclareStatement   → DECLARE @variable
 *   visitSetStatement       → SET @var = ... (dynamic SQL detection)
 *   visitExecStatement      → EXEC / EXECUTE outbound calls + sp_executesql detection
 *   visitInsertStatement    → INSERT table op
 *   visitUpdateStatement    → UPDATE table op
 *   visitDeleteStatement    → DELETE table op
 *   visitSelectStatement    → SELECT table op (scans stmtTail for FROM clause)
 *
 * <p><b>Regex to grammar migration status (batch 2).</b> SELECT/FROM/JOIN extraction was already
 * migrated onto the {@code queryUnit} parse tree in Stages 1-9. This batch moved the following
 * further constructs onto real grammar rules, each keeping its legacy regex ONLY as a counted
 * fallback (see the per-site {@code *_REGEX_FALLBACKS} counters): trigger header
 * (triggerOnTable/triggerTiming/triggerEvent), NEXT VALUE FOR (nextValueFor), CTE names
 * (withStatement/cteDefinition), cursor lifecycle (cursorStatement), EXISTS-in-IF (existsPredicate),
 * IDENTITY columns (createTableStatement/identitySpec), CREATE SYNONYM in a routine body
 * (createSynonym), sp_executesql arguments (SP_EXECUTESQL token + spExecuteSqlArgs), and the
 * CREATE SEQUENCE / CREATE INDEX option tails (sequenceOption/indexOption).
 *
 * <p><b>Task #30 additions (batch 3):</b> qualified-call extraction now runs off a real
 * {@code callExpression} rule with {@code QUALIFIED_CALL_REGEX_FALLBACKS} counting the residual
 * regex scan; CASE statement-inventory counting now runs off {@code caseExpression} contexts with
 * {@code CASE_REGEX_FALLBACKS} counting the residual; and a standalone CREATE SYNONYM is a valid
 * {@code compilationUnit} alternative.
 *
 * <p><b>Deliberately deferred (still regex-primary, documented):</b> the otherStatement text
 * recovery for MERGE/WITH-led statements (KEYWORD_OP_PATTERN / DELETE_TARGET_FROM_PATTERN /
 * TABLE_REF_PATTERN / JOIN_PATTERN / FROM_PATTERN / ALIAS_TABLE_PATTERN), SUBQUERY
 * statement-inventory counting (subqueries are parsed by the nested {@code queryUnit} entry point,
 * not this tree), and the helpers SQL_LIKE_PATTERN / STATEMENT_HEAD / PERMISSION_STMT_PATTERN /
 * ARG_OUTPUT_TRAILING_PATTERN. Promoting MERGE/WITH to structural rules is the largest remaining
 * piece (effort L), left for a follow-up; the WITH side is partially covered by
 * withStatement/cteDefinition. The qualified-call and CASE regex scans are retained as counted
 * fallbacks precisely so calls/markers inside those deferred bodies are not lost.
 */
public class SqlServerAnalysisVisitor extends SqlServerParserBaseVisitor<Void> {

    // Bug (comment-between-schema-and-dot audit): a block comment (already blanked to spaces by
    // stripLineAndBlockComments, length-preserving) or plain whitespace between a qualifier and its
    // dot — e.g. "dbo/*comment*/.Orders" or "dbo /* x */ . Orders" — used to break the tight
    // ident.ident adjacency this pattern required, so only the qualifier ("dbo") ever matched and
    // the real table name after the dot was lost entirely. `\s*` around the dot tolerates it.
    // Bug (bracketed-identifier-with-space audit): `\[?[\w#$]+\]?` cannot represent a
    // bracket-quoted identifier containing a space ("[table name with spaces]") because `\w` does
    // not include space — the lexer's own BRACKET_ID rule allows any character except `]` inside
    // brackets. `\[[^\]]+\]|[\w#$]+` is a proper either/or: the WHOLE bracketed body (any
    // non-`]` run) when bracket-quoted, or the plain word-character run when not.
    private static final Pattern FROM_PATTERN = Pattern.compile(
            "(?i)\\bFROM\\s+(\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$]+)(?:\\s*\\.\\s*(\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$]+))?");

    // Captures every table referenced in a FROM or JOIN clause (join parity).
    // Bug (cross-db audit): a linked-server/cross-database reference can carry up to 4 dotted
    // parts (server.db.schema.table). The whole dotted name is captured as one group (1) so the
    // real table (the LAST segment) can be resolved in code via splitQualifiedName(), instead of
    // hard-capping at a single optional dot and mis-splitting a 3-/4-part name into a phantom
    // fragment (e.g. "OtherDb.dbo.Orders" used to yield schema=OTHERDB, name=DBO, losing Orders).
    // Bug (comment-between-schema-and-dot audit): `\s*` (not a bare `.`) around each dot tolerates
    // a comment (blanked to spaces) or plain whitespace between a qualifier and its dot — e.g.
    // "dbo/*comment*/.Orders" or "dbo /* x */ . Orders" — which previously desynced the tight
    // ident.ident adjacency this pattern required and caused only "dbo" (the qualifier) to be
    // captured as if it were the table, silently losing the real table name after the dot.
    // splitQualifiedName() trims each segment, since the captured dotted name can now itself
    // contain the whitespace that surrounded a masked-out comment.
    // Bug (bracketed-identifier-with-space audit): `\[?[\w#$@]+\]?` cannot represent a
    // bracket-quoted identifier containing a space ("[table name with spaces]") because `\w` does
    // not include space, even though the lexer's BRACKET_ID rule allows any non-`]` character
    // inside brackets — `FROM [table name with spaces]` used to capture only "table", losing the
    // rest of the identifier. `\[[^\]]+\]|[\w#$@]+` is a proper either/or: the WHOLE bracketed
    // body when bracket-quoted, or the plain word-character run when not.
    private static final Pattern TABLE_REF_PATTERN = Pattern.compile(
            "(?i)\\b(?:FROM|JOIN)\\s+((?:\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$@]+)(?:\\s*\\.\\s*(?:\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$]+)){0,3})"
          + "(?:\\s+(?:AS\\s+)?([A-Za-z_]\\w*))?");   // optional table alias, with or without AS

    // Schema-qualified function/proc call:  schema.name(   →  proc→function / →TVF edges.
    // Built-ins (SUM, COUNT, ISNULL, CAST…) are unqualified, so they are naturally excluded.
    private static final Pattern QUALIFIED_CALL_PATTERN = Pattern.compile(
            "\\b([\\w#$]+)\\.([\\w#$]+)\\s*\\(");

    // Bug 7: SQL Server's fixed set of built-in XML data-type methods (col.value/.exist/.query/.nodes).
    private static final Set<String> XML_METHOD_NAMES = Set.of("VALUE", "EXIST", "QUERY", "NODES");

    // REMOVED (batch 2 cleanup): QUALIFIED_TABLE_PATTERN was declared here but never referenced by
    // any matcher — dead code. The recovery it was intended for is actually done by
    // KEYWORD_OP_PATTERN / scanOps(). Deleting it removes one entry from the regex cluster without
    // changing any behaviour.

    // JOIN clause:  [<type>] JOIN | <type> APPLY  schema.table[(args)] [AS] alias [WITH (hint)] [ON <condition>]
    //
    // Bug (bare JOIN audit): the qualifier group used to be mandatory, so a plain "JOIN" — the
    // ANSI-standard synonym for "INNER JOIN" and arguably the most common join spelling in real
    // T-SQL — never matched this pattern at all and produced zero join edges. The qualifier is now
    // optional (defaulted to INNER in attachJoins) when followed by JOIN. APPLY has no bare form in
    // T-SQL (only CROSS APPLY / OUTER APPLY exist), so the qualifier stays effectively mandatory for
    // APPLY: attachJoins skips any match where op=="APPLY" and the qualifier group is absent.
    // Bug 4 (linked-server audit): the joined-table group used to cap at one optional dot, so a
    // 4-part linked-server name (server.database.schema.table) after JOIN broke the match before
    // the alias/ON/boundary groups could ever be reached, dropping the join edge entirely. The
    // whole dotted name is now captured as one group (like TABLE_REF_PATTERN) and resolved via
    // splitQualifiedName() in attachJoins() — the LAST segment is always the real table.
    // Bug (TVP-JOIN audit): the joined-table group's char class excluded `@`, so
    // `INNER JOIN @tvp t ON ...` (joining a table-valued parameter) never matched this pattern
    // at all — the whole join clause was silently dropped rather than recorded. `@` is now
    // accepted as the leading character of the first dotted segment so a TVP joined-table name
    // is captured (as the joined-table identifier) like any other table; the FROM-clause
    // base-table-op scan (a separate pattern/path) is untouched, so `@tvp`s still correctly stay
    // excluded from being mis-recorded as their own base table op.
    // Bug (bracketed-identifier-with-space audit): same either/or fix as TABLE_REF_PATTERN —
    // `\w` cannot represent a bracketed identifier containing a space.
    private static final Pattern JOIN_PATTERN = Pattern.compile(
            "(?i)\\b(?:(INNER|LEFT\\s+OUTER|LEFT|RIGHT\\s+OUTER|RIGHT|FULL\\s+OUTER|FULL|CROSS|OUTER)\\s+)?(JOIN|APPLY)\\s+"
          + "((?:\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$@]+)(?:\\.(?:\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$]+)){0,3})"        // up to 4-part dotted name (or @tvp)
          // Bug (TVF-APPLY audit): CROSS/OUTER APPLY of a parameterised table-valued function
          // (e.g. "CROSS APPLY dbo.GetOrderLines(o.id) ol") is followed immediately by the call's
          // argument list, which satisfied none of the alternatives that used to follow the
          // schema.table group, breaking the match before the alias/boundary could ever be reached.
          // Consume a (non-nested) argument list here so the alias/ON/boundary groups still see the
          // rest of the clause as before.
          + "(?:\\s*\\([^()]*\\))?"                                 // optional TVF call argument list
          // Bug 6 (XML CROSS APPLY audit): "CROSS APPLY xml_col.nodes(...) AS t(c)" has an alias
          // immediately followed (no whitespace) by a parenthesised column-list — the old alias
          // group only matched a bare identifier, so the whole match failed and the CROSS APPLY
          // was silently dropped. Accept and discard an optional "(col[,col...])" right after the
          // alias so the match (and the boundary lookahead beyond it) still succeeds.
          + "(?:\\s+(?:AS\\s+)?([A-Za-z]\\w*)(?:\\s*\\([^()]*\\))?)?"   // optional alias, optionally alias(col-list)
          // Bug (table-hint-between-alias-and-ON audit): a "WITH (NOLOCK)" / "WITH (INDEX(ix1))"
          // table hint clause commonly sits between the joined table's alias and its ON condition;
          // without consuming it here, the boundary lookahead below never matched and the whole
          // join clause (not just the hint) was silently dropped.
          + "(?:\\s+WITH\\s*\\([^)]*\\))?"                          // optional table hint, e.g. WITH (NOLOCK)
          // Bug 5 (chained-CTE audit): "." does not match a line terminator by default, so when
          // the ON condition is immediately followed by a newline then the boundary character
          // (e.g. a CTE body's closing ")" on its own line, common with formatted SQL), the lazy
          // dot-group could never expand far enough to reach it and the WHOLE join match failed —
          // not just this join, but silently, with no fallback. [\s\S] matches any character
          // (including newlines) so the lazy group can cross line boundaries to find its terminator.
          + "(?:\\s+ON\\s+([\\s\\S]{0,200}?))?"                     // optional ON condition (bounded to avoid backtracking)
          // Bug 2 (§DML-audit): the terminator lookahead previously required `\s+` before EVERY
          // alternative, including `$` — so the LAST join in a chain (nothing trailing but
          // end-of-text/end-of-statement, no whitespace) never satisfied the lookahead and was
          // silently dropped by find(), even though JOIN_PATTERN matched the first N-1 joins
          // fine. `;`/`)`/`$` no longer require a preceding `\s+` to be recognised as a boundary.
          // "JOIN" itself must also be a boundary now that a bare (unqualified) JOIN is legal —
          // otherwise the lazy ON-condition group in a JOIN...JOIN chain swallows the next bare
          // JOIN clause instead of stopping at it.
          + "(?=\\s+(?:INNER|LEFT|RIGHT|FULL|CROSS|OUTER|JOIN|WHERE|GROUP|ORDER|HAVING|UNION)|;|\\)|$)");

    // NEXT VALUE FOR schema.seq  →  sequence usage
    private static final Pattern SEQ_PATTERN = Pattern.compile(
            "(?i)\\bNEXT\\s+VALUE\\s+FOR\\s+\\[?([\\w#$]+)\\]?(?:\\.\\[?([\\w#$]+)\\]?)?");

    // CREATE TABLE [schema.]name ( ... )  →  used to locate a table's column-def body so
    // IDENTITY columns (audit #8) can be scanned within it, scoped to the right table.
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
            "(?i)\\bCREATE\\s+TABLE\\s+\\[?([\\w#$@]+)\\]?(?:\\.\\[?([\\w#$]+)\\]?)?\\s*\\(");

    // A column definition ending in IDENTITY[(seed, increment)], e.g. "Id INT IDENTITY(1,1)".
    // Bare IDENTITY (no parens) defaults to seed=1, increment=1 per SQL Server semantics.
    private static final Pattern IDENTITY_PATTERN = Pattern.compile(
            "(?i)\\[?([A-Za-z_]\\w*)\\]?\\s+[A-Za-z_]+\\w*(?:\\s*\\([^)]*\\))?\\s+IDENTITY\\s*"
          + "(?:\\(\\s*(-?\\d+)\\s*,\\s*(-?\\d+)\\s*\\))?");

    // Any DDL/DML keyword that precedes a table name → operation type. Multi-word
    // alternatives come first so "CREATE TABLE" wins over "CREATE", etc.
    private static final Pattern KEYWORD_OP_PATTERN = Pattern.compile(
            // Bug 3 (§DML-audit): `USING` added so MERGE's source table (`MERGE INTO t USING
            // src ...`) is captured as a read op — previously absent from this alternation, so
            // the MERGE's target was the only side ever recorded.
            "(?i)\\b(CREATE\\s+TABLE|ALTER\\s+TABLE|DROP\\s+TABLE|TRUNCATE\\s+TABLE|MERGE\\s+INTO|MERGE|"
          + "INSERT\\s+INTO|DELETE\\s+FROM|UPDATE|FROM|JOIN|USING|INTO)\\s+"
          + "\\[?([\\w#$]+)\\]?(?:\\.\\[?([\\w#$]+)\\]?)?"
          + "(?:\\s+(?:AS\\s+)?([A-Za-z_]\\w*))?");   // optional table alias, with or without AS

    // T-SQL's `DELETE <target> FROM <sources> …` form — the DELETE-side counterpart of the
    // `UPDATE <target> … FROM <sources> …` shape that scanOps already resolves via
    // aliasTargetInText. KEYWORD_OP_PATTERN deliberately only knows `DELETE FROM`, so a statement
    // routed through otherStatement's text recovery (anything beginning with WITH) recorded NO
    // delete op at all for this form: only the FROM/JOIN reads were picked up.
    //
    // This is a SEPARATE pattern rather than a widening of KEYWORD_OP_PATTERN to bare `DELETE`,
    // and that is the whole safety argument. A bare-`DELETE` alternation there would also match
    // MERGE's `WHEN NOT MATCHED … THEN DELETE`, whose next word is whatever follows the clause —
    // fabricating a delete op against a non-table. Here the match REQUIRES the full three-part
    // shape `DELETE <name> FROM`, with the FROM ADJACENT to the target name:
    //
    //   `DELETE t FROM dbo.Target t JOIN …`        → matches (target `t`, resolved via the alias)
    //   `DELETE dbo.Target FROM dbo.Target JOIN …` → matches (target already qualified)
    //   `WHEN NOT MATCHED BY SOURCE THEN DELETE;`  → no name, no FROM → no match
    //   `THEN DELETE OUTPUT deleted.* INTO @log`   → next word is OUTPUT, not FROM → no match
    //   `DELETE FROM dbo.T WHERE …`                → excluded by the (?!FROM\b) lookahead, so the
    //                                                existing KEYWORD_OP_PATTERN path still owns it
    private static final Pattern DELETE_TARGET_FROM_PATTERN = Pattern.compile(
            "(?i)\\bDELETE\\s+(?!FROM\\b)(\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$]+)"
          + "(?:\\s*\\.\\s*(\\[(?:[^\\]]|\\]\\])+\\]|[\\w#$]+))?\\s+FROM\\b");

    // Bug 8: first argument of `EXEC sp_executesql @sql, ...` — the @sql variable name.
    private static final Pattern SP_EXECUTESQL_FIRST_ARG_PATTERN = Pattern.compile("^\\s*(@\\w+)");

    // sp_executesql parameter-signature audit: the second argument, a quoted string literal
    // declaring the parameter list (e.g. N'@p1 INT'), immediately following the @sql argument's
    // trailing comma.
    private static final Pattern SP_EXECUTESQL_PARAM_SIG_PATTERN = Pattern.compile("^\\s*,\\s*(N?'[^']*')");

    // sp_executesql parameter-signature audit: bound name=value pairs following the signature,
    // e.g. "@p1=5, @p2='x'". Values run up to the next top-level comma or the end/`;`.
    private static final Pattern SP_EXECUTESQL_BOUND_PARAM_PATTERN =
            Pattern.compile("(?i)@(\\w+)\\s*=\\s*([^,;]+)");

    // CTE name definition:  WITH x AS (   or   ), y AS (
    private static final Pattern CTE_PATTERN = Pattern.compile(
            "(?i)(?:\\bWITH|,)\\s+([A-Za-z_]\\w*)\\s+AS\\s*\\(");

    // CASE expression (simple or searched) — the grammar consumes CASE...END inside
    // stmtTail as tokens, so there is no dedicated context; recover each from body text.
    private static final Pattern CASE_PATTERN = Pattern.compile("(?i)\\bCASE\\b");

    // Nested SELECT (subquery) in an expression / FROM / WHERE / IN — a parenthesised
    // SELECT. One match == one subquery.
    private static final Pattern SUBQUERY_PATTERN = Pattern.compile("(?i)\\(\\s*SELECT\\b");

    // Bug 5: a quoted RHS is only dynamic SQL if its content actually looks like a SQL
    // statement, not just any string literal (e.g. SET @v = 'plain string' is not dynamic SQL).
    private static final Pattern SQL_LIKE_PATTERN = Pattern.compile(
            "(?i)\\b(SELECT|INSERT|UPDATE|DELETE|EXEC|EXECUTE|CREATE|DROP|ALTER|MERGE|TRUNCATE|WITH)\\b");

    private static boolean looksLikeSql(String literalText) {
        return literalText != null && SQL_LIKE_PATTERN.matcher(literalText).find();
    }

    // De-dupe outbound calls (name keyed) so the same target isn't emitted repeatedly.
    private final Set<String> seenCalls = new HashSet<>();
    // De-dupe table ops (schema.name keyed).
    private final Set<String> seenTables = new HashSet<>();
    // Common-table-expression names — excluded from table extraction (they are not real tables).
    private final Set<String> cteNames = new HashSet<>();
    // Nested-transaction depth counter — reset at the start of each routine (see
    // addTransactionStatement()'s javadoc / SqlServerStatementInfo.transactionDepth for the
    // recording convention). Simple additive metadata only; no correlation/pairing objects.
    private int transactionDepth = 0;
    // Bug 6: DECLARE'd cursor names — excluded from table extraction the same way CTE names are,
    // so "FETCH NEXT FROM cur" is not mis-scanned as a FROM-clause table reference.
    private final Set<String> cursorNames = new HashSet<>();
    // Bug (trigger pseudo-table audit): `inserted`/`deleted` are SQL Server's in-memory trigger
    // delta pseudo-tables — they only exist inside a trigger body, and are never real base tables
    // there. Scoped (not blanket) exclusion: true only while visiting a TRIGGER's body, so a real
    // user table elsewhere in the schema literally named `inserted`/`deleted` (outside a trigger
    // context) is unaffected. Set true on entry to visitTriggerDefinition's body walk, restored
    // to its prior value (false, since triggers don't nest) on exit.
    private boolean insideTriggerBody = false;
    // Scalar/TVP parameter names (from the routine's own param list) — a `FROM @tvp` referencing
    // one of these is the parameter itself (e.g. a table-valued parameter), already recorded as a
    // param, not a fresh table op. Table VARIABLEs (DECLARE @tbl TABLE(...)) are NOT added here,
    // so they remain eligible to be captured as real table ops when used in a JOIN/FROM.
    private final Set<String> paramNames = new HashSet<>();
    // Bug 8: variable name -> last-known literal SQL text seen via a `DECLARE @sql = '...'` or
    // `SET @sql = '...'` literal assignment in this routine — lets a later `EXEC(@sql)` resolve
    // its own tables and recognise, when no such assignment exists, that it is genuinely unresolved.
    private final java.util.Map<String, String> sqlVarLiteralMap = new java.util.HashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    // Regex-retirement counters (regex → grammar migration, batch 2).
    //
    // Mirrors the Batch-1 pattern in PlSqlAnalysisVisitor.DYN_SQL_REGEX_FALLBACKS: each construct
    // that has been migrated onto a real parse-tree walk keeps its legacy regex ONLY as a
    // documented fallback, and every time that fallback actually runs the corresponding counter is
    // incremented (and the first use per site is logged once). The counters make the retirement
    // rate observable: on a corpus the structured rules handle, they stay flat; a rising counter
    // names exactly which construct still needs grammar work.
    // ─────────────────────────────────────────────────────────────────────────

    /** TRIGGER header (ON table / timing / events) — structured by triggerOnTable/triggerTiming/triggerEvent. */
    public static final java.util.concurrent.atomic.AtomicLong TRIGGER_HEADER_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** NEXT VALUE FOR sequence usage — structured by nextValueFor. */
    public static final java.util.concurrent.atomic.AtomicLong SEQUENCE_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** CREATE SYNONYM — structured by createSynonym. */
    public static final java.util.concurrent.atomic.AtomicLong SYNONYM_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** Cursor lifecycle (OPEN/FETCH/CLOSE/DEALLOCATE) — structured by cursorStatement. */
    public static final java.util.concurrent.atomic.AtomicLong CURSOR_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** EXISTS(subquery) in an IF condition — structured by existsPredicate. */
    public static final java.util.concurrent.atomic.AtomicLong EXISTS_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** CTE names — structured by cteDefinition. */
    public static final java.util.concurrent.atomic.AtomicLong CTE_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** IDENTITY columns — structured by createTableStatement/identitySpec. */
    public static final java.util.concurrent.atomic.AtomicLong IDENTITY_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** sp_executesql argument decode — structured by spExecuteSql. */
    public static final java.util.concurrent.atomic.AtomicLong SP_EXECUTESQL_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** CREATE SEQUENCE / CREATE INDEX option tails — structured by sequenceOption/indexOption. */
    public static final java.util.concurrent.atomic.AtomicLong CREATE_OPTION_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** CASE statement-inventory markers — structured by caseExpression (task #30). */
    public static final java.util.concurrent.atomic.AtomicLong CASE_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    /** Schema-qualified call extraction — structured by callExpression (task #30). */
    public static final java.util.concurrent.atomic.AtomicLong QUALIFIED_CALL_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();

    private static final Set<String> FALLBACK_LOGGED = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Record one use of a legacy regex fallback for a migrated construct; logs the first use per site. */
    private static void countRegexFallback(java.util.concurrent.atomic.AtomicLong counter, String site) {
        long n = counter.incrementAndGet();
        if (FALLBACK_LOGGED.add(site)) {
            System.err.println("[SqlServerAnalysisVisitor] " + site
                    + ": structured parse produced nothing; using legacy regex fallback (fallback #" + n + ")");
        }
    }

    private final SqlServerRoutineInfo result;

    public SqlServerAnalysisVisitor(String objectName, String schema) {
        this.result = new SqlServerRoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
        result.setType(SqlServerRoutineInfo.RoutineType.PROCEDURE);
    }

    public SqlServerRoutineInfo getResult() {
        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Routine identity
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitRoutineDefinition(SqlServerParser.RoutineDefinitionContext ctx) {
        transactionDepth = 0;
        // Routine type
        String kw = ctx.routineKw().getText().toUpperCase();
        result.setType(kw.startsWith("FUNC")
                ? SqlServerRoutineInfo.RoutineType.FUNCTION
                : SqlServerRoutineInfo.RoutineType.PROCEDURE);

        // Routine name: routineName is (namePart (DOT namePart)?)
        SqlServerParser.RoutineNameContext rn = ctx.routineName();
        List<SqlServerParser.NamePartContext> parts = rn.namePart();
        if (parts.size() == 2) {
            // schema.name
            result.setSchema(stripBrackets(parts.get(0).getText()));
            result.setName(stripBrackets(parts.get(1).getText()));
        } else if (parts.size() == 1) {
            result.setName(stripBrackets(parts.get(0).getText()));
        }

        // Line range
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        // RETURNS clause (tasks #26 + #27). Grammar: `RETURNS AT_VAR? dataType`; the permissive
        // dataType swallows a declared table shape — `RETURNS @result TABLE(id INT, name …)` — via
        // its parenGroup, so a multi-statement TVF's column list is available here. An inline TVF
        // (`RETURNS TABLE AS RETURN (SELECT …)`) declares no list: returnType is "TABLE" and
        // returnsColumns stays empty rather than guessing the projection.
        SqlServerParser.ReturnsClauseContext rc = ctx.returnsClause();
        if (rc != null && rc.dataType() != null) {
            result.setReturnType(originalText(rc.dataType()));
            if (rc.dataType().parenGroup() != null) {
                for (String col : extractDeclaredColumns(originalText(rc.dataType().parenGroup()))) {
                    result.getReturnsColumns().add(col);
                }
            }
        }

        collectCteNames(ctx.routineBody());
        Void r = visitChildren(ctx);
        extractQualifiedCalls(ctx.routineBody());
        extractSequences(ctx.routineBody());   // proc→function / function→function / →TVF
        extractCaseAndSubqueries(ctx.routineBody());
        return r;
    }

    @Override
    public Void visitTriggerDefinition(SqlServerParser.TriggerDefinitionContext ctx) {
        transactionDepth = 0;
        result.setType(SqlServerRoutineInfo.RoutineType.TRIGGER);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        applyTriggerHeader(ctx.triggerHeader());
        collectCteNames(ctx.routineBody());
        insideTriggerBody = true;
        Void r;
        try {
            r = visitChildren(ctx);   // walk body → DML table ops, calls, dynamic SQL
        } finally {
            insideTriggerBody = false;
        }
        extractQualifiedCalls(ctx.routineBody());
        extractSequences(ctx.routineBody());
        extractCaseAndSubqueries(ctx.routineBody());
        return r;
    }

    /**
     * Decode the trigger header — target table, timing and event list.
     *
     * <p>Grammar-first (batch 2): the header rule now carries real {@code triggerOnTable} /
     * {@code triggerTiming} / {@code triggerEvent} sub-nodes, so the three values are read off the
     * parse tree with no text scanning at all. Only when the header matched the permissive
     * {@code ~AS+} fallback alternative (no structured children) do the legacy regexes run, and
     * that use is counted in {@link #TRIGGER_HEADER_REGEX_FALLBACKS}.
     */
    private void applyTriggerHeader(SqlServerParser.TriggerHeaderContext ctx) {
        if (ctx == null) return;
        SqlServerParser.TriggerOnTableContext on = ctx.triggerOnTable();
        if (on != null && on.tableRef() != null) {
            result.setTriggerTable(extractTableName(on.tableRef()));
            SqlServerParser.TriggerTimingContext tm = ctx.triggerTiming();
            if (tm != null) {
                result.setTriggerTiming(originalText(tm).replaceAll("\\s+", " ").trim().toUpperCase());
            }
            StringBuilder events = new StringBuilder();
            for (SqlServerParser.TriggerEventContext ev : ctx.triggerEvent()) {
                if (events.length() > 0) events.append(",");
                events.append(ev.getText().toUpperCase());
            }
            if (events.length() > 0) result.setTriggerEvent(events.toString());
            return;
        }
        // Fallback: the permissive header alternative matched. Legacy regexes, counted.
        String header = originalText(ctx);
        if (header == null) return;
        countRegexFallback(TRIGGER_HEADER_REGEX_FALLBACKS, "trigger header");
        Matcher t = Pattern.compile("(?i)\\bON\\s+\\[?([\\w#$]+)\\]?(?:\\.\\[?([\\w#$]+)\\]?)?").matcher(header);
        if (t.find()) result.setTriggerTable(t.group(2) != null ? t.group(2) : t.group(1));
        Matcher tm = Pattern.compile("(?i)\\b(INSTEAD\\s+OF|AFTER|BEFORE|FOR)\\b").matcher(header);
        if (tm.find()) result.setTriggerTiming(tm.group(1).replaceAll("\\s+", " ").toUpperCase());
        Matcher ev = Pattern.compile("(?i)\\b(INSERT|UPDATE|DELETE)\\b").matcher(header);
        StringBuilder events = new StringBuilder();
        while (ev.find()) { if (events.length() > 0) events.append(","); events.append(ev.group(1).toUpperCase()); }
        if (events.length() > 0) result.setTriggerEvent(events.toString());
    }

    @Override
    public Void visitViewDefinition(SqlServerParser.ViewDefinitionContext ctx) {
        transactionDepth = 0;
        result.setType(SqlServerRoutineInfo.RoutineType.VIEW);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        result.setViewQuery(originalText(ctx.routineBody()));   // the defining SELECT
        collectCteNames(ctx.routineBody());
        Void r = visitChildren(ctx);   // walk SELECT body → source tables
        extractQualifiedCalls(ctx.routineBody());
        extractSequences(ctx.routineBody());
        extractCaseAndSubqueries(ctx.routineBody());
        return r;
    }

    /**
     * CREATE SEQUENCE [schema.]name [AS type] [START WITH n] [INCREMENT BY n]
     *                 [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE]
     *                 [CYCLE | NO CYCLE] [CACHE [n] | NO CACHE]
     *
     * The grammar consumes the option list permissively (createSequenceTail), so the individual
     * options are recovered here by regex over the original tail text — the same technique
     * visitTriggerDefinition uses for the trigger header. The sequence is also recorded in the
     * shared sequence inventory so downstream consumers see the *definition* alongside the
     * NEXT-VALUE-FOR *usages* that extractSequences() already collects.
     */
    @Override
    public Void visitCreateSequence(SqlServerParser.CreateSequenceContext ctx) {
        transactionDepth = 0;
        result.setType(SqlServerRoutineInfo.RoutineType.SEQUENCE);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        // Grammar-first (batch 2): each option is a named sub-rule; read the values off the tree.
        // A createSequenceTail alternative means an option shape the named rules do not model —
        // only then does the legacy regex run, on that tail, and that use is counted.
        StringBuilder fallbackTail = new StringBuilder();
        for (SqlServerParser.SequenceOptionContext opt : ctx.sequenceOption()) {
            if (opt.AS() != null && opt.dataType() != null) {
                result.setSequenceDataType(stripBrackets(opt.dataType().getText().replaceAll("\\s+", "")));
            } else if (opt.startWithOption() != null) {
                result.setSequenceStartWith(signedNum(opt.startWithOption()));
            } else if (opt.incrementByOption() != null) {
                result.setSequenceIncrementBy(signedNum(opt.incrementByOption()));
            } else if (opt.negatedOption() != null) {
                String name = opt.negatedOption().optionNameKw() != null
                        ? opt.negatedOption().optionNameKw().getText().toUpperCase() : "";
                if ("CYCLE".equals(name)) result.setSequenceCycle(false);
                else if ("CACHE".equals(name)) result.setSequenceCache(false);
                // NO MINVALUE / NO MAXVALUE leave the value unset — matching the legacy regex,
                // which only set a min/max when a numeric value followed.
            } else if (opt.minValueOption() != null) {
                SqlServerParser.MinValueOptionContext o = opt.minValueOption();
                if (o.INT_NUM() != null) result.setSequenceMinValue(Long.valueOf(o.INT_NUM().getText()));
            } else if (opt.maxValueOption() != null) {
                SqlServerParser.MaxValueOptionContext o = opt.maxValueOption();
                if (o.INT_NUM() != null) result.setSequenceMaxValue(Long.valueOf(o.INT_NUM().getText()));
            } else if (opt.cycleOption() != null) {
                result.setSequenceCycle(true);
            } else if (opt.cacheOption() != null) {
                SqlServerParser.CacheOptionContext o = opt.cacheOption();
                result.setSequenceCache(true);
                if (o.INT_NUM() != null) result.setSequenceCacheSize(Long.valueOf(o.INT_NUM().getText()));
            } else if (opt.createSequenceTail() != null) {
                fallbackTail.append(originalText(opt.createSequenceTail()));
            }
        }
        String tail = fallbackTail.length() > 0 ? fallbackTail.toString() : null;
        if (tail != null) {
            countRegexFallback(CREATE_OPTION_REGEX_FALLBACKS, "CREATE SEQUENCE options");
            Matcher m;
            m = Pattern.compile("(?i)^\\s*AS\\s+([\\w\\[\\]]+(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?)").matcher(tail);
            if (m.find()) result.setSequenceDataType(stripBrackets(m.group(1).replaceAll("\\s+", "")));

            m = Pattern.compile("(?i)\\bSTART\\s+WITH\\s+(-?\\d+)").matcher(tail);
            if (m.find()) result.setSequenceStartWith(Long.valueOf(m.group(1)));

            m = Pattern.compile("(?i)\\bINCREMENT\\s+BY\\s+(-?\\d+)").matcher(tail);
            if (m.find()) result.setSequenceIncrementBy(Long.valueOf(m.group(1)));

            m = Pattern.compile("(?i)\\b(NO\\s+)?MINVALUE(?:\\s+(-?\\d+))?").matcher(tail);
            if (m.find() && m.group(1) == null && m.group(2) != null) result.setSequenceMinValue(Long.valueOf(m.group(2)));

            m = Pattern.compile("(?i)\\b(NO\\s+)?MAXVALUE(?:\\s+(-?\\d+))?").matcher(tail);
            if (m.find() && m.group(1) == null && m.group(2) != null) result.setSequenceMaxValue(Long.valueOf(m.group(2)));

            m = Pattern.compile("(?i)\\b(NO\\s+)?CYCLE\\b").matcher(tail);
            if (m.find()) result.setSequenceCycle(m.group(1) == null);

            m = Pattern.compile("(?i)\\b(NO\\s+)?CACHE\\b(?:\\s+(\\d+))?").matcher(tail);
            if (m.find()) {
                result.setSequenceCache(m.group(1) == null);
                if (m.group(1) == null && m.group(2) != null) result.setSequenceCacheSize(Long.valueOf(m.group(2)));
            }
        }

        SqlServerSequenceInfo seq = new SqlServerSequenceInfo();
        seq.setName(result.getName());
        seq.setSchema(result.getSchema());
        seq.setLine(ctx.getStart().getLine());
        seq.setSeed(result.getSequenceStartWith());
        seq.setIncrement(result.getSequenceIncrementBy());
        result.getSequences().add(seq);
        return null;
    }

    /**
     * CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] INDEX name ON table (cols) [INCLUDE (cols)] [WHERE ...]
     *
     * The key-column list is structurally parsed (indexColumnList); UNIQUE/CLUSTERED/NONCLUSTERED
     * are matched positionally as indexModifier IDs; and the optional INCLUDE / WHERE tail is
     * consumed permissively by createIndexTail and decoded here by regex.
     */
    @Override
    public Void visitCreateIndex(SqlServerParser.CreateIndexContext ctx) {
        transactionDepth = 0;
        result.setType(SqlServerRoutineInfo.RoutineType.INDEX);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        for (SqlServerParser.IndexModifierContext mod : ctx.indexModifier()) {
            String kw = mod.getText().toUpperCase();
            if ("UNIQUE".equals(kw)) result.setIndexUnique(true);
            else if ("CLUSTERED".equals(kw) || "NONCLUSTERED".equals(kw)) result.setIndexClustering(kw);
        }

        // DEFENSIVE (crash audit): tableRef()/indexColumnList() are mandatory in the grammar, so
        // they are only ever null when ANTLR error recovery hands us a partial CreateIndexContext
        // for input that was mis-routed here (this is exactly how `CREATE TYPE ...` used to reach
        // this method and NPE before createType existed). Skip gracefully instead of throwing —
        // the parse errors already recorded by the error listener describe the real problem.
        if (ctx.tableRef() == null || ctx.indexColumnList() == null) {
            return null;
        }

        result.setIndexTable(stripBrackets(ctx.tableRef().getText()));

        for (SqlServerParser.IndexColumnContext col : ctx.indexColumnList().indexColumn()) {
            result.getIndexColumns().add(stripBrackets(
                    col.ID() != null ? col.ID().getText() : col.BRACKET_ID().getText()));
        }

        // Grammar-first (batch 2): INCLUDE and WHERE are named option nodes. A createIndexTail
        // option means an unmodelled tail shape is present — only then does the legacy regex run,
        // and that use is counted.
        StringBuilder fallbackTail = new StringBuilder();
        for (SqlServerParser.IndexOptionContext opt : ctx.indexOption()) {
            if (opt.includeOption() != null) {
                for (SqlServerParser.IndexColumnContext col : opt.includeOption().indexColumnList().indexColumn()) {
                    String t = stripBrackets(col.ID() != null ? col.ID().getText() : col.BRACKET_ID().getText());
                    if (!t.isEmpty()) result.getIndexIncludeColumns().add(t);
                }
            } else if (opt.whereOption() != null && opt.whereOption().expr() != null) {
                String pred = originalText(opt.whereOption().expr()).trim();
                if (!pred.isEmpty()) result.setIndexFilter(pred);
            } else if (opt.createIndexTail() != null) {
                fallbackTail.append(originalText(opt.createIndexTail()));
            }
        }
        String tail = fallbackTail.length() > 0 ? fallbackTail.toString() : null;
        if (tail != null) {
            countRegexFallback(CREATE_OPTION_REGEX_FALLBACKS, "CREATE INDEX options");
            Matcher inc = Pattern.compile("(?is)\\bINCLUDE\\s*\\(([^)]*)\\)").matcher(tail);
            if (inc.find()) {
                for (String c : inc.group(1).split(",")) {
                    String t = stripBrackets(c.trim());
                    if (!t.isEmpty()) result.getIndexIncludeColumns().add(t);
                }
            }
            Matcher w = Pattern.compile("(?is)\\bWHERE\\b(.*?)(?=\\bWITH\\s*\\(|$)").matcher(tail);
            if (w.find()) {
                String pred = w.group(1).trim();
                if (!pred.isEmpty()) result.setIndexFilter(pred);
            }
        }
        return null;
    }

    /**
     * CREATE TYPE [schema.]name FROM &lt;base type&gt; [NOT NULL | NULL]   -- alias (UDT) type
     * CREATE TYPE [schema.]name AS TABLE ( &lt;column defs&gt; )           -- table type (TVP)
     *
     * Before createType existed, either form was greedily absorbed by createIndex and crashed
     * visitCreateIndex with an NPE on a null tableRef(). Both forms are now routed here.
     *
     * The alias form's base data type is parsed structurally (dataType); the table form's column
     * list is consumed as a balanced parenGroup and its column NAMES are recovered by scanning
     * the top-level, comma-separated entries — the same permissive decode used for
     * createSequenceTail / createIndexTail, since a full column-definition grammar would add no
     * extraction value here.
     */
    @Override
    public Void visitCreateType(SqlServerParser.CreateTypeContext ctx) {
        transactionDepth = 0;
        result.setType(SqlServerRoutineInfo.RoutineType.TYPE);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        if (ctx.FROM() != null) {
            result.setTypeForm("ALIAS");
            if (ctx.dataType() != null) {
                result.setTypeBaseType(stripBrackets(ctx.dataType().getText()));
            }
            String tail = ctx.createTypeTail() != null ? originalText(ctx.createTypeTail()) : null;
            if (tail != null) {
                if (Pattern.compile("(?i)\\bNOT\\s+NULL\\b").matcher(tail).find())      result.setTypeNotNull(Boolean.TRUE);
                else if (Pattern.compile("(?i)\\bNULL\\b").matcher(tail).find())        result.setTypeNotNull(Boolean.FALSE);
            }
        } else {
            result.setTypeForm("TABLE");
            if (ctx.parenGroup() != null) {
                String cols = originalText(ctx.parenGroup());
                if (cols != null && cols.length() >= 2) {
                    cols = cols.substring(1, cols.length() - 1);   // strip outer ( )
                    for (String entry : splitTopLevelCommas(cols)) {
                        Matcher nm = Pattern.compile("^\\s*(\\[[^\\]]*\\]|[\\w#$]+)").matcher(entry);
                        if (!nm.find()) continue;
                        String colName = stripBrackets(nm.group(1));
                        // Table-level constraint clauses are not columns.
                        String up = colName.toUpperCase();
                        if ("PRIMARY".equals(up) || "UNIQUE".equals(up) || "CHECK".equals(up)
                                || "CONSTRAINT".equals(up) || "INDEX".equals(up) || "FOREIGN".equals(up)) {
                            continue;
                        }
                        if (!colName.isEmpty()) result.getTypeColumns().add(colName);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Standalone (top-level) ALTER TABLE.
     *
     * Previously there was no top-level alterTable alternative at all: the whole statement fell
     * through compilationUnit and was only half-recovered by the KEYWORD_OP_PATTERN text scan,
     * which captures the name after ALTER TABLE but has no notion of REFERENCES — so a foreign
     * key's referenced table was never recorded and the ALTER's most valuable lineage (table A
     * now depends on table B) was silently lost.
     *
     * The altered table is always recorded as an ALTER op. For the structurally-parsed
     * ADD ... FOREIGN KEY (...) REFERENCES t (...) form the referenced table is additionally
     * recorded as a SELECT op — a read-through dependency, the same shape extractSynonym uses
     * for a synonym's target, so the existing lineage pipeline picks it up with no new concept.
     */
    @Override
    public Void visitAlterTable(SqlServerParser.AlterTableContext ctx) {
        transactionDepth = 0;
        int line = ctx.getStart().getLine();
        result.setLineStart(line);
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : line);

        String alteredTable  = null;
        String alteredSchema = null;
        if (ctx.tableRef() != null) {
            String altered = ctx.tableRef().getText();
            result.setAlterTableName(stripBrackets(altered));
            String[] parts = splitQualifiedName(altered);
            alteredSchema = parts.length >= 2 ? parts[parts.length - 2] : null;
            alteredTable  = parts[parts.length - 1];
            addTableOp(alteredSchema, alteredTable, line, SqlServerTableOpInfo.DmlType.ALTER);
        }

        SqlServerParser.AlterTableClauseContext clause = ctx.alterTableClause();
        if (clause == null) return null;

        SqlServerParser.AddForeignKeyContext fk = clause.addForeignKey();
        if (fk != null) {
            result.setAlterAction(fk.ID().getText().toUpperCase());           // ADD

            // One ADD list may declare several constraints. Each is recorded as its own
            // SqlServerForeignKeyInfo — the single-valued fields this replaced could only keep
            // one, so every constraint after the first was silently dropped.
            for (SqlServerParser.ForeignKeyConstraintContext c : fk.foreignKeyConstraint()) {
                recordForeignKey(c, alteredTable, alteredSchema, line);
            }
            // alterConstraintName stays single-valued (it describes the statement, not the
            // constraint set); the first named constraint is the one it reports, as before.
            return null;
        }

        // Every other sub-form (ADD column, ADD PRIMARY KEY/UNIQUE/CHECK, DROP COLUMN,
        // DROP CONSTRAINT, ALTER COLUMN, ...) is consumed permissively; record the leading
        // action word and the constraint name when one is present.
        String tail = ctx.alterTableClause().alterTableTail() != null
                ? originalText(ctx.alterTableClause().alterTableTail()) : null;
        if (tail != null) {
            Matcher a = Pattern.compile("(?i)^\\s*(ADD|DROP|ALTER|WITH|ENABLE|DISABLE|REBUILD|SWITCH|SET)\\b").matcher(tail);
            if (a.find()) result.setAlterAction(a.group(1).toUpperCase());
            Matcher c = Pattern.compile("(?i)\\bCONSTRAINT\\s+(\\[[^\\]]*\\]|[\\w#$]+)").matcher(tail);
            if (c.find()) result.setAlterConstraintName(stripBrackets(c.group(1)));
        }
        return null;
    }

    /**
     * One {@code [CONSTRAINT x] FOREIGN KEY (cols) REFERENCES t (cols)} constraint.
     *
     * <p>Records the referenced table as a SELECT op (a read-through dependency, the same shape
     * extractSynonym uses, so the existing lineage pipeline needs no new concept) AND the
     * structured constraint itself, which is what the multidb bridge carries downstream.
     */
    private void recordForeignKey(SqlServerParser.ForeignKeyConstraintContext ctx,
                                  String alteredTable, String alteredSchema, int line) {
        if (ctx == null || ctx.tableRef() == null) return;

        String constraintName = ctx.namePart() != null ? stripBrackets(ctx.namePart().getText()) : null;
        if (constraintName != null && result.getAlterConstraintName() == null) {
            result.setAlterConstraintName(constraintName);
        }

        String ref = ctx.tableRef().getText();
        String[] rp = splitQualifiedName(ref);
        String refSchema = rp.length >= 2 ? rp[rp.length - 2] : null;
        String refTable  = rp[rp.length - 1];
        addTableOp(refSchema, refTable, line, SqlServerTableOpInfo.DmlType.SELECT);

        SqlServerForeignKeyInfo fk = new SqlServerForeignKeyInfo();
        fk.setConstraintName(constraintName);
        fk.setTable(alteredTable);
        fk.setSchema(alteredSchema);
        fk.setReferencedTable(refTable);
        fk.setReferencedSchema(refSchema);

        List<SqlServerParser.ColumnNameListContext> lists = ctx.columnNameList();
        if (!lists.isEmpty())  collectColumnNames(lists.get(0), fk.getColumns());
        if (lists.size() > 1)  collectColumnNames(lists.get(1), fk.getReferencedColumns());

        fk.setLine(ctx.getStart().getLine());
        result.getForeignKeys().add(fk);
    }

    private void collectColumnNames(SqlServerParser.ColumnNameListContext ctx, List<String> out) {
        if (ctx == null) return;
        for (SqlServerParser.NamePartContext np : ctx.namePart()) {
            out.add(stripBrackets(np.getText()));
        }
    }

    /** Split on commas that are not nested inside parentheses (e.g. DECIMAL(18,2)). */
    private static List<String> splitTopLevelCommas(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                out.add(s.substring(start, i));
                start = i + 1;
            }
        }
        out.add(s.substring(start));
        return out;
    }

    /**
     * Recover CASE expressions and nested SELECT subqueries for the statement inventory.
     *
     * <p><b>CASE — grammar-first (task #30).</b> The {@code caseExpression} rule now gives every
     * occurrence a real context, so the markers are read off the tree (one per occurrence, nested
     * CASEs included as distinct child contexts). The legacy whole-body regex survives only for a
     * body where the structured walk found none, and that use is counted in
     * {@link #CASE_REGEX_FALLBACKS}.
     *
     * <p><b>SUBQUERY — still regex-primary.</b> Subqueries are parsed by the separate nested
     * {@code queryUnit} entry point, not as nodes of this tree, so there is no context to count yet;
     * the scan is unchanged. Each match uses the common vocabulary tokens "CASE" / "SUBQUERY" so
     * all dialects report uniformly.
     */
    private void extractCaseAndSubqueries(ParserRuleContext bodyCtx) {
        if (bodyCtx == null) return;
        String text = originalText(bodyCtx);
        if (text == null || text.isEmpty()) return;
        int baseLine = bodyCtx.getStart().getLine();

        List<SqlServerParser.CaseExpressionContext> cases = new ArrayList<>();
        collectNodes(bodyCtx, SqlServerParser.CaseExpressionContext.class, cases);
        if (!cases.isEmpty()) {
            for (SqlServerParser.CaseExpressionContext c : cases) {
                result.getStatements().add(
                        new SqlServerStatementInfo("CASE", c.getStart().getLine()));
            }
        } else {
            countRegexFallback(CASE_REGEX_FALLBACKS, "CASE");
            Matcher c = CASE_PATTERN.matcher(text);
            while (c.find()) {
                result.getStatements().add(
                        new SqlServerStatementInfo("CASE", lineAt(text, c.start(), baseLine)));
            }
        }

        Matcher s = SUBQUERY_PATTERN.matcher(text);
        while (s.find()) {
            result.getStatements().add(
                    new SqlServerStatementInfo("SUBQUERY", lineAt(text, s.start(), baseLine)));
        }
    }

    private void applyRoutineName(SqlServerParser.RoutineNameContext rn) {
        if (rn == null) return;
        List<SqlServerParser.NamePartContext> parts = rn.namePart();
        if (parts.size() == 2) {
            result.setSchema(stripBrackets(parts.get(0).getText()));
            result.setName(stripBrackets(parts.get(1).getText()));
        } else if (parts.size() == 1) {
            result.setName(stripBrackets(parts.get(0).getText()));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parameters
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitParam(SqlServerParser.ParamContext ctx) {
        SqlServerParamInfo p = new SqlServerParamInfo();
        p.setName(ctx.AT_VAR().getText());
        p.setDataType(ctx.dataType().getText());
        boolean isOutput = false;
        for (SqlServerParser.ParamModifierContext mod : ctx.paramModifier()) {
            if (mod.OUTPUT() != null || mod.OUT() != null) { isOutput = true; break; }
        }
        p.setDirection(isOutput
                ? SqlServerParamInfo.Direction.OUT
                : SqlServerParamInfo.Direction.IN);
        // Bug 9: the grammar's `param` rule already parses "(EQ defaultVal)?" fully — just read it.
        if (ctx.defaultVal() != null) {
            p.setDefaultValue(ctx.defaultVal().getText());
        }
        p.setLine(ctx.getStart().getLine());
        result.getParams().add(p);
        paramNames.add(p.getName().toUpperCase());
        return null; // don't recurse — no nested params
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE @variable
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareStatement(SqlServerParser.DeclareStatementContext ctx) {
        // DECLARE @a INT, @b DECIMAL(18,2) ... — one variable per declareItem.
        for (SqlServerParser.DeclareItemContext item : ctx.declareItem()) {
            SqlServerVariableInfo v = new SqlServerVariableInfo();
            v.setName(item.AT_VAR().getText());
            v.setDataType(item.dataType() != null ? item.dataType().getText() : null);
            v.setLine(item.getStart().getLine());
            result.getVariables().add(v);

            // DECLARE @sql ... = N'...SELECT...FROM...' — dynamic SQL built in an initializer.
            if (item.expr() != null) {
                String et = item.expr().getText();
                if ((et.startsWith("'") || et.startsWith("N'") || et.startsWith("n'")) && looksLikeSql(et)) {
                    int line = item.getStart().getLine();
                    String orig = originalText(item.expr());
                    SqlServerDynamicSqlInfo d = new SqlServerDynamicSqlInfo();
                    d.setKind(SqlServerDynamicSqlInfo.Kind.SET_SQL);
                    d.setSqlSource(orig);
                    d.setLine(line);
                    result.getDynamicSql().add(d);
                    scanDynamicSqlLiteralForTables(orig, line);
                    // Bug 8: remember this variable's literal SQL text so a later EXEC(@var) /
                    // EXEC sp_executesql @var can resolve it instead of being flagged unresolved.
                    sqlVarLiteralMap.put(item.AT_VAR().getText(), orig);
                }
            }
        }
        // Cursor form: DECLARE <name> CURSOR ... FOR <select>
        if (ctx.declareItem().isEmpty() && ctx.ID() != null) {
            String tail = originalText(ctx.stmtTail());
            if (tail != null && tail.toUpperCase().contains("CURSOR")) {
                SqlServerCursorInfo cur = new SqlServerCursorInfo();
                cur.setName(ctx.ID().getText());
                cursorNames.add(cur.getName().toUpperCase()); // Bug 6: exclude from table-ref scans
                String query = tail.trim();
                // Bug 7 fix (extended for Bug 5): stmtTail() stops right before the SELECT/EXEC
                // keyword (both are tokens stmtToken excludes so the next real statement can
                // start), so `tail` only ever spans "CURSOR FOR" — the cursor's own query body
                // (a static SELECT, or a dynamic-SQL EXEC(@sql)/EXEC('literal')) is parsed as a
                // separate sibling statement immediately after this one. Find that sibling and
                // append its own original text so getQuery() returns the full
                // "CURSOR FOR SELECT ..." / "CURSOR FOR EXEC(@sql)" text. Table capture itself is
                // unaffected — it is driven independently from visitSelectStatement /
                // visitExecStatement — this only fixes the reported query/text field.
                SqlServerParser.StatementContext sib = findNextSiblingStatement(ctx);
                if (sib != null) {
                    ParserRuleContext body = sib.selectStatement() != null
                            ? sib.selectStatement()
                            : sib.execStatement();
                    if (body != null) {
                        String bodyText = originalText(body);
                        if (bodyText != null) {
                            query = query + " " + bodyText.trim();
                        }
                    }
                }
                cur.setQuery(query);
                cur.setLine(ctx.getStart().getLine());
                // DECLARE itself is not added to stmtOperations: the downstream reader
                // (NodeScanFacetReader) already synthesizes a base "DECLARE" operation for every
                // cursor from its declaration; stmtOperations carries only the post-DECLARE
                // lifecycle (OPEN/FETCH/CLOSE/DEALLOCATE), matching the Postgres dialect's convention.
                result.getCursors().add(cur);
            }
        }
        return null; // no nested declares
    }

    /**
     * Finds the sibling {@code statement} immediately following {@code declareCtx}'s own wrapping
     * {@code statement}, inside the same statement list ({@code routineBody} or
     * {@code beginEndBlock}). Used to recover the query/body of a
     * {@code DECLARE cur CURSOR FOR SELECT ...} / {@code DECLARE cur CURSOR FOR EXEC(@sql)} whose
     * {@code stmtTail()} stops before the body (Bug 7 / Bug 5).
     */
    private SqlServerParser.StatementContext findNextSiblingStatement(
            SqlServerParser.DeclareStatementContext declareCtx) {
        ParserRuleContext stmtCtx = declareCtx.getParent(); // the wrapping "statement" node
        if (stmtCtx == null) return null;
        ParserRuleContext list = (ParserRuleContext) stmtCtx.getParent();
        if (list == null) return null;
        int idx = list.children == null ? -1 : list.children.indexOf(stmtCtx);
        if (idx < 0 || idx + 1 >= list.getChildCount()) return null;
        org.antlr.v4.runtime.tree.ParseTree next = list.getChild(idx + 1);
        if (next instanceof SqlServerParser.StatementContext) {
            return (SqlServerParser.StatementContext) next;
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Statement inventory (drives the statement summary) + TRY/CATCH handlers
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitStatement(SqlServerParser.StatementContext ctx) {
        if (ctx.getChildCount() > 0) {
            org.antlr.v4.runtime.tree.ParseTree c0 = ctx.getChild(0);
            if (c0 instanceof SqlServerParser.TransactionStatementContext) {
                addTransactionStatement((SqlServerParser.TransactionStatementContext) c0);
            } else {
                String type = statementType(c0);
                if (type != null) {
                    result.getStatements().add(
                            new SqlServerStatementInfo(type, ctx.getStart().getLine()));
                }
                // Task #26: a statement-level bare SELECT is one result set handed to the client.
                // Subqueries are a different grammar rule and never reach a `statement`, and
                // `SELECT … INTO` fails selectList (INTO is excluded) so it lands in otherStatement.
                // The one SELECT that is NOT a result set is the variable-assignment form
                // (`SELECT @x = col FROM t`), which selectList accepts — exclude it by its leading
                // `@var =` shape.
                if (c0 instanceof SqlServerParser.SelectStatementContext
                        && !isAssignmentSelect((SqlServerParser.SelectStatementContext) c0)) {
                    result.setResultSets(result.getResultSets() + 1);
                    result.setReturnsResultSet(true);
                }
            }
        }
        return visitChildren(ctx);
    }

    /**
     * Transaction-control capture (T-SQL):
     *   COMMIT [TRAN|TRANSACTION]                → COMMIT
     *   ROLLBACK [TRAN|TRANSACTION] [savepoint]  → ROLLBACK (+ savepointName if present)
     *   SAVE TRAN|TRANSACTION savepoint          → SAVEPOINT (+ savepointName)
     *   BEGIN TRAN|TRANSACTION                    → BEGIN_TRAN
     *
     * Nesting-depth tracking (additive-only; see SqlServerStatementInfo.transactionDepth):
     * a simple per-routine counter (transactionDepth) is incremented on BEGIN_TRAN (recorded
     * AFTER incrementing — i.e. the depth being entered) and decremented on COMMIT/ROLLBACK
     * (recorded BEFORE decrementing — i.e. the depth being closed). SAVEPOINT (SAVE TRANSACTION)
     * records the current depth without changing the counter. No correlation/pairing object is
     * built — the depth number alone lets downstream consumers reconstruct nesting/balance.
     */
    private void addTransactionStatement(SqlServerParser.TransactionStatementContext ctx) {
        int line = ctx.getStart().getLine();
        SqlServerStatementInfo si = new SqlServerStatementInfo();
        si.setLine(line);
        String name = ctx.ID() != null ? ctx.ID().getText() : null;
        if (ctx.COMMIT() != null) {
            si.setType("COMMIT");
            if (transactionDepth > 0) si.setTransactionDepth(transactionDepth--);
        } else if (ctx.ROLLBACK() != null) {
            si.setType("ROLLBACK");
            if (name != null) si.setSavepointName(name);
            if (transactionDepth > 0) si.setTransactionDepth(transactionDepth--);
        } else if (ctx.SAVE() != null) {
            si.setType("SAVEPOINT");
            if (name != null) si.setSavepointName(name);
            si.setTransactionDepth(transactionDepth);
        } else {
            si.setType("BEGIN_TRAN");
            si.setTransactionDepth(++transactionDepth);
        }
        result.getStatements().add(si);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // THROW / RAISERROR (bugs 6/7): dedicated grammar rules now exist (previously both fell
    // into otherStatement's catch-all, indistinguishable bare-vs-args, args never extracted).
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitThrowStatement(SqlServerParser.ThrowStatementContext ctx) {
        List<SqlServerParser.ThrowArgContext> args = ctx.throwArg();
        boolean bare = args == null || args.isEmpty();
        SqlServerStatementInfo si = new SqlServerStatementInfo(
                bare ? "THROW_BARE" : "THROW", ctx.getStart().getLine());
        if (!bare) {
            si.setErrorNumber(args.get(0).getText());
            si.setMessage(args.get(1).getText());
            si.setState(args.get(2).getText());
        }
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitRaiserrorStatement(SqlServerParser.RaiserrorStatementContext ctx) {
        SqlServerStatementInfo si = new SqlServerStatementInfo("RAISERROR", ctx.getStart().getLine());
        List<SqlServerParser.ThrowArgContext> args = ctx.throwArg();
        if (args != null) {
            if (args.size() > 0) si.setMessage(args.get(0).getText());
            if (args.size() > 1) si.setSeverity(args.get(1).getText());
            if (args.size() > 2) si.setState(args.get(2).getText());
        }
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitTryBlock(SqlServerParser.TryBlockContext ctx) {
        SqlServerExceptionHandlerInfo eh = new SqlServerExceptionHandlerInfo();
        eh.setLine(ctx.getStart().getLine());
        eh.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        eh.setStatementsCount(ctx.statement() != null ? ctx.statement().size() : 0);
        result.getExceptionHandlers().add(eh);
        return visitChildren(ctx);
    }

    private String statementType(org.antlr.v4.runtime.tree.ParseTree child) {
        if (child instanceof SqlServerParser.ExecStatementContext)        return "EXEC";
        if (child instanceof SqlServerParser.DeclareStatementContext)     return "DECLARE";
        if (child instanceof SqlServerParser.SetStatementContext)         return "SET";
        if (child instanceof SqlServerParser.InsertStatementContext)      return "INSERT";
        if (child instanceof SqlServerParser.UpdateStatementContext)      return "UPDATE";
        if (child instanceof SqlServerParser.DeleteStatementContext)      return "DELETE";
        if (child instanceof SqlServerParser.SelectStatementContext)      return "SELECT";
        if (child instanceof SqlServerParser.IfStatementContext)          return "IF";
        if (child instanceof SqlServerParser.WhileStatementContext)       return "WHILE_LOOP";
        if (child instanceof SqlServerParser.TryBlockContext)             return "TRY_CATCH";
        if (child instanceof SqlServerParser.TransactionStatementContext) return "TRANSACTION";
        if (child instanceof SqlServerParser.CreateStatementContext)      return "CREATE";
        if (child instanceof SqlServerParser.ReturnStatementContext)      return "RETURN";
        if (child instanceof SqlServerParser.BeginEndBlockContext)        return "BLOCK";
        if (child instanceof SqlServerParser.ThrowStatementContext)       return null; // handled by visitThrowStatement
        if (child instanceof SqlServerParser.RaiserrorStatementContext)   return null; // handled by visitRaiserrorStatement
        if (child instanceof SqlServerParser.OtherStatementContext) {
            // derive from the leading keyword only (MERGE/OPEN/FETCH/CLOSE/PRINT/THROW/WITH…)
            org.antlr.v4.runtime.ParserRuleContext prc = (org.antlr.v4.runtime.ParserRuleContext) child;
            if (prc.getStart() != null) {
                String first = prc.getStart().getText();
                if (first != null && first.matches("[A-Za-z]+")) {
                    String kw = first.toUpperCase();
                    // Normalize WITH-led statements to the common vocabulary token "CTE"
                    // so all dialects report common-table-expressions uniformly.
                    if ("WITH".equals(kw)) return "CTE";
                    return kw;
                }
            }
            return "OTHER";
        }
        return null;
    }

    /**
     * Task #26: distinguishes the variable-assignment SELECT ({@code SELECT @x = col FROM t}) from a
     * genuine result-set SELECT. The grammar's permissive {@code selectList} accepts both, so the
     * difference is read from the first two select-list tokens: an assignment starts with
     * {@code AT_VAR} then {@code EQ}.
     */
    private boolean isAssignmentSelect(SqlServerParser.SelectStatementContext sel) {
        // selectStatement is `SELECT stmtTail` with stmtTail opaque, so the select list is not a
        // subtree to inspect; the statement's own text is. `SELECT @x = …` is the assignment form.
        String tail = sel.stmtTail() != null ? originalText(sel.stmtTail()) : null;
        return tail != null && ASSIGNMENT_SELECT.matcher(tail.trim()).find();
    }

    private static final Pattern ASSIGNMENT_SELECT =
            Pattern.compile("^@[A-Za-z_][A-Za-z0-9_]*\\s*=");

    /**
     * Task #27: split a declared table shape's parenGroup text — e.g.
     * {@code "(id INT, name VARCHAR(50), amount DECIMAL(18,2))"} — into its column names, in
     * declaration order. Top-level commas separate columns; commas nested inside a type's own parens
     * (DECIMAL(18,2)) do not. The parenGroup rule is deliberately opaque, so this reads the tokens
     * the grammar already grouped rather than re-scanning the statement.
     */
    private static List<String> extractDeclaredColumns(String parenText) {
        List<String> cols = new ArrayList<>();
        if (parenText == null) return cols;
        String inner = parenText.trim();
        if (inner.startsWith("(")) inner = inner.substring(1);
        if (inner.endsWith(")")) inner = inner.substring(0, inner.length() - 1);
        List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) { parts.add(inner.substring(start, i)); start = i + 1; }
        }
        parts.add(inner.substring(start));
        for (String part : parts) {
            String p = part.trim();
            if (p.isEmpty()) continue;
            if (p.startsWith("[")) {
                int end = p.indexOf(']');
                if (end > 1) { cols.add(p.substring(1, end)); continue; }
            }
            Matcher m = DECLARED_COL_NAME.matcher(p);
            if (m.find()) cols.add(m.group(1));
        }
        return cols;
    }

    private static final Pattern DECLARED_COL_NAME =
            Pattern.compile("^([A-Za-z_][A-Za-z0-9_]*)");

    // ─────────────────────────────────────────────────────────────────────────
    // IF [NOT] EXISTS(SELECT ...) — the condition is an opaque `expr`/`parenGroup`, so no
    // table-extraction pass ever ran over it; the EXISTS-subquery's read was silently dropped.
    // ─────────────────────────────────────────────────────────────────────────

    private static final Pattern EXISTS_PATTERN = Pattern.compile("(?i)\\bEXISTS\\s*\\(");

    @Override
    public Void visitIfStatement(SqlServerParser.IfStatementContext ctx) {
        if (ctx.expr() != null) {
            // Grammar-first (batch 2): an EXISTS(subquery) in the condition is an existsPredicate
            // node whose queryExpression is a real subtree, so its tables/joins are extracted by
            // the SAME structured walk a standalone SELECT uses (processQueryExpression). The
            // node's tokens carry absolute source lines, so the walk is called with baseLine=1
            // (structuredLine then returns the token's own line, i.e. no offset).
            List<SqlServerParser.ExistsPredicateContext> preds = new ArrayList<>();
            collectNodes(ctx.expr(), SqlServerParser.ExistsPredicateContext.class, preds);
            if (!preds.isEmpty()) {
                for (SqlServerParser.ExistsPredicateContext p : preds) {
                    if (p.queryExpression() == null) continue;
                    processQueryExpression(p.queryExpression(), 1,
                            buildStructuredAliasMap(p.queryExpression()));
                }
            } else {
                extractExistsSubqueryTables(originalText(ctx.expr()), ctx.getStart().getLine());
            }
        }
        return visitChildren(ctx);
    }

    /** Scans an IF condition for EXISTS(...)/NOT EXISTS(...) subqueries and records the
     *  subquery's FROM/JOIN tables as SELECT table ops (and their joins), same as a real
     *  SELECT statement's stmtTail would. Duplicate table+op-type reads (e.g. also appearing
     *  in the IF/ELSE branch bodies) are naturally suppressed by addTableOp's own de-dup key.
     *  Legacy fallback (batch 2) — reached only when the grammar produced no existsPredicate node. */
    private void extractExistsSubqueryTables(String condText, int baseLine) {
        if (condText == null) return;
        Matcher m = EXISTS_PATTERN.matcher(condText);
        boolean counted = false;
        while (m.find()) {
            if (!counted) { counted = true; countRegexFallback(EXISTS_REGEX_FALLBACKS, "EXISTS subquery"); }
            int openParen = m.end() - 1;
            int closeParen = matchingCloseParen(condText, openParen);
            if (closeParen < 0) continue;
            String sub = condText.substring(openParen + 1, closeParen);
            int line = lineAt(condText, m.start(), baseLine);
            addTableOpsFromText(sub, line);
            attachJoins(sub, line);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SET @var = expr  — detect dynamic SQL pattern (SET @sql = '...')
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitSetStatement(SqlServerParser.SetStatementContext ctx) {
        // Session-option form (SET NOCOUNT ON) has no expr — nothing to inspect.
        if (ctx.expr() == null) return null;
        String exprText = ctx.expr().getText();
        // Detect: the RHS is a string literal whose content actually looks like SQL → dynamic SQL.
        // A plain string (e.g. SET @v = 'x') must not be misclassified just for being quoted.
        if ((exprText.startsWith("'") || exprText.startsWith("N'") || exprText.startsWith("n'"))
                && looksLikeSql(exprText)) {
            int line = ctx.getStart().getLine();
            String orig = originalText(ctx.expr());   // preserves whitespace inside the literal
            SqlServerDynamicSqlInfo d = new SqlServerDynamicSqlInfo();
            d.setKind(SqlServerDynamicSqlInfo.Kind.SET_SQL);
            d.setSqlSource(orig);
            d.setLine(line);
            result.getDynamicSql().add(d);
            // Analyze the SQL text embedded in the string literal: recover its tables + joins.
            scanDynamicSqlLiteralForTables(orig, line);
            // Bug 8: remember this variable's literal SQL text for later EXEC(@var) resolution.
            if (ctx.AT_VAR() != null) {
                sqlVarLiteralMap.put(ctx.AT_VAR().getText(), orig);
            }
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EXEC / EXECUTE  — proc calls + sp_executesql detection
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitExecStatement(SqlServerParser.ExecStatementContext ctx) {
        // Structured `EXEC sp_executesql @sql [, N'@p INT'][, @p=…]` (batch 2): the arguments are
        // real nodes, so no text scanning is needed.
        if (ctx.SP_EXECUTESQL() != null) {
            handleSpExecuteSqlStructured(ctx);
            return null;
        }
        // EXEC(@sql) / EXEC('literal') form has no routineName — plain string execution, a
        // structurally different mechanism from EXEC sp_executesql (no parameterization).
        if (ctx.routineName() == null) {
            SqlServerDynamicSqlInfo d = new SqlServerDynamicSqlInfo();
            d.setKind(SqlServerDynamicSqlInfo.Kind.EXEC_STRING);
            d.setLine(ctx.getStart().getLine());
            result.getDynamicSql().add(d);
            // Bug 9 (§DML-audit, lower priority, literal-only scope): `EXEC('<literal SQL>')`
            // (no variable/concatenation) has its table name known at parse time — extract it,
            // same mechanism as the SET @sql = '...' literal path above.
            if (ctx.expr() != null) {
                String exprText = ctx.expr().getText();
                if ((exprText.startsWith("'") || exprText.startsWith("N'") || exprText.startsWith("n'"))
                        && looksLikeSql(exprText)) {
                    String orig = originalText(ctx.expr());
                    int line = ctx.getStart().getLine();
                    scanDynamicSqlLiteralForTables(orig, line);
                } else if (exprText.startsWith("@")) {
                    // Bug 8: EXEC(@var) — resolve @var against a literal seen earlier in this
                    // routine (DECLARE @sql = '...' / SET @sql = '...'); if none was recorded,
                    // flag it as unresolved (Option C, mirroring plsql-parser's DynamicSqlInfo).
                    String literal = sqlVarLiteralMap.get(exprText.trim());
                    if (literal != null) {
                        d.setSqlSource(literal);
                        int line = ctx.getStart().getLine();
                        scanDynamicSqlLiteralForTables(literal, line);
                    } else {
                        d.setUnresolvedVarName(exprText.trim());
                    }
                }
            }
            return null;
        }

        String procName = extractRoutineName(ctx.routineName());

        // sp_executesql → dynamic SQL, not a regular call.
        // Reached only via the generic alternative (e.g. a schema-qualified `dbo.sp_executesql`,
        // where the structured alternative does not apply) — i.e. the legacy text decode below is
        // now a fallback, and its use is counted.
        if ("sp_executesql".equalsIgnoreCase(procName)) {
            countRegexFallback(SP_EXECUTESQL_REGEX_FALLBACKS, "sp_executesql args");
            SqlServerDynamicSqlInfo d = new SqlServerDynamicSqlInfo();
            d.setKind(SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL);
            d.setLine(ctx.getStart().getLine());
            // Bug 8: first argument is always the @sql variable — resolve it the same way as
            // EXEC(@var) above, so sp_executesql gets the same unresolved/resolved treatment.
            String tail = originalText(ctx.stmtTail());
            if (tail != null) {
                Matcher am = SP_EXECUTESQL_FIRST_ARG_PATTERN.matcher(tail);
                if (am.find()) {
                    String varName = am.group(1);
                    String literal = sqlVarLiteralMap.get(varName);
                    if (literal != null) {
                        d.setSqlSource(literal);
                        int line = ctx.getStart().getLine();
                        scanDynamicSqlLiteralForTables(literal, line);
                    } else {
                        d.setUnresolvedVarName(varName);
                    }
                    // sp_executesql parameter-signature/binding audit: capture the parameter
                    // signature literal (N'@p1 INT') and any bound name=value pairs (@p1=5) that
                    // follow the @sql argument, so a caller can see not just *what* dynamic query
                    // ran but *what value(s)* it ran with.
                    String remainder = tail.substring(am.end());
                    Matcher sigM = SP_EXECUTESQL_PARAM_SIG_PATTERN.matcher(remainder);
                    if (sigM.find()) {
                        d.setParamSignature(sigM.group(1));
                        remainder = remainder.substring(sigM.end());
                    }
                    Matcher bp = SP_EXECUTESQL_BOUND_PARAM_PATTERN.matcher(remainder);
                    while (bp.find()) {
                        d.getBoundParameters().put("@" + bp.group(1), bp.group(2).trim());
                    }
                }
            }
            result.getDynamicSql().add(d);
            return null;
        }

        String execSchema = extractRoutineSchema(ctx.routineName());
        String execKey = (execSchema != null ? execSchema.toUpperCase() + "." : "") + (procName != null ? procName.toUpperCase() : "");
        if (procName != null && seenCalls.add(execKey)) {
            SqlServerCallInfo c = new SqlServerCallInfo();
            c.setSchema(execSchema);
            c.setName(procName);
            c.setLine(ctx.getStart().getLine());
            // Bug 8: capture each call-site argument and whether it carries a trailing
            // OUTPUT/OUT keyword — previously the grammar consumed it (inside stmtTail) but
            // nothing ever inspected it, so OUTPUT-direction arguments were indistinguishable
            // from plain IN arguments in the captured model.
            String argsText = originalText(ctx.stmtTail());
            for (String rawArg : splitTopLevelArgs(argsText)) {
                String arg = rawArg.trim();
                if (arg.isEmpty()) continue;
                Matcher om = ARG_OUTPUT_TRAILING_PATTERN.matcher(arg);
                boolean isOutputArg = om.find();
                String argText = isOutputArg ? arg.substring(0, om.start()).trim() : arg;
                c.getArgs().add(new SqlServerCallInfo.CallArgInfo(argText, isOutputArg));
            }
            result.getCalls().add(c);
        }
        return null;
    }

    /**
     * Grammar-first decode of {@code EXEC sp_executesql @sql [, N'@p INT'][, @p=…]}: the @sql
     * variable (resolved through the same literal map as EXEC(@var)), the parameter-signature
     * literal and the bound name=value pairs all come off the {@code spExecuteSqlArgs} node.
     */
    private void handleSpExecuteSqlStructured(SqlServerParser.ExecStatementContext ctx) {
        SqlServerDynamicSqlInfo d = new SqlServerDynamicSqlInfo();
        d.setKind(SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL);
        d.setLine(ctx.getStart().getLine());
        SqlServerParser.SpExecuteSqlArgsContext args = ctx.spExecuteSqlArgs();
        if (args != null && args.AT_VAR() != null) {
            String varName = args.AT_VAR().getText();
            String literal = sqlVarLiteralMap.get(varName);
            if (literal != null) {
                d.setSqlSource(literal);
                scanDynamicSqlLiteralForTables(literal, ctx.getStart().getLine());
            } else {
                d.setUnresolvedVarName(varName);
            }
            for (SqlServerParser.SpExecuteSqlArgContext a : args.spExecuteSqlArg()) {
                if (a.N_STRING() != null || a.SQ_STRING() != null) {
                    if (d.getParamSignature() == null) d.setParamSignature(originalText(a));
                } else if (a.AT_VAR() != null && a.EQ() != null && a.spArgValue() != null) {
                    d.getBoundParameters().put(a.AT_VAR().getText(), originalText(a.spArgValue()).trim());
                }
            }
        }
        result.getDynamicSql().add(d);
    }

    // Bug 8: a trailing OUTPUT/OUT keyword on a call-site argument, e.g. "@total OUTPUT".
    private static final Pattern ARG_OUTPUT_TRAILING_PATTERN = Pattern.compile("(?i)\\s+(?:OUTPUT|OUT)\\s*$");

    /** Splits a call's argument-list text on top-level commas (honouring string literals and
     *  nested parens), so a comma inside a literal or a nested function call argument list does
     *  not split a single argument into two. */
    private List<String> splitTopLevelArgs(String text) {
        List<String> out = new ArrayList<>();
        if (text == null || text.isEmpty()) return out;
        int depth = 0;
        StringBuilder cur = new StringBuilder();
        int i = 0;
        int len = text.length();
        while (i < len) {
            char ch = text.charAt(i);
            if (ch == '\'') {
                cur.append(ch);
                i++;
                while (i < len) {
                    cur.append(text.charAt(i));
                    if (text.charAt(i) == '\'') {
                        if (i + 1 < len && text.charAt(i + 1) == '\'') { i++; cur.append(text.charAt(i)); i++; continue; }
                        i++;
                        break;
                    }
                    i++;
                }
                continue;
            }
            if (ch == '(') { depth++; cur.append(ch); i++; continue; }
            if (ch == ')') { depth--; cur.append(ch); i++; continue; }
            if (ch == ',' && depth == 0) { out.add(cur.toString()); cur = new StringBuilder(); i++; continue; }
            cur.append(ch);
            i++;
        }
        if (cur.length() > 0) out.add(cur.toString());
        return out;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: INSERT
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitInsertStatement(SqlServerParser.InsertStatementContext ctx) {
        SqlServerTableOpInfo op = new SqlServerTableOpInfo();
        op.setDmlType(SqlServerTableOpInfo.DmlType.INSERT);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(statementText(ctx, true));
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: UPDATE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitUpdateStatement(SqlServerParser.UpdateStatementContext ctx) {
        SqlServerTableOpInfo op = new SqlServerTableOpInfo();
        op.setDmlType(SqlServerTableOpInfo.DmlType.UPDATE);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(statementText(ctx, false));
        result.getTableOps().add(op);
        // Stage 8: T-SQL `UPDATE t SET … FROM <sources> JOIN … ON …`. No-op for a plain UPDATE.
        applyDmlFromClause(ctx.stmtTail(), op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: DELETE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeleteStatement(SqlServerParser.DeleteStatementContext ctx) {
        SqlServerTableOpInfo op = new SqlServerTableOpInfo();
        op.setDmlType(SqlServerTableOpInfo.DmlType.DELETE);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(statementText(ctx, false));
        result.getTableOps().add(op);
        // Stage 8: T-SQL `DELETE t FROM <sources> JOIN … ON …`.
        //
        // Note the grammar rule is `DELETE FROM? tableRef stmtTail`, so for the ORDINARY
        // `DELETE FROM dbo.T WHERE …` the FROM is consumed by the rule itself and never reaches
        // the tail — topLevelFromToken finds nothing and this correctly does nothing. Only the
        // T-SQL-specific second FROM (`DELETE t FROM …`) lands in the tail.
        applyDmlFromClause(ctx.stmtTail(), op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: SELECT — scan stmtTail text for FROM clause
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitSelectStatement(SqlServerParser.SelectStatementContext ctx) {
        // Extract every table in the FROM and JOIN clauses (embedded in stmtTail text).
        int line = ctx.getStart().getLine();
        int before = result.getTableOps().size();
        boolean fromClausePresent = false;
        if (ctx.stmtTail() != null) {
            String txt = originalText(ctx.stmtTail());
            // Strangler-fig migration (Stage 1): try the real grammar first. The nested parse is
            // fed the tail with a "SELECT " prefix ON THE SAME LINE, so the nested parser's line
            // numbers line up exactly with what lineAt() computes over `txt` for the regex path —
            // the two mechanisms therefore agree on line attribution, not merely on table names.
            // Anything outside the migrated subset returns false and the untouched regex path runs.
            if (extractSelectStructurally("SELECT " + txt, line)) {
                fromClausePresent = true;
            } else {
                fromClausePresent = addTableOpsFromText(txt, line);
                attachJoins(txt, line);
            }
        }
        // Guarantee at least one SELECT op even when no table was found (e.g. SELECT of vars),
        // preserving the prior behaviour that every SELECT statement records an op.
        //
        // Bug (unique-concepts audit item 6): this fallback used to fire whenever no table op
        // survived addTableOpsFromText, which conflated two different cases: "SELECT ... 1"
        // genuinely has no FROM clause, but "SELECT ... FROM @tvp" (a table-valued-parameter
        // reference) DOES have a FROM clause — it's just deliberately filtered out by addTableOp
        // because @tvp is a parameter, not a real table. The old code added a bogus placeholder
        // SELECT op with tableName=null for the latter case too. Only fall back when no FROM/JOIN
        // clause was found in the text at all.
        if (result.getTableOps().size() == before && !fromClausePresent) {
            SqlServerTableOpInfo op = new SqlServerTableOpInfo();
            op.setDmlType(SqlServerTableOpInfo.DmlType.SELECT);
            op.setLine(line);
            result.getTableOps().add(op);
        } else if (result.getTableOps().size() == before + 1
                && distinctTableRefs(originalText(ctx.stmtTail())) == 1) {
            // Single-table SELECT: every selected column belongs to that one table, so the
            // statement text can be attributed to this op safely.
            //
            // A multi-table SELECT deliberately gets none. Column detail is folded per table by
            // its operation's table name, so handing the same column list to two table ops would
            // print one table's columns on the other's page — stated as fact. Absent detail is
            // the correct answer there until the enricher can resolve qualifiers to tables.
            result.getTableOps().get(result.getTableOps().size() - 1)
                    .setSqlText(statementText(ctx, false));
        }
        return visitChildren(ctx);
    }

    // Inline TVF body: RETURN ( SELECT ... ) — scan the parenthesised body for tables.
    @Override
    public Void visitReturnStatement(SqlServerParser.ReturnStatementContext ctx) {
        if (ctx.parenGroup() != null) {
            String txt = originalText(ctx.parenGroup());
            addTableOpsFromText(txt, ctx.getStart().getLine());
            attachJoins(txt, ctx.getStart().getLine());
        }
        return visitChildren(ctx);
    }

    /** Original source text for a context, with whitespace preserved (getText() strips it). */

    /**
     * The source text of the statement enclosing {@code ctx}.
     *
     * <p>A SELECT's table op is built from a table reference inside the FROM clause, which knows
     * nothing about the statement around it — so it carried no SQL at all, and the column enricher
     * had nothing to work from. Measured before this: DB2 2,732 and PostgreSQL 2,392 statements with
     * no {@code sqlText}, every one a SELECT, which is why those two dialects showed almost no
     * column detail while MySQL (which already had this helper) showed 3,450.
     *
     * <p>Walks up to the nearest context whose name ends in {@code StatementContext} or
     * {@code StmtContext}, which is the naming convention every grammar here follows.
     */
    private String enclosingStatementText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        org.antlr.v4.runtime.ParserRuleContext c = ctx;
        org.antlr.v4.runtime.ParserRuleContext bestSelect = null;
        org.antlr.v4.runtime.ParserRuleContext stmt = null;
        while (c != null) {
            String n = c.getClass().getSimpleName();
            // Prefer the outermost SELECT over the statement wrapping it.
            //
            // PL/pgSQL and SQL PL wrap reads in control flow — `FOR rec IN SELECT ... LOOP`. Walking
            // straight to the enclosing StatementContext captured the loop, so the text handed to
            // the ANSI parser ended in a bare `LOOP` and failed. Measured: 979 PostgreSQL statements
            // captured-but-unparseable, every one of that shape. Taking the outermost Select keeps
            // the joins and WHERE (which is where the column detail lives) and stops before the
            // control-flow keyword.
            if (n.contains("Select") || n.contains("QuerySpec")) bestSelect = c;
            else if (n.endsWith("StatementContext") || n.endsWith("StmtContext")) { stmt = c; break; }
            c = c.getParent();
        }
        org.antlr.v4.runtime.ParserRuleContext pick = bestSelect != null ? bestSelect : stmt;
        return pick != null ? cutAtStatementEnd(trimTrailingBlockKeyword(originalText(pick))) : null;
    }


    /**
     * Cuts the text at the first unquoted semicolon.
     *
     * <p>A grammar context can span more than one statement, and the captured text then reads
     * {@code SELECT COUNT(*) INTO v_n FROM erp.erp_product; CALL erp.erp_usp_write_audit_trail(...)}.
     * ANSI parsing sees two statements and yields nothing for either, so a read with perfectly
     * ordinary columns and predicates came back empty — 1,223 PostgreSQL statements of exactly that
     * shape, all of which had parsed "successfully" while producing no detail.
     *
     * <p>Quote-aware because a semicolon inside a literal is data, not a terminator; cutting there
     * would truncate mid-string and turn a parseable statement into an unparseable one.
     */
    private String cutAtStatementEnd(String sql) {
        if (sql == null) return null;
        char quote = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == ';') {
                return sql.substring(0, i).stripTrailing();
            }
        }
        return sql;
    }
    /**
     * Strips a trailing block keyword the grammar may have included.
     *
     * <p>Belt and braces for the case above: even with the Select preferred, a grammar rule can end
     * on {@code LOOP}/{@code THEN}/{@code DO}, and one stray keyword makes the whole statement
     * unparseable — losing all of its column detail rather than part of it.
     */
    private String trimTrailingBlockKeyword(String sql) {
        if (sql == null) return null;
        String s = sql.stripTrailing();
        for (String kw : new String[]{"LOOP", "THEN", "DO", "BEGIN"}) {
            if (s.length() > kw.length()) {
                String tail = s.substring(s.length() - kw.length());
                if (tail.equalsIgnoreCase(kw)
                        && Character.isWhitespace(s.charAt(s.length() - kw.length() - 1))) {
                    s = s.substring(0, s.length() - kw.length()).stripTrailing();
                }
            }
        }
        return s;
    }

    private String originalText(ParserRuleContext ctx) {
        if (ctx == null || ctx.getStart() == null || ctx.getStop() == null) return "";
        CharStream cs = ctx.getStart().getInputStream();
        if (cs == null) return ctx.getText();
        return cs.getText(Interval.of(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // statementText — the WHOLE statement, for column-level analysis
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Keywords that can only begin a new statement, never continue the current one.
     * Used to bound {@link #statementText} when T-SQL omits the statement terminator.
     */
    private static final Set<String> STATEMENT_START_KEYWORDS = Set.of(
            "BEGIN", "END", "DECLARE", "IF", "ELSE", "WHILE", "EXEC", "EXECUTE",
            "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "CREATE", "ALTER", "DROP",
            "SET", "RETURN", "PRINT", "THROW", "RAISERROR", "GOTO", "BREAK", "CONTINUE",
            "COMMIT", "ROLLBACK", "SAVE", "TRY", "CATCH", "FETCH", "OPEN", "CLOSE",
            "DEALLOCATE", "WAITFOR", "GO");

    /** Never grow a captured statement beyond this; over-capture would be worse than under. */
    private static final int MAX_STATEMENT_CHARS = 8000;

    /**
     * The full source text of a DML statement, for {@code TableOperationEnricher}.
     *
     * <h3>Why {@link #originalText} is not enough</h3>
     * The grammar's {@code stmtTail} rule stops at the first statement-starting keyword, so
     * {@code INSERT INTO t(a,b) SELECT a,b FROM s} yields a context ending at {@code (a,b)} —
     * {@code "INSERT INTO t(a, b)"}, which does not parse and therefore produced <em>no</em>
     * column detail at all. This was measured, not guessed: 57 real T-SQL sources gave
     * 258/3682 operations with column detail, and the unparsed ones were dominated by
     * {@code INSERT ... SELECT} truncated at the column list.
     *
     * <p>So capture continues past the context, tracking string literals, bracket identifiers,
     * comments and parenthesis depth, until the statement genuinely ends: a {@code ;} at depth
     * zero, or — because T-SQL frequently omits it — a keyword that cannot continue this
     * statement. {@code SELECT}/{@code WITH}/{@code VALUES} are legal continuations of an
     * {@code INSERT} and so are not treated as boundaries for one.
     *
     * <p>If the scan would exceed {@link #MAX_STATEMENT_CHARS} the narrow context text is
     * returned instead. Over-capturing would splice unrelated statements together and could
     * attribute one table's columns to another; no column detail is the safer failure.
     */
    private String statementText(ParserRuleContext ctx, boolean insertContinuations) {
        if (ctx == null || ctx.getStart() == null || ctx.getStop() == null) return "";
        CharStream cs = ctx.getStart().getInputStream();
        if (cs == null) return ctx.getText();
        int start = ctx.getStart().getStartIndex();
        int narrowStop = ctx.getStop().getStopIndex();
        String all = wholeSource(cs);
        int size = all.length();
        // The grammar's DML rules end with an optional SEMI. When it is there the statement is
        // already complete and scanning on would only pick up trailing whitespace.
        if (narrowStop >= 0 && narrowStop < size && all.charAt(narrowStop) == ';') {
            return cs.getText(Interval.of(start, narrowStop));
        }
        int i = narrowStop + 1;
        int depth = 0;
        int limit = Math.min(size, start + MAX_STATEMENT_CHARS);
        while (i < limit) {
            char ch = all.charAt(i);
            if (ch == '\'') {                              // string literal, '' escapes
                i++;
                while (i < limit) {
                    if (all.charAt(i) == '\'') {
                        if (i + 1 < limit && all.charAt(i + 1) == '\'') { i += 2; continue; }
                        break;
                    }
                    i++;
                }
                i++;
                continue;
            }
            if (ch == '[') {                               // bracket identifier
                while (i < limit && all.charAt(i) != ']') i++;
                i++;
                continue;
            }
            if (ch == '-' && i + 1 < limit && all.charAt(i + 1) == '-') {
                while (i < limit && all.charAt(i) != '\n') i++;
                continue;
            }
            if (ch == '/' && i + 1 < limit && all.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < limit && !(all.charAt(i) == '*' && all.charAt(i + 1) == '/')) i++;
                i += 2;
                continue;
            }
            if (ch == '(') { depth++; i++; continue; }
            if (ch == ')') {
                if (depth == 0) break;                     // closing paren we never opened
                depth--; i++; continue;
            }
            if (ch == ';' && depth == 0) break;
            if (depth == 0 && (Character.isLetter(ch) || ch == '_')) {
                int j = i;
                while (j < limit && (Character.isLetterOrDigit(all.charAt(j)) || all.charAt(j) == '_')) j++;
                String word = all.substring(i, j).toUpperCase();
                if (isBoundaryKeyword(word, insertContinuations)) break;
                i = j;
                continue;
            }
            i++;
        }
        if (i >= limit && limit < size) {
            // Ran into the size cap without finding an end — do not guess.
            return cs.getText(Interval.of(start, narrowStop));
        }
        int end = Math.min(i - 1, size - 1);
        if (end < narrowStop) end = narrowStop;
        return cs.getText(Interval.of(start, end));
    }

    private CharStream cachedStream;
    private String cachedSource;

    /**
     * The whole source, materialised once. {@code CharStream.getText} copies, and
     * {@link #statementText} is called for every DML statement in a routine — thousands on the
     * real sources — so re-extracting per call would be quadratic in source size.
     */
    private String wholeSource(CharStream cs) {
        if (cs != cachedStream) {
            cachedStream = cs;
            cachedSource = cs.getText(Interval.of(0, cs.size() - 1));
        }
        return cachedSource;
    }

    private boolean isBoundaryKeyword(String word, boolean insertContinuations) {
        if (insertContinuations && ("SELECT".equals(word) || "WITH".equals(word)
                || "VALUES".equals(word) || "EXEC".equals(word) || "EXECUTE".equals(word))) {
            return false;
        }
        return STATEMENT_START_KEYWORDS.contains(word) || "SELECT".equals(word);
    }

    /** Emit sequence usages (NEXT VALUE FOR schema.seq) found in the body. */
    private void extractSequences(ParserRuleContext bodyCtx) {
        if (bodyCtx == null) return;
        // Grammar-first (batch 2): sequence usages come from the nextValueFor nodes in the tree.
        List<SqlServerParser.NextValueForContext> uses = new ArrayList<>();
        collectNodes(bodyCtx, SqlServerParser.NextValueForContext.class, uses);
        if (!uses.isEmpty()) {
            Set<String> seen = new HashSet<>();
            for (SqlServerParser.NextValueForContext u : uses) {
                if (u.tableRef() == null) continue;
                String[] parts = splitQualifiedName(u.tableRef().getText());
                String schema = parts.length >= 2 ? stripBrackets(parts[parts.length - 2]) : null;
                String name   = stripBrackets(parts[parts.length - 1]);
                if (!seen.add((schema != null ? schema + "." : "") + name.toUpperCase())) continue;
                SqlServerSequenceInfo s = new SqlServerSequenceInfo();
                s.setSchema(schema);
                s.setName(name);
                s.setLine(u.getStart().getLine());
                result.getSequences().add(s);
            }
            return;
        }
        // Fallback: no structured NEXT VALUE FOR node — scan the body text, counted.
        String text = originalText(bodyCtx);
        if (text == null || text.isEmpty()) return;
        int bodyStartLine = bodyCtx.getStart().getLine();
        Set<String> seen = new HashSet<>();
        Matcher m = SEQ_PATTERN.matcher(text);
        boolean matched = false;
        while (m.find()) {
            if (!matched) { matched = true; countRegexFallback(SEQUENCE_REGEX_FALLBACKS, "NEXT VALUE FOR"); }
            String schema = m.group(2) != null ? stripBrackets(m.group(1)) : null;
            String name   = m.group(2) != null ? stripBrackets(m.group(2)) : stripBrackets(m.group(1));
            if (!seen.add((schema != null ? schema + "." : "") + name.toUpperCase())) continue;
            SqlServerSequenceInfo s = new SqlServerSequenceInfo();
            s.setSchema(schema);
            s.setName(name);
            // Actual line = body start line + newlines before the match (not just the body start).
            s.setLine(lineAt(text, m.start(), bodyStartLine));
            result.getSequences().add(s);
        }
    }

    /** Emit proc→function / function→function / →TVF edges for schema-qualified calls in the body. */
    private void extractQualifiedCalls(ParserRuleContext bodyCtx) {
        if (bodyCtx == null) return;
        String text = originalText(bodyCtx);
        if (text == null || text.isEmpty()) return;

        // Grammar-first (task #30): schema-qualified calls now come off callExpression nodes. A call
        // with fewer than two name parts is unqualified (ISNULL, GETDATE, COUNT…) — the regex
        // required a dot, so these are ignored exactly as before. The last two segments are
        // schema.name, matching the regex's `([\w#$]+)\.([\w#$]+)\s*\(`.
        List<SqlServerParser.CallExpressionContext> nodes = new ArrayList<>();
        collectNodes(bodyCtx, SqlServerParser.CallExpressionContext.class, nodes);
        for (SqlServerParser.CallExpressionContext node : nodes) {
            if (node.callName() == null) continue;
            List<SqlServerParser.NamePartContext> parts = node.callName().namePart();
            if (parts.size() < 2) continue;
            String schema = stripBrackets(parts.get(parts.size() - 2).getText());
            String name = stripBrackets(parts.get(parts.size() - 1).getText());
            addQualifiedCall(schema, name, node.getStart().getLine());
        }

        // Counted fallback: a qualified call the structured walk could not see — one inside a
        // deferred MERGE/WITH/otherStatement body, inside a parenthesised subquery (consumed as a
        // balanced parenGroup, not tokenised), or a bracketed/odd shape the rule did not reach.
        // Deduped against the structured set, so on a fully-migrated body this contributes nothing
        // and the counter stays flat. Comment/literal masking is preserved (Bug 3 / Bug 7 below).
        String scanText = stripLineAndBlockComments(text);
        Matcher m = QUALIFIED_CALL_PATTERN.matcher(scanText);
        boolean counted = false;
        while (m.find()) {
            String schema = m.group(1);
            String name = m.group(2);
            // Skip INSERT-target tables: "INSERT INTO sales.SalesOrder(col,...)" — the
            // "table(columns)" looks like a call but is a table (already captured as a table-op).
            // Only INTO is excluded; FROM/JOIN may precede legit table-valued-function calls.
            String pre = scanText.substring(Math.max(0, m.start() - 10), m.start());
            if (pre.matches("(?is).*\\bINTO\\s+$")) continue;
            // Bug 2: a CLR user-defined-type method call on a local @variable/@parameter,
            // e.g. @geom1.STIntersects(@geom2), matches this same qualified-call shape but is not
            // a real outbound call to a schema/routine named after that variable — the "schema"
            // part here strips down to a declared @-sigil name in scope. Suppress it, matching
            // the same false-positive class the Oracle/Postgres dialects guard against.
            if (isDeclaredAtVariable(schema)) continue;
            // Same guards addQualifiedCall applies, checked BEFORE the fallback is counted so the
            // counter records only calls that are genuinely added (not self-refs/XML methods).
            if (name.equalsIgnoreCase(result.getName())) continue;
            if (XML_METHOD_NAMES.contains(name.toUpperCase())) continue;
            String callKey = (schema != null ? schema.toUpperCase() + "." : "") + name.toUpperCase();
            if (seenCalls.contains(callKey)) continue;   // already captured structurally
            if (!counted) {
                counted = true;
                countRegexFallback(QUALIFIED_CALL_REGEX_FALLBACKS, "qualified call");
            }
            addQualifiedCall(schema, name, lineAt(text, m.start(), bodyCtx.getStart().getLine()));
        }
    }

    /**
     * Record one schema-qualified call, applying the guards shared by the structured and regex
     * paths: skip self-reference, skip SQL Server's fixed built-in XML data-type methods
     * (Bug 7), and dedup on the SCHEMA-QUALIFIED name so same-named routines in different schemas
     * (sales.fn_X vs hr.fn_X) stay distinct.
     */
    private void addQualifiedCall(String schema, String name, int line) {
        if (name == null || name.isEmpty()) return;
        if (name.equalsIgnoreCase(result.getName())) return;
        // Bug 7 (XML-method audit): xml_col.value(...)/.exist(...)/.query(...)/.nodes(...) are
        // SQL Server's fixed set of built-in XML data-type methods, not calls to a
        // schema-qualified routine. Exclude this exact 4-method set unconditionally.
        if (XML_METHOD_NAMES.contains(name.toUpperCase())) return;
        String callKey = (schema != null ? schema.toUpperCase() + "." : "") + name.toUpperCase();
        if (!seenCalls.add(callKey)) return;
        SqlServerCallInfo c = new SqlServerCallInfo();
        c.setSchema(schema);
        c.setName(name);
        c.setLine(line);
        result.getCalls().add(c);
    }

    /**
     * True when {@code rawSchemaToken} (the qualifier captured by {@link #QUALIFIED_CALL_PATTERN},
     * before bracket-stripping) is really a declared {@code @}-sigil parameter or local variable
     * of the current routine, not a schema name. The pattern's qualifier group never includes the
     * {@code @} sigil itself (it is not in the character class), so a CLR UDT call like
     * {@code @geom1.STIntersects(...)} is captured with {@code rawSchemaToken == "geom1"} — the
     * same "sigil stripped" shape as a real declared name minus its leading {@code @}.
     */
    private boolean isDeclaredAtVariable(String rawSchemaToken) {
        if (rawSchemaToken == null || rawSchemaToken.isEmpty()) return false;
        String candidate = "@" + rawSchemaToken;
        for (SqlServerParamInfo p : result.getParams()) {
            if (candidate.equalsIgnoreCase(p.getName())) return true;
        }
        for (SqlServerVariableInfo v : result.getVariables()) {
            if (candidate.equalsIgnoreCase(v.getName())) return true;
        }
        return false;
    }

    /**
     * Replace the contents of {@code --} line comments and {@code /* ... *}{@code /} block comments
     * with spaces (length-preserving, so line/offset tracking via {@link #lineAt} stays accurate).
     * Mirrors the comment-skipping character handling already used inline by {@link #statementText}
     * for the same reason: raw source text scanned by a regex must not treat commented-out code as
     * live code.
     */
    private String stripLineAndBlockComments(String text) {
        return maskStringLiterals(stripCommentsOnly(text));
    }

    /**
     * The comment-blanking half of {@link #stripLineAndBlockComments}, WITHOUT the string-literal
     * masking — length-preserving in exactly the same way (a comment becomes an equally long run of
     * spaces, newlines preserved).
     *
     * <p>The two were fused because every caller was a REGEX scan, and there the masking is a
     * necessary defence: {@code TABLE_REF_PATTERN} and friends cannot tell a real {@code JOIN}
     * keyword from the word "JOIN" sitting inside {@code N'Please JOIN Support ON call'}, so the
     * literal's contents have to be destroyed before they are scanned.
     *
     * <p>That rationale is purely an artefact of scanning with regex over raw text, and it does not
     * transfer to the structured path: there a string literal is already a single {@code SQ_STRING}
     * / {@code N_STRING} token that the grammar can never confuse with a keyword. Blanking it there
     * would destroy real condition text to defend against a problem that cannot occur — so the
     * structured join-condition path uses THIS method and keeps the literal intact.
     */
    private String stripCommentsOnly(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        int i = 0;
        int len = text.length();
        while (i < len) {
            char ch = text.charAt(i);
            if (ch == '-' && i + 1 < len && text.charAt(i + 1) == '-') {
                while (i < len && text.charAt(i) != '\n') { sb.append(' '); i++; }
                continue;
            }
            if (ch == '/' && i + 1 < len && text.charAt(i + 1) == '*') {
                sb.append(' ').append(' ');
                i += 2;
                while (i + 1 < len && !(text.charAt(i) == '*' && text.charAt(i + 1) == '/')) {
                    sb.append(text.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                if (i + 1 < len) { sb.append(' ').append(' '); i += 2; }
                continue;
            }
            sb.append(ch);
            i++;
        }
        return sb.toString();
    }

    /**
     * Bug (string-literal-content audit): replaces the CONTENTS of every {@code '...'} /
     * {@code N'...'} string literal with a same-length run of spaces, leaving the surrounding
     * quotes (and the optional leading {@code N}/{@code n}) untouched and the overall text length
     * unchanged (so {@link #lineAt} offsets computed against the original text stay valid).
     *
     * <p>Without this, {@code TABLE_REF_PATTERN}/{@code JOIN_PATTERN}/{@code FROM_PATTERN} — which
     * only ever saw comments stripped, never literal contents — happily matched keyword-looking
     * substrings INSIDE a string literal (e.g. {@code WHERE note = N'Please JOIN Support ON call
     * for help'}) and invented a phantom table/join from plain text data. The SQL-standard doubled
     * single-quote escape ({@code ''} for a literal {@code '} inside a literal) is honoured so
     * masking does not terminate at an escaped quote.
     */
    private String maskStringLiterals(String text) {
        int len = text.length();
        StringBuilder sb = new StringBuilder(len);
        int i = 0;
        while (i < len) {
            char ch = text.charAt(i);
            // Optional N/n prefix immediately before the opening quote (Unicode string literal).
            if ((ch == 'N' || ch == 'n') && i + 1 < len && text.charAt(i + 1) == '\'') {
                sb.append(ch);
                i++;
                continue;
            }
            if (ch == '\'') {
                sb.append(ch); // opening quote preserved
                i++;
                while (i < len) {
                    char c = text.charAt(i);
                    if (c == '\'') {
                        if (i + 1 < len && text.charAt(i + 1) == '\'') {
                            // Escaped quote ('') inside the literal — mask both chars, not a terminator.
                            sb.append(' ').append(' ');
                            i += 2;
                            continue;
                        }
                        sb.append(c); // closing quote preserved
                        i++;
                        break;
                    }
                    sb.append(c == '\n' ? '\n' : ' '); // mask content, preserve newlines for lineAt
                    i++;
                }
                continue;
            }
            sb.append(ch);
            i++;
        }
        return sb.toString();
    }

    /**
     * Scans dynamic-SQL text recovered from a string-literal source (DECLARE/SET @sql = '...',
     * EXEC('...'), a resolved sp_executesql/EXEC(@var) literal, …) for its own tables/joins.
     *
     * <p>Unlike {@link #addTableOpsFromText}/{@link #attachJoins}'s normal callers — which are
     * handed real surrounding SQL code that merely CONTAINS a string literal somewhere inside it —
     * this text IS itself a whole string literal (its content is the actual dynamic SQL to
     * analyze). {@link #maskStringLiterals} must not blank it out just because it is quoted, so the
     * literal's outer quotes (and any doubled {@code ''} escapes) are unwrapped first, exposing the
     * real SQL text beneath — any string literal genuinely nested INSIDE that dynamic SQL (e.g.
     * {@code WHERE note = 'x'}) is still correctly masked by the normal scan afterwards.
     *
     * <p>When {@code literalOrCode} is instead a CONCATENATION of literal fragments and non-literal
     * pieces (e.g. {@code 'SELECT ... FROM orders WHERE id = ' + CAST(@id AS VARCHAR)}, a common
     * dynamic-SQL-building idiom), each top-level {@code '+'}-separated literal fragment IS itself
     * real SQL text (not string "data" to hide) and non-literal fragments (variables, casts) are
     * simply unknown at parse time — so the literal fragments are unwrapped and concatenated back
     * together (in source order, non-literal fragments contributing nothing) to reconstruct as much
     * of the intended dynamic SQL as is statically knowable, before the usual scan runs on it.
     */
    private void scanDynamicSqlLiteralForTables(String literalOrCode, int line) {
        if (literalOrCode == null) return;
        String whole = unwrapWholeStringLiteral(literalOrCode);
        String sql = whole != null ? whole : reconstructDynamicSqlFromConcatenation(literalOrCode);
        if (sql == null || sql.isEmpty()) return;
        addTableOpsFromText(sql, line);
        attachJoins(sql, line);
    }

    /**
     * When {@code s} (after trimming) is ENTIRELY one string literal — an optional {@code N}/{@code
     * n} prefix followed by a single {@code '...'} run spanning the whole string, honouring {@code
     * ''} escapes — returns its unescaped content (no surrounding quotes, {@code ''} collapsed to
     * {@code '}). Otherwise (e.g. a concatenation expression with multiple literal fragments and
     * variables, `'a' + @v + 'b'`) returns {@code null}, since there is no single literal whose
     * whole content is safe to expose.
     */
    private String unwrapWholeStringLiteral(String s) {
        String trimmed = s.trim();
        int i = 0;
        int len = trimmed.length();
        if (i < len && (trimmed.charAt(i) == 'N' || trimmed.charAt(i) == 'n') && i + 1 < len && trimmed.charAt(i + 1) == '\'') {
            i++;
        }
        if (i >= len || trimmed.charAt(i) != '\'') return null;
        int contentStart = i + 1;
        int j = contentStart;
        StringBuilder content = new StringBuilder();
        while (j < len) {
            char c = trimmed.charAt(j);
            if (c == '\'') {
                if (j + 1 < len && trimmed.charAt(j + 1) == '\'') {
                    content.append('\'');
                    j += 2;
                    continue;
                }
                // Closing quote — must be the very last character for this to be a WHOLE literal.
                if (j == len - 1) return content.toString();
                return null; // more text follows the literal (concatenation) — not a whole literal
            }
            content.append(c);
            j++;
        }
        return null; // unterminated — leave untouched
    }

    /** Reconstructs the statically-knowable portion of a concatenation-built dynamic SQL string by
     *  unwrapping and joining every top-level {@code '+'}-separated literal fragment, in order,
     *  and discarding non-literal fragments (variables, CAST(...), etc.) entirely. */
    private String reconstructDynamicSqlFromConcatenation(String exprText) {
        if (exprText == null) return null;
        StringBuilder sql = new StringBuilder();
        for (String part : splitTopLevelPlus(exprText)) {
            String frag = unwrapWholeStringLiteral(part.trim());
            if (frag != null) sql.append(frag);
        }
        return sql.toString();
    }

    /** Splits text on top-level {@code '+'} characters, honouring string literals (with {@code ''}
     *  escapes) and nested parens so a {@code +} inside either does not split a single operand. */
    private List<String> splitTopLevelPlus(String text) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        StringBuilder cur = new StringBuilder();
        int i = 0;
        int len = text.length();
        while (i < len) {
            char ch = text.charAt(i);
            if (ch == '\'') {
                cur.append(ch);
                i++;
                while (i < len) {
                    cur.append(text.charAt(i));
                    if (text.charAt(i) == '\'') {
                        if (i + 1 < len && text.charAt(i + 1) == '\'') { i++; cur.append(text.charAt(i)); i++; continue; }
                        i++;
                        break;
                    }
                    i++;
                }
                continue;
            }
            if (ch == '(') { depth++; cur.append(ch); i++; continue; }
            if (ch == ')') { depth--; cur.append(ch); i++; continue; }
            if (ch == '+' && depth == 0) { out.add(cur.toString()); cur = new StringBuilder(); i++; continue; }
            cur.append(ch);
            i++;
        }
        if (cur.length() > 0) out.add(cur.toString());
        return out;
    }

    // ═════════════════════════════════════════════════════════════════════════
    // STRUCTURED (GRAMMAR-BASED) QUERY EXTRACTION  —  strangler-fig migration
    //
    // Stage 1 of moving SELECT/FROM/JOIN extraction off regex and onto a real ANTLR parse tree
    // (see the long design note above `queryUnit` in SqlServerParser.g4 for why the new rules are
    // a SEPARATE entry point rather than an alternative on `selectStatement`).
    //
    // The contract of this path is deliberately narrow and self-policing:
    //
    //   1. Re-parse the SELECT's own text through the `queryUnit` entry rule. Any syntax error at
    //      all — including "did not consume every token up to EOF" — means null, i.e. fall back.
    //   2. Gate the resulting tree to the subset the migration actually claims so far
    //      (isMigratedSubset) — Stage 1: plain SELECT/FROM/JOIN; Stage 2: table hints;
    //      Stage 3: CROSS/OUTER APPLY; Stage 4: bracket-quoted and 3-/4-part names;
    //      Stage 5: derived tables; Stage 6: WHERE/select-list subqueries; Stage 7: set operators.
    //      Anything outside it falls back, UNCHANGED, to the legacy regex scan.
    //   3. Inside the subset, reproduce the legacy path's OBSERVABLE BEHAVIOUR exactly — the same
    //      addTableOp calls in the same order, the same line attribution, the same alias/CTE/
    //      reserved-word handling — but derived from parse-tree nodes instead of text offsets.
    //
    // Point 3 is the whole reason the ~250-test suite is the regression contract: this stage is a
    // change of MECHANISM, not of behaviour. Behaviour changes (e.g. the FROM_PATTERN base-table
    // quirk on 3-part names, noted in structuredBaseTableName) are deferred to the stage that owns
    // them, so any test movement is attributable to one deliberate step.
    // ═════════════════════════════════════════════════════════════════════════

    /**
     * The legacy {@link #JOIN_PATTERN} bounds its ON-condition capture at 200 characters
     * ({@code [\s\S]{0,200}?}) purely to stop catastrophic regex backtracking; a longer condition
     * makes the whole join match fail and the edge is DROPPED OUTRIGHT.
     *
     * <p>This constant is retained only to document that legacy limit. It is no longer used as a
     * gate. Holding a long condition back to the legacy path was never a behaviour-preserving
     * choice — the legacy path does not produce a different edge for it, it produces NO edge at
     * all — so the gate's only effect was to propagate a regex implementation artefact into the
     * structured path as if it were a specification. The structured path parses the condition from
     * a real grammar node ({@code joinCondition}) with no backtracking and therefore no reason to
     * bound it: the limit is now simply not applied, and a long ON condition yields the join edge
     * it always should have. Verified as a pure gain against the full suite — nothing pinned the
     * dropped-edge behaviour.
     */
    private static final int LEGACY_MAX_JOIN_CONDITION_CHARS = 200;

    /**
     * Re-parses {@code selectText} through the dedicated {@code queryUnit} entry rule.
     *
     * <p>Returns null on ANY lexer or parser error. {@link BailErrorStrategy} is used so a failure
     * aborts immediately instead of letting ANTLR's default error recovery fabricate a plausible
     * but wrong tree — a recovered tree is exactly the kind of "succeeded by accident" result this
     * migration must never act on.
     */
    private SqlServerParser.QueryUnitContext parseQueryUnit(String selectText) {
        if (selectText == null || selectText.isEmpty()) return null;
        try {
            SqlServerParseErrorListener errors = new SqlServerParseErrorListener();
            SqlServerLexer lexer = new SqlServerLexer(CharStreams.fromString(selectText));
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);
            SqlServerParser parser = new SqlServerParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(errors);
            parser.setErrorHandler(new BailErrorStrategy());
            SqlServerParser.QueryUnitContext unit = parser.queryUnit();
            return errors.hasErrors() ? null : unit;
        } catch (RuntimeException ex) {
            return null;   // ParseCancellationException from BailErrorStrategy, or any lexer fault
        }
    }

    /**
     * Grammar-based replacement for {@code addTableOpsFromText(text) + attachJoins(text)} on a
     * SELECT, for the subset of queries Stage 1 has migrated.
     *
     * @return true when this path handled the query (the caller must then NOT run the regex path);
     *         false when the query is outside the migrated subset and the caller must fall back.
     */
    private boolean extractSelectStructurally(String selectText, int baseLine) {
        return extractQueryStructurally(selectText, baseLine) != null;
    }

    /**
     * As {@link #extractSelectStructurally}, but returns the walked {@code queryExpression} (or
     * null when the query fell outside the migrated subset and nothing was recorded).
     *
     * <p>Split out for Stage 8: an {@code UPDATE … FROM … JOIN} needs the extraction to happen AND
     * needs the resulting tree afterwards, to resolve its target alias back to a real table name.
     * A boolean cannot carry that, and re-parsing to get it would risk the two parses disagreeing.
     */
    private SqlServerParser.QueryExpressionContext extractQueryStructurally(String selectText,
                                                                           int baseLine) {
        SqlServerParser.QueryUnitContext unit = parseQueryUnit(selectText);
        if (unit == null) return null;
        SqlServerParser.QueryExpressionContext qe = unit.queryExpression();
        if (qe == null || qe.querySpecification().isEmpty()) return null;
        // EVERY top-level branch must have a FROM clause. This is load-bearing, not a tidy-up:
        // `SELECT @x` (no FROM) has to keep returning false so the caller's `fromClausePresent`
        // stays false and the "every SELECT records at least one op" placeholder still fires.
        // Returning true here would silently delete that placeholder op for every FROM-less
        // SELECT in the corpus. (A FROM-less branch nested INSIDE a subquery is fine and is
        // handled permissively — it just contributes nothing.)
        for (SqlServerParser.QuerySpecificationContext qs : qe.querySpecification()) {
            if (qs.fromClause() == null) return null;
        }
        if (!isMigratedSubset(qe, selectText)) return null;

        // The alias map is built ONCE over the WHOLE tree (outer query plus every derived table),
        // not per query specification. That mirrors the legacy path exactly: buildAliasMap() scans
        // the entire statement TEXT in one pass, so an inner query's ON condition could always
        // resolve against an outer alias and vice versa. Building one map per specification would
        // quietly break that for correlated conditions.
        java.util.Map<String, String> aliasMap = buildStructuredAliasMap(qe);
        processQueryExpression(qe, baseLine, aliasMap);
        return qe;
    }

    // ═════════════════════════════════════════════════════════════════════════
    // STAGE 8 — T-SQL `UPDATE … FROM … JOIN` / `DELETE … FROM … JOIN`
    // ═════════════════════════════════════════════════════════════════════════
    //
    // T-SQL allows an UPDATE or DELETE to take a full FROM clause, with joins:
    //
    //     UPDATE  t  SET t.n = s.n  FROM dbo.Target t JOIN dbo.Src s ON t.id = s.id  WHERE …
    //     DELETE  t                 FROM dbo.Target t JOIN dbo.Src s ON t.id = s.id
    //
    // The grammar models these as `UPDATE tableRef stmtTail` / `DELETE FROM? tableRef stmtTail`,
    // so everything from the SET clause onward — the entire FROM clause and all its joins — was
    // opaque token soup. Three separate things were wrong as a result:
    //
    //   1. The join edges were lost entirely (no FROM clause was ever analysed for these).
    //   2. The source tables (`dbo.Src`, and `dbo.Target` as a READ) were never recorded.
    //   3. Worse, the recorded TARGET was wrong. In this idiom the token after UPDATE/DELETE is
    //      conventionally the ALIAS, and the real table is named only in the FROM clause — so the
    //      op came out as a table literally called `t`. Not a missing dependency: a WRONG one,
    //      pointing at an object that does not exist.
    //
    // The approach is the same strangler-fig one the SELECT migration uses, and reuses its
    // machinery wholesale rather than duplicating it. The statement's own FROM clause is handed to
    // the SAME `queryUnit` nested parse by prefixing it with a synthetic `SELECT * `, which makes
    // it a syntactically complete query without altering a single character of the FROM clause
    // itself. Everything downstream — the migrated-subset gate, table ops, join edges, alias
    // resolution, derived tables, APPLY — then applies unchanged and automatically stays in step
    // with the SELECT path as later stages widen it.
    //
    // The synthetic prefix sits on the SAME LINE as the real FROM keyword, and the base line
    // passed in is the FROM keyword's own source line, so `structuredLine` attributes every table
    // to the line it truly occupies — the same alignment trick `visitSelectStatement` relies on.
    //
    // If ANY of this fails the gate, nothing at all happens: the target op that the caller already
    // recorded stands exactly as it did before, so a statement outside the subset is left in its
    // previous state rather than half-converted.

    /**
     * Stage 8 entry point: analyses the FROM clause of an UPDATE/DELETE, and corrects the target op
     * when the statement named its target by alias.
     *
     * @param tail      the statement's opaque tail (which contains the FROM clause, if any)
     * @param targetOp  the UPDATE/DELETE op the caller has already recorded
     */
    private void applyDmlFromClause(SqlServerParser.StmtTailContext tail,
                                    SqlServerTableOpInfo targetOp) {
        if (tail == null || targetOp == null) return;
        Token fromTok = topLevelFromToken(tail);
        if (fromTok == null) return;                 // plain UPDATE/DELETE — nothing to do

        CharStream cs = fromTok.getInputStream();
        if (cs == null) return;
        String fromText = cs.getText(
                Interval.of(fromTok.getStartIndex(), tail.getStop().getStopIndex()));

        // The synthetic select list is `*` and sits on the FROM keyword's own line, so line
        // attribution inside the nested parse lands on real source lines.
        SqlServerParser.QueryExpressionContext qe =
                extractQueryStructurally("SELECT * " + fromText, fromTok.getLine());
        if (qe == null) return;                      // outside the subset — target op left as-is

        retargetDmlOpFromAlias(qe, targetOp);
    }

    /**
     * The FROM keyword that introduces this statement's own FROM clause, or null if it has none.
     *
     * <p>Only a TOP-LEVEL FROM counts. A `stmtTail` is a flat list of `stmtToken`s in which a
     * parenthesised group (a subquery in the SET expression, an `IN (SELECT … FROM …)` in the
     * WHERE) is a single nested child — so a FROM belonging to such a subquery is NOT a direct
     * child here and is correctly skipped. Taking the first match is right for the same reason:
     * the statement's own FROM is the first one that is not nested inside anything.
     */
    private Token topLevelFromToken(SqlServerParser.StmtTailContext tail) {
        for (SqlServerParser.StmtTokenContext t : tail.stmtToken()) {
            if (t.getChildCount() == 1
                    && t.getChild(0) instanceof org.antlr.v4.runtime.tree.TerminalNode) {
                Token tok = ((org.antlr.v4.runtime.tree.TerminalNode) t.getChild(0)).getSymbol();
                if (tok.getType() == SqlServerLexer.FROM) return tok;
            }
        }
        return null;
    }

    /**
     * Rewrites an UPDATE/DELETE target that was named by ALIAS to the real table it aliases.
     *
     * <p>`UPDATE t SET … FROM dbo.Target t JOIN …` targets dbo.Target; `t` is only an alias bound
     * by the FROM clause. The op is corrected in place, preserving the source's own CASING (the
     * shared alias map is uppercased, so the matching source item is located in the tree instead
     * and its text used verbatim).
     *
     * <p>Deliberately narrow, because the same syntax position can legitimately hold a real table:
     * only an UNQUALIFIED target with no schema is considered, and only when it matches an alias
     * actually declared in this FROM clause. `UPDATE dbo.Target SET … FROM dbo.Target t JOIN …`
     * names its target properly and is left untouched.
     */
    private void retargetDmlOpFromAlias(SqlServerParser.QueryExpressionContext qe,
                                        SqlServerTableOpInfo targetOp) {
        String target = targetOp.getTableName();
        if (target == null || targetOp.getSchemaName() != null) return;

        for (SqlServerParser.QuerySpecificationContext qs : qe.querySpecification()) {
            if (qs.fromClause() == null) continue;
            for (SqlServerParser.TableSourceContext ts : qs.fromClause().tableSource()) {
                SqlServerParser.TableSourceItemContext hit = matchAliasedSource(ts, target);
                if (hit == null) continue;
                String[] parts = structuredNameParts(hit.qualifiedTableName());
                targetOp.setTableName(parts[parts.length - 1]);
                if (parts.length >= 2) targetOp.setSchemaName(parts[parts.length - 2]);
                targetOp.setAlias(target);
                return;
            }
        }
    }

    /** The source in this FROM source (base or any joined one) whose alias is {@code alias}. */
    private SqlServerParser.TableSourceItemContext matchAliasedSource(
            SqlServerParser.TableSourceContext ts, String alias) {
        SqlServerParser.TableSourceItemContext base = ts.tableSourceItem();
        if (aliasMatches(base, alias)) return base;
        for (SqlServerParser.JoinClauseContext jc : ts.joinClause()) {
            if (!(jc instanceof SqlServerParser.JoinOnContext)) continue;
            SqlServerParser.TableSourceItemContext it =
                    ((SqlServerParser.JoinOnContext) jc).tableSourceItem();
            if (aliasMatches(it, alias)) return it;
        }
        return null;
    }

    /**
     * Whether this source carries {@code alias} AND has a real table name to retarget onto — a
     * derived table or a function call has none, so such a source can never resolve a target.
     */
    private boolean aliasMatches(SqlServerParser.TableSourceItemContext item, String alias) {
        if (item.qualifiedTableName() == null || isCallSource(item)) return false;
        String a = structuredAlias(item);
        return a != null && a.equalsIgnoreCase(alias);
    }

    /**
     * Stage 7: every branch of a UNION / UNION ALL / INTERSECT / EXCEPT, in source order.
     *
     * <p>A single-branch query is just the degenerate case, so all the earlier stages flow through
     * here unchanged. Source order matters for the same reason it did in Stage 6: the legacy path
     * scanned the statement text left to right, so branch one's tables were recorded before branch
     * two's.
     */
    private void processQueryExpression(SqlServerParser.QueryExpressionContext qe, int baseLine,
                                        java.util.Map<String, String> aliasMap) {
        for (SqlServerParser.QuerySpecificationContext qs : qe.querySpecification()) {
            processQuerySpecification(qs, baseLine, aliasMap);
        }
    }

    /**
     * Records the table ops and join edges of ONE query specification, recursing into any derived
     * table in its FROM clause (Stage 5).
     *
     * <p>Sources are visited in textual order — the base source, then each joined source — which is
     * the same order TABLE_REF_PATTERN's successive {@code find()} calls produced, so the resulting
     * {@code tableOps} list keeps the legacy ordering.
     */
    private void processQuerySpecification(SqlServerParser.QuerySpecificationContext qs,
                                           int baseLine,
                                           java.util.Map<String, String> aliasMap) {
        // Stage 6: a scalar subquery in the SELECT LIST is processed BEFORE the FROM clause, and a
        // WHERE/HAVING subquery AFTER the joins. That is not cosmetic — it reproduces the legacy
        // path's TEXTUAL order. addTableOpsFromText scanned the statement text left to right, so
        // in `SELECT (SELECT … FROM S), a FROM A` the op for S was recorded before the op for A.
        // Processing structurally in tree order instead would silently reverse the tableOps list.
        for (SqlServerParser.SubqueryContext sub : selectListSubqueries(qs)) {
            processSubquery(sub, baseLine, aliasMap);
        }

        // A branch with no FROM clause at all (`SELECT 1`, a UNION branch of bare literals)
        // contributes no tables and no joins — but its subqueries, handled either side of this
        // block, still do.
        if (qs.fromClause() == null) {
            for (SqlServerParser.SubqueryContext sub : queryRestSubqueries(qs)) {
                processSubquery(sub, baseLine, aliasMap);
            }
            return;
        }
        // Comma-separated FROM sources (`FROM a, b`) are each processed, in source order.
        //
        // This is the one place the structured path records something the legacy path MISSED
        // rather than merely recording it by a different mechanism. TABLE_REF_PATTERN is anchored
        // on `\b(?:FROM|JOIN)\s+<name>`, and in `FROM a, b` only `a` follows a FROM keyword — `b`
        // follows a comma — so the legacy scan recorded `a` and silently dropped `b`. That is a
        // plain omission of a real table dependency (an old-style implicit-join query loses half
        // its lineage), not a defensible alternative reading, so it is fixed here rather than
        // reproduced. Each source's op is still anchored on the single FROM keyword for its line,
        // which is what the legacy path did for the one source it did capture.
        for (SqlServerParser.TableSourceContext ts : qs.fromClause().tableSource()) {
            // The FROM table's op line is the line of the FROM keyword, because TABLE_REF_PATTERN
            // anchors its match on `\b(?:FROM|JOIN)` — not on the table name that follows it.
            processTableSourceItem(ts.tableSourceItem(), baseLine,
                    qs.fromClause().FROM().getSymbol(), aliasMap);
            for (SqlServerParser.JoinClauseContext jc : ts.joinClause()) {
                // Only a real JOIN contributes a table op. An APPLY source deliberately does NOT:
                // TABLE_REF_PATTERN is anchored on `\b(?:FROM|JOIN)`, so the legacy path never
                // recorded a table for a CROSS/OUTER APPLY — it only ever produced a join EDGE for
                // it (and the applied source is usually a function, already in the call graph).
                if (!(jc instanceof SqlServerParser.JoinOnContext)) continue;
                SqlServerParser.JoinOnContext on = (SqlServerParser.JoinOnContext) jc;
                // Likewise, a joined table's op line comes from the JOIN keyword, NOT from a
                // preceding INNER/LEFT/… qualifier — TABLE_REF_PATTERN cannot see the qualifier at
                // all. (The SqlServerJoinInfo's own line, set below, DOES start at the qualifier,
                // because JOIN_PATTERN anchors there. The two differ by design; both preserved.)
                processTableSourceItem(on.tableSourceItem(), baseLine,
                        on.JOIN().getSymbol(), aliasMap);
            }
        }

        // Joins are attached in a SECOND pass over the sources, after every source's table op
        // exists. Ordering matters: attachJoinsStructurally resolves each edge onto a base table
        // op via findOrCreateSelectOp, and interleaving the two passes would let a join edge
        // CREATE an op for a comma-sibling that the first pass was about to record properly
        // (with its alias), leaving a duplicate, alias-less op behind.
        for (SqlServerParser.TableSourceContext ts : qs.fromClause().tableSource()) {
            attachJoinsStructurally(ts, baseLine, aliasMap);
        }

        // Stage 6: WHERE / GROUP BY / HAVING / ORDER BY subqueries, after the joins — again
        // matching the legacy left-to-right text scan order.
        for (SqlServerParser.SubqueryContext sub : queryRestSubqueries(qs)) {
            processSubquery(sub, baseLine, aliasMap);
        }
    }

    /** A parenthesised subquery in an expression position → its inner query, walked as usual. */
    private void processSubquery(SqlServerParser.SubqueryContext sub, int baseLine,
                                 java.util.Map<String, String> aliasMap) {
        SqlServerParser.QueryExpressionContext inner = sub == null ? null : sub.queryExpression();
        // A FROM-less subquery (`WHERE EXISTS (SELECT 1)`) contributes nothing; the per-branch
        // guard inside processQuerySpecification handles it.
        if (inner != null) processQueryExpression(inner, baseLine, aliasMap);
    }

    /**
     * The scalar subqueries directly in a SELECT list.
     *
     * <p>Only DIRECT children are collected, and that is correct rather than a shortcut: a
     * subquery nested inside another subquery is reached by recursing through
     * {@link #processQuerySpecification} on the outer one, so collecting it here as well would
     * process it — and record its tables — twice.
     */
    private List<SqlServerParser.SubqueryContext> selectListSubqueries(
            SqlServerParser.QuerySpecificationContext qs) {
        List<SqlServerParser.SubqueryContext> out = new ArrayList<>();
        if (qs.selectList() == null) return out;
        for (SqlServerParser.SelectListTokenContext t : qs.selectList().selectListToken()) {
            if (t.subquery() != null) out.add(t.subquery());
        }
        return out;
    }

    /** The subqueries directly in the trailing WHERE / GROUP BY / HAVING / ORDER BY clauses. */
    private List<SqlServerParser.SubqueryContext> queryRestSubqueries(
            SqlServerParser.QuerySpecificationContext qs) {
        List<SqlServerParser.SubqueryContext> out = new ArrayList<>();
        if (qs.queryRest() == null) return out;
        for (SqlServerParser.QueryRestTokenContext t : qs.queryRest().queryRestToken()) {
            if (t.subquery() != null) out.add(t.subquery());
        }
        return out;
    }

    /**
     * One FROM/JOIN source. A plain named table becomes a table op directly; a DERIVED TABLE
     * (Stage 5) contributes no op of its own — it has no table name — and is instead recursed
     * into, so the tables and joins of its inner SELECT are extracted by the same code at any
     * nesting depth.
     *
     * <p>Contributing no op for the derived table itself is what the legacy regex path did too:
     * TABLE_REF_PATTERN is anchored on {@code \b(?:FROM|JOIN)\s+<name>}, and {@code FROM (} has no
     * name to capture, so the derived table never produced a row — only the inner query's own
     * FROM/JOIN matches did.
     */
    private void processTableSourceItem(SqlServerParser.TableSourceItemContext item,
                                        int baseLine, Token anchor,
                                        java.util.Map<String, String> aliasMap) {
        SqlServerParser.QueryExpressionContext inner = derivedQueryExpression(item);
        if (inner != null) {
            processQueryExpression(inner, baseLine, aliasMap);
            return;
        }
        recordStructuredTableOp(item, baseLine, anchor);
    }

    /** The inner query of a derived-table source, or null for a plain named table. */
    private SqlServerParser.QueryExpressionContext derivedQueryExpression(
            SqlServerParser.TableSourceItemContext item) {
        return item.derivedTable() == null ? null : item.derivedTable().queryExpression();
    }

    /**
     * Whether a FROM/JOIN source is a CALL (table-valued or rowset function) rather than a named
     * table — structurally, by the presence of the argument list that {@code tableSourceItem} now
     * parses.
     *
     * <p>A derived table has no {@code qualifiedTableName} at all and is not a call.
     */
    private boolean isCallSource(SqlServerParser.TableSourceItemContext item) {
        return item.qualifiedTableName() != null && item.parenGroup() != null;
    }

    /** One FROM/JOIN source → one SELECT table op, mirroring addTableOpsFromText's per-match work. */
    private void recordStructuredTableOp(SqlServerParser.TableSourceItemContext item,
                                         int baseLine, Token anchor) {
        // A table-valued / rowset function call names a FUNCTION, not a table. Recording an op for
        // it would fabricate a table literally called GetOrders / OPENQUERY — precisely the
        // phantom the legacy looksLikeCallAt() guard exists to suppress, and suppressed here too.
        // The dependency is not lost: extractQualifiedCalls() already records the call edge.
        if (isCallSource(item)) return;
        String[] parts = structuredNameParts(item.qualifiedTableName());
        String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
        String name   = parts[parts.length - 1];
        String alias  = structuredAlias(item);
        if (alias != null && RESERVED_NON_TABLE.contains(alias.toUpperCase())) alias = null;
        SqlServerTableOpInfo added = addTableOp(schema, name,
                structuredLine(anchor, baseLine), SqlServerTableOpInfo.DmlType.SELECT);
        if (added != null && alias != null) added.setAlias(alias);
    }

    /** Grammar-based counterpart of {@link #attachJoins} for a single, already-gated FROM clause. */
    private void attachJoinsStructurally(SqlServerParser.TableSourceContext ts, int baseLine,
                                         java.util.Map<String, String> aliasMap) {
        // Every join in this FROM clause hangs off the single base source, so the "nearest
        // preceding FROM" that baseTableBefore() computes textually is, structurally, always this.
        String base = structuredBaseTableName(ts.tableSourceItem());
        // A base that is a table-valued / rowset FUNCTION call owns no table op, for the same
        // reason a CTE or a table-valued parameter does not: it is not a table. Without this the
        // edge would be attached via findOrCreateSelectOp(base), which would CREATE the very
        // phantom op ("GetOrders", "OPENQUERY") that recordStructuredTableOp just declined to.
        boolean baseIsOpless = isCallSource(ts.tableSourceItem());

        for (SqlServerParser.JoinClauseContext jc : ts.joinClause()) {
            SqlServerJoinInfo j = new SqlServerJoinInfo();
            SqlServerParser.QualifiedTableNameContext joinedName;
            String alias;

            if (jc instanceof SqlServerParser.JoinOnContext) {
                SqlServerParser.JoinOnContext on = (SqlServerParser.JoinOnContext) jc;
                // JOIN (SELECT …) d ON … — a derived table on the RIGHT of a join produces no
                // edge, because there is no joined-table NAME to name it by. The legacy path did
                // the same for the same reason: JOIN_PATTERN's joined-table group requires an
                // identifier, and `JOIN (` has none, so the match failed and no edge was emitted.
                // The derived table's own inner tables are still extracted — processQuerySpecification
                // recursed into it before this loop ran.
                if (on.tableSourceItem().qualifiedTableName() == null) continue;
                // A bare JOIN (no qualifier) is the ANSI-standard synonym for INNER JOIN.
                j.setJoinType(on.joinTypeSpec() != null
                        ? normalizeKeywordSpacing(originalText(on.joinTypeSpec()))
                        : "INNER");
                joinedName = on.tableSourceItem().qualifiedTableName();
                alias = structuredAlias(on.tableSourceItem());
                if (on.joinCondition() != null) {
                    // stripCommentsOnly is LENGTH-PRESERVING (a comment becomes an equally long run
                    // of spaces), so for the overwhelmingly common comment-free, literal-free
                    // condition this is the identity and the emitted string is exactly the raw text.
                    // For a condition carrying a COMMENT it still reproduces attachJoins()
                    // character-for-character, since that path blanks comments over the whole
                    // statement text before JOIN_PATTERN captures the condition.
                    //
                    // It deliberately does NOT mask STRING LITERALS, which is where this path now
                    // diverges from — and improves on — the legacy one. The legacy path emits
                    // `ON a.status = '      '` for `ON a.status = 'ACTIVE'`, because it must blank
                    // literal contents before letting a regex loose on the text. That blanked
                    // artefact is not a specification anyone should inherit: it is lossy, it is
                    // invisible to the reader of the emitted condition, and it exists only to
                    // protect a scanner this path does not use. The structured path reads the
                    // condition from a real `joinCondition` node in which the literal is a single
                    // token that can never be mistaken for a keyword, so it emits the TRUE text.
                    j.setCondition(stripCommentsOnly(originalText(on.joinCondition())).trim());
                }
            } else {
                // CROSS APPLY / OUTER APPLY. The legacy JOIN_PATTERN spelled the type as the
                // qualifier plus a literal " APPLY", and an APPLY never carries an ON condition,
                // so the condition stays null — both reproduced here.
                SqlServerParser.ApplyJoinContext ap = (SqlServerParser.ApplyJoinContext) jc;
                j.setJoinType((ap.CROSS() != null ? "CROSS" : "OUTER") + " APPLY");
                joinedName = ap.applySource().qualifiedTableName();
                alias = ap.applySource().tableAlias() == null
                        ? null
                        : stripBrackets(ap.applySource().tableAlias().namePart().getText());
            }

            String[] parts = splitQualifiedName(joinedName.getText());
            if (parts.length >= 2) j.setJoinedTableSchema(parts[parts.length - 2]);
            j.setJoinedTable(parts[parts.length - 1]);
            // Unlike the table-op path, the raw alias is kept as-is here: attachJoins() never
            // filtered a join's alias through RESERVED_NON_TABLE.
            if (alias != null) j.setJoinedTableAlias(alias);

            // JOIN_PATTERN anchors on the optional qualifier, so the edge's line is the qualifier's
            // line when one is present, and the JOIN keyword's line otherwise.
            j.setLine(structuredLine(jc.getStart(), baseLine));
            j.setQueryId(baseLine);

            if (j.getJoinedTable() == null
                    || cteNames.contains(j.getJoinedTable().toUpperCase())) continue;
            resolveLeftSide(j, aliasMap);
            if (base == null) continue;
            SqlServerTableOpInfo baseOp = (baseIsOpless || baseOwnsNoTableOp(base))
                    // Chained-CTE-join fix: the base is a CTE, which deliberately has no table op
                    // of its own, so the edge is attached to the JOINED table's op instead of being
                    // dropped. leftTable/leftTableAlias still name the CTE as the structural base.
                    ? findOrCreateSelectOp(j.getJoinedTable(), j.getLine())
                    : findOrCreateSelectOp(base, j.getLine());
            baseOp.getJoins().add(j);
        }
    }

    /**
     * Whether a FROM-clause base source legitimately has NO table op of its own, so a join hanging
     * off it must be attached to the JOINED table's op rather than to the base's.
     *
     * <p>Two cases, and they are the same case: a CTE name and a table-valued PARAMETER are both
     * deliberately filtered out by {@link #addTableOp} because neither is a base table. Calling
     * {@code findOrCreateSelectOp} on either would therefore not FIND an op — it would CREATE one,
     * fabricating exactly the phantom row the filtering existed to prevent.
     *
     * <p>A table VARIABLE (`DECLARE @tbl TABLE(...)`) is deliberately NOT included: addTableOp
     * keeps it (it genuinely is a table source), so it owns a real op and the edge attaches there
     * normally.
     */
    private boolean baseOwnsNoTableOp(String base) {
        String upper = base.toUpperCase();
        return cteNames.contains(upper) || paramNames.contains(upper);
    }

    /** Structural equivalent of {@link #buildAliasMap}: alias → table, and table → table. */
    private java.util.Map<String, String> buildStructuredAliasMap(
            SqlServerParser.QueryExpressionContext qe) {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        collectStructuredAliases(m, qe);
        return m;
    }

    /** Every branch of a set operation contributes its aliases to the one shared map. */
    private void collectStructuredAliases(java.util.Map<String, String> m,
                                          SqlServerParser.QueryExpressionContext qe) {
        if (qe == null) return;
        for (SqlServerParser.QuerySpecificationContext qs : qe.querySpecification()) {
            collectStructuredAliases(m, qs);
        }
    }

    /**
     * Collects alias entries for a query specification AND every query nested inside it — derived
     * tables, select-list subqueries and WHERE/HAVING subqueries alike.
     *
     * <p>One map for the whole tree, because the legacy {@link #buildAliasMap} scanned the entire
     * statement TEXT in a single pass: a correlated subquery's ON/WHERE condition could always
     * resolve an outer alias, and an outer condition could resolve an inner one. Scoping the map
     * per query specification would be more "correct" SQL-wise but would change which joins get a
     * resolved {@code leftTable}, which is a behaviour change this stage does not own.
     */
    private void collectStructuredAliases(java.util.Map<String, String> m,
                                          SqlServerParser.QuerySpecificationContext qs) {
        if (qs.fromClause() != null) {
            // Every comma-separated source contributes its aliases, not just the first — an
            // implicit-join query (`FROM a, b WHERE a.id = b.aid`) is precisely the shape whose
            // conditions need BOTH aliases resolvable.
            for (SqlServerParser.TableSourceContext ts : qs.fromClause().tableSource()) {
                collectStructuredAliases(m, ts);
            }
        }
        for (SqlServerParser.SubqueryContext sub : selectListSubqueries(qs)) {
            collectStructuredAliases(m, sub.queryExpression());
        }
        for (SqlServerParser.SubqueryContext sub : queryRestSubqueries(qs)) {
            collectStructuredAliases(m, sub.queryExpression());
        }
    }

    private void collectStructuredAliases(java.util.Map<String, String> m,
                                          SqlServerParser.TableSourceContext ts) {
        addStructuredAliasEntry(m, ts.tableSourceItem());
        for (SqlServerParser.JoinClauseContext jc : ts.joinClause()) {
            // APPLY sources are intentionally absent: the legacy ALIAS_TABLE_PATTERN is anchored
            // on `\b(?:FROM|JOIN)`, so an APPLY's alias was never in the map, and resolveLeftSide
            // therefore never resolved a left side through one. Kept identical.
            if (jc instanceof SqlServerParser.JoinOnContext) {
                addStructuredAliasEntry(m, ((SqlServerParser.JoinOnContext) jc).tableSourceItem());
            }
        }
    }

    private void addStructuredAliasEntry(java.util.Map<String, String> m,
                                         SqlServerParser.TableSourceItemContext item) {
        SqlServerParser.QueryExpressionContext inner = derivedQueryExpression(item);
        if (inner != null) {
            // A derived table contributes its INNER sources' entries, and NOT its own alias.
            //
            // That asymmetry is the legacy behaviour, reproduced deliberately rather than
            // "improved": ALIAS_TABLE_PATTERN is anchored on `\b(?:FROM|JOIN)\s+<name>`, so
            // `FROM (SELECT … FROM Orders) AS d` put ORDERS in the map but never `d` — and
            // resolveLeftSide therefore could not resolve a left side through a derived-table
            // alias. Mapping `d` here would be a real behaviour change (join edges would start
            // acquiring a leftTable they never had), which belongs to whichever stage owns it.
            collectStructuredAliases(m, inner);
            return;
        }
        String[] parts = structuredNameParts(item.qualifiedTableName());
        String table = parts[parts.length - 1].toUpperCase();
        String alias = structuredAlias(item);
        if (alias != null && !ALIAS_STOPWORDS.contains(alias.toUpperCase())) {
            m.put(alias.toUpperCase(), table);
        }
        m.put(table, table);
    }

    /**
     * The base-table name a join is attached to — the structural equivalent of
     * {@link #baseTableBefore}.
     *
     * <p>This is the one place Stage 4 deliberately IMPROVES on the legacy behaviour rather than
     * merely reproducing it. {@link #FROM_PATTERN} caps at two dotted segments and reports the
     * second, so for {@code FROM LinkedSrv.OtherDb.dbo.Orders o … JOIN …} it returns
     * {@code OtherDb} — a database qualifier — and the join edge was attached to a phantom table
     * op of that name instead of to {@code Orders}. The structured path takes the last segment,
     * which is always the real object, so the edge lands on the table it actually belongs to.
     *
     * <p>This was verified to be a pure gain, not a silent behaviour swap: the full suite was
     * green before and after the gate was widened, so no test pinned the old two-segment result.
     */
    private String structuredBaseTableName(SqlServerParser.TableSourceItemContext item) {
        SqlServerParser.QueryExpressionContext inner = derivedQueryExpression(item);
        if (inner != null) {
            // The base of a join whose left side is a DERIVED TABLE resolves to the derived
            // table's OWN base table, recursively. Again this reproduces the legacy path rather
            // than reinventing it: baseTableBefore() took the LAST FROM_PATTERN match before the
            // join, and since `FROM (` never matches, that was the innermost query's FROM — i.e.
            // exactly this table. So `FROM (SELECT … FROM Orders) d JOIN Cust c ON …` attaches the
            // edge to Orders, both before and after this stage.
            // The LAST branch, not the first: baseTableBefore() took the LAST FROM_PATTERN match
            // before the join, so for `FROM (SELECT … FROM A UNION SELECT … FROM B) d JOIN C`
            // the legacy base was B. Reproduced exactly.
            List<SqlServerParser.QuerySpecificationContext> branches = inner.querySpecification();
            if (branches.isEmpty()) return null;
            SqlServerParser.QuerySpecificationContext last = branches.get(branches.size() - 1);
            if (last.fromClause() == null) return null;
            return structuredBaseTableName(last.fromClause().tableSource(0).tableSourceItem());
        }
        String[] parts = structuredNameParts(item.qualifiedTableName());
        return parts[parts.length - 1];
    }

    /** Dotted name → plain identifier segments, brackets stripped (shared with the regex path). */
    private String[] structuredNameParts(SqlServerParser.QualifiedTableNameContext name) {
        return splitQualifiedName(name.getText());
    }

    private String structuredAlias(SqlServerParser.TableSourceItemContext item) {
        return item.tableAlias() == null ? null : stripBrackets(item.tableAlias().namePart().getText());
    }

    /**
     * Absolute source line for a token of the NESTED parse.
     *
     * <p>The nested parser sees only the SELECT's own text, so its lines restart at 1. The caller
     * passes that text starting at the SELECT keyword with no leading newline, which makes this
     * offset identical to what {@link #lineAt} computes for the regex path on the same text.
     */
    private int structuredLine(Token token, int baseLine) {
        return baseLine + token.getLine() - 1;
    }

    private String normalizeKeywordSpacing(String s) {
        return s.replaceAll("\\s+", " ").toUpperCase();
    }

    /**
     * Whether a successfully-parsed query is inside the subset Stage 1 claims to have migrated.
     *
     * <p>Everything excluded here still works — it simply goes down the legacy regex path,
     * unchanged. Each exclusion is a construct owned by a LATER stage, and lifting one is that
     * stage's deliverable. Being conservative here is what makes the migration incremental rather
     * than an all-or-nothing rewrite.
     */
    private boolean isMigratedSubset(SqlServerParser.QueryExpressionContext qe, String selectText) {
        int accounted = countMigratedQueryExpression(qe, selectText);
        if (accounted < 0) return false;

        // Defence in depth: every FROM/JOIN keyword in the text must be one this tree accounts for.
        // If the token stream holds a FROM or JOIN the structured walk does not visit (e.g. buried
        // inside a parenthesised group the permissive sub-rules swallowed — a WHERE-clause
        // subquery, say, which is Stage 6), the regex path would have recorded a table the
        // structured path is about to miss, so fall back instead. Counting RECURSIVELY (derived
        // tables included) is what keeps this guard honest now that the walk itself recurses.
        return countFromJoinTokens(selectText) == accounted;
    }

    /**
     * Validates one query specification against the migrated subset and returns how many FROM/JOIN
     * KEYWORD TOKENS the structured walk will account for in it (recursing into derived tables), or
     * -1 if it contains anything outside the subset.
     *
     * <p>Validation and counting are fused deliberately: they have to walk exactly the same nodes,
     * and keeping them in one traversal makes it impossible for the gate to accept a construct the
     * token-count guard then fails to account for (or vice versa).
     *
     * <p>Everything rejected here still works — it simply goes down the legacy regex path,
     * unchanged. Each exclusion is a construct owned by a LATER stage.
     */
    private int countMigratedFromJoin(SqlServerParser.QuerySpecificationContext qs, String selectText) {
        SqlServerParser.FromClauseContext from = qs.fromClause();
        if (from == null) return -1;

        // Comma-separated FROM sources (`FROM a, b`) are now MIGRATED rather than held back.
        //
        // The old note here said reproducing the legacy behaviour was pointless and fixing it was
        // a behaviour change, so the whole query was pushed to the regex path. But pushing it to
        // the regex path IS choosing the broken behaviour — the legacy scan records `a` and drops
        // `b` outright. There is no third option in which the query is left untouched, so the
        // choice was only ever between recording one table and recording both. See
        // processQuerySpecification for the fix; the gate below now just validates every source.
        int n = 1;                                   // this clause's own FROM keyword
        for (SqlServerParser.TableSourceContext tsAny : from.tableSource()) {
            int sub = countMigratedTableSource(tsAny, selectText);
            if (sub < 0) return -1;
            n += sub;
        }

        // Stage 6: every subquery in the select list and in the trailing clauses is walked too,
        // so its own FROM/JOIN keywords are accounted for here. Without this the token-count
        // guard below would see "more FROM keywords than the tree explains" and fall back —
        // which is exactly how these queries behaved before this stage.
        for (SqlServerParser.SubqueryContext sub : selectListSubqueries(qs)) {
            int s = countMigratedSubquery(sub, selectText);
            if (s < 0) return -1;
            n += s;
        }
        for (SqlServerParser.SubqueryContext sub : queryRestSubqueries(qs)) {
            int s = countMigratedSubquery(sub, selectText);
            if (s < 0) return -1;
            n += s;
        }
        return n;
    }

    /**
     * Validates ONE comma-separated FROM source — its own table/derived-table item plus every join
     * hanging off it — and returns the number of FROM/JOIN keyword tokens it accounts for BEYOND
     * the single FROM keyword that introduces the whole clause (so a plain `FROM a` source
     * contributes 0, and each JOIN on it contributes 1).
     *
     * <p>Split out of {@link #countMigratedFromJoin} when comma-separated sources were migrated:
     * a FROM clause now has N sources rather than exactly one, and each needs identical validation.
     */
    private int countMigratedTableSource(SqlServerParser.TableSourceContext ts, String selectText) {
        int n = 0;
        int baseSub = countMigratedSource(ts.tableSourceItem(), selectText);
        if (baseSub < 0) return -1;
        n += baseSub;

        for (SqlServerParser.JoinClauseContext jcAny : ts.joinClause()) {
            if (jcAny instanceof SqlServerParser.ApplyJoinContext) {
                if (!isMigratedApplySource(((SqlServerParser.ApplyJoinContext) jcAny).applySource())) {
                    return -1;
                }
                continue;   // an APPLY contributes no JOIN token and has no ON condition
            }
            SqlServerParser.JoinOnContext jc = (SqlServerParser.JoinOnContext) jcAny;
            n++;                                     // this join's JOIN keyword
            int joinedSub = countMigratedSource(jc.tableSourceItem(), selectText);
            if (joinedSub < 0) return -1;
            n += joinedSub;
            if (jc.joinCondition() != null) {
                // NOTE: no length gate. See LEGACY_MAX_JOIN_CONDITION_CHARS — the legacy 200-char
                // bound is a regex-backtracking artefact that DROPS the edge, not an alternative
                // rendering of it, so deferring to it here only reproduced a loss.
                // attachJoins() scans text whose comments are blanked and whose string-literal
                // CONTENTS are masked to spaces, so a condition containing either yields a
                // different condition STRING there than the raw parse-tree text does here.
                //
                // Style-variance audit: a COMMENT inside the ON condition — `ON c.id = /*pk*/ a.cid`,
                // or a `--` note on a continuation line of a multi-line condition — is an extremely
                // common real-world formatting habit, and rejecting it here meant the whole query
                // silently dropped to the regex path for a purely cosmetic reason. It is now
                // accepted: attachJoinsStructurally runs the SAME length-preserving
                // stripLineAndBlockComments() over the condition text that the legacy path's input
                // had already been through, so the emitted condition STRING is byte-identical to
                // what the regex path produced — the masking artefacts are reproduced exactly
                // rather than avoided, which is what keeps this a mechanism change and not a
                // behaviour change.
                //
                // A STRING LITERAL in the condition is now ACCEPTED too, and is the one place this
                // path deliberately emits something DIFFERENT from the legacy path rather than
                // identical to it. Holding it back was justified on the grounds that reproducing
                // maskStringLiterals' blanked literal was undesirable — but the conclusion drawn
                // from that was the wrong one: the alternative to reproducing the artefact is not
                // falling back to the path that PRODUCES the artefact, it is not producing it.
                // attachJoinsStructurally now uses stripCommentsOnly (see the note there), so the
                // emitted condition carries the literal's real text. That is strictly more
                // information than `'      '` and is owned here as an intentional improvement.
            }
        }
        return n;
    }

    private int countMigratedSubquery(SqlServerParser.SubqueryContext sub, String selectText) {
        return countMigratedQueryExpression(sub.queryExpression(), selectText);
    }

    /**
     * Stage 7: validates and counts every branch of a set operation.
     *
     * <p>A branch with no FROM clause (`SELECT 1`, `WHERE EXISTS (SELECT 1)`) contributes no
     * tables and no FROM keyword, so it is trivially inside the subset and counts zero — it must
     * NOT reject the whole query.
     */
    private int countMigratedQueryExpression(SqlServerParser.QueryExpressionContext qe,
                                             String selectText) {
        if (qe == null) return -1;
        List<SqlServerParser.QuerySpecificationContext> branches = qe.querySpecification();
        if (branches.isEmpty()) return -1;
        int n = 0;
        for (SqlServerParser.QuerySpecificationContext qs : branches) {
            if (qs.fromClause() == null) continue;
            int c = countMigratedFromJoin(qs, selectText);
            if (c < 0) return -1;
            n += c;
        }
        return n;
    }

    /**
     * Validates ONE FROM/JOIN source and returns the number of FROM/JOIN tokens it contributes
     * BEYOND the keyword that introduces it (0 for a plain table; the inner query's own count for
     * a derived table), or -1 when the source is outside the migrated subset.
     */
    private int countMigratedSource(SqlServerParser.TableSourceItemContext item, String selectText) {
        SqlServerParser.QueryExpressionContext inner = derivedQueryExpression(item);
        if (inner == null) {
            return isMigratedSource(item, selectText) ? 0 : -1;
        }
        // Stage 5: a derived table. Its alias is mandatory in the grammar; hold it to the same
        // shape the legacy alias groups (`[A-Za-z_]\w*`, never bracketed) could capture, so the
        // structured path cannot start recognising aliases the regex path never could.
        String alias = structuredAlias(item);
        if (alias == null || !alias.matches("[A-Za-z_]\\w*")) return -1;
        if (item.tableAlias().namePart().BRACKET_ID() != null) return -1;
        return countMigratedQueryExpression(inner, selectText);
    }

    /**
     * Whether a CROSS/OUTER APPLY source is within the migrated subset.
     *
     * <p>Looser than {@link #isMigratedSource} in exactly one respect: an argument list IS allowed,
     * because a parameterised table-valued function (`dbo.GetOrderLines(o.id) ol`) and an XML
     * data-type method (`x.nodes('/a/b') AS t(c)`) are the two normal right-hand sides of an APPLY.
     * The legacy JOIN_PATTERN recorded the FUNCTION's own name as the joined table for these, and
     * {@link #attachJoinsStructurally} reproduces that; no table op is produced either way.
     */
    private boolean isMigratedApplySource(SqlServerParser.ApplySourceContext src) {
        List<SqlServerParser.NamePartContext> parts = src.qualifiedTableName().namePart();
        if (src.qualifiedTableName().AT_VAR() != null) return false;
        if (parts.size() < 1 || parts.size() > 4) return false;      // Stage 4: 3-/4-part allowed
        if (src.tableAlias() != null) {
            if (src.tableAlias().namePart().BRACKET_ID() != null) return false;
            // The legacy alias group for a JOIN/APPLY is `[A-Za-z]\w*`.
            if (!src.tableAlias().namePart().getText().matches("[A-Za-z]\\w*")) return false;
        }
        return true;
    }

    /** A FROM/JOIN source is migrated iff it is a (possibly bracket-quoted) 1-to-4-part name. */
    private boolean isMigratedSource(SqlServerParser.TableSourceItemContext item, String selectText) {
        SqlServerParser.QualifiedTableNameContext name = item.qualifiedTableName();
        // A source that is really a CALL, not a table: a table-valued function
        // (`FROM dbo.GetOrders(@id) t`) or a rowset function (`FROM OPENQUERY(srv, '…') x`,
        // `FROM OPENROWSET(…) x`).
        //
        // `tableSourceItem` now parses the argument list itself (see the note on that rule), so
        // this is recognised STRUCTURALLY — `item.parenGroup() != null` — instead of by the
        // textual lookahead. Such a source is ACCEPTED into the migrated subset but still records
        // NO table op (see recordStructuredTableOp / isCallSource), so the phantom named
        // GetOrders / OPENQUERY / OPENROWSET that looksLikeCallAt() was added to prevent is still
        // prevented, by construction rather than by scanning. The call itself is already in the
        // call graph via QUALIFIED_CALL_PATTERN, so nothing is lost by not duplicating it here.
        //
        // The gain is that the query is now migratable at all. Previously a TVF anywhere in the
        // FROM clause pushed the whole query to the regex path, where baseTableBefore() ALSO skips
        // a call source (same looksLikeCallAt guard) and returns null — so every join hanging off
        // a TVF base was dropped outright, and a TVF-only FROM produced a bogus table op with a
        // NULL name (the "every SELECT gets an op" placeholder firing on an empty scan).
        if (isCallSource(item)) {
            String callAlias = structuredAlias(item);
            if (callAlias != null && !callAlias.matches("[A-Za-z_]\\w*")) return false;
            return item.tableAlias() == null || item.tableAlias().namePart().BRACKET_ID() == null;
        }
        // Defence in depth for the textual form the grammar still cannot see: a name whose
        // argument list did NOT parse into a parenGroup here (so item.parenGroup() is null) but
        // which the raw text shows continuing into `(`. Keeping the original guard means the
        // structured path can never fabricate a call-derived phantom even if the grammar misses.
        if (looksLikeCallAt(selectText, name.getStop().getStopIndex() + 1)) return false;
        // Table-valued parameters and table variables (`@tvp` / `@tbl`) in FROM position are now
        // MIGRATED. The asymmetry the old note deferred turns out not to be the one it described.
        //
        // What it is NOT: a disagreement about whether an @-variable is a table. Both positions
        // already agree, and both are already right — addTableOp() filters a table-valued
        // PARAMETER (it is the parameter, already recorded as one) and keeps a table VARIABLE
        // (which really is a table source). That logic is shared and needs no change.
        //
        // What it IS: the FROM position silently DROPS EVERY JOIN EDGE hanging off it. The legacy
        // base-table lookup, baseTableBefore(), matches with FROM_PATTERN, whose character class
        // `[\w#$]+` has no `@` — so `FROM @tvp x JOIN dbo.A a ON …` resolves base == null and
        // attachJoins() bails out before emitting the edge. JOIN_PATTERN, by contrast, WAS fixed to
        // accept a leading `@` (see the "Bug (TVP-JOIN audit)" note on it), so the very same join
        // written `FROM dbo.A a JOIN @tvp x ON …` IS recorded. The identical logical join is kept
        // or lost purely according to which side the author wrote first — order-dependence, which
        // no reading of the data model justifies.
        //
        // Accepting AT_VAR here routes these queries through the structured path, where the base
        // source is a parse-tree node rather than a regex match and its name is simply available.
        // attachJoinsStructurally handles a base that legitimately owns no table op (a TVP) via the
        // same branch already used for a CTE base — see baseOwnsNoTableOp().
        if (name.AT_VAR() != null) {
            // An @-variable is a single unqualified name; the alias rules below still apply.
            String atAlias = structuredAlias(item);
            if (atAlias != null && !atAlias.matches("[A-Za-z_]\\w*")) return false;
            return item.tableAlias() == null || item.tableAlias().namePart().BRACKET_ID() == null;
        }
        // Stage 4: bracket-quoted parts and 3-/4-part linked-server names (server.db.schema.table)
        // are now migrated. A dotted name of any legal length is split by the shared
        // splitQualifiedName(), whose LAST segment is always the real object and whose
        // second-to-last is the schema — identical to what the regex path derives, and it already
        // handles the `]]` escape for a literal `]` inside a bracketed identifier.
        List<SqlServerParser.NamePartContext> parts = name.namePart();
        if (parts.size() < 1 || parts.size() > 4) return false;
        String alias = structuredAlias(item);
        // The legacy alias groups are `[A-Za-z_]\w*` / `[A-Za-z]\w*`; an ID may also start with
        // `#`/`$`, which those groups could not capture.
        if (alias != null && !alias.matches("[A-Za-z_]\\w*")) return false;
        if (item.tableAlias() != null && item.tableAlias().namePart().BRACKET_ID() != null) return false;
        return true;
    }

    /** Counts FROM/JOIN keyword tokens in the nested text, ignoring comments and string literals. */
    private int countFromJoinTokens(String selectText) {
        SqlServerLexer lexer = new SqlServerLexer(CharStreams.fromString(selectText));
        lexer.removeErrorListeners();
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        int n = 0;
        for (Token t : tokens.getTokens()) {
            if (t.getType() == SqlServerLexer.FROM || t.getType() == SqlServerLexer.JOIN) n++;
        }
        return n;
    }

    /** Adds a SELECT table op for each FROM/JOIN table found in the given SQL text (accurate lines).
     * Returns true iff a FROM/JOIN clause with a table reference was found in the text at all —
     * regardless of whether addTableOp actually kept it (e.g. a table-valued-parameter/table
     * variable reference like "FROM @tvp" is deliberately filtered out by addTableOp, but a FROM
     * clause was still present). Callers use this to distinguish "no FROM clause at all" (e.g.
     * `SELECT @x`) from "FROM clause present but its only source was filtered out", which are not
     * the same case. */
    private boolean addTableOpsFromText(String sqlText, int baseLine) {
        if (sqlText == null) return false;
        // Bug 2: strip comments before scanning — otherwise a comment placed right after FROM
        // (e.g. "FROM -- the orders table\n  Orders o") gets matched by TABLE_REF_PATTERN as
        // the table token instead of the real table name, silently dropping the real table.
        // lineAt still works correctly because stripLineAndBlockComments is length-preserving.
        String scanText = stripLineAndBlockComments(sqlText);
        Matcher m = TABLE_REF_PATTERN.matcher(scanText);
        boolean foundAny = false;
        while (m.find()) {
            // Bug (table-function audit): a FROM/JOIN source that is actually a table-valued-
            // function call — e.g. "FROM dbo.MyTableFunc(@p) t" or a schema-qualified
            // "JOIN mydb.dbo.GetOrders(@id) t" — is already captured as an outbound CALL by
            // extractQualifiedCalls/QUALIFIED_CALL_PATTERN. TABLE_REF_PATTERN only recognises one
            // dot, so for a 3-part TVF name it stops at the middle part (e.g. "mydb.dbo") and, not
            // seeing a real alias next (the true continuation is ".GetOrders(...)"), invents a
            // phantom table op literally named the schema fragment ("DBO"). Detect a call
            // signature — the dotted name immediately continues into "(" once any remaining
            // ".part" segments are skipped — and skip recording a table op for it entirely; the
            // call graph already has it.
            int coreEnd = m.end(1);
            if (looksLikeCallAt(scanText, coreEnd)) continue;
            foundAny = true;
            String[] parts = splitQualifiedName(m.group(1));
            String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
            String name   = parts[parts.length - 1];
            String alias  = m.group(2);
            if (alias != null && RESERVED_NON_TABLE.contains(alias.toUpperCase())) alias = null;
            SqlServerTableOpInfo added =
                    addTableOp(schema, name, lineAt(sqlText, m.start(), baseLine), SqlServerTableOpInfo.DmlType.SELECT);
            if (added != null && alias != null) added.setAlias(alias);
        }
        return foundAny;
    }

    /**
     * True when, starting at {@code pos} (skipping whitespace and any further {@code .identifier}
     * segments — covering multi-part schema-qualified names TABLE_REF_PATTERN's single optional
     * dot didn't fully consume), the next significant character is {@code (} — i.e. the name just
     * matched is really a function call, not a table reference.
     */
    private boolean looksLikeCallAt(String text, int pos) {
        int i = pos;
        int len = text.length();
        while (true) {
            while (i < len && Character.isWhitespace(text.charAt(i))) i++;
            if (i < len && text.charAt(i) == '(') return true;
            if (i < len && text.charAt(i) == '.') {
                i++;
                while (i < len && Character.isWhitespace(text.charAt(i))) i++;
                if (i < len && text.charAt(i) == '[') {
                    while (i < len && text.charAt(i) != ']') i++;
                    i++;
                } else {
                    while (i < len && (Character.isLetterOrDigit(text.charAt(i))
                            || text.charAt(i) == '_' || text.charAt(i) == '#' || text.charAt(i) == '$')) i++;
                }
                continue;
            }
            return false;
        }
    }

    /** How many distinct tables a FROM/JOIN clause set names — 1 means unambiguous attribution. */
    private int distinctTableRefs(String sqlText) {
        if (sqlText == null) return 0;
        Set<String> names = new HashSet<>();
        Matcher m = TABLE_REF_PATTERN.matcher(stripLineAndBlockComments(sqlText));
        while (m.find()) {
            String[] parts = splitQualifiedName(m.group(1));
            String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
            String name   = parts[parts.length - 1];
            names.add(((schema != null ? schema + "." : "") + name).toUpperCase());
        }
        return names.size();
    }

    /** Splits a possibly multi-part (2/3/4-segment) dotted, optionally bracketed, qualified
     * name into its plain identifier segments (brackets stripped). The LAST segment is always
     * the real table/object name; earlier segments are server/database/schema qualifiers. */
    private String[] splitQualifiedName(String qualified) {
        // Bug (escaped-']]'-in-bracket-identifier audit): a blanket `.replace("]", "")` below would
        // also delete the ESCAPED `]]` inside a bracketed identifier like `[table]]name]` (meaning
        // the identifier `table]name`), losing the literal `]` entirely rather than collapsing the
        // escape to one. Protect each doubled `]]` with a placeholder that cannot collide with real
        // SQL text first, strip the remaining (real bracket-delimiter) brackets, then restore the
        // placeholder to a single literal `]`.
        final String escMark = "\u0001";
        String protectedText = qualified.replace("]]", escMark);
        String stripped = protectedText.replace("[", "").replace("]", "").replace(escMark, "]");
        String[] parts = stripped.split("\\.");
        // Bug (comment-between-schema-and-dot audit): TABLE_REF_PATTERN/FROM_PATTERN now tolerate
        // whitespace around the dot (to survive a masked-out comment there), so a captured segment
        // can itself carry leading/trailing whitespace (e.g. "dbo " / "  Orders") — trim it away.
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
        return parts;
    }

    /** Leading GRANT/REVOKE/DENY — permission DDL, which carries no table reads. */
    private static final Pattern PERMISSION_STMT_PATTERN =
            Pattern.compile("^\\s*(?i:GRANT|REVOKE|DENY)\\b");

    /** Recovers all DDL/DML operations (MERGE/TRUNCATE/CREATE/ALTER/DROP/INSERT/UPDATE/DELETE/
     *  SELECT reads) from statements consumed generically by otherStatement (WITH-CTE, MERGE, …). */
    @Override
    public Void visitOtherStatement(SqlServerParser.OtherStatementContext ctx) {
        String txt = originalText(ctx);
        // Permission DDL (GRANT/REVOKE/DENY) also lands in otherStatement, but it contains no
        // DML: the name after ON is a securable and the name after TO/FROM is a PRINCIPAL, not a
        // table. Text recovery read `REVOKE INSERT ON app.orders FROM rpt` as a "FROM <table>"
        // read and invented a phantom SELECT table op named `rpt` (the grantee). The statement
        // itself is still classified as GRANT/REVOKE/DENY elsewhere; only op scanning is skipped.
        if (PERMISSION_STMT_PATTERN.matcher(txt == null ? "" : txt).find()) return null;
        scanOps(txt, ctx.getStart().getLine());
        attachJoins(txt, ctx.getStart().getLine());   // MERGE USING / WITH-CTE joins
        // Grammar-first (batch 2): the cursor lifecycle statement, when the grammar recognised it,
        // is read off the cursorStatement node. Only an unrecognised shape falls through to the
        // legacy text scan (counted).
        if (ctx.cursorStatement() != null) {
            recordCursorLifecycleStructured(ctx.cursorStatement());
        } else {
            recordCursorLifecycleOp(txt, ctx.getStart().getLine());
        }
        return null;
    }

    // OPEN cur; FETCH [NEXT] FROM cur [INTO ...]; CLOSE cur; DEALLOCATE cur;  — these fall through
    // the generic otherStatement catch-all, so recover them here and attach to the matching
    // cursor (by name, within this routine) so its lifecycle is tracked past DECLARE.
    private static final Pattern CURSOR_LIFECYCLE_PATTERN = Pattern.compile(
            "(?i)\\b(OPEN|CLOSE|DEALLOCATE)\\s+([A-Za-z_][\\w]*)\\b"
          + "|\\bFETCH\\b(?:\\s+(?:NEXT|PRIOR|FIRST|LAST|ABSOLUTE|RELATIVE)\\b)?(?:\\s+FROM)?\\s+([A-Za-z_][\\w]*)\\b");

    /** Grammar-first counterpart of {@link #recordCursorLifecycleOp}: reads the lifecycle op and
     *  cursor name from the structured cursorStatement node. */
    private void recordCursorLifecycleStructured(SqlServerParser.CursorStatementContext ctx) {
        if (ctx == null) return;
        String op = null;
        String name = null;
        if (ctx.openCursor() != null) {
            op = "OPEN"; name = ctx.openCursor().namePart() != null ? ctx.openCursor().namePart().getText() : null;
        } else if (ctx.closeCursor() != null) {
            op = "CLOSE"; name = ctx.closeCursor().namePart() != null ? ctx.closeCursor().namePart().getText() : null;
        } else if (ctx.deallocateCursor() != null) {
            op = "DEALLOCATE"; name = ctx.deallocateCursor().namePart() != null ? ctx.deallocateCursor().namePart().getText() : null;
        } else if (ctx.fetchCursor() != null) {
            op = "FETCH"; name = ctx.fetchCursor().namePart() != null ? ctx.fetchCursor().namePart().getText() : null;
        }
        if (op == null || name == null) return;
        attachCursorLifecycleOp(op, stripBrackets(name), ctx.getStart().getLine());
    }

    private void attachCursorLifecycleOp(String op, String cursorName, int line) {
        for (SqlServerCursorInfo cur : result.getCursors()) {
            if (cur.getName() != null && cur.getName().equalsIgnoreCase(cursorName)) {
                cur.addStmtOperation(op, line);
                break;
            }
        }
    }

    private void recordCursorLifecycleOp(String text, int baseLine) {
        if (text == null || result.getCursors().isEmpty()) return;
        Matcher m = CURSOR_LIFECYCLE_PATTERN.matcher(text);
        boolean counted = false;
        while (m.find()) {
            if (!counted) { counted = true; countRegexFallback(CURSOR_REGEX_FALLBACKS, "cursor lifecycle"); }
            String op;
            String cursorName;
            if (m.group(1) != null) {
                op = m.group(1).toUpperCase();
                cursorName = m.group(2);
            } else {
                op = "FETCH";
                cursorName = m.group(3);
            }
            if (cursorName == null) continue;
            for (SqlServerCursorInfo cur : result.getCursors()) {
                if (cur.getName() != null && cur.getName().equalsIgnoreCase(cursorName)) {
                    cur.addStmtOperation(op, lineAt(text, m.start(), baseLine));
                    break;
                }
            }
        }
    }

    /** CREATE TABLE #temp( ... ) etc. inside a routine body. */
    @Override
    public Void visitCreateStatement(SqlServerParser.CreateStatementContext ctx) {
        String text = originalText(ctx);
        int line = ctx.getStart().getLine();
        scanOps(text, line);
        extractIdentityColumns(text, line, ctx);
        if (ctx.createSynonym() == null) {
            extractSynonym(text, line);   // fallback only when the grammar did not structure it
        }
        return visitChildren(ctx);        // visitCreateSynonym records the structured synonym
    }

    /**
     * CREATE SYNONYM [schema.]syn FOR [server.db.]schema.target [@linked_server].
     *
     * <p>Grammar-first (batch 2): recorded from the {@code createSynonym} node, whether it appears
     * at the top level (compilationUnit) or inside a routine body (createStatement). The synonym
     * object is a CREATE op, its target a SELECT op — the same read-through shape the Oracle
     * create_synonym capture uses. The legacy regex runs only when no structured node exists, and
     * that use is counted.
     */
    @Override
    public Void visitCreateSynonym(SqlServerParser.CreateSynonymContext ctx) {
        recordSynonym(ctx);
        return null;
    }

    private void recordSynonym(SqlServerParser.CreateSynonymContext ctx) {
        if (ctx == null || ctx.routineName() == null || ctx.tableRef() == null) return;
        int line = ctx.getStart().getLine();
        List<SqlServerParser.NamePartContext> parts = ctx.routineName().namePart();
        String synSchema = parts.size() == 2 ? stripBrackets(parts.get(0).getText()) : null;
        String synName   = stripBrackets(parts.get(parts.size() - 1).getText());
        addTableOp(synSchema, synName, line, SqlServerTableOpInfo.DmlType.CREATE);

        String[] tp = splitQualifiedName(ctx.tableRef().getText());
        String targetSchema = tp.length >= 2 ? stripBrackets(tp[tp.length - 2]) : null;
        String targetName   = stripBrackets(tp[tp.length - 1]);
        addTableOp(targetSchema, targetName, line, SqlServerTableOpInfo.DmlType.SELECT);
    }

    // CREATE SYNONYM [schema.]syn_name FOR [server.database.]schema.target [@linked_server]
    // Bug 3: CREATE SYNONYM is completely unrecognised inside a procedure body — the grammar's
    // createStatement is a fully-generic catch-all and KEYWORD_OP_PATTERN only knows the
    // TABLE-suffixed DDL keywords. Recognise it additively here (does not touch scanOps/
    // KEYWORD_OP_PATTERN so nothing else regresses): the synonym object itself is recorded as a
    // CREATE table-op (its own schema/name), and its target (up to a 4-part linked-server name,
    // last segment = real table) is recorded as a SELECT table-op (a synonym is a read-through
    // reference to it), mirroring the Oracle visitCreate_synonym capture shape.
    private static final Pattern SYNONYM_PATTERN = Pattern.compile(
            "(?i)\\bCREATE\\s+SYNONYM\\s+\\[?([\\w#$]+)\\]?(?:\\.\\[?([\\w#$]+)\\]?)?\\s+FOR\\s+"
          + "(\\[?[\\w#$]+\\]?(?:\\.\\[?[\\w#$]+\\]?){0,3})");

    private void extractSynonym(String text, int baseLine) {
        if (text == null) return;
        Matcher m = SYNONYM_PATTERN.matcher(text);
        boolean counted = false;
        while (m.find()) {
            if (!counted) { counted = true; countRegexFallback(SYNONYM_REGEX_FALLBACKS, "CREATE SYNONYM"); }
            String synSchema = m.group(2) != null ? stripBrackets(m.group(1)) : null;
            String synName   = m.group(2) != null ? stripBrackets(m.group(2)) : stripBrackets(m.group(1));
            int line = lineAt(text, m.start(), baseLine);
            addTableOp(synSchema, synName, line, SqlServerTableOpInfo.DmlType.CREATE);

            String[] parts = splitQualifiedName(m.group(3));
            String targetSchema = parts.length >= 2 ? parts[parts.length - 2] : null;
            String targetName   = parts[parts.length - 1];
            addTableOp(targetSchema, targetName, line, SqlServerTableOpInfo.DmlType.SELECT);
        }
    }

    /**
     * IDENTITY columns (audit #8) — SQL Server's most common auto-increment mechanism, distinct
     * from (and unrelated to) {@code CREATE SEQUENCE} / {@code NEXT VALUE FOR}. Recorded on the
     * shared {@link SqlServerSequenceInfo} model (see its class doc) so the existing
     * sequences-facet pipeline picks these up without a second concept.
     */
    private void extractIdentityColumns(String text, int baseLine,
                                        SqlServerParser.CreateStatementContext stmtCtx) {
        // Grammar-first (batch 2): when the CREATE TABLE was parsed structurally, the IDENTITY
        // column specs come straight off the tree. The regex path below runs only for a
        // CREATE TABLE shape the structured rule did not match, and that use is counted.
        SqlServerParser.CreateTableStatementContext tbl =
                stmtCtx != null ? stmtCtx.createTableStatement() : null;
        if (tbl != null && tbl.tableRef() != null) {
            String[] tp = splitQualifiedName(tbl.tableRef().getText());
            String schema = tp.length >= 2 ? stripBrackets(tp[tp.length - 2]) : null;
            String table  = stripBrackets(tp[tp.length - 1]);
            for (SqlServerParser.CreateTableItemContext item : tbl.createTableItem()) {
                SqlServerParser.ColumnDefinitionContext col = item.columnDefinition();
                if (col == null || col.namePart() == null) continue;
                SqlServerParser.IdentitySpecContext id = null;
                for (SqlServerParser.ColumnOptionContext opt : col.columnOption()) {
                    if (opt.identitySpec() != null) { id = opt.identitySpec(); break; }
                }
                if (id == null) continue;
                SqlServerSequenceInfo s = new SqlServerSequenceInfo();
                s.setIdentityColumn(true);
                s.setSchema(schema);
                s.setTableName(table);
                s.setColumnName(stripBrackets(col.namePart().getText()));
                long[] seedInc = signedIdentityValues(id);
                s.setSeed(seedInc[0]);
                s.setIncrement(seedInc[1]);
                s.setLine(id.getStart().getLine());
                result.getSequences().add(s);
            }
            return;
        }
        if (text == null || text.isEmpty()) return;
        boolean counted = false;
        Matcher tableM = CREATE_TABLE_PATTERN.matcher(text);
        while (tableM.find()) {
            if (!counted) { counted = true; countRegexFallback(IDENTITY_REGEX_FALLBACKS, "IDENTITY columns"); }
            String schema = tableM.group(2) != null ? stripBrackets(tableM.group(1)) : null;
            String table  = tableM.group(2) != null ? stripBrackets(tableM.group(2)) : stripBrackets(tableM.group(1));
            int openParen = tableM.end() - 1;
            int closeParen = matchingCloseParen(text, openParen);
            if (closeParen < 0) continue;
            String body = text.substring(openParen + 1, closeParen);
            int bodyOffset = openParen + 1;

            Matcher idM = IDENTITY_PATTERN.matcher(body);
            while (idM.find()) {
                SqlServerSequenceInfo s = new SqlServerSequenceInfo();
                s.setIdentityColumn(true);
                s.setSchema(schema);
                s.setTableName(table);
                s.setColumnName(stripBrackets(idM.group(1)));
                s.setSeed(idM.group(2) != null ? Long.valueOf(idM.group(2)) : Long.valueOf(1L));
                s.setIncrement(idM.group(3) != null ? Long.valueOf(idM.group(3)) : Long.valueOf(1L));
                s.setLine(lineAt(text, bodyOffset + idM.start(), baseLine));
                result.getSequences().add(s);
            }
        }
    }

    /** First (optionally minus-signed) integer token in a rule, e.g. START WITH -5 / INCREMENT BY 2. */
    private static long signedNum(ParserRuleContext ctx) {
        boolean neg = false;
        for (int i = 0; i < ctx.getChildCount(); i++) {
            org.antlr.v4.runtime.tree.ParseTree c = ctx.getChild(i);
            if (!(c instanceof org.antlr.v4.runtime.tree.TerminalNode)) continue;
            String tx = c.getText();
            if ("-".equals(tx)) { neg = true; continue; }
            if (tx.chars().allMatch(Character::isDigit)) {
                return neg ? -Long.parseLong(tx) : Long.parseLong(tx);
            }
        }
        return 0L;
    }

    /**
     * Reads {@code IDENTITY(seed, increment)} into {@code {seed, increment}}, defaulting both to
     * 1 (bare IDENTITY) and honouring an optional leading minus on either number.
     */
    private static long[] signedIdentityValues(SqlServerParser.IdentitySpecContext id) {
        long[] out = {1L, 1L};
        int vi = 0;
        boolean neg = false;
        for (int i = 0; i < id.getChildCount() && vi < 2; i++) {
            org.antlr.v4.runtime.tree.ParseTree c = id.getChild(i);
            if (!(c instanceof org.antlr.v4.runtime.tree.TerminalNode)) continue;
            String tx = c.getText();
            if ("-".equals(tx)) { neg = true; continue; }
            if (tx.chars().allMatch(Character::isDigit)) {
                out[vi] = neg ? -Long.parseLong(tx) : Long.parseLong(tx);
                neg = false;
                vi++;
            }
        }
        return out;
    }

    /** Index of the {@code )} that matches the {@code (} at {@code openParenIdx}, or -1. */
    private int matchingCloseParen(String text, int openParenIdx) {
        int depth = 0;
        for (int i = openParenIdx; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    /** Scan text for every keyword-led table operation and record it with the correct op type. */
    private void scanOps(String text, int baseLine) {
        if (text == null) return;
        Matcher m = KEYWORD_OP_PATTERN.matcher(text);
        while (m.find()) {
            String kw = m.group(1).replaceAll("\\s+", " ").toUpperCase();
            SqlServerTableOpInfo.DmlType op = opForKeyword(kw);
            if (op == null) continue;
            String schema = m.group(3) != null ? stripBrackets(m.group(2)) : null;
            String name   = m.group(3) != null ? stripBrackets(m.group(3)) : stripBrackets(m.group(2));
            // Round-3 probe (CTE + `UPDATE <alias> … FROM … JOIN` audit): resolve an UPDATE target
            // that is really a FROM-clause ALIAS back to the table it aliases.
            //
            // Stage 8 already does this for a statement the grammar routes to visitUpdateStatement
            // (see retargetDmlOpFromAlias). But `WITH c AS (…) UPDATE t SET … FROM dbo.Target t …`
            // begins with WITH, so it is consumed whole by otherStatement and never reaches that
            // visitor — it is recovered only by this text scan, which had no alias notion at all.
            // Measured on the probe: the op came out as a table literally named `t`, while the real
            // target dbo.Target was recorded only as a SELECT. That is the same class of defect
            // Stage 8 was created to fix (a WRONG dependency pointing at a non-existent object,
            // not merely a missing one), simply on the code path Stage 8 does not cover.
            //
            // Deliberately as narrow as Stage 8's own version, and for the same reasons: only the
            // UPDATE keyword, only an UNQUALIFIED target, and only when the very same statement
            // text binds that exact word as an alias of a DIFFERENT table. `UPDATE dbo.Target …`
            // (qualified) and `UPDATE Orders … FROM Orders o` (resolves to itself) are untouched.
            if ("UPDATE".equals(kw) && schema == null) {
                String[] real = aliasTargetInText(text, name);
                if (real != null) { schema = real[0]; name = real[1]; }
            }
            SqlServerTableOpInfo added =
                    addTableOp(schema, name, lineAt(text, m.start(), baseLine), op);
            // Only FROM/JOIN carry a meaningful table alias in this grammar position; for the
            // other keywords (INSERT INTO/UPDATE/DELETE FROM/...) the following bare word is a
            // clause keyword, not an alias, and RESERVED_NON_TABLE below filters those out anyway.
            if (added != null && added.getAlias() == null && (kw.equals("FROM") || kw.equals("JOIN"))) {
                String alias = m.group(4);
                if (alias != null && !RESERVED_NON_TABLE.contains(alias.toUpperCase())) {
                    added.setAlias(alias);
                }
            }
            // DDL inside a routine (TRUNCATE/ALTER/DROP/CREATE TABLE) reaches here rather than a
            // DML visitor, because the grammar consumes it through otherStatement. Without the
            // statement text the enricher never sees it, and `ddl` stayed empty for every
            // analysis — including the SQL Server demo schema, which contains 120 occurrences of
            // `TRUNCATE TABLE erp.InventoryTxn`. Measured on disk, not inferred from the absence.
            if (added != null && added.getSqlText() == null) {
                // Sep05-13: every recovered operation gets its statement text, not only DDL.
                //
                // Measured: 235 SQL Server rows reached the screen with no `sqlText` at all — 210
                // recovered from a FROM/JOIN and 25 from a MERGE — so the enricher never saw them
                // and every cell was blank. That was the whole of this dialect's remaining gap.
                //
                // The two cases need different starting points, and using one for both is what
                // makes this subtle. A DDL/DML keyword *begins* its statement, so the match
                // position is the start. `FROM` and `JOIN` sit in the *middle* of one, so starting
                // there yields "FROM erp.Product p INNER JOIN …", which is not a statement in any
                // dialect and would parse to nothing — trading a blank cell for a blank cell plus
                // a misleading "not parsed" label.
                added.setSqlText(kw.startsWith("FROM") || kw.startsWith("JOIN") || kw.startsWith("USING")
                        ? statementAround(text, m.start())
                        : oneStatementFrom(text, m.start()));
            }
        }
        scanDeleteTargetFrom(text, baseLine);
    }

    /**
     * Records the delete TARGET of T-SQL's `DELETE &lt;target&gt; FROM &lt;sources&gt; …` form.
     *
     * <p>The DELETE-side counterpart of the `UPDATE &lt;alias&gt; … FROM …` recovery a few lines
     * above, for the same code path and the same reason: a statement beginning with `WITH` is
     * consumed whole by otherStatement and never reaches {@link #visitDeleteStatement} (whose
     * Stage-8 {@code applyDmlFromClause}/{@code retargetDmlOpFromAlias} already handle the
     * non-WITH form correctly). Measured before this: `WITH c AS (…) DELETE t FROM dbo.Target t
     * JOIN dbo.Other o ON …` recorded no delete op whatsoever — only the two SELECT-side reads —
     * so the statement's single most important fact, that it deletes from dbo.Target, was absent.
     *
     * <p>Runs as its own pass AFTER the main {@link #KEYWORD_OP_PATTERN} loop, so the op ordering
     * of every input that does not contain this shape is byte-for-byte unchanged.
     *
     * <p>Exactly as narrow as the UPDATE version: an UNQUALIFIED target is resolved through
     * {@link #aliasTargetInText} (`DELETE t FROM dbo.Target t …` → dbo.Target), a QUALIFIED one is
     * taken as written (`DELETE dbo.Target FROM dbo.Target INNER JOIN …`), and an unqualified name
     * that binds no alias in this text is left alone — {@link #addTableOp}'s own CTE/param/
     * reserved-word filters then decide whether it is a table at all.
     */
    private void scanDeleteTargetFrom(String text, int baseLine) {
        if (text == null) return;
        Matcher m = DELETE_TARGET_FROM_PATTERN.matcher(stripLineAndBlockComments(text));
        while (m.find()) {
            String schema = m.group(2) != null ? stripBrackets(m.group(1)) : null;
            String name   = m.group(2) != null ? stripBrackets(m.group(2)) : stripBrackets(m.group(1));
            String alias  = null;
            if (schema == null) {
                String[] real = aliasTargetInText(text, name);
                if (real != null) { alias = name; schema = real[0]; name = real[1]; }
            }
            SqlServerTableOpInfo added = addTableOp(schema, name, lineAt(text, m.start(), baseLine),
                    SqlServerTableOpInfo.DmlType.DELETE);
            if (added != null) {
                if (alias != null && added.getAlias() == null) added.setAlias(alias);
                if (added.getSqlText() == null) added.setSqlText(oneStatementFrom(text, m.start()));
            }
        }
    }

    /**
     * The real table that {@code alias} is bound to by a FROM/JOIN clause in {@code text}, as
     * {@code {schema, name}} in the SOURCE's own casing, or null when {@code alias} is not an
     * alias there (or aliases itself).
     *
     * <p>Uses {@link #TABLE_REF_PATTERN} — the same scanner {@link #addTableOpsFromText} uses to
     * record these tables in the first place — so the alias binding this reads is exactly the one
     * the surrounding extraction already acted on, rather than a second, possibly divergent view.
     *
     * <p>Comments and string-literal contents are blanked first, for the reason every other regex
     * scan here does it: the word after a {@code FROM} inside a comment or a literal is not an
     * alias binding.
     */
    private String[] aliasTargetInText(String text, String alias) {
        if (text == null || alias == null || alias.isEmpty()) return null;
        Matcher m = TABLE_REF_PATTERN.matcher(stripLineAndBlockComments(text));
        while (m.find()) {
            String a = m.group(2);
            if (a == null || !a.equalsIgnoreCase(alias)) continue;
            String[] parts = splitQualifiedName(m.group(1));
            String name = stripBrackets(parts[parts.length - 1]);
            // An alias that resolves to its own name carries no correction to make.
            if (name.equalsIgnoreCase(alias)) return null;
            return new String[]{parts.length >= 2 ? stripBrackets(parts[parts.length - 2]) : null, name};
        }
        return null;
    }

    /**
     * Words that can never be an unquoted table name, so a "table" of this name is a mis-parse.
     *
     * <p>These are T-SQL reserved words: the server itself rejects {@code CREATE TABLE SET}, so a
     * recovered name from this set is always a scanning artefact and never a real object being
     * hidden. That is what makes dropping it safe rather than a guess — the alternative, reporting
     * a table the database cannot contain, is a fact the reader has no way to disprove.
     *
     * <p>A bracketed or quoted identifier ({@code [SET]}) is legal in T-SQL, but
     * {@link #stripBrackets} has already removed the brackets by this point, so such a table would
     * be suppressed too. That is accepted deliberately: a table genuinely named {@code [SET]} is
     * vanishingly rare, whereas the mis-parse was measured on the first schema examined.
     */
    private static final Set<String> RESERVED_NON_TABLE = Set.of(
            "SET", "SELECT", "VALUES", "WHERE", "FROM", "JOIN", "INTO", "ON", "AS", "AND", "OR",
            "WHEN", "THEN", "ELSE", "END", "CASE", "BY", "GROUP", "ORDER", "HAVING", "UNION",
            "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "APPLY", "LATERAL", "USING",
            "MATCHED", "TARGET", "SOURCE", "OUTPUT", "TOP", "DISTINCT", "EXISTS", "NOT", "NULL",
            "WITH", "TABLE", "UPDATE", "INSERT", "DELETE", "MERGE", "EXEC", "EXECUTE", "DECLARE",
            "BEGIN", "IF", "WHILE", "RETURN", "PRINT", "THROW", "INTERSECT", "EXCEPT",
            // Round-3 probe (PIVOT / UNPIVOT / temporal-table audit): the alias group of
            // TABLE_REF_PATTERN / KEYWORD_OP_PATTERN is `(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?` — it
            // simply takes the next bare word after the table name. Three T-SQL reserved words
            // legitimately occupy that position and were therefore captured as if they were the
            // table's ALIAS:
            //
            //   FROM dbo.SalesData PIVOT ( … ) AS pvt          → alias recorded as "PIVOT"
            //   FROM dbo.QuarterlySales UNPIVOT ( … ) AS unpvt → alias recorded as "UNPIVOT"
            //   FROM dbo.Emp FOR SYSTEM_TIME AS OF '…' AS e    → alias recorded as "FOR"
            //
            // In every case the real alias (pvt / unpvt / e) sits further right, past a clause the
            // regex cannot model, so the recorded alias was not merely imprecise but WRONG — a
            // reader cannot distinguish it from a genuine alias, and any alias-keyed lookup
            // (resolveLeftSide's alias map included) resolves the wrong thing.
            //
            // All three are reserved words in T-SQL, so they satisfy this set's stated criterion
            // exactly: the server itself rejects `CREATE TABLE FOR` / `CREATE TABLE PIVOT`, so a
            // "table" or "alias" of this name is always a scanning artefact and never a real
            // object being hidden. Suppressing the wrong alias (leaving it absent) is the honest
            // outcome; recovering the true alias would need real PIVOT/UNPIVOT/FOR SYSTEM_TIME
            // grammar rules, which is a separate, larger piece of work.
            "PIVOT", "UNPIVOT", "FOR");

    /** Keywords whose statement is DDL, as {@link #KEYWORD_OP_PATTERN} normalises them. */
    private static final Set<String> DDL_KEYWORDS = Set.of(
            "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "TRUNCATE TABLE");

    /** One statement out of a text block: from {@code from} to the next {@code ;}, or to the end. */
    /** Keywords that begin a statement whose text we can hand to the column enricher. */
    private static final java.util.regex.Pattern STATEMENT_HEAD = java.util.regex.Pattern.compile(
            "(?i)\\b(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH)\\b");

    /**
     * The statement surrounding a match that sits <em>inside</em> one, such as a {@code FROM} or
     * {@code JOIN}.
     *
     * <p>Walks back to the nearest statement-beginning keyword and forward to the terminator. The
     * nearest one rather than the outermost is deliberate: for a {@code FROM} inside a subquery the
     * inner {@code SELECT} is the statement that actually reads the table, and handing over the
     * outer one would attribute the subquery's columns to the wrong query.
     *
     * <p>Returns null when no head is found, so the caller leaves {@code sqlText} unset rather than
     * passing the enricher a fragment. An honest absence beats a confident mis-parse.
     */
    private static String statementAround(String text, int matchStart) {
        Matcher h = STATEMENT_HEAD.matcher(text);
        int head = -1;
        while (h.find()) {
            if (h.start() > matchStart) break;
            head = h.start();
        }
        if (head < 0) return null;
        String out = oneStatementFrom(text, head);
        return out == null || out.isBlank() ? null : out;
    }

    private static String oneStatementFrom(String text, int from) {
        int end = text.indexOf(';', from);
        return end < 0 ? text.substring(from) : text.substring(from, end);
    }

    private SqlServerTableOpInfo.DmlType opForKeyword(String kw) {
        if (kw.startsWith("FROM") || kw.startsWith("JOIN") || kw.startsWith("USING"))
                                                                 return SqlServerTableOpInfo.DmlType.SELECT;
        if (kw.startsWith("INSERT"))                            return SqlServerTableOpInfo.DmlType.INSERT;
        if (kw.startsWith("UPDATE"))                            return SqlServerTableOpInfo.DmlType.UPDATE;
        if (kw.startsWith("DELETE"))                            return SqlServerTableOpInfo.DmlType.DELETE;
        if (kw.startsWith("MERGE"))                             return SqlServerTableOpInfo.DmlType.MERGE;
        if (kw.startsWith("TRUNCATE"))                          return SqlServerTableOpInfo.DmlType.TRUNCATE;
        if (kw.startsWith("CREATE"))                            return SqlServerTableOpInfo.DmlType.CREATE;
        if (kw.startsWith("ALTER"))                             return SqlServerTableOpInfo.DmlType.ALTER;
        if (kw.startsWith("DROP"))                              return SqlServerTableOpInfo.DmlType.DROP;
        if (kw.startsWith("INTO"))                              return SqlServerTableOpInfo.DmlType.INSERT;
        return null;
    }

    /** Parse JOIN / APPLY clauses from SQL text and attach each to its ACTUAL base table op
     *  (the nearest preceding FROM), looked up by name — robust to op de-duplication. Handles
     *  SELECTs, MERGE USING subqueries and CTEs. */
    private void attachJoins(String sqlText, int baseLine) {
        if (sqlText == null) return;
        // Bug 2: strip comments before scanning, same reasoning as addTableOpsFromText — a
        // comment sitting right after FROM/JOIN must not be mistaken for the table token.
        sqlText = stripLineAndBlockComments(sqlText);
        // alias->table map for the whole statement text, so BOTH sides of each join can be
        // resolved from its ON-condition aliases (left side is NOT defaulted to nearest FROM).
        java.util.Map<String, String> aliasMap = buildAliasMap(sqlText);
        Matcher m = JOIN_PATTERN.matcher(sqlText);
        while (m.find()) {
            String op = m.group(2).toUpperCase();
            // Bare "APPLY" (no INNER/LEFT/.../CROSS/OUTER qualifier) is not valid T-SQL — only
            // CROSS APPLY / OUTER APPLY exist — so skip it rather than invent a fake join type.
            if ("APPLY".equals(op) && m.group(1) == null) continue;
            SqlServerJoinInfo j = new SqlServerJoinInfo();
            // Bare "JOIN" (no qualifier) is the ANSI-standard synonym for "INNER JOIN".
            String kw = m.group(1) != null ? m.group(1).replaceAll("\\s+", " ").toUpperCase() : "INNER";
            j.setJoinType("APPLY".equals(op) ? kw + " APPLY" : kw);   // e.g. "CROSS APPLY", "LEFT OUTER"
            String[] parts = splitQualifiedName(m.group(3));
            if (parts.length >= 2) j.setJoinedTableSchema(stripBrackets(parts[parts.length - 2]));
            j.setJoinedTable(stripBrackets(parts[parts.length - 1]));
            if (m.group(4) != null) j.setJoinedTableAlias(m.group(4));
            if (m.group(5) != null) j.setCondition(m.group(5).trim());
            j.setLine(lineAt(sqlText, m.start(), baseLine));
            j.setQueryId(baseLine);
            // Skip joins to/from CTE aliases (not real tables).
            if (j.getJoinedTable() == null
                    || cteNames.contains(j.getJoinedTable().toUpperCase())) continue;
            resolveLeftSide(j, aliasMap);
            // Attach to the join's base table (nearest FROM before this JOIN), by name.
            String base = baseTableBefore(sqlText, m.start());
            if (base == null) continue;
            SqlServerTableOpInfo baseOp;
            if (cteNames.contains(base.toUpperCase())) {
                // Bug 5 (chained-CTE audit): the join's base really is a prior CTE (e.g.
                // "FROM cte_a a JOIN sales.Orders o ON ..."), which has no table op of its own
                // (CTE names are deliberately never recorded as real tables). Rather than silently
                // dropping the edge, attribute/attach it to the joined table's OWN op instead —
                // leftTable/leftTableAlias (set above via resolveLeftSide, from the ON-condition's
                // alias map, which does include CTE aliases) still correctly show the CTE name as
                // the structural base, so the relationship "FROM cte_a JOIN sales.Orders" is not
                // lost, without inventing a phantom "cte_a" table object in the results.
                baseOp = findOrCreateSelectOp(j.getJoinedTable(), j.getLine());
            } else {
                baseOp = findOrCreateSelectOp(base, j.getLine());
            }
            baseOp.getJoins().add(j);   // per-occurrence, like the Oracle path; the UI counts distinct
        }
    }

    // Reserved words that must not be mistaken for a table alias.
    private static final java.util.Set<String> ALIAS_STOPWORDS = java.util.Set.of(
            "ON", "WHERE", "GROUP", "ORDER", "HAVING", "UNION", "INNER", "LEFT", "RIGHT",
            "FULL", "CROSS", "OUTER", "JOIN", "APPLY", "AS", "SET", "WITH", "AND", "OR",
            // Same three reserved words as the RESERVED_NON_TABLE addition above. Without them
            // buildAliasMap() binds PIVOT/UNPIVOT/FOR to the preceding table, so resolveLeftSide
            // would happily resolve a join's left side through a keyword that is not an alias.
            "PIVOT", "UNPIVOT", "FOR");

    private static final Pattern ALIAS_TABLE_PATTERN = Pattern.compile(
            "(?i)\\b(?:FROM|JOIN)\\s+\\[?([\\w#$@]+)\\]?(?:\\.\\[?([\\w#$]+)\\]?)?"
          + "(?:\\s+(?:AS\\s+)?([A-Za-z]\\w*))?");

    /** Scan FROM/JOIN clauses in the statement text, mapping each alias (and bare table name) to its table. */
    private java.util.Map<String, String> buildAliasMap(String sqlText) {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        Matcher mt = ALIAS_TABLE_PATTERN.matcher(sqlText);
        while (mt.find()) {
            String table = mt.group(2) != null ? stripBrackets(mt.group(2)) : stripBrackets(mt.group(1));
            if (table == null) continue;
            table = table.toUpperCase();
            String alias = mt.group(3);
            if (alias != null && !ALIAS_STOPWORDS.contains(alias.toUpperCase())) {
                m.put(alias.toUpperCase(), table);
            }
            m.put(table, table);
        }
        return m;
    }

    /** Resolve leftTable/leftTableAlias from the ON-condition alias that is NOT the joined table. */
    private void resolveLeftSide(SqlServerJoinInfo j, java.util.Map<String, String> aliasMap) {
        if (j.getCondition() == null) return;
        String joinedAlias = j.getJoinedTableAlias() != null ? j.getJoinedTableAlias().toUpperCase() : null;
        Matcher mt = Pattern.compile("([A-Za-z_][\\w#$]*)\\s*\\.").matcher(j.getCondition());
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        while (mt.find()) seen.add(mt.group(1).toUpperCase());
        for (String a : seen) {
            if (joinedAlias != null && a.equals(joinedAlias)) continue;
            String t = aliasMap.get(a);
            if (t != null) { j.setLeftTableAlias(a); j.setLeftTable(t); return; }
        }
    }

    /** The table of the nearest FROM clause appearing before the given offset.
     *  Bug (bare-JOIN-after-TVF audit): a FROM source that is actually a table-valued-function
     *  call (e.g. "FROM dbo.MyTableFunc(@p) t") is already captured as an outbound CALL, not a
     *  table op (see addTableOpsFromText/looksLikeCallAt) — skip it here too, or a subsequent
     *  bare JOIN would resurrect it as a phantom base table op via findOrCreateSelectOp. */
    private String baseTableBefore(String text, int pos) {
        Matcher m = FROM_PATTERN.matcher(text.substring(0, Math.min(pos, text.length())));
        String base = null;
        while (m.find()) {
            if (looksLikeCallAt(text, m.end(1))) continue;
            base = m.group(2) != null ? stripBrackets(m.group(2)) : stripBrackets(m.group(1));
        }
        return base;
    }

    /** Find an existing table op by name (prefer a SELECT op); create a SELECT op if none. */
    private SqlServerTableOpInfo findOrCreateSelectOp(String name, int line) {
        SqlServerTableOpInfo any = null;
        for (SqlServerTableOpInfo t : result.getTableOps()) {
            if (name.equalsIgnoreCase(t.getTableName())) {
                if (t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT) return t;
                if (any == null) any = t;
            }
        }
        if (any != null) return any;
        SqlServerTableOpInfo t = new SqlServerTableOpInfo();
        t.setDmlType(SqlServerTableOpInfo.DmlType.SELECT);
        t.setTableName(name);
        t.setLine(line);
        result.getTableOps().add(t);
        return t;
    }

    /** Collect CTE names from a routine body so they aren't mistaken for tables. */
    private void collectCteNames(ParserRuleContext bodyCtx) {
        if (bodyCtx == null) return;
        // Grammar-first (batch 2): the CTE names are read off the cteDefinition nodes the
        // withStatement rule now produces. The regex survives only for a WITH-shape the structured
        // rule could not parse, and that use is counted.
        List<SqlServerParser.CteDefinitionContext> defs = new ArrayList<>();
        collectNodes(bodyCtx, SqlServerParser.CteDefinitionContext.class, defs);
        if (!defs.isEmpty()) {
            for (SqlServerParser.CteDefinitionContext d : defs) {
                if (d.namePart() != null) cteNames.add(stripBrackets(d.namePart().getText()).toUpperCase());
            }
            return;
        }
        String text = originalText(bodyCtx);
        if (text == null) return;
        countRegexFallback(CTE_REGEX_FALLBACKS, "CTE names");
        Matcher m = CTE_PATTERN.matcher(text);
        while (m.find()) cteNames.add(m.group(1).toUpperCase());
    }

    /** Depth-first collection of every node of {@code type} in the subtree rooted at {@code t}. */
    private static <T extends ParserRuleContext> void collectNodes(
            org.antlr.v4.runtime.tree.ParseTree t, Class<T> type, List<T> out) {
        if (t == null) return;
        if (type.isInstance(t)) out.add(type.cast(t));
        for (int i = 0; i < t.getChildCount(); i++) collectNodes(t.getChild(i), type, out);
    }

    /** Add a table op, de-duped by schema.name + operation (so a table can show many op types). */
    /** @return the operation just added, or null when it was skipped or already seen. */
    private SqlServerTableOpInfo addTableOp(String schema, String name, int line,
                                            SqlServerTableOpInfo.DmlType op) {
        if (name == null || name.isEmpty()) return null;
        // Skip CTE aliases, table variables, and cursor names — none are real tables. Bug 6:
        // "FETCH NEXT FROM cur" reaches the same generic FROM/JOIN scan as a real FROM clause, so
        // without this a declared cursor is mis-scanned as a table reference.
        // Bug (table-variable audit): table variables (@tbl, from DECLARE @tbl TABLE(...)) ARE
        // real table sources and must be captured when used in FROM/JOIN. But a table-valued
        // PARAMETER (@tvp, declared in the routine's own param list) referenced via
        // "FROM @tvp" is the parameter itself — already recorded as a param — not a fresh table
        // op, so it is still filtered here (by paramNames, not by a blanket '@' check).
        if (schema == null && (cteNames.contains(name.toUpperCase())
                || cursorNames.contains(name.toUpperCase())
                || paramNames.contains(name.toUpperCase()))) return null;
        // Bug (trigger pseudo-table audit): `inserted`/`deleted` are SQL Server's in-memory
        // trigger delta pseudo-tables, never real base tables — but only inside a trigger body,
        // where alone they can occur. Unqualified-only (schema == null), mirroring the
        // RESERVED_NON_TARGET/SOURCE precedent: a real base table can be schema-qualified even if
        // it shares the bare name, so only the bare pseudo-table reference is excluded.
        if (insideTriggerBody && schema == null
                && ("INSERTED".equals(name.toUpperCase()) || "DELETED".equals(name.toUpperCase()))) {
            return null;
        }
        // Sep05-11: a recovered name that is a reserved clause keyword is not a table.
        //
        // The recovery patterns above are text-based by necessity — MERGE and WITH-CTEs reach the
        // generic otherStatement rule, so their tables can only be found by scanning. But
        // `MERGE … WHEN MATCHED THEN UPDATE SET tgt.Qty = …` makes KEYWORD_OP_PATTERN read
        // "UPDATE" followed by "SET" and invent a table called SET — 13 phantom rows measured, and
        // a fake entry in the table list that a reader cannot tell from a real one.
        //
        // The guard sits here, at the single funnel, rather than in the regex: all three recovery
        // patterns pass through this method, and fixing one regex would leave the others to be
        // rediscovered one keyword at a time. That is how the previous instance of this defect —
        // the same "SET" phantom in the MySQL MERGE path — came back in a second dialect.
        // Bug (MERGE Target/Source name-collision audit): RESERVED_NON_TABLE contains the bare
        // words TARGET/SOURCE to suppress MERGE's own conventional bare aliases (e.g. "MERGE t
        // USING s AS source"), but that guard used to fire on the captured NAME regardless of
        // whether it was schema-qualified — silently dropping a real, explicitly-qualified table
        // object literally named Target/Source (e.g. "dbo.Target"), a common MERGE staging-table
        // naming choice. A schema-qualified reference can never be MERGE's own bare keyword usage
        // (which is always unqualified), so only exclude TARGET/SOURCE when unqualified.
        String upperName = name.toUpperCase();
        boolean isQualifiedTargetOrSource = schema != null
                && ("TARGET".equals(upperName) || "SOURCE".equals(upperName));
        if (RESERVED_NON_TABLE.contains(upperName) && !isQualifiedTargetOrSource) return null;
        String key = ((schema != null ? schema : "") + "." + name + "|" + op).toUpperCase();
        if (!seenTables.add(key)) return null;
        SqlServerTableOpInfo t = new SqlServerTableOpInfo();
        t.setDmlType(op);
        t.setSchemaName(schema);
        t.setTableName(name);
        t.setLine(line);
        // TEMP as the fourth table kind. The name's own prefix IS the signal T-SQL uses to make a
        // table session-local — `#name` (local temp), `##name` (global temp), `@name` (table
        // variable) — so this is a parse-side classification, never a catalog lookup (temp tables
        // and table variables are absent from sys.objects by construction). A schema-qualified
        // reference can never be a temp table (temp objects are always unqualified), so only the
        // bare name is classified.
        if (schema == null && !name.isEmpty()) {
            if (name.startsWith("##")) { t.setTableKind("TEMP"); t.setTempScope("GLOBAL SESSION"); }
            else if (name.startsWith("#")) { t.setTableKind("TEMP"); t.setTempScope("SESSION"); }
            else if (name.startsWith("@")) { t.setTableKind("TEMP"); t.setTempScope("BATCH"); }
        }
        result.getTableOps().add(t);
        return t;
    }

    /** Line number of a match at char offset within text that begins at baseLine. */
    private int lineAt(String text, int offset, int baseLine) {
        int n = baseLine;
        int end = Math.min(offset, text.length());
        for (int i = 0; i < end; i++) if (text.charAt(i) == '\n') n++;
        return n;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private String extractRoutineName(SqlServerParser.RoutineNameContext ctx) {
        if (ctx == null) return null;
        List<SqlServerParser.NamePartContext> parts = ctx.namePart();
        if (parts.isEmpty()) return null;
        // Last part is the name
        return stripBrackets(parts.get(parts.size() - 1).getText());
    }

    private String extractRoutineSchema(SqlServerParser.RoutineNameContext ctx) {
        if (ctx == null) return null;
        List<SqlServerParser.NamePartContext> parts = ctx.namePart();
        if (parts.size() < 2) return null;
        // First part is the schema
        return stripBrackets(parts.get(0).getText());
    }

    private String extractTableName(SqlServerParser.TableRefContext ctx) {
        if (ctx == null) return null;
        // tableRef: up to 4 dotted parts (server.database.schema.table). The LAST segment is
        // always the real table/object name, regardless of how many qualifiers precede it.
        String text = ctx.getText(); // e.g. "dbo.employees" or "LinkedSrv.OtherDb.dbo.Orders"
        String[] parts = splitQualifiedName(text);
        return stripBrackets(parts[parts.length - 1]);
    }

    private String extractTableSchema(SqlServerParser.TableRefContext ctx) {
        if (ctx == null) return null;
        String text = ctx.getText();
        String[] parts = splitQualifiedName(text);
        if (parts.length < 2) return null;
        // Second-to-last segment is the schema; earlier segments (server/database) are dropped,
        // matching the SELECT-path regex's already-fixed splitQualifiedName() behaviour.
        return stripBrackets(parts[parts.length - 2]);
    }

    private String stripBrackets(String s) {
        if (s == null) return null;
        if (s.startsWith("[") && s.endsWith("]") && s.length() >= 2) {
            // Bug (escaped-']]'-in-bracket-identifier audit): mirrors the grammar-level BRACKET_ID
            // fix — T-SQL's escaping convention for a literal `]` inside a bracketed identifier is
            // a doubled `]]`, e.g. `[table]]name]` is the identifier `table]name`. Collapse it back
            // to a single `]` in the final captured name, same as the lexer now consumes it as one
            // token instead of stopping at the first `]`.
            return s.substring(1, s.length() - 1).replace("]]", "]");
        }
        return s;
    }
}
