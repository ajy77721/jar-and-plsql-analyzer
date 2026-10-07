package com.plsql.mysql.visitor;

import com.plsql.mysql.grammar.MySqlParser;
import com.plsql.mysql.grammar.MySqlParserBaseVisitor;
import com.plsql.mysql.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * ANTLR4 visitor over the MySqlParser parse tree.
 *
 * Each method cluster handles one structural concern:
 *   visitCreateRoutine    → routine identity + return type
 *   visitParam            → parameter list
 *   visitDeclareVar       → DECLARE variable
 *   visitDeclareCursor    → DECLARE cursor
 *   visitCallStatement    → CALL outbound edges  (call-tree)
 *   visitFromClause       → SELECT table references
 *   visitInsertStatement  → INSERT table op
 *   visitUpdateStatement  → UPDATE table op
 *   visitDeleteStatement  → DELETE table op
 *   visitPrepareStatement → dynamic SQL PREPARE
 *   visitExecuteStatement → dynamic SQL EXECUTE
 *   visitDeallocateStatement → dynamic SQL DEALLOCATE
 */
public class MySqlAnalysisVisitor extends MySqlParserBaseVisitor<Void> {

    private static final Logger log = LoggerFactory.getLogger(MySqlAnalysisVisitor.class);

    private final MySqlRoutineInfo result;

    /**
     * CTE names declared via WITH — these are not base tables and must be skipped when recording
     * table ops. Real bug, found via a live audit: without this guard, a `WITH cust_829 AS (...)
     * SELECT * FROM cust_829` recorded `cust_829` as a genuine table (`SELECT` op), polluting the
     * table inventory with a phantom entry that doesn't exist in the schema. DB2's visitor already
     * has this guard (`isCte`/`cteNames`); this ports the same fix to MySQL.
     */
    private final Set<String> cteNames = new java.util.HashSet<>();

    /**
     * Index into {@code result.getExceptionHandlers()} for the handler whose body is currently
     * being visited, or {@code null} when visiting main-body code. Tags every CALL/statement/table
     * op created while inside a handler body, so they no longer read as ordinary top-level code
     * indistinguishable from the routine's own logic (audit #9).
     */
    private Integer currentHandlerIndex = null;

    /**
     * Bug 5 (nested-handler ancestry): stack of handler indices currently being visited, from
     * outermost to innermost. {@link #currentHandlerIndex} previously restored to the *previous*
     * single int on exit, which meant nesting depth/ancestry (which handler is nested inside
     * which) could not be reconstructed from the flat int alone. This stack lets
     * {@link #visitDeclareHandler} record each handler's parent (the stack's top before pushing),
     * mirroring Oracle's {@code ExceptionHandlerInfo.parentHandler}.
     */
    private final java.util.Deque<Integer> handlerStack = new java.util.ArrayDeque<>();

    /**
     * Enrichment: branch/loop context stack. Each frame records the enclosing IF/CASE/LOOP and
     * its (collapsed, ≤200 char) condition, so a transaction statement emitted while inside a
     * branch carries {@code guardKind/guardLine/guardCondition}. Purely additive — never changes
     * which statements are captured.
     */
    private final java.util.Deque<Guard> guardStack = new java.util.ArrayDeque<>();

    /** Enrichment: name/line of the handler whose body is currently being visited, or null. */
    private String currentHandlerName = null;
    private Integer currentHandlerLine = null;

    /** One branch/loop frame. {@code kind} ∈ {IF, CASE, LOOP}; {@code condition} may be null. */
    private static final class Guard {
        final String kind;
        final int line;
        final String condition;
        Guard(String kind, int line, String condition) {
            this.kind = kind;
            this.line = line;
            this.condition = condition;
        }
    }

    /**
     * Bug 8 (MySQL portion): user variable name (without the leading {@code @}) → name of the
     * runtime identifier that interrupted a {@code CONCAT('SELECT * FROM ', v_table_name, ...)}
     * assigned to it via {@code SET @var = CONCAT(...)}. Populated by {@link #visitSetAssignment}
     * and consulted by {@link #visitPrepareStatement} when the PREPARE's source is that same
     * {@code @var} and no concrete table could be extracted from it — mirrors Oracle's
     * {@code varValueStack}-based "variable-interrupted" tracking (Option C: unresolvedVarName).
     */
    private final java.util.Map<String, String> userVarInterruptingVar = new java.util.HashMap<>();

    /**
     * Bug fix (silent table drop in CONCAT-built dynamic SQL): user variable name (without the
     * leading {@code @}) → the full expression text it was assigned via
     * {@code SET @var = CONCAT(...)}. Unlike {@link #userVarInterruptingVar} (which only tracks
     * the *interrupting* identifier immediately before the interrupt point), this retains the
     * entire assignment so that when {@code PREPARE ... FROM @var} is later encountered, ALL
     * literal string fragments of the CONCAT can be scanned for an embedded table reference —
     * not just the one immediately adjacent to the interrupt point.
     *
     * <p>Stores the assignment's parse-tree node (not its text) so the fragments can be read off
     * the grammar's own {@code literal} nodes instead of being re-split out of text with a regex.
     */
    private final java.util.Map<String, MySqlParser.ExprContext> userVarAssignedExpr =
            new java.util.HashMap<>();

    /**
     * Maps a PREPARE statement-name (upper-cased) to the SQL source it was prepared from, so a
     * later {@code EXECUTE stmt} / {@code DEALLOCATE PREPARE stmt} can carry the same source text
     * instead of leaving {@link MySqlDynamicSqlInfo#getSqlSource()} null — previously those two
     * only ever recorded the statement name, silently dropping the SQL text already known from
     * PREPARE.
     */
    private final java.util.Map<String, String> preparedStmtSql = new java.util.HashMap<>();

    // Mirrors mysql-parser's own DYN_SQL_TABLE_PATTERN keyword set (visitPrepareStatement); a
    // literal argument ENDING in one of these keywords, immediately followed by a bare (unquoted,
    // unqualified) identifier argument, is the signature of `CONCAT('...FROM ', v_table_name, ...)`.
    // The adjacency itself is read off the grammar's callArgList (see findInterruptingVar); only
    // the keyword test below inspects the literal's own inner text, which no grammar can split.
    private static final java.util.List<String> CONCAT_INTERRUPT_KEYWORDS =
            java.util.List.of("FROM", "INTO", "UPDATE", "JOIN");

    public MySqlAnalysisVisitor(String objectName, String schema) {
        this.result = new MySqlRoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
    }

    public MySqlRoutineInfo getResult() { return result; }

    // ─────────────────────────────────────────────────────────────────────────
    // Routine identity
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateRoutine(MySqlParser.CreateRoutineContext ctx) {
        if (ctx.routineType == null) {
            // On a malformed CREATE (e.g. a construct the grammar still does not model),
            // ANTLR's error recovery can produce a CreateRoutineContext with the routineType
            // token unset. Don't crash - just skip identity extraction and let the
            // ANTLR syntax error (already recorded via the error listener) surface normally.
            return visitChildren(ctx);
        }
        String typeText = ctx.routineType.getText().toUpperCase();
        result.setType("FUNCTION".equals(typeText)
                ? MySqlRoutineInfo.RoutineType.FUNCTION
                : MySqlRoutineInfo.RoutineType.PROCEDURE);

        result.setName(extractObjectId(ctx.routineName().objectId()));
        if (ctx.routineName().schemaRef() != null) {
            result.setSchema(extractSimpleId(ctx.routineName().schemaRef().simpleId()));
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Trigger identity
    //
    // Only the trigger-specific header facts are handled here (name, target table,
    // timing, event). The body is walked with the ordinary visitChildren, so every
    // CALL / INSERT / UPDATE / DELETE / SELECT inside it is captured by exactly the
    // same methods that handle procedure bodies — nothing trigger-specific duplicates
    // that logic.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateTrigger(MySqlParser.CreateTriggerContext ctx) {
        result.setType(MySqlRoutineInfo.RoutineType.TRIGGER);

        result.setName(extractObjectId(ctx.triggerName().objectId()));
        if (ctx.triggerName().schemaRef() != null) {
            result.setSchema(extractSimpleId(ctx.triggerName().schemaRef().simpleId()));
        }
        if (ctx.triggerTiming != null) {
            result.setTriggerTiming(ctx.triggerTiming.getText().toUpperCase());
        }
        if (ctx.triggerEvent != null) {
            result.setTriggerEvent(ctx.triggerEvent.getText().toUpperCase());
        }
        if (ctx.tableRef() != null) {
            result.setTriggerTable(extractObjectId(ctx.tableRef().objectId()));
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // View identity
    //
    // Only the view's name is handled here. The defining SELECT is walked with the
    // ordinary visitChildren, so the underlying tables (FROM, JOINs, subqueries) are
    // recorded as SELECT table-ops by the same fromClause logic used for procedure
    // bodies — the view's dependencies come out of the existing lineage machinery.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateView(MySqlParser.CreateViewContext ctx) {
        result.setType(MySqlRoutineInfo.RoutineType.VIEW);

        result.setName(extractObjectId(ctx.viewName().objectId()));
        if (ctx.viewName().schemaRef() != null) {
            result.setSchema(extractSimpleId(ctx.viewName().schemaRef().simpleId()));
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Event identity (MySQL scheduled job)
    //
    // Captures the event name, its ON SCHEDULE clause, its ENABLE/DISABLE status and the
    // DO body. The body is walked with the ordinary visitChildren, so every CALL / DML
    // inside it is captured by exactly the same methods that handle procedure bodies —
    // which is what turns a scheduled event into a real entry point with outbound edges.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateEvent(MySqlParser.CreateEventContext ctx) {
        result.setType(MySqlRoutineInfo.RoutineType.EVENT);

        result.setName(extractObjectId(ctx.eventName().objectId()));
        if (ctx.eventName().schemaRef() != null) {
            result.setSchema(extractSimpleId(ctx.eventName().schemaRef().simpleId()));
        }
        if (ctx.eventSchedule() != null) {
            result.setEventSchedule(ctx.eventSchedule().getText());
        }
        if (ctx.eventStatus() != null && ctx.eventStatus().ENABLE() != null) {
            result.setEventEnabled(true);
        } else if (ctx.eventStatus() != null && ctx.eventStatus().DISABLE() != null) {
            result.setEventEnabled(false);
        }
        if (ctx.eventBody() != null) {
            result.setEventBody(ctx.eventBody().getText());
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Index identity
    //
    // Captures the index name, its target table, the indexed columns (in declaration
    // order), the UNIQUE/FULLTEXT/SPATIAL flavour and the USING index type. There is
    // no body to walk, so nothing else is recorded — but the target table is stored
    // under indexTable so the index's dependency is visible alongside the trigger
    // table captured the same way above.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateIndex(MySqlParser.CreateIndexContext ctx) {
        result.setType(MySqlRoutineInfo.RoutineType.INDEX);

        result.setName(extractObjectId(ctx.indexName().objectId()));
        if (ctx.indexName().schemaRef() != null) {
            result.setSchema(extractSimpleId(ctx.indexName().schemaRef().simpleId()));
        }
        if (ctx.indexKind != null) {
            result.setIndexKind(ctx.indexKind.getText().toUpperCase());
        }
        if (ctx.indexTypeClause() != null && ctx.indexTypeClause().indexType != null) {
            result.setIndexType(ctx.indexTypeClause().indexType.getText().toUpperCase());
        }
        if (ctx.tableRef() != null) {
            result.setIndexTable(extractObjectId(ctx.tableRef().objectId()));
        }
        for (MySqlParser.IndexColumnContext col : ctx.indexColumn()) {
            result.getIndexColumns().add(extractObjectId(col.objectId()));
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ALTER TABLE
    //
    // Two things are recorded. The altered table, as an ALTER table-op — previously
    // the whole statement went to the unknownStatement catch-all and the table was
    // invisible. And, for every FOREIGN KEY constraint the statement adds, the
    // declared dependency edge: the referenced table as a REFERENCE table-op AND a
    // structured MySqlForeignKeyInfo carrying both column lists.
    //
    // The edge is the valuable half. Table-to-table relationships elsewhere in this
    // analyzer are inferred from how DML happens to be written; a FOREIGN KEY is the
    // schema stating the relationship outright, and it was being discarded whole.
    //
    // Both spellings feed this one path: the table-level `ADD [CONSTRAINT x] FOREIGN
    // KEY (c) REFERENCES t(c)` and the column-level `ADD COLUMN c INT REFERENCES
    // t(c)`. Sub-forms absorbed by the permissive alterOtherAction tail contribute
    // the ALTER op and nothing else — their tokens were matched by a wildcard, so
    // reading anything out of them would be a guess presented as a finding (same
    // rule visitStmtUnknown follows).
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitAlterTable(MySqlParser.AlterTableContext ctx) {
        int line = ctx.getStart().getLine();
        MySqlParser.TableRefContext target = ctx.tableRef();

        // A standalone `ALTER TABLE …` script has no enclosing routine, so nothing has
        // set an identity yet. Mirrors visitCreateIndex, which names the result after the
        // index. Inside a procedure/trigger body the type is already set and untouched.
        if (result.getType() == null) {
            result.setType(MySqlRoutineInfo.RoutineType.ALTER_TABLE);
            if (target != null) {
                result.setName(extractObjectId(target.objectId()));
                if (target.schemaRef() != null) {
                    result.setSchema(extractSimpleId(target.schemaRef().simpleId()));
                }
            }
            result.setLineStart(line);
            result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : line);
        }

        addTableOp(target, MySqlTableOpInfo.DmlType.ALTER, line);

        String tableName   = target != null ? extractObjectId(target.objectId()) : null;
        String tableSchema = (target != null && target.schemaRef() != null)
                ? extractSimpleId(target.schemaRef().simpleId()) : null;

        for (MySqlParser.AlterActionContext action : ctx.alterAction()) {
            if (action instanceof MySqlParser.AlterAddConstraintContext add
                    && add.tableConstraint() instanceof MySqlParser.FkTableConstraintContext fk) {
                recordForeignKey(tableName, tableSchema,
                        constraintNameOf(fk.constraintName()),
                        columnNames(fk.columnList(0)),
                        fk.tableRef(),
                        fk.columnList().size() > 1 ? columnNames(fk.columnList(1)) : List.of(),
                        fk.getStart().getLine());
            } else if (action instanceof MySqlParser.AlterAddColumnContext addCol) {
                recordInlineColumnReferences(addCol.columnDef(), tableName, tableSchema);
            }
        }
        return visitChildren(ctx);
    }

    /** Column-level {@code col type REFERENCES t(c)} — the same edge, written inline. */
    private void recordInlineColumnReferences(MySqlParser.ColumnDefContext def,
                                              String tableName, String tableSchema) {
        if (def == null) return;
        String column = extractSimpleId(def.simpleId());
        for (MySqlParser.ColumnConstraintContext cc : def.columnConstraint()) {
            if (cc.REFERENCES() == null) continue;
            recordForeignKey(tableName, tableSchema, null,
                    column != null ? List.of(column) : List.<String>of(),
                    cc.tableRef(),
                    cc.columnList() != null ? columnNames(cc.columnList()) : List.<String>of(),
                    cc.getStart().getLine());
        }
    }

    private void recordForeignKey(String tableName, String tableSchema, String constraintName,
                                  List<String> columns, MySqlParser.TableRefContext refTable,
                                  List<String> refColumns, int line) {
        if (refTable == null) return;

        // The referenced table as a table-op too, so consumers that only walk tableOps for
        // lineage still see both ends of the edge.
        addTableOp(refTable, MySqlTableOpInfo.DmlType.REFERENCE, line);

        MySqlForeignKeyInfo fk = new MySqlForeignKeyInfo();
        fk.setConstraintName(constraintName);
        fk.setTable(tableName);
        fk.setSchema(tableSchema);
        fk.setReferencedTable(extractObjectId(refTable.objectId()));
        if (refTable.schemaRef() != null) {
            fk.setReferencedSchema(extractSimpleId(refTable.schemaRef().simpleId()));
        }
        fk.getColumns().addAll(columns);
        fk.getReferencedColumns().addAll(refColumns);
        fk.setLine(line);
        result.getForeignKeys().add(fk);
    }

    /** {@code CONSTRAINT} with no name after it is legal MySQL, hence the null-safety. */
    private String constraintNameOf(MySqlParser.ConstraintNameContext ctx) {
        return (ctx == null || ctx.objectId() == null) ? null : extractObjectId(ctx.objectId());
    }

    private List<String> columnNames(MySqlParser.ColumnListContext ctx) {
        if (ctx == null) return List.of();
        return ctx.simpleId().stream().map(this::extractSimpleId).toList();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parameters
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitParam(MySqlParser.ParamContext ctx) {
        MySqlParamInfo p = new MySqlParamInfo();
        p.setName(extractObjectId(ctx.paramName().objectId()));
        p.setDataType(formatDataType(ctx.dataType()));
        p.setLine(ctx.getStart().getLine());
        if (ctx.direction != null) {
            p.setDirection(switch (ctx.direction.getType()) {
                case MySqlParser.OUT   -> MySqlParamInfo.Direction.OUT;
                case MySqlParser.INOUT -> MySqlParamInfo.Direction.INOUT;
                default                -> MySqlParamInfo.Direction.IN;
            });
        }
        result.getParams().add(p);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE variables
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareVar(MySqlParser.DeclareVarContext ctx) {
        String dataType = formatDataType(ctx.dataType());
        String defVal   = ctx.expr() != null ? ctx.expr().getText() : null;
        int    line     = ctx.getStart().getLine();
        for (MySqlParser.ObjectIdContext idCtx : ctx.varNameList().objectId()) {
            MySqlVariableInfo v = new MySqlVariableInfo();
            v.setName(extractObjectId(idCtx));
            v.setDataType(dataType);
            v.setDefaultValue(defVal);
            v.setLine(line);
            result.getVariables().add(v);
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE cursors
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareCursor(MySqlParser.DeclareCursorContext ctx) {
        MySqlCursorInfo c = new MySqlCursorInfo();
        c.setName(extractSimpleId(ctx.objectId().simpleId()));
        // Bug 4 fix: ctx.getText() concatenates only visible-channel tokens with no separators
        // (e.g. "SELECTid,statusFROMorders..."). Use the input-stream slice by start/stop token
        // index instead, preserving original whitespace/comments — the standard ANTLR pattern,
        // mirrored from DB2's originalText()/StatementTextCapture approach.
        c.setSelectText(originalText(ctx.selectStatement()));
        c.setLine(ctx.getStart().getLine());
        result.getCursors().add(c);
        return visitChildren(ctx);  // still find table ops inside cursor SELECT
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CALL statement — call-tree edges
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCallStatement(MySqlParser.CallStatementContext ctx) {
        MySqlCallInfo call = new MySqlCallInfo();
        call.setName(extractObjectId(ctx.routineName().objectId()));
        if (ctx.routineName().schemaRef() != null) {
            call.setSchema(extractSimpleId(ctx.routineName().schemaRef().simpleId()));
        }
        call.setLine(ctx.getStart().getLine());
        call.setHandlerIndex(currentHandlerIndex);
        result.getCalls().add(call);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Function calls inside expressions — user-routine invocations (call-tree)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Common MySQL built-in functions to exclude — only user-defined routine
     * invocations become call-tree edges. Names compared case-insensitively.
     */
    private static final Set<String> BUILTIN_FUNCTIONS = Set.of(
        "ABS","ACOS","ADDDATE","ADDTIME","ASCII","ASIN","ATAN","ATAN2","AVG",
        "BIN","BIT_AND","BIT_COUNT","BIT_LENGTH","BIT_OR","BIT_XOR",
        "CAST","CEIL","CEILING","CHAR","CHAR_LENGTH","CHARACTER_LENGTH","COALESCE",
        "CONCAT","CONCAT_WS","CONV","CONVERT","COS","COT","COUNT","CRC32","CURDATE",
        "CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP","CURRENT_USER","CURTIME",
        "DATABASE","DATE","DATE_ADD","DATE_FORMAT","DATE_SUB","DATEDIFF","DAY",
        "DAYNAME","DAYOFMONTH","DAYOFWEEK","DAYOFYEAR","DEGREES","ELT","EXP",
        "EXPORT_SET","EXTRACT","FIELD","FIND_IN_SET","FLOOR","FORMAT","FROM_DAYS",
        "FROM_UNIXTIME","GREATEST","GROUP_CONCAT","HEX","HOUR","IF","IFNULL","INSTR",
        "ISNULL","JSON_ARRAY","JSON_EXTRACT","JSON_OBJECT","LAST_DAY","LAST_INSERT_ID",
        "LCASE","LEAST","LEFT","LENGTH","LN","LOCATE","LOG","LOG10","LOG2","LOWER",
        "LPAD","LTRIM","MAKEDATE","MAKETIME","MAX","MD5","MID","MIN","MINUTE","MOD",
        "MONTH","MONTHNAME","NOW","NULLIF","OCT","ORD","PERIOD_ADD","PERIOD_DIFF","PI",
        "POSITION","POW","POWER","QUARTER","QUOTE","RADIANS","RAND","REGEXP_REPLACE",
        "REGEXP_SUBSTR","REPEAT","REPLACE","REVERSE","RIGHT","ROUND","RPAD","RTRIM",
        "SEC_TO_TIME","SECOND","SHA","SHA1","SHA2","SIGN","SIN","SLEEP","SOUNDEX",
        "SPACE","SQRT","STD","STDDEV","STDDEV_POP","STDDEV_SAMP","STR_TO_DATE","SUBDATE",
        "SUBSTR","SUBSTRING","SUBSTRING_INDEX","SUBTIME","SUM","SYSDATE","TAN","TIME",
        "TIME_FORMAT","TIME_TO_SEC","TIMEDIFF","TIMESTAMP","TIMESTAMPADD","TIMESTAMPDIFF",
        "TO_DAYS","TO_SECONDS","TRIM","TRUNCATE","UCASE","UNHEX","UNIX_TIMESTAMP","UPPER",
        "USER","UTC_DATE","UTC_TIME","UTC_TIMESTAMP","UUID","VALUES","VAR_POP","VAR_SAMP",
        "VARIANCE","VERSION","WEEK","WEEKDAY","WEEKOFYEAR","YEAR","YEARWEEK",
        // Session / info / lock built-ins (no-arg style — otherwise mis-read as user calls).
        // NOTE: deliberately excluding high-collision names like SCHEMA that could be real
        // user-routine names invoked unqualified.
        "ROW_COUNT","FOUND_ROWS","CONNECTION_ID","SESSION_USER","SYSTEM_USER",
        "CURRENT_ROLE","LOCALTIME","LOCALTIMESTAMP","INET_ATON","INET_NTOA",
        "GET_LOCK","RELEASE_LOCK","IS_FREE_LOCK","IS_USED_LOCK","BENCHMARK",
        "UUID_SHORT",
        // Additional string built-ins.
        "FROM_BASE64","TO_BASE64","MAKE_SET","STRCMP","INSERT",
        // Additional date/time built-ins.
        "CONVERT_TZ",
        // JSON aggregate + JSON manipulation built-ins.
        "JSON_ARRAYAGG","JSON_OBJECTAGG",
        "JSON_UNQUOTE","JSON_SET","JSON_INSERT","JSON_REPLACE","JSON_REMOVE",
        "JSON_KEYS","JSON_LENGTH","JSON_TYPE","JSON_VALID","JSON_CONTAINS",
        "JSON_MERGE_PATCH","JSON_QUOTE","JSON_TABLE",
        // Window / analytic functions (engine keywords, never user routines).
        // NOTE: still deliberately excluding SCHEMA (high-collision) per note above.
        "ROW_NUMBER","RANK","DENSE_RANK","PERCENT_RANK","CUME_DIST","NTILE",
        "LAG","LEAD","FIRST_VALUE","LAST_VALUE","NTH_VALUE"
    );

    @Override
    public Void visitFunctionCall(MySqlParser.FunctionCallContext ctx) {
        MySqlParser.FunctionRefContext ref = ctx.functionRef();
        String name = extractSimpleId(ref.simpleId());
        String schema = ref.schemaRef() != null
                ? extractSimpleId(ref.schemaRef().simpleId()) : null;

        // Exclude built-ins; only unqualified/qualified user-routine names become edges.
        if (name != null && (schema != null || !BUILTIN_FUNCTIONS.contains(name.toUpperCase()))) {
            String qualified = (schema != null && !schema.isBlank()) ? schema + "." + name : name;
            boolean dup = result.getCalls().stream()
                    .anyMatch(c -> qualified.equalsIgnoreCase(c.qualifiedName()));
            if (!dup) {
                MySqlCallInfo call = new MySqlCallInfo();
                call.setName(name);
                call.setSchema(schema);
                call.setLine(ctx.getStart().getLine());
                call.setHandlerIndex(currentHandlerIndex);
                result.getCalls().add(call);
            }
        }
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML table ops — SELECT (via FROM clause)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitFromClause(MySqlParser.FromClauseContext ctx) {
        for (MySqlParser.TableReferenceContext tr : ctx.tableReferences().tableReference()) {
            processTableReference(tr);
        }
        return visitChildren(ctx);   // recurse for nested subquery SELECTs / CTEs
    }

    /**
     * Walk one {@code tableReference} (a base tableAtom followed by zero or more joinClauses).
     * The base atom becomes a SELECT table op; each JOINed atom becomes its own SELECT table op
     * AND a MySqlJoinInfo attached to the base op — all via ANTLR rule contexts, no text scanning.
     */
    private void processTableReference(MySqlParser.TableReferenceContext tr) {
        MySqlTableOpInfo baseOp = tableOpForAtom(tr.tableAtom());
        attachJoins(tr, baseOp);
    }

    /** Adds each JOINed atom as its own SELECT table op plus a MySqlJoinInfo on {@code baseOp}. */
    private void attachJoins(MySqlParser.TableReferenceContext tr, MySqlTableOpInfo baseOp) {
        // alias->table map for the whole join chain (base atom + every joined atom), so BOTH
        // sides of each join can be resolved from its ON-condition aliases.
        java.util.Map<String, String> aliasMap = buildAliasMap(tr);
        int queryId = tr.getStart().getLine();

        java.util.List<MySqlParser.JoinClauseContext> clauses = tr.joinClause();
        for (int ji = 0; ji < clauses.size(); ji++) {
            MySqlParser.JoinClauseContext jc = clauses.get(ji);
            MySqlTableOpInfo joinedOp = tableOpForAtom(jc.tableAtom());
            if (baseOp == null) continue;

            if (jc.tableAtom() instanceof MySqlParser.AtomTableContext jat) {
                MySqlParser.TableRefContext jref = jat.tableRef();
                MySqlJoinInfo j = new MySqlJoinInfo();
                j.setJoinType(joinTypeText(jc.joinType()));
                j.setJoinedTable(extractObjectId(jref.objectId()));
                if (jref.schemaRef() != null) {
                    j.setJoinedTableSchema(extractSimpleId(jref.schemaRef().simpleId()));
                }
                if (jat.alias() != null) {
                    j.setJoinedTableAlias(aliasText(jat.alias()));
                }
                if (jc.joinCondition() != null && jc.joinCondition().expr() != null) {
                    j.setCondition(textWithoutComments(jc.joinCondition().expr()));
                }
                j.setLine(jc.getStart().getLine());
                j.setQueryId(queryId);
                resolveLeftSide(j,
                        jc.joinCondition() != null ? jc.joinCondition().expr() : null, aliasMap);
                if (j.getLeftTable() == null) {
                    // Style-variance fix: a join written with `USING (col)`, or with no condition
                    // at all (CROSS/comma-style JOIN), carries no ON expression, so the
                    // qualifier-based resolution above has nothing to read and the left side was
                    // silently left null even though it is unambiguous from the chain's shape:
                    // it is the atom immediately preceding this join clause.
                    setLeftFromAtom(j, ji == 0 ? tr.tableAtom() : clauses.get(ji - 1).tableAtom());
                }
                baseOp.getJoins().add(j);
            }
        }
    }

    /** Sets a join's left side directly from a plain table atom (used when there is no ON condition). */
    private void setLeftFromAtom(MySqlJoinInfo j, MySqlParser.TableAtomContext atom) {
        if (!(atom instanceof MySqlParser.AtomTableContext at)) return;
        String tbl = extractObjectId(at.tableRef().objectId());
        if (tbl == null) return;
        j.setLeftTable(tbl.toUpperCase());
        j.setLeftTableAlias(at.alias() != null
                ? aliasText(at.alias()).toUpperCase() : tbl.toUpperCase());
    }

    /** Build alias->table (both upper-cased) for the base atom and each joined atom of a tableReference. */
    private java.util.Map<String, String> buildAliasMap(MySqlParser.TableReferenceContext tr) {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        addAtomAlias(m, tr.tableAtom());
        for (MySqlParser.JoinClauseContext jc : tr.joinClause()) {
            addAtomAlias(m, jc.tableAtom());
        }
        return m;
    }

    private void addAtomAlias(java.util.Map<String, String> m, MySqlParser.TableAtomContext atom) {
        if (!(atom instanceof MySqlParser.AtomTableContext at)) return;
        String tbl = extractObjectId(at.tableRef().objectId());
        if (tbl == null) return;
        tbl = tbl.toUpperCase();
        if (at.alias() != null) m.put(aliasText(at.alias()).toUpperCase(), tbl);
        m.put(tbl, tbl);
    }

    /**
     * Resolve leftTable/leftTableAlias from the ON-condition alias that is NOT the joined table.
     *
     * <p>Grammar-driven: instead of regex-scanning the condition's raw text for {@code name.}
     * prefixes (which also matched qualifier-looking text inside string literals and comments),
     * this walks the ON-condition's real parse tree and collects the qualifier of every
     * {@code columnRef} / {@code functionRef} node in source order.
     */
    private void resolveLeftSide(MySqlJoinInfo j, MySqlParser.ExprContext cond,
                                 java.util.Map<String, String> aliasMap) {
        if (cond == null) return;
        String joinedAlias = j.getJoinedTableAlias() != null ? j.getJoinedTableAlias().toUpperCase() : null;
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        collectQualifiers(cond, seen);
        for (String a : seen) {
            if (joinedAlias != null && a.equals(joinedAlias)) continue;
            String t = aliasMap.get(a);
            if (t != null) { j.setLeftTableAlias(a); j.setLeftTable(t); return; }
        }
    }

    /**
     * Depth-first, source-order collection of every dotted qualifier in an expression subtree:
     * the {@code tableRef} of a {@code columnRef} ({@code a.col} / {@code s.t.col}) and the
     * {@code schemaRef} of a {@code functionRef} ({@code s.fn(...)}). Multi-part qualifiers
     * contribute each part separately, matching what the previous regex saw.
     */
    private void collectQualifiers(org.antlr.v4.runtime.tree.ParseTree node,
                                   java.util.Set<String> out) {
        if (node instanceof MySqlParser.ColumnRefContext cr && cr.tableRef() != null) {
            MySqlParser.TableRefContext tref = cr.tableRef();
            if (tref.schemaRef() != null) {
                out.add(unquoteIdentifier(tref.schemaRef().getText()).toUpperCase());
            }
            if (tref.objectId() != null) {
                out.add(unquoteIdentifier(tref.objectId().getText()).toUpperCase());
            }
        } else if (node instanceof MySqlParser.FunctionRefContext fr && fr.schemaRef() != null) {
            out.add(unquoteIdentifier(fr.schemaRef().getText()).toUpperCase());
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectQualifiers(node.getChild(i), out);
        }
    }

    /**
     * Adds a SELECT table op for the given tableAtom and returns it. For a plain table atom this
     * is the referenced table; for a parenthesised list of table references it recurses (returning
     * the first base op); subquery atoms are handled by the visitChildren recursion in the caller.
     */
    private MySqlTableOpInfo tableOpForAtom(MySqlParser.TableAtomContext atom) {
        if (atom instanceof MySqlParser.AtomTableContext at) {
            if (isCte(at.tableRef())) return null; // a CTE reference, not a base table — see cteNames doc
            MySqlTableOpInfo op = addTableOp(at.tableRef(), MySqlTableOpInfo.DmlType.SELECT,
                    at.getStart().getLine());
            if (op != null && at.alias() != null) op.setAlias(aliasText(at.alias()));
            return op;
        }
        if (atom instanceof MySqlParser.AtomParenRefContext paren) {
            MySqlTableOpInfo first = null;
            for (MySqlParser.TableReferenceContext tr : paren.tableReferences().tableReference()) {
                processTableReference(tr);
                if (first == null && !result.getTableOps().isEmpty()) {
                    first = result.getTableOps().get(result.getTableOps().size() - 1);
                }
            }
            return first;
        }
        return null;   // AtomSubquery — inner SELECT handled by visitChildren recursion
    }

    /** Render a joinType context as spaced upper-case text (e.g. "LEFT OUTER"), or "JOIN" if absent. */
    private String joinTypeText(MySqlParser.JoinTypeContext jt) {
        if (jt == null) return "JOIN";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < jt.getChildCount(); i++) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(jt.getChild(i).getText().toUpperCase());
        }
        return sb.length() == 0 ? "JOIN" : sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML table ops — INSERT / UPDATE / DELETE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitInsertStatement(MySqlParser.InsertStatementContext ctx) {
        addTableOp(ctx.tableRef(), MySqlTableOpInfo.DmlType.INSERT, ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    /** REPLACE [INTO] t … — recorded as a REPLACE table op (delete-then-insert). */
    @Override
    public Void visitReplaceStatement(MySqlParser.ReplaceStatementContext ctx) {
        addTableOp(ctx.tableRef(), MySqlTableOpInfo.DmlType.REPLACE, ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    @Override
    public Void visitUpdateStatement(MySqlParser.UpdateStatementContext ctx) {
        // First tableReference: base atom is the UPDATE target; any JOINed atoms are sources.
        // Additional comma-separated references (multi-table UPDATE) are source tables too.
        java.util.List<MySqlParser.TableReferenceContext> refs =
                ctx.tableReferences().tableReference();
        for (int i = 0; i < refs.size(); i++) {
            MySqlParser.TableReferenceContext tr = refs.get(i);
            if (i == 0) {
                MySqlTableOpInfo target = updateTargetOp(tr.tableAtom());
                attachJoins(tr, target);
            } else {
                processTableReference(tr);
            }
        }
        return visitChildren(ctx);
    }

    /** Adds an UPDATE table op for a plain target atom; delegates other atom kinds to the SELECT path. */
    private MySqlTableOpInfo updateTargetOp(MySqlParser.TableAtomContext atom) {
        if (atom instanceof MySqlParser.AtomTableContext at) {
            MySqlTableOpInfo op = addTableOp(at.tableRef(), MySqlTableOpInfo.DmlType.UPDATE,
                    at.getStart().getLine());
            if (op != null && at.alias() != null) op.setAlias(aliasText(at.alias()));
            return op;
        }
        return tableOpForAtom(atom);
    }

    @Override
    public Void visitDeleteStatement(MySqlParser.DeleteStatementContext ctx) {
        addTableOp(ctx.tableRef(), MySqlTableOpInfo.DmlType.DELETE, ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitPrepareStatement(MySqlParser.PrepareStatementContext ctx) {
        MySqlDynamicSqlInfo d = new MySqlDynamicSqlInfo();
        d.setKind(MySqlDynamicSqlInfo.Kind.PREPARE);
        d.setStmtName(extractSimpleId(ctx.simpleId()));
        String sqlSource = ctx.dynamicSqlSource().getText();
        d.setSqlSource(sqlSource);
        d.setLine(ctx.getStart().getLine());
        // Bug 9 (§DML-audit, MySQL — lower priority, literal-only scope): a `PREPARE ... FROM
        // '<fully literal SQL string>'` (no @variable/CONCAT indirection) has its table name
        // known at parse time. Modeled on Oracle's DYNAMIC bucket.
        boolean tableFound = extractLiteralDynamicSqlTable(ctx.dynamicSqlSource(),
                ctx.getStart().getLine());
        // Bug 8 (§DML-audit, MySQL): `PREPARE stmt FROM @var` where @var was assigned via
        // `SET @var = CONCAT('...FROM ', v_table_name, ...)` — no literal table name is available
        // (extractLiteralDynamicSqlTable above is a no-op for a bare @var reference), so flag the
        // interrupting variable the same way Oracle's Option C does.
        if (!tableFound && ctx.dynamicSqlSource().AT_VAR() != null) {
            String varName = ctx.dynamicSqlSource().AT_VAR().getText().substring(1);
            // Silent-table-drop fix: before falling back to flagging the interrupting variable as
            // unresolved, scan ALL literal string fragments of the SET @var = CONCAT(...)
            // assignment (not just the one immediately before the interrupt point) for an
            // embedded table reference — handles text (e.g. IFNULL(...)) intervening between a
            // literal `FROM <table>` fragment and the variable-interrupt point.
            MySqlParser.ExprContext assignedExpr = userVarAssignedExpr.get(varName);
            tableFound = extractTableFromConcatLiterals(assignedExpr, ctx.getStart().getLine());
            if (!tableFound) {
                String interruptingVar = userVarInterruptingVar.get(varName);
                if (interruptingVar != null) {
                    d.setUnresolvedVarName(interruptingVar);
                }
            }
        }
        if (d.getStmtName() != null && sqlSource != null) {
            preparedStmtSql.put(d.getStmtName().toUpperCase(), sqlSource);
        }
        result.getDynamicSql().add(d);
        return null;
    }

    @Override
    public Void visitSetAssignment(MySqlParser.SetAssignmentContext ctx) {
        // Bug 8 (MySQL portion): track `SET @var = CONCAT('...FROM ', v_table_name, ...)` so a
        // later `PREPARE stmt FROM @var` can flag the interrupting variable as unresolved, mirroring
        // Oracle's varValueStack-based "variable-interrupted" tracking.
        if (ctx.setTarget() != null && ctx.expr() != null) {
            String target = ctx.setTarget().getText();
            if (target != null && target.startsWith("@")) {
                String varName = target.substring(1);
                userVarAssignedExpr.put(varName, ctx.expr());
                String interrupting = findInterruptingVar(ctx.expr());
                if (interrupting != null) {
                    userVarInterruptingVar.put(varName, interrupting);
                } else {
                    userVarInterruptingVar.remove(varName);
                }
            }
        }
        return visitChildren(ctx);
    }

    // Matches the object name after a table-introducing keyword inside a literal dynamic-SQL
    // string, mirroring plsql-parser's DYN_SQL_TABLE_PATTERN (Oracle) for cross-dialect parity.
    private static final java.util.regex.Pattern DYN_SQL_TABLE_PATTERN = java.util.regex.Pattern.compile(
            // Style-variance fix: the object name may be backtick-quoted (`orders`), and the
            // separating whitespace may be a MySQL escape sequence (\n / \t / \r) rather than a
            // real space, since the dynamic SQL lives inside a quoted string literal. Both
            // spellings previously failed to match and the table was silently dropped.
            "(?i)(?:FROM|INTO|UPDATE|INSERT(?:\\s|\\\\[nrt])+INTO|DELETE(?:\\s|\\\\[nrt])+FROM|JOIN)"
                    + "(?:\\s|\\\\[nrt])+"
                    + "`?([A-Za-z_][A-Za-z0-9_$#]*)`?(?:\\.`?([A-Za-z_][A-Za-z0-9_$#]*)`?)?");

    // ─────────────────────────────────────────────────────────────────────────
    // Grammar-first recursive dynamic-SQL parse (regex → fallback only)
    //
    // A fully-literal PREPARE ... FROM '<sql>' is real SQL, so it is re-parsed with this same
    // MySQL grammar and its table targets read off the nested parse tree by the same visitor the
    // static path uses. DYN_SQL_TABLE_PATTERN survives only as the recovery path for literals the
    // grammar cannot attribute a table to (CONCAT fragments, \n-escaped separators …), logged once
    // and counted.
    // ─────────────────────────────────────────────────────────────────────────
    private static final int DYN_PARSE_MAX_DEPTH = 3;
    private static final int DYN_PARSE_MAX_CHARS = 64 * 1024;
    private static final ThreadLocal<Integer> DYN_PARSE_DEPTH = ThreadLocal.withInitial(() -> 0);
    private static final java.util.concurrent.atomic.AtomicLong DYN_SQL_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicBoolean DYN_FALLBACK_LOGGED =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Number of literals the recursive grammar parse could not attribute a table to. */
    public static long dynamicSqlRegexFallbacks() { return DYN_SQL_REGEX_FALLBACKS.get(); }

    /**
     * Extracts a table name from a dynamic-SQL string that is a single, fully-literal quoted
     * string (no {@code @variable}/{@code CONCAT(...)} indirection) and records it as a
     * {@code DYNAMIC} table op. Anything else (variable interpolation, unresolved expressions)
     * is left alone — this deliberately stops at the literal-only case.
     *
     * <p>The literal-vs-{@code @variable} decision comes from the grammar's own
     * {@code dynamicSqlSource} alternatives, not from inspecting quote characters in text.
     */
    private boolean extractLiteralDynamicSqlTable(MySqlParser.DynamicSqlSourceContext src, int line) {
        if (src == null) return false;
        org.antlr.v4.runtime.tree.TerminalNode tok =
                src.SQ_STRING() != null ? src.SQ_STRING() : src.DQ_STRING();
        if (tok == null) return false; // AT_VAR — out of scope for this fix
        String inner = literalInner(tok.getText());

        // Grammar first: recursively parse the literal with the same MySQL grammar.
        java.util.List<MySqlTableOpInfo> grammarOps = nestedParseDynamicSqlTables(inner);
        if (!grammarOps.isEmpty()) {
            java.util.Set<String> seen = new java.util.HashSet<>();
            boolean added = false;
            for (MySqlTableOpInfo g : grammarOps) {
                if (g.getTableName() == null || g.getTableName().isEmpty()) continue;
                String key = (g.getSchemaName() != null ? g.getSchemaName() + "." : "")
                        + g.getTableName();
                if (!seen.add(key.toLowerCase())) continue;
                MySqlTableOpInfo op = new MySqlTableOpInfo();
                op.setDmlType(MySqlTableOpInfo.DmlType.DYNAMIC);
                op.setTableName(g.getTableName());
                op.setSchemaName(g.getSchemaName());
                op.setLine(line);
                result.getTableOps().add(op);
                added = true;
            }
            if (added) return true;
        }

        // Recovery path: not a statement this grammar can attribute a table to. Logged once,
        // counted, so the regex retirement rate of the migration is observable.
        long fallbackNo = DYN_SQL_REGEX_FALLBACKS.incrementAndGet();
        if (DYN_FALLBACK_LOGGED.compareAndSet(false, true)) {
            log.warn("MySQL dynamic-SQL recursive parse produced no table target; using legacy "
                    + "regex fallback (#{})", fallbackNo);
        }
        java.util.regex.Matcher m = DYN_SQL_TABLE_PATTERN.matcher(inner);
        if (m.find()) {
            MySqlTableOpInfo op = new MySqlTableOpInfo();
            op.setDmlType(MySqlTableOpInfo.DmlType.DYNAMIC);
            op.setTableName(matchedTableName(m));
            op.setLine(line);
            result.getTableOps().add(op);
            return true;
        }
        return false;
    }

    /**
     * Recursively parse a literal dynamic-SQL string with the MySQL grammar and return the table
     * operations the nested visitor recorded; empty when the literal is not a statement this
     * grammar can parse (callers then run the legacy regex), or when a guard fires.
     */
    private java.util.List<MySqlTableOpInfo> nestedParseDynamicSqlTables(String innerSql) {
        if (innerSql == null || innerSql.isBlank()) return java.util.List.of();
        if (innerSql.length() > DYN_PARSE_MAX_CHARS) return java.util.List.of();
        int depth = DYN_PARSE_DEPTH.get();
        if (depth >= DYN_PARSE_MAX_DEPTH) return java.util.List.of();
        DYN_PARSE_DEPTH.set(depth + 1);
        try {
            MySqlDynParseErrorListener errors = new MySqlDynParseErrorListener();
            com.plsql.mysql.grammar.MySqlLexer lexer = new com.plsql.mysql.grammar.MySqlLexer(
                    org.antlr.v4.runtime.CharStreams.fromString(innerSql));
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);
            MySqlParser parser = new MySqlParser(new org.antlr.v4.runtime.CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(errors);

            String kw = leadingKeyword(innerSql);
            org.antlr.v4.runtime.ParserRuleContext stmt;
            switch (kw) {
                case "SELECT": case "WITH": stmt = parser.selectStatement(); break;
                case "INSERT": stmt = parser.insertStatement(); break;
                case "UPDATE": stmt = parser.updateStatement(); break;
                case "DELETE": stmt = parser.deleteStatement(); break;
                case "REPLACE": stmt = parser.replaceStatement(); break;
                default: return java.util.List.of();
            }
            if (errors.hasErrors()) return java.util.List.of();
            MySqlAnalysisVisitor sub = new MySqlAnalysisVisitor("JAR$DYN_SQL", null);
            sub.visit(stmt);
            return new java.util.ArrayList<>(sub.getResult().getTableOps());
        } catch (Throwable t) {
            return java.util.List.of();
        } finally {
            DYN_PARSE_DEPTH.set(depth);
        }
    }

    /** The first bare word of the SQL text, upper-cased (empty when none). */
    private static String leadingKeyword(String sql) {
        int i = 0;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) i++;
        int j = i;
        while (j < sql.length() && Character.isLetter(sql.charAt(j))) j++;
        return sql.substring(i, j).toUpperCase();
    }

    /** Collects ANTLR syntax errors so a nested dynamic-SQL parse is trusted only when clean. */
    private static final class MySqlDynParseErrorListener extends org.antlr.v4.runtime.BaseErrorListener {
        private final java.util.List<String> errors = new java.util.ArrayList<>();

        @Override
        public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                                Object offendingSymbol, int line, int charPositionInLine,
                                String msg, org.antlr.v4.runtime.RecognitionException e) {
            errors.add(msg);
        }

        boolean hasErrors() { return !errors.isEmpty(); }
    }

    /**
     * Re-joins the (optionally schema-qualified) object name matched by
     * {@link #DYN_SQL_TABLE_PATTERN}. The two parts are captured separately only so that each can
     * carry its own optional backticks; the reported name stays the same {@code schema.table}
     * string the single-group pattern used to produce, minus the quoting.
     */
    private static String matchedTableName(java.util.regex.Matcher m) {
        return m.group(2) != null ? m.group(1) + "." + m.group(2) : m.group(1);
    }

    /**
     * Collapses the single-child expression chain ({@code expr → orExpr → … → primaryExpr}) down
     * to the {@code primaryExpr} alternative it wraps, or returns the first node that branches.
     */
    private static org.antlr.v4.runtime.tree.ParseTree collapseToPrimary(
            org.antlr.v4.runtime.tree.ParseTree n) {
        while (n != null && !(n instanceof MySqlParser.PrimaryExprContext)
                && n.getChildCount() == 1
                && n.getChild(0) instanceof org.antlr.v4.runtime.RuleContext) {
            n = n.getChild(0);
        }
        return n;
    }

    /** The single-quoted literal a {@code callArg} consists of, quotes included, else null. */
    private static String singleQuotedArg(MySqlParser.CallArgContext arg) {
        if (arg == null || arg.expr() == null) return null;
        org.antlr.v4.runtime.tree.ParseTree p = collapseToPrimary(arg.expr());
        if (p instanceof MySqlParser.PrimLiteralContext lit
                && lit.literal() != null && lit.literal().SQ_STRING() != null) {
            return lit.literal().SQ_STRING().getText();
        }
        return null;
    }

    /** The bare (unqualified, non-{@code @}) identifier a {@code callArg} consists of, else null. */
    private static String bareIdentifierArg(MySqlParser.CallArgContext arg) {
        if (arg == null || arg.expr() == null) return null;
        org.antlr.v4.runtime.tree.ParseTree p = collapseToPrimary(arg.expr());
        if (p instanceof MySqlParser.PrimColumnRefContext cr
                && cr.columnRef() != null && cr.columnRef().tableRef() == null) {
            return unquoteIdentifier(cr.columnRef().simpleId().getText());
        }
        return null;
    }

    /** Strips the surrounding quotes from a literal token's text. */
    private static String literalInner(String quoted) {
        return quoted.length() >= 2 ? quoted.substring(1, quoted.length() - 1) : "";
    }

    /**
     * Grammar-driven replacement for the old {@code CONCAT_INTERRUPT_PATTERN} regex: walks the
     * assignment expression's parse tree for any function call whose argument list contains a
     * single-quoted literal ending in a table-introducing keyword immediately followed by a bare
     * identifier argument, and returns that identifier — the runtime value that "interrupted" the
     * otherwise-literal dynamic SQL string. Returns null if there is no such pair.
     */
    private String findInterruptingVar(org.antlr.v4.runtime.tree.ParseTree node) {
        if (node instanceof MySqlParser.FunctionCallContext fc && fc.callArgList() != null) {
            java.util.List<MySqlParser.CallArgContext> args = fc.callArgList().callArg();
            for (int i = 0; i + 1 < args.size(); i++) {
                String quoted = singleQuotedArg(args.get(i));
                if (quoted == null || !endsWithInterruptKeyword(literalInner(quoted))) continue;
                String id = bareIdentifierArg(args.get(i + 1));
                if (id != null) return id;
            }
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            String found = findInterruptingVar(node.getChild(i));
            if (found != null) return found;
        }
        return null;
    }

    /** True if the literal's inner text ends with a table-introducing keyword (ignoring trailing spaces). */
    private static boolean endsWithInterruptKeyword(String inner) {
        // Style-variance fix: inside a quoted SQL string the whitespace after the keyword may be
        // written as a MySQL escape sequence (\n / \r / \t) rather than a literal space, which
        // stripTrailing() alone cannot see — `CONCAT('SELECT *\n FROM\n ', v_tbl)` then failed to
        // register as an interrupt at all.
        String s = inner.replaceAll("(?:\\s|\\\\[nrt])+$", "").toUpperCase();
        for (String kw : CONCAT_INTERRUPT_KEYWORDS) {
            if (s.endsWith(kw)
                    && (s.length() == kw.length()
                        || !Character.isLetterOrDigit(s.charAt(s.length() - kw.length() - 1)))) {
                return true;
            }
        }
        return false;
    }

    /** Every single-quoted literal in an expression subtree, in source order, quotes stripped. */
    private void collectSingleQuotedLiterals(org.antlr.v4.runtime.tree.ParseTree node,
                                             java.util.List<String> out) {
        if (node instanceof MySqlParser.LiteralContext lit && lit.SQ_STRING() != null) {
            out.add(literalInner(lit.SQ_STRING().getText()));
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectSingleQuotedLiterals(node.getChild(i), out);
        }
    }

    /**
     * Scans EVERY literal string fragment of a {@code SET @var = CONCAT(...)} assignment's
     * expression text for an embedded table reference (via {@link #DYN_SQL_TABLE_PATTERN}),
     * regardless of where that fragment sits relative to any variable-interrupt point. Fixes the
     * silent table drop where a literal {@code FROM <table>} fragment is followed by intervening
     * text (e.g. a function call) before the interrupting bare identifier, which defeats the
     * narrower {@link #findInterruptingVar} (immediately-adjacent argument pair only). The
     * fragments themselves come from the grammar's own {@code literal} nodes, not a text split.
     */
    private boolean extractTableFromConcatLiterals(MySqlParser.ExprContext assignedExpr, int line) {
        if (assignedExpr == null) return false;
        java.util.List<String> fragments = new java.util.ArrayList<>();
        collectSingleQuotedLiterals(assignedExpr, fragments);
        for (String fragment : fragments) {
            java.util.regex.Matcher m = DYN_SQL_TABLE_PATTERN.matcher(fragment);
            if (m.find()) {
                MySqlTableOpInfo op = new MySqlTableOpInfo();
                op.setDmlType(MySqlTableOpInfo.DmlType.DYNAMIC);
                op.setTableName(matchedTableName(m));
                op.setLine(line);
                result.getTableOps().add(op);
                return true;
            }
        }
        return false;
    }

    @Override
    public Void visitExecuteStatement(MySqlParser.ExecuteStatementContext ctx) {
        MySqlDynamicSqlInfo d = new MySqlDynamicSqlInfo();
        d.setKind(MySqlDynamicSqlInfo.Kind.EXECUTE);
        String stmtName = extractSimpleId(ctx.simpleId());
        d.setStmtName(stmtName);
        // Bug fix: carry the SQL text already known from the matching PREPARE, so EXECUTE stmt
        // doesn't leave sqlSource null when it is actually resolvable.
        if (stmtName != null) {
            String resolved = preparedStmtSql.get(stmtName.toUpperCase());
            if (resolved != null) d.setSqlSource(resolved);
        }
        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    @Override
    public Void visitDeallocateStatement(MySqlParser.DeallocateStatementContext ctx) {
        MySqlDynamicSqlInfo d = new MySqlDynamicSqlInfo();
        d.setKind(MySqlDynamicSqlInfo.Kind.DEALLOCATE);
        String stmtName = extractSimpleId(ctx.simpleId());
        d.setStmtName(stmtName);
        if (stmtName != null) {
            String resolved = preparedStmtSql.get(stmtName.toUpperCase());
            if (resolved != null) d.setSqlSource(resolved);
        }
        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * The SQL of the statement enclosing this context, with original spacing.
     *
     * <p>Walks UP to the nearest enclosing statement rather than taking the text of the table
     * reference itself, and is called from {@code addTableOp} so all five DML call sites are
     * covered by one place — the SELECT and UPDATE paths only ever receive a table atom, so
     * threading the statement context through their helpers would have meant changing each.
     *
     * <p>The text is what lets the shared {@code TableOperationEnricher} derive column-level detail
     * for this dialect without this module implementing any of it.
     */
    private String enclosingStatementText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        org.antlr.v4.runtime.ParserRuleContext c = ctx;
        org.antlr.v4.runtime.ParserRuleContext best = null;
        while (c != null) {
            String n = c.getClass().getSimpleName();
            if (n.endsWith("StatementContext") || n.endsWith("StmtContext")) { best = c; break; }
            c = c.getParent();
        }
        return best != null ? originalText(best) : null;
    }

    /** The source text of a context, with its original spacing. */
    private String originalText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        try {
            org.antlr.v4.runtime.CharStream cs = ctx.getStart().getInputStream();
            if (cs == null) return null;
            int a = ctx.getStart().getStartIndex();
            int b = ctx.getStop() != null ? ctx.getStop().getStopIndex() : -1;
            if (b < a) return null;
            return cs.getText(org.antlr.v4.runtime.misc.Interval.of(a, b));
        } catch (Exception e) {
            return null;   // text is a bonus; never fail the parse for it
        }
    }

    /**
     * The source text of a context with its original spacing, but with any comments embedded in it
     * removed — what a reader hand-copying the condition out of the SQL would write down.
     *
     * <p>Used for the captured ON-condition of a JOIN. Verbatim {@link #originalText} put whole
     * comments into the condition string
     * ({@code "a.id = /* the key *}{@code / b.id -- trailing\n AND a.x = b.x"}), which then travels
     * to the join edge shown on a table page. Of the other four dialects, SQL Server already strips
     * comments from its ON condition and Postgres never had them (it captures {@code getText()},
     * which is rebuilt from default-channel tokens); this brings MySQL in line with them while
     * keeping the original spacing that {@code getText()} would destroy.
     *
     * <p>The reason this is safe rather than a regex guess: it never inspects the condition text as
     * a whole. It walks the context's own terminal nodes — all of which the lexer already placed on
     * the default channel — and only scans the <i>gaps</i> between consecutive tokens. A gap
     * between two default-channel tokens can contain nothing but whitespace and hidden-channel
     * comments, by construction, so the scanner cannot possibly mistake the contents of a string
     * literal (say {@code WHERE note = 'rate -- see memo'}) for a comment: literals are tokens, and
     * tokens are copied through untouched.
     */
    private String textWithoutComments(org.antlr.v4.runtime.ParserRuleContext ctx) {
        String verbatim = originalText(ctx);
        if (verbatim == null) return null;
        try {
            org.antlr.v4.runtime.CharStream cs = ctx.getStart().getInputStream();
            if (cs == null) return verbatim;
            java.util.List<org.antlr.v4.runtime.Token> leaves = new java.util.ArrayList<>();
            collectTokens(ctx, leaves);
            if (leaves.isEmpty()) return verbatim;
            StringBuilder sb = new StringBuilder(verbatim.length());
            int prevStop = -1;
            for (org.antlr.v4.runtime.Token t : leaves) {
                int a = t.getStartIndex();
                int b = t.getStopIndex();
                if (a < 0 || b < a) continue;
                if (prevStop >= 0 && a > prevStop + 1) {
                    sb.append(stripCommentsFromGap(
                            cs.getText(org.antlr.v4.runtime.misc.Interval.of(prevStop + 1, a - 1))));
                }
                sb.append(cs.getText(org.antlr.v4.runtime.misc.Interval.of(a, b)));
                prevStop = b;
            }
            String out = sb.toString().strip();
            return out.isEmpty() ? verbatim : out;
        } catch (Exception e) {
            return verbatim;   // text is a bonus; never fail the parse for it
        }
    }

    /** Collects the context's terminal-node tokens, in source order, skipping EOF. */
    private void collectTokens(org.antlr.v4.runtime.tree.ParseTree node,
                               java.util.List<org.antlr.v4.runtime.Token> out) {
        if (node instanceof org.antlr.v4.runtime.tree.TerminalNode tn) {
            org.antlr.v4.runtime.Token t = tn.getSymbol();
            if (t != null && t.getType() != org.antlr.v4.runtime.Token.EOF) {
                out.add(t);
            }
            return;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectTokens(node.getChild(i), out);
        }
    }

    /**
     * Removes comments from the text between two adjacent default-channel tokens. That text is
     * whitespace and comments only, so the scan needs no string-literal or quoting awareness.
     * Newlines are kept so a multi-line condition still reads as one.
     */
    private String stripCommentsFromGap(String gap) {
        if (gap.indexOf('-') < 0 && gap.indexOf('#') < 0 && gap.indexOf('/') < 0) {
            return gap;   // the overwhelmingly common case: plain whitespace
        }
        StringBuilder sb = new StringBuilder(gap.length());
        int i = 0;
        int n = gap.length();
        while (i < n) {
            char ch = gap.charAt(i);
            if (ch == '#' || (ch == '-' && i + 1 < n && gap.charAt(i + 1) == '-')) {
                while (i < n && gap.charAt(i) != '\n') i++;
                continue;   // the terminating newline itself is appended on the next pass
            }
            if (ch == '/' && i + 1 < n && gap.charAt(i + 1) == '*') {
                int end = gap.indexOf("*/", i + 2);
                i = (end < 0) ? n : end + 2;
                continue;
            }
            sb.append(ch);
            i++;
        }
        return sb.toString();
    }

    /**
     * {@code TRUNCATE TABLE t} — recorded as a table operation like any other write.
     *
     * <p>It is DDL rather than DML, but it empties a table, so a reader of that table's page needs
     * to see it exactly as much as they need to see a DELETE. Before the grammar accepted it, a
     * procedure containing one produced no operations at all — not even its INSERTs — because no
     * alternative matched and the whole body failed.
     */
    @Override
    public Void visitStmtTruncate(MySqlParser.StmtTruncateContext ctx) {
        MySqlParser.TruncateStatementContext t = ctx.truncateStatement();
        if (t != null) {
            addTableOp(t.tableRef(), MySqlTableOpInfo.DmlType.TRUNCATE, ctx.getStart().getLine());
        }
        return visitChildren(ctx);
    }

    /**
     * {@code CREATE [TEMPORARY] TABLE t (...)} — Bug (temp-table audit): this rule already parsed
     * cleanly (createTmpTableStatement / # stmtCreateTmpTable), but had no visitor hook at all, so
     * the CREATE itself was silently invisible in the output (and, unlike a genuinely unsupported
     * statement, never even recorded on {@code errors}, since it did match a real rule).
     */
    @Override
    public Void visitStmtCreateTmpTable(MySqlParser.StmtCreateTmpTableContext ctx) {
        MySqlParser.CreateTmpTableStatementContext t = ctx.createTmpTableStatement();
        if (t != null) {
            MySqlTableOpInfo op = addTableOp(t.tableRef(0), MySqlTableOpInfo.DmlType.CREATE, ctx.getStart().getLine());
            // TEMP as the fourth table kind (grammar-first): the rule accepts an OPTIONAL TEMPORARY
            // keyword, so a plain CREATE TABLE reaches it too — only the TEMPORARY form is temp. A
            // MySQL temp table is session-scoped (dropped at connection end).
            if (op != null && t.TEMPORARY() != null) {
                op.setTableKind("TEMP");
                op.setTempScope("SESSION");
            }
        }
        return visitChildren(ctx);
    }

    /**
     * {@code DROP [TEMPORARY] TABLE t} — same gap as CREATE above (dropTmpTableStatement /
     * # stmtDropTmpTable parsed cleanly but had no visitor hook). Recorded as TRUNCATE-style write
     * op (no dedicated DROP DmlType exists); the goal here is visibility, not a new taxonomy.
     */
    @Override
    public Void visitStmtDropTmpTable(MySqlParser.StmtDropTmpTableContext ctx) {
        MySqlParser.DropTmpTableStatementContext t = ctx.dropTmpTableStatement();
        if (t != null) {
            addTableOp(t.tableRef(), MySqlTableOpInfo.DmlType.TRUNCATE, ctx.getStart().getLine());
        }
        return visitChildren(ctx);
    }

    /**
     * A statement this grammar does not model, skipped so its neighbours survive.
     *
     * <p>Skipping is the right trade — losing one statement beats losing the whole procedure — but
     * it must not be silent. An analysis that quietly omits a statement is indistinguishable from
     * one where the statement was never there, which is how a procedure came to report zero tables
     * while looking complete. Recording it on {@code errors} keeps the omission visible and gives
     * the line and text needed to decide whether a real rule is worth adding.
     *
     * <p>Deliberately does not descend into the statement: its tokens were matched by a wildcard,
     * not by a rule, so anything read out of them would be a guess presented as a finding.
     */
    @Override
    public Void visitStmtUnknown(MySqlParser.StmtUnknownContext ctx) {
        int line = ctx.getStart().getLine();
        String text = originalText(ctx);
        if (text != null && text.length() > 120) text = text.substring(0, 120) + " …";
        result.getErrors().add("line " + line + ": unsupported statement skipped — " + text);
        log.debug("skipping unsupported statement at line {}: {}", line, text);
        return null;
    }

    private MySqlTableOpInfo addTableOp(MySqlParser.TableRefContext tr, MySqlTableOpInfo.DmlType dml, int line) {
        if (tr == null) return null;
        MySqlTableOpInfo op = new MySqlTableOpInfo();
        op.setDmlType(dml);
        op.setTableName(extractObjectId(tr.objectId()));
        if (tr.schemaRef() != null) {
            op.setSchemaName(extractSimpleId(tr.schemaRef().simpleId()));
        }
        op.setLine(line);
        op.setSqlText(enclosingStatementText(tr));
        op.setHandlerIndex(currentHandlerIndex);
        result.getTableOps().add(op);
        return op;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction-control statements → StatementInfo (COMMIT / ROLLBACK / SAVEPOINT)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitTxnCommit(MySqlParser.TxnCommitContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("COMMIT", ctx.getStart().getLine());
        si.setSqlText(statementText(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnRollback(MySqlParser.TxnRollbackContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("ROLLBACK", ctx.getStart().getLine());
        si.setSqlText(statementText(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnRollbackTo(MySqlParser.TxnRollbackToContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("ROLLBACK", ctx.getStart().getLine());
        si.setSavepointName(extractObjectId(ctx.objectId()));
        si.setSqlText(statementText(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnSavepoint(MySqlParser.TxnSavepointContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("SAVEPOINT", ctx.getStart().getLine());
        si.setSavepointName(extractObjectId(ctx.objectId()));
        si.setSqlText(statementText(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    /** {@code START TRANSACTION} — grammar rule exists (txnStart) but had no visitor hook. */
    @Override
    public Void visitTxnStart(MySqlParser.TxnStartContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("START_TRANSACTION", ctx.getStart().getLine());
        si.setSqlText(statementText(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    /** {@code RELEASE SAVEPOINT name} — grammar rule exists (txnReleaseSavepoint), no visitor hook. */
    @Override
    public Void visitTxnReleaseSavepoint(MySqlParser.TxnReleaseSavepointContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("RELEASE_SAVEPOINT", ctx.getStart().getLine());
        si.setSavepointName(extractObjectId(ctx.objectId()));
        si.setSqlText(statementText(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Control-flow → StatementInfo (IF / CASE / WHILE / LOOP / REPEAT)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitIfStatement(MySqlParser.IfStatementContext ctx) {
        addStatement(new MySqlStatementInfo("IF", ctx.getStart().getLine()));
        pushGuard("IF", ctx.getStart().getLine(), conditionText(ctx.expr()));
        try {
            return visitChildren(ctx);
        } finally {
            popGuard();
        }
    }

    @Override
    public Void visitCaseStatement(MySqlParser.CaseStatementContext ctx) {
        addStatement(new MySqlStatementInfo("CASE", ctx.getStart().getLine()));
        // Statement-level CASE: a per-WHEN guard (visitCaseWhenClause) is pushed for each WHEN
        // branch; anything outside a WHEN (e.g. ELSE) sees the generic "<case>" guard.
        pushGuard("CASE", ctx.getStart().getLine(), "<case>");
        try {
            return visitChildren(ctx);
        } finally {
            popGuard();
        }
    }

    /** Per-WHEN branch of a statement-level CASE: push the WHEN condition as the guard. */
    @Override
    public Void visitCaseWhenClause(MySqlParser.CaseWhenClauseContext ctx) {
        pushGuard("CASE", ctx.getStart().getLine(), conditionText(ctx.expr()));
        try {
            return visitChildren(ctx);
        } finally {
            popGuard();
        }
    }

    @Override
    public Void visitPrimCaseExpr(MySqlParser.PrimCaseExprContext ctx) {
        addStatement(new MySqlStatementInfo("CASE", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitWhileStatement(MySqlParser.WhileStatementContext ctx) {
        addStatement(new MySqlStatementInfo("WHILE_LOOP", ctx.getStart().getLine()));
        pushGuard("LOOP", ctx.getStart().getLine(), conditionText(ctx.expr()));
        try {
            return visitChildren(ctx);
        } finally {
            popGuard();
        }
    }

    @Override
    public Void visitLoopStatement(MySqlParser.LoopStatementContext ctx) {
        addStatement(new MySqlStatementInfo("LOOP", ctx.getStart().getLine()));
        pushGuard("LOOP", ctx.getStart().getLine(), null);
        try {
            return visitChildren(ctx);
        } finally {
            popGuard();
        }
    }

    @Override
    public Void visitRepeatStatement(MySqlParser.RepeatStatementContext ctx) {
        addStatement(new MySqlStatementInfo("REPEAT", ctx.getStart().getLine()));
        pushGuard("LOOP", ctx.getStart().getLine(), conditionText(ctx.expr()));
        try {
            return visitChildren(ctx);
        } finally {
            popGuard();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Nested queries → StatementInfo (SUBQUERY / CTE / UPSERT)
    // ─────────────────────────────────────────────────────────────────────────

    /** Expression-level subquery: (SELECT ...) inside WHERE/SELECT-list/etc. */
    @Override
    public Void visitSubqueryExpr(MySqlParser.SubqueryExprContext ctx) {
        addStatement(new MySqlStatementInfo("SUBQUERY", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /** FROM-clause derived-table subquery: FROM (SELECT ...) alias. */
    @Override
    public Void visitAtomSubquery(MySqlParser.AtomSubqueryContext ctx) {
        addStatement(new MySqlStatementInfo("SUBQUERY", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /** WITH [RECURSIVE] common-table-expression clause. */
    @Override
    public Void visitWithClause(MySqlParser.WithClauseContext ctx) {
        addStatement(new MySqlStatementInfo("CTE", ctx.getStart().getLine()));
        for (MySqlParser.CteDefinitionContext cte : ctx.cteDefinition()) {
            if (cte.simpleId() != null) {
                cteNames.add(extractSimpleId(cte.simpleId()).toUpperCase());
            }
        }
        return visitChildren(ctx);
    }

    /**
     * Adds a statement, tagging it with the handler it's inside (if any) and with the enclosing
     * branch/loop guard (if any). All tags are additive — the statement is always added.
     */
    private void addStatement(MySqlStatementInfo si) {
        si.setHandlerIndex(currentHandlerIndex);
        stampContext(si);
        result.getStatements().add(si);
    }

    /** Stamps the current guard + handler context onto a statement row (nulls left unset). */
    private void stampContext(MySqlStatementInfo si) {
        Guard g = guardStack.peek();
        if (g != null) {
            si.setGuardKind(g.kind);
            si.setGuardLine(g.line);
            if (g.condition != null) si.setGuardCondition(g.condition);
        }
        if (currentHandlerName != null) {
            si.setHandlerName(currentHandlerName);
            si.setHandlerLine(currentHandlerLine);
        }
    }

    private void pushGuard(String kind, int line, String condition) {
        guardStack.push(new Guard(kind, line, condition));
    }

    private void popGuard() {
        guardStack.poll();
    }

    /** The collapsed source text of a statement context, capped at 300 chars. */
    private String statementText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        return collapse(originalText(ctx), 300);
    }

    /** The collapsed source text of a condition context, capped at 200 chars. */
    private String conditionText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        return collapse(originalText(ctx), 200);
    }

    /** Collapses all whitespace runs to single spaces, trims, and truncates to {@code max}. */
    private static String collapse(String s, int max) {
        if (s == null) return null;
        String c = s.replaceAll("\\s+", " ").trim();
        if (c.isEmpty()) return null;
        return c.length() > max ? c.substring(0, max) : c;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE ... HANDLER FOR ...  (audit #9)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * {@code DECLARE {CONTINUE|EXIT|UNDO} HANDLER FOR <conditions> <action>}.
     *
     * <p>Records the handler itself, then visits its action statement with
     * {@link #currentHandlerIndex} set, so every CALL/statement/table-op created while inside the
     * handler body is tagged with this handler's index rather than silently joining the routine's
     * flat top-level lists as if it were main-body code.
     */
    @Override
    public Void visitDeclareHandler(MySqlParser.DeclareHandlerContext ctx) {
        MySqlExceptionHandlerInfo h = new MySqlExceptionHandlerInfo();
        h.setLine(ctx.getStart().getLine());
        if (ctx.handlerKind() != null) {
            MySqlParser.HandlerKindContext hk = ctx.handlerKind();
            if (hk.CONTINUE() != null) h.setKind(MySqlExceptionHandlerInfo.Kind.CONTINUE);
            else if (hk.EXIT() != null) h.setKind(MySqlExceptionHandlerInfo.Kind.EXIT);
            else if (hk.UNDO() != null) h.setKind(MySqlExceptionHandlerInfo.Kind.UNDO);
        }
        if (ctx.handlerConditionList() != null) {
            for (MySqlParser.HandlerConditionContext hc : ctx.handlerConditionList().handlerCondition()) {
                h.getConditions().add(handlerConditionText(hc));
            }
        }
        h.setParentHandlerIndex(handlerStack.peek());
        result.getExceptionHandlers().add(h);

        Integer previous = currentHandlerIndex;
        String previousName = currentHandlerName;
        Integer previousLine = currentHandlerLine;
        int thisIndex = result.getExceptionHandlers().size() - 1;
        currentHandlerIndex = thisIndex;
        currentHandlerName = h.getConditions().isEmpty()
                ? "OTHERS" : String.join(" OR ", h.getConditions());
        currentHandlerLine = h.getLine();
        handlerStack.push(thisIndex);
        try {
            if (ctx.routineStatement() != null) {
                visit(ctx.routineStatement());
            }
        } finally {
            handlerStack.pop();
            currentHandlerIndex = previous;
            currentHandlerName = previousName;
            currentHandlerLine = previousLine;
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SIGNAL / RESIGNAL / DECLARE ... CONDITION FOR ...  (bug 4: grammar has these fully wired
    // into routineStatement, but there was no visitor override for any of them — SQLSTATE,
    // named-condition, and MESSAGE_TEXT all vanished with zero structured output. Pattern
    // adapted from Db2AnalysisVisitor.visitSignalStmt/visitResignalStmt.)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareCondition(MySqlParser.DeclareConditionContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("DECLARE_CONDITION", ctx.getStart().getLine());
        if (ctx.simpleId() != null) {
            si.setConditionName(extractSimpleId(ctx.simpleId()));
        }
        if (ctx.conditionValue() != null && ctx.conditionValue().SQ_STRING() != null) {
            si.setSqlstate(ctx.conditionValue().SQ_STRING().getText());
        }
        addStatement(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitSignalStatement(MySqlParser.SignalStatementContext ctx) {
        MySqlStatementInfo si = new MySqlStatementInfo("SIGNAL", ctx.getStart().getLine());
        populateSignalConditionAndMessage(si, ctx.signalConditionValue(), ctx.signalSetClause());
        addStatement(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitResignalStatement(MySqlParser.ResignalStatementContext ctx) {
        boolean bare = ctx.signalConditionValue() == null && ctx.signalSetClause() == null;
        MySqlStatementInfo si = new MySqlStatementInfo(
                bare ? "RESIGNAL_BARE" : "RESIGNAL_WITH_ARGS", ctx.getStart().getLine());
        populateSignalConditionAndMessage(si, ctx.signalConditionValue(), ctx.signalSetClause());
        addStatement(si);
        return visitChildren(ctx);
    }

    private void populateSignalConditionAndMessage(MySqlStatementInfo si,
            MySqlParser.SignalConditionValueContext cond, MySqlParser.SignalSetClauseContext setClause) {
        if (cond != null) {
            if (cond.SQ_STRING() != null) {
                si.setSqlstate(cond.SQ_STRING().getText());
            } else if (cond.simpleId() != null) {
                si.setConditionName(extractSimpleId(cond.simpleId()));
            }
        }
        if (setClause != null) {
            for (MySqlParser.SignalItemContext item : setClause.signalItem()) {
                if (item.simpleId() != null && "MESSAGE_TEXT".equalsIgnoreCase(extractSimpleId(item.simpleId()))) {
                    si.setMessageText(item.expr() != null ? item.expr().getText() : null);
                    break;
                }
            }
            if (si.getMessageText() == null && !setClause.signalItem().isEmpty()) {
                MySqlParser.SignalItemContext first = setClause.signalItem().get(0);
                si.setMessageText(first.expr() != null ? first.expr().getText() : null);
            }
        }
    }

    /** Renders one {@code handlerCondition} alternative as a human-readable string. */
    private String handlerConditionText(MySqlParser.HandlerConditionContext hc) {
        if (hc instanceof MySqlParser.HandlerSqlStateContext hs) {
            return "SQLSTATE " + hs.SQ_STRING().getText();
        }
        if (hc instanceof MySqlParser.HandlerSqlWarningContext) return "SQLWARNING";
        if (hc instanceof MySqlParser.HandlerNotFoundContext) return "NOT FOUND";
        if (hc instanceof MySqlParser.HandlerSqlExceptionContext) return "SQLEXCEPTION";
        if (hc instanceof MySqlParser.HandlerNamedConditionContext hn) {
            return extractSimpleId(hn.simpleId());
        }
        return hc.getText();
    }

    private boolean isCte(MySqlParser.TableRefContext tr) {
        if (tr == null || tr.schemaRef() != null) return false; // a schema-qualified ref can't be a CTE
        String name = extractObjectId(tr.objectId());
        return name != null && cteNames.contains(name.toUpperCase());
    }

    /** INSERT ... ON DUPLICATE KEY UPDATE → upsert. */
    @Override
    public Void visitOnDuplicateClause(MySqlParser.OnDuplicateClauseContext ctx) {
        addStatement(new MySqlStatementInfo("UPSERT", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * Bug 2 fix: {@code ctx.getText()} on a {@code dataType()} context concatenates all tokens
     * with NO separating whitespace, so {@code INT UNSIGNED} becomes {@code INTUNSIGNED},
     * {@code DECIMAL(10,2) UNSIGNED} becomes {@code DECIMAL(10,2)UNSIGNED}, etc. This walks the
     * token stream for the context and joins adjacent tokens with a single space, except that no
     * space is inserted immediately before/after {@code (}, {@code )}, or {@code ,} — so a
     * precision/scale spec like {@code DECIMAL(10,2)} stays tight while a following modifier like
     * {@code UNSIGNED} gets its own space.
     */
    private String formatDataType(org.antlr.v4.runtime.ParserRuleContext ctx) {
        if (ctx == null) return null;
        java.util.List<org.antlr.v4.runtime.Token> toks = new java.util.ArrayList<>();
        collectLeafTokens(ctx, toks);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < toks.size(); i++) {
            String text = toks.get(i).getText();
            if (i > 0) {
                String prev = toks.get(i - 1).getText();
                boolean noSpace = "(".equals(prev) || "(".equals(text)
                        || ")".equals(text) || ",".equals(text) || ",".equals(prev);
                if (!noSpace) sb.append(' ');
            }
            sb.append(text);
        }
        return sb.toString();
    }

    private void collectLeafTokens(org.antlr.v4.runtime.tree.ParseTree node,
                                    java.util.List<org.antlr.v4.runtime.Token> out) {
        if (node instanceof org.antlr.v4.runtime.tree.TerminalNode t) {
            out.add(t.getSymbol());
            return;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectLeafTokens(node.getChild(i), out);
        }
    }

    private String extractSimpleId(MySqlParser.SimpleIdContext ctx) {
        if (ctx == null) return null;
        String text = ctx.getText();
        if (!text.startsWith("`")) return text;
        // Strip the outer backticks, then collapse MySQL's doubled-backtick escape (``) into a
        // single literal backtick, e.g. `weird``name` -> weird`name.
        return text.substring(1, text.length() - 1).replace("``", "`");
    }

    private String extractObjectId(MySqlParser.ObjectIdContext ctx) {
        return ctx != null ? extractSimpleId(ctx.simpleId()) : null;
    }

    /**
     * Style-variance fix (quoted vs bare identifiers): every identifier this visitor reports must
     * read the same whether the author wrote it bare, backtick-quoted, or (for an alias) as a
     * quoted string. {@link #extractSimpleId} already did this for {@code simpleId} nodes, but
     * three call sites read {@code getText()} straight off the tree — {@code alias()},
     * the qualifiers collected from an ON condition, and the bare identifier of a CONCAT argument
     * — so {@code `o`} arrived as "`o`" and never matched the alias map built from unquoted names.
     * Result: a JOIN chain that mixed backticked and bare spellings silently lost its left side.
     */
    private static String unquoteIdentifier(String text) {
        if (text == null || text.length() < 2) return text;
        char c = text.charAt(0);
        if ((c == '`' || c == '\'' || c == '"') && text.charAt(text.length() - 1) == c) {
            return text.substring(1, text.length() - 1).replace("" + c + c, "" + c);
        }
        return text;
    }

    /** The alias's identifier text with any backticks / quotes removed, or null. */
    private static String aliasText(MySqlParser.AliasContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.getText());
    }
}
