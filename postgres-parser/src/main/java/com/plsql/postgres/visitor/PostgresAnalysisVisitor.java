package com.plsql.postgres.visitor;

import com.plsql.postgres.grammar.PostgresParser;
import com.plsql.postgres.grammar.PostgresParserBaseVisitor;
import com.plsql.postgres.model.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ANTLR4 visitor over the PostgresParser parse tree.
 *
 * Extracts:
 *   visitRoutineDefinition  → routine identity (name, schema, type, line range)
 *   visitParam              → parameter list (IN/OUT/INOUT, no @ prefix)
 *   visitDeclareVariableStmt → DECLARE variable
 *   visitDeclareCursorStmt  → DECLARE CURSOR [WITH HOLD] FOR select
 *   visitCallStatement      → CALL [schema.]name(args)
 *   visitInsertStatement    → INSERT table op
 *   visitUpdateStatement    → UPDATE table op
 *   visitDeleteStatement    → DELETE table op
 *   visitSelectStatement    → SELECT table op (scans stmtTail for FROM clause)
 *   visitPrepareStatement   → PREPARE stmt FROM source
 *   visitExecuteImmStmt     → EXECUTE IMMEDIATE source
 *   visitExecuteStatement   → EXECUTE stmt [USING args]
 */
public class PostgresAnalysisVisitor extends PostgresParserBaseVisitor<Void> {

    private final PostgresRoutineInfo result;

    /** CTE names declared via WITH — these are not base tables and must be skipped. */
    private final Set<String> cteNames = new HashSet<>();

    // Bug #2 fix: scope stack for nested BEGIN...END blocks. Each frame holds the names of
    // variables DECLAREd directly inside that block (or the routine's top-level body, for the
    // base frame). Pushed on entering a nested beginBlock, popped on exit, so
    // isDeclaredVariableInScope() can check innermost-to-outermost and correctly say "no" once
    // a block-local variable's declaring block has closed. result.getVariables() is left as a
    // full flat list for backward-compat/reporting — only the scope-CHECK logic is fixed here.
    private final Deque<Set<String>> variableScopeStack = new ArrayDeque<>();
    {
        variableScopeStack.push(new HashSet<>()); // base/routine-level scope frame
    }

    public PostgresAnalysisVisitor(String objectName, String schema) {
        this.result = new PostgresRoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
        result.setType(PostgresRoutineInfo.RoutineType.PROCEDURE);
    }

    public PostgresRoutineInfo getResult() {
        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Routine identity
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitRoutineDefinition(PostgresParser.RoutineDefinitionContext ctx) {
        // Routine type: PROCEDURE or FUNCTION
        if (ctx.FUNCTION() != null) {
            result.setType(PostgresRoutineInfo.RoutineType.FUNCTION);
        } else {
            result.setType(PostgresRoutineInfo.RoutineType.PROCEDURE);
        }

        // routineName: (id DOT)? id
        PostgresParser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<PostgresParser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                // schema.name
                result.setSchema(cleanId(ids.get(0).getText()));
                result.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                result.setName(cleanId(ids.get(0).getText()));
            }
        }

        // Line range
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        // RETURNS clause (tasks #26 + #27). The grammar already models all three shapes:
        //   RETURNS SETOF <type>      -> setofKw? dataType
        //   RETURNS TABLE (a int, …)  -> TABLE aliasColumnList
        //   RETURNS <type>            -> setofKw? dataType (scalar, or the refcursor pseudo-type)
        // Nothing here was read before, so a set-returning function was indistinguishable from a
        // plain procedure. `refcursor` has no dedicated token (it lexes as a plain id), so it is
        // recognised from the data-type text.
        PostgresParser.ReturnTypeContext rt = ctx.returnType();
        if (rt != null) {
            if (rt.setofKw() != null && rt.dataType() != null) {
                result.setReturnType("SETOF " + rt.dataType().getText());
                result.setReturnsResultSet(true);
                result.setResultSets(1);
            } else if (rt.TABLE() != null && rt.aliasColumnList() != null) {
                result.setReturnType("TABLE");
                for (PostgresParser.IdContext col : rt.aliasColumnList().id()) {
                    result.getReturnsColumns().add(cleanId(col.getText()));
                }
                result.setReturnsResultSet(true);
                result.setResultSets(1);
            } else if (rt.dataType() != null) {
                String dt = rt.dataType().getText();
                result.setReturnType(dt);
                if (dt.toLowerCase(java.util.Locale.ROOT).contains("refcursor")) {
                    result.setReturnsResultSet(true);
                    result.setResultSets(1);
                }
            }
        }

        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Anonymous DO block — no routine identity to extract (DO has no name/params);
    // just record the line range and fall through to visitChildren so the body's
    // table ops/calls/cursors/exception-handlers are captured via the same paths
    // a routine body uses (bodyBlock is shared between the two grammar rules).
    // ─────────────────────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────────────────────
    // Nested BEGIN...END block (bug #2 fix): push a fresh scope frame so variables
    // DECLAREd inside this block are only visible to isDeclaredVariableInScope while we're
    // inside it, then pop the frame on exit so they go out of scope afterwards.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitBeginBlock(PostgresParser.BeginBlockContext ctx) {
        variableScopeStack.push(new HashSet<>());
        Void result0 = visitChildren(ctx);
        variableScopeStack.pop();
        return result0;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE TRIGGER — metadata only.
    //
    // A PostgreSQL CREATE TRIGGER statement carries no body: the row-level logic lives in the
    // separately defined trigger FUNCTION named by EXECUTE FUNCTION/PROCEDURE (which parses via
    // routineDefinition on its own). So this captures the trigger's identity and metadata
    // (name, timing, events, FOR EACH scope, WHEN condition), records the target table as a
    // table op so the trigger shows up as depending on that table, and records the referenced
    // trigger function as a call — the dependency edge to where the logic actually is.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateTrigger(PostgresParser.CreateTriggerContext ctx) {
        result.setType(PostgresRoutineInfo.RoutineType.TRIGGER);

        List<PostgresParser.RoutineNameContext> names = ctx.routineName();
        // routineName #0 = the trigger's own name; routineName #1 = the EXECUTE FUNCTION target.
        if (!names.isEmpty()) applyRoutineName(names.get(0));

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        if (ctx.triggerTiming() != null) {
            result.setTriggerTiming(normalizeKeywords(ctx.triggerTiming()));
        }
        StringBuilder events = new StringBuilder();
        for (PostgresParser.TriggerEventContext ev : ctx.triggerEvent()) {
            if (events.length() > 0) events.append(",");
            // Only the event keyword itself (an `UPDATE OF col1,col2` still reports "UPDATE").
            events.append(ev.getStart().getText().toUpperCase());
        }
        if (events.length() > 0) result.setTriggerEvent(events.toString());
        if (ctx.triggerForEach() != null) {
            result.setTriggerForEach(normalizeKeywords(ctx.triggerForEach())
                    .replaceFirst("^FOR\\s+(EACH\\s+)?", ""));
        }
        if (ctx.triggerWhen() != null && ctx.triggerWhen().expr() != null) {
            result.setTriggerWhen(originalText(ctx.triggerWhen().expr()));
        }

        // Target table → table op (DmlType.SELECT: the trigger references, not modifies, it).
        PostgresParser.TableRefContext tref = ctx.tableRef();
        if (tref != null) {
            result.setTriggerTable(extractTableName(tref));
            PostgresTableOpInfo op = new PostgresTableOpInfo();
            op.setDmlType(PostgresTableOpInfo.DmlType.SELECT);
            op.setTableName(extractTableName(tref));
            op.setSchemaName(extractTableSchema(tref));
            op.setLine(tref.getStart().getLine());
            op.setSqlText(originalText(ctx));
            result.getTableOps().add(op);
        }

        // EXECUTE FUNCTION <func>() → call edge.
        if (names.size() > 1) {
            PostgresParser.RoutineNameContext fn = names.get(1);
            List<PostgresParser.IdContext> ids = fn.id();
            PostgresCallInfo call = new PostgresCallInfo();
            if (ids.size() == 2) {
                call.setSchema(cleanId(ids.get(0).getText()));
                call.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                call.setName(cleanId(ids.get(0).getText()));
            }
            call.setLine(fn.getStart().getLine());
            result.getCalls().add(call);
            result.setTriggerFunction(call.qualifiedName());
        }

        // Descend only into the optional WHEN condition (its own function calls, if any).
        // The rest of the statement is metadata already consumed above; visiting it would
        // re-record the EXECUTE target through visitFunctionCall.
        if (ctx.triggerWhen() != null) visit(ctx.triggerWhen());
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE [MATERIALIZED] VIEW — identity + the defining SELECT.
    //
    // The underlying tables/joins need no view-specific extraction: the defining query is a
    // real selectStatement context, so descending into it routes through the same
    // visitSelectStatement/visitFromClause/processTableReference path every other SELECT uses.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateView(PostgresParser.CreateViewContext ctx) {
        result.setType(PostgresRoutineInfo.RoutineType.VIEW);
        result.setMaterializedView(ctx.MATERIALIZED() != null);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        if (ctx.selectStatement() != null) {
            result.setViewQuery(originalText(ctx.selectStatement()));
            // Descend only into the defining SELECT — viewPreAs/viewTail are opaque token runs.
            return visit(ctx.selectStatement());
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE SEQUENCE — metadata only.
    //
    // Like CREATE TRIGGER, a sequence definition has no body and no query: it is a standalone
    // schema object whose entire content is its name plus its options. So this captures the
    // (optionally schema-qualified) name and each option that was actually written, leaving
    // every unwritten option null so "not specified" stays distinguishable from a written value.
    // There is nothing to descend into — sequenceTail is an opaque token run — so it returns
    // without visiting children.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateSequence(PostgresParser.CreateSequenceContext ctx) {
        result.setType(PostgresRoutineInfo.RoutineType.SEQUENCE);
        applyRoutineName(ctx.routineName());
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        for (PostgresParser.SequenceOptionContext opt : ctx.sequenceOption()) {
            if (opt instanceof PostgresParser.SeqAsTypeContext) {
                result.setSequenceDataType(
                        ((PostgresParser.SeqAsTypeContext) opt).dataType().getText());
            } else if (opt instanceof PostgresParser.SeqStartContext) {
                result.setSequenceStart(
                        ((PostgresParser.SeqStartContext) opt).signedNumber().getText());
            } else if (opt instanceof PostgresParser.SeqIncrementContext) {
                result.setSequenceIncrement(
                        ((PostgresParser.SeqIncrementContext) opt).signedNumber().getText());
            } else if (opt instanceof PostgresParser.SeqMinValueContext) {
                result.setSequenceMinValue(
                        ((PostgresParser.SeqMinValueContext) opt).signedNumber().getText());
            } else if (opt instanceof PostgresParser.SeqNoMinValueContext) {
                result.setSequenceMinValue("NO MINVALUE");
            } else if (opt instanceof PostgresParser.SeqMaxValueContext) {
                result.setSequenceMaxValue(
                        ((PostgresParser.SeqMaxValueContext) opt).signedNumber().getText());
            } else if (opt instanceof PostgresParser.SeqNoMaxValueContext) {
                result.setSequenceMaxValue("NO MAXVALUE");
            } else if (opt instanceof PostgresParser.SeqCycleContext) {
                result.setSequenceCycle(Boolean.TRUE);
            } else if (opt instanceof PostgresParser.SeqNoCycleContext) {
                result.setSequenceCycle(Boolean.FALSE);
            } else if (opt instanceof PostgresParser.SeqCacheContext) {
                result.setSequenceCache(
                        ((PostgresParser.SeqCacheContext) opt).signedNumber().getText());
            }
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE INDEX — metadata plus the dependency edge onto the indexed table.
    //
    // Like a trigger, an index has no body; what matters for analysis is (a) which table it is
    // built on — recorded as a SELECT table op so the index shows up as depending on that table,
    // exactly as visitCreateTrigger does for its target table — and (b) which columns/expressions
    // it covers. The index's own name is optional in PostgreSQL, so `name` is only set when one
    // was actually written; indexName mirrors it for callers that want the index-specific field.
    //
    // The partial-index WHERE predicate is both stored as text and descended into, so any
    // function call inside it is still recorded through the normal visitFunctionCall path.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateIndex(PostgresParser.CreateIndexContext ctx) {
        result.setType(PostgresRoutineInfo.RoutineType.INDEX);
        result.setIndexUnique(ctx.UNIQUE() != null);
        result.setIndexConcurrently(ctx.CONCURRENTLY() != null);
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        if (ctx.routineName() != null) {
            applyRoutineName(ctx.routineName());
            result.setIndexName(result.getName());
        }

        PostgresParser.TableRefContext tref = ctx.tableRef();
        if (tref != null) {
            result.setIndexTable(extractTableName(tref));
            result.setIndexTableSchema(extractTableSchema(tref));
            PostgresTableOpInfo op = new PostgresTableOpInfo();
            op.setDmlType(PostgresTableOpInfo.DmlType.SELECT);
            op.setTableName(extractTableName(tref));
            op.setSchemaName(extractTableSchema(tref));
            op.setLine(tref.getStart().getLine());
            op.setSqlText(originalText(ctx));
            result.getTableOps().add(op);
        }

        // USING <method>: the `id` right after the USING token (the only one in this rule).
        if (ctx.USING() != null && ctx.id() != null) {
            result.setIndexMethod(cleanId(ctx.id().getText()));
        }

        for (PostgresParser.IndexElemContext elem : ctx.indexElem()) {
            // Store the element's source text (a bare column, or an expression like lower(email)).
            result.getIndexColumns().add(spanText(elem.expr()));
        }

        if (ctx.expr() != null) {
            result.setIndexWhere(spanText(ctx.expr()));
            visit(ctx.expr());
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ALTER TABLE — the altered table plus, for a FOREIGN KEY constraint, the REFERENCED table.
    //
    // This runs on BOTH paths: a standalone top-level ALTER TABLE (a compilationUnit
    // alternative) and an ALTER TABLE written inside a routine/DO body (a `statement`
    // alternative). The only difference is that the top-level form also owns the result's
    // identity (type = ALTER_TABLE, name = the altered table); the embedded form leaves the
    // enclosing routine's name/type alone and only contributes table ops + metadata, since the
    // routine itself is still the object being analysed.
    //
    // Both tables of an `ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES parent(...)` are recorded
    // as table ops — the altered (child) table as a CREATE-kind DDL op, the referenced (parent)
    // table as a SELECT-kind dependency — and the edge itself is kept verbatim in
    // alterForeignKeyRefs as "child->parent". Before this method existed the embedded form
    // produced no ops and no errors at all, so the edge disappeared with zero signal.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitAlterTable(PostgresParser.AlterTableContext ctx) {
        PostgresParser.TableRefContext target = ctx.tableRef();
        String tableName   = target != null ? extractTableName(target)   : null;
        String tableSchema = target != null ? extractTableSchema(target) : null;

        boolean topLevel = ctx.getParent() instanceof PostgresParser.CompilationUnitContext;
        if (topLevel) {
            result.setType(PostgresRoutineInfo.RoutineType.ALTER_TABLE);
            if (tableName != null) {
                result.setName(tableName);
                if (tableSchema != null) result.setSchema(tableSchema);
            }
            result.setLineStart(ctx.getStart().getLine());
            result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        }

        if (tableName != null) {
            result.setAlterTableName(tableName);
            result.setAlterTableSchema(tableSchema);
            if (!result.getAlteredTables().contains(tableName)) {
                result.getAlteredTables().add(tableName);
            }
            addAlterTableOp(PostgresTableOpInfo.DmlType.CREATE, tableName, tableSchema, ctx, ctx);
        }

        for (PostgresParser.AlterActionContext action : ctx.alterAction()) {
            result.getAlterActions().add(describeAlterAction(action));

            if (action instanceof PostgresParser.AlterAddForeignKeyContext) {
                PostgresParser.AlterAddForeignKeyContext fk =
                        (PostgresParser.AlterAddForeignKeyContext) action;
                PostgresParser.TableRefContext ref = fk.tableRef();
                if (ref != null) {
                    String refName   = extractTableName(ref);
                    String refSchema = extractTableSchema(ref);
                    addAlterTableOp(PostgresTableOpInfo.DmlType.SELECT, refName, refSchema, ref, action);
                    if (tableName != null && refName != null) {
                        result.getAlterForeignKeyRefs().add(tableName + "->" + refName);
                    }
                    recordForeignKey(fk, tableName, tableSchema, refName, refSchema);
                }
            } else if (action instanceof PostgresParser.AlterAddCheckContext) {
                // Descend into the CHECK predicate only — any function call in it is still
                // recorded through the normal visitFunctionCall path.
                PostgresParser.ExprContext e = ((PostgresParser.AlterAddCheckContext) action).expr();
                if (e != null) visit(e);
            }
        }
        // Everything else in this statement is either metadata already captured above or an
        // opaque permissive token run (alterActionTail / alterActionOther), so do not descend.
        return null;
    }

    /**
     * Keeps a FOREIGN KEY constraint structurally — constraint name and both column lists.
     *
     * <p>The {@code "child->parent"} string in {@code alterForeignKeyRefs} names the two tables
     * and drops everything else, yet the column pairing is right there in the grammar
     * ({@code … FOREIGN KEY (idList) REFERENCES tableRef (idList)?}). Which columns join to
     * which is the whole content of the constraint, so it is read out rather than re-guessed
     * downstream from column names that happen to match.
     *
     * <p>The referenced column list is optional in Postgres — omitting it means "the parent's
     * primary key". That is left as an empty list rather than invented, because the primary key
     * is not knowable from this statement alone.
     */
    private void recordForeignKey(PostgresParser.AlterAddForeignKeyContext fk,
                                  String tableName, String tableSchema,
                                  String refName, String refSchema) {
        if (refName == null) return;

        PostgresForeignKeyInfo info = new PostgresForeignKeyInfo();
        info.setConstraintName(fk.id() != null ? cleanId(fk.id().getText()) : null);
        info.setTable(tableName);
        info.setSchema(tableSchema);
        info.setReferencedTable(refName);
        info.setReferencedSchema(refSchema);

        List<PostgresParser.IdListContext> lists = fk.idList();
        if (!lists.isEmpty()) info.getColumns().addAll(idNames(lists.get(0)));
        if (lists.size() > 1) info.getReferencedColumns().addAll(idNames(lists.get(1)));
        info.setLine(fk.getStart().getLine());

        result.getForeignKeys().add(info);
    }

    private List<String> idNames(PostgresParser.IdListContext ctx) {
        if (ctx == null) return List.of();
        List<String> out = new ArrayList<>();
        for (PostgresParser.IdContext id : ctx.id()) out.add(cleanId(id.getText()));
        return out;
    }

    /** Records one table op for an ALTER TABLE participant (altered table or FK target). */
    private void addAlterTableOp(PostgresTableOpInfo.DmlType kind, String tableName, String schema,
                                 org.antlr.v4.runtime.ParserRuleContext at,
                                 org.antlr.v4.runtime.ParserRuleContext sqlSpan) {
        if (tableName == null) return;
        PostgresTableOpInfo op = new PostgresTableOpInfo();
        op.setDmlType(kind);
        op.setTableName(tableName);
        op.setSchemaName(schema);
        op.setLine(at.getStart().getLine());
        op.setSqlText(originalText(sqlSpan));
        result.getTableOps().add(op);
    }

    /** A short, stable label for one ALTER TABLE action, e.g. "ADD CONSTRAINT FOREIGN KEY". */
    private String describeAlterAction(PostgresParser.AlterActionContext action) {
        if (action instanceof PostgresParser.AlterAddForeignKeyContext)   return "ADD FOREIGN KEY";
        if (action instanceof PostgresParser.AlterAddPrimaryKeyContext)   return "ADD PRIMARY KEY";
        if (action instanceof PostgresParser.AlterAddUniqueContext)       return "ADD UNIQUE";
        if (action instanceof PostgresParser.AlterAddCheckContext)        return "ADD CHECK";
        if (action instanceof PostgresParser.AlterAddColumnContext)       return "ADD COLUMN";
        if (action instanceof PostgresParser.AlterDropConstraintContext)  return "DROP CONSTRAINT";
        if (action instanceof PostgresParser.AlterDropColumnContext)      return "DROP COLUMN";
        return "OTHER";
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE TYPE (enum / composite / shell) and CREATE DOMAIN — metadata only.
    //
    // Same shape as visitCreateSequence: these objects have no body and no query, so all there
    // is to capture is the identity plus the declared shape. typeKind distinguishes the three
    // CREATE TYPE forms and the domain, so a caller never has to re-inspect the source text.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateEnumType(PostgresParser.CreateEnumTypeContext ctx) {
        beginTypeObject(PostgresRoutineInfo.RoutineType.TYPE, "ENUM", ctx, ctx.routineName());
        for (org.antlr.v4.runtime.tree.TerminalNode label : ctx.SQ_STRING()) {
            result.getTypeEnumLabels().add(stripSingleQuotes(label.getText()));
        }
        return null;
    }

    @Override
    public Void visitCreateCompositeType(PostgresParser.CreateCompositeTypeContext ctx) {
        beginTypeObject(PostgresRoutineInfo.RoutineType.TYPE, "COMPOSITE", ctx, ctx.routineName());
        for (PostgresParser.TypeAttributeContext attr : ctx.typeAttribute()) {
            result.getTypeAttributes().add(
                    cleanId(attr.id().getText()) + " " + attr.dataType().getText());
        }
        return null;
    }

    @Override
    public Void visitCreateShellType(PostgresParser.CreateShellTypeContext ctx) {
        beginTypeObject(PostgresRoutineInfo.RoutineType.TYPE, "SHELL", ctx, ctx.routineName());
        return null;
    }

    @Override
    public Void visitCreateDomain(PostgresParser.CreateDomainContext ctx) {
        beginTypeObject(PostgresRoutineInfo.RoutineType.DOMAIN, "DOMAIN", ctx, ctx.routineName());
        if (ctx.dataType() != null) result.setDomainBaseType(ctx.dataType().getText());
        for (PostgresParser.DomainConstraintContext c : ctx.domainConstraint()) {
            if (c instanceof PostgresParser.DomainCheckContext) {
                PostgresParser.ExprContext e = ((PostgresParser.DomainCheckContext) c).expr();
                if (e != null) {
                    // First CHECK wins the dedicated field; all of them are still descended into
                    // so any function call inside the predicate is recorded as usual.
                    if (result.getDomainCheck() == null) result.setDomainCheck(spanText(e));
                    visit(e);
                }
            }
        }
        return null;
    }

    /** Shared identity/kind/line setup for CREATE TYPE and CREATE DOMAIN. */
    private void beginTypeObject(PostgresRoutineInfo.RoutineType type, String kind,
                                 org.antlr.v4.runtime.ParserRuleContext ctx,
                                 PostgresParser.RoutineNameContext nameCtx) {
        result.setType(type);
        result.setTypeKind(kind);
        applyRoutineName(nameCtx);
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
    }

    /** 'ACTIVE' -> ACTIVE, with embedded '' un-escaped. */
    private static String stripSingleQuotes(String s) {
        if (s == null) return null;
        if (s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            return s.substring(1, s.length() - 1).replace("''", "'");
        }
        return s;
    }

    /** Applies a routineName context ((id DOT)? id) to the result's schema/name. */
    private void applyRoutineName(PostgresParser.RoutineNameContext rnCtx) {
        if (rnCtx == null) return;
        List<PostgresParser.IdContext> ids = rnCtx.id();
        if (ids.size() == 2) {
            result.setSchema(cleanId(ids.get(0).getText()));
            result.setName(cleanId(ids.get(1).getText()));
        } else if (ids.size() == 1) {
            result.setName(cleanId(ids.get(0).getText()));
        }
    }

    /** Upper-cased, single-space-separated token text of a keyword-only context (e.g. "INSTEAD OF"). */
    private String normalizeKeywords(org.antlr.v4.runtime.ParserRuleContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(ctx.getChild(i).getText().toUpperCase());
        }
        return sb.toString();
    }

    @Override
    public Void visitDoStatement(PostgresParser.DoStatementContext ctx) {
        // Task #25: a DO block has no routine identity of its own (no name, no parameters, no
        // catalog entry), so it is typed DO_BLOCK rather than masquerading as the PROCEDURE the
        // constructor defaults to. The body is still walked below, so its table ops / calls /
        // cursors / exception handlers are captured exactly like a routine body's.
        result.setType(PostgresRoutineInfo.RoutineType.DO_BLOCK);
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parameters
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitParam(PostgresParser.ParamContext ctx) {
        PostgresParamInfo p = new PostgresParamInfo();

        // Direction: IN_KW / OUT_KW / INOUT
        if (ctx.INOUT() != null) {
            p.setDirection(PostgresParamInfo.Direction.INOUT);
        } else if (ctx.OUT_KW() != null) {
            p.setDirection(PostgresParamInfo.Direction.OUT);
        } else {
            p.setDirection(PostgresParamInfo.Direction.IN);
        }

        // Name (no @ prefix in POSTGRES).
        // Real-corpus bug E: a PostgreSQL signature may declare a bare TYPE with no parameter
        // name at all (`set_config(text, text, boolean)`), which `param` now accepts, so `id()`
        // is legitimately absent there — record the positional index instead of dereferencing it.
        if (ctx.id() != null) {
            p.setName(cleanId(ctx.id().getText()));
        } else {
            p.setName("$" + (result.getParams().size() + 1));
        }

        // Data type
        p.setDataType(ctx.dataType().getText());

        p.setLine(ctx.getStart().getLine());
        result.getParams().add(p);
        return null; // don't recurse — no nested params
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE variable
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareVariableStmt(PostgresParser.DeclareVariableStmtContext ctx) {
        String name = cleanId(ctx.id().getText());
        String dataType = ctx.dataType().getText();

        // Filter out handler keywords (CONTINUE/EXIT/UNDO HANDLER is a different rule,
        // but be defensive)
        if ("CONTINUE".equalsIgnoreCase(name) || "EXIT".equalsIgnoreCase(name)
                || "UNDO".equalsIgnoreCase(name)) {
            return null;
        }

        // Filter out CURSOR type (those are handled by declareCursorStmt)
        if ("CURSOR".equalsIgnoreCase(dataType)) {
            return null;
        }

        PostgresVariableInfo v = new PostgresVariableInfo();
        v.setName(name);
        v.setDataType(dataType);
        if (ctx.expr() != null) {
            v.setDefaultValue(ctx.expr().getText());
        }
        v.setLine(ctx.getStart().getLine());
        result.getVariables().add(v);
        variableScopeStack.peek().add(name.toUpperCase()); // Bug #2 fix: scope to current block frame
        return null; // no nested declares
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Idiomatic PL/pgSQL "DECLARE ... BEGIN" variable section (bug 9 fix): one DECLARE
    // keyword followed by one-or-more bare "name type [:= expr];" items. Captured the same
    // way as declareVariableStmt (a single DECLARE per variable, used inside a body).
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitPreBeginDeclareItem(PostgresParser.PreBeginDeclareItemContext ctx) {
        // Bug 5 fix (§DML-audit): `id CURSOR FOR selectStatement` inside the idiomatic
        // one-DECLARE-keyword block — record it as a cursor (like declareCursorStmt) and
        // descend into the SELECT so its tables are captured, whichever alternative matched.
        if (ctx.CURSOR() != null) {
            PostgresCursorInfo c = new PostgresCursorInfo();
            c.setName(cleanId(ctx.id().getText()));
            c.setWithHold(ctx.HOLD() != null);
            extractCursorParams(ctx.paramList(), c);
            if (ctx.selectStatement() != null) {
                // Bug 6 fix: ctx.getText() concatenates only visible-channel tokens with no
                // separators, squishing the SQL onto one run-on line. Use the shared
                // originalText()/StatementTextCapture helper instead (mirrors the MySQL/DB2 fix
                // for the same class of bug), preserving whitespace/comments.
                c.setSelectText(originalText(ctx.selectStatement()));
            }
            c.setLine(ctx.getStart().getLine());
            result.getCursors().add(c);
            if (ctx.selectStatement() != null) {
                visit(ctx.selectStatement());
            }
            return null;
        }

        String name = cleanId(ctx.id().getText());
        String dataType = ctx.dataType().getText();

        PostgresVariableInfo v = new PostgresVariableInfo();
        v.setName(name);
        v.setDataType(dataType);
        if (ctx.expr() != null) {
            v.setDefaultValue(ctx.expr().getText());
        }
        v.setLine(ctx.getStart().getLine());
        result.getVariables().add(v);
        variableScopeStack.peek().add(name.toUpperCase()); // Bug #2 fix: scope to current block frame
        return null; // no nested declares
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE cursor
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareCursorStmt(PostgresParser.DeclareCursorStmtContext ctx) {
        PostgresCursorInfo c = new PostgresCursorInfo();
        c.setName(cleanId(ctx.id(0).getText()));
        c.setWithHold(ctx.HOLD() != null);
        extractCursorParams(ctx.paramList(), c);
        if (ctx.selectStatement() != null) {
            // Bug 6 fix: see comment on the sibling preBeginDeclareItem cursor path above.
            c.setSelectText(originalText(ctx.selectStatement()));
        }
        c.setLine(ctx.getStart().getLine());
        result.getCursors().add(c);
        // Visit children to catch table ops in the SELECT inside the cursor
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cursor lifecycle: OPEN / FETCH / CLOSE
    //
    // DECLARE was the only operation ever recorded against a cursor — every c1 in the real corpus
    // showed operations:["DECLARE"] even though the source also does OPEN c1 / FETCH c1 INTO v_rec
    // in a loop / CLOSE c1. PL/pgSQL has no DEALLOCATE; CLOSE is the real terminal operation, so
    // none is synthesized here.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitOpenStatement(PostgresParser.OpenStatementContext ctx) {
        recordCursorOp(ctx.id().getText(), "OPEN", ctx.getStart().getLine());

        // Bug 2 (§dynamic-cursor-audit): OPEN cur FOR EXECUTE <expr> [USING ...] — a refcursor
        // opened directly against dynamic SQL. Modeled the same way as a plain EXECUTE statement
        // (see visitExecuteStatement) so `format(...)`/literal table extraction is shared.
        if (ctx.EXECUTE() != null) {
            List<PostgresParser.ExprContext> exprs = ctx.expr();
            if (exprs != null && !exprs.isEmpty()) {
                PostgresDynamicSqlInfo d = new PostgresDynamicSqlInfo();
                d.setKind(PostgresDynamicSqlInfo.Kind.EXECUTE);
                String exprText = sourceTextOf(exprs.get(0));
                d.setStmtName(cleanId(exprs.get(0).getText()));
                d.setSqlSource(exprText);
                for (int i = 1; i < exprs.size(); i++) {
                    d.getUsingExprs().add(sourceTextOf(exprs.get(i)));
                }
                extractDynamicSqlTables(exprs.get(0), ctx.getStart().getLine(), d);
                d.setLine(ctx.getStart().getLine());
                result.getDynamicSql().add(d);
            }
            return null;
        }

        // OPEN cur FOR <select> — descend so the inline SELECT's tables are captured.
        if (ctx.selectStatement() != null) {
            visit(ctx.selectStatement());
            return null;
        }

        return visitChildren(ctx);
    }

    @Override
    public Void visitFetchStatement(PostgresParser.FetchStatementContext ctx) {
        recordCursorOp(ctx.id().getText(), "FETCH", ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    @Override
    public Void visitCloseStatement(PostgresParser.CloseStatementContext ctx) {
        recordCursorOp(ctx.id().getText(), "CLOSE", ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    /**
     * Bug 4 (§dynamic-cursor-audit): populates a parameterized cursor's parameter list
     * (e.g. {@code CURSOR cur(p_id INT) FOR ...}) using the same {@link PostgresParamInfo}
     * shape as the routine's own {@code visitParam}.
     */
    private void extractCursorParams(PostgresParser.ParamListContext paramListCtx, PostgresCursorInfo c) {
        if (paramListCtx == null) return;
        for (PostgresParser.ParamContext pCtx : paramListCtx.param()) {
            PostgresParamInfo p = new PostgresParamInfo();
            if (pCtx.INOUT() != null) {
                p.setDirection(PostgresParamInfo.Direction.INOUT);
            } else if (pCtx.OUT_KW() != null) {
                p.setDirection(PostgresParamInfo.Direction.OUT);
            } else {
                p.setDirection(PostgresParamInfo.Direction.IN);
            }
            // `param` also accepts the anonymous (type-only) PostgreSQL form; see visitParam.
            p.setName(pCtx.id() != null ? cleanId(pCtx.id().getText())
                                        : "$" + (c.getParameters().size() + 1));
            p.setDataType(pCtx.dataType().getText());
            p.setLine(pCtx.getStart().getLine());
            c.getParameters().add(p);
        }
    }

    /** Attaches an OPEN/FETCH/CLOSE occurrence to the cursor of the same name in this routine. */
    private void recordCursorOp(String rawName, String kind, int line) {
        if (rawName == null) return;
        String name = cleanId(rawName);
        for (PostgresCursorInfo c : result.getCursors()) {
            if (c.getName() != null && c.getName().equalsIgnoreCase(name)) {
                c.getOperations().add(new PostgresCursorInfo.Op(kind, line));
                return;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CALL statement
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCallStatement(PostgresParser.CallStatementContext ctx) {
        PostgresCallInfo call = new PostgresCallInfo();
        PostgresParser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<PostgresParser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                call.setSchema(cleanId(ids.get(0).getText()));
                call.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                call.setName(cleanId(ids.get(0).getText()));
            }
        }
        call.setLine(ctx.getStart().getLine());
        result.getCalls().add(call);
        return null; // don't recurse into args for more calls
    }

    // Bug (temp-table/CREATE TABLE audit): createStatement (CREATE (~SEMI)* SEMI ;) had no
    // visitor override, so ANY CREATE TABLE — TEMP/TEMPORARY or ordinary — was consumed as an
    // opaque token stream and produced zero table ops. Kept minimal/additive: only CREATE TABLE
    // (optionally TEMP/TEMPORARY/UNLOGGED, optionally IF NOT EXISTS, optionally schema-qualified)
    // is recognized here; every other CREATE variant (INDEX, VIEW, FUNCTION, ...) is left exactly
    // as it was — untouched opaque consumption — so this does not affect anything that currently
    // works around other CREATE statements.
    // Grammar-driven (was CREATE_TABLE_NAME_PATTERN regex): the `createTableStmt` alternative of
    // createStatement exposes the table name as a real `tableRef` node, so the name/schema come
    // from the same extractTableName/extractTableSchema helpers every other DML op uses.
    @Override
    public Void visitCreateTableStmt(PostgresParser.CreateTableStmtContext ctx) {
        String child = recordCreateTable(ctx.tableRef(), ctx);
        recordInherits(child, ctx.createTableTailItem(), ctx);
        visitCtasQuery(ctx.createTableTailItem());
        return null;   // rest of the body consumed opaquely, exactly as before
    }

    // Top-level `CREATE TABLE t (...)` — a standalone schema script rather than a statement in a
    // routine body. Before `createTableTop` existed this was not a compilationUnit alternative at
    // all, so it failed with "no viable alternative at input 'CREATETABLE'" and produced no table
    // op whatsoever. The table extraction is byte-for-byte the same as the in-body form (shared
    // recordCreateTable); the only extra work is claiming the result's identity, exactly as the
    // top-level branch of visitAlterTable does for ALTER TABLE.
    @Override
    public Void visitCreateTableTop(PostgresParser.CreateTableTopContext ctx) {
        String tableName = recordCreateTable(ctx.tableRef(), ctx);
        recordInherits(tableName, ctx.createTableTailItem(), ctx);

        result.setType(PostgresRoutineInfo.RoutineType.CREATE_TABLE);
        if (tableName != null) {
            result.setName(tableName);
            String schema = extractTableSchema(ctx.tableRef());
            if (schema != null) result.setSchema(schema);
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        visitCtasQuery(ctx.createTableTailItem());
        return null;
    }

    /**
     * CREATE TABLE … AS SELECT: descends into the CTAS query so its source tables are recorded
     * (the surrounding CREATE TABLE visitors deliberately do NOT visitChildren, to keep the rest
     * of the tail opaque). Without this the source of a CTAS was silently dropped.
     */
    private void visitCtasQuery(java.util.List<PostgresParser.CreateTableTailItemContext> tail) {
        if (tail == null) return;
        for (PostgresParser.CreateTableTailItemContext item : tail) {
            if (item.selectCore() != null) {
                if (item.withClause() != null) visit(item.withClause());
                visit(item.selectCore());
            }
        }
    }

    /**
     * Records the CREATE table op for a CREATE TABLE in either position (in-body or top-level).
     *
     * @return the table's name, or {@code null} if it could not be read.
     */
    private String recordCreateTable(PostgresParser.TableRefContext ref,
                                     org.antlr.v4.runtime.ParserRuleContext stmt) {
        PostgresTableOpInfo op = new PostgresTableOpInfo();
        op.setDmlType(PostgresTableOpInfo.DmlType.CREATE);
        op.setSchemaName(extractTableSchema(ref));
        op.setTableName(extractTableName(ref));
        op.setLine(stmt.getStart().getLine());
        op.setSqlText(originalText(stmt));
        result.getTableOps().add(op);
        return op.getTableName();
    }

    /**
     * Records the parent tables of {@code CREATE TABLE child (...) INHERITS (parent, ...)}.
     *
     * <p>Before this, the child was captured as a CREATE op but the parent was swallowed by the
     * statement's opaque tail, so the child-depends-on-parent edge was lost with no signal —
     * precisely the failure mode ALTER TABLE ... REFERENCES used to have.
     *
     * <p>It is therefore fixed the same way, reusing the existing model surface rather than
     * extending it: each parent becomes a SELECT-kind {@link PostgresTableOpInfo} (the
     * established way this dialect says "this statement depends on that table" — see
     * {@code visitAlterTable}'s FK branch), and the edge is also kept verbatim as
     * {@code "child->parent"}. SELECT is reused deliberately: the multidb bridge passes
     * {@code DmlType.name()} straight through as a string, so a new enum value would surface an
     * operation name downstream code has never seen, whereas a read dependency is both true and
     * already handled everywhere.
     */
    private void recordInherits(String childTable,
                                List<PostgresParser.CreateTableTailItemContext> tail,
                                org.antlr.v4.runtime.ParserRuleContext stmt) {
        if (tail == null) return;
        for (PostgresParser.CreateTableTailItemContext item : tail) {
            PostgresParser.InheritsClauseContext inh = item.inheritsClause();
            if (inh == null) continue;
            for (PostgresParser.TableRefContext parent : inh.tableRef()) {
                String parentName = extractTableName(parent);
                if (parentName == null) continue;
                addAlterTableOp(PostgresTableOpInfo.DmlType.SELECT, parentName,
                                extractTableSchema(parent), parent, stmt);
                if (childTable != null) {
                    result.getTableInheritsRefs().add(childTable + "->" + parentName);
                }
            }
        }
    }

    /** Every non-TABLE CREATE stays opaque, exactly as it was before the split. */
    @Override
    public Void visitCreateOtherStmt(PostgresParser.CreateOtherStmtContext ctx) {
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: INSERT
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitInsertStatement(PostgresParser.InsertStatementContext ctx) {
        PostgresTableOpInfo op = new PostgresTableOpInfo();
        op.setDmlType(PostgresTableOpInfo.DmlType.INSERT);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(originalText(ctx));
        populateReturningColumns(op, ctx.returningClause());
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: UPDATE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitUpdateStatement(PostgresParser.UpdateStatementContext ctx) {
        PostgresTableOpInfo op = new PostgresTableOpInfo();
        op.setDmlType(PostgresTableOpInfo.DmlType.UPDATE);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(originalText(ctx));
        populateReturningColumns(op, ctx.returningClause());
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: DELETE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeleteStatement(PostgresParser.DeleteStatementContext ctx) {
        PostgresTableOpInfo op = new PostgresTableOpInfo();
        op.setDmlType(PostgresTableOpInfo.DmlType.DELETE);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(originalText(ctx));
        populateReturningColumns(op, ctx.returningClause());
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    /**
     * TRUNCATE [TABLE] t [, t2 …] — one TRUNCATE table op per target, plus a single TRUNCATE
     * statement record. Before this existed, TRUNCATE was swallowed by otherStatement's
     * catch-all and the affected table never appeared anywhere in the output.
     */
    @Override
    public Void visitTruncateStatement(PostgresParser.TruncateStatementContext ctx) {
        for (PostgresParser.TableRefContext tr : ctx.tableRef()) {
            PostgresTableOpInfo op = new PostgresTableOpInfo();
            op.setDmlType(PostgresTableOpInfo.DmlType.TRUNCATE);
            op.setTableName(extractTableName(tr));
            op.setSchemaName(extractTableSchema(tr));
            op.setLine(ctx.getStart().getLine());
            op.setSqlText(originalText(ctx));
            result.getTableOps().add(op);
        }
        result.getStatements().add(
                new PostgresStatementInfo("TRUNCATE", ctx.getStart().getLine()));
        return null;
    }

    /** Fills {@code op.getReturningColumns()} from an optional RETURNING clause context. */
    private void populateReturningColumns(PostgresTableOpInfo op, PostgresParser.ReturningClauseContext rc) {
        if (rc == null) return;
        for (PostgresParser.ReturningItemContext item : rc.returningItem()) {
            op.getReturningColumns().add(item.getText());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SELECT — register CTE names up-front so their references are skipped, then
    // descend so FROM/JOIN contexts (incl. inside CTE bodies) are captured.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitSelectStatement(PostgresParser.SelectStatementContext ctx) {
        if (ctx.withClause() != null) {
            for (PostgresParser.CteDefContext cte : ctx.withClause().cteDef()) {
                if (cte.id() != null) {
                    cteNames.add(cleanId(cte.id().getText()).toUpperCase());
                }
            }
        }
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FROM clause — base tables + joins (grammar contexts only, no text scanning)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitFromClause(PostgresParser.FromClauseContext ctx) {
        for (PostgresParser.TableReferenceContext tr : ctx.tableReference()) {
            processTableReference(tr);
        }
        return visitChildren(ctx);   // recurse for nested subquery SELECTs / CTEs
    }

    /**
     * Walk one {@code tableReference} (a base tableAtom followed by zero or more
     * joinClauses). The base atom becomes a SELECT table op; each JOINed atom
     * becomes its own SELECT table op AND a PostgresJoinInfo attached to the base op.
     */
    private void processTableReference(PostgresParser.TableReferenceContext tr) {
        PostgresTableOpInfo baseOp = tableOpForAtom(tr.tableAtom());

        java.util.Map<String, String> aliasMap = buildAliasMap(tr);
        int queryId = tr.getStart().getLine();

        for (PostgresParser.JoinClauseContext jc : tr.joinClause()) {
            PostgresTableOpInfo joinedOp = tableOpForAtom(jc.tableAtom());
            if (baseOp == null) continue;

            if (jc.tableAtom() instanceof PostgresParser.AtomTableContext jat) {
                PostgresParser.TableRefContext jref = jat.tableRef();
                // A joined CTE ref yields no standalone table op (handled above) but the
                // JOIN relationship itself is still recorded on the base table.
                PostgresJoinInfo j = new PostgresJoinInfo();
                j.setJoinType(joinTypeText(jc.joinType()));
                j.setJoinedTable(extractTableName(jref));
                j.setJoinedTableSchema(extractTableSchema(jref));
                if (aliasIdOf(jat) != null) {
                    j.setJoinedTableAlias(cleanId(aliasIdOf(jat).getText()));
                }
                if (jc.expr() != null) {
                    j.setCondition(jc.expr().getText());
                }
                j.setLine(jc.getStart().getLine());
                j.setQueryId(queryId);
                resolveLeftSide(j, aliasMap, jc.expr());
                if (jc.joinUsing() != null) {
                    // `JOIN b USING (c1, c2)` carries no ON expression, but it does state an
                    // equality on those columns between the joined table and the left side of
                    // the join. Record it in the same normalized shape the ON form produces
                    // (`USING (c1, c2)`), and resolve the left side from the FROM-list base
                    // table, since no alias qualifiers exist in a USING list to resolve from.
                    StringBuilder cond = new StringBuilder("USING (");
                    List<PostgresParser.IdContext> cols = jc.joinUsing().id();
                    for (int ci = 0; ci < cols.size(); ci++) {
                        if (ci > 0) cond.append(", ");
                        cond.append(cleanId(cols.get(ci).getText()));
                    }
                    j.setCondition(cond.append(')').toString());
                    resolveLeftSideFromBase(j, tr);
                }
                baseOp.getJoins().add(j);
            }
        }
    }

    /** Build alias->table (both upper-cased) for the base atom and each joined atom of a tableReference. */
    private java.util.Map<String, String> buildAliasMap(PostgresParser.TableReferenceContext tr) {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        addAtomAlias(m, tr.tableAtom());
        for (PostgresParser.JoinClauseContext jc : tr.joinClause()) {
            addAtomAlias(m, jc.tableAtom());
        }
        return m;
    }

    private void addAtomAlias(java.util.Map<String, String> m, PostgresParser.TableAtomContext atom) {
        if (!(atom instanceof PostgresParser.AtomTableContext at)) return;
        String tbl = extractTableName(at.tableRef());
        if (tbl == null) return;
        tbl = tbl.toUpperCase();
        if (aliasIdOf(at) != null) m.put(cleanId(aliasIdOf(at).getText()).toUpperCase(), tbl);
        m.put(tbl, tbl);
    }

    /**
     * Fallback left-side resolution for a join with no ON expression to read aliases out of
     * (currently the {@code USING (cols)} form): the left side is the FROM-list base table.
     * Only set when the base atom is a plain table reference and is not the joined table itself.
     */
    private void resolveLeftSideFromBase(PostgresJoinInfo j, PostgresParser.TableReferenceContext tr) {
        if (j.getLeftTable() != null) return;
        if (!(tr.tableAtom() instanceof PostgresParser.AtomTableContext bat)) return;
        String base = extractTableName(bat.tableRef());
        if (base == null) return;
        j.setLeftTable(base.toUpperCase());
        if (aliasIdOf(bat) != null) j.setLeftTableAlias(cleanId(aliasIdOf(bat).getText()).toUpperCase());
    }

    /**
     * Resolve leftTable/leftTableAlias from the ON-condition alias that is NOT the joined table.
     *
     * Grammar-driven (was a {@code ([A-Za-z_]\w*)\s*\.} regex over the condition's text): the
     * qualifiers are collected by walking the real {@code ON expr} parse tree and picking every
     * {@code id} node that is immediately followed by a {@code DOT} sibling — the same document
     * order the regex scanned in, but token-accurate, so a dotted name inside a string literal
     * or a numeric literal can no longer be mistaken for a table alias.
     */
    private void resolveLeftSide(PostgresJoinInfo j, java.util.Map<String, String> aliasMap,
                                 PostgresParser.ExprContext onExpr) {
        if (onExpr == null) return;
        String joinedAlias = j.getJoinedTableAlias() != null ? j.getJoinedTableAlias().toUpperCase() : null;
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        collectDotQualifiers(onExpr, seen);
        for (String a : seen) {
            if (joinedAlias != null && a.equals(joinedAlias)) continue;
            String t = aliasMap.get(a);
            if (t != null) { j.setLeftTableAlias(a); j.setLeftTable(t); return; }
        }
    }

    /**
     * Pre-order walk collecting the upper-cased text of every {@code id} node whose next sibling
     * is a {@code DOT} terminal (i.e. the qualifier part of {@code alias.column},
     * {@code schema.table.column}, {@code alias.*}, {@code schema.fn(...)}, …).
     */
    private void collectDotQualifiers(org.antlr.v4.runtime.tree.ParseTree node,
                                      java.util.Set<String> out) {
        if (node instanceof org.antlr.v4.runtime.ParserRuleContext prc) {
            int n = prc.getChildCount();
            for (int i = 0; i < n; i++) {
                org.antlr.v4.runtime.tree.ParseTree child = prc.getChild(i);
                if (child instanceof PostgresParser.IdContext idc
                        && i + 1 < n
                        && child(prc, i + 1) == PostgresParser.DOT) {
                    out.add(cleanId(idc.getText()).toUpperCase());
                }
                collectDotQualifiers(child, out);
            }
        }
    }

    /** Token type of the {@code i}-th child if it is a terminal, else -1. */
    private static int child(org.antlr.v4.runtime.ParserRuleContext prc, int i) {
        org.antlr.v4.runtime.tree.ParseTree c = prc.getChild(i);
        return (c instanceof org.antlr.v4.runtime.tree.TerminalNode tn)
                ? tn.getSymbol().getType() : -1;
    }

    /**
     * Adds a SELECT table op for the given tableAtom and returns it (null for a CTE
     * reference or a subquery atom, whose inner SELECT is handled by recursion).
     */
    private PostgresTableOpInfo tableOpForAtom(PostgresParser.TableAtomContext atom) {
        // A set-returning function used as a table source (FROM fn(...)) is NOT a table — it is a
        // routine invocation. Return null here; the call edge is captured by visitFunctionCall
        // when visitFromClause descends via visitChildren.
        if (atom instanceof PostgresParser.AtomFuncContext) {
            return null;
        }
        if (atom instanceof PostgresParser.AtomTableContext at) {
            if (isCte(at.tableRef())) return null;
            PostgresTableOpInfo op = new PostgresTableOpInfo();
            op.setDmlType(PostgresTableOpInfo.DmlType.SELECT);
            op.setTableName(extractTableName(at.tableRef()));
            op.setSchemaName(extractTableSchema(at.tableRef()));
            if (aliasIdOf(at) != null) op.setAlias(cleanId(aliasIdOf(at).getText()));
            op.setLine(at.getStart().getLine());
            // Without this a SELECT carried no SQL, so no column detail could be derived from it.
            op.setSqlText(enclosingStatementText(at));
            result.getTableOps().add(op);
            return op;
        }
        if (atom instanceof PostgresParser.AtomParenContext paren) {
            PostgresTableOpInfo first = null;
            for (PostgresParser.TableReferenceContext tr : paren.tableReference()) {
                processTableReference(tr);
                if (first == null && !result.getTableOps().isEmpty()) {
                    first = result.getTableOps().get(result.getTableOps().size() - 1);
                }
            }
            return first;
        }
        return null;   // AtomSubquery — inner SELECT handled by visitChildren recursion
    }

    private boolean isCte(PostgresParser.TableRefContext tr) {
        if (tr == null) return false;
        String schema = extractTableSchema(tr);
        String name = extractTableName(tr);
        return schema == null && name != null && cteNames.contains(name.toUpperCase());
    }

    /** The alias identifier of a table atom, or null when it has none. The alias now lives in the
     *  dedicated {@code tableAtomAlias} sub-rule (introduced so a bare NATURAL cannot be mistaken
     *  for an alias), so it is read through here instead of directly off the atom context. */
    private static PostgresParser.IdContext aliasIdOf(PostgresParser.AtomTableContext at) {
        return at.tableAtomAlias() == null ? null : at.tableAtomAlias().id();
    }

    /** Render a joinType context as spaced upper-case text (e.g. "LEFT OUTER"), or "JOIN" if absent. */
    private String joinTypeText(PostgresParser.JoinTypeContext jt) {
        if (jt == null) return "JOIN";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < jt.getChildCount(); i++) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(jt.getChild(i).getText().toUpperCase());
        }
        return sb.length() == 0 ? "JOIN" : sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Function calls inside expressions — user-routine invocations (call-tree)
    // ─────────────────────────────────────────────────────────────────────────

    /** Common POSTGRES built-in functions to exclude — only user-routine calls become edges. */
    private static final Set<String> BUILTIN_FUNCTIONS = Set.of(
        "ABS","ACOS","ASCII","ASIN","ATAN","ATAN2","AVG","BIGINT","CARDINALITY",
        "CEIL","CEILING","CHAR","CHARACTER_LENGTH","CHR","COALESCE","CONCAT","CORR",
        "COS","COSH","COT","COUNT","COUNT_BIG","COVARIANCE","CURRENT_DATE","CURRENT_TIME",
        "CURRENT_TIMESTAMP","DATE","DAY","DAYNAME","DAYOFWEEK","DAYOFYEAR","DAYS","DEC",
        "DECIMAL","DECODE","DEGREES","DIGITS","DOUBLE","EXP","FLOAT","FLOOR","GREATEST",
        "HASH","HEX","HOUR","IFNULL","INSERT","INT","INTEGER","JULIAN_DAY","LCASE","LEAST",
        "LEFT","LENGTH","LN","LOCATE","LOG","LOG10","LOWER","LPAD","LTRIM","MAX","MICROSECOND",
        "MIDNIGHT_SECONDS","MIN","MINUTE","MOD","MONTH","MONTHNAME","NULLIF","NVL","POSITION",
        "POSSTR","POWER","QUARTER","RADIANS","RAISE_ERROR","RAND","REAL","REPEAT","REPLACE",
        "RIGHT","ROUND","RPAD","RTRIM","SECOND","SIGN","SIN","SINH","SMALLINT","SOUNDEX",
        "SPACE","SQRT","STDDEV","SUBSTR","SUBSTRING","SUM","TAN","TANH","TIME","TIMESTAMP",
        "TIMESTAMPDIFF","TIMESTAMP_FORMAT","TO_CHAR","TO_DATE","TRANSLATE","TRIM","TRUNC",
        "TRUNCATE","UCASE","UPPER","VALUE","VARCHAR","VARIANCE","WEEK","YEAR",
        // Special/register built-ins (otherwise mis-read as user routine calls).
        // NOTE: window functions (ROW_NUMBER/RANK/DENSE_RANK) and VARCHAR_FORMAT deliberately
        // excluded — they are higher-collision with real user-routine names and were not
        // observed as false-positive call nodes.
        "IDENTITY_VAL_LOCAL","LAST_INSERT_ID","PREVVAL","GENERATE_UNIQUE",
        "CURRENT_SCHEMA","CURRENT_USER","SESSION_USER","SYSTEM_USER","INTNAND","MAX_CARDINALITY",
        // PostgreSQL built-ins (array / JSON / string / config / time / set-returning) — these are
        // engine functions, not user routines, so they must not be crawled as call nodes.
        "ARRAY_LENGTH","ARRAY_AGG","ARRAY_APPEND","ARRAY_CAT","ARRAY_TO_STRING","STRING_TO_ARRAY",
        "ARRAY_POSITION","ARRAY_REMOVE","ARRAY_REPLACE","GENERATE_SERIES",
        "TO_JSONB","TO_JSON","JSONB_BUILD_OBJECT","JSONB_BUILD_ARRAY","JSON_BUILD_OBJECT","JSON_BUILD_ARRAY",
        "JSONB_AGG","JSON_AGG","JSONB_OBJECT_AGG","ROW_TO_JSON","JSONB_SET","JSONB_PRETTY",
        "JSONB_EACH","JSONB_ARRAY_ELEMENTS","JSONB_EXTRACT_PATH","JSONB_EXTRACT_PATH_TEXT","JSONB_TYPEOF",
        "CURRENT_SETTING","SET_CONFIG","PG_TYPEOF","PG_GET_FUNCTIONDEF",
        "NOW","CLOCK_TIMESTAMP","STATEMENT_TIMESTAMP","TRANSACTION_TIMESTAMP","AGE","DATE_TRUNC","DATE_PART","EXTRACT",
        "STRING_AGG","CONCAT_WS","SPLIT_PART","REGEXP_REPLACE","REGEXP_MATCHES","REGEXP_SPLIT_TO_ARRAY",
        "INITCAP","MD5","ENCODE","FORMAT","QUOTE_IDENT","QUOTE_LITERAL","QUOTE_NULLABLE",
        "ROW_NUMBER","RANK","DENSE_RANK","LAG","LEAD","FIRST_VALUE","LAST_VALUE","NTILE","PERCENT_RANK","CUME_DIST",
        "BOOL_AND","BOOL_OR","EVERY","MODE","PERCENTILE_CONT","PERCENTILE_DISC",
        // --- Expanded authoritative PostgreSQL built-in coverage (deduped against all above) ---
        // String / text
        "BTRIM","STRPOS","REGEXP_MATCH","REGEXP_SPLIT_TO_TABLE","TO_ASCII","CONVERT",
        "CONVERT_FROM","CONVERT_TO","REVERSE","OVERLAY","PARSE_IDENT","STARTS_WITH",
        "TO_HEX","NORMALIZE","UNISTR","QUOTE","BIT_LENGTH","OCTET_LENGTH","CHAR_LENGTH",
        // Math
        "CBRT","DIV","PI","RANDOM","SETSEED","GCD","LCM","WIDTH_BUCKET","LOG2","FACTORIAL",
        "ACOSD","ASIND","ATAND","ATAN2D","COSD","COTD","SIND","TAND","ACOSH","ASINH","ATANH",
        "SCALE","MIN_SCALE","TRIM_SCALE",
        // Date / time
        "LOCALTIME","LOCALTIMESTAMP","MAKE_DATE","MAKE_TIME","MAKE_TIMESTAMP","MAKE_TIMESTAMPTZ",
        "MAKE_INTERVAL","TO_TIMESTAMP","TO_NUMBER","JUSTIFY_DAYS","JUSTIFY_HOURS","JUSTIFY_INTERVAL",
        "ISFINITE","TIMEOFDAY","DATE_BIN",
        // Aggregate / statistical
        "STDDEV_POP","STDDEV_SAMP","VAR_POP","VAR_SAMP","VARIANCE_POP","COVAR_POP","COVAR_SAMP",
        "BIT_AND","BIT_OR","BIT_XOR","XMLAGG","RANGE_AGG","RANGE_INTERSECT_AGG",
        "REGR_SLOPE","REGR_INTERCEPT","REGR_COUNT","REGR_R2","REGR_AVGX","REGR_AVGY",
        "REGR_SXX","REGR_SYY","REGR_SXY",
        // Array
        "ARRAY_PREPEND","ARRAY_POSITIONS","ARRAY_DIMS","ARRAY_UPPER","ARRAY_LOWER",
        "ARRAY_NDIMS","ARRAY_FILL","TRIM_ARRAY",
        // JSON / JSONB
        "JSONB_INSERT","JSONB_STRIP_NULLS","JSON_TYPEOF","JSONB_ARRAY_ELEMENTS_TEXT",
        "JSON_ARRAY_ELEMENTS","JSON_ARRAY_ELEMENTS_TEXT","JSONB_EACH_TEXT","JSON_EACH","JSON_EACH_TEXT",
        "JSONB_OBJECT_KEYS","JSON_OBJECT_KEYS","JSONB_ARRAY_LENGTH","JSON_ARRAY_LENGTH",
        "JSON_OBJECT","JSONB_OBJECT","JSON_EXTRACT_PATH","JSON_EXTRACT_PATH_TEXT",
        "JSONB_PATH_QUERY","JSONB_PATH_EXISTS","JSONB_PATH_MATCH","JSONB_PATH_QUERY_ARRAY",
        "JSONB_PATH_QUERY_FIRST","JSONB_POPULATE_RECORD","JSON_POPULATE_RECORD",
        "JSON_STRIP_NULLS","JSONB_CONTAINED",
        // Set-returning
        "GENERATE_SUBSCRIPTS",
        // NOTE: UNNEST is deliberately NOT in this list (unlike GENERATE_SERIES etc.). Unlike a
        // scalar built-in (COUNT/SUM/...), UNNEST is routinely used as a table source in FROM/JOIN
        // (e.g. JOIN unnest(arr) AS t(val)), the same role played by user-defined set-returning
        // functions. tableOpForAtom() already skips emitting a table-op for any AtomFuncContext, so
        // excluding UNNEST from the call graph too would leave that usage invisible to both. Letting
        // it fall through to a recorded call keeps it visible without inventing a fake "UNNEST" table.
        // Sequence functions (NEXTVAL/CURRVAL/SETVAL/LASTVAL) are deliberately NOT excluded here —
        // visitFunctionCall intercepts them first and records a PostgresSequenceInfo instead of
        // letting them fall through to (or be discarded by) this generic builtin-exclusion path.
        // Bit / encoding / binary
        "GET_BIT","SET_BIT","GET_BYTE","SET_BYTE",
        // System / catalog / info
        "CURRENT_SCHEMAS","CURRENT_DATABASE","CURRENT_CATALOG","VERSION","PG_BACKEND_PID",
        "TXID_CURRENT","TXID_CURRENT_SNAPSHOT","INET_CLIENT_ADDR","INET_SERVER_ADDR",
        "OBJ_DESCRIPTION","COL_DESCRIPTION","SHOBJ_DESCRIPTION","CURRENT_QUERY","PG_SLEEP",
        "PG_COLUMN_SIZE","PG_SIZE_PRETTY","PG_DATABASE_SIZE","PG_TABLE_SIZE","PG_RELATION_SIZE",
        "PG_TOTAL_RELATION_SIZE","PG_GET_VIEWDEF","PG_GET_INDEXDEF","PG_GET_CONSTRAINTDEF",
        "PG_GET_EXPR","PG_GET_SERIAL_SEQUENCE","PG_GET_USERBYID","HAS_TABLE_PRIVILEGE",
        "HAS_SCHEMA_PRIVILEGE","HAS_COLUMN_PRIVILEGE","PG_NOTIFY","PG_CANCEL_BACKEND",
        "PG_TERMINATE_BACKEND");

    /** Sequence functions Postgres invokes as regular function calls (not Oracle's dot-syntax). */
    private static final Set<String> SEQUENCE_FUNCTIONS = Set.of("NEXTVAL", "CURRVAL", "SETVAL", "LASTVAL");

    /**
     * SQL operators/predicates that the permissive {@code id} rule (PostgresParser.g4) admits as
     * identifiers, and which therefore structurally match {@code functionCall} when they are
     * followed by a parenthesised operand — {@code WHERE t IN ('a','b')}, {@code EXISTS (SELECT 1)},
     * {@code CAST(x AS int)}, {@code a = 1 OR (b = 2)}, {@code sum(x) OVER (PARTITION BY y)}.
     *
     * <p>None of these is a call. Every entry is a PostgreSQL reserved word that cannot be the
     * name of an unqualified user routine, so skipping them can never drop a real edge. The check
     * is deliberately limited to UNQUALIFIED names ({@code schema == null}): a genuinely
     * schema-qualified {@code s.f(...)} is always a real call and must keep being recorded.
     *
     * <p>Previously only VALUES was special-cased here, so {@code source_type IN (...)} was
     * recorded as a call to a routine named "IN" — producing a phantom {@code ERP$IN} node and a
     * phantom call edge in the call graph (graph rendered 117 nodes for an analysis whose node
     * index held 116).
     */
    private static final Set<String> SQL_NON_CALL_KEYWORDS = Set.of(
        "IN", "EXISTS", "VALUES", "CAST", "CASE", "IS", "LIKE", "OR", "OVER");

    @Override
    public Void visitFunctionCall(PostgresParser.FunctionCallContext ctx) {
        List<PostgresParser.IdContext> ids = ctx.id();
        String schema = ids.size() > 1 ? cleanId(ids.get(0).getText()) : null;
        String name   = cleanId(ids.get(ids.size() - 1).getText());

        // The permissive stmtTail/stmtToken rule (INSERT's column list + VALUES tuple, etc.) now
        // includes a `functionCall` alternative so nextval()/currval() calls nested inside an
        // INSERT ... VALUES(...) list are still visited (see PostgresParser.g4 stmtToken). Because
        // `id` permissively accepts SQL keywords themselves, constructs like `VALUES (...)`,
        // `IN (...)`, `EXISTS (...)` and `CAST(...)` structurally match functionCall too
        // (name="VALUES"/"IN"/…) — that is grammar noise, not a real call, so it must not be
        // recorded as one. See SQL_NON_CALL_KEYWORDS.
        if (name != null && schema == null && SQL_NON_CALL_KEYWORDS.contains(name.toUpperCase())) {
            return visitChildren(ctx);
        }

        if (name != null && schema == null && SEQUENCE_FUNCTIONS.contains(name.toUpperCase())) {
            recordSequenceUsage(name.toUpperCase(), ctx);
            return visitChildren(ctx);
        }

        // BUG-CT-048 analog: PL/pgSQL genuinely supports "attribute notation" call sugar — for a
        // single-argument function, x.f() is parsed identically to f(x). So when the qualifier
        // resolves to a declared parameter/local variable of the current routine (not a real
        // schema), this is not an external call to a nonexistent schema/package named after that
        // variable — it is (at most) a call to f with the variable as the implicit argument.
        // Treat it as the equivalent unqualified call to `name`, letting the existing builtin
        // filter below decide whether it is even worth recording.
        boolean qualifierIsVariable = schema != null && isDeclaredVariableInScope(schema);
        String effectiveSchema = qualifierIsVariable ? null : schema;

        // Fixed: the previous condition `schema != null || !BUILTIN_FUNCTIONS.contains(name)`
        // short-circuited true whenever ANY qualifier was present, so a builtin invoked via
        // attribute-notation sugar (e.g. v_param.upper(), effectively UPPER(v_param)) was never
        // filtered. The builtin check must apply based on the EFFECTIVE (post-scope-resolution)
        // qualifier, not the raw one — a real schema-qualified name is never a builtin, but an
        // attribute-notation call whose qualifier resolved away to null must still be checked
        // against BUILTIN_FUNCTIONS like any other unqualified call.
        boolean isBuiltin = effectiveSchema == null && BUILTIN_FUNCTIONS.contains(name.toUpperCase());
        if (name != null && !isBuiltin) {
            String qualified = (effectiveSchema != null && !effectiveSchema.isBlank())
                    ? effectiveSchema + "." + name : name;
            boolean dup = result.getCalls().stream()
                    .anyMatch(c -> qualified.equalsIgnoreCase(c.qualifiedName()));
            if (!dup) {
                PostgresCallInfo call = new PostgresCallInfo();
                call.setName(name);
                call.setSchema(effectiveSchema);
                call.setLine(ctx.getStart().getLine());
                result.getCalls().add(call);
            }
        }
        return visitChildren(ctx);
    }

    /**
     * True if {@code name} is a declared IN/OUT/INOUT parameter or DECLAREd local variable of the
     * current routine. postgres-parser's {@code engine.parse()} creates one fresh visitor (and
     * fresh {@link PostgresRoutineInfo}) per routine, so unlike the PL/SQL package-body visitor
     * there is no cross-procedure scope stack to manage here — {@code result.getParams()}/
     * {@code result.getVariables()} already ARE this routine's scope.
     */
    private boolean isDeclaredVariableInScope(String name) {
        if (name == null || name.isEmpty()) return false;
        for (PostgresParamInfo p : result.getParams()) {
            if (p.getName() != null && p.getName().equalsIgnoreCase(name)) return true;
        }
        // Bug #2 fix: check the scope stack (innermost to outermost) instead of the flat
        // result.getVariables() list, so a variable declared inside a nested BEGIN...END block
        // is not considered in scope once that block has closed.
        for (Set<String> frame : variableScopeStack) {
            if (frame.contains(name.toUpperCase())) return true;
        }
        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL: PREPARE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitPrepareStatement(PostgresParser.PrepareStatementContext ctx) {
        PostgresDynamicSqlInfo d = new PostgresDynamicSqlInfo();
        d.setKind(PostgresDynamicSqlInfo.Kind.PREPARE);

        // PREPARE id FROM (id | SQ_STRING)
        List<PostgresParser.IdContext> ids = ctx.id();
        if (!ids.isEmpty()) {
            d.setStmtName(cleanId(ids.get(0).getText()));
        }
        if (ids.size() > 1) {
            // Second id is the source variable
            d.setSqlSource(cleanId(ids.get(1).getText()));
        } else if (ctx.SQ_STRING() != null) {
            d.setSqlSource(ctx.SQ_STRING().getText());
        }

        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL: EXECUTE IMMEDIATE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitExecuteImmStmt(PostgresParser.ExecuteImmStmtContext ctx) {
        PostgresDynamicSqlInfo d = new PostgresDynamicSqlInfo();
        d.setKind(PostgresDynamicSqlInfo.Kind.EXECUTE_IMMEDIATE);

        if (ctx.id() != null) {
            d.setSqlSource(cleanId(ctx.id().getText()));
        } else if (ctx.SQ_STRING() != null) {
            d.setSqlSource(ctx.SQ_STRING().getText());
        }

        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL: EXECUTE stmt [USING ...]
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitExecuteStatement(PostgresParser.ExecuteStatementContext ctx) {
        PostgresDynamicSqlInfo d = new PostgresDynamicSqlInfo();
        d.setKind(PostgresDynamicSqlInfo.Kind.EXECUTE);

        // EXECUTE <expr> [INTO ids] [USING …] — the first expr is the (dynamic) SQL text/expression,
        // e.g. a string literal or a format(...) call. Captured via the original source interval
        // (not expr(0).getText(), which ANTLR renders with whitespace stripped between tokens) so
        // the real EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) text survives verbatim —
        // this was previously dropped entirely (stored only under stmtName, which nothing reads).
        List<PostgresParser.ExprContext> exprs = ctx.expr();
        if (exprs != null && !exprs.isEmpty()) {
            String exprText = sourceTextOf(exprs.get(0));
            d.setStmtName(cleanId(exprs.get(0).getText()));
            d.setSqlSource(exprText);
            // Any expr()s after the first are the USING-bound values.
            for (int i = 1; i < exprs.size(); i++) {
                d.getUsingExprs().add(sourceTextOf(exprs.get(i)));
            }
            // Bug 9 (§DML-audit, lower priority, literal-only scope): a fully-literal
            // EXECUTE '<sql>' (no ||-concatenation / format() indirection) has its table name
            // known at parse time — extract it, modeled on Oracle's DYNAMIC bucket. Deliberately
            // stops here: format(...)/concatenated strings are out of scope for this fix.
            //
            // Bug 3 (§dynamic-cursor-audit): a `format('...%I...', arg)` call is also handled —
            // see extractDynamicSqlTables/extractFormatDynamicSqlTable below.
            extractDynamicSqlTables(exprs.get(0), ctx.getStart().getLine(), d);
        }

        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    private static final java.util.regex.Pattern DYN_SQL_TABLE_PATTERN = java.util.regex.Pattern.compile(
            "(?i)(?:FROM|INTO|UPDATE|INSERT\\s+INTO|DELETE\\s+FROM|JOIN)\\s+"
                    + "([A-Za-z_][A-Za-z0-9_$#]*(?:\\.[A-Za-z_][A-Za-z0-9_$#]*)?)");

    // ─────────────────────────────────────────────────────────────────────────
    // Grammar-first recursive dynamic-SQL parse (regex → fallback only)
    //
    // A fully-literal EXECUTE '<sql>' (and the fully-resolved text of a format('%I') template) is
    // real SQL, so it is re-parsed with this same Postgres grammar and its table targets read off
    // the nested parse tree by the same visitor the static path uses. DYN_SQL_TABLE_PATTERN
    // survives only as the recovery path for text the grammar cannot attribute a table to,
    // logged once and counted.
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
     * Recursively parse a recovered dynamic-SQL string with the Postgres grammar and return the
     * table operations the nested visitor recorded; empty when the text is not a statement this
     * grammar can parse (callers then run the legacy regex), or when a guard fires.
     */
    private List<PostgresTableOpInfo> nestedParseDynamicSqlTables(String innerSql) {
        if (innerSql == null || innerSql.isBlank()) return List.of();
        if (innerSql.length() > DYN_PARSE_MAX_CHARS) return List.of();
        int depth = DYN_PARSE_DEPTH.get();
        if (depth >= DYN_PARSE_MAX_DEPTH) return List.of();
        DYN_PARSE_DEPTH.set(depth + 1);
        try {
            PostgresDynParseErrorListener errors = new PostgresDynParseErrorListener();
            com.plsql.postgres.grammar.PostgresLexer lexer = new com.plsql.postgres.grammar.PostgresLexer(
                    org.antlr.v4.runtime.CharStreams.fromString(innerSql));
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);
            PostgresParser parser = new PostgresParser(new org.antlr.v4.runtime.CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(errors);
            PostgresParser.StatementContext stmt = parser.statement();
            if (errors.hasErrors()) return List.of();
            PostgresAnalysisVisitor sub = new PostgresAnalysisVisitor("JAR$DYN_SQL", null);
            sub.visit(stmt);
            return new ArrayList<>(sub.getResult().getTableOps());
        } catch (Throwable t) {
            return List.of();
        } finally {
            DYN_PARSE_DEPTH.set(depth);
        }
    }

    /** Collects ANTLR syntax errors so a nested dynamic-SQL parse is trusted only when clean. */
    private static final class PostgresDynParseErrorListener
            extends org.antlr.v4.runtime.BaseErrorListener {
        private final List<String> errors = new ArrayList<>();

        @Override
        public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                                Object offendingSymbol, int line, int charPositionInLine,
                                String msg, org.antlr.v4.runtime.RecognitionException e) {
            errors.add(msg);
        }

        boolean hasErrors() { return !errors.isEmpty(); }
    }

    /** Logs (once) and counts a use of the legacy regex recovery path. */
    private void recordRegexFallback(String dialectNote) {
        long fallbackNo = DYN_SQL_REGEX_FALLBACKS.incrementAndGet();
        if (DYN_FALLBACK_LOGGED.compareAndSet(false, true)) {
            System.err.println("[PostgresAnalysisVisitor] dynamic-SQL recursive parse produced no "
                    + "table target (" + dialectNote + "); using legacy regex fallback (#"
                    + fallbackNo + ")");
        }
    }

    /** Runs both the literal-only and the format(%I,...) dynamic-SQL table extractors. */
    private void extractDynamicSqlTables(PostgresParser.ExprContext e, int line, PostgresDynamicSqlInfo d) {
        extractLiteralDynamicSqlTable(soleStringLiteral(e), line);
        extractFormatDynamicSqlTable(e, line, d);
    }

    /**
     * Grammar-driven (was a {@code ^\s*'…'$} / {@code ^\s*format\s*\((.*)\)\s*$} text test): if
     * the whole dynamic-SQL expression is a single {@code SQ_STRING} literal atom, return its
     * contents with the surrounding quotes stripped; otherwise null.
     */
    private static String soleStringLiteral(PostgresParser.ExprContext e) {
        if (e == null || e.exprAtom().size() != 1) return null;
        PostgresParser.LiteralContext lit = e.exprAtom(0).literal();
        if (lit == null) return null;
        if (lit.SQ_STRING() != null) {
            String t = lit.SQ_STRING().getText();
            return t.length() >= 2 ? t.substring(1, t.length() - 1) : null;
        }
        // E'…' escape string (ESQ_STRING): drop the leading E/e as well as the quotes. Backslash
        // escapes are left verbatim — nothing in the SQL-structure/table-name extraction below
        // depends on them, and un-escaping would risk changing literal table names.
        if (lit.ESQ_STRING() != null) {
            String t = lit.ESQ_STRING().getText();
            return t.length() >= 3 ? t.substring(2, t.length() - 1) : null;
        }
        return null;
    }

    /**
     * If the whole dynamic-SQL expression is a single unqualified {@code format(...)} call,
     * return that {@code functionCall} node (whose {@code functionArgs} give the argument list
     * directly, so no hand-rolled comma splitting is needed); otherwise null.
     */
    private static PostgresParser.FunctionCallContext soleFormatCall(PostgresParser.ExprContext e) {
        if (e == null || e.exprAtom().size() != 1) return null;
        PostgresParser.FunctionCallContext fc = e.exprAtom(0).functionCall();
        if (fc == null) return null;
        // A schema-qualified call (`pg_catalog.format(…)`) is the same builtin — only the final
        // segment decides. Previously any DOT disqualified the call outright.
        PostgresParser.IdContext last = fc.id(fc.id().size() - 1);
        return last.getText().equalsIgnoreCase("format") ? fc : null;
    }

    /** Extracts a table name from a dynamic-SQL string that is a single, fully-literal quoted
     *  string (no ||/format() indirection) and records it as a {@code DYNAMIC} table op. */
    private void extractLiteralDynamicSqlTable(String inner, int line) {
        if (inner == null) return;

        // Grammar first: recursively parse the literal with the same Postgres grammar.
        List<PostgresTableOpInfo> grammarOps = nestedParseDynamicSqlTables(inner);
        if (!grammarOps.isEmpty()) {
            Set<String> seen = new HashSet<>();
            for (PostgresTableOpInfo g : grammarOps) {
                if (g.getTableName() == null || g.getTableName().isEmpty()) continue;
                String key = (g.getSchemaName() != null ? g.getSchemaName() + "." : "")
                        + g.getTableName();
                if (!seen.add(key.toLowerCase())) continue;
                PostgresTableOpInfo op = new PostgresTableOpInfo();
                op.setDmlType(PostgresTableOpInfo.DmlType.DYNAMIC);
                op.setTableName(g.getTableName());
                op.setSchemaName(g.getSchemaName());
                op.setLine(line);
                result.getTableOps().add(op);
            }
            return;
        }

        // Recovery path: text the grammar cannot attribute a table to.
        recordRegexFallback("literal EXECUTE");
        java.util.regex.Matcher m = DYN_SQL_TABLE_PATTERN.matcher(inner);
        if (m.find()) {
            PostgresTableOpInfo op = new PostgresTableOpInfo();
            op.setDmlType(PostgresTableOpInfo.DmlType.DYNAMIC);
            op.setTableName(m.group(1));
            op.setLine(line);
            result.getTableOps().add(op);
        }
    }

    /**
     * Bug 3 (§dynamic-cursor-audit): extracts the table name out of a Postgres
     * {@code format('...%I...', args...)} idiom, e.g.
     * {@code format('SELECT * FROM %I WHERE id = $1', v_table_name)}.
     *
     * If the positional argument feeding the {@code %I} placeholder is itself a literal string,
     * the concrete table name is substituted in and extracted via the same pattern used for a
     * fully-literal EXECUTE. If that argument is a variable (unknown at parse time), a table
     * name is never fabricated — instead {@code d.unresolvedVarName} is set, mirroring
     * plsql-parser's {@code DynamicSqlInfo.unresolvedVarName} (Option C) for the same class of
     * "SQL structure known, table name is a runtime variable" gap.
     */
    private void extractFormatDynamicSqlTable(PostgresParser.ExprContext e, int line, PostgresDynamicSqlInfo d) {
        PostgresParser.FunctionCallContext fc = soleFormatCall(e);
        if (fc == null || fc.functionArgs() == null) return;

        List<PostgresParser.ExprContext> args = fc.functionArgs().expr();
        if (args == null || args.isEmpty()) return;

        String template = soleStringLiteral(args.get(0));
        if (template == null) return;

        // Walk the template's %-specifiers in order (skipping the literal %% escape) and record
        // EVERY %I (identifier) placeholder with the positional argument that feeds it — a table
        // named by the second %I of a two-placeholder template used to be dropped entirely.
        // A specifier is  %[n$][flags/width]{s|I|L}  — the optional `n$` prefix (an explicit
        // argument position, e.g. `%1$I`) and any flags/width between it and the conversion
        // letter are both part of PostgreSQL's format() syntax, so they are parsed here rather
        // than assuming every specifier is a bare two-character `%X`.
        int argIndex = 0;          // running position for specifiers without an explicit n$
        // one entry per %I: {1-based arg position, index of the leading '%', full specifier length}
        List<int[]> identSpecs = new java.util.ArrayList<>();
        for (int i = 0; i < template.length(); i++) {
            if (template.charAt(i) != '%' || i + 1 >= template.length()) continue;
            if (template.charAt(i + 1) == '%') { i++; continue; }   // literal %% escape

            int j = i + 1;
            int explicitPos = -1;
            int digitsStart = j;
            while (j < template.length() && Character.isDigit(template.charAt(j))) j++;
            if (j > digitsStart && j < template.length() && template.charAt(j) == '$') {
                explicitPos = Integer.parseInt(template.substring(digitsStart, j));
                j++;                                                 // consume the '$'
            } else {
                j = i + 1;                                           // the digits were a width
            }
            // flags / width between the optional n$ and the conversion letter
            while (j < template.length()
                   && (Character.isDigit(template.charAt(j)) || template.charAt(j) == '-')) j++;
            if (j >= template.length()) break;

            char conv = template.charAt(j);
            if (conv != 's' && conv != 'I' && conv != 'L') { i = j; continue; }  // not a specifier
            int pos = explicitPos > 0 ? explicitPos : ++argIndex;
            if (conv == 'I') {
                identSpecs.add(new int[]{pos, i, j - i + 1});
            }
            i = j;
        }
        if (identSpecs.isEmpty()) return;

        // Substitute every %I whose argument is a literal, left to right, remembering where each
        // substituted name landed in the rebuilt template. Placeholders fed by a variable are
        // left as-is (a table name is never fabricated) and the first of those names the
        // dynamic-SQL record's unresolvedVarName, exactly as before.
        StringBuilder rebuilt = new StringBuilder();
        List<int[]> resolvedRanges = new java.util.ArrayList<>();   // {startInclusive, endExclusive}
        int cursor = 0;
        for (int[] spec : identSpecs) {
            int pos = spec[0], start = spec[1], len = spec[2];
            if (pos <= 0 || pos >= args.size()) continue;
            PostgresParser.ExprContext argExpr = args.get(pos);
            String tableName = soleStringLiteral(argExpr);
            if (tableName == null) {
                if (d != null && d.getUnresolvedVarName() == null) {
                    // The %I argument is a variable — never fabricate a table name from it.
                    d.setUnresolvedVarName(cleanId(sourceTextOf(argExpr).trim()));
                }
                continue;
            }
            rebuilt.append(template, cursor, start);
            int from = rebuilt.length();
            rebuilt.append(tableName);
            resolvedRanges.add(new int[]{from, rebuilt.length()});
            cursor = start + len;
        }
        if (resolvedRanges.isEmpty()) return;
        rebuilt.append(template, cursor, template.length());

        // Emit one DYNAMIC table op per substituted placeholder that a FROM/INTO/UPDATE/JOIN …
        // clause actually reads as a table name. Matches that do not cover a substituted region
        // are static text in the template and stay out of scope here, as before.
        String resolvedSql = rebuilt.toString();
        List<PostgresTableOpInfo> grammarOps = nestedParseDynamicSqlTables(resolvedSql);
        if (!grammarOps.isEmpty()) {
            // Keep only targets whose name is one of the substituted placeholder values — static
            // table text in the template stays out of scope, exactly as the positional regex did.
            Set<String> substituted = new HashSet<>();
            for (int[] r : resolvedRanges) {
                substituted.add(resolvedSql.substring(r[0], r[1]).toUpperCase());
            }
            Set<String> seen = new HashSet<>();
            for (PostgresTableOpInfo g : grammarOps) {
                if (g.getTableName() == null) continue;
                String simple = g.getTableName().toUpperCase();
                String qualified = g.getSchemaName() != null
                        ? (g.getSchemaName() + "." + g.getTableName()).toUpperCase() : simple;
                if (!substituted.contains(simple) && !substituted.contains(qualified)) continue;
                if (!seen.add(qualified)) continue;
                PostgresTableOpInfo op = new PostgresTableOpInfo();
                op.setDmlType(PostgresTableOpInfo.DmlType.DYNAMIC);
                op.setTableName(g.getTableName());
                op.setSchemaName(g.getSchemaName());
                op.setLine(line);
                result.getTableOps().add(op);
            }
            return;
        }

        // Recovery path: the resolved template is not a statement the grammar can attribute a
        // table to — fall back to the positional regex scan.
        recordRegexFallback("format(%I) template");
        java.util.regex.Matcher tm = DYN_SQL_TABLE_PATTERN.matcher(resolvedSql);
        while (tm.find()) {
            boolean fromPlaceholder = false;
            for (int[] r : resolvedRanges) {
                if (tm.start(1) < r[1] && r[0] < tm.end(1)) { fromPlaceholder = true; break; }
            }
            if (!fromPlaceholder) continue;
            PostgresTableOpInfo op = new PostgresTableOpInfo();
            op.setDmlType(PostgresTableOpInfo.DmlType.DYNAMIC);
            op.setTableName(tm.group(1));
            op.setLine(line);
            result.getTableOps().add(op);
        }
    }

    /**
     * Records a {@code nextval('seq')}/{@code currval('seq')}/{@code setval('seq', n)}/
     * {@code lastval()} invocation. The sequence name is the first argument's string-literal
     * text (quotes and schema-dot preserved as written); {@code lastval()} takes no argument.
     */
    private void recordSequenceUsage(String op, PostgresParser.FunctionCallContext ctx) {
        PostgresSequenceInfo seq = new PostgresSequenceInfo();
        seq.setOperation(PostgresSequenceInfo.Operation.valueOf(op));
        seq.setLine(ctx.getStart().getLine());

        String seqName = null;
        PostgresParser.FunctionArgsContext argsCtx = ctx.functionArgs();
        if (argsCtx != null && argsCtx.expr() != null && !argsCtx.expr().isEmpty()) {
            String raw = argsCtx.expr(0).getText();
            if (raw.length() >= 2 && raw.startsWith("'") && raw.endsWith("'")) {
                raw = raw.substring(1, raw.length() - 1);
            }
            seqName = raw;
        }
        seq.setSequenceName(seqName);
        result.getSequences().add(seq);
    }

    /** Verbatim original source text of a parse-tree node's interval, whitespace and all. */
    private String sourceTextOf(org.antlr.v4.runtime.ParserRuleContext ctx) {
        if (ctx == null || ctx.getStart() == null || ctx.getStop() == null) return null;
        org.antlr.v4.runtime.misc.Interval interval = new org.antlr.v4.runtime.misc.Interval(
                ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex());
        return ctx.getStart().getInputStream().getText(interval);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PERFORM <call>(...);  — idiomatic PL/pgSQL side-effect-only function call.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitPerformStatement(PostgresParser.PerformStatementContext ctx) {
        PostgresCallInfo call = new PostgresCallInfo();
        PostgresParser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<PostgresParser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                call.setSchema(cleanId(ids.get(0).getText()));
                call.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                call.setName(cleanId(ids.get(0).getText()));
            }
        }
        call.setLine(ctx.getStart().getLine());
        result.getCalls().add(call);
        return null; // don't recurse into args for more calls
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RAISE statement → StatementInfo.
    //   Bare re-raise:      RAISE;                          → type "RAISE_BARE"
    //   With args/message:  RAISE EXCEPTION 'msg' USING ...  → type "RAISE"
    // Distinguished purely by whether any tokens exist between the leading RAISE id and the
    // terminating SEMI (mirrors how the Oracle bare-RAISE-vs-named-RAISE fix distinguishes the
    // no-argument form). The raw USING ERRCODE=.../MESSAGE=... text (if any) is preserved
    // verbatim in `detail` for downstream consumers, without attempting to fully re-parse it.
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Grammar/token-driven replacement for the old {@code ERRCODE\s*=\s*'…'} /
     * {@code MESSAGE\s*=\s*'…'} regexes. {@code raiseStatement} consumes its tail as a flat run
     * of terminals, so the USING options are read straight off that token run: an identifier
     * whose text is {@code optName}, followed by {@code =}, followed by a single-quoted string
     * literal. Because it matches on real lexer tokens, a "MESSAGE =" appearing inside another
     * string literal or a comment can no longer be picked up by mistake.
     *
     * @return the literal's contents (quotes stripped), or null if the option is not present.
     */
    private static String raiseUsingOption(PostgresParser.RaiseStatementContext ctx, String optName) {
        int n = ctx.getChildCount();
        for (int i = 0; i + 2 < n; i++) {
            if (child(ctx, i) == -1) continue;                                     // not a terminal
            if (!ctx.getChild(i).getText().equalsIgnoreCase(optName)) continue;
            if (child(ctx, i + 1) != PostgresParser.EQ) continue;
            int valTok = child(ctx, i + 2);
            // An E'…' escape string is as much a literal option value as a plain one.
            if (valTok != PostgresParser.SQ_STRING && valTok != PostgresParser.ESQ_STRING) continue;
            String lit = ctx.getChild(i + 2).getText();
            if (valTok == PostgresParser.ESQ_STRING) {
                return lit.length() >= 3 ? lit.substring(2, lit.length() - 1) : lit;
            }
            return lit.length() >= 2 ? lit.substring(1, lit.length() - 1) : lit;
        }
        return null;
    }

    @Override
    public Void visitRaiseStatement(PostgresParser.RaiseStatementContext ctx) {
        int line = ctx.getStart().getLine();
        // Bare re-raise: nothing but the leading RAISE id (and optional trailing SEMI) remains
        // once the leading "RAISE" id and trailing ';' are stripped from the raw source text.
        String raw = sourceTextOf(ctx);
        String withoutLeadId = raw == null ? "" : raw.trim();
        if (withoutLeadId.regionMatches(true, 0, "RAISE", 0, 5)) {
            withoutLeadId = withoutLeadId.substring(5);
        }
        if (withoutLeadId.endsWith(";")) {
            withoutLeadId = withoutLeadId.substring(0, withoutLeadId.length() - 1);
        }
        boolean isBare = withoutLeadId.trim().isEmpty();

        PostgresStatementInfo si = new PostgresStatementInfo(isBare ? "RAISE_BARE" : "RAISE", line);
        if (!isBare) {
            String errcode = raiseUsingOption(ctx, "ERRCODE");
            String message = raiseUsingOption(ctx, "MESSAGE");
            StringBuilder detail = new StringBuilder();
            if (errcode != null) detail.append("ERRCODE=").append(errcode);
            if (message != null) {
                if (detail.length() > 0) detail.append("; ");
                detail.append("MESSAGE=").append(message);
            }
            si.setDetail(detail.length() > 0 ? detail.toString() : withoutLeadId.trim());
        }
        result.getStatements().add(si);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction-control statements → StatementInfo (COMMIT / ROLLBACK / SAVEPOINT)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitTxnCommit(PostgresParser.TxnCommitContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("COMMIT", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnRollback(PostgresParser.TxnRollbackContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("ROLLBACK", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnRollbackTo(PostgresParser.TxnRollbackToContext ctx) {
        PostgresStatementInfo si = new PostgresStatementInfo("ROLLBACK", ctx.getStart().getLine());
        si.setSavepointName(cleanId(ctx.id().getText()));
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnSavepoint(PostgresParser.TxnSavepointContext ctx) {
        PostgresStatementInfo si = new PostgresStatementInfo("SAVEPOINT", ctx.getStart().getLine());
        si.setSavepointName(cleanId(ctx.id().getText()));
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Control-flow / query-shape statements → StatementInfo
    // Each override records ONE statement record per occurrence and still calls
    // visitChildren so nested constructs (and existing capture of calls, table
    // ops, cursors, transactions) are preserved.
    // ─────────────────────────────────────────────────────────────────────────

    /** IF … THEN … [ELSEIF …] [ELSE …] END IF */
    @Override
    public Void visitIfStatement(PostgresParser.IfStatementContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("IF", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /** CASE [expr] WHEN … THEN … [ELSE …] END CASE (statement form). */
    @Override
    public Void visitCaseStatement(PostgresParser.CaseStatementContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("CASE", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /** WHILE expr (DO | LOOP) body END (WHILE | LOOP) */
    @Override
    public Void visitWhileStatement(PostgresParser.WhileStatementContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("WHILE_LOOP", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * FOR loop — all forms (DB2 cursor-FOR, PL/pgSQL FOR rec IN <query> LOOP,
     * PL/pgSQL FOR i IN a..b LOOP). Hooked on the forStatement rule itself so the
     * opaque forExprTail handling does not affect detection.
     */
    @Override
    public Void visitForStatement(PostgresParser.ForStatementContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("FOR_LOOP", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /** Plain LOOP … END LOOP */
    @Override
    public Void visitLoopStatement(PostgresParser.LoopStatementContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("LOOP", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * Subquery — a parenthesized SELECT used as a table source
     * ( "( SELECT … ) AS alias" in a FROM/JOIN). This is the grammar's dedicated
     * subquery context (tableAtom # atomSubquery); one record per nested SELECT.
     */
    @Override
    public Void visitAtomSubquery(PostgresParser.AtomSubqueryContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("SUBQUERY", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * CTE — each common-table-expression definition inside a WITH clause
     * ( "name AS ( SELECT … )" ). One record per defined CTE.
     */
    @Override
    public Void visitCteDef(PostgresParser.CteDefContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("CTE", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * Subquery — a parenthesized SELECT used in an expression / WHERE / IN / EXISTS
     * ( "( SELECT … )" ) via the dedicated {@code subquery} grammar rule. One record
     * per nested SELECT; descends so inner table ops / calls are still captured.
     */
    @Override
    public Void visitSubquery(PostgresParser.SubqueryContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("SUBQUERY", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * MERGE … (PG15+ upsert-by-match). One record per MERGE statement; descends so
     * any USING (SELECT …) subquery and nested table ops are still captured.
     */
    @Override
    public Void visitMergeStatement(PostgresParser.MergeStatementContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("MERGE", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    /**
     * INSERT … ON CONFLICT … (upsert). Emitted from the onConflictClause so it is
     * recorded once per upsert, in addition to the enclosing INSERT table op.
     */
    @Override
    public Void visitOnConflictClause(PostgresParser.OnConflictClauseContext ctx) {
        result.getStatements().add(new PostgresStatementInfo("UPSERT", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Exception handler: EXCEPTION WHEN cond [OR cond]* THEN <stmts>
    // Statements inside are visited normally via visitChildren (still land in the flat
    // statement/call/table-op lists) — this override only adds the handler-level record.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitExceptionHandler(PostgresParser.ExceptionHandlerContext ctx) {
        PostgresExceptionHandlerInfo eh = new PostgresExceptionHandlerInfo();
        eh.setLine(ctx.getStart().getLine());
        eh.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());

        List<String> names = new java.util.ArrayList<>();
        for (PostgresParser.HandlerCondContext hc : ctx.handlerCond()) {
            names.add(cleanId(hc.getText()));
        }
        eh.setExceptionName(String.join(" OR ", names));
        eh.setStatementsCount(ctx.statement() != null ? ctx.statement().size() : 0);

        result.getExceptionHandlers().add(eh);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private String cleanId(String s) {
        if (s == null) return null;
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() > 1) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private String extractTableName(PostgresParser.TableRefContext ctx) {
        if (ctx == null) return null;
        List<PostgresParser.IdContext> ids = ctx.id();
        if (ids.isEmpty()) return null;
        // Last id is the table name
        return cleanId(ids.get(ids.size() - 1).getText());
    }

    private String extractTableSchema(PostgresParser.TableRefContext ctx) {
        if (ctx == null) return null;
        List<PostgresParser.IdContext> ids = ctx.id();
        // Bug (3-part db.schema.table audit): for a 3-part reference, the schema is the
        // second-to-last id (not the first, which would be the database) — ids.get(0) is only
        // correct in the 2-part case where it IS the schema. Any earlier database segment is
        // intentionally dropped rather than mis-attributed as schema/table.
        if (ids.size() > 1) {
            return cleanId(ids.get(ids.size() - 2).getText());
        }
        return null;
    }

    /**
     * The full SQL of the statement starting at this context.
     *
     * <p>Extends to the statement terminator rather than to {@code ctx.getStop()}. This dialect's
     * grammar rule for a DML statement ends before the {@code SET}/{@code WHERE} clauses, so the
     * context's own interval yielded only {@code "UPDATE orders"} — which parses to nothing useful
     * and produced no column detail at all. Verified against the alternative: SQL Server's rule
     * does span the whole statement and worked, which is what made the truncation visible here.
     *
     * <p>Quotes are tracked so a semicolon inside a string literal does not end the statement
     * early.
     */

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

    /**
     * True when the word at {@code [from, to]} is a keyword that must end a captured statement.
     *
     * <p>Restricted to {@code LOOP} and {@code BEGIN}: neither can appear inside a SQL expression,
     * so cutting at one can never truncate a valid statement. See the caller for why {@code THEN}
     * and {@code END} are excluded despite also ending blocks.
     */
    /**
     * Delegates to {@link com.plsql.dialect.StatementTextCapture}.
     *
     * <p>This method used to be a byte-identical copy of the DB2/PostgreSQL sibling, and
     * both copies carried the same two defects — running into a loop body, and closing a
     * parenthesis never opened — which then had to be found and fixed twice. Sharing it is
     * the point: the next fix lands in one place.
     */
    /**
     * Exact source text spanned by {@code ctx}'s own tokens, and nothing more.
     *
     * <p>Distinct from {@link #originalText}: that one goes through StatementTextCapture, which
     * deliberately extends the span out to a STATEMENT boundary (it exists to reconstruct whole
     * statements). For a sub-expression fragment — a single CREATE INDEX column element, or a
     * partial-index WHERE predicate — that over-extension swallows the rest of the column list
     * and the trailing ';', so those call sites use this narrow span instead.
     */
    private String spanText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        if (ctx == null || ctx.getStart() == null || ctx.getStop() == null) return null;
        org.antlr.v4.runtime.CharStream cs = ctx.getStart().getInputStream();
        if (cs == null) return ctx.getText();
        return cs.getText(org.antlr.v4.runtime.misc.Interval.of(
                ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
    }

    private String originalText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        return com.plsql.dialect.StatementTextCapture.capture(
                ctx, com.plsql.dialect.StatementTextCapture.DEFAULT_BLOCK_KEYWORDS);
    }
}
