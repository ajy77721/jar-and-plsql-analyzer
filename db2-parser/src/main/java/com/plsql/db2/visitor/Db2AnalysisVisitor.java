package com.plsql.db2.visitor;

import com.plsql.db2.grammar.Db2Lexer;
import com.plsql.db2.grammar.Db2Parser;
import com.plsql.db2.grammar.Db2ParserBaseVisitor;
import com.plsql.db2.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ANTLR4 visitor over the Db2Parser parse tree.
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
public class Db2AnalysisVisitor extends Db2ParserBaseVisitor<Void> {

    private final Db2RoutineInfo result;

    /** CTE names declared via WITH — these are not base tables and must be skipped. */
    private final Set<String> cteNames = new HashSet<>();

    /**
     * Bug 1 / Bug 8 support: variable name (upper-cased) -> leading literal-string content from
     * a {@code SET var = '<literal>' [|| <runtime-expr> ...];} assignment. Mirrors (a scoped-down
     * version of) Oracle's {@code varValueStack} accumulation used to resolve
     * {@code EXECUTE IMMEDIATE <var>}: only the literal prefix up to the first {@code ||} is kept,
     * so a fully-literal assignment resolves in full, while a variable-interrupted concatenation
     * (e.g. {@code 'SELECT * FROM ' || p_table || ...}) intentionally yields a prefix with no table
     * name — which correctly fails table extraction and flags the source as unresolved rather than
     * guessing a wrong table name.
     */
    private final Map<String, String> varLiteralSql = new HashMap<>();

    /**
     * Maps a PREPARE statement-name (upper-cased) to the resolved literal SQL text it prepared,
     * so a later {@code EXECUTE <stmtName>} can carry the same source text instead of leaving
     * {@link Db2DynamicSqlInfo#getSqlSource()} null — previously EXECUTE only recorded the
     * statement name, silently dropping the SQL text that was already known from PREPARE.
     */
    private final Map<String, String> preparedStmtSql = new HashMap<>();

    public Db2AnalysisVisitor(String objectName, String schema) {
        this.result = new Db2RoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
        result.setType(Db2RoutineInfo.RoutineType.PROCEDURE);
    }

    public Db2RoutineInfo getResult() {
        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Routine identity
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitBodyBlock(Db2Parser.BodyBlockContext ctx) {
        if (ctx.ATOMIC() != null) result.setAtomic(true);
        return visitChildren(ctx);
    }

    @Override
    public Void visitRoutineDefinition(Db2Parser.RoutineDefinitionContext ctx) {
        // Routine type: PROCEDURE or FUNCTION
        if (ctx.FUNCTION() != null) {
            result.setType(Db2RoutineInfo.RoutineType.FUNCTION);
        } else {
            result.setType(Db2RoutineInfo.RoutineType.PROCEDURE);
        }

        // routineName: (id DOT)? id
        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
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

        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // MODULES (task #28) — DB2's package analog
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * {@code CREATE MODULE m [; declarations] [END MODULE]} — records the module identity and
     * descends so any {@code CREATE VARIABLE} declarations inside are captured as module globals.
     * If the module also contains inline routines, {@link #visitRoutineDefinition} overwrites the
     * identity for the last one (the common published-member form is a separate ALTER MODULE).
     */
    @Override
    public Void visitCreateModule(Db2Parser.CreateModuleContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.MODULE);
        Db2Parser.RoutineNameContext rn = ctx.moduleName;
        if (rn != null) {
            String module = lastIdText(rn);
            result.setName(module);
            result.setModuleName(module);
            if (rn.id().size() == 2) result.setSchema(cleanId(rn.id(0).getText()));
        }
        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    /**
     * {@code ALTER MODULE m PUBLISH FUNCTION|PROCEDURE f(...) ... body} — the published member's
     * identity, with {@code moduleName=m} so the shared model keys it as MODULE.SUB. The body is
     * visited so calls/table ops inside the member are captured.
     */
    @Override
    public Void visitAlterModule(Db2Parser.AlterModuleContext ctx) {
        Db2Parser.RoutineNameContext moduleRn = ctx.moduleName;
        if (moduleRn != null) result.setModuleName(lastIdText(moduleRn));

        Db2Parser.AlterModuleActionContext action = ctx.alterModuleAction();
        if (action != null) {
            Db2Parser.RoutineNameContext member = action.memberName;
            if (action.VARIABLE() != null && member != null) {
                // ALTER MODULE … PUBLISH|ADD VARIABLE v type — a module-scoped variable (a module
                // global), not a routine: capture it for the globals facet.
                result.setType(Db2RoutineInfo.RoutineType.MODULE);
                Db2VariableInfo v = new Db2VariableInfo();
                v.setName(lastIdText(member));
                if (action.dataType() != null) v.setDataType(action.dataType().getText());
                if (action.defaultVal() != null) v.setDefaultValue(action.defaultVal().getText());
                v.setLine(ctx.getStart().getLine());
                result.getModuleVariables().add(v);
            } else {
                if (action.FUNCTION() != null) {
                    result.setType(Db2RoutineInfo.RoutineType.FUNCTION);
                } else if (action.PROCEDURE() != null) {
                    result.setType(Db2RoutineInfo.RoutineType.PROCEDURE);
                }
                if (member != null) {
                    String memberName = lastIdText(member);
                    result.setName(memberName);
                    // A member may be schema-qualified as module.member; the module is the qualifier.
                    if (member.id().size() == 2) result.setSchema(cleanId(member.id(0).getText()));
                }
            }
            result.setLineStart(ctx.getStart().getLine());
            result.setLineEnd(ctx.getStop() != null ? ctx.getStop().getLine() : ctx.getStart().getLine());
        }
        return visitChildren(ctx);
    }

    /**
     * {@code CREATE VARIABLE [schema.]name type [DEFAULT ...]} (top-level or module-scoped) —
     * DB2's global/module variable. Captured as a module global so the shared model surfaces it in
     * the globals facet (the Oracle package-globals shape).
     */
    @Override
    public Void visitCreateVariable(Db2Parser.CreateVariableContext ctx) {
        Db2VariableInfo v = new Db2VariableInfo();
        Db2Parser.RoutineNameContext rn = ctx.routineName();
        if (rn != null) {
            v.setName(lastIdText(rn));
            if (rn.id().size() == 2) result.setSchema(cleanId(rn.id(0).getText()));
        }
        if (ctx.dataType() != null) v.setDataType(ctx.dataType().getText());
        if (ctx.defaultVal() != null) v.setDefaultValue(ctx.defaultVal().getText());
        v.setLine(ctx.getStart().getLine());
        result.getModuleVariables().add(v);
        return visitChildren(ctx);
    }

    /** The final identifier of a (schema-qualified) routine name. */
    private String lastIdText(Db2Parser.RoutineNameContext rn) {
        if (rn == null || rn.id().isEmpty()) return null;
        return cleanId(rn.id(rn.id().size() - 1).getText());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE TRIGGER
    //
    // Captures the trigger identity/metadata, then descends into the SAME
    // bodyBlock/statement rules a procedure body uses, so CALLs and DML inside the
    // triggered action reach visitCallStatement / visitInsertStatement / … unchanged.
    // The trigger's subject table is recorded as a table op typed by the triggering
    // event (BEFORE INSERT ON orders → INSERT on orders), so lineage sees the table
    // the trigger is attached to, not only the tables its body touches.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateTrigger(Db2Parser.CreateTriggerContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.TRIGGER);

        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                result.setSchema(cleanId(ids.get(0).getText()));
                result.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                result.setName(cleanId(ids.get(0).getText()));
            }
        }

        // Timing: BEFORE / AFTER / INSTEAD OF (NO CASCADE is a qualifier on BEFORE).
        Db2Parser.TriggerTimingContext timing = ctx.triggerTiming();
        if (timing != null) {
            if (timing.INSTEAD() != null)     result.setTriggerTiming("INSTEAD OF");
            else if (timing.AFTER() != null)  result.setTriggerTiming("AFTER");
            else if (timing.BEFORE() != null) result.setTriggerTiming("BEFORE");
        }

        // Event: INSERT / DELETE / UPDATE [OF col, …]
        Db2TableOpInfo.DmlType eventDml = null;
        Db2Parser.TriggerEventContext ev = ctx.triggerEvent();
        if (ev != null) {
            if (ev.INSERT() != null) {
                result.setTriggerEvent("INSERT");
                eventDml = Db2TableOpInfo.DmlType.INSERT;
            } else if (ev.DELETE() != null) {
                result.setTriggerEvent("DELETE");
                eventDml = Db2TableOpInfo.DmlType.DELETE;
            } else if (ev.UPDATE() != null) {
                result.setTriggerEvent(ev.idList() != null
                        ? "UPDATE OF " + ev.idList().getText()
                        : "UPDATE");
                eventDml = Db2TableOpInfo.DmlType.UPDATE;
            }
        }

        // Granularity: FOR EACH ROW / FOR EACH STATEMENT
        Db2Parser.TriggerGranularityContext gran = ctx.triggerGranularity();
        if (gran != null) {
            result.setTriggerGranularity(gran.ROW() != null ? "ROW" : "STATEMENT");
        }

        // REFERENCING OLD AS o NEW AS n → ["OLD AS o", "NEW AS n"]
        if (ctx.triggerReferencing() != null) {
            for (Db2Parser.TriggerCorrelationContext c : ctx.triggerReferencing().triggerCorrelation()) {
                result.getTriggerReferencing().add(
                        cleanId(c.id(0).getText()) + " AS " + cleanId(c.id(1).getText()));
            }
        }

        // Subject table — same extraction helpers the DML visitors use.
        Db2Parser.TableRefContext tref = ctx.tableRef();
        if (tref != null) {
            String tbl = extractTableName(tref);
            String sch = extractTableSchema(tref);
            result.setTriggerTable(sch != null ? sch + "." + tbl : tbl);

            Db2TableOpInfo op = new Db2TableOpInfo();
            op.setDmlType(eventDml != null ? eventDml : Db2TableOpInfo.DmlType.SELECT);
            op.setTableName(tbl);
            op.setSchemaName(sch);
            op.setLine(tref.getStart().getLine());
            op.setSqlText(originalText(ctx.triggerEvent() != null ? ctx.triggerEvent() : ctx));
            result.getTableOps().add(op);
        }

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE VIEW
    //
    // The defining SELECT is a normal selectStatement, so descending here routes its
    // FROM/JOIN table references through visitFromClause — the view's base tables and
    // joins are captured by the same code path as any other query.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateView(Db2Parser.CreateViewContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.VIEW);

        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                result.setSchema(cleanId(ids.get(0).getText()));
                result.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                result.setName(cleanId(ids.get(0).getText()));
            }
        }

        if (ctx.selectStatement() != null) {
            result.setViewQuery(originalText(ctx.selectStatement()));
        }

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE SEQUENCE
    //
    // Captures the sequence identity plus whichever generation options the DDL
    // spells out. Options are order-independent in the grammar, so each
    // sequenceOption child is inspected by its leading token rather than by
    // position. Numeric values are stored as written (including a leading minus)
    // so "INCREMENT BY -1" survives round-tripping.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateSequence(Db2Parser.CreateSequenceContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.SEQUENCE);

        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                result.setSchema(cleanId(ids.get(0).getText()));
                result.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                result.setName(cleanId(ids.get(0).getText()));
            }
        }

        if (ctx.dataType() != null) {
            result.setSequenceDataType(ctx.dataType().getText());
        }

        for (Db2Parser.SequenceOptionContext opt : ctx.sequenceOption()) {
            boolean no = opt.NO() != null;
            String value = opt.sequenceValue() != null ? opt.sequenceValue().getText() : null;

            if (opt.START() != null) {
                result.setSequenceStartWith(value);
            } else if (opt.INCREMENT() != null) {
                result.setSequenceIncrementBy(value);
            } else if (opt.MINVALUE() != null) {
                if (no) result.setSequenceNoMinValue(true);
                else    result.setSequenceMinValue(value);
            } else if (opt.MAXVALUE() != null) {
                if (no) result.setSequenceNoMaxValue(true);
                else    result.setSequenceMaxValue(value);
            } else if (opt.CACHE() != null) {
                if (no) result.setSequenceNoCache(true);
                else if (opt.INT_NUM() != null) result.setSequenceCache(opt.INT_NUM().getText());
            } else if (opt.CYCLE() != null) {
                if (no) result.setSequenceNoCycle(true);
                else    result.setSequenceCycle(true);
            } else if (opt.ORDER() != null) {
                result.setSequenceOrder(!no);
            }
        }

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE INDEX
    //
    // The indexed table is recorded as a table op via the same extraction helpers the
    // DML visitors use, so lineage sees the index → table dependency; the key columns
    // are captured in declaration order with their explicit sort direction, if any.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateIndex(Db2Parser.CreateIndexContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.INDEX);
        result.setIndexUnique(ctx.UNIQUE() != null);

        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                result.setSchema(cleanId(ids.get(0).getText()));
                result.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                result.setName(cleanId(ids.get(0).getText()));
            }
        }

        Db2Parser.TableRefContext tref = ctx.tableRef();
        if (tref != null) {
            String tbl = extractTableName(tref);
            String sch = extractTableSchema(tref);
            result.setIndexTable(sch != null ? sch + "." + tbl : tbl);

            Db2TableOpInfo op = new Db2TableOpInfo();
            op.setDmlType(Db2TableOpInfo.DmlType.SELECT);
            op.setTableName(tbl);
            op.setSchemaName(sch);
            op.setLine(tref.getStart().getLine());
            op.setSqlText(originalText(ctx));
            result.getTableOps().add(op);
        }

        for (Db2Parser.IndexColumnContext col : ctx.indexColumn()) {
            String name = cleanId(col.id().getText());
            if (col.ASC() != null)       name += " ASC";
            else if (col.DESC() != null) name += " DESC";
            result.getIndexColumns().add(name);
        }

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ALTER TABLE
    //
    // The altered table is reported as an UPDATE-flavoured table op (the statement
    // mutates that object), and every ADD ... FOREIGN KEY ... REFERENCES target is
    // reported as a second, SELECT-flavoured op — that pair is the real lineage edge
    // the statement carries, and it is what generic consumers read. Each action also
    // gets a short human-readable descriptor in alterActions, in written order.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitAlterTable(Db2Parser.AlterTableContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.ALTER_TABLE);

        Db2Parser.TableRefContext tref = ctx.tableRef();
        String tbl = extractTableName(tref);
        String sch = extractTableSchema(tref);
        String qualified = sch != null ? sch + "." + tbl : tbl;
        result.setAlterTableName(qualified);
        if (result.getName() == null || result.getName().isEmpty()) {
            result.setName(tbl);
        }
        if (sch != null && result.getSchema() == null) {
            result.setSchema(sch);
        }

        Db2TableOpInfo target = new Db2TableOpInfo();
        target.setDmlType(Db2TableOpInfo.DmlType.UPDATE);
        target.setTableName(tbl);
        target.setSchemaName(sch);
        target.setLine(tref.getStart().getLine());
        target.setSqlText(originalText(ctx));
        result.getTableOps().add(target);

        for (Db2Parser.AlterActionContext act : ctx.alterAction()) {
            if (act instanceof Db2Parser.AlterAddForeignKeyContext) {
                Db2Parser.AlterAddForeignKeyContext fk = (Db2Parser.AlterAddForeignKeyContext) act;
                Db2Parser.TableRefContext ref = fk.tableRef();
                String refTbl = extractTableName(ref);
                String refSch = extractTableSchema(ref);
                String refQualified = refSch != null ? refSch + "." + refTbl : refTbl;
                result.getAlterReferencedTables().add(refQualified);

                Db2TableOpInfo op = new Db2TableOpInfo();
                op.setDmlType(Db2TableOpInfo.DmlType.SELECT);
                op.setTableName(refTbl);
                op.setSchemaName(refSch);
                op.setLine(ref.getStart().getLine());
                op.setSqlText(originalText(act));
                result.getTableOps().add(op);

                result.getAlterActions().add("ADD CONSTRAINT "
                        + constraintName(fk.id()) + "FOREIGN KEY ("
                        + fk.idList(0).getText() + ") REFERENCES " + refQualified);

                // The structured form. The rendered descriptor above keeps the source columns
                // only as raw text and never carried the referenced columns at all, though both
                // lists are right here in the tree — a declared relationship reduced to prose.
                Db2ForeignKeyInfo fkInfo = new Db2ForeignKeyInfo();
                fkInfo.setConstraintName(fk.id() != null ? cleanId(fk.id().getText()) : null);
                fkInfo.setTable(tbl);
                fkInfo.setSchema(sch);
                fkInfo.setReferencedTable(refTbl);
                fkInfo.setReferencedSchema(refSch);
                fkInfo.getColumns().addAll(columnNames(fk.idList(0)));
                // The referenced column list is optional in DB2 (it defaults to the parent's
                // primary key), so its absence is a legal statement, not missing data.
                if (fk.idList().size() > 1) {
                    fkInfo.getReferencedColumns().addAll(columnNames(fk.idList(1)));
                }
                fkInfo.setLine(fk.getStart().getLine());
                result.getForeignKeys().add(fkInfo);
            } else if (act instanceof Db2Parser.AlterAddPrimaryKeyContext) {
                Db2Parser.AlterAddPrimaryKeyContext pk = (Db2Parser.AlterAddPrimaryKeyContext) act;
                result.getAlterActions().add("ADD CONSTRAINT " + constraintName(pk.id())
                        + "PRIMARY KEY (" + pk.idList().getText() + ")");
            } else if (act instanceof Db2Parser.AlterAddUniqueContext) {
                Db2Parser.AlterAddUniqueContext uq = (Db2Parser.AlterAddUniqueContext) act;
                result.getAlterActions().add("ADD CONSTRAINT " + constraintName(uq.id())
                        + "UNIQUE (" + uq.idList().getText() + ")");
            } else if (act instanceof Db2Parser.AlterAddCheckContext) {
                Db2Parser.AlterAddCheckContext ck = (Db2Parser.AlterAddCheckContext) act;
                result.getAlterActions().add("ADD CONSTRAINT " + constraintName(ck.id())
                        + "CHECK (" + originalText(ck.expr()) + ")");
            } else if (act instanceof Db2Parser.AlterAddColumnContext) {
                Db2Parser.AlterAddColumnContext ac = (Db2Parser.AlterAddColumnContext) act;
                String col = cleanId(ac.id().getText());
                result.getAlterAddedColumns().add(col);
                result.getAlterActions().add("ADD COLUMN " + col + " " + ac.dataType().getText());

                // ADD COLUMN c INT REFERENCES parent (id) — the column-level spelling of a
                // foreign key. It declares the same relationship as the table-level form, so
                // it feeds the same structured list; the added column is the source column.
                if (ac.REFERENCES() != null && ac.tableRef() != null) {
                    Db2Parser.TableRefContext ref = ac.tableRef();
                    String refTbl = extractTableName(ref);
                    String refSch = extractTableSchema(ref);
                    String refQualified = refSch != null ? refSch + "." + refTbl : refTbl;
                    result.getAlterReferencedTables().add(refQualified);

                    Db2TableOpInfo op = new Db2TableOpInfo();
                    op.setDmlType(Db2TableOpInfo.DmlType.SELECT);
                    op.setTableName(refTbl);
                    op.setSchemaName(refSch);
                    op.setLine(ref.getStart().getLine());
                    op.setSqlText(originalText(act));
                    result.getTableOps().add(op);

                    Db2ForeignKeyInfo fkInfo = new Db2ForeignKeyInfo();
                    fkInfo.setTable(tbl);
                    fkInfo.setSchema(sch);
                    fkInfo.setReferencedTable(refTbl);
                    fkInfo.setReferencedSchema(refSch);
                    fkInfo.getColumns().add(col);
                    // Same convention as the table-level form: an omitted parent column list
                    // is a legal statement (it defaults to the parent's primary key).
                    if (ac.idList() != null) {
                        fkInfo.getReferencedColumns().addAll(columnNames(ac.idList()));
                    }
                    fkInfo.setLine(ac.getStart().getLine());
                    result.getForeignKeys().add(fkInfo);
                }
            } else if (act instanceof Db2Parser.AlterDropColumnContext) {
                Db2Parser.AlterDropColumnContext dc = (Db2Parser.AlterDropColumnContext) act;
                String col = cleanId(dc.id().getText());
                result.getAlterDroppedColumns().add(col);
                result.getAlterActions().add("DROP COLUMN " + col);
            } else if (act instanceof Db2Parser.AlterDropConstraintContext) {
                Db2Parser.AlterDropConstraintContext dk = (Db2Parser.AlterDropConstraintContext) act;
                result.getAlterActions().add("DROP CONSTRAINT " + cleanId(dk.id().getText()));
            } else {
                result.getAlterActions().add(originalText(act));
            }
        }

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        // Deliberately does NOT descend: every child of interest (the altered table, the
        // referenced tables, the action descriptors) has already been consumed above, and
        // descending would let a CHECK expression's function calls leak into the call list.
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE TYPE / CREATE DISTINCT TYPE
    //
    // A user-defined type is a schema object, not a data flow, so no table op is
    // emitted — only its identity, its kind (STRUCTURED vs DISTINCT) and its shape
    // (attributes for structured, base type for distinct).
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCreateType(Db2Parser.CreateTypeContext ctx) {
        result.setType(Db2RoutineInfo.RoutineType.TYPE);

        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                result.setSchema(cleanId(ids.get(0).getText()));
                result.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                result.setName(cleanId(ids.get(0).getText()));
            }
        }

        if (ctx.dataType() != null) {
            // CREATE [DISTINCT] TYPE x AS <base-type> — a distinct (strong alias) type.
            result.setTypeKind("DISTINCT");
            result.setTypeBaseType(ctx.dataType().getText());
        } else {
            result.setTypeKind("STRUCTURED");
            for (Db2Parser.TypeAttributeContext attr : ctx.typeAttribute()) {
                result.getTypeAttributes().add(
                        cleanId(attr.id().getText()) + " " + attr.dataType().getText());
            }
            if (ctx.typeModeClause() != null) {
                result.setTypeMode(ctx.typeModeClause().id(1).getText());
            }
        }

        // Trailing "WITH COMPARISONS" (the only WITH clause DB2 allows here).
        if (ctx.WITH() != null && ctx.id() != null) {
            result.setTypeWithComparisons("COMPARISONS".equalsIgnoreCase(ctx.id().getText()));
        }

        result.setLineStart(ctx.getStart().getLine());
        result.setLineEnd(ctx.getStop() != null
                ? ctx.getStop().getLine()
                : ctx.getStart().getLine());

        return null;
    }

    /** "name " for a named constraint, "" for an anonymous one — used to build descriptors. */
    /** The column names of an {@code idList}, in declaration order — the order is the pairing. */
    private List<String> columnNames(Db2Parser.IdListContext ctx) {
        if (ctx == null) return List.of();
        List<String> out = new ArrayList<>();
        for (Db2Parser.IdContext id : ctx.id()) {
            out.add(cleanId(id.getText()));
        }
        return out;
    }

    private String constraintName(Db2Parser.IdContext id) {
        return id != null ? cleanId(id.getText()) + " " : "";
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parameters
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitParam(Db2Parser.ParamContext ctx) {
        Db2ParamInfo p = new Db2ParamInfo();

        // Direction: IN_KW / OUT_KW / INOUT
        if (ctx.INOUT() != null) {
            p.setDirection(Db2ParamInfo.Direction.INOUT);
        } else if (ctx.OUT_KW() != null) {
            p.setDirection(Db2ParamInfo.Direction.OUT);
        } else {
            p.setDirection(Db2ParamInfo.Direction.IN);
        }

        // Name (no @ prefix in DB2)
        p.setName(cleanId(ctx.id().getText()));

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
    public Void visitDeclareVariableStmt(Db2Parser.DeclareVariableStmtContext ctx) {
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

        Db2VariableInfo v = new Db2VariableInfo();
        v.setName(name);
        v.setDataType(dataType);
        if (ctx.expr() != null) {
            v.setDefaultValue(ctx.expr().getText());
        }
        v.setLine(ctx.getStart().getLine());
        result.getVariables().add(v);
        return null; // no nested declares
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE cursor
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareCursorStmt(Db2Parser.DeclareCursorStmtContext ctx) {
        Db2CursorInfo c = new Db2CursorInfo();
        c.setName(cleanId(ctx.id(0).getText()));
        boolean withHold = false;
        boolean withReturn = false;
        for (Db2Parser.CursorOptionContext opt : ctx.cursorOption()) {
            if (opt.HOLD() != null) withHold = true;
            // Task #26: WITH RETURN [TO CALLER|TO CLIENT] — the cursor's result set is returned to
            // the caller. HOLD and RETURN are alternatives on the same cursorOption rule, so read
            // both in one pass instead of stopping at the first.
            if (opt.RETURN() != null) withReturn = true;
        }
        c.setWithHold(withHold);
        c.setWithReturn(withReturn);
        if (withReturn) {
            result.setResultSets(result.getResultSets() + 1);
            result.setReturnsResultSet(true);
        }
        if (ctx.selectStatement() != null) {
            // Bug 5 fix: ctx.getText() concatenates only visible-channel tokens with no
            // separators, squishing the SQL onto one run-on line. Reuse the same
            // originalText()/StatementTextCapture helper already used elsewhere in this file
            // for regular statement sqlText, which preserves whitespace/comments correctly.
            c.setSelectText(originalText(ctx.selectStatement()));
        } else if (ctx.id().size() > 1) {
            // Bug 1 fix: grammar's second alternative — DECLARE id CURSOR FOR id — is a cursor
            // over a previously PREPAREd statement name, not an inline SELECT. This was silently
            // dropped before: no cursor selectText, no cross-reference to the PREPARE, and (for
            // the variable-interrupted case) no unresolved-source flag either. Look up the
            // PREPARE record by statement name and resolve it exactly like EXECUTE IMMEDIATE does.
            String stmtName = cleanId(ctx.id(1).getText());
            Db2DynamicSqlInfo prep = findPrepareByStmtName(stmtName);
            if (prep != null) {
                boolean isVariable = !isQuotedLiteral(prep.getSqlSource());
                String resolved = resolveDynamicSqlSource(
                        prep.getSqlSource(), isVariable, ctx.getStart().getLine(), prep);
                if (resolved != null) {
                    c.setSelectText(resolved);
                }
            }
        }
        c.setLine(ctx.getStart().getLine());
        result.getCursors().add(c);
        // Visit children to catch table ops in the SELECT inside the cursor
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SET var = expr — tracked only to support dynamic-SQL variable resolution
    // (Bug 1 / Bug 8): SET v_sql = '<literal>' [|| runtime-expr ...];
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitSetStatement(Db2Parser.SetStatementContext ctx) {
        if (ctx.expr() != null && !ctx.id().isEmpty()) {
            String varName = cleanId(ctx.id(0).getText());
            String exprText = originalText(ctx.expr());
            // Constant-folding first: a concatenation made up ONLY of string literals
            // (`'SELECT * FROM ' || 'CAT_TBL' || ' WHERE x = 1'`) has a fully-known value at
            // analysis time, so folding it is not a guess. If ANY operand is a variable/
            // expression the fold refuses and we fall back to the leading-literal-prefix rule
            // below, which deliberately truncates at the first `||` so a table name contributed
            // by a runtime variable can never be matched (Option C: flag, don't guess).
            // Note: the fold reads ctx.expr().getText() (the concatenated token texts of the
            // expr rule ONLY), not originalText(): the latter's forward scan deliberately runs
            // on to the statement terminator, so it carries a trailing `;` that would make an
            // otherwise all-literal expression look like it had a non-literal operand. Dropping
            // inter-token whitespace is harmless here — only the literals' own contents (which
            // SQ_STRING tokens carry verbatim) end up in the folded string.
            String resolved = foldLiteralConcat(ctx.expr().getText());
            if (resolved == null) resolved = literalPrefixOf(exprText);
            if (resolved != null) {
                varLiteralSql.put(varName.toUpperCase(), resolved);
            }
        }
        return visitChildren(ctx);
    }

    /**
     * Returns the unquoted content of the leading single-quoted string literal in {@code exprText},
     * or {@code null} if the expression does not start with one. For a fully-literal assignment
     * (no concatenation) this is the whole SQL text; for {@code '<literal>' || <var> || ...} it is
     * only the text up to (not including) the closing quote of that first literal — deliberately
     * dropping everything the runtime variable contributes, so a table name that lives past that
     * point does not match and the source is correctly treated as unresolved.
     */
    /**
     * Constant-folds a concatenation whose operands are ALL single-quoted string literals, e.g.
     * {@code 'SELECT * FROM ' || 'CAT_TBL' || ' WHERE x = 1'} → {@code SELECT * FROM CAT_TBL
     * WHERE x = 1}. Returns {@code null} the moment anything other than a literal or a {@code ||}
     * operator appears (a variable, a function call, a {@code +}, trailing junk) — so the
     * "flag rather than guess" behaviour for variable-interrupted concatenation is untouched;
     * this only handles the case where the value is already fully known from the source text.
     *
     * <p>A single literal with no {@code ||} also folds (same result the prefix rule gives).
     * Escaped {@code ''} is kept verbatim, matching {@link #literalPrefixOf}.
     */
    private String foldLiteralConcat(String exprText) {
        if (exprText == null) return null;
        String s = exprText.trim();
        if (s.isEmpty() || s.charAt(0) != '\'') return null;
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (true) {
            if (i >= s.length() || s.charAt(i) != '\'') return null;
            int j = i + 1;
            boolean closed = false;
            while (j < s.length()) {
                if (s.charAt(j) == '\'') {
                    if (j + 1 < s.length() && s.charAt(j + 1) == '\'') { j += 2; continue; }
                    closed = true;
                    break;
                }
                j++;
            }
            if (!closed) return null;          // unterminated literal — malformed, bail out
            out.append(s, i + 1, j);
            i = j + 1;
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= s.length()) return out.toString();   // consumed the whole expression
            if (!s.startsWith("||", i)) return null;      // something that is not a `||` operand
            i += 2;
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }
    }

    private String literalPrefixOf(String exprText) {
        if (exprText == null) return null;
        String s = exprText.trim();
        if (s.isEmpty() || s.charAt(0) != '\'') return null;
        int i = 1;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\'') {
                if (i + 1 < s.length() && s.charAt(i + 1) == '\'') { i += 2; continue; } // escaped ''
                return s.substring(1, i);
            }
            i++;
        }
        return null; // unterminated — malformed input, bail out rather than guess
    }

    private boolean isQuotedLiteral(String s) {
        return s != null && s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'';
    }

    /** Finds the most recent PREPARE record with the given statement name, or {@code null}. */
    private Db2DynamicSqlInfo findPrepareByStmtName(String stmtName) {
        if (stmtName == null) return null;
        for (int i = result.getDynamicSql().size() - 1; i >= 0; i--) {
            Db2DynamicSqlInfo d = result.getDynamicSql().get(i);
            if (d.getKind() == Db2DynamicSqlInfo.Kind.PREPARE && stmtName.equalsIgnoreCase(d.getStmtName())) {
                return d;
            }
        }
        return null;
    }

    /**
     * Shared dynamic-SQL resolution logic — used by both {@code EXECUTE IMMEDIATE <var>} and
     * {@code DECLARE cur CURSOR FOR <preparedStmtName>} (via its PREPARE record) so the two idioms
     * don't carry two copies of the same literal-vs-variable-interrupted resolution rules.
     *
     * <p>Mirrors Oracle's {@code PlSqlAnalysisVisitor#visitExecute_immediate}: a fully-literal
     * source resolves and yields a DYNAMIC table op; a variable whose traced value is
     * variable-interrupted (table name not literally present) cannot be resolved, and {@code dsi}
     * is flagged via {@code unresolvedVarName} (Option C) instead of guessing.
     *
     * @return the resolved literal SQL text, or {@code null} if unresolved.
     */
    private String resolveDynamicSqlSource(String source, boolean isVariable, int line, Db2DynamicSqlInfo dsi) {
        if (source == null) return null;
        if (!isVariable) {
            if (isQuotedLiteral(source)) {
                String inner = source.substring(1, source.length() - 1);
                if (extractDynamicSqlTable(inner, line)) return inner;
            }
            return null;
        }
        String literal = varLiteralSql.get(source.toUpperCase());
        if (literal != null && extractDynamicSqlTable(literal, line)) {
            return literal;
        }
        if (dsi != null) dsi.setUnresolvedVarName(source);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CALL statement
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitCallStatement(Db2Parser.CallStatementContext ctx) {
        Db2CallInfo call = new Db2CallInfo();
        Db2Parser.RoutineNameContext rnCtx = ctx.routineName();
        if (rnCtx != null) {
            List<Db2Parser.IdContext> ids = rnCtx.id();
            if (ids.size() == 2) {
                call.setSchema(cleanId(ids.get(0).getText()));
                call.setName(cleanId(ids.get(1).getText()));
            } else if (ids.size() == 1) {
                call.setName(cleanId(ids.get(0).getText()));
            }
        }
        call.setLine(ctx.getStart().getLine());
        result.getCalls().add(call);
        // Recurse into the argument list so nested function calls (e.g.
        // CALL PROC(FN(x))) are also captured as call edges via visitFunctionCall.
        // routineName itself is a distinct grammar rule (not functionCall), so this
        // does not re-record the outer CALL target.
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: INSERT
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitInsertStatement(Db2Parser.InsertStatementContext ctx) {
        Db2TableOpInfo op = new Db2TableOpInfo();
        op.setDmlType(Db2TableOpInfo.DmlType.INSERT);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(originalText(ctx));
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: UPDATE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitUpdateStatement(Db2Parser.UpdateStatementContext ctx) {
        Db2TableOpInfo op = new Db2TableOpInfo();
        op.setDmlType(Db2TableOpInfo.DmlType.UPDATE);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(originalText(ctx));
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DML: DELETE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeleteStatement(Db2Parser.DeleteStatementContext ctx) {
        Db2TableOpInfo op = new Db2TableOpInfo();
        op.setDmlType(Db2TableOpInfo.DmlType.DELETE);
        op.setTableName(extractTableName(ctx.tableRef()));
        op.setSchemaName(extractTableSchema(ctx.tableRef()));
        op.setLine(ctx.getStart().getLine());
        op.setSqlText(originalText(ctx));
        result.getTableOps().add(op);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SELECT — register CTE names up-front so their references are skipped, then
    // descend so FROM/JOIN contexts (incl. inside CTE bodies) are captured.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitSelectStatement(Db2Parser.SelectStatementContext ctx) {
        // A nested SELECT that is a scalar subquery (exprAtom: LPAREN selectStatement RPAREN)
        // or a derived-table subquery in FROM (tableAtom #atomSubquery) is a SUBQUERY.
        // CTE bodies (cteDef), FOR-loop cursors (forStatement), declared cursors, and
        // top-level statement SELECTs are NOT subqueries and are excluded by parent type.
        org.antlr.v4.runtime.tree.ParseTree parent = ctx.getParent();
        if (parent instanceof Db2Parser.ExprAtomContext
                || parent instanceof Db2Parser.AtomSubqueryContext
                || parent instanceof Db2Parser.SelectRestTokenContext
                // A subquery in a MERGE ON condition / WHEN-branch predicate — the MERGE tail's
                // equivalent of SelectRestTokenContext (see mergeRestToken's first alternative).
                || parent instanceof Db2Parser.MergeRestTokenContext) {
            result.getStatements().add(new Db2StatementInfo("SUBQUERY", ctx.getStart().getLine()));
        }
        if (ctx.withClause() != null) {
            for (Db2Parser.CteDefContext cte : ctx.withClause().cteDef()) {
                if (cte.id() != null) {
                    cteNames.add(cleanId(cte.id().getText()).toUpperCase());
                }
                // Emit a common "CTE" statement per WITH factoring clause so it is captured
                // uniformly with the other dialects (visible in the UI construct breakdown).
                result.getStatements().add(new Db2StatementInfo("CTE", cte.getStart().getLine()));
            }
        }
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FROM clause — base tables + joins (grammar contexts only, no text scanning)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitFromClause(Db2Parser.FromClauseContext ctx) {
        for (Db2Parser.TableReferenceContext tr : ctx.tableReference()) {
            processTableReference(tr);
        }
        return visitChildren(ctx);   // recurse for nested subquery SELECTs / CTEs
    }

    /**
     * Walk one {@code tableReference} (a base tableAtom followed by zero or more
     * joinClauses). The base atom becomes a SELECT table op; each JOINed atom
     * becomes its own SELECT table op AND a Db2JoinInfo attached to the base op.
     */
    private void processTableReference(Db2Parser.TableReferenceContext tr) {
        Db2TableOpInfo baseOp = tableOpForAtom(tr.tableAtom());

        java.util.Map<String, String> aliasMap = buildAliasMap(tr);
        int queryId = tr.getStart().getLine();

        for (Db2Parser.JoinClauseContext jc : tr.joinClause()) {
            Db2TableOpInfo joinedOp = tableOpForAtom(jc.tableAtom());
            if (baseOp == null) continue;

            if (jc.tableAtom() instanceof Db2Parser.AtomTableContext jat) {
                Db2Parser.TableRefContext jref = jat.tableRef();
                // A joined CTE ref yields no standalone table op (handled above) but the
                // JOIN relationship itself is still recorded on the base table.
                Db2JoinInfo j = new Db2JoinInfo();
                j.setJoinType(joinTypeText(jc.joinType()));
                j.setJoinedTable(extractTableName(jref));
                j.setJoinedTableSchema(extractTableSchema(jref));
                if (jat.id() != null) {
                    j.setJoinedTableAlias(cleanId(jat.id().getText()));
                }
                if (jc.expr() != null) {
                    j.setCondition(originalText(jc.expr()));
                }
                j.setLine(jc.getStart().getLine());
                j.setQueryId(queryId);
                resolveLeftSide(j, aliasMap, jc.expr());
                baseOp.getJoins().add(j);
            }
        }
    }

    /** Build alias->table (both upper-cased) for the base atom and each joined atom of a tableReference. */
    private java.util.Map<String, String> buildAliasMap(Db2Parser.TableReferenceContext tr) {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        addAtomAlias(m, tr.tableAtom());
        for (Db2Parser.JoinClauseContext jc : tr.joinClause()) {
            addAtomAlias(m, jc.tableAtom());
        }
        return m;
    }

    private void addAtomAlias(java.util.Map<String, String> m, Db2Parser.TableAtomContext atom) {
        if (!(atom instanceof Db2Parser.AtomTableContext at)) return;
        String tbl = extractTableName(at.tableRef());
        if (tbl == null) return;
        tbl = tbl.toUpperCase();
        if (at.id() != null) m.put(cleanId(at.id().getText()).toUpperCase(), tbl);
        m.put(tbl, tbl);
    }

    /**
     * Resolve leftTable/leftTableAlias from the ON-condition qualifier that is NOT the joined
     * table.
     *
     * <p>Grammar-driven: the ON condition's {@code expr} parse tree is walked for qualified
     * references ({@code exprAtom : id (DOT id)* [DOT STAR]}) and the leading {@code id} of each
     * is taken as the qualifier, in textual (traversal) order. This replaces the previous
     * {@code ([A-Za-z_]\w*)\s*\.} text scan of the condition, which could not tell a real column
     * qualifier from the same character sequence inside a string literal or a comment.
     */
    private void resolveLeftSide(Db2JoinInfo j, java.util.Map<String, String> aliasMap,
                                 Db2Parser.ExprContext onExpr) {
        if (onExpr == null) return;
        String joinedAlias = j.getJoinedTableAlias() != null ? j.getJoinedTableAlias().toUpperCase() : null;
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        collectQualifiers(onExpr, seen);
        for (String a : seen) {
            if (joinedAlias != null && a.equals(joinedAlias)) continue;
            String t = aliasMap.get(a);
            if (t != null) { j.setLeftTableAlias(a); j.setLeftTable(t); return; }
        }
    }

    /**
     * Depth-first (= textual order) collection of the leading identifier of every qualified
     * reference {@code <qualifier> . <rest>} in a parse subtree, upper-cased.
     */
    private void collectQualifiers(org.antlr.v4.runtime.tree.ParseTree node,
                                   java.util.LinkedHashSet<String> out) {
        if (node instanceof Db2Parser.ExprAtomContext atom
                && atom.getChildCount() >= 2
                && atom.getChild(0) instanceof Db2Parser.IdContext qual
                && isDot(atom.getChild(1))) {
            out.add(cleanId(qual.getText()).toUpperCase());
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectQualifiers(node.getChild(i), out);
        }
    }

    private boolean isDot(org.antlr.v4.runtime.tree.ParseTree t) {
        return t instanceof org.antlr.v4.runtime.tree.TerminalNode tn
                && tn.getSymbol().getType() == Db2Parser.DOT;
    }

    /**
     * Adds a SELECT table op for the given tableAtom and returns it (null for a CTE
     * reference or a subquery atom, whose inner SELECT is handled by recursion).
     */
    private Db2TableOpInfo tableOpForAtom(Db2Parser.TableAtomContext atom) {
        if (atom instanceof Db2Parser.AtomTableContext at) {
            if (isCte(at.tableRef())) return null;
            Db2TableOpInfo op = new Db2TableOpInfo();
            op.setDmlType(Db2TableOpInfo.DmlType.SELECT);
            op.setTableName(extractTableName(at.tableRef()));
            op.setSchemaName(extractTableSchema(at.tableRef()));
            if (at.id() != null) op.setAlias(cleanId(at.id().getText()));
            op.setLine(at.getStart().getLine());
            // Without this a SELECT carried no SQL, so no column detail could be derived from it.
            op.setSqlText(enclosingStatementText(at));
            result.getTableOps().add(op);
            return op;
        }
        if (atom instanceof Db2Parser.AtomParenContext paren) {
            Db2TableOpInfo first = null;
            for (Db2Parser.TableReferenceContext tr : paren.tableReference()) {
                processTableReference(tr);
                if (first == null && !result.getTableOps().isEmpty()) {
                    first = result.getTableOps().get(result.getTableOps().size() - 1);
                }
            }
            return first;
        }
        return null;   // AtomSubquery — inner SELECT handled by visitChildren recursion
                        // AtomFunc (e.g. XMLTABLE(...)) and AtomTableFunctionCall (TABLE(fn(...)))
                        // are derived/table-valued-function sources, not base tables, so no table
                        // op is emitted for them. AtomTableFunctionCall's inner functionCall is
                        // still visited (and captured as a call) via ordinary visitChildren
                        // recursion over the parse tree.
    }

    private boolean isCte(Db2Parser.TableRefContext tr) {
        if (tr == null) return false;
        String schema = extractTableSchema(tr);
        String name = extractTableName(tr);
        return schema == null && name != null && cteNames.contains(name.toUpperCase());
    }

    /** Render a joinType context as spaced upper-case text (e.g. "LEFT OUTER"), or "JOIN" if absent. */
    private String joinTypeText(Db2Parser.JoinTypeContext jt) {
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

    /**
     * Authoritative catalog of IBM Db2 (LUW) engine built-in / scalar / aggregate / OLAP
     * function names (uppercase). A bare (non schema-qualified) function whose name is in
     * this set is an engine built-in and is NOT recorded as a user-routine call edge.
     * Schema-qualified calls (e.g. {@code MYSCHEMA.FOO(...)}) are always treated as
     * user-routine calls regardless of this set — see {@link #visitFunctionCall}.
     *
     * NOTE: the window/ranking functions ROW_NUMBER, RANK, DENSE_RANK and the formatting
     * function VARCHAR_FORMAT are DELIBERATELY EXCLUDED — they are higher-collision with
     * real user-routine names and were not observed as false-positive call nodes.
     */
    private static final Set<String> BUILTIN_FUNCTIONS = Set.of(
        // ── String / character ──────────────────────────────────────────────
        "LENGTH","LENGTH2","LENGTH4","CHARACTER_LENGTH","SUBSTR","SUBSTRING","LEFT","RIGHT",
        "LOCATE","LOCATE_IN_STRING","POSSTR","POSITION","INSTR","REPLACE","TRANSLATE","OVERLAY",
        "INSERT","REPEAT","LPAD","RPAD","LTRIM","RTRIM","TRIM","STRIP","UPPER","UCASE","LOWER",
        "LCASE","INITCAP","CONCAT","REVERSE","SOUNDEX","DIFFERENCE","ASCII","CHR","HEX","DIGITS",
        "SPACE","TO_CHAR","CHAR","GRAPHIC","VARGRAPHIC","VARCHAR","XMLCAST",
        // ── Numeric / math / cast ───────────────────────────────────────────
        "ABS","CEIL","CEILING","FLOOR","ROUND","TRUNC","TRUNCATE","MOD","POWER","POW","SQRT",
        "EXP","LN","LOG","LOG10","SIGN","RAND","ACOS","ASIN","ATAN","ATAN2","COS","COSH","COT",
        "SIN","SINH","TAN","TANH","DEGREES","RADIANS","DECFLOAT","INTEGER","INT","BIGINT",
        "SMALLINT","DECIMAL","DEC","DOUBLE","REAL","FLOAT",
        // ── Date / time ─────────────────────────────────────────────────────
        "CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP","DAY","DAYS","DAYOFWEEK",
        "DAYOFWEEK_ISO","DAYOFYEAR","DAYNAME","MONTH","MONTHNAME","YEAR","WEEK","WEEK_ISO",
        "HOUR","MINUTE","SECOND","MICROSECOND","QUARTER","MIDNIGHT_SECONDS","JULIAN_DAY",
        "LAST_DAY","NEXT_DAY","ADD_MONTHS","MONTHS_BETWEEN","TIMESTAMP_FORMAT","TO_DATE",
        "TO_TIMESTAMP","TIMESTAMPDIFF","TIMESTAMP_ISO","DATE","TIME","TIMESTAMP","EXTRACT",
        // ── Null / conditional ──────────────────────────────────────────────
        "COALESCE","VALUE","NULLIF","NVL","NVL2","IFNULL","DECODE","MAX","MIN","GREATEST","LEAST",
        // ── Aggregate ───────────────────────────────────────────────────────
        "COUNT","COUNT_BIG","SUM","AVG","STDDEV","STDDEV_SAMP","VARIANCE","VAR_SAMP",
        "CORRELATION","CORR","COVARIANCE","LISTAGG","XMLAGG","ARRAY_AGG",
        // ── OLAP / window ───────────────────────────────────────────────────
        "PERCENT_RANK","CUME_DIST","NTILE","LAG","LEAD","FIRST_VALUE","LAST_VALUE","NTH_VALUE",
        "RATIO_TO_REPORT",
        // ── Special registers / special built-ins ───────────────────────────
        "IDENTITY_VAL_LOCAL","LAST_INSERT_ID","GENERATE_UNIQUE","NEXTVAL","PREVVAL",
        "CURRENT_SCHEMA","CURRENT_USER","SESSION_USER","SYSTEM_USER","CURRENT_SERVER","HASH",
        "MAX_CARDINALITY","CARDINALITY","INTNAND","RAISE_ERROR",
        // ── Window / OLAP (added) ───────────────────────────────────────────
        "ROW_NUMBER","RANK","DENSE_RANK",
        // ── Aggregate / statistical (added) ─────────────────────────────────
        "MEDIAN","PERCENTILE_CONT","PERCENTILE_DISC","STDDEV_POP","VAR_POP","GROUPING",
        "REGR_SLOPE","REGR_INTERCEPT","REGR_COUNT","REGR_R2","REGR_AVGX","REGR_AVGY",
        // ── Numeric / bit (added) ───────────────────────────────────────────
        "WIDTH_BUCKET","BITAND","BITOR","BITXOR","BITNOT","BITANDNOT","MULTIPLY_ALT",
        "TO_NUMBER","QUANTIZE","NORMALIZE_DECFLOAT","COMPARE_DECFLOAT","TOTALORDER",
        // ── String / regexp / lob (added) ───────────────────────────────────
        "REGEXP_REPLACE","REGEXP_SUBSTR","REGEXP_INSTR","REGEXP_COUNT","REGEXP_LIKE",
        "TO_CLOB","CLOB","BLOB","DBCLOB","OVERLAY_ALT","INSTRB",
        // ── Date / time (added) ─────────────────────────────────────────────
        "TIMESTAMPADD","DAYOFMONTH","DAYS_BETWEEN","ROUND_TIMESTAMP","TRUNC_TIMESTAMP","FROM_UTC_TIMESTAMP","TO_UTC_TIMESTAMP",
        // ── XML / JSON (added) ──────────────────────────────────────────────
        "XMLELEMENT","XMLATTRIBUTES","XMLSERIALIZE","XMLPARSE","XMLQUERY","XMLNAMESPACES",
        "XMLCONCAT","XMLFOREST","XMLDOCUMENT","XMLTEXT","XMLROW","XMLGROUP",
        "JSON_VAL","JSON_TABLE","JSON_OBJECT","JSON_ARRAY","JSON_QUERY","JSON_VALUE","JSON_EXISTS",
        // ── Identity / catalog (added) ──────────────────────────────────────
        "TABLE_NAME","TABLE_SCHEMA","TYPE_NAME","TYPE_SCHEMA","DBPARTITIONNUM","HASHEDVALUE","RID","RID_BIT","GETHINT","ENCRYPT","DECRYPT");

    @Override
    public Void visitFunctionCall(Db2Parser.FunctionCallContext ctx) {
        List<Db2Parser.IdContext> ids = ctx.id();
        String schema = ids.size() > 1 ? cleanId(ids.get(0).getText()) : null;
        String name   = cleanId(ids.get(ids.size() - 1).getText());

        // Bug fix: bare `VALUES(...)` immediately following an INSERT's column list is matched
        // by the permissive `functionCall` alt inside `stmtTail` (VALUES is not a lexer keyword
        // here, so it falls through the generic `id` rule). This produced a spurious call record
        // "null.VALUES" on nearly every INSERT statement. VALUES is a reserved SQL keyword — it
        // can never legitimately be a real (unqualified) routine name — so a bare `VALUES` call
        // is unconditionally excluded, without touching schema-qualified calls.
        boolean isBareValuesKeyword = schema == null && "VALUES".equalsIgnoreCase(name);
        if (name != null && !isBareValuesKeyword
                && (schema != null || !BUILTIN_FUNCTIONS.contains(name.toUpperCase()))) {
            String qualified = (schema != null && !schema.isBlank()) ? schema + "." + name : name;
            boolean dup = result.getCalls().stream()
                    .anyMatch(c -> qualified.equalsIgnoreCase(c.qualifiedName()));
            if (!dup) {
                Db2CallInfo call = new Db2CallInfo();
                call.setName(name);
                call.setSchema(schema);
                call.setLine(ctx.getStart().getLine());
                result.getCalls().add(call);
            }
        }
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL: PREPARE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitPrepareStatement(Db2Parser.PrepareStatementContext ctx) {
        Db2DynamicSqlInfo d = new Db2DynamicSqlInfo();
        d.setKind(Db2DynamicSqlInfo.Kind.PREPARE);

        // PREPARE id FROM (id | SQ_STRING)
        List<Db2Parser.IdContext> ids = ctx.id();
        if (!ids.isEmpty()) {
            d.setStmtName(cleanId(ids.get(0).getText()));
        }
        String resolvedSql = null;
        if (ids.size() > 1) {
            // Second id is the source variable
            String varName = cleanId(ids.get(1).getText());
            d.setSqlSource(varName);
            resolvedSql = varLiteralSql.get(varName.toUpperCase());
        } else if (ctx.SQ_STRING() != null) {
            String literal = ctx.SQ_STRING().getText();
            d.setSqlSource(literal);
            resolvedSql = literal;
        }

        // Remember the resolved SQL text (if any) by statement name so a later
        // EXECUTE <stmtName> can carry it instead of leaving sqlSource null.
        if (d.getStmtName() != null && resolvedSql != null) {
            preparedStmtSql.put(d.getStmtName().toUpperCase(), resolvedSql);
        }

        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL: EXECUTE IMMEDIATE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitExecuteImmStmt(Db2Parser.ExecuteImmStmtContext ctx) {
        Db2DynamicSqlInfo d = new Db2DynamicSqlInfo();
        d.setKind(Db2DynamicSqlInfo.Kind.EXECUTE_IMMEDIATE);

        if (ctx.id() != null) {
            String varName = cleanId(ctx.id().getText());
            d.setSqlSource(varName);
            // Bug 8 (DB2 portion): resolve the variable the same way DECLARE CURSOR FOR
            // <preparedStmtName> does — a fully-literal SET assignment resolves to a table;
            // a variable-interrupted one is flagged via unresolvedVarName instead of guessed.
            resolveDynamicSqlSource(varName, true, ctx.getStart().getLine(), d);
        } else if (ctx.SQ_STRING() != null) {
            d.setSqlSource(ctx.SQ_STRING().getText());
            // Bug 9 (§DML-audit, lower priority, literal-only scope): a fully-literal
            // EXECUTE IMMEDIATE '<sql>' (ctx.id() == null means no variable was used) has its
            // table name known at parse time — extract it, modeled on Oracle's DYNAMIC bucket.
            extractLiteralDynamicSqlTable(ctx.SQ_STRING().getText(), ctx.getStart().getLine());
        }

        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    private static final java.util.regex.Pattern DYN_SQL_TABLE_PATTERN = java.util.regex.Pattern.compile(
            "(?i)(?:FROM|INTO|UPDATE|INSERT\\s+INTO|DELETE\\s+FROM|JOIN)\\s+"
                    + "([A-Za-z_][A-Za-z0-9_$#]*(?:\\.[A-Za-z_][A-Za-z0-9_$#]*)?)");

    // ─────────────────────────────────────────────────────────────────────────
    // Guards + observability for the grammar-first recursive dynamic-SQL parse.
    // DYN_SQL_TABLE_PATTERN survives only as the recovery path for text the grammar cannot
    // attribute a table to; every such use is logged once and counted.
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

    /** Extracts a table name from a fully-literal dynamic-SQL string (no host-variable
     *  indirection) and records it as a {@code DYNAMIC} table op. */
    private void extractLiteralDynamicSqlTable(String sqLiteral, int line) {
        if (sqLiteral == null) return;
        String t = sqLiteral.trim();
        if (t.length() < 2 || t.charAt(0) != '\'' || t.charAt(t.length() - 1) != '\'') return;
        extractDynamicSqlTable(t.substring(1, t.length() - 1), line);
    }

    /**
     * Extracts a table name from already-unquoted SQL text and, if found, records it as a
     * {@code DYNAMIC} table op. Shared by the literal EXECUTE IMMEDIATE path above and
     * {@link #resolveDynamicSqlSource} (used by EXECUTE IMMEDIATE variable resolution and
     * DECLARE CURSOR FOR &lt;preparedStmtName&gt;).
     *
     * @return true if a table name was found (and recorded), false otherwise.
     */
    private boolean extractDynamicSqlTable(String innerSql, int line) {
        if (innerSql == null) return false;
        // Grammar first: the recovered string is real SQL, so re-parse it with the same DB2
        // grammar and read the table off the nested parse tree. Only a clean, error-free parse
        // is trusted. Falls back to the legacy text scan for fragments the grammar cannot parse
        // (concatenation leftovers, `?` parameter markers, vendor syntax outside this grammar).
        List<String> tableTexts = nestedParseDynamicSqlTables(innerSql);
        if (tableTexts.isEmpty()) {
            // Recovery path: text the grammar cannot attribute a table to. Logged once, counted,
            // so the regex retirement rate of the migration is observable.
            long fallbackNo = DYN_SQL_REGEX_FALLBACKS.incrementAndGet();
            if (DYN_FALLBACK_LOGGED.compareAndSet(false, true)) {
                System.err.println("[Db2AnalysisVisitor] dynamic-SQL recursive parse produced no "
                        + "table target; using legacy regex fallback (#" + fallbackNo + ")");
            }
            // Legacy text scan: single-table only by construction (the regex has one capture
            // group and no join alternatives), so this path is unchanged.
            java.util.regex.Matcher m = DYN_SQL_TABLE_PATTERN.matcher(innerSql);
            if (m.find()) tableTexts = List.of(m.group(1));
        }
        if (tableTexts.isEmpty()) return false;
        for (String tableText : tableTexts) {
            Db2TableOpInfo op = new Db2TableOpInfo();
            op.setDmlType(Db2TableOpInfo.DmlType.DYNAMIC);
            op.setTableName(foldTableId(tableText));
            op.setLine(line);
            result.getTableOps().add(op);
        }
        return true;
    }

    /**
     * Re-parses a recovered dynamic-SQL string with the DB2 grammar and returns the raw text of
     * EVERY table reference of the statement, in textual order, or an empty list if the string
     * does not parse cleanly as a single statement.
     *
     * <p>Previously returned only the FIRST table reference, so a dynamic join
     * ({@code 'SELECT … FROM a JOIN b ON …'}) recorded only its driving table and every joined
     * table was silently dropped. The nested parse already had the whole tree — only the reader
     * was narrow. Names are de-duplicated (case-insensitively, on the folded form) so a table
     * mentioned twice, or a self-join, does not double-report.
     */
    private List<String> nestedParseDynamicSqlTables(String innerSql) {
        if (innerSql == null || innerSql.isBlank()) return List.of();
        if (innerSql.length() > DYN_PARSE_MAX_CHARS) return List.of();
        int depth = DYN_PARSE_DEPTH.get();
        if (depth >= DYN_PARSE_MAX_DEPTH) return List.of();
        DYN_PARSE_DEPTH.set(depth + 1);
        try {
            Db2ParseErrorListener errors = new Db2ParseErrorListener();
            Db2Lexer lexer = new Db2Lexer(org.antlr.v4.runtime.CharStreams.fromString(innerSql));
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);
            Db2Parser parser = new Db2Parser(new org.antlr.v4.runtime.CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(errors);
            Db2Parser.StatementContext stmt = parser.statement();
            if (errors.hasErrors()) return List.of();
            // Only DML carries a table to attribute; anything else (including the otherStatement
            // catch-all, which would happily swallow arbitrary text) is rejected.
            // mergeStatement is included since MERGE gained a real grammar rule: without it a
            // dynamic MERGE fell through to the DYN_SQL_TABLE_PATTERN text scan, which has no
            // MERGE alternative and matched the `UPDATE SET …` of the WHEN MATCHED branch
            // instead — recording a bogus table literally named "SET" for
            // `MERGE tgt USING … WHEN MATCHED THEN UPDATE SET …` (no INTO keyword). The nested
            // parse reads the real MERGE target off the tree, consistent with how a non-dynamic
            // MERGE is handled by visitMergeStatement.
            if (stmt.insertStatement() == null && stmt.updateStatement() == null
                    && stmt.deleteStatement() == null && stmt.selectStatement() == null
                    && stmt.mergeStatement() == null) {
                return List.of();
            }
            List<String> out = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            collectTableRefTexts(stmt, out, seen);
            return out;
        } catch (Exception e) {
            return List.of();   // never let a nested-parse failure break the outer analysis
        } finally {
            DYN_PARSE_DEPTH.set(depth);
        }
    }

    /** Every {@code tableRef} in traversal (= textual) order within a subtree, raw text, deduped. */
    private void collectTableRefTexts(org.antlr.v4.runtime.tree.ParseTree node,
                                      List<String> out, Set<String> seen) {
        if (node instanceof Db2Parser.TableRefContext tr) {
            String raw = tr.getText();
            if (seen.add(foldTableId(raw))) out.add(raw);
            return;   // a tableRef has no nested tableRef children
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectTableRefTexts(node.getChild(i), out, seen);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic SQL: EXECUTE stmt [USING ...]
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitExecuteStatement(Db2Parser.ExecuteStatementContext ctx) {
        Db2DynamicSqlInfo d = new Db2DynamicSqlInfo();
        d.setKind(Db2DynamicSqlInfo.Kind.EXECUTE);

        if (ctx.id() != null) {
            String stmtName = cleanId(ctx.id().getText());
            d.setStmtName(stmtName);
            // Bug fix: carry the SQL text already known from the matching PREPARE, so
            // EXECUTE <stmtName> doesn't leave sqlSource null when it is actually resolvable.
            String resolved = preparedStmtSql.get(stmtName.toUpperCase());
            if (resolved != null) {
                d.setSqlSource(resolved);
                extractDynamicSqlTable(resolved, ctx.getStart().getLine());
            }
        }

        d.setLine(ctx.getStart().getLine());
        result.getDynamicSql().add(d);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction-control statements → StatementInfo (COMMIT / ROLLBACK / SAVEPOINT)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitTxnCommit(Db2Parser.TxnCommitContext ctx) {
        result.getStatements().add(new Db2StatementInfo("COMMIT", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnRollback(Db2Parser.TxnRollbackContext ctx) {
        result.getStatements().add(new Db2StatementInfo("ROLLBACK", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnRollbackTo(Db2Parser.TxnRollbackToContext ctx) {
        Db2StatementInfo si = new Db2StatementInfo("ROLLBACK", ctx.getStart().getLine());
        si.setSavepointName(cleanId(ctx.id().getText()));
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitTxnSavepoint(Db2Parser.TxnSavepointContext ctx) {
        Db2StatementInfo si = new Db2StatementInfo("SAVEPOINT", ctx.getStart().getLine());
        si.setSavepointName(cleanId(ctx.id().getText()));
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Control-flow statements → StatementInfo (IF / CASE / loops)
    // Each override records ONE statement record per occurrence, then descends
    // (visitChildren) so nested statements / table ops / calls are still captured.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitIfStatement(Db2Parser.IfStatementContext ctx) {
        result.getStatements().add(new Db2StatementInfo("IF", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitWhileStatement(Db2Parser.WhileStatementContext ctx) {
        result.getStatements().add(new Db2StatementInfo("WHILE_LOOP", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitForStatement(Db2Parser.ForStatementContext ctx) {
        result.getStatements().add(new Db2StatementInfo("FOR_LOOP", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitLoopStatement(Db2Parser.LoopStatementContext ctx) {
        result.getStatements().add(new Db2StatementInfo("LOOP", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitRepeatStatement(Db2Parser.RepeatStatementContext ctx) {
        result.getStatements().add(new Db2StatementInfo("REPEAT", ctx.getStart().getLine()));
        return visitChildren(ctx);
    }

    @Override
    public Void visitLeaveStatement(Db2Parser.LeaveStatementContext ctx) {
        recordLabeledJump("LEAVE", ctx.id().getText(), ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    @Override
    public Void visitIterateStatement(Db2Parser.IterateStatementContext ctx) {
        recordLabeledJump("ITERATE", ctx.id().getText(), ctx.getStart().getLine());
        return visitChildren(ctx);
    }

    /**
     * Records a LEAVE/ITERATE with a BARE statement type and the loop label in its own
     * {@code label} field. The label must never be concatenated into the type string:
     * a routine with N uniquely-labeled loops would then produce N one-off statement-type
     * keys (observed: 554 distinct "LEAVE:&lt;label&gt;" types out of 567 statements),
     * which pollutes downstream statement-type breakdowns and schema-safety guards.
     */
    private void recordLabeledJump(String type, String rawLabel, int line) {
        Db2StatementInfo si = new Db2StatementInfo(type, line);
        String label = cleanId(rawLabel);
        if (label != null && !label.isBlank()) si.setLabel(label);
        result.getStatements().add(si);
    }

    /**
     * CASE (expression or statement form) has no dedicated grammar rule — it appears
     * as {@code CASE … END} inside exprAtom / stmtToken / selectRestToken. Each such
     * context carries exactly one CASE token, and a given CASE keyword lives in exactly
     * one of those rules, so counting per-context yields one record per occurrence with
     * no double-counting (nested CASEs are distinct child contexts).
     */
    @Override
    public Void visitExprAtom(Db2Parser.ExprAtomContext ctx) {
        if (ctx.CASE() != null) {
            result.getStatements().add(new Db2StatementInfo("CASE", ctx.getStart().getLine()));
        }
        if (ctx.FOR() != null) {
            List<Db2Parser.IdContext> ids = ctx.id();
            if (ctx.NEXT() != null && ctx.VALUE() != null) {
                // NEXT VALUE FOR (id DOT)? id  — always a NEXTVAL reference
                recordSequenceRef("NEXTVAL", ids, ctx.getStart().getLine());
            } else if (!ids.isEmpty()) {
                // id FOR (id DOT)? id  — Oracle-compat NEXTVAL/PREVVAL FOR seq;
                // the first id is the operation keyword (not a real identifier here).
                String op = cleanId(ids.get(0).getText()).toUpperCase();
                if ("NEXTVAL".equals(op) || "PREVVAL".equals(op)) {
                    recordSequenceRef(op, ids.subList(1, ids.size()), ctx.getStart().getLine());
                }
            }
        }
        return visitChildren(ctx);
    }

    /** Records a NEXTVAL/PREVVAL sequence reference from a trailing (schema.)?name id list. */
    private void recordSequenceRef(String operation, List<Db2Parser.IdContext> seqIds, int line) {
        if (seqIds.isEmpty()) return;
        Db2SequenceInfo s = new Db2SequenceInfo();
        s.setOperation(operation);
        s.setSchema(seqIds.size() > 1 ? cleanId(seqIds.get(0).getText()) : null);
        s.setSequenceName(cleanId(seqIds.get(seqIds.size() - 1).getText()));
        s.setLine(line);
        result.getSequences().add(s);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE (CONTINUE|EXIT|UNDO) HANDLER FOR <conditions> <action>
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareHandlerStmt(Db2Parser.DeclareHandlerStmtContext ctx) {
        Db2ExceptionHandlerInfo h = new Db2ExceptionHandlerInfo();
        if (ctx.EXIT() != null) {
            h.setKind(Db2ExceptionHandlerInfo.Kind.EXIT);
        } else if (ctx.UNDO() != null) {
            h.setKind(Db2ExceptionHandlerInfo.Kind.UNDO);
        } else {
            h.setKind(Db2ExceptionHandlerInfo.Kind.CONTINUE);
        }
        List<String> conditions = new java.util.ArrayList<>();
        for (Db2Parser.HandlerCondContext hc : ctx.handlerCondList().handlerCond()) {
            conditions.add(handlerCondText(hc));
        }
        h.setConditions(conditions);
        h.setLine(ctx.getStart().getLine());
        result.getExceptionHandlers().add(h);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DECLARE id CONDITION FOR SQLSTATE '...'  — bind a custom condition name to a SQLSTATE.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitDeclareConditionStmt(Db2Parser.DeclareConditionStmtContext ctx) {
        Db2StatementInfo si = new Db2StatementInfo("DECLARE_CONDITION", ctx.getStart().getLine());
        String condName = cleanId(ctx.id().getText());
        String sqlstate = ctx.SQ_STRING() != null ? ctx.SQ_STRING().getText() : "";
        si.setSavepointName(null);
        si.setConditionName(condName);
        si.setSqlstate(sqlstate);
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SIGNAL SQLSTATE '...' [SET ...] ; / SIGNAL <condition-name> [SET ...] ;
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitSignalStmt(Db2Parser.SignalStmtContext ctx) {
        Db2StatementInfo si = new Db2StatementInfo("SIGNAL", ctx.getStart().getLine());
        if (ctx.SQLSTATE() != null && ctx.SQ_STRING() != null) {
            si.setSqlstate(ctx.SQ_STRING().getText());
        } else if (ctx.id() != null && !ctx.id().isEmpty()) {
            si.setConditionName(cleanId(ctx.id(0).getText()));
        }
        si.setMessageText(extractMessageText(ctx.expr()));
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RESIGNAL ; (bare re-raise) / RESIGNAL SQLSTATE '...' [SET ...] ; / RESIGNAL <name> [SET ...] ;
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Void visitResignalStmt(Db2Parser.ResignalStmtContext ctx) {
        boolean bare = ctx.SQLSTATE() == null && (ctx.id() == null || ctx.id().isEmpty());
        Db2StatementInfo si = new Db2StatementInfo(bare ? "RESIGNAL_BARE" : "RESIGNAL_WITH_ARGS", ctx.getStart().getLine());
        if (ctx.SQLSTATE() != null && ctx.SQ_STRING() != null) {
            si.setSqlstate(ctx.SQ_STRING().getText());
        } else if (ctx.id() != null && !ctx.id().isEmpty()) {
            si.setConditionName(cleanId(ctx.id(0).getText()));
        }
        si.setMessageText(extractMessageText(ctx.expr()));
        result.getStatements().add(si);
        return visitChildren(ctx);
    }

    /** SET MESSAGE_TEXT = 'x' clauses attach a list of `id EQ expr`; pull the first as the message text. */
    private String extractMessageText(List<Db2Parser.ExprContext> exprs) {
        if (exprs == null || exprs.isEmpty()) return null;
        return exprs.get(0).getText();
    }

    private String handlerCondText(Db2Parser.HandlerCondContext hc) {
        if (hc.SQLEXCEPTION() != null) return "SQLEXCEPTION";
        if (hc.SQLWARNING() != null) return "SQLWARNING";
        if (hc.SQLSTATE() != null) {
            String val = hc.SQ_STRING() != null ? hc.SQ_STRING().getText() : "";
            return ("SQLSTATE " + val).trim();
        }
        if (hc.NOT() != null && hc.id() != null) {
            return "NOT " + cleanId(hc.id().getText());
        }
        if (hc.id() != null) {
            return cleanId(hc.id().getText());
        }
        return hc.getText();
    }

    @Override
    public Void visitStmtToken(Db2Parser.StmtTokenContext ctx) {
        if (ctx.CASE() != null) {
            result.getStatements().add(new Db2StatementInfo("CASE", ctx.getStart().getLine()));
        }
        // Mirrors visitExprAtom's NEXT VALUE FOR / id FOR sequence-reference handling: stmtToken
        // (the permissive rule that consumes an INSERT ... VALUES(...) tuple, among other
        // clauses) gained the same two sequence alternatives so NEXT VALUE FOR/NEXTVAL FOR/
        // PREVVAL FOR calls nested inside it are still recorded rather than silently dropped.
        if (ctx.FOR() != null) {
            List<Db2Parser.IdContext> ids = ctx.id();
            if (ctx.NEXT() != null && ctx.VALUE() != null) {
                recordSequenceRef("NEXTVAL", ids, ctx.getStart().getLine());
            } else if (!ids.isEmpty()) {
                String op = cleanId(ids.get(0).getText()).toUpperCase();
                if ("NEXTVAL".equals(op) || "PREVVAL".equals(op)) {
                    recordSequenceRef(op, ids.subList(1, ids.size()), ctx.getStart().getLine());
                }
            }
        }
        return visitChildren(ctx);
    }

    /** A CASE expression in a MERGE tail is reported the same way one in a SELECT tail is. */
    @Override
    public Void visitMergeRestToken(Db2Parser.MergeRestTokenContext ctx) {
        if (ctx.CASE() != null) {
            result.getStatements().add(new Db2StatementInfo("CASE", ctx.getStart().getLine()));
        }
        return visitChildren(ctx);
    }

    @Override
    public Void visitSelectRestToken(Db2Parser.SelectRestTokenContext ctx) {
        if (ctx.CASE() != null) {
            result.getStatements().add(new Db2StatementInfo("CASE", ctx.getStart().getLine()));
        }
        return visitChildren(ctx);
    }

    /**
     * MERGE has no lexer keyword — it tokenizes as an identifier and is parsed by the
     * otherStatement catch-all. Detect it by the leading token text (visitor-only).
     */
    // Bug (temp-table audit): DECLARE GLOBAL TEMPORARY TABLE previously produced no entity of any
    // kind — no dedicated visitor override existed, and the declaration itself (as opposed to a
    // later JOIN/INSERT referencing the temp table) vanished silently. Record it as both a
    // statement entry and a CREATE table op so it is at least visible in the output; the model has
    // no isTemporary flag, so schema/table are captured as best-effort.
    @Override
    public Void visitDeclareGlobalTempTable(Db2Parser.DeclareGlobalTempTableContext ctx) {
        result.getStatements().add(new Db2StatementInfo("DECLARE_GLOBAL_TEMPORARY_TABLE", ctx.getStart().getLine()));
        List<Db2Parser.IdContext> ids = ctx.id();
        // ids = [GLOBAL, TEMPORARY, TABLE, name] or [GLOBAL, TEMPORARY, TABLE, schema, name]
        if (ids.size() >= 4) {
            Db2TableOpInfo op = new Db2TableOpInfo();
            if (ids.size() >= 5) {
                op.setSchemaName(foldTableId(ids.get(3).getText()));
                op.setTableName(foldTableId(ids.get(4).getText()));
            } else {
                op.setTableName(foldTableId(ids.get(3).getText()));
            }
            op.setLine(ctx.getStart().getLine());
            op.setDmlType(Db2TableOpInfo.DmlType.CREATE);
            // TEMP as the fourth table kind (grammar-first): DECLARE GLOBAL TEMPORARY TABLE is a
            // session-scoped temporary table; the ON COMMIT PRESERVE/DELETE ROWS clause of the same
            // parsed statement gives its lifetime (Db2's default is DELETE ROWS when omitted).
            op.setTableKind("TEMP");
            op.setTempScope(db2TempScope(ctx.getText()));
            result.getTableOps().add(op);
        }
        return null;
    }

    /**
     * The Db2 declared-temp-table lifetime spelled out by an {@code ON COMMIT ... ROWS} clause of
     * an already-parsed DECLARE GLOBAL TEMPORARY TABLE statement. Db2's default when the clause is
     * omitted is {@code DELETE ROWS}.
     */
    private static String db2TempScope(String ddl) {
        if (ddl == null) return "ON COMMIT DELETE ROWS";
        // ctx.getText() concatenates tokens without whitespace, so strip all whitespace before
        // matching the (spaced) clause.
        String u = ddl.replaceAll("\\s+", "").toUpperCase(java.util.Locale.ROOT);
        if (u.contains("ONCOMMITPRESERVEROWS")) return "ON COMMIT PRESERVE ROWS";
        return "ON COMMIT DELETE ROWS";
    }

    @Override
    public Void visitOtherStatement(Db2Parser.OtherStatementContext ctx) {
        if (ctx.getStart() != null && "MERGE".equalsIgnoreCase(ctx.getStart().getText())) {
            // Degenerate safety net only: a MERGE normally matches the dedicated mergeStatement
            // rule (see visitMergeStatement) and never reaches this catch-all. If some malformed
            // shape ever does land here, still record the statement — but no table ops, since
            // without a parse tree there is nothing structural to read them from (the old
            // regex-over-raw-text extraction that used to live here has been removed).
            result.getStatements().add(new Db2StatementInfo("MERGE", ctx.getStart().getLine()));
        }
        return visitChildren(ctx);
    }

    // MERGE (Bug 4, §DML-audit): target (after INTO) is a write, source (after USING) is a
    // read — modeled on Oracle's/SQL Server's MERGE handling. Both sides now come from the
    // dedicated mergeStatement grammar rule instead of the previous regex scan of the raw
    // statement text (which also needed a hand-rolled comment-tolerance pattern to survive a
    // comment between MERGE and INTO; the lexer skips comments, so the grammar gets that free).
    /**
     * MERGE via its own grammar rule (preferred path): both sides come straight from the parse
     * tree — {@code tableRef} for the target, {@code mergeSourceTable} for a plain-table source.
     * A parenthesized source ((VALUES …) / a subquery) yields no source table op, exactly as the
     * previous text scan did; a subquery source's inner SELECT is still walked by visitChildren.
     */
    @Override
    public Void visitMergeStatement(Db2Parser.MergeStatementContext ctx) {
        int line = ctx.getStart().getLine();
        result.getStatements().add(new Db2StatementInfo("MERGE", line));

        Db2TableOpInfo target = new Db2TableOpInfo();
        target.setSchemaName(extractTableSchema(ctx.tableRef()));
        target.setTableName(extractTableName(ctx.tableRef()));
        Db2Parser.IdContext targetAlias = mergeAlias(ctx, true);
        if (targetAlias != null) target.setAlias(cleanId(targetAlias.getText()));
        target.setLine(line);
        target.setDmlType(Db2TableOpInfo.DmlType.MERGE);
        result.getTableOps().add(target);

        if (ctx.mergeSource() instanceof Db2Parser.MergeSourceTableContext st) {
            Db2TableOpInfo source = new Db2TableOpInfo();
            source.setSchemaName(extractTableSchema(st.tableRef()));
            source.setTableName(extractTableName(st.tableRef()));
            Db2Parser.IdContext sourceAlias = mergeAlias(ctx, false);
            if (sourceAlias != null) source.setAlias(cleanId(sourceAlias.getText()));
            source.setLine(line);
            source.setDmlType(Db2TableOpInfo.DmlType.SELECT);
            result.getTableOps().add(source);
        }
        return visitChildren(ctx);
    }

    /**
     * mergeStatement has two optional direct {@code id} aliases (target and source). Which is
     * which is decided by position relative to the USING keyword, not by list index — either one
     * may be absent.
     */
    private Db2Parser.IdContext mergeAlias(Db2Parser.MergeStatementContext ctx, boolean beforeUsing) {
        if (ctx.USING() == null) return null;
        int usingIdx = ctx.USING().getSymbol().getTokenIndex();
        for (Db2Parser.IdContext id : ctx.id()) {
            boolean before = id.getStart().getTokenIndex() < usingIdx;
            if (before == beforeUsing) return id;
        }
        return null;
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

    // DB2, like Oracle, folds an unquoted identifier to upper-case per its default
    // collation; a delimited (double-quoted) identifier preserves its exact case
    // verbatim. cleanId() already strips the surrounding quotes for the quoted
    // case (and leaves an unquoted identifier's case untouched) — foldTableId()
    // adds the upper-casing for the unquoted case, applied specifically at the
    // table/schema-name capture sites (Db2TableOpInfo), matching the case-fold
    // rule DB2 actually applies to those names. It intentionally does NOT touch
    // routine/parameter/cursor/variable names, which existing behavior/tests
    // preserve verbatim regardless of quoting.
    private String foldTableId(String rawText) {
        if (rawText == null) return null;
        boolean wasQuoted = rawText.startsWith("\"") && rawText.endsWith("\"") && rawText.length() > 1;
        String cleaned = cleanId(rawText);
        return wasQuoted ? cleaned : (cleaned == null ? null : cleaned.toUpperCase());
    }

    private String extractTableName(Db2Parser.TableRefContext ctx) {
        if (ctx == null) return null;
        List<Db2Parser.IdContext> ids = ctx.id();
        if (ids.isEmpty()) return null;
        // Last id is the table name
        return foldTableId(ids.get(ids.size() - 1).getText());
    }

    private String extractTableSchema(Db2Parser.TableRefContext ctx) {
        if (ctx == null) return null;
        List<Db2Parser.IdContext> ids = ctx.id();
        // Bug (3-part catalog audit): for a 3-part catalog.schema.table reference, the schema is
        // the second-to-last id (not the first, which would be the catalog) — take ids.get(0) only
        // in the 2-part case where it IS the schema. Any earlier catalog segment is intentionally
        // dropped rather than mis-attributed as schema/table.
        if (ids.size() > 1) {
            return foldTableId(ids.get(ids.size() - 2).getText());
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
     * Delegates to {@link com.plsql.dialect.StatementTextCapture}.
     *
     * <p>This method used to be a byte-identical copy of the DB2/PostgreSQL sibling, and
     * both copies carried the same two defects — running into a loop body, and closing a
     * parenthesis never opened — which then had to be found and fixed twice. Sharing it is
     * the point: the next fix lands in one place.
     */
    private String originalText(org.antlr.v4.runtime.ParserRuleContext ctx) {
        return com.plsql.dialect.StatementTextCapture.capture(
                ctx, com.plsql.dialect.StatementTextCapture.DB2_BLOCK_KEYWORDS);
    }
}
