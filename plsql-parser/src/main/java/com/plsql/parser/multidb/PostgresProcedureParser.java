package com.plsql.parser.multidb;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import com.plsql.parser.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter — delegates all grammar work to the PostgresAnalysisEngine (postgres-parser module).
 * Owns only the conversion from PostgresRoutineInfo → shared ParseResult.
 */
public class PostgresProcedureParser implements StoredProcedureParser {

    /**
     * Column-level detail is derived here, by the one shared enricher.
     *
     * <p>Each dialect's own job stays unchanged: record the table operation and the statement's
     * SQL. Working out which columns are written and where the values come from is the same
     * problem in every dialect, so it is solved once in {@code TableOperationEnricher} rather
     * than reimplemented per grammar. Applied inside the parser rather than by decorating the
     * factory, because the factory's contract — returning this concrete type — is asserted by
     * {@code SourceFetchStrategyMockTest}.
     */
    private static final com.plsql.parser.service.TableOperationEnricher ENRICHER =
            new com.plsql.parser.service.TableOperationEnricher();

    private static final PostgresAnalysisEngine ENGINE = new PostgresAnalysisEngine();

    @Override
    public ParseResult parse(String source, String objectName, String schema) {
        long start = System.currentTimeMillis();
        PostgresRoutineInfo info = ENGINE.parse(source, objectName, schema);

        ParseResult result = new ParseResult();
        result.setFileName(info.getName() + ".sql");
        result.getErrors().addAll(info.getErrors());

        ParsedObject obj = new ParsedObject();
        obj.setName(info.getName());
        obj.setSchema(info.getSchema());
        obj.setType(mapType(info.getType()));
        obj.setLineStart(info.getLineStart());
        obj.setLineEnd(info.getLineEnd() > 0 ? info.getLineEnd() : countLines(source));
        obj.setParameters(toParams(info.getParams()));
        obj.setGlobalVariables(toVars(info.getVariables(), info.getCursors()));
        List<CallInfo> calls = toCalls(info.getCalls());
        // Sequence usages (nextval/currval/setval/lastval) → synthetic BUILTIN calls so the shared
        // sequence report (Oracle pipeline) picks them up, mirroring SqlServerProcedureParser's
        // "NEXT VALUE FOR" wiring. Typed BUILTIN so the crawler never tries to follow them as routines.
        for (PostgresSequenceInfo s : info.getSequences()) {
            CallInfo seqCall = new CallInfo();
            seqCall.setName(s.getOperation() != null ? s.getOperation().name() : "NEXTVAL");
            seqCall.setPackageName(s.getSequenceName());
            seqCall.setType("BUILTIN");
            seqCall.setLine(s.getLine());
            calls.add(seqCall);
        }
        obj.setCalls(calls);
        obj.setTableOperations(toTableOps(info.getTableOps()));
        obj.setCursors(toCursors(info.getCursors()));
        obj.setDynamicSql(toDynamicSql(info.getDynamicSql()));
        obj.setStatements(toStatements(info.getStatements()));
        obj.setExceptionHandlers(toExceptionHandlers(info.getExceptionHandlers()));
        obj.setForeignKeys(toForeignKeys(info.getForeignKeys()));
        // Tasks #26/#27: result-set marker + TVF/SETOF return shape. PostgresProcedureParser owns
        // the only conversion into the shared ParsedObject that the artifact writer sees, so a
        // field left here would be invisible downstream.
        obj.setReturnsResultSet(info.isReturnsResultSet());
        obj.setResultSets(info.getResultSets());
        obj.setReturnType(info.getReturnType());
        if (info.getReturnsColumns() != null && !info.getReturnsColumns().isEmpty()) {
            obj.setReturnsColumns(new ArrayList<>(info.getReturnsColumns()));
        }

        result.getObjects().add(obj);
        result.setParseTimeMs(System.currentTimeMillis() - start);
        if (result.getObjects() != null) ENRICHER.enrich(result.getObjects());
        return result;
    }

    // ── Converters ────────────────────────────────────────────────────────────

    /**
     * Declared FOREIGN KEY relationships, carried across the bridge.
     *
     * <p>Without this the dialect model's structured foreign keys end at the parser: this
     * adapter is the only conversion into the shared {@link ParsedObject} that the artifact
     * writer — and so Mongo ingest and the UI — ever sees. The lists are copied rather than
     * shared so that mutating the shared model cannot reach back into the parse result.
     */
    private List<ForeignKeyInfo> toForeignKeys(List<PostgresForeignKeyInfo> src) {
        List<ForeignKeyInfo> out = new ArrayList<>();
        if (src == null) return out;
        for (PostgresForeignKeyInfo fk : src) {
            ForeignKeyInfo fi = new ForeignKeyInfo();
            fi.setConstraintName(fk.getConstraintName());
            fi.setTable(fk.getTable());
            fi.setSchema(fk.getSchema());
            fi.setReferencedTable(fk.getReferencedTable());
            fi.setReferencedSchema(fk.getReferencedSchema());
            fi.setColumns(new ArrayList<>(fk.getColumns()));
            fi.setReferencedColumns(new ArrayList<>(fk.getReferencedColumns()));
            fi.setLine(fk.getLine());
            out.add(fi);
        }
        return out;
    }

    private List<ParameterInfo> toParams(List<PostgresParamInfo> src) {
        List<ParameterInfo> out = new ArrayList<>();
        for (PostgresParamInfo p : src) {
            ParameterInfo pi = new ParameterInfo();
            pi.setName(p.getName());
            pi.setDataType(p.getDataType());
            pi.setDirection(p.getDirection().name());
            out.add(pi);
        }
        return out;
    }

    private List<VariableInfo> toVars(List<PostgresVariableInfo> vars, List<PostgresCursorInfo> cursors) {
        List<VariableInfo> out = new ArrayList<>();
        for (PostgresVariableInfo v : vars) {
            VariableInfo vi = new VariableInfo();
            vi.setName(v.getName());
            vi.setDataType(v.getDataType());
            vi.setDefaultValue(v.getDefaultValue());
            vi.setLine(v.getLine());
            out.add(vi);
        }
        for (PostgresCursorInfo c : cursors) {
            VariableInfo vi = new VariableInfo();
            vi.setName(c.getName());
            vi.setDataType("CURSOR");
            vi.setLine(c.getLine());
            out.add(vi);
        }
        return out;
    }

    private List<CallInfo> toCalls(List<PostgresCallInfo> src) {
        List<CallInfo> out = new ArrayList<>();
        for (PostgresCallInfo c : src) {
            CallInfo ci = new CallInfo();
            ci.setName(c.getName());
            ci.setSchema(c.getSchema());
            ci.setType("EXTERNAL");
            ci.setLine(c.getLine());
            out.add(ci);
        }
        return out;
    }

    private List<TableOperationInfo> toTableOps(List<PostgresTableOpInfo> src) {
        List<TableOperationInfo> out = new ArrayList<>();
        for (PostgresTableOpInfo op : src) {
            TableOperationInfo ti = new TableOperationInfo();
            ti.setOperation(op.getDmlType().name());
            ti.setTableName(op.getTableName());
            ti.setSchema(op.getSchemaName());
            ti.setAlias(op.getAlias());
            ti.setLine(op.getLine());
            // Carried through for the shared TableOperationEnricher.
            ti.setSqlText(op.getSqlText());
            // TEMP classification set by the CREATE TEMP|TEMPORARY TABLE visitor.
            ti.setTableKind(op.getTableKind());
            ti.setTempScope(op.getTempScope());
            if (op.getReturningColumns() != null && !op.getReturningColumns().isEmpty()) {
                ti.setReturningColumns(new ArrayList<>(op.getReturningColumns()));
            }
            if (op.getJoins() != null && !op.getJoins().isEmpty()) {
                List<JoinInfo> joins = new ArrayList<>();
                for (PostgresJoinInfo j : op.getJoins()) {
                    JoinInfo ji = new JoinInfo();
                    ji.setJoinType(j.getJoinType());
                    ji.setJoinedTable(j.getJoinedTable());
                    ji.setJoinedTableAlias(j.getJoinedTableAlias());
                    ji.setLeftTable(j.getLeftTable());
                    ji.setLeftTableAlias(j.getLeftTableAlias());
                    ji.setCondition(j.getCondition());
                    ji.setLine(j.getLine());
                    ji.setQueryId(j.getQueryId());
                    joins.add(ji);
                }
                ti.setJoins(joins);
            }
            out.add(ti);
        }
        return out;
    }

    private List<CursorInfo> toCursors(List<PostgresCursorInfo> src) {
        List<CursorInfo> out = new ArrayList<>();
        for (PostgresCursorInfo c : src) {
            CursorInfo ci = new CursorInfo();
            ci.setName(c.getName());
            ci.setQuery(c.getSelectText());
            ci.setLine(c.getLine());
            for (PostgresCursorInfo.Op op : c.getOperations()) {
                ci.getStmtOperations().add(new CursorInfo.StmtOp(op.getKind(), op.getLine()));
            }
            out.add(ci);
        }
        return out;
    }

    private List<DynamicSqlInfo> toDynamicSql(List<PostgresDynamicSqlInfo> src) {
        List<DynamicSqlInfo> out = new ArrayList<>();
        for (PostgresDynamicSqlInfo d : src) {
            DynamicSqlInfo di = new DynamicSqlInfo();
            di.setType(d.getKind() != null ? d.getKind().name() : "DYNAMIC");
            di.setSqlExpression(d.getSqlSource());
            di.setLine(d.getLine());
            if (d.getUsingExprs() != null && !d.getUsingExprs().isEmpty()) {
                di.getUsingVariables().addAll(d.getUsingExprs());
            }
            out.add(di);
        }
        return out;
    }

    private List<StatementInfo> toStatements(List<PostgresStatementInfo> src) {
        List<StatementInfo> out = new ArrayList<>();
        for (PostgresStatementInfo s : src) {
            StatementInfo si = new StatementInfo();
            si.setType(s.getType());
            si.setLine(s.getLine());
            if (s.getSavepointName() != null) si.setSavepointName(s.getSavepointName());
            // Transaction-capture enrichment — carry the new fields through the dialect→common
            // copy (the writer maps them to the additive artifact keys); without this they were
            // captured by the visitor and silently dropped here.
            si.setSqlText(s.getSqlText());
            si.setGuardKind(s.getGuardKind());
            si.setGuardLine(s.getGuardLine());
            si.setGuardCondition(s.getGuardCondition());
            si.setHandlerName(s.getHandlerName());
            si.setHandlerLine(s.getHandlerLine());
            si.setForUpdate(s.getForUpdate());
            out.add(si);
        }
        return out;
    }

    private List<ExceptionHandlerInfo> toExceptionHandlers(List<PostgresExceptionHandlerInfo> src) {
        List<ExceptionHandlerInfo> out = new ArrayList<>();
        for (PostgresExceptionHandlerInfo e : src) {
            ExceptionHandlerInfo ei = new ExceptionHandlerInfo();
            ei.setExceptionName(e.getExceptionName());
            ei.setLine(e.getLine());
            ei.setLineEnd(e.getLineEnd());
            ei.setStatementsCount(e.getStatementsCount());
            out.add(ei);
        }
        return out;
    }

    /** Maps the dialect routine type onto the shared ParsedObject type string. */
    private String mapType(PostgresRoutineInfo.RoutineType t) {
        if (t == null) return "PROCEDURE";
        switch (t) {
            case FUNCTION: return "FUNCTION";
            case TRIGGER:  return "TRIGGER";
            case VIEW:     return "VIEW";
            // Task #25: keep the DO block's own identity instead of folding it into PROCEDURE.
            case DO_BLOCK: return "DO_BLOCK";
            default:       return "PROCEDURE";
        }
    }

    private int countLines(String src) {
        int n = 1;
        for (char c : src.toCharArray()) if (c == '\n') n++;
        return n;
    }
}
