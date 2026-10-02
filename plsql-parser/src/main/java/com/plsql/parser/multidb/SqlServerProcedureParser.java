package com.plsql.parser.multidb;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import com.plsql.parser.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter — delegates all grammar work to the SqlServerAnalysisEngine (sqlserver-parser module).
 * Owns only the conversion from SqlServerRoutineInfo → shared ParseResult.
 */
public class SqlServerProcedureParser implements StoredProcedureParser {

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

    private static final SqlServerAnalysisEngine ENGINE = new SqlServerAnalysisEngine();

    @Override
    public ParseResult parse(String source, String objectName, String schema) {
        long start = System.currentTimeMillis();
        // Bug 7 fix: split on standalone "GO" batch separators first — compilationUnit only
        // accepts one routine per parse, so a script with multiple CREATE PROCEDUREs separated
        // by GO used to merge them into a single parse and misattribute the second procedure's
        // calls to the first. parseBatch() parses each batch independently; every batch becomes
        // its own ParsedObject below.
        List<SqlServerRoutineInfo> infos = ENGINE.parseBatch(source, objectName, schema);

        ParseResult result = new ParseResult();
        result.setFileName((infos.get(0).getName() != null ? infos.get(0).getName() : objectName) + ".sql");
        for (SqlServerRoutineInfo info : infos) {
            result.getErrors().addAll(info.getErrors());
            result.getObjects().add(toParsedObject(info, source));
        }
        result.setParseTimeMs(System.currentTimeMillis() - start);
        if (result.getObjects() != null) ENRICHER.enrich(result.getObjects(),
                com.plsql.parser.service.TableOperationEnricher.Dialect.SQLSERVER);
        return result;
    }

    private ParsedObject toParsedObject(SqlServerRoutineInfo info, String source) {
        ParsedObject obj = new ParsedObject();
        obj.setName(info.getName());
        obj.setSchema(info.getSchema());
        obj.setType(mapType(info.getType()));
        obj.setLineStart(info.getLineStart());
        obj.setLineEnd(info.getLineEnd() > 0 ? info.getLineEnd() : countLines(source));
        obj.setParameters(toParams(info.getParams()));
        obj.setGlobalVariables(toVars(info.getVariables()));
        List<CallInfo> calls = toCalls(info.getCalls());
        // Sequence usages (NEXT VALUE FOR seq) → synthetic NEXTVAL calls so the shared
        // sequence report (Oracle pipeline) picks them up. Typed BUILTIN so the crawler
        // never tries to follow them as routines.
        for (SqlServerSequenceInfo s : info.getSequences()) {
            // IDENTITY columns (audit #8) share this model (see SqlServerSequenceInfo's class
            // doc) but are not a NEXT-VALUE-FOR sequence usage — skip them here, they have no
            // callable target to synthesise. They're carried through when getSequences() is
            // reused directly.
            if (s.isIdentityColumn()) {
                IdentityColumnInfo ic = new IdentityColumnInfo();
                ic.setSchema(s.getSchema());
                ic.setTableName(s.getTableName());
                ic.setColumnName(s.getColumnName());
                ic.setSeed(s.getSeed() != null ? s.getSeed() : 1L);
                ic.setIncrement(s.getIncrement() != null ? s.getIncrement() : 1L);
                ic.setLine(s.getLine());
                obj.getIdentityColumns().add(ic);
                continue;
            }
            CallInfo seqCall = new CallInfo();
            seqCall.setName("NEXTVAL");
            seqCall.setPackageName(s.getName());
            seqCall.setSchema(s.getSchema());
            seqCall.setType("BUILTIN");
            seqCall.setLine(s.getLine());
            calls.add(seqCall);
        }
        obj.setCalls(calls);
        obj.setTableOperations(toTableOps(info.getTableOps()));
        obj.setDynamicSql(toDynamicSql(info.getDynamicSql()));
        obj.setCursors(toCursors(info.getCursors()));
        obj.setStatements(toStatements(info.getStatements()));
        obj.setExceptionHandlers(toExceptionHandlers(info.getExceptionHandlers()));
        obj.setForeignKeys(toForeignKeys(info.getForeignKeys()));

        // Trigger / view metadata
        if (info.getTriggerTable()  != null) obj.setTriggerTable(info.getTriggerTable());
        if (info.getTriggerEvent()  != null) obj.setTriggerEvent(info.getTriggerEvent());
        if (info.getTriggerTiming() != null) obj.setTriggerTiming(info.getTriggerTiming());
        if (info.getViewQuery()     != null) obj.setViewQuery(info.getViewQuery());

        // Tasks #26/#27: result-set count + TVF return shape — this adapter is the only bridge to
        // the shared model the artifact writer consumes.
        obj.setReturnsResultSet(info.isReturnsResultSet());
        obj.setResultSets(info.getResultSets());
        obj.setReturnType(info.getReturnType());
        if (info.getReturnsColumns() != null && !info.getReturnsColumns().isEmpty()) {
            obj.setReturnsColumns(new ArrayList<>(info.getReturnsColumns()));
        }

        return obj;
    }

    private String mapType(SqlServerRoutineInfo.RoutineType t) {
        if (t == null) return "PROCEDURE";
        switch (t) {
            case FUNCTION: return "FUNCTION";
            case TRIGGER:  return "TRIGGER";
            case VIEW:     return "VIEW";
            default:       return "PROCEDURE";
        }
    }

    // ── Converters ────────────────────────────────────────────────────────────

    private List<ParameterInfo> toParams(List<SqlServerParamInfo> src) {
        List<ParameterInfo> out = new ArrayList<>();
        for (SqlServerParamInfo p : src) {
            ParameterInfo pi = new ParameterInfo();
            pi.setName(p.getName());
            pi.setDataType(p.getDataType());
            pi.setDirection(p.getDirection().name());
            out.add(pi);
        }
        return out;
    }

    /**
     * Declared FOREIGN KEY relationships, carried across the bridge.
     *
     * <p>This adapter owns the only conversion from the dialect model to the shared
     * {@link ParsedObject} that everything downstream consumes; a list it does not copy ends at
     * the parser, invisible to the artifact writer and so to Mongo ingest and the UI. The copy
     * is deep — the shared model must own its own mutable column lists, or a caller mutating the
     * shared object would reach back into the parse result.
     */
    private List<ForeignKeyInfo> toForeignKeys(List<SqlServerForeignKeyInfo> src) {
        List<ForeignKeyInfo> out = new ArrayList<>();
        if (src == null) return out;
        for (SqlServerForeignKeyInfo fk : src) {
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

    private List<VariableInfo> toVars(List<SqlServerVariableInfo> vars) {
        List<VariableInfo> out = new ArrayList<>();
        for (SqlServerVariableInfo v : vars) {
            VariableInfo vi = new VariableInfo();
            vi.setName(v.getName());
            vi.setDataType(v.getDataType());
            vi.setLine(v.getLine());
            out.add(vi);
        }
        return out;
    }

    private List<CallInfo> toCalls(List<SqlServerCallInfo> src) {
        List<CallInfo> out = new ArrayList<>();
        for (SqlServerCallInfo c : src) {
            CallInfo ci = new CallInfo();
            ci.setName(c.getName());
            ci.setSchema(c.getSchema());
            ci.setType("EXTERNAL");   // outbound call to another routine — crawler follows EXTERNAL/INTERNAL
            ci.setLine(c.getLine());
            out.add(ci);
        }
        return out;
    }

    private List<TableOperationInfo> toTableOps(List<SqlServerTableOpInfo> src) {
        List<TableOperationInfo> out = new ArrayList<>();
        for (SqlServerTableOpInfo op : src) {
            TableOperationInfo ti = new TableOperationInfo();
            ti.setOperation(op.getDmlType().name());
            ti.setTableName(op.getTableName());
            ti.setSchema(op.getSchemaName());
            ti.setAlias(op.getAlias());
            ti.setLine(op.getLine());
            // Carried through for the shared TableOperationEnricher.
            ti.setSqlText(op.getSqlText());
            if (op.getJoins() != null && !op.getJoins().isEmpty()) {
                List<JoinInfo> joins = new ArrayList<>();
                for (SqlServerJoinInfo j : op.getJoins()) {
                    JoinInfo ji = new JoinInfo();
                    ji.setJoinType(j.getJoinType());
                    // Store the BARE table name (no schema prefix) so the UI's per-table
                    // "Joins" tab — which matches incoming joins by bare table name — resolves
                    // both sides (e.g. clicking Inventory shows "Product LEFT JOIN Inventory").
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

    private List<DynamicSqlInfo> toDynamicSql(List<SqlServerDynamicSqlInfo> src) {
        List<DynamicSqlInfo> out = new ArrayList<>();
        for (SqlServerDynamicSqlInfo d : src) {
            DynamicSqlInfo di = new DynamicSqlInfo();
            di.setType(d.getKind() != null ? d.getKind().name() : "DYNAMIC");
            di.setSqlExpression(d.getSqlSource());
            di.setLine(d.getLine());
            out.add(di);
        }
        return out;
    }

    private List<CursorInfo> toCursors(List<SqlServerCursorInfo> src) {
        List<CursorInfo> out = new ArrayList<>();
        for (SqlServerCursorInfo c : src) {
            CursorInfo ci = new CursorInfo();
            ci.setName(c.getName());
            ci.setQuery(c.getQuery());
            ci.setLine(c.getLine());
            if (c.getStmtOperations() != null) {
                List<CursorInfo.StmtOp> ops = new ArrayList<>();
                for (SqlServerCursorInfo.CursorStmtOp op : c.getStmtOperations()) {
                    ops.add(new CursorInfo.StmtOp(op.getOperation(), op.getLine()));
                }
                ci.setStmtOperations(ops);
            }
            out.add(ci);
        }
        return out;
    }

    private List<StatementInfo> toStatements(List<SqlServerStatementInfo> src) {
        List<StatementInfo> out = new ArrayList<>();
        for (SqlServerStatementInfo s : src) {
            StatementInfo si = new StatementInfo();
            si.setType(s.getType());
            si.setLine(s.getLine());
            if (s.getSavepointName() != null) si.setSavepointName(s.getSavepointName());
            out.add(si);
        }
        return out;
    }

    private List<ExceptionHandlerInfo> toExceptionHandlers(List<SqlServerExceptionHandlerInfo> src) {
        List<ExceptionHandlerInfo> out = new ArrayList<>();
        for (SqlServerExceptionHandlerInfo e : src) {
            ExceptionHandlerInfo ei = new ExceptionHandlerInfo();
            ei.setExceptionName(e.getExceptionName());
            ei.setLine(e.getLine());
            ei.setLineEnd(e.getLineEnd());
            ei.setStatementsCount(e.getStatementsCount());
            out.add(ei);
        }
        return out;
    }

    private int countLines(String src) {
        int n = 1;
        for (char c : src.toCharArray()) if (c == '\n') n++;
        return n;
    }
}
