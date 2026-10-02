package com.plsql.parser.multidb;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import com.plsql.parser.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter — delegates all grammar work to the Db2AnalysisEngine (db2-parser module).
 * Owns only the conversion from Db2RoutineInfo → shared ParseResult.
 */
public class Db2ProcedureParser implements StoredProcedureParser {

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

    private static final Db2AnalysisEngine ENGINE = new Db2AnalysisEngine();

    @Override
    public ParseResult parse(String source, String objectName, String schema) {
        long start = System.currentTimeMillis();
        Db2RoutineInfo info = ENGINE.parse(source, objectName, schema);

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
        // Sequence usages (NEXT VALUE FOR seq / NEXTVAL FOR seq / PREVVAL FOR seq) → synthetic
        // NEXTVAL/CURRVAL calls so the shared sequence report (Oracle pipeline, driven by
        // ChunkedFlowWriter#extractSequences) picks them up, mirroring SqlServerProcedureParser.
        // PREVVAL maps to CURRVAL — same "read the last generated value" semantics the shared
        // pipeline already recognizes; it has no notion of a DB2-specific PREVVAL name.
        for (Db2SequenceInfo s : info.getSequences()) {
            CallInfo seqCall = new CallInfo();
            seqCall.setName("PREVVAL".equals(s.getOperation()) ? "CURRVAL" : "NEXTVAL");
            seqCall.setPackageName(s.getSequenceName());
            seqCall.setSchema(s.getSchema());
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
        // Task #26: WITH RETURN cursors are DB2's result-set hand-back — carry the count through
        // the one bridge the artifact writer consumes.
        obj.setReturnsResultSet(info.isReturnsResultSet());
        obj.setResultSets(info.getResultSets());

        result.getObjects().add(obj);
        result.setParseTimeMs(System.currentTimeMillis() - start);
        if (result.getObjects() != null) ENRICHER.enrich(result.getObjects());
        return result;
    }

    // ── Converters ────────────────────────────────────────────────────────────

    /**
     * Declared FOREIGN KEY relationships, carried across the bridge.
     *
     * <p>Without this, {@code Db2RoutineInfo.foreignKeys} would end at the parser: the artifact
     * writer only sees {@link ParsedObject}, so an unconverted list is invisible to Mongo ingest
     * and to every screen. The lists are copied rather than shared so nothing downstream can
     * mutate its way back into the parse result.
     */
    private List<ForeignKeyInfo> toForeignKeys(List<Db2ForeignKeyInfo> src) {
        List<ForeignKeyInfo> out = new ArrayList<>();
        if (src == null) return out;
        for (Db2ForeignKeyInfo fk : src) {
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

    private List<ParameterInfo> toParams(List<Db2ParamInfo> src) {
        List<ParameterInfo> out = new ArrayList<>();
        for (Db2ParamInfo p : src) {
            ParameterInfo pi = new ParameterInfo();
            pi.setName(p.getName());
            pi.setDataType(p.getDataType());
            pi.setDirection(p.getDirection().name());
            out.add(pi);
        }
        return out;
    }

    private List<VariableInfo> toVars(List<Db2VariableInfo> vars, List<Db2CursorInfo> cursors) {
        List<VariableInfo> out = new ArrayList<>();
        for (Db2VariableInfo v : vars) {
            VariableInfo vi = new VariableInfo();
            vi.setName(v.getName());
            vi.setDataType(v.getDataType());
            vi.setDefaultValue(v.getDefaultValue());
            vi.setLine(v.getLine());
            out.add(vi);
        }
        for (Db2CursorInfo c : cursors) {
            VariableInfo vi = new VariableInfo();
            vi.setName(c.getName());
            vi.setDataType("CURSOR");
            vi.setLine(c.getLine());
            out.add(vi);
        }
        return out;
    }

    private List<CallInfo> toCalls(List<Db2CallInfo> src) {
        List<CallInfo> out = new ArrayList<>();
        for (Db2CallInfo c : src) {
            CallInfo ci = new CallInfo();
            ci.setName(c.getName());
            ci.setSchema(c.getSchema());
            ci.setType("EXTERNAL");
            ci.setLine(c.getLine());
            out.add(ci);
        }
        return out;
    }

    private List<TableOperationInfo> toTableOps(List<Db2TableOpInfo> src) {
        List<TableOperationInfo> out = new ArrayList<>();
        for (Db2TableOpInfo op : src) {
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
                for (Db2JoinInfo j : op.getJoins()) {
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

    private List<CursorInfo> toCursors(List<Db2CursorInfo> src) {
        List<CursorInfo> out = new ArrayList<>();
        for (Db2CursorInfo c : src) {
            CursorInfo ci = new CursorInfo();
            ci.setName(c.getName());
            ci.setQuery(c.getSelectText());
            ci.setLine(c.getLine());
            out.add(ci);
        }
        return out;
    }

    private List<DynamicSqlInfo> toDynamicSql(List<Db2DynamicSqlInfo> src) {
        List<DynamicSqlInfo> out = new ArrayList<>();
        for (Db2DynamicSqlInfo d : src) {
            DynamicSqlInfo di = new DynamicSqlInfo();
            di.setType(d.getKind() != null ? d.getKind().name() : "DYNAMIC");
            di.setSqlExpression(d.getSqlSource());
            di.setLine(d.getLine());
            di.setStmtName(d.getStmtName());
            out.add(di);
        }
        return out;
    }

    private List<StatementInfo> toStatements(List<Db2StatementInfo> src) {
        List<StatementInfo> out = new ArrayList<>();
        for (Db2StatementInfo s : src) {
            StatementInfo si = new StatementInfo();
            si.setType(s.getType());
            si.setLine(s.getLine());
            if (s.getSavepointName() != null) si.setSavepointName(s.getSavepointName());
            out.add(si);
        }
        return out;
    }

    private List<ExceptionHandlerInfo> toExceptionHandlers(List<Db2ExceptionHandlerInfo> src) {
        List<ExceptionHandlerInfo> out = new ArrayList<>();
        for (Db2ExceptionHandlerInfo h : src) {
            ExceptionHandlerInfo ei = new ExceptionHandlerInfo();
            ei.setKind(h.getKind() != null ? h.getKind().name() : null);
            ei.setConditions(h.getConditions());
            ei.setExceptionName(String.join(", ", h.getConditions()));
            ei.setLine(h.getLine());
            out.add(ei);
        }
        return out;
    }

    private int countLines(String src) {
        int n = 1;
        for (char c : src.toCharArray()) if (c == '\n') n++;
        return n;
    }

    // Was previously a two-way FUNCTION-vs-everything-else check that mislabelled every
    // TRIGGER/VIEW as "PROCEDURE" once db2-parser gained trigger/view support — mirrors
    // SqlServerProcedureParser/PostgresProcedureParser's mapType().
    private String mapType(Db2RoutineInfo.RoutineType t) {
        if (t == null) return "PROCEDURE";
        switch (t) {
            case FUNCTION: return "FUNCTION";
            case TRIGGER:  return "TRIGGER";
            case VIEW:     return "VIEW";
            default:       return "PROCEDURE";
        }
    }
}
