package com.plsql.parser.multidb;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.*;
import com.plsql.parser.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL stored-procedure parser — adapter layer.
 *
 * Delegates all grammar work to the dedicated mysql-parser module
 * (MySqlGrammarEngine, ANTLR4-based, completely separate grammar from Oracle).
 * This class owns only the conversion from MySqlRoutineInfo → shared ParseResult.
 */
public class MySqlProcedureParser implements StoredProcedureParser {

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

    private static final MySqlGrammarEngine ENGINE = new MySqlGrammarEngine();

    @Override
    public ParseResult parse(String source, String objectName, String schema) {
        long start = System.currentTimeMillis();
        MySqlRoutineInfo info = ENGINE.parse(source, objectName, schema);

        ParseResult result = new ParseResult();
        result.setFileName(info.getName() + ".sql");
        result.getErrors().addAll(info.getErrors());

        ParsedObject obj = new ParsedObject();
        obj.setName(info.getName());
        obj.setSchema(info.getSchema());
        // TRIGGER and VIEW are now parseable by the MySQL grammar too, so pass the captured
        // type straight through instead of collapsing everything that isn't a FUNCTION to
        // "PROCEDURE" (which would have mislabelled every trigger and view). Null (nothing
        // recognized, e.g. a hard parse failure) keeps the previous PROCEDURE default.
        obj.setType(info.getType() == null ? "PROCEDURE" : info.getType().name());
        obj.setLineStart(info.getLineStart());
        obj.setLineEnd(info.getLineEnd() > 0 ? info.getLineEnd() : countLines(source));
        obj.setParameters(toParams(info.getParams()));
        obj.setGlobalVariables(toVars(info.getVariables(), info.getCursors()));
        obj.setCalls(toCalls(info.getCalls()));
        obj.setTableOperations(toTableOps(info.getTableOps()));
        obj.setCursors(toCursors(info.getCursors()));
        obj.setDynamicSql(toDynamicSql(info.getDynamicSql()));
        obj.setStatements(toStatements(info.getStatements()));
        obj.setExceptionHandlers(toExceptionHandlers(info));
        obj.setForeignKeys(toForeignKeys(info.getForeignKeys()));
        // Scheduled EVENT metadata (RoutineType.EVENT only): the ON SCHEDULE clause, the
        // ENABLE/DISABLE status and the first procedure the DO body CALLs — the job's target,
        // which the crawler turns into a SCHEDULES edge. Null for every other routine type.
        if ("EVENT".equals(obj.getType())) {
            obj.setSchedule(info.getEventSchedule());
            obj.setJobEnabled(info.getEventEnabled());
            for (MySqlCallInfo c : info.getCalls()) {
                if (c.getName() != null && !c.getName().isBlank()) {
                    obj.setJobTarget(c.getSchema() != null && !c.getSchema().isBlank()
                            ? c.getSchema() + "." + c.getName() : c.getName());
                    break;
                }
            }
        }

        result.getObjects().add(obj);
        result.setParseTimeMs(System.currentTimeMillis() - start);
        if (result.getObjects() != null) ENRICHER.enrich(result.getObjects());
        return result;
    }

    // ── Converters ────────────────────────────────────────────────────────────

    private List<ParameterInfo> toParams(List<MySqlParamInfo> src) {
        List<ParameterInfo> out = new ArrayList<>();
        for (MySqlParamInfo p : src) {
            ParameterInfo pi = new ParameterInfo();
            pi.setName(p.getName());
            pi.setDataType(p.getDataType());
            pi.setDirection(p.getDirection().name());
            out.add(pi);
        }
        return out;
    }

    private List<VariableInfo> toVars(List<MySqlVariableInfo> vars, List<MySqlCursorInfo> cursors) {
        List<VariableInfo> out = new ArrayList<>();
        for (MySqlVariableInfo v : vars) {
            VariableInfo vi = new VariableInfo();
            vi.setName(v.getName());
            vi.setDataType(v.getDataType());
            vi.setDefaultValue(v.getDefaultValue());
            vi.setLine(v.getLine());
            out.add(vi);
        }
        for (MySqlCursorInfo c : cursors) {
            VariableInfo vi = new VariableInfo();
            vi.setName(c.getName());
            vi.setDataType("CURSOR");
            vi.setLine(c.getLine());
            out.add(vi);
        }
        return out;
    }

    private List<CallInfo> toCalls(List<MySqlCallInfo> src) {
        List<CallInfo> out = new ArrayList<>();
        for (MySqlCallInfo c : src) {
            CallInfo ci = new CallInfo();
            ci.setName(c.getName());
            ci.setSchema(c.getSchema());
            ci.setType("EXTERNAL");
            ci.setLine(c.getLine());
            out.add(ci);
        }
        return out;
    }

    /**
     * Declared FOREIGN KEY relationships, carried across the bridge.
     *
     * <p>{@code MySqlRoutineInfo.foreignKeys} was populated by the visitor but read by nothing:
     * this adapter converts every other list it owns, and omitting this one left the data ending
     * at the parser, invisible to the artifact writer and so to Mongo ingest and the UI.
     */
    private List<ForeignKeyInfo> toForeignKeys(List<MySqlForeignKeyInfo> src) {
        List<ForeignKeyInfo> out = new ArrayList<>();
        if (src == null) return out;
        for (MySqlForeignKeyInfo fk : src) {
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

    private List<TableOperationInfo> toTableOps(List<MySqlTableOpInfo> src) {
        List<TableOperationInfo> out = new ArrayList<>();
        for (MySqlTableOpInfo op : src) {
            TableOperationInfo ti = new TableOperationInfo();
            ti.setOperation(op.getDmlType().name());
            ti.setTableName(op.getTableName());
            ti.setSchema(op.getSchemaName());
            ti.setAlias(op.getAlias());
            ti.setLine(op.getLine());
            // Carried through so the shared TableOperationEnricher can derive column-level detail.
            ti.setSqlText(op.getSqlText());
            if (op.getJoins() != null && !op.getJoins().isEmpty()) {
                List<JoinInfo> joins = new ArrayList<>();
                for (MySqlJoinInfo j : op.getJoins()) {
                    JoinInfo ji = new JoinInfo();
                    ji.setJoinType(j.getJoinType());
                    // Bare joined-table name (no schema prefix) so the UI's per-table Joins tab
                    // resolves both sides by name — mirrors the SqlServer converter.
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

    private List<CursorInfo> toCursors(List<MySqlCursorInfo> src) {
        List<CursorInfo> out = new ArrayList<>();
        for (MySqlCursorInfo c : src) {
            CursorInfo ci = new CursorInfo();
            ci.setName(c.getName());
            ci.setQuery(c.getSelectText());
            ci.setLine(c.getLine());
            out.add(ci);
        }
        return out;
    }

    private List<DynamicSqlInfo> toDynamicSql(List<MySqlDynamicSqlInfo> src) {
        List<DynamicSqlInfo> out = new ArrayList<>();
        for (MySqlDynamicSqlInfo d : src) {
            DynamicSqlInfo di = new DynamicSqlInfo();
            di.setType(d.getKind() != null ? d.getKind().name() : "DYNAMIC");
            di.setSqlExpression(d.getSqlSource());
            di.setLine(d.getLine());
            di.setStmtName(d.getStmtName());
            out.add(di);
        }
        return out;
    }

    /**
     * Converts {@code DECLARE ... HANDLER} blocks (audit #9) to the shared
     * {@code ExceptionHandlerInfo} model — the same DTO Oracle's visitor already populates — so
     * handler bodies stop being indistinguishable from main-body code in the flat top-level lists.
     *
     * <p>{@code info}'s CALL/table-op lists tag each entry with the index of the handler it was
     * found inside ({@code handlerIndex}); this pulls those tagged entries into their owning
     * handler's own {@code calls}/{@code tableOperations}, and counts every tagged statement kind
     * (CALL, table op, or plain statement) toward {@code statementsCount}.
     */
    private List<ExceptionHandlerInfo> toExceptionHandlers(MySqlRoutineInfo info) {
        List<MySqlExceptionHandlerInfo> src = info.getExceptionHandlers();
        List<ExceptionHandlerInfo> out = new ArrayList<>();
        for (int i = 0; i < src.size(); i++) {
            MySqlExceptionHandlerInfo h = src.get(i);
            ExceptionHandlerInfo ehi = new ExceptionHandlerInfo();
            String kind = h.getKind() != null ? h.getKind().name() : "UNKNOWN";
            String conditions = String.join(", ", h.getConditions());
            ehi.setExceptionName(conditions.isBlank() ? kind : kind + " " + conditions);
            ehi.setLine(h.getLine());

            int count = 0;
            List<CallInfo> calls = new ArrayList<>();
            for (MySqlCallInfo c : info.getCalls()) {
                if (Integer.valueOf(i).equals(c.getHandlerIndex())) {
                    CallInfo ci = new CallInfo();
                    ci.setName(c.getName());
                    ci.setSchema(c.getSchema());
                    ci.setType("EXTERNAL");
                    ci.setLine(c.getLine());
                    calls.add(ci);
                    count++;
                }
            }
            ehi.setCalls(calls);

            List<TableOperationInfo> ops = new ArrayList<>();
            for (MySqlTableOpInfo op : info.getTableOps()) {
                if (Integer.valueOf(i).equals(op.getHandlerIndex())) {
                    TableOperationInfo ti = new TableOperationInfo();
                    ti.setOperation(op.getDmlType().name());
                    ti.setTableName(op.getTableName());
                    ti.setSchema(op.getSchemaName());
                    ti.setAlias(op.getAlias());
                    ti.setLine(op.getLine());
                    ti.setSqlText(op.getSqlText());
                    ops.add(ti);
                    count++;
                }
            }
            ehi.setTableOperations(ops);

            for (MySqlStatementInfo s : info.getStatements()) {
                if (Integer.valueOf(i).equals(s.getHandlerIndex())) count++;
            }
            ehi.setStatementsCount(count);

            out.add(ehi);
        }
        return out;
    }

    private List<StatementInfo> toStatements(List<MySqlStatementInfo> src) {
        List<StatementInfo> out = new ArrayList<>();
        for (MySqlStatementInfo s : src) {
            StatementInfo si = new StatementInfo();
            si.setType(s.getType());
            si.setLine(s.getLine());
            if (s.getSavepointName() != null) si.setSavepointName(s.getSavepointName());
            out.add(si);
        }
        return out;
    }

    private int countLines(String src) {
        int n = 1;
        for (char c : src.toCharArray()) if (c == '\n') n++;
        return n;
    }
}
