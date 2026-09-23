package com.plsqlanalyzer.parser.model;

import java.util.ArrayList;
import java.util.List;

public class SqlAnalysisResult {
    private String rawSql;
    private SqlOperationType operationType;
    private List<TableReference> tableReferences = new ArrayList<>();
    private List<WhereFilter> whereFilters = new ArrayList<>();
    private List<JoinInfo> joinInfos = new ArrayList<>();
    private List<SequenceReference> sequenceReferences = new ArrayList<>();

    /**
     * Column-level detail: what a SELECT reads (and into which PL/SQL variable), and what an
     * UPDATE/INSERT/MERGE writes and from where. See {@link ColumnUsage}.
     */
    private List<ColumnUsage> columnUsages = new ArrayList<>();

    /** DDL found in this statement — see {@link DdlInfo}. Empty for DML. */
    private List<DdlInfo> ddl = new ArrayList<>();
    private int lineNumber;

    public SqlAnalysisResult() {}

    public String getRawSql() { return rawSql; }
    public void setRawSql(String rawSql) { this.rawSql = rawSql; }

    public SqlOperationType getOperationType() { return operationType; }
    public void setOperationType(SqlOperationType operationType) { this.operationType = operationType; }

    public List<TableReference> getTableReferences() { return tableReferences; }
    public void setTableReferences(List<TableReference> tableReferences) { this.tableReferences = tableReferences; }

    public List<WhereFilter> getWhereFilters() { return whereFilters; }
    public void setWhereFilters(List<WhereFilter> whereFilters) { this.whereFilters = whereFilters; }

    public List<JoinInfo> getJoinInfos() { return joinInfos; }
    public void setJoinInfos(List<JoinInfo> joinInfos) { this.joinInfos = joinInfos; }

    public List<ColumnUsage> getColumnUsages() { return columnUsages; }
    public void setColumnUsages(List<ColumnUsage> columnUsages) { this.columnUsages = columnUsages; }
    public List<DdlInfo> getDdl() { return ddl; }
    public void setDdl(List<DdlInfo> ddl) { this.ddl = ddl; }
    public List<SequenceReference> getSequenceReferences() { return sequenceReferences; }
    public void setSequenceReferences(List<SequenceReference> sequenceReferences) { this.sequenceReferences = sequenceReferences; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
