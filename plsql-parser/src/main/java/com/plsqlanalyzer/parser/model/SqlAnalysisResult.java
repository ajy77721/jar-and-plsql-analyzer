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

    public List<SequenceReference> getSequenceReferences() { return sequenceReferences; }
    public void setSequenceReferences(List<SequenceReference> sequenceReferences) { this.sequenceReferences = sequenceReferences; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
