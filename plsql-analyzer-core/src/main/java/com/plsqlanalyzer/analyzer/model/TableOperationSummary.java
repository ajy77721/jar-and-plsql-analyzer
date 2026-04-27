package com.plsqlanalyzer.analyzer.model;

import com.plsqlanalyzer.parser.model.SqlOperationType;
import com.plsqlanalyzer.parser.model.WhereFilter;

import java.util.*;

public class TableOperationSummary {
    private String tableName;
    private String schemaName;
    private Set<SqlOperationType> operations = new LinkedHashSet<>();
    private List<TableAccessDetail> accessDetails = new ArrayList<>();
    private List<TriggerDetail> triggers = new ArrayList<>();
    private boolean external; // table is in a different schema than the analyzed object
    private int accessCount;
    private String tableType; // TABLE, VIEW, MATERIALIZED VIEW

    public TableOperationSummary() {}

    public TableOperationSummary(String tableName, String schemaName) {
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public Set<SqlOperationType> getOperations() { return operations; }
    public void setOperations(Set<SqlOperationType> operations) { this.operations = operations; }

    public List<TableAccessDetail> getAccessDetails() { return accessDetails; }
    public void setAccessDetails(List<TableAccessDetail> accessDetails) { this.accessDetails = accessDetails; }

    public List<TriggerDetail> getTriggers() { return triggers; }
    public void setTriggers(List<TriggerDetail> triggers) { this.triggers = triggers; }

    public boolean isExternal() { return external; }
    public void setExternal(boolean external) { this.external = external; }

    public int getAccessCount() { return accessCount; }
    public void setAccessCount(int accessCount) { this.accessCount = accessCount; }

    public String getTableType() { return tableType; }
    public void setTableType(String tableType) { this.tableType = tableType; }

    public static class TriggerDetail {
        private String triggerName;
        private String triggerOwner;
        private String triggeringEvent; // BEFORE INSERT, AFTER UPDATE, etc.
        private String triggerType;     // BEFORE EACH ROW, AFTER STATEMENT, etc.
        private String status;          // ENABLED/DISABLED
        private List<String> calledProcedures = new ArrayList<>(); // procs called from trigger body
        private String triggerBody; // full trigger source code

        public TriggerDetail() {}

        public String getTriggerName() { return triggerName; }
        public void setTriggerName(String triggerName) { this.triggerName = triggerName; }

        public String getTriggerOwner() { return triggerOwner; }
        public void setTriggerOwner(String triggerOwner) { this.triggerOwner = triggerOwner; }

        public String getTriggeringEvent() { return triggeringEvent; }
        public void setTriggeringEvent(String triggeringEvent) { this.triggeringEvent = triggeringEvent; }

        public String getTriggerType() { return triggerType; }
        public void setTriggerType(String triggerType) { this.triggerType = triggerType; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public List<String> getCalledProcedures() { return calledProcedures; }
        public void setCalledProcedures(List<String> calledProcedures) { this.calledProcedures = calledProcedures; }

        public String getTriggerBody() { return triggerBody; }
        public void setTriggerBody(String triggerBody) { this.triggerBody = triggerBody; }
    }

    public static class TableAccessDetail {
        private String procedureId;
        private String procedureName;
        private SqlOperationType operation;
        private int lineNumber;
        private String sourceFile;
        private List<WhereFilter> whereFilters = new ArrayList<>();

        public TableAccessDetail() {}

        public String getProcedureId() { return procedureId; }
        public void setProcedureId(String procedureId) { this.procedureId = procedureId; }

        public String getProcedureName() { return procedureName; }
        public void setProcedureName(String procedureName) { this.procedureName = procedureName; }

        public SqlOperationType getOperation() { return operation; }
        public void setOperation(SqlOperationType operation) { this.operation = operation; }

        public int getLineNumber() { return lineNumber; }
        public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

        public String getSourceFile() { return sourceFile; }
        public void setSourceFile(String sourceFile) { this.sourceFile = sourceFile; }

        public List<WhereFilter> getWhereFilters() { return whereFilters; }
        public void setWhereFilters(List<WhereFilter> whereFilters) { this.whereFilters = whereFilters; }
    }
}
