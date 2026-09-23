package com.plsqlanalyzer.analyzer.model;

import java.util.*;

/**
 * Summary of all references to a specific Oracle SEQUENCE across all procedures.
 * Analogous to TableOperationSummary for tables.
 */
public class SequenceOperationSummary {
    private String sequenceName;
    private String schemaName;
    private Set<String> operations = new LinkedHashSet<>(); // NEXTVAL, CURRVAL
    private List<SequenceAccessDetail> accessDetails = new ArrayList<>();
    private int accessCount;

    public SequenceOperationSummary() {}

    public SequenceOperationSummary(String sequenceName, String schemaName) {
        this.sequenceName = sequenceName;
        this.schemaName = schemaName;
    }

    public String getSequenceName() { return sequenceName; }
    public void setSequenceName(String sequenceName) { this.sequenceName = sequenceName; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public Set<String> getOperations() { return operations; }
    public void setOperations(Set<String> operations) { this.operations = operations; }

    public List<SequenceAccessDetail> getAccessDetails() { return accessDetails; }
    public void setAccessDetails(List<SequenceAccessDetail> accessDetails) { this.accessDetails = accessDetails; }

    public int getAccessCount() { return accessCount; }
    public void setAccessCount(int accessCount) { this.accessCount = accessCount; }

    public static class SequenceAccessDetail {
        private String procedureId;
        private String procedureName;
        private String operation; // NEXTVAL or CURRVAL
        private int lineNumber;
        private String sourceFile;

        public SequenceAccessDetail() {}

        public String getProcedureId() { return procedureId; }
        public void setProcedureId(String procedureId) { this.procedureId = procedureId; }

        public String getProcedureName() { return procedureName; }
        public void setProcedureName(String procedureName) { this.procedureName = procedureName; }

        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }

        public int getLineNumber() { return lineNumber; }
        public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

        public String getSourceFile() { return sourceFile; }
        public void setSourceFile(String sourceFile) { this.sourceFile = sourceFile; }
    }
}
