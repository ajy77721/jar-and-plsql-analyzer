package com.plsqlanalyzer.analyzer.model;

import java.util.*;

/**
 * Summary of all references to a specific cursor across all procedures.
 * Key is the cursor name (e.g., "C_CUSTOMERS").
 */
public class CursorOperationSummary {
    private String cursorName;
    private String cursorType;   // EXPLICIT, REF_CURSOR, FOR_LOOP, OPEN_FOR
    private String queryText;    // The SELECT query (from DECLARE or OPEN FOR)
    private Set<String> operations = new LinkedHashSet<>(); // DECLARE, OPEN, FETCH, CLOSE, FOR_LOOP
    private List<CursorAccessDetail> accessDetails = new ArrayList<>();
    private int accessCount;
    private boolean external;    // true if cursor is declared in a different package

    public CursorOperationSummary() {}

    public CursorOperationSummary(String cursorName) {
        this.cursorName = cursorName;
    }

    public String getCursorName() { return cursorName; }
    public void setCursorName(String cursorName) { this.cursorName = cursorName; }

    public String getCursorType() { return cursorType; }
    public void setCursorType(String cursorType) { this.cursorType = cursorType; }

    public String getQueryText() { return queryText; }
    public void setQueryText(String queryText) { this.queryText = queryText; }

    public Set<String> getOperations() { return operations; }
    public void setOperations(Set<String> operations) { this.operations = operations; }

    public List<CursorAccessDetail> getAccessDetails() { return accessDetails; }
    public void setAccessDetails(List<CursorAccessDetail> accessDetails) { this.accessDetails = accessDetails; }

    public int getAccessCount() { return accessCount; }
    public void setAccessCount(int accessCount) { this.accessCount = accessCount; }

    public boolean isExternal() { return external; }
    public void setExternal(boolean external) { this.external = external; }

    public static class CursorAccessDetail {
        private String procedureId;
        private String procedureName;
        private String operation;    // DECLARE, OPEN, FETCH, CLOSE, FOR_LOOP
        private String cursorType;   // EXPLICIT, REF_CURSOR, FOR_LOOP, OPEN_FOR
        private String queryText;    // Only present for DECLARE/OPEN_FOR/FOR_LOOP
        private int lineNumber;
        private String sourceFile;

        public CursorAccessDetail() {}

        public String getProcedureId() { return procedureId; }
        public void setProcedureId(String procedureId) { this.procedureId = procedureId; }

        public String getProcedureName() { return procedureName; }
        public void setProcedureName(String procedureName) { this.procedureName = procedureName; }

        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }

        public String getCursorType() { return cursorType; }
        public void setCursorType(String cursorType) { this.cursorType = cursorType; }

        public String getQueryText() { return queryText; }
        public void setQueryText(String queryText) { this.queryText = queryText; }

        public int getLineNumber() { return lineNumber; }
        public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

        public String getSourceFile() { return sourceFile; }
        public void setSourceFile(String sourceFile) { this.sourceFile = sourceFile; }
    }
}
