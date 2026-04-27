package com.plsqlanalyzer.analyzer.model;

import java.util.*;

/**
 * Summary of all JOINs found in the analysis.
 * Key is "LEFT_TABLE|RIGHT_TABLE" to group unique join pairs.
 */
public class JoinOperationSummary {
    private String leftTable;
    private String rightTable;
    private Set<String> joinTypes = new LinkedHashSet<>(); // INNER, LEFT, RIGHT, etc.
    private List<JoinAccessDetail> accessDetails = new ArrayList<>();
    private int accessCount;

    public JoinOperationSummary() {}

    public JoinOperationSummary(String leftTable, String rightTable) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public String getLeftTable() { return leftTable; }
    public void setLeftTable(String leftTable) { this.leftTable = leftTable; }

    public String getRightTable() { return rightTable; }
    public void setRightTable(String rightTable) { this.rightTable = rightTable; }

    public Set<String> getJoinTypes() { return joinTypes; }
    public void setJoinTypes(Set<String> joinTypes) { this.joinTypes = joinTypes; }

    public List<JoinAccessDetail> getAccessDetails() { return accessDetails; }
    public void setAccessDetails(List<JoinAccessDetail> accessDetails) { this.accessDetails = accessDetails; }

    public int getAccessCount() { return accessCount; }
    public void setAccessCount(int accessCount) { this.accessCount = accessCount; }

    public static class JoinAccessDetail {
        private String procedureId;
        private String procedureName;
        private String joinType;     // INNER, LEFT, RIGHT, etc.
        private String onPredicate;  // The ON clause
        private int lineNumber;
        private String sourceFile;

        public JoinAccessDetail() {}

        public String getProcedureId() { return procedureId; }
        public void setProcedureId(String procedureId) { this.procedureId = procedureId; }

        public String getProcedureName() { return procedureName; }
        public void setProcedureName(String procedureName) { this.procedureName = procedureName; }

        public String getJoinType() { return joinType; }
        public void setJoinType(String joinType) { this.joinType = joinType; }

        public String getOnPredicate() { return onPredicate; }
        public void setOnPredicate(String onPredicate) { this.onPredicate = onPredicate; }

        public int getLineNumber() { return lineNumber; }
        public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

        public String getSourceFile() { return sourceFile; }
        public void setSourceFile(String sourceFile) { this.sourceFile = sourceFile; }
    }
}
