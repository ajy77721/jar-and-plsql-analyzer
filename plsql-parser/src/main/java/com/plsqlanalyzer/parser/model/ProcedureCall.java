package com.plsqlanalyzer.parser.model;

public class ProcedureCall {
    private String schemaName;
    private String packageName;
    private String procedureName;
    private int lineNumber;
    private boolean isDynamicSql;

    public ProcedureCall() {}

    public ProcedureCall(String schemaName, String packageName, String procedureName,
                         int lineNumber, boolean isDynamicSql) {
        this.schemaName = schemaName;
        this.packageName = packageName;
        this.procedureName = procedureName;
        this.lineNumber = lineNumber;
        this.isDynamicSql = isDynamicSql;
    }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }

    public String getProcedureName() { return procedureName; }
    public void setProcedureName(String procedureName) { this.procedureName = procedureName; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    public boolean isDynamicSql() { return isDynamicSql; }
    public void setDynamicSql(boolean dynamicSql) { isDynamicSql = dynamicSql; }

    public String getQualifiedName() {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null) sb.append(schemaName).append(".");
        if (packageName != null) sb.append(packageName).append(".");
        sb.append(procedureName);
        return sb.toString();
    }
}
