package com.plsqlanalyzer.analyzer.model;

import com.plsqlanalyzer.parser.model.PlsqlUnitType;

import java.util.Objects;

public class CallGraphNode {
    private String id;
    private String baseId;          // SCHEMA.PACKAGE.PROC (without overload suffix)
    private String schemaName;
    private String packageName;
    private String procedureName;
    private PlsqlUnitType unitType;
    private String sourceFile;
    private int startLine;
    private int endLine;
    private String callType;        // INTERNAL, EXTERNAL, TRIGGER, DYNAMIC
    private boolean placeholder;    // true if node was inferred from a call but not found in parsed sources
    private String paramSignature;  // compact display: "3IN_3OUT" or "FUNCTION(IN V2,OUT NUM)"
    private int paramCount;

    public CallGraphNode() {}

    public CallGraphNode(String schemaName, String packageName, String procedureName) {
        this.schemaName = schemaName;
        this.packageName = packageName;
        this.procedureName = procedureName;
        this.id = buildId(schemaName, packageName, procedureName);
        this.baseId = this.id;
    }

    public static String buildId(String schema, String pkg, String proc) {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema).append(".");
        if (pkg != null && !pkg.isEmpty()) sb.append(pkg).append(".");
        if (proc != null) sb.append(proc);
        return sb.toString().toUpperCase();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getBaseId() { return baseId != null ? baseId : id; }
    public void setBaseId(String baseId) { this.baseId = baseId; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }

    public String getProcedureName() { return procedureName; }
    public void setProcedureName(String procedureName) { this.procedureName = procedureName; }

    public PlsqlUnitType getUnitType() { return unitType; }
    public void setUnitType(PlsqlUnitType unitType) { this.unitType = unitType; }

    public String getSourceFile() { return sourceFile; }
    public void setSourceFile(String sourceFile) { this.sourceFile = sourceFile; }

    public int getStartLine() { return startLine; }
    public void setStartLine(int startLine) { this.startLine = startLine; }

    public int getEndLine() { return endLine; }
    public void setEndLine(int endLine) { this.endLine = endLine; }

    public String getCallType() { return callType; }
    public void setCallType(String callType) { this.callType = callType; }

    public boolean isPlaceholder() { return placeholder; }
    public void setPlaceholder(boolean placeholder) { this.placeholder = placeholder; }

    public String getParamSignature() { return paramSignature; }
    public void setParamSignature(String paramSignature) { this.paramSignature = paramSignature; }

    public int getParamCount() { return paramCount; }
    public void setParamCount(int paramCount) { this.paramCount = paramCount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CallGraphNode that = (CallGraphNode) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
