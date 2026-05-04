package com.jaranalyzer.model;

/**
 * Represents a field access (read/write) in a method's bytecode.
 * Captures GETFIELD, PUTFIELD, GETSTATIC, PUTSTATIC instructions.
 */
public class FieldAccessInfo {
    private String ownerClass;
    private String fieldName;
    private String fieldType;
    private String accessType;  // GET, PUT, GET_STATIC, PUT_STATIC
    private int lineNumber;

    public FieldAccessInfo() {}

    public String getOwnerClass() { return ownerClass; }
    public void setOwnerClass(String ownerClass) { this.ownerClass = ownerClass; }
    public String getFieldName() { return fieldName; }
    public void setFieldName(String fieldName) { this.fieldName = fieldName; }
    public String getFieldType() { return fieldType; }
    public void setFieldType(String fieldType) { this.fieldType = fieldType; }
    public String getAccessType() { return accessType; }
    public void setAccessType(String accessType) { this.accessType = accessType; }
    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
