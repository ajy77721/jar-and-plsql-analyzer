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

    /** CALL-005: set only when this PUT/PUT_STATIC access directly stores the result of an
     *  invokedynamic (a captured lambda/method reference) — e.g. {@code this.loader = repo::findAll;}.
     *  Mirrors MethodCallInfo's lambdaTargetClass/Method/Descriptor naming so CallGraphIndex can
     *  build a per-class field-name -> method-reference-target map (see IndexedClass.fieldLambdaTargets),
     *  analogous to the existing injectedFields DI map. Null for every ordinary field write. */
    private String lambdaTargetClass;
    private String lambdaTargetMethod;
    private String lambdaTargetDescriptor;

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
    public String getLambdaTargetClass() { return lambdaTargetClass; }
    public void setLambdaTargetClass(String lambdaTargetClass) { this.lambdaTargetClass = lambdaTargetClass; }
    public String getLambdaTargetMethod() { return lambdaTargetMethod; }
    public void setLambdaTargetMethod(String lambdaTargetMethod) { this.lambdaTargetMethod = lambdaTargetMethod; }
    public String getLambdaTargetDescriptor() { return lambdaTargetDescriptor; }
    public void setLambdaTargetDescriptor(String lambdaTargetDescriptor) { this.lambdaTargetDescriptor = lambdaTargetDescriptor; }
}
