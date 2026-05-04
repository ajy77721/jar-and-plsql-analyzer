package com.jaranalyzer.model;

public class MethodCallInfo {
    private String ownerClass;
    private String methodName;
    private String descriptor;
    private String returnType;
    private int opcode;
    private int lineNumber;

    /** LDC string constants loaded between the previous method call and this one.
     *  Used to associate string arguments (e.g. collection names) with specific invocations. */
    private java.util.List<String> recentStringArgs;

    /** Field on 'this' whose value was loaded just before this call (GETFIELD → INVOKE sequence).
     *  Used to link interface method calls to the DI-injected field that holds the reference. */
    private String receiverFieldName;

    /** Set when this invocation originates from an invokedynamic (lambda / method reference).
     *  The lambda target fields carry the resolved method handle from the bootstrap args. */
    private boolean isLambdaInvocation;
    private String lambdaTargetClass;
    private String lambdaTargetMethod;
    private String lambdaTargetDescriptor;

    public MethodCallInfo() {}

    public String getOwnerClass() { return ownerClass; }
    public void setOwnerClass(String ownerClass) { this.ownerClass = ownerClass; }
    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }
    public String getDescriptor() { return descriptor; }
    public void setDescriptor(String descriptor) { this.descriptor = descriptor; }
    public String getReturnType() { return returnType; }
    public void setReturnType(String returnType) { this.returnType = returnType; }
    public int getOpcode() { return opcode; }
    public void setOpcode(int opcode) { this.opcode = opcode; }
    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
    public java.util.List<String> getRecentStringArgs() { return recentStringArgs; }
    public void setRecentStringArgs(java.util.List<String> recentStringArgs) { this.recentStringArgs = recentStringArgs; }
    public String getReceiverFieldName() { return receiverFieldName; }
    public void setReceiverFieldName(String receiverFieldName) { this.receiverFieldName = receiverFieldName; }
    public boolean isLambdaInvocation() { return isLambdaInvocation; }
    public void setLambdaInvocation(boolean lambdaInvocation) { this.isLambdaInvocation = lambdaInvocation; }
    public String getLambdaTargetClass() { return lambdaTargetClass; }
    public void setLambdaTargetClass(String lambdaTargetClass) { this.lambdaTargetClass = lambdaTargetClass; }
    public String getLambdaTargetMethod() { return lambdaTargetMethod; }
    public void setLambdaTargetMethod(String lambdaTargetMethod) { this.lambdaTargetMethod = lambdaTargetMethod; }
    public String getLambdaTargetDescriptor() { return lambdaTargetDescriptor; }
    public void setLambdaTargetDescriptor(String lambdaTargetDescriptor) { this.lambdaTargetDescriptor = lambdaTargetDescriptor; }
}
