package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One bytecode invocation site.
 *
 * <p><b>Why the {@link JsonInclude} annotations matter here more than anywhere else:</b> this is by
 * far the most numerous object in {@code analysis.json}. Measured across the real analyses,
 * {@code methods.invocations} is <b>31-78% of the whole document</b> — 8-21x all decompiled source
 * combined — and most of each object is empty. Of 12,744 invocations in one analysis, the four
 * lambda fields are unset in <b>99.0%</b>, {@code receiverFieldName} in 95.2%, and
 * {@code recentStringArgs} in 78.0%. Serialising those absences costs <b>4.7 MB across five
 * analyses (26.9%)</b> for no information.
 *
 * <p>The annotations are deliberately per-field rather than a class-level {@code NON_DEFAULT}:
 * that would also drop {@code opcode} and {@code lineNumber} when they are 0, and those are real
 * values rather than absences. {@code lambdaInvocation} needs {@code NON_DEFAULT} specifically
 * because it is a primitive {@code boolean} — {@code NON_NULL} does not drop {@code false}.
 *
 * <p>Consumers already tolerate the fields being absent: every read in
 * {@code summary-codeview.js} is truthiness-based ({@code m.invocations || []},
 * {@code inv.lineNumber || 0}), with no {@code in} or {@code hasOwnProperty} test anywhere.
 */
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

    /**
     * A non-literal argument source (ALOAD of a local var/param, or GETFIELD/GETSTATIC of a
     * field) captured between the previous method call and this one — the variable/field
     * counterpart to recentStringArgs' LDC-literal tracking. Used when a collection-name
     * argument was a variable rather than a literal, so detectors can still report WHICH
     * variable/field was used instead of silently finding nothing.
     */
    public record DynamicArgSource(String kind, String name, String ownerClass) {}

    /** Non-literal (variable/field) argument sources loaded between the previous call and this one. */
    private java.util.List<DynamicArgSource> dynamicArgSources;

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
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public java.util.List<String> getRecentStringArgs() { return recentStringArgs; }
    public void setRecentStringArgs(java.util.List<String> recentStringArgs) { this.recentStringArgs = recentStringArgs; }
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getReceiverFieldName() { return receiverFieldName; }
    public void setReceiverFieldName(String receiverFieldName) { this.receiverFieldName = receiverFieldName; }
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isLambdaInvocation() { return isLambdaInvocation; }
    public void setLambdaInvocation(boolean lambdaInvocation) { this.isLambdaInvocation = lambdaInvocation; }
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getLambdaTargetClass() { return lambdaTargetClass; }
    public void setLambdaTargetClass(String lambdaTargetClass) { this.lambdaTargetClass = lambdaTargetClass; }
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getLambdaTargetMethod() { return lambdaTargetMethod; }
    public void setLambdaTargetMethod(String lambdaTargetMethod) { this.lambdaTargetMethod = lambdaTargetMethod; }
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getLambdaTargetDescriptor() { return lambdaTargetDescriptor; }
    public void setLambdaTargetDescriptor(String lambdaTargetDescriptor) { this.lambdaTargetDescriptor = lambdaTargetDescriptor; }
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public java.util.List<DynamicArgSource> getDynamicArgSources() { return dynamicArgSources; }
    public void setDynamicArgSources(java.util.List<DynamicArgSource> dynamicArgSources) { this.dynamicArgSources = dynamicArgSources; }
}
