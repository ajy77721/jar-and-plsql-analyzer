package com.jaranalyzer.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MethodInfo {
    private String name;
    private String descriptor;
    private String returnType;
    private List<ParameterInfo> parameters = new ArrayList<>();
    private List<AnnotationInfo> annotations = new ArrayList<>();
    private List<MethodCallInfo> invocations = new ArrayList<>();

    /**
     * Ordinals into the document's {@code invocationPool}, present instead of {@link #invocations}
     * in a pooled document.
     *
     * <p>{@code PersistenceService.rehydrate} resolves these back into {@link #invocations} on
     * load, so nothing downstream sees them. It must stay declared even though no application code
     * reads it: the mapper ignores unknown properties, so without this field a pooled document
     * would deserialise with an <b>empty</b> invocations list and no error at all.
     */
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY)
    private List<Integer> invocationRefs = new ArrayList<>();
    private int accessFlags;
    private String httpMethod;
    private String path;
    private List<String> stringLiterals = new ArrayList<>();

    // Enhanced: field accesses (GETFIELD, PUTFIELD, GETSTATIC, PUTSTATIC)
    private List<FieldAccessInfo> fieldAccesses = new ArrayList<>();

    // Enhanced: local variable table (name -> type)
    private Map<String, String> localVariables = new LinkedHashMap<>();

    // Enhanced: line number range (first and last line in source/bytecode)
    private int startLine;
    private int endLine;

    /**
     * CALL-018: this method's own return value, resolved ONLY when the whole method has exactly
     * one {@code ARETURN}, directly fed by an LDC string literal (never guessed across multiple
     * return paths or a return computed from a further call/field/param) — see
     * {@code BytecodeClassParser}'s whole-method ARETURN scan. Null for every method that doesn't
     * meet that narrow single-literal-return shape.
     */
    private String singleLiteralReturnValue;

    /**
     * CALL-019: this method's own return value, resolved ONLY when the whole method has exactly
     * one {@code ARETURN}, and that {@code ARETURN} is directly (adjacently) fed by ANOTHER
     * method call's own return value — a pure pass-through, e.g. {@code return c.foo();} — with
     * NO literal, field, param, or ASTORE in between. Records that callee's owner class + simple
     * method name so {@code CollectionResolver} can walk one more hop down (bounded by
     * {@code CollectionResolver.MAX_HOP_DEPTH}, reusing CALL-001/002's own convention) to check
     * whether THAT method resolves to a literal (or is itself another pass-through). Null for
     * every method that doesn't meet this narrow single-pass-through shape, including when the
     * method already resolved via {@link #singleLiteralReturnValue}.
     */
    private String singleReturnPassThroughOwner;
    private String singleReturnPassThroughMethod;

    public MethodInfo() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescriptor() { return descriptor; }
    public void setDescriptor(String descriptor) { this.descriptor = descriptor; }
    public String getReturnType() { return returnType; }
    public void setReturnType(String returnType) { this.returnType = returnType; }
    public List<ParameterInfo> getParameters() { return parameters; }
    public void setParameters(List<ParameterInfo> parameters) { this.parameters = parameters; }
    public List<AnnotationInfo> getAnnotations() { return annotations; }
    public void setAnnotations(List<AnnotationInfo> annotations) { this.annotations = annotations; }
    public List<MethodCallInfo> getInvocations() { return invocations; }
    public void setInvocations(List<MethodCallInfo> invocations) { this.invocations = invocations; }
    public List<Integer> getInvocationRefs() { return invocationRefs; }
    public void setInvocationRefs(List<Integer> invocationRefs) { this.invocationRefs = invocationRefs; }
    public int getAccessFlags() { return accessFlags; }
    public void setAccessFlags(int accessFlags) { this.accessFlags = accessFlags; }
    public String getHttpMethod() { return httpMethod; }
    public void setHttpMethod(String httpMethod) { this.httpMethod = httpMethod; }
    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
    public List<String> getStringLiterals() { return stringLiterals; }
    public void setStringLiterals(List<String> stringLiterals) { this.stringLiterals = stringLiterals; }

    public List<FieldAccessInfo> getFieldAccesses() { return fieldAccesses; }
    public void setFieldAccesses(List<FieldAccessInfo> fieldAccesses) { this.fieldAccesses = fieldAccesses; }
    public Map<String, String> getLocalVariables() { return localVariables; }
    public void setLocalVariables(Map<String, String> localVariables) { this.localVariables = localVariables; }
    public int getStartLine() { return startLine; }
    public void setStartLine(int startLine) { this.startLine = startLine; }
    public int getEndLine() { return endLine; }
    public void setEndLine(int endLine) { this.endLine = endLine; }
    public String getSingleLiteralReturnValue() { return singleLiteralReturnValue; }
    public void setSingleLiteralReturnValue(String singleLiteralReturnValue) { this.singleLiteralReturnValue = singleLiteralReturnValue; }
    public String getSingleReturnPassThroughOwner() { return singleReturnPassThroughOwner; }
    public String getSingleReturnPassThroughMethod() { return singleReturnPassThroughMethod; }
    public void setSingleReturnPassThrough(String owner, String method) {
        this.singleReturnPassThroughOwner = owner;
        this.singleReturnPassThroughMethod = method;
    }
}
