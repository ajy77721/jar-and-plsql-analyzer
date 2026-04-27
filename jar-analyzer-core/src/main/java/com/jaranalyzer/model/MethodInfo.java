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
}
