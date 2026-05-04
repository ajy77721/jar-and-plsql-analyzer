package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JarAnalysis {
    private String jarName;
    private String projectName;
    private long jarSize;
    private String analyzedAt;
    private int totalClasses;
    private int totalEndpoints;
    private String analysisMode;         // "STATIC" or "CORRECTED"
    private String basePackage;          // filter applied at upload time (null = no filter)
    private String correctionAppliedAt;  // ISO timestamp of last correction merge
    private int correctionCount;         // number of corrections applied
    private int claudeIteration;         // how many times full Claude scan ran
    private List<ClassInfo> classes = new ArrayList<>();
    private List<EndpointInfo> endpoints = new ArrayList<>();

    public JarAnalysis() {}

    public String getJarName() { return jarName; }
    public void setJarName(String jarName) { this.jarName = jarName; }
    public String getProjectName() { return projectName; }
    public void setProjectName(String projectName) { this.projectName = projectName; }
    public long getJarSize() { return jarSize; }
    public void setJarSize(long jarSize) { this.jarSize = jarSize; }
    public String getAnalyzedAt() { return analyzedAt; }
    public void setAnalyzedAt(String analyzedAt) { this.analyzedAt = analyzedAt; }
    public int getTotalClasses() { return totalClasses; }
    public void setTotalClasses(int totalClasses) { this.totalClasses = totalClasses; }
    public int getTotalEndpoints() { return totalEndpoints; }
    public void setTotalEndpoints(int totalEndpoints) { this.totalEndpoints = totalEndpoints; }
    public String getAnalysisMode() { return analysisMode; }
    public void setAnalysisMode(String analysisMode) { this.analysisMode = analysisMode; }
    public String getBasePackage() { return basePackage; }
    public void setBasePackage(String basePackage) { this.basePackage = basePackage; }
    public String getCorrectionAppliedAt() { return correctionAppliedAt; }
    public void setCorrectionAppliedAt(String correctionAppliedAt) { this.correctionAppliedAt = correctionAppliedAt; }
    public int getCorrectionCount() { return correctionCount; }
    public void setCorrectionCount(int correctionCount) { this.correctionCount = correctionCount; }
    public int getClaudeIteration() { return claudeIteration; }
    public void setClaudeIteration(int claudeIteration) { this.claudeIteration = claudeIteration; }
    public List<ClassInfo> getClasses() { return classes; }
    public void setClasses(List<ClassInfo> classes) { this.classes = classes; }
    public List<EndpointInfo> getEndpoints() { return endpoints; }
    public void setEndpoints(List<EndpointInfo> endpoints) { this.endpoints = endpoints; }
}
