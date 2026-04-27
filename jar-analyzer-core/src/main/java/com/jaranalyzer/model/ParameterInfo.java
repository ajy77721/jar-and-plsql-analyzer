package com.jaranalyzer.model;

import java.util.ArrayList;
import java.util.List;

public class ParameterInfo {
    private String name;
    private String type;
    private List<AnnotationInfo> annotations = new ArrayList<>();

    public ParameterInfo() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public List<AnnotationInfo> getAnnotations() { return annotations; }
    public void setAnnotations(List<AnnotationInfo> annotations) { this.annotations = annotations; }
}
