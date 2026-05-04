package com.jaranalyzer.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class AnnotationInfo {
    private String name;
    private Map<String, Object> attributes = new LinkedHashMap<>();

    public AnnotationInfo() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public Map<String, Object> getAttributes() { return attributes; }
    public void setAttributes(Map<String, Object> attributes) { this.attributes = attributes; }
}
