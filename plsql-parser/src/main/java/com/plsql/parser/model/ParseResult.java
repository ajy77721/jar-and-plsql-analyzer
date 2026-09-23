package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ParseResult {
    private String fileName;
    private long parseTimeMs;
    private List<String> errors = new ArrayList<>();
    private List<ParsedObject> objects = new ArrayList<>();

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public long getParseTimeMs() { return parseTimeMs; }
    public void setParseTimeMs(long parseTimeMs) { this.parseTimeMs = parseTimeMs; }
    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
    public List<ParsedObject> getObjects() { return objects; }
    public void setObjects(List<ParsedObject> objects) { this.objects = objects; }
}
