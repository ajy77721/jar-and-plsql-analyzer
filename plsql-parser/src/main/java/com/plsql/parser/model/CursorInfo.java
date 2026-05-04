package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class CursorInfo {
    private String name;
    private String query;
    private int line;
    private int lineEnd;
    private boolean forLoop;
    private boolean refCursor;
    private List<ParameterInfo> parameters = new ArrayList<>();
    private List<String> tables = new ArrayList<>();

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getQuery() { return query; }
    public void setQuery(String query) { this.query = query; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public boolean isForLoop() { return forLoop; }
    public void setForLoop(boolean forLoop) { this.forLoop = forLoop; }
    public boolean isRefCursor() { return refCursor; }
    public void setRefCursor(boolean refCursor) { this.refCursor = refCursor; }
    public List<ParameterInfo> getParameters() { return parameters; }
    public void setParameters(List<ParameterInfo> parameters) { this.parameters = parameters; }
    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }
}
