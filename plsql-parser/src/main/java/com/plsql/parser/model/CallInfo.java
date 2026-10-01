package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class CallInfo {
    private String type;
    private String packageName;
    private String name;
    private String schema;
    private int line;
    private int arguments;
    /**
     * Coarse type hint per call argument (e.g. NUMBER for a numeric literal, VARCHAR2 for a
     * string literal; null where the expression type is not statically known), used to match the
     * call against a declared overload SIGNATURE. Additive and empty for every existing call →
     * callers that never populate it serialise byte-identically (NON_EMPTY drops it).
     */
    private List<String> argTypes;
    /**
     * Which subprogram within a package this call originated from, set ONLY when this item was
     * produced by {@code DependencyCrawler}'s package-as-entry-point aggregation (a bare package
     * name crawled as a single {@code FlowNode} — see the {@code targetObj} branch there). Null
     * for every other crawl (single proc/function entry points), so existing single-proc output
     * stays byte-identical.
     */
    private String subprogramName;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getArguments() { return arguments; }
    public void setArguments(int arguments) { this.arguments = arguments; }
    public List<String> getArgTypes() { return argTypes; }
    public void setArgTypes(List<String> argTypes) { this.argTypes = argTypes; }
    public String getSubprogramName() { return subprogramName; }
    public void setSubprogramName(String subprogramName) { this.subprogramName = subprogramName; }
}
