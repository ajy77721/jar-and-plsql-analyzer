package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ParameterInfo {
    private String name;
    private String dataType;
    private String direction;
    private String defaultValue;
    private int line;
    // Task #26: a REF CURSOR parameter is the Oracle way for a procedure/function to hand a result
    // set back to its caller. isRefCursor is true when the declared type is a REF CURSOR
    // (SYS_REFCURSOR, the literal REF CURSOR, or a locally-declared `TYPE x IS REF CURSOR`);
    // outRefCursor additionally requires the mode to include OUT (a pure IN ref cursor is an input,
    // not a result set). Both default false, so every non-Oracle dialect and every ordinary
    // parameter keeps its previous shape.
    private boolean refCursor;
    private boolean outRefCursor;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }
    public String getDirection() { return direction; }
    public void setDirection(String direction) { this.direction = direction; }
    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public boolean isRefCursor() { return refCursor; }
    public void setRefCursor(boolean refCursor) { this.refCursor = refCursor; }
    public boolean isOutRefCursor() { return outRefCursor; }
    public void setOutRefCursor(boolean outRefCursor) { this.outRefCursor = outRefCursor; }
}
