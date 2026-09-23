package com.plsql.db2.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code DECLARE (CONTINUE|EXIT|UNDO) HANDLER FOR <conditions> <action>} handler.
 */
public class Db2ExceptionHandlerInfo {

    public enum Kind { CONTINUE, EXIT, UNDO }

    private Kind kind;
    private List<String> conditions = new ArrayList<>();
    private int line;

    public Kind getKind() { return kind; }
    public void setKind(Kind kind) { this.kind = kind; }
    public List<String> getConditions() { return conditions; }
    public void setConditions(List<String> conditions) { this.conditions = conditions; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
}
