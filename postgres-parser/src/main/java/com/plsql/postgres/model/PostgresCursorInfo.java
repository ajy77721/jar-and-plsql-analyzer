package com.plsql.postgres.model;

import java.util.ArrayList;
import java.util.List;

public class PostgresCursorInfo {

    private String name;
    private String selectText;
    private boolean withHold;
    private int    line;
    /** OPEN/FETCH/CLOSE operations seen against this cursor after its DECLARE. */
    private final List<Op> operations = new ArrayList<>();
    /**
     * Bug 4 (§dynamic-cursor-audit): parameters of a parameterized cursor declaration
     * (e.g. {@code CURSOR cur(p_id INT) FOR SELECT ... WHERE id = p_id}). Reuses
     * {@link PostgresParamInfo} — the same model the routine's own parameter list uses —
     * rather than duplicating the parameter shape (mirrors Oracle's cursor
     * {@code ParameterInfo} reuse in plsql-parser's {@code CursorInfo}).
     */
    private final List<PostgresParamInfo> parameters = new ArrayList<>();

    public String  getName()       { return name; }
    public void    setName(String name)           { this.name = name; }
    public String  getSelectText() { return selectText; }
    public void    setSelectText(String selectText) { this.selectText = selectText; }
    public boolean isWithHold()    { return withHold; }
    public void    setWithHold(boolean withHold)  { this.withHold = withHold; }
    public int     getLine()       { return line; }
    public void    setLine(int line)              { this.line = line; }
    public List<Op> getOperations() { return operations; }
    public List<PostgresParamInfo> getParameters() { return parameters; }

    /** One OPEN/FETCH/CLOSE occurrence: its kind and source line. */
    public static class Op {
        private final String kind;
        private final int line;

        public Op(String kind, int line) { this.kind = kind; this.line = line; }
        public String getKind() { return kind; }
        public int getLine() { return line; }
    }
}
