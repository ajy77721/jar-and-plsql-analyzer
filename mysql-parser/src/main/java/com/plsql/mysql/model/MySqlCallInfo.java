package com.plsql.mysql.model;

/** A CALL statement found inside a MySQL stored procedure body. */
public class MySqlCallInfo {
    private String schema;
    private String name;
    private int    line;

    /**
     * Index into the owning routine's {@code exceptionHandlers} list when this CALL is inside a
     * {@code DECLARE ... HANDLER} body, else {@code null}. Without this, a handler's CALL is
     * indistinguishable from a main-body CALL in the flat top-level {@code calls} list (audit #9).
     */
    private Integer handlerIndex;

    public String getSchema()       { return schema; }
    public void   setSchema(String s) { this.schema = s; }

    public String getName()         { return name; }
    public void   setName(String n) { this.name = n; }

    public int  getLine()      { return line; }
    public void setLine(int l) { this.line = l; }

    public Integer getHandlerIndex()          { return handlerIndex; }
    public void    setHandlerIndex(Integer i) { this.handlerIndex = i; }

    /** Fully-qualified name for display / call-tree edges. */
    public String qualifiedName() {
        return (schema != null && !schema.isBlank()) ? schema + "." + name : name;
    }
}
