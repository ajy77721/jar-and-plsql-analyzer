package com.plsql.sqlserver.model;

import java.util.ArrayList;
import java.util.List;

public class SqlServerCallInfo {

    private String schema;
    private String name;
    private int    line;
    // Bug 8: call-site arguments (e.g. "EXEC dbo.usp_GetTotal @order_id, @total OUTPUT;"), so an
    // OUTPUT/OUT argument's direction is no longer silently discarded.
    private final List<CallArgInfo> args = new ArrayList<>();

    public String getName()   { return name; }
    public void   setName(String name)     { this.name = name; }
    public String getSchema() { return schema; }
    public void   setSchema(String schema) { this.schema = schema; }
    public int    getLine()   { return line; }
    public void   setLine(int line)        { this.line = line; }
    public List<CallArgInfo> getArgs() { return args; }

    public String qualifiedName() {
        return schema != null ? schema + "." + name : name;
    }

    /** One argument passed at a call site: its raw text/name and whether it is an OUTPUT/OUT arg. */
    public static class CallArgInfo {
        private String  text;
        private boolean output;

        public CallArgInfo() { }
        public CallArgInfo(String text, boolean output) { this.text = text; this.output = output; }

        public String  getText()   { return text; }
        public void    setText(String text) { this.text = text; }
        public boolean isOutput()  { return output; }
        public void    setOutput(boolean output) { this.output = output; }

        @Override
        public String toString() {
            return text + (output ? " OUTPUT" : "");
        }
    }
}
