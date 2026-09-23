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
    /** OPEN/FETCH/CLOSE (and any other post-DECLARE) operations seen against this cursor. */
    private List<StmtOp> stmtOperations = new ArrayList<>();

    /** One post-DECLARE operation on a cursor: its kind (OPEN/FETCH/CLOSE) and source line. */
    public static class StmtOp {
        private String operation;
        private int line;

        public StmtOp() {}
        public StmtOp(String operation, int line) { this.operation = operation; this.line = line; }

        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        public int getLine() { return line; }
        public void setLine(int line) { this.line = line; }
    }

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

    public List<StmtOp> getStmtOperations() { return stmtOperations; }
    public void setStmtOperations(List<StmtOp> stmtOperations) { this.stmtOperations = stmtOperations; }
}
