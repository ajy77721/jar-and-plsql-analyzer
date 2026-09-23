package com.plsql.sqlserver.model;

import java.util.ArrayList;
import java.util.List;

/** A DECLARE ... CURSOR captured from a routine body, plus its subsequent lifecycle
 *  statements (OPEN/FETCH/CLOSE/DEALLOCATE) tracked against the same cursor by name. */
public class SqlServerCursorInfo {
    private String name;
    private String query;
    private int line;
    private List<CursorStmtOp> stmtOperations = new ArrayList<>();

    public String getName() { return name; }
    public void setName(String v) { this.name = v; }
    public String getQuery() { return query; }
    public void setQuery(String v) { this.query = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }

    public List<CursorStmtOp> getStmtOperations() { return stmtOperations; }
    public void setStmtOperations(List<CursorStmtOp> v) { this.stmtOperations = v; }

    public void addStmtOperation(String operation, int line) {
        CursorStmtOp op = new CursorStmtOp();
        op.setOperation(operation);
        op.setLine(line);
        stmtOperations.add(op);
    }

    /** One lifecycle operation recorded against a cursor: OPEN, FETCH, CLOSE, DEALLOCATE
     *  (DECLARE itself is captured separately via {@link #getLine()} / the declaring statement). */
    public static class CursorStmtOp {
        private String operation;
        private int line;

        public String getOperation() { return operation; }
        public void setOperation(String v) { this.operation = v; }
        public int getLine() { return line; }
        public void setLine(int v) { this.line = v; }
    }
}
