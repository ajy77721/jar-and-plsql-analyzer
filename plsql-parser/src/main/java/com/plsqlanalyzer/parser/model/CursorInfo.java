package com.plsqlanalyzer.parser.model;

/**
 * Represents a cursor declaration or cursor usage in PL/SQL code.
 * Tracks: explicit cursors (CURSOR c IS SELECT...), REF CURSORs (SYS_REFCURSOR),
 * cursor FOR loops, and OPEN/FETCH/CLOSE operations.
 */
public class CursorInfo {
    private String cursorName;
    private String cursorType;   // EXPLICIT, REF_CURSOR, FOR_LOOP, OPEN_FOR
    private String operation;    // DECLARE, OPEN, FETCH, CLOSE, FOR_LOOP
    private String queryText;    // The SELECT statement (for EXPLICIT and FOR_LOOP)
    private int lineNumber;

    public CursorInfo() {}

    public CursorInfo(String cursorName, String cursorType, String operation, String queryText, int lineNumber) {
        this.cursorName = cursorName;
        this.cursorType = cursorType;
        this.operation = operation;
        this.queryText = queryText;
        this.lineNumber = lineNumber;
    }

    public String getCursorName() { return cursorName; }
    public void setCursorName(String cursorName) { this.cursorName = cursorName; }

    public String getCursorType() { return cursorType; }
    public void setCursorType(String cursorType) { this.cursorType = cursorType; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public String getQueryText() { return queryText; }
    public void setQueryText(String queryText) { this.queryText = queryText; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
