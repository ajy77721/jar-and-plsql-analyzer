package com.plsql.mysql.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code DECLARE ... HANDLER FOR ...} block inside a MySQL routine body.
 *
 * <p>Mirrors what the grammar already parses (`declareHandler`/`handlerKind`/
 * `handlerConditionList`, {@code MySqlParser.g4}) but that, until now, the visitor never turned
 * into a model — handler bodies leaked into the flat top-level statement/call lists
 * indistinguishable from main-body code (audit #9).
 */
public class MySqlExceptionHandlerInfo {

    public enum Kind { CONTINUE, EXIT, UNDO }

    private Kind kind;
    /** e.g. ["SQLEXCEPTION"], ["NOT FOUND"], ["SQLSTATE '02000'"], or a named condition. */
    private final List<String> conditions = new ArrayList<>();
    private int line;
    /**
     * Index (into the same {@code getExceptionHandlers()} list) of the handler this one is
     * lexically nested within, or {@code null} if this is a top-level handler. Reconstructs
     * ancestry for nested {@code DECLARE HANDLER} blocks (a handler whose action is itself a
     * nested compound block that declares another handler), mirroring Oracle's
     * {@code ExceptionHandlerInfo.parentHandler}.
     */
    private Integer parentHandlerIndex;

    public Kind getKind() { return kind; }
    public void setKind(Kind kind) { this.kind = kind; }

    public List<String> getConditions() { return conditions; }

    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }

    public Integer getParentHandlerIndex() { return parentHandlerIndex; }
    public void setParentHandlerIndex(Integer parentHandlerIndex) { this.parentHandlerIndex = parentHandlerIndex; }
}
