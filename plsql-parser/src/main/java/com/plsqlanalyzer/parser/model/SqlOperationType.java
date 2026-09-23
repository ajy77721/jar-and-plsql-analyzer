package com.plsqlanalyzer.parser.model;

public enum SqlOperationType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    MERGE,
    /**
     * CREATE / ALTER / DROP / TRUNCATE / CREATE INDEX found inside PL/SQL.
     *
     * <p>Added late: DDL was previously not detected at all, so a procedure that ALTERed or
     * TRUNCATEd a table left no trace on that table's page. Safe to add — no switch over this enum
     * exists in Java. The browser has one hard-coded list of operations
     * (plsql/js/claude.js: ['SELECT','INSERT','UPDATE','DELETE','MERGE']) which must gain DDL or
     * those operations stay invisible there.
     */
    DDL
}
