package com.plsql.parser.multidb;

import com.plsql.parser.model.ParseResult;

/**
 * Parses stored procedure / function source text into the standard ParseResult model.
 * Implemented per DB dialect (MySQL, SQL Server).
 * Oracle continues to use the ANTLR-based PlSqlParserEngine.
 */
public interface StoredProcedureParser {

    /**
     * Parse source text and return a populated ParseResult.
     *
     * @param source     full CREATE PROCEDURE / FUNCTION DDL text
     * @param objectName hint for naming when signature extraction fails
     * @param schema     owning schema (may be null)
     * @return ParseResult with at least one ParsedObject; never null
     */
    ParseResult parse(String source, String objectName, String schema);
}
