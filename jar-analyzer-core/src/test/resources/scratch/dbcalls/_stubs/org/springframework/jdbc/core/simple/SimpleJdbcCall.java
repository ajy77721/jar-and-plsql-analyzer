package org.springframework.jdbc.core.simple;

/** Minimal same-FQN stub of Spring's SimpleJdbcCall fluent API. */
public class SimpleJdbcCall {
    public SimpleJdbcCall(Object dataSource) {}

    public SimpleJdbcCall withCatalogName(String catalogName) { return this; }

    public SimpleJdbcCall withSchemaName(String schemaName) { return this; }

    public SimpleJdbcCall withProcedureName(String procedureName) { return this; }

    public SimpleJdbcCall withFunctionName(String functionName) { return this; }

    public Object execute() { return null; }

    public Object executeFunction() { return null; }
}
