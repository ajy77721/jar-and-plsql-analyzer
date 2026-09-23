package com.plsql.parser.multidb;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Abstracts how stored procedure / function source is fetched from a database.
 * Each database type implements its own strategy.
 */
public interface SourceFetchStrategy {

    /**
     * Fetch source text for the given object from the database connection.
     *
     * @param conn       an active JDBC connection to the target database
     * @param objectName procedure or function name (case-insensitive)
     * @param objectType PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, etc.
     * @param schema     owning schema / database name (may be null to search all accessible)
     * @return full DDL / source text, or null if not found
     * @throws SQLException on JDBC errors
     */
    String fetchSource(Connection conn, String objectName, String objectType, String schema)
            throws SQLException;

    /**
     * List all stored procedure / function names visible to this connection in the given schema.
     */
    java.util.List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException;

    /**
     * True when the fetched {@code source} looks truncated (cut off mid-body) for THIS dialect's
     * own fetch mechanism, meaning the parser's errors should be suppressed rather than reported.
     * Each dialect decides this on its own — there is deliberately no shared/cross-dialect logic:
     * the way source is retrieved (Oracle ALL_SOURCE line chunks vs. MySQL SHOW CREATE vs. DB2
     * SYSCAT vs. Postgres pg_get_functiondef) determines whether truncation is even possible.
     */
    boolean isTruncated(String source);

    /** Lightweight object descriptor returned by listObjects(). */
    class ObjectRef {
        public final String name;
        public final String type;
        public final String schema;

        public ObjectRef(String name, String type, String schema) {
            this.name   = name;
            this.type   = type;
            this.schema = schema;
        }

        @Override public String toString() {
            return schema + "." + name + " [" + type + "]";
        }
    }
}
