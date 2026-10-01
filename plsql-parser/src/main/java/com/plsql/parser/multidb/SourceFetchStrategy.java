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
     * Fetch routine source by its catalog OID (PostgreSQL {@code pg_proc.oid}). This is how a
     * specific OVERLOAD is addressed unambiguously — the name-based {@link #fetchSource} can only
     * return one arbitrary overload. Dialects without a per-overload object id return null.
     */
    default String fetchSourceByOid(Connection conn, String oid) throws SQLException {
        return null;
    }

    /**
     * Fetch routine source by its catalog-unique specific name (DB2 {@code SYSCAT.ROUTINES.
     * SPECIFICNAME}), which distinguishes overloads that share a ROUTINENAME. Dialects without a
     * specific name return null.
     */
    default String fetchSourceBySpecificName(Connection conn, String specificName, String schema)
            throws SQLException {
        return null;
    }

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
        /** PostgreSQL {@code pg_proc.oid} (as a string), else null. Addresses one exact overload. */
        public final String oid;
        /** PostgreSQL {@code pg_get_function_identity_arguments(oid)} — the overload's signature. */
        public final String identityArgs;
        /** DB2 {@code SYSCAT.ROUTINES.SPECIFICNAME} — unique per overload, else null. */
        public final String specificName;

        public ObjectRef(String name, String type, String schema) {
            this(name, type, schema, null, null, null);
        }

        public ObjectRef(String name, String type, String schema,
                         String oid, String identityArgs, String specificName) {
            this.name         = name;
            this.type         = type;
            this.schema       = schema;
            this.oid          = oid;
            this.identityArgs = identityArgs;
            this.specificName = specificName;
        }

        @Override public String toString() {
            return schema + "." + name + " [" + type + "]";
        }
    }
}
