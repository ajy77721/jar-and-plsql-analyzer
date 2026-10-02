package com.plsql.parser.multidb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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

    /**
     * List the scheduled jobs visible to this connection in the given schema — Oracle
     * DBMS_SCHEDULER ({@code ALL_SCHEDULER_JOBS}) and legacy DBMS_JOB ({@code ALL_JOBS}),
     * SQL Server Agent ({@code msdb.dbo.sysjobs}), pg_cron ({@code cron.job}) or MySQL
     * ({@code information_schema.EVENTS}).
     *
     * <p>Dialects/catalogs that are not present (pg_cron not installed, no privileges, a
     * non-scheduler database) return an empty list rather than throwing — the crawl must
     * degrade honestly. Implementations should still declare {@code throws SQLException} for
     * genuine driver failures; callers wrap the whole call and treat failure as "no jobs".
     */
    default List<JobRef> listJobs(Connection conn, String schema) throws SQLException {
        return java.util.Collections.emptyList();
    }

    /**
     * Fetch a job's action source by name when the catalog stores it separately from the job row
     * (Oracle {@code USER_SOURCE}/{@code ALL_SOURCE} type {@code 'JOB'}). Returns null when the
     * dialect has no such source or the job has none.
     */
    default String fetchJobAction(Connection conn, String jobName, String schema)
            throws SQLException {
        return null;
    }

    /** One scheduled job discovered from a dialect's scheduler catalog. */
    class JobRef {
        public final String name;
        public final String schema;
        /** Human-readable schedule: repeat_interval / cron expression / SQL Agent schedule / EVERY … */
        public final String schedule;
        /** TRUE enabled, FALSE disabled, null unknown. */
        public final Boolean enabled;
        /** The routine the job invokes (bare name or PACKAGE.MEMBER), or null when not statically known. */
        public final String target;
        /** Inline action/program source when the catalog carries it (SQL Agent step command, pg_cron command). */
        public final String action;
        /** Node type: "JOB" for Oracle/SQL Server/Postgres, "EVENT" for MySQL. */
        public final String type;

        public JobRef(String name, String schema, String schedule, Boolean enabled,
                      String target, String action, String type) {
            this.name = name;
            this.schema = schema;
            this.schedule = schedule;
            this.enabled = enabled;
            this.target = target;
            this.action = action;
            this.type = type == null ? "JOB" : type;
        }

        @Override public String toString() {
            return schema + "." + name + " [" + schedule + "]";
        }
    }

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
        /**
         * DB2 {@code SYSCAT.ROUTINES.ROUTINEMODULENAME} — the owning module when this routine is a
         * module member, else null (task #28). Used as the package slot so the member keys as
         * MODULE.SUB.
         */
        public final String moduleName;

        public ObjectRef(String name, String type, String schema) {
            this(name, type, schema, null, null, null, null);
        }

        public ObjectRef(String name, String type, String schema,
                         String oid, String identityArgs, String specificName) {
            this(name, type, schema, oid, identityArgs, specificName, null);
        }

        public ObjectRef(String name, String type, String schema,
                         String oid, String identityArgs, String specificName, String moduleName) {
            this.name         = name;
            this.type         = type;
            this.schema       = schema;
            this.oid          = oid;
            this.identityArgs = identityArgs;
            this.specificName = specificName;
            this.moduleName   = moduleName;
        }

        @Override public String toString() {
            return schema + "." + name + " [" + type + "]";
        }
    }
}
