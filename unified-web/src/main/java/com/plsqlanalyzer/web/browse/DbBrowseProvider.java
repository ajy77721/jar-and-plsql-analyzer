package com.plsqlanalyzer.web.browse;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Dialect-neutral abstraction for the "Database Browse" feature.
 *
 * <p>Implementations run dialect-specific catalog SQL and return plain
 * Map/List structures whose field names mirror the Oracle response shapes
 * produced by {@code DatabaseController}, so the UI renders identically
 * regardless of the underlying database.</p>
 *
 * <p>All methods receive an already-open {@link Connection}; the caller owns
 * its lifecycle (open/close). A {@code null} schema means "search across all
 * visible schemas".</p>
 */
public interface DbBrowseProvider {

    /** PL/SQL-style objects (procedures, functions, packages...) visible in the schema. */
    List<Map<String, Object>> listObjects(Connection c, String schema) throws Exception;

    /** Source text (routine/view definition) for a single object, or {@code null}. */
    String fetchSource(Connection c, String schema, String objectName) throws Exception;

    /** Columns / constraints / indexes / triggers for a table or view. */
    Map<String, Object> tableInfo(Connection c, String schema, String tableName) throws Exception;

    /** Locate an object by name (or schema.name). Default: not found. */
    default Map<String, Object> findObject(Connection c, String input) throws Exception {
        return Map.of("found", false, "input", input);
    }

    /** Package-style grouping. Databases without packages return standalone routines. */
    default List<Map<String, Object>> listPackages(Connection c, String schema) throws Exception {
        return List.of();
    }
}
