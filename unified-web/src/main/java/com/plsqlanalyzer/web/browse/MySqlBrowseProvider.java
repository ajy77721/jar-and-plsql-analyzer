package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * MySQL implementation of {@link DbBrowseProvider} using {@code information_schema} (routines,
 * tables, columns, statistics, triggers, views). SQL patterns mirror the crawler's
 * {@code MySqlSourceFetchStrategy} and {@code MySqlTableEnricher}.
 *
 * <p>MySQL has no packages — {@link #listPackages} returns the standalone routines as flat
 * entries, same convention as {@link SqlServerBrowseProvider}. When {@code schema} is null,
 * queries search across all visible (non-system) schemas.
 */
public class MySqlBrowseProvider implements DbBrowseProvider {

    private static final String SYSTEM_SCHEMAS =
            "'mysql','information_schema','performance_schema','sys'";

    @Override
    public List<Map<String, Object>> listObjects(Connection c, String schema) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();

        String routineSql = "SELECT routine_name nm, routine_type rt, routine_schema sch "
                + "FROM information_schema.routines "
                + (schema != null
                    ? "WHERE UPPER(routine_schema) = ? ORDER BY routine_type, routine_name"
                    : "WHERE routine_schema NOT IN (" + SYSTEM_SCHEMAS + ") ORDER BY routine_schema, routine_type, routine_name");
        try (PreparedStatement ps = c.prepareStatement(routineSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch"));
                    obj.put("objectName", rs.getString("nm"));
                    obj.put("objectType", "PROCEDURE".equalsIgnoreCase(rs.getString("rt")) ? "PROCEDURE" : "FUNCTION");
                    out.add(obj);
                }
            }
        }

        String tableSql = "SELECT table_name nm, table_type tt, table_schema sch "
                + "FROM information_schema.tables "
                + (schema != null
                    ? "WHERE UPPER(table_schema) = ? ORDER BY table_type, table_name"
                    : "WHERE table_schema NOT IN (" + SYSTEM_SCHEMAS + ") ORDER BY table_schema, table_type, table_name");
        try (PreparedStatement ps = c.prepareStatement(tableSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch"));
                    obj.put("objectName", rs.getString("nm"));
                    obj.put("objectType", "VIEW".equalsIgnoreCase(rs.getString("tt")) ? "VIEW" : "TABLE");
                    out.add(obj);
                }
            }
        }
        return out;
    }

    @Override
    public String fetchSource(Connection c, String schema, String objectName) throws SQLException {
        for (String routineType : List.of("PROCEDURE", "FUNCTION")) {
            String safeName = objectName.replace("`", "``");
            String qualName = (schema != null && !schema.isBlank())
                    ? "`" + schema.replace("`", "``") + "`.`" + safeName + "`"
                    : "`" + safeName + "`";
            String showSql = "SHOW CREATE " + routineType + " " + qualName;
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(showSql)) {
                if (rs.next()) {
                    String ddl = rs.getString(3); // "Create Procedure"/"Create Function" column
                    if (ddl != null && !ddl.isBlank()) return ddl;
                }
            } catch (SQLException ignored) {
                // Wrong routine type for this name (or not found) — try the next one.
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> tableInfo(Connection c, String schema, String tableName) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", tableName.toUpperCase());
        String[] resolved = MySqlTableQueries.resolveObject(c, schema, tableName);
        if (resolved == null) {
            result.put("found", false);
            return result;
        }
        String sch = resolved[0];
        boolean isView = "VIEW".equals(resolved[1]);
        result.put("schema", sch);
        if (isView) {
            result.put("isView", true);
            String def = MySqlTableQueries.viewDefinition(c, sch, tableName);
            if (def != null) result.put("viewDefinition", def);
        }
        List<Map<String, Object>> columns = MySqlTableQueries.columns(c, sch, tableName);
        result.put("columns", columns);
        result.put("columnCount", columns.size());
        result.put("constraints", MySqlTableQueries.constraints(c, sch, tableName));
        result.put("indexes", MySqlTableQueries.indexes(c, sch, tableName));
        result.put("triggers", MySqlTableQueries.triggers(c, sch, tableName));
        result.put("found", true);
        return result;
    }

    @Override
    public Map<String, Object> findObject(Connection c, String input) throws SQLException {
        String in = input.toUpperCase().trim();
        String[] parts = in.split("\\.", -1);
        String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
        String name = parts[parts.length - 1];

        Map<String, Object> result = new LinkedHashMap<>();

        String routineSql = "SELECT routine_schema sch, routine_name nm, routine_type rt "
                + "FROM information_schema.routines WHERE UPPER(routine_name) = ? "
                + (schema != null ? "AND UPPER(routine_schema) = ? " : "") + "LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(routineSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch"));
                    result.put("objectName", rs.getString("nm"));
                    result.put("objectType", "PROCEDURE".equalsIgnoreCase(rs.getString("rt")) ? "PROCEDURE" : "FUNCTION");
                    return result;
                }
            }
        }

        String tableSql = "SELECT table_schema sch, table_name nm, table_type tt "
                + "FROM information_schema.tables WHERE UPPER(table_name) = ? "
                + (schema != null ? "AND UPPER(table_schema) = ? " : "") + "LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(tableSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch"));
                    result.put("objectName", rs.getString("nm"));
                    result.put("objectType", "VIEW".equalsIgnoreCase(rs.getString("tt")) ? "VIEW" : "TABLE");
                    return result;
                }
            }
        }

        result.put("found", false);
        result.put("input", in);
        result.put("message", "Object not found in any visible schema");
        return result;
    }

    @Override
    public List<Map<String, Object>> listPackages(Connection c, String schema) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> obj : listObjects(c, schema)) {
            String type = String.valueOf(obj.get("objectType"));
            if (!"PROCEDURE".equals(type) && !"FUNCTION".equals(type)) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("objectName", obj.get("objectName"));
            m.put("objectType", type);
            m.put("status", "VALID");
            m.put("hasBody", true);
            m.put("procCount", 1);
            out.add(m);
        }
        return out;
    }
}
