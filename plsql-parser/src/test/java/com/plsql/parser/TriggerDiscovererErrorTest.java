package com.plsql.parser;

import com.plsql.parser.flow.TriggerDiscoverer;
import com.plsql.parser.model.SchemaTableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error handling tests for TriggerDiscoverer.
 * Tests behavior with empty tableInfoMap and null connManager.
 */
@DisplayName("TriggerDiscoverer Error Handling")
public class TriggerDiscovererErrorTest {

    // ── discover with empty tableInfoMap ──

    @Test
    @DisplayName("discover with empty tableInfoMap returns empty list")
    void testDiscoverEmptyTableInfoMap() {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        List<Map<String, Object>> triggers = discoverer.discover(new LinkedHashMap<>());
        assertNotNull(triggers);
        assertTrue(triggers.isEmpty());
    }

    // ── discover with null connManager ──

    @Test
    @DisplayName("discover with null connManager returns empty list")
    void testDiscoverNullConnManager() {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        Map<String, SchemaTableInfo> tableInfoMap = new LinkedHashMap<>();
        SchemaTableInfo info = new SchemaTableInfo();
        info.setSchema("HR");
        info.setTableName("EMPLOYEES");
        info.getOperations().add("INSERT");
        tableInfoMap.put("HR.EMPLOYEES", info);

        List<Map<String, Object>> triggers = discoverer.discover(tableInfoMap);
        assertNotNull(triggers);
        assertTrue(triggers.isEmpty(),
                "Should return empty when connManager is null");
    }

    // ── discover with tables that have no DML operations ──

    @Test
    @DisplayName("discover with SELECT-only tables returns empty (no DML)")
    void testDiscoverSelectOnlyTables() {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        Map<String, SchemaTableInfo> tableInfoMap = new LinkedHashMap<>();
        SchemaTableInfo info = new SchemaTableInfo();
        info.setSchema("HR");
        info.setTableName("EMPLOYEES");
        info.getOperations().add("SELECT");
        tableInfoMap.put("HR.EMPLOYEES", info);

        List<Map<String, Object>> triggers = discoverer.discover(tableInfoMap);
        assertTrue(triggers.isEmpty(),
                "Should not query triggers for SELECT-only tables");
    }

    // ── extractDmlFromBody via reflection (package-private method) ──

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> invokeExtractDml(TriggerDiscoverer discoverer,
                                                        String body,
                                                        Map<String, String> knownTypes) throws Exception {
        Method method = TriggerDiscoverer.class.getDeclaredMethod(
                "extractDmlFromBody", String.class, Map.class);
        method.setAccessible(true);
        return (List<Map<String, Object>>) method.invoke(discoverer, body, knownTypes);
    }

    @Test
    @DisplayName("extractDmlFromBody with null body returns empty")
    void testExtractDmlFromBodyNull() throws Exception {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        List<Map<String, Object>> ops = invokeExtractDml(discoverer, null, new HashMap<>());
        assertNotNull(ops);
        assertTrue(ops.isEmpty());
    }

    @Test
    @DisplayName("extractDmlFromBody with empty body returns empty")
    void testExtractDmlFromBodyEmpty() throws Exception {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        List<Map<String, Object>> ops = invokeExtractDml(discoverer, "", new HashMap<>());
        assertNotNull(ops);
        assertTrue(ops.isEmpty());
    }

    @Test
    @DisplayName("extractDmlFromBody detects INSERT INTO, UPDATE, DELETE FROM")
    void testExtractDmlFromBodyDetectsStatements() throws Exception {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        String body = """
                BEGIN
                  INSERT INTO AUDIT_LOG (action) VALUES ('trigger fired');
                  UPDATE EMPLOYEE_STATUS SET active = 'N' WHERE emp_id = :OLD.id;
                  DELETE FROM TEMP_TABLE WHERE created < SYSDATE - 30;
                END;
                """;
        List<Map<String, Object>> ops = invokeExtractDml(discoverer, body, new HashMap<>());
        assertNotNull(ops);
        assertFalse(ops.isEmpty(), "Should detect DML statements");

        Set<String> tableNames = new HashSet<>();
        for (Map<String, Object> op : ops) {
            tableNames.add((String) op.get("tableName"));
        }
        assertTrue(tableNames.contains("AUDIT_LOG"));
        assertTrue(tableNames.contains("EMPLOYEE_STATUS"));
        assertTrue(tableNames.contains("TEMP_TABLE"));
    }

    @Test
    @DisplayName("extractDmlFromBody skips SQL keywords used as table names")
    void testExtractDmlSkipsKeywords() throws Exception {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        String body = "BEGIN SELECT 1 FROM DUAL; END;";
        List<Map<String, Object>> ops = invokeExtractDml(discoverer, body, new HashMap<>());
        for (Map<String, Object> op : ops) {
            assertNotEquals("DUAL", op.get("tableName"));
        }
    }

    @Test
    @DisplayName("extractDmlFromBody deduplicates operations")
    void testExtractDmlDeduplicates() throws Exception {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        String body = """
                BEGIN
                  INSERT INTO AUDIT_LOG (a) VALUES (1);
                  INSERT INTO AUDIT_LOG (a) VALUES (2);
                END;
                """;
        List<Map<String, Object>> ops = invokeExtractDml(discoverer, body, new HashMap<>());
        long insertAuditCount = ops.stream()
                .filter(op -> "INSERT".equals(op.get("operation"))
                        && "AUDIT_LOG".equals(op.get("tableName")))
                .count();
        assertEquals(1, insertAuditCount, "Duplicate INSERT INTO AUDIT_LOG should be deduplicated");
    }

    // ── Multiple DML ops on same table ──

    @Test
    @DisplayName("discover processes tables with INSERT, UPDATE, DELETE, MERGE, TRUNCATE operations")
    void testDmlOperationTypes() {
        TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);
        Map<String, SchemaTableInfo> tableInfoMap = new LinkedHashMap<>();

        for (String op : List.of("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE")) {
            SchemaTableInfo info = new SchemaTableInfo();
            info.setSchema("HR");
            info.setTableName("TABLE_" + op);
            info.getOperations().add(op);
            tableInfoMap.put("HR.TABLE_" + op, info);
        }

        // With null connManager, returns empty but exercises collectDmlTables logic
        List<Map<String, Object>> triggers = discoverer.discover(tableInfoMap);
        assertTrue(triggers.isEmpty(), "No triggers returned without DB connection");
    }
}
