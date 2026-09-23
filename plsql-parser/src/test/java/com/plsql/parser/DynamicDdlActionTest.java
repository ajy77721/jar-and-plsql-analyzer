package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-019 (§122) — Oracle PL/SQL cannot express DDL directly in a procedure body (only via
 * {@code EXECUTE IMMEDIATE}), and nothing told a DDL verb apart from a DML one in dynamic SQL
 * before this: {@code runDynSqlPattern} classified every dynamic-SQL table reference as the
 * generic {@code DYNAMIC} bucket, with no {@code sqlText} set, so {@code TableOperationEnricher}'s
 * existing {@code SqlAnalyzer.analyze()} → {@code extractDdl()} path (already fully built and
 * already used by the multidb dialects) never even ran against it. Fixed at the point of
 * classification, reusing the existing extractor rather than duplicating it.
 *
 * <p>Standalone procedures here — the shape this repo's own real corpus actually is (`packageName`
 * null on all 52 ORACLE nodes, checked directly), so this is what makes `am_ddl_action` produce
 * real rows here. The package-body-contained case (once its own separate gap —
 * {@code SubprogramInfo.tableOperations} never being enriched at all — was found and fixed,
 * ING-024) is proven identical to this file's own standalone cases by
 * {@code PackageBodyEnrichmentTest}, not duplicated here.
 */
public class DynamicDdlActionTest extends ParserTestBase {

    private TableOperationInfo firstOp(ParseResult result, String tableName) {
        for (ParsedObject obj : result.getObjects()) {
            for (TableOperationInfo t : obj.getTableOperations()) {
                if (tableName.equalsIgnoreCase(t.getTableName())) return t;
            }
        }
        return null;
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE 'CREATE TABLE ...' is classified as DDL with the real action")
    void testExecuteImmediateCreateTable() {
        String sql = """
                CREATE OR REPLACE PROCEDURE SETUP IS
                BEGIN
                  EXECUTE IMMEDIATE 'CREATE TABLE staging_orders (id NUMBER, amt NUMBER)';
                END SETUP;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE CREATE TABLE");
        TableOperationInfo op = firstOp(result, "STAGING_ORDERS");
        assertNotNull(op, "STAGING_ORDERS should be extracted from the dynamic CREATE TABLE");
        assertEquals("DDL", op.getOperation());
        assertFalse(op.getDdl().isEmpty(), "enrichment must have populated DdlInfo");
        assertEquals("CREATE_TABLE", op.getDdl().get(0).getAction());
        assertEquals("STAGING_ORDERS", op.getDdl().get(0).getObjectName());
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE 'ALTER TABLE ...' is classified as DDL")
    void testExecuteImmediateAlterTable() {
        String sql = """
                CREATE OR REPLACE PROCEDURE MIGRATE IS
                BEGIN
                  EXECUTE IMMEDIATE 'ALTER TABLE customer ADD (status VARCHAR2(1))';
                END MIGRATE;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE ALTER TABLE");
        TableOperationInfo op = firstOp(result, "CUSTOMER");
        assertNotNull(op, "CUSTOMER should be extracted from the dynamic ALTER TABLE");
        assertEquals("DDL", op.getOperation());
        assertFalse(op.getDdl().isEmpty());
        assertEquals("ALTER_TABLE", op.getDdl().get(0).getAction());
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE 'DROP TABLE ...' is classified as DDL")
    void testExecuteImmediateDropTable() {
        String sql = """
                CREATE OR REPLACE PROCEDURE CLEANUP IS
                BEGIN
                  EXECUTE IMMEDIATE 'DROP TABLE temp_batch';
                END CLEANUP;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE DROP TABLE");
        TableOperationInfo op = firstOp(result, "TEMP_BATCH");
        assertNotNull(op, "TEMP_BATCH should be extracted from the dynamic DROP TABLE");
        assertEquals("DDL", op.getOperation());
        assertFalse(op.getDdl().isEmpty());
        assertEquals("DROP", op.getDdl().get(0).getAction());
    }

    @Test
    @DisplayName("A regular dynamic SELECT is still classified DYNAMIC, not DDL — no regression")
    void testExecuteImmediateSelectStaysDynamic() {
        String sql = """
                CREATE OR REPLACE PROCEDURE READ_IT IS
                  v_cnt NUMBER;
                BEGIN
                  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM orders' INTO v_cnt;
                END READ_IT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE SELECT");
        TableOperationInfo op = firstOp(result, "ORDERS");
        assertNotNull(op);
        assertEquals("DYNAMIC", op.getOperation());
        assertTrue(op.getDdl().isEmpty());
    }
}
