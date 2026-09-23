package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Read-only audit (2026-09-17): probes 3 Oracle PL/SQL concepts against the real ANTLR
 * parser/visitor and PRINTS what was captured. Diagnostic harness, not a permanent
 * regression suite.
 *
 * 1. CREATE GLOBAL TEMPORARY TABLE
 * 2. Optimizer hint comments (/*+ ... *&#47;) immediately after DML keywords
 * 3. Synonyms + qualified/dblink table references
 */
public class OracleTempTableHintSynonymAuditTest extends ParserTestBase {

    private TableOperationInfo firstOp(ParseResult result, String tableName) {
        for (ParsedObject obj : result.getObjects()) {
            for (TableOperationInfo t : obj.getTableOperations()) {
                if (tableName.equalsIgnoreCase(t.getTableName())) return t;
            }
            for (SubprogramInfo sub : obj.getSubprograms()) {
                for (TableOperationInfo t : sub.getTableOperations()) {
                    if (tableName.equalsIgnoreCase(t.getTableName())) return t;
                }
            }
        }
        return null;
    }

    private void dumpAll(ParseResult result, String label) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + result.getErrors());
        System.out.println("  objects=" + result.getObjects().size());
        for (ParsedObject obj : result.getObjects()) {
            System.out.println("  object: name=" + obj.getName() + " type=" + obj.getType());
            for (TableOperationInfo t : obj.getTableOperations()) {
                System.out.println("    top-op: table=" + t.getTableName() + " schema=" + t.getSchema()
                        + " op=" + t.getOperation() + " dbLink=" + t.getDbLink());
            }
            for (SubprogramInfo sub : obj.getSubprograms()) {
                System.out.println("    sub: name=" + sub.getName() + " type=" + sub.getType());
                for (TableOperationInfo t : sub.getTableOperations()) {
                    System.out.println("      op: table=" + t.getTableName() + " schema=" + t.getSchema()
                            + " op=" + t.getOperation() + " dbLink=" + t.getDbLink() + " alias=" + t.getAlias());
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 1. CREATE GLOBAL TEMPORARY TABLE
    // ------------------------------------------------------------------
    @Test
    @DisplayName("1a: standalone CREATE GLOBAL TEMPORARY TABLE ON COMMIT DELETE ROWS")
    void globalTemporaryTableStandalone() {
        String sql = """
                CREATE GLOBAL TEMPORARY TABLE gtt_staging (
                  id NUMBER,
                  amt NUMBER
                ) ON COMMIT DELETE ROWS;
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 1a: standalone GTT ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        dumpAll(result, "1a: standalone GTT");
        assertNoParsErrors(result, "standalone GTT");
        TableOperationInfo op = firstOp(result, "gtt_staging");
        org.junit.jupiter.api.Assertions.assertNotNull(op,
                "standalone CREATE GLOBAL TEMPORARY TABLE must produce a table op");
        org.junit.jupiter.api.Assertions.assertEquals("DDL", op.getOperation());
    }

    @Test
    @DisplayName("1b: CREATE GLOBAL TEMPORARY TABLE via EXECUTE IMMEDIATE (dynamic DDL)")
    void globalTemporaryTableDynamic() {
        String sql = """
                CREATE OR REPLACE PROCEDURE SETUP_GTT IS
                BEGIN
                  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE gtt_dyn (id NUMBER) ON COMMIT PRESERVE ROWS';
                END SETUP_GTT;
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 1b: dynamic GTT ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        dumpAll(result, "1b: dynamic GTT");
        TableOperationInfo op = firstOp(result, "GTT_DYN");
        System.out.println("  firstOp(GTT_DYN)=" + (op == null ? "NOT FOUND" : (op.getOperation() + " ddl=" + op.getDdl())));
        org.junit.jupiter.api.Assertions.assertNotNull(op,
                "EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE ...' must be extracted as DDL, same as a plain dynamic CREATE TABLE");
        org.junit.jupiter.api.Assertions.assertEquals("DDL", op.getOperation());
    }

    @Test
    @DisplayName("1c: GLOBAL TEMPORARY TABLE used afterwards as normal DML target inside a procedure")
    void globalTemporaryTableThenDml() {
        String sql = """
                CREATE OR REPLACE PROCEDURE USE_GTT IS
                BEGIN
                  INSERT INTO gtt_staging (id, amt) VALUES (1, 100);
                  UPDATE gtt_staging SET amt = amt + 1 WHERE id = 1;
                  DELETE FROM gtt_staging WHERE id = 1;
                END USE_GTT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "GTT used as DML target");
        dumpAll(result, "1c: GTT used as DML target");
    }

    // ------------------------------------------------------------------
    // 2. Optimizer hint comments immediately after DML keyword
    // ------------------------------------------------------------------
    @Test
    @DisplayName("2a: SELECT /*+ INDEX(t idx) FULL(t) */ ... FROM table")
    void hintOnSelect() {
        String sql = """
                CREATE OR REPLACE PROCEDURE RUN_SELECT_HINT IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT /*+ INDEX(t idx) FULL(t) */ COUNT(*) INTO v_cnt
                  FROM orders t
                  WHERE t.status = 'ACTIVE';
                END RUN_SELECT_HINT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "hint on SELECT");
        dumpAll(result, "2a: hint on SELECT");
        TableOperationInfo op = firstOp(result, "ORDERS");
        System.out.println("  firstOp(ORDERS)=" + (op == null ? "NOT FOUND" : op.getOperation()));
    }

    @Test
    @DisplayName("2b: INSERT /*+ APPEND */ INTO table")
    void hintOnInsert() {
        String sql = """
                CREATE OR REPLACE PROCEDURE RUN_INSERT_HINT IS
                BEGIN
                  INSERT /*+ APPEND */ INTO staging_orders (id, amt)
                  SELECT id, amt FROM raw_orders;
                END RUN_INSERT_HINT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "hint on INSERT");
        dumpAll(result, "2b: hint on INSERT");
        System.out.println("  firstOp(STAGING_ORDERS)=" + (firstOp(result, "STAGING_ORDERS") == null ? "NOT FOUND" : firstOp(result, "STAGING_ORDERS").getOperation()));
        System.out.println("  firstOp(RAW_ORDERS)=" + (firstOp(result, "RAW_ORDERS") == null ? "NOT FOUND" : firstOp(result, "RAW_ORDERS").getOperation()));
    }

    @Test
    @DisplayName("2c: UPDATE /*+ INDEX(t idx) */ table SET ...")
    void hintOnUpdate() {
        String sql = """
                CREATE OR REPLACE PROCEDURE RUN_UPDATE_HINT IS
                BEGIN
                  UPDATE /*+ INDEX(t idx) */ orders t
                  SET t.status = 'DONE'
                  WHERE t.id = 1;
                END RUN_UPDATE_HINT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "hint on UPDATE");
        dumpAll(result, "2c: hint on UPDATE");
        System.out.println("  firstOp(ORDERS)=" + (firstOp(result, "ORDERS") == null ? "NOT FOUND" : firstOp(result, "ORDERS").getOperation()));
    }

    @Test
    @DisplayName("2d: DELETE /*+ INDEX(t idx) */ FROM table")
    void hintOnDelete() {
        String sql = """
                CREATE OR REPLACE PROCEDURE RUN_DELETE_HINT IS
                BEGIN
                  DELETE /*+ INDEX(t idx) */ FROM orders t
                  WHERE t.status = 'CANCELLED';
                END RUN_DELETE_HINT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "hint on DELETE");
        dumpAll(result, "2d: hint on DELETE");
        System.out.println("  firstOp(ORDERS)=" + (firstOp(result, "ORDERS") == null ? "NOT FOUND" : firstOp(result, "ORDERS").getOperation()));
    }

    @Test
    @DisplayName("2e: MERGE /*+ INDEX(t idx) */ INTO table t USING ...")
    void hintOnMerge() {
        String sql = """
                CREATE OR REPLACE PROCEDURE RUN_MERGE_HINT IS
                BEGIN
                  MERGE /*+ INDEX(t idx) */ INTO orders t
                  USING staging_orders s
                  ON (t.id = s.id)
                  WHEN MATCHED THEN UPDATE SET t.amt = s.amt
                  WHEN NOT MATCHED THEN INSERT (id, amt) VALUES (s.id, s.amt);
                END RUN_MERGE_HINT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "hint on MERGE");
        dumpAll(result, "2e: hint on MERGE");
        System.out.println("  firstOp(ORDERS)=" + (firstOp(result, "ORDERS") == null ? "NOT FOUND" : firstOp(result, "ORDERS").getOperation()));
        System.out.println("  firstOp(STAGING_ORDERS)=" + (firstOp(result, "STAGING_ORDERS") == null ? "NOT FOUND" : firstOp(result, "STAGING_ORDERS").getOperation()));
    }

    // ------------------------------------------------------------------
    // 3. Synonyms + qualified / dblink table references
    // ------------------------------------------------------------------
    @Test
    @DisplayName("3a: CREATE SYNONYM then usage of synonym name as a normal table")
    void synonymCreateAndUse() {
        String sql = """
                CREATE OR REPLACE SYNONYM orders_syn FOR hr.orders;

                CREATE OR REPLACE PROCEDURE USE_SYNONYM IS
                BEGIN
                  INSERT INTO orders_syn (id, amt) VALUES (1, 100);
                  UPDATE orders_syn SET amt = amt + 1 WHERE id = 1;
                END USE_SYNONYM;
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 3a: CREATE SYNONYM + usage ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        dumpAll(result, "3a: CREATE SYNONYM + usage");
        System.out.println("  firstOp(ORDERS_SYN)=" + (firstOp(result, "ORDERS_SYN") == null ? "NOT FOUND" : firstOp(result, "ORDERS_SYN").getOperation()));

        // CREATE SYNONYM must now be captured as its own SYNONYM object, with a DDL table-op
        // resolving the synonym to its real target (schema-qualified).
        ParsedObject synonymObj = null;
        for (ParsedObject obj : result.getObjects()) {
            if ("SYNONYM".equals(obj.getType()) && "orders_syn".equalsIgnoreCase(obj.getName())) {
                synonymObj = obj;
                break;
            }
        }
        org.junit.jupiter.api.Assertions.assertNotNull(synonymObj,
                "CREATE SYNONYM must produce a SYNONYM object");
        org.junit.jupiter.api.Assertions.assertEquals(1, synonymObj.getTableOperations().size());
        TableOperationInfo synonymOp = synonymObj.getTableOperations().get(0);
        org.junit.jupiter.api.Assertions.assertEquals("orders", synonymOp.getTableName());
        org.junit.jupiter.api.Assertions.assertEquals("hr", synonymOp.getSchema());
        org.junit.jupiter.api.Assertions.assertNull(synonymOp.getDbLink());
        org.junit.jupiter.api.Assertions.assertEquals("DDL", synonymOp.getOperation());
        org.junit.jupiter.api.Assertions.assertEquals("SYNONYM", synonymOp.getObjectType());
        org.junit.jupiter.api.Assertions.assertEquals("orders_syn", synonymOp.getAlias());

        // Usage of the synonym name as a normal table target must still work unchanged.
        TableOperationInfo usageOp = firstOp(result, "ORDERS_SYN");
        org.junit.jupiter.api.Assertions.assertNotNull(usageOp,
                "synonym usage as a normal table target must still be captured");
        org.junit.jupiter.api.Assertions.assertEquals("INSERT", usageOp.getOperation());
    }

    @Test
    @DisplayName("3e: CREATE PUBLIC SYNONYM FOR schema.table@dblink")
    void publicSynonymWithDbLink() {
        String sql = """
                CREATE OR REPLACE PUBLIC SYNONYM pub_orders_syn FOR hr.orders@remote_db;
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "public synonym with dblink");
        dumpAll(result, "3e: PUBLIC SYNONYM + dblink");

        ParsedObject synonymObj = null;
        for (ParsedObject obj : result.getObjects()) {
            if ("SYNONYM".equals(obj.getType()) && "pub_orders_syn".equalsIgnoreCase(obj.getName())) {
                synonymObj = obj;
                break;
            }
        }
        org.junit.jupiter.api.Assertions.assertNotNull(synonymObj,
                "CREATE PUBLIC SYNONYM must produce a SYNONYM object");
        org.junit.jupiter.api.Assertions.assertNull(synonymObj.getSchema(),
                "PUBLIC synonyms have no owning schema");
        org.junit.jupiter.api.Assertions.assertEquals(1, synonymObj.getTableOperations().size());
        TableOperationInfo op = synonymObj.getTableOperations().get(0);
        org.junit.jupiter.api.Assertions.assertEquals("orders", op.getTableName());
        org.junit.jupiter.api.Assertions.assertEquals("hr", op.getSchema());
        org.junit.jupiter.api.Assertions.assertEquals("remote_db", op.getDbLink());
        org.junit.jupiter.api.Assertions.assertEquals("PUBLIC SYNONYM", op.getObjectType());
        org.junit.jupiter.api.Assertions.assertEquals("pub_orders_syn", op.getAlias());
    }

    @Test
    @DisplayName("3b: schema-qualified table reference (schema.table)")
    void schemaQualifiedReference() {
        String sql = """
                CREATE OR REPLACE PROCEDURE USE_SCHEMA_QUALIFIED IS
                BEGIN
                  INSERT INTO hr.orders (id, amt) VALUES (1, 100);
                  SELECT COUNT(*) INTO v_cnt FROM hr.orders WHERE id = 1;
                END USE_SCHEMA_QUALIFIED;
                /
                """;
        // NOTE: v_cnt intentionally undeclared here; not the point of this probe. If the
        // parser chokes on that, fall back to a variant without SELECT..INTO.
        ParseResult result = parse(sql);
        dumpAll(result, "3b: schema.table qualified reference");
        System.out.println("  errors=" + result.getErrors());
    }

    @Test
    @DisplayName("3c: dblink-qualified table reference (table@dblink)")
    void dbLinkQualifiedReference() {
        String sql = """
                CREATE OR REPLACE PROCEDURE USE_DBLINK IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM orders@remote_db WHERE status = 'ACTIVE';
                  INSERT INTO local_orders (id, amt) SELECT id, amt FROM orders@remote_db;
                END USE_DBLINK;
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 3c: table@dblink ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        dumpAll(result, "3c: table@dblink");
        TableOperationInfo op = firstOp(result, "ORDERS");
        System.out.println("  firstOp(ORDERS)=" + (op == null ? "NOT FOUND" : (op.getOperation() + " dbLink=" + op.getDbLink())));
    }

    @Test
    @DisplayName("3d: schema-qualified dblink reference (schema.table@dblink)")
    void schemaAndDbLinkQualifiedReference() {
        String sql = """
                CREATE OR REPLACE PROCEDURE USE_SCHEMA_DBLINK IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM hr.orders@remote_db WHERE status = 'ACTIVE';
                END USE_SCHEMA_DBLINK;
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 3d: schema.table@dblink ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        dumpAll(result, "3d: schema.table@dblink");
        TableOperationInfo op = firstOp(result, "ORDERS");
        System.out.println("  firstOp(ORDERS)=" + (op == null ? "NOT FOUND" : (op.getOperation() + " schema=" + op.getSchema() + " dbLink=" + op.getDbLink())));
    }
}
