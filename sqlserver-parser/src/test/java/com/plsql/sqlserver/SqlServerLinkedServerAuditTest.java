package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (evidence-gathering only, no fixes applied here).
 *
 * Concept: 4-part linked-server table names (server.database.schema.table) and the
 * OPENQUERY/OPENROWSET remote-query constructs.
 *
 * Context: TABLE_REF_PATTERN (used to scan FROM/JOIN clauses out of SELECT's stmtTail text)
 * was earlier fixed this session to capture up to 4 dotted segments and resolve the *last*
 * segment as the real table name via splitQualifiedName(). This file checks whether that fix
 * actually covers every DML entry point, or only the regex-based SELECT path.
 */
@DisplayName("AUDIT: SQL Server linked-server / 4-part name / OPENQUERY / OPENROWSET capture")
public class SqlServerLinkedServerAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private List<String> tableNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }

    /** Readable dump of table-ops (default toString() is just Object@hash) for evidence output. */
    private String dumpOps(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(op -> op.getDmlType() + " schema=" + op.getSchemaName() + " table=" + op.getTableName())
                .toList().toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 1: 4-part linked-server names across SELECT / UPDATE / INSERT
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("1a: SELECT FROM 4-part linked-server name -- control case, resolves via regex TABLE_REF_PATTERN")
    void auditFourPartNameInSelect() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM LinkedSrv.OtherDb.dbo.Orders o WHERE o.status = 'X';\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[1a] tableOps=" + dumpOps(r));
        assertTrue(names.contains("ORDERS"),
                "the real table (Orders, last segment of the 4-part name) must be resolved in a SELECT");
        assertFalse(names.contains("DBO"), "must not mis-split and yield a phantom 'DBO' table name");
    }

    @Test
    @DisplayName("1b: UPDATE target is a 4-part linked-server name -- grammar tableRef caps at 2 parts, real table lost")
    void auditFourPartNameInUpdateTarget() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  UPDATE LinkedSrv.OtherDb.dbo.Orders SET status = 'Y' WHERE id = @id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[1b] tableOps=" + dumpOps(r));
        // Root cause: the grammar rule `tableRef` (SqlServerParser.g4) is
        //   tableRef : AT_VAR | (BRACKET_ID | ID) (DOT (BRACKET_ID | ID))? ;
        // i.e. it structurally accepts AT MOST 2 dotted parts. It is NOT regex-based like the
        // SELECT path, so the 4-/3-part-name fix applied to TABLE_REF_PATTERN never touches it.
        // For "LinkedSrv.OtherDb.dbo.Orders", the parser can only consume "LinkedSrv.OtherDb" as
        // the tableRef; the trailing ".dbo.Orders" is left over and swallowed by the permissive
        // stmtTail rule instead. extractTableName/extractTableSchema then split THAT 2-part text
        // on its single dot, yielding schema=LinkedSrv, name=OtherDb -- the real target table
        // "Orders" is silently dropped from the table-ops list entirely (not just misnamed).
        assertTrue(names.contains("ORDERS"),
                "EXPECTED: the real UPDATE target table (Orders) should be captured. "
              + "ACTUAL observed names=" + names + " -- confirms the table is lost/mis-split.");
    }

    @Test
    @DisplayName("1c: INSERT target is a 4-part linked-server name -- same grammar-level cap applies")
    void auditFourPartNameInInsertTarget() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  INSERT INTO LinkedSrv.OtherDb.dbo.Orders (id, status) VALUES (@id, 'Z');\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[1c] tableOps=" + dumpOps(r));
        assertTrue(names.contains("ORDERS"),
                "EXPECTED: the real INSERT target table (Orders) should be captured. "
              + "ACTUAL observed names=" + names + " -- confirms the table is lost/mis-split.");
    }

    @Test
    @DisplayName("1d: DELETE target is a 4-part linked-server name -- same grammar-level cap applies")
    void auditFourPartNameInDeleteTarget() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  DELETE FROM LinkedSrv.OtherDb.dbo.Orders WHERE id = @id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        System.out.println("[1d] tableOps=" + dumpOps(r));
        assertTrue(names.contains("ORDERS"),
                "EXPECTED: the real DELETE target table (Orders) should be captured. "
              + "ACTUAL observed names=" + names + " -- confirms the table is lost/mis-split.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 2: OPENQUERY
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("2a: OPENQUERY -- must not crash and must not invent a phantom OPENQUERY/linkedsrv table")
    void auditOpenQueryNoCrashNoPhantom() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM OPENQUERY(linkedsrv, 'SELECT * FROM remote_table');\n" +
                "END";
        SqlServerRoutineInfo r;
        try {
            r = parse(sql);
        } catch (RuntimeException e) {
            fail("OPENQUERY must not crash the parser/visitor: " + e);
            return;
        }
        List<String> names = tableNames(r);
        System.out.println("[2a] tableOps=" + dumpOps(r));
        assertFalse(names.contains("OPENQUERY"), "must not invent a phantom table literally named OPENQUERY");
        assertFalse(names.contains("LINKEDSRV"), "must not invent a phantom table from the linked-server name arg");
        // Bonus (not required): extracting remote_table from the embedded query-string literal.
        System.out.println("[2a] bonus check -- does it recover remote_table from the string literal? "
                + names.contains("REMOTE_TABLE"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 3: OPENROWSET
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("3a: OPENROWSET with provider/connection-string -- must not crash or invent phantom tables")
    void auditOpenRowsetProviderNoCrashNoPhantom() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM OPENROWSET('SQLNCLI', 'server=Remote;trusted_connection=yes;', 'SELECT * FROM remote_table');\n" +
                "END";
        SqlServerRoutineInfo r;
        try {
            r = parse(sql);
        } catch (RuntimeException e) {
            fail("OPENROWSET must not crash the parser/visitor: " + e);
            return;
        }
        List<String> names = tableNames(r);
        System.out.println("[3a] tableOps=" + dumpOps(r));
        assertFalse(names.contains("OPENROWSET"), "must not invent a phantom table literally named OPENROWSET");
        assertFalse(names.contains("SQLNCLI"), "must not invent a phantom table from the provider name arg");
    }

    @Test
    @DisplayName("3b: OPENROWSET(BULK 'file', ...) -- bulk file form must not crash or invent phantom tables")
    void auditOpenRowsetBulkNoCrashNoPhantom() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM OPENROWSET(BULK 'C:\\file.csv', SINGLE_CLOB) AS x;\n" +
                "END";
        SqlServerRoutineInfo r;
        try {
            r = parse(sql);
        } catch (RuntimeException e) {
            fail("OPENROWSET(BULK ...) must not crash the parser/visitor: " + e);
            return;
        }
        List<String> names = tableNames(r);
        System.out.println("[3b] tableOps=" + dumpOps(r));
        assertFalse(names.contains("OPENROWSET"), "must not invent a phantom table literally named OPENROWSET");
        assertFalse(names.contains("BULK"), "must not invent a phantom table literally named BULK");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 4: JOIN combining a local table with a 4-part linked-server table
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("4a: JOIN local table to a 4-part linked-server table -- join edge + both table names")
    void auditJoinLocalToFourPartLinkedServerTable() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT c.id, o.status\n" +
                "  FROM local_customers c\n" +
                "  JOIN LinkedSrv.OtherDb.dbo.Orders o ON o.customer_id = c.id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNames(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[4a] tableOps=" + names + " joins=" + joins);
        assertTrue(names.contains("LOCAL_CUSTOMERS"), "local table must be captured");
        assertTrue(names.contains("ORDERS"),
                "EXPECTED: the linked-server table's real name (Orders) must be captured. "
              + "ACTUAL names=" + names);
        assertFalse(joins.isEmpty(),
                "EXPECTED: a join edge between local_customers and the linked-server Orders table must be recorded. "
              + "ACTUAL joins=" + joins);
    }
}
