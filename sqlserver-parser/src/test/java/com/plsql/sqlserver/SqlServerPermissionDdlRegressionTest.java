package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the GRANT/REVOKE/DENY phantom-table defect found in the 2026-09-19 audit
 * pass. Permission DDL falls into the grammar's generic {@code otherStatement}, whose text-based
 * op recovery read {@code REVOKE INSERT ON app.orders FROM rpt} as a "FROM &lt;table&gt;" read and
 * invented a SELECT table op named {@code rpt} — the grantee principal — with {@code errors=[]}.
 * A phantom entry in the table inventory is indistinguishable from a real one to a reader.
 */
class SqlServerPermissionDdlRegressionTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo run(String body) {
        return engine.parse("CREATE PROCEDURE p AS\nBEGIN\n" + body + "\nEND\n", "p", null);
    }

    private static List<String> ops(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList();
    }

    private static List<String> stmtTypes(SqlServerRoutineInfo r) {
        return r.getStatements().stream().map(SqlServerStatementInfo::getType).toList();
    }

    @Test
    @DisplayName("REVOKE does not invent a table op from its grantee")
    void revokeGranteeIsNotATable() {
        SqlServerRoutineInfo r = run("REVOKE INSERT ON app.orders FROM rpt;");
        assertEquals(List.of(), ops(r), "phantom table op from REVOKE grantee");
        assertTrue(stmtTypes(r).contains("REVOKE"), stmtTypes(r).toString());
    }

    @Test
    @DisplayName("GRANT / DENY record their statement type and no table ops")
    void grantAndDenyRecordNoTableOps() {
        assertEquals(List.of(), ops(run("GRANT SELECT ON app.orders TO rpt;")));
        assertEquals(List.of(), ops(run("GRANT EXECUTE ON dbo.some_proc TO app_role;")));
        assertEquals(List.of(), ops(run("DENY DELETE ON dbo.orders TO rpt;")));
    }

    @Test
    @DisplayName("Permission DDL does not corrupt the DML statements around it")
    void siblingStatementsSurvive() {
        SqlServerRoutineInfo r = run("""
                INSERT INTO audit_log(msg) VALUES ('before');
                GRANT SELECT ON app.orders TO rpt;
                REVOKE INSERT ON app.orders FROM rpt;
                UPDATE customers SET flag = 1 WHERE id = 7;""");
        assertEquals(List.of("INSERT:audit_log", "UPDATE:customers"), ops(r));
    }
}
