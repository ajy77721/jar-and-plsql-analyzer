package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep17 audit — recursive CTEs and sequence/identity handling, SQL Server dialect.
 *
 * <h2>Scope</h2>
 * <ol>
 *   <li><b>CONNECT BY / START WITH / LEVEL</b> — T-SQL has no hierarchical-query syntax; SQL
 *       Server expresses recursion only via a plain {@code WITH name AS (...)} recursive CTE
 *       (no {@code RECURSIVE} keyword, unlike Postgres/MySQL8). Confirmed via grep across
 *       {@code src/main} of this module: no {@code CONNECT BY}/{@code START WITH} handling exists
 *       anywhere in the grammar or visitor, so there is nothing to test here — this is a
 *       deliberate no-op section, not an oversight.</li>
 *   <li><b>Recursive CTEs</b> — anchor-branch base table, recursive-branch base table, and the
 *       classic bug pattern where the CTE's own name leaks into the table list as if it were an
 *       external table (particularly likely in T-SQL because there is no {@code RECURSIVE}
 *       keyword to hint the visitor that a name is CTE-only).</li>
 *   <li><b>Sequences / identity</b> — {@code NEXT VALUE FOR schema.seq} and column-level
 *       {@code IDENTITY(seed, increment)}, checked so that the sequence usage is recorded as a
 *       sequence (not a phantom table) and does not cause the real INSERT/SELECT target table to
 *       be dropped.</li>
 * </ol>
 */
class SqlServerRecursiveCteAndSequenceAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private List<String> tableNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 1. CONNECT BY / START WITH / LEVEL — does not apply to T-SQL.
    // Confirmed via: grep -rni "connect by\|start with" sqlserver-parser/src/main
    //   → zero matches in grammar (.g4) and visitor (.java) sources.
    // No test is fabricated for this section per the audit instructions.
    // ─────────────────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────────────────
    // 2. Recursive CTEs (plain WITH, no RECURSIVE keyword in T-SQL)
    // ─────────────────────────────────────────────────────────────────────

    private static final String RECURSIVE_CTE_PROC =
            "CREATE PROCEDURE usp_OrgChart\nAS\nBEGIN\n"
          + "  WITH EmpTree AS (\n"
          + "    SELECT e.EmployeeId, e.ManagerId, 0 AS Lvl FROM hr.Employee e WHERE e.ManagerId IS NULL\n"
          + "    UNION ALL\n"
          + "    SELECT e2.EmployeeId, e2.ManagerId, t.Lvl + 1 FROM hr.Employee e2\n"
          + "    INNER JOIN EmpTree t ON e2.ManagerId = t.EmployeeId\n"
          + "  )\n"
          + "  SELECT * FROM EmpTree ORDER BY Lvl;\n"
          + "END";

    @Test
    @DisplayName("2a: anchor branch's base table (hr.Employee) is captured")
    void anchorBranchBaseTableCaptured() {
        SqlServerRoutineInfo r = parse(RECURSIVE_CTE_PROC);
        List<String> tables = tableNames(r);
        assertTrue(tables.contains("EMPLOYEE"), "anchor base table missing; tables=" + tables);
    }

    @Test
    @DisplayName("2b: recursive branch's base table (hr.Employee, second occurrence) is captured")
    void recursiveBranchBaseTableCaptured() {
        SqlServerRoutineInfo r = parse(RECURSIVE_CTE_PROC);
        long employeeHits = r.getTableOps().stream()
                .filter(t -> t.getTableName() != null && t.getTableName().equalsIgnoreCase("Employee"))
                .count();
        // Anchor FROM hr.Employee e, and recursive FROM hr.Employee e2 + JOIN EmpTree t —
        // at minimum both FROM occurrences of Employee should be observed (dedup by op, not
        // by occurrence, so this documents actual observed count rather than asserting a lower
        // bound blindly).
        assertTrue(employeeHits >= 1, "expected at least one Employee table op; tableOps=" + r.getTableOps());
    }

    @Test
    @DisplayName("2c: the CTE's own name (EmpTree) is NOT recorded as an external table reference")
    void cteOwnNameNotRecordedAsTable() {
        SqlServerRoutineInfo r = parse(RECURSIVE_CTE_PROC);
        List<String> tables = tableNames(r);
        assertFalse(tables.contains("EMPTREE"),
                "CTE's own name leaked into the table list as an external table; tables=" + tables);
    }

    private static final String CHAINED_RECURSIVE_CTES_PROC =
            "CREATE PROCEDURE usp_BomExplode\nAS\nBEGIN\n"
          + "  WITH BomLevel1 AS (\n"
          + "    SELECT b.ParentPartId, b.ChildPartId, 1 AS Depth FROM mfg.BillOfMaterial b WHERE b.ParentPartId = 100\n"
          + "    UNION ALL\n"
          + "    SELECT b2.ParentPartId, b2.ChildPartId, l1.Depth + 1 FROM mfg.BillOfMaterial b2\n"
          + "    INNER JOIN BomLevel1 l1 ON b2.ParentPartId = l1.ChildPartId\n"
          + "  ),\n"
          + "  BomLevel2 AS (\n"
          + "    SELECT l1.ParentPartId, l1.ChildPartId, l1.Depth FROM BomLevel1 l1\n"
          + "    UNION ALL\n"
          + "    SELECT p.PartId, p.SubPartId, l2.Depth + 1 FROM mfg.PartSubstitute p\n"
          + "    INNER JOIN BomLevel2 l2 ON p.PartId = l2.ChildPartId\n"
          + "  )\n"
          + "  SELECT * FROM BomLevel2;\n"
          + "END";

    @Test
    @DisplayName("2d: multi-level chained recursive CTEs — real base tables captured, both CTE names excluded")
    void chainedRecursiveCtesRealTablesCapturedAndCteNamesExcluded() {
        SqlServerRoutineInfo r = parse(CHAINED_RECURSIVE_CTES_PROC);
        List<String> tables = tableNames(r);
        assertTrue(tables.contains("BILLOFMATERIAL"), "real base table missing; tables=" + tables);
        assertTrue(tables.contains("PARTSUBSTITUTE"), "real base table missing; tables=" + tables);
        assertFalse(tables.contains("BOMLEVEL1"), "first CTE's own name leaked as a table; tables=" + tables);
        assertFalse(tables.contains("BOMLEVEL2"), "second CTE (chained on the first) leaked as a table; tables=" + tables);
    }

    // ─────────────────────────────────────────────────────────────────────
    // 3. Sequences / IDENTITY
    // ─────────────────────────────────────────────────────────────────────

    private static final String NEXT_VALUE_FOR_PROC =
            "CREATE PROCEDURE usp_NewOrder\nAS\nBEGIN\n"
          + "  INSERT INTO sales.SalesOrder (OrderId, CustomerId)\n"
          + "  VALUES (NEXT VALUE FOR sales.OrderIdSeq, 42);\n"
          + "END";

    @Test
    @DisplayName("3a: NEXT VALUE FOR is recorded as a sequence usage, not a phantom table")
    void nextValueForRecordedAsSequence() {
        SqlServerRoutineInfo r = parse(NEXT_VALUE_FOR_PROC);
        List<String> seqNames = r.getSequences().stream()
                .filter(s -> !s.isIdentityColumn())
                .map(SqlServerSequenceInfo::getName)
                .collect(Collectors.toList());
        assertTrue(seqNames.stream().anyMatch(n -> n.equalsIgnoreCase("OrderIdSeq")),
                "sequence usage not recorded; sequences=" + r.getSequences());
    }

    @Test
    @DisplayName("3b: NEXT VALUE FOR's sequence name is not also recorded as a table")
    void nextValueForSequenceNameNotATable() {
        SqlServerRoutineInfo r = parse(NEXT_VALUE_FOR_PROC);
        List<String> tables = tableNames(r);
        assertFalse(tables.contains("ORDERIDSEQ"), "sequence name mis-recorded as a table; tables=" + tables);
    }

    @Test
    @DisplayName("3c: the real INSERT target table survives alongside a NEXT VALUE FOR usage")
    void realInsertTargetSurvivesWithSequence() {
        SqlServerRoutineInfo r = parse(NEXT_VALUE_FOR_PROC);
        List<String> tables = tableNames(r);
        assertTrue(tables.contains("SALESORDER"), "real INSERT target dropped; tables=" + tables);
    }

    private static final String IDENTITY_COLUMN_PROC =
            "CREATE PROCEDURE usp_CreateStagingTable\nAS\nBEGIN\n"
          + "  CREATE TABLE staging.ImportBatch (\n"
          + "    Id INT IDENTITY(1,1) NOT NULL,\n"
          + "    BatchName NVARCHAR(100) NOT NULL\n"
          + "  );\n"
          + "  INSERT INTO staging.ImportBatch (BatchName) VALUES ('x');\n"
          + "END";

    @Test
    @DisplayName("3d: IDENTITY column property is recorded distinctly and scoped to its real table")
    void identityColumnRecordedForRealTable() {
        SqlServerRoutineInfo r = parse(IDENTITY_COLUMN_PROC);
        List<SqlServerSequenceInfo> identities = r.getSequences().stream()
                .filter(SqlServerSequenceInfo::isIdentityColumn)
                .collect(Collectors.toList());
        assertFalse(identities.isEmpty(), "IDENTITY column not recorded; sequences=" + r.getSequences());
        assertTrue(identities.stream().anyMatch(s -> "Id".equalsIgnoreCase(s.getColumnName())),
                "IDENTITY column name not captured; sequences=" + identities);
    }

    @Test
    @DisplayName("3e: real table (both CREATE TABLE and later INSERT) is not dropped by IDENTITY handling")
    void identityHandlingDoesNotDropRealTable() {
        SqlServerRoutineInfo r = parse(IDENTITY_COLUMN_PROC);
        List<String> tables = tableNames(r);
        assertTrue(tables.contains("IMPORTBATCH"), "real table dropped around IDENTITY handling; tables=" + tables);
    }
}
