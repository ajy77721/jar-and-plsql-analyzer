package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import com.plsql.postgres.model.PostgresTableOpInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Top-level CREATE TABLE, and table inheritance (INHERITS).
 *
 * ORIGINAL AUDIT FINDINGS:
 *
 *   (1) A standalone `CREATE TABLE t (...);` — i.e. a plain schema script with no enclosing
 *       routine — was NOT a compilationUnit alternative. Every other top-level DDL form
 *       (SEQUENCE / INDEX / VIEW / TRIGGER / TYPE / DOMAIN / ALTER TABLE) was, so CREATE TABLE
 *       was the odd one out: it failed outright with
 *       `line 1:7 no viable alternative at input 'CREATETABLE'` and produced zero table ops.
 *       CREATE TABLE was only reachable INSIDE a routine/DO body.
 *
 *   (2) `CREATE TABLE child (...) INHERITS (parent)` recorded the child as a CREATE table op but
 *       recorded NOTHING pointing at `parent`, so the child->parent lineage edge was lost.
 *
 * FIXED: `createTableTop` is now a compilationUnit alternative sharing `createTableTailItem`
 * with the in-body `createTableStmt`, and `inheritsClause` is recognised in that shared tail.
 * The parent table is recorded exactly the way ALTER TABLE ... REFERENCES already records an FK
 * target — a SELECT-kind table op (a read dependency) plus a verbatim "child->parent" edge —
 * so no new enum value or shared-model surface was needed.
 */
class PostgresCreateTableDdlAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_obj", null);
    }

    private static PostgresRoutineInfo dump(String label, PostgresRoutineInfo r) {
        System.out.println("[" + label + "] name=" + r.getName() + " type=" + r.getType()
                + " ops=" + ops(r) + " inherits=" + r.getTableInheritsRefs()
                + " errors=" + r.getErrors());
        return r;
    }

    private static List<String> ops(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .map(o -> o.getDmlType() + ":" + o.getTableName())
                .collect(Collectors.toList());
    }

    private static PostgresTableOpInfo op(PostgresRoutineInfo r,
                                          PostgresTableOpInfo.DmlType kind, String table) {
        return r.getTableOps().stream()
                .filter(o -> o.getDmlType() == kind && table.equalsIgnoreCase(o.getTableName()))
                .findFirst()
                .orElse(null);
    }

    // ── (1) top-level CREATE TABLE ───────────────────────────────────────────

    @Test
    void topLevelCreateTableParsesWithoutErrors() {
        PostgresRoutineInfo r = dump("topLevelPlain", parse(
                "CREATE TABLE orders (id INT PRIMARY KEY, total NUMERIC(10,2));"));
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors, got " + r.getErrors());
    }

    @Test
    void topLevelCreateTableRecordsCreateOp() {
        PostgresRoutineInfo r = parse("CREATE TABLE orders (id INT, total NUMERIC(10,2));");
        assertEquals(List.of("CREATE:orders"), ops(r));
    }

    @Test
    void topLevelCreateTableClaimsObjectIdentity() {
        PostgresRoutineInfo r = parse("CREATE TABLE orders (id INT);");
        assertEquals(PostgresRoutineInfo.RoutineType.CREATE_TABLE, r.getType());
        assertEquals("orders", r.getName());
    }

    @Test
    void topLevelCreateTableCapturesSchemaQualification() {
        PostgresRoutineInfo r = parse("CREATE TABLE sales.orders (id INT);");
        PostgresTableOpInfo o = op(r, PostgresTableOpInfo.DmlType.CREATE, "orders");
        assertTrue(o != null, "expected a CREATE op for orders, got " + ops(r));
        assertEquals("sales", o.getSchemaName());
        assertEquals("sales", r.getSchema());
    }

    @Test
    void topLevelCreateTableWithoutTrailingSemicolonStillParses() {
        // A script's last statement very often has no terminator; the in-body form still
        // requires one, so this specifically covers the top-level relaxation.
        PostgresRoutineInfo r = dump("topLevelNoSemi", parse("CREATE TABLE orders (id INT)"));
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors, got " + r.getErrors());
        assertEquals(List.of("CREATE:orders"), ops(r));
    }

    @Test
    void topLevelCreateTableIfNotExistsAndTempStillRecognised() {
        PostgresRoutineInfo r = parse("CREATE TEMP TABLE IF NOT EXISTS staging (id INT);");
        assertEquals(List.of("CREATE:staging"), ops(r));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    void topLevelCreateIndexStillRoutesToTheIndexRule() {
        // Guard against the new CREATE TABLE alternative stealing other top-level CREATEs.
        PostgresRoutineInfo r = parse("CREATE UNIQUE INDEX idx_orders ON orders (id);");
        assertEquals(PostgresRoutineInfo.RoutineType.INDEX, r.getType());
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
    }

    @Test
    void inBodyCreateTableIsUnchanged() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE setup() LANGUAGE plpgsql AS $$ BEGIN "
              + "CREATE TABLE staging (id INT); END; $$;");
        assertEquals(PostgresRoutineInfo.RoutineType.PROCEDURE, r.getType());
        assertEquals(List.of("CREATE:staging"), ops(r));
    }

    // ── (2) INHERITS ─────────────────────────────────────────────────────────

    @Test
    void inheritsRecordsParentAsReadDependency() {
        PostgresRoutineInfo r = dump("inheritsSingle", parse(
                "CREATE TABLE child (extra TEXT) INHERITS (parent);"));
        assertTrue(r.getErrors().isEmpty(), "errors=" + r.getErrors());
        assertTrue(op(r, PostgresTableOpInfo.DmlType.CREATE, "child") != null,
                "expected CREATE op for child, got " + ops(r));
        assertTrue(op(r, PostgresTableOpInfo.DmlType.SELECT, "parent") != null,
                "expected SELECT (read-dependency) op for parent, got " + ops(r));
    }

    @Test
    void inheritsRecordsTheLineageEdge() {
        PostgresRoutineInfo r = parse("CREATE TABLE child (extra TEXT) INHERITS (parent);");
        assertEquals(List.of("child->parent"), r.getTableInheritsRefs());
    }

    @Test
    void multipleInheritanceRecordsEveryParent() {
        PostgresRoutineInfo r = dump("inheritsMulti", parse(
                "CREATE TABLE child (extra TEXT) INHERITS (p1, sales.p2);"));
        assertEquals(List.of("child->p1", "child->p2"), r.getTableInheritsRefs());
        PostgresTableOpInfo p2 = op(r, PostgresTableOpInfo.DmlType.SELECT, "p2");
        assertTrue(p2 != null, "expected SELECT op for p2, got " + ops(r));
        assertEquals("sales", p2.getSchemaName());
    }

    @Test
    void inheritsWorksForAnInBodyCreateTableToo() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE setup() LANGUAGE plpgsql AS $$ BEGIN "
              + "CREATE TABLE child (extra TEXT) INHERITS (parent); END; $$;");
        assertEquals(List.of("child->parent"), r.getTableInheritsRefs());
        assertTrue(op(r, PostgresTableOpInfo.DmlType.SELECT, "parent") != null,
                "expected SELECT op for parent, got " + ops(r));
    }

    @Test
    void aColumnNamedInheritsIsNotMistakenForInheritance() {
        // INHERITS is matched by a case-insensitive predicate on ID (it is not a reserved
        // keyword token), so an ordinary column of that name must still be inert.
        PostgresRoutineInfo r = dump("inheritsAsColumnName", parse(
                "CREATE TABLE t (inherits TEXT, other INT);"));
        assertTrue(r.getTableInheritsRefs().isEmpty(),
                "expected no inheritance edge, got " + r.getTableInheritsRefs());
        assertEquals(List.of("CREATE:t"), ops(r));
    }

    @Test
    void createTableWithoutInheritsRecordsNoEdge() {
        PostgresRoutineInfo r = parse("CREATE TABLE orders (id INT);");
        assertTrue(r.getTableInheritsRefs().isEmpty(),
                "expected no inheritance edge, got " + r.getTableInheritsRefs());
    }
}
