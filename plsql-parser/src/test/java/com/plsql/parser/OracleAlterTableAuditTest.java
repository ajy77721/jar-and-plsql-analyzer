package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (no fixes applied). Standalone ALTER TABLE statements and constraint
 * clauses — lineage capture evidence.
 *
 * Grammar confirmed to structurally support this (PlSqlParser.g4): alter_table (line ~4987),
 * constraint_clauses (ADD out_of_line_constraint incl. FOREIGN KEY ... REFERENCES, ~line 5577),
 * add_column_clause / drop_column_clause (~5286, ~5321).
 *
 * Visitor searched: com.plsql.parser.visitor.PlSqlAnalysisVisitor — grep for
 * "visitAlter_table" / "alter_table" found ZERO overrides for the alter_table parser rule (the
 * only ALTER TABLE-aware code is a regex, DYN_SQL_TABLE_PATTERN, used solely for ALTER TABLE
 * text embedded inside dynamic EXECUTE IMMEDIATE strings — irrelevant to standalone DDL).
 * visitTableview_name (the one place table names are turned into TableOperationInfo /
 * dependency records) is gated on `currentDmlOperation != null`, which is only set by
 * INSERT/UPDATE/DELETE/MERGE/SELECT visitors — never by anything under alter_table. So every
 * tableview_name inside a standalone ALTER TABLE (the altered table itself, and any
 * REFERENCES target) is visited via the ANTLR-generated default visitChildren fall-through and
 * produces no TableOperationInfo, no dependency edge, and no ParsedObject at all.
 *
 * These tests print (and assert on) that observed absence.
 */
@DisplayName("Oracle ALTER TABLE lineage audit")
public class OracleAlterTableAuditTest extends ParserTestBase {

    private static void dump(String label, ParseResult result) {
        System.out.println("=== " + label + " ===");
        System.out.println("errors: " + result.getErrors());
        System.out.println("objects: " + result.getObjects().size());
        for (ParsedObject po : result.getObjects()) {
            System.out.println("  ParsedObject type=" + po.getType() + " name=" + po.getName()
                    + " schema=" + po.getSchema());
            List<TableOperationInfo> ops = po.getTableOperations();
            System.out.println("  tableOperations on this object: " + (ops == null ? 0 : ops.size()));
            if (ops != null) {
                for (TableOperationInfo t : ops) {
                    System.out.println("    -> table=" + t.getTableName() + " schema=" + t.getSchema()
                            + " op=" + t.getOperation());
                }
            }
        }
        System.out.println();
    }

    @Test
    @DisplayName("1a. ALTER TABLE ... ADD COLUMN priority NUMBER (non-Oracle COLUMN keyword)")
    void testAddColumnKeyword() {
        String sql = "ALTER TABLE orders ADD COLUMN priority NUMBER;\n";
        ParseResult result = parse(sql);
        dump("ADD COLUMN (with COLUMN keyword)", result);
        // Oracle's real ADD syntax has no COLUMN keyword; grammar's add_column_clause is
        // "ADD ( column_definition | ... )" with no COLUMN token, so this is expected to fail
        // to parse cleanly under the real Oracle-conformant grammar.
        assertFalse(result.getErrors() == null || result.getErrors().isEmpty(),
                "Expected parse errors for non-Oracle 'ADD COLUMN' syntax, but got none: "
                        + result.getErrors());
    }

    @Test
    @DisplayName("1b. ALTER TABLE ... ADD priority NUMBER (real Oracle syntax, no COLUMN keyword)")
    void testAddColumnNoKeyword() {
        String sql = "ALTER TABLE orders ADD priority NUMBER;\n";
        ParseResult result = parse(sql);
        dump("ADD (real Oracle syntax)", result);
        assertNoParsErrors(result, "ALTER TABLE ADD priority NUMBER");
        boolean captured = result.getObjects().stream().anyMatch(o ->
                "ORDERS".equalsIgnoreCase(o.getName())
                        || (o.getTableOperations() != null && o.getTableOperations().stream()
                                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))));
        System.out.println("Altered table 'orders' captured anywhere in ParseResult? " + captured);
        assertTrue(captured, "altered table ORDERS must be captured for ALTER TABLE ... ADD column");
    }

    @Test
    @DisplayName("2. KEY CASE: ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY ... REFERENCES customers(id)")
    void testAddForeignKeyConstraint() {
        String sql = "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) "
                + "REFERENCES customers(id);\n";
        ParseResult result = parse(sql);
        dump("ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES", result);
        assertNoParsErrors(result, "ALTER TABLE ADD CONSTRAINT FOREIGN KEY");

        boolean ordersCaptured = result.getObjects().stream().anyMatch(o ->
                "ORDERS".equalsIgnoreCase(o.getName())
                        || (o.getTableOperations() != null && o.getTableOperations().stream()
                                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))));
        boolean customersCaptured = result.getObjects().stream().anyMatch(o ->
                "CUSTOMERS".equalsIgnoreCase(o.getName())
                        || (o.getTableOperations() != null && o.getTableOperations().stream()
                                .anyMatch(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()))));
        System.out.println("Altered table 'orders' captured? " + ordersCaptured);
        System.out.println("Referenced table 'customers' captured? " + customersCaptured);
        System.out.println("Total ParsedObjects produced for this statement: " + result.getObjects().size());

        assertTrue(ordersCaptured, "'orders' (altered table) must be captured");
        assertTrue(customersCaptured, "'customers' (FK REFERENCES target) must be captured");
        assertEquals(1, result.getObjects().size(), "one ALTER_TABLE ParsedObject expected");

        ParsedObject po = result.getObjects().get(0);
        assertEquals("ALTER_TABLE", po.getType());
        assertEquals("ORDERS", po.getName().toUpperCase());

        TableOperationInfo alterOp = po.getTableOperations().stream()
                .filter(t -> "ALTER".equals(t.getOperation())).findFirst().orElse(null);
        assertNotNull(alterOp, "an ALTER table operation for ORDERS is expected");
        assertEquals("ORDERS", alterOp.getTableName().toUpperCase());

        TableOperationInfo refOp = po.getTableOperations().stream()
                .filter(t -> "REFERENCE".equals(t.getOperation())).findFirst().orElse(null);
        assertNotNull(refOp, "a REFERENCE table operation for CUSTOMERS is expected");
        assertEquals("CUSTOMERS", refOp.getTableName().toUpperCase());

        // The relationship edge itself, modelled with the same JoinInfo used for query joins.
        assertEquals(1, alterOp.getJoins().size(), "one FK relationship edge expected");
        JoinInfo edge = alterOp.getJoins().get(0);
        assertEquals("FOREIGN KEY", edge.getJoinType());
        assertEquals("ORDERS", edge.getLeftTable().toUpperCase());
        assertEquals("CUSTOMERS", edge.getJoinedTable().toUpperCase());
        assertTrue(edge.getCondition().toUpperCase().contains("CUSTOMER_ID"),
                "edge condition should name the FK columns: " + edge.getCondition());

        // ...and, in addition to that edge, the structured dialect-neutral ForeignKeyInfo that
        // the other four dialects bridge into ParsedObject.foreignKeys. Both column lists are
        // kept intact here rather than only rendered into the edge's condition string.
        assertEquals(1, po.getForeignKeys().size(), "one structured ForeignKeyInfo expected");
        ForeignKeyInfo fk = po.getForeignKeys().get(0);
        assertEquals("FK_CUSTOMER", fk.getConstraintName().toUpperCase());
        assertEquals("ORDERS", fk.getTable().toUpperCase());
        assertNull(fk.getSchema());
        assertEquals("CUSTOMERS", fk.getReferencedTable().toUpperCase());
        assertNull(fk.getReferencedSchema());
        assertEquals(List.of("CUSTOMER_ID"),
                fk.getColumns().stream().map(String::toUpperCase).toList());
        assertEquals(List.of("ID"),
                fk.getReferencedColumns().stream().map(String::toUpperCase).toList());
        assertEquals(1, fk.getLine());
    }

    @Test
    @DisplayName("2b. Multi-column, schema-qualified FK populates ParsedObject.foreignKeys")
    void testForeignKeyStructuredMultiColumn() {
        String sql = "ALTER TABLE sales.orders ADD CONSTRAINT fk_cust "
                + "FOREIGN KEY (cust_id, region_id) "
                + "REFERENCES crm.customers(id, region);\n";
        ParseResult result = parse(sql);
        dump("multi-column schema-qualified FK", result);
        assertNoParsErrors(result, "multi-column schema-qualified FK");

        ParsedObject po = result.getObjects().get(0);
        assertEquals(1, po.getForeignKeys().size());
        ForeignKeyInfo fk = po.getForeignKeys().get(0);
        assertEquals("FK_CUST", fk.getConstraintName().toUpperCase());
        assertEquals("ORDERS", fk.getTable().toUpperCase());
        assertEquals("SALES", fk.getSchema().toUpperCase());
        assertEquals("CUSTOMERS", fk.getReferencedTable().toUpperCase());
        assertEquals("CRM", fk.getReferencedSchema().toUpperCase());
        assertEquals(List.of("CUST_ID", "REGION_ID"),
                fk.getColumns().stream().map(String::toUpperCase).toList());
        assertEquals(List.of("ID", "REGION"),
                fk.getReferencedColumns().stream().map(String::toUpperCase).toList());
    }

    @Test
    @DisplayName("3. ADD CONSTRAINT PRIMARY KEY / CHECK / UNIQUE — altered table capture")
    void testOtherConstraintTypes() {
        String[] stmts = {
                "ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id);",
                "ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0);",
                "ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code);"
        };
        for (String stmt : stmts) {
            ParseResult result = parse(stmt + "\n");
            dump("ADD CONSTRAINT: " + stmt, result);
            assertNoParsErrors(result, stmt);
            boolean ordersCaptured = result.getObjects().stream().anyMatch(o ->
                    "ORDERS".equalsIgnoreCase(o.getName())
                            || (o.getTableOperations() != null && o.getTableOperations().stream()
                                    .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))));
            System.out.println("[" + stmt + "] altered table captured? " + ordersCaptured
                    + " ; objects=" + result.getObjects().size());
            assertTrue(ordersCaptured, "'orders' must be captured for: " + stmt);
            assertEquals(1, result.getObjects().size(), "one ALTER_TABLE ParsedObject for: " + stmt);
            assertTrue(result.getObjects().get(0).getTableOperations().stream()
                            .noneMatch(t -> "REFERENCE".equals(t.getOperation())),
                    "no FK edge expected for a non-FK constraint: " + stmt);
        }
    }

    @Test
    @DisplayName("4. DROP COLUMN / DROP CONSTRAINT — altered table capture")
    void testDropClauses() {
        String[] stmts = {
                "ALTER TABLE orders DROP COLUMN priority;",
                "ALTER TABLE orders DROP CONSTRAINT fk_customer;"
        };
        for (String stmt : stmts) {
            ParseResult result = parse(stmt + "\n");
            dump("DROP clause: " + stmt, result);
            assertNoParsErrors(result, stmt);
            boolean ordersCaptured = result.getObjects().stream().anyMatch(o ->
                    "ORDERS".equalsIgnoreCase(o.getName())
                            || (o.getTableOperations() != null && o.getTableOperations().stream()
                                    .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))));
            System.out.println("[" + stmt + "] altered table captured? " + ordersCaptured
                    + " ; objects=" + result.getObjects().size());
            assertTrue(ordersCaptured, "'orders' must be captured for: " + stmt);
            assertEquals(1, result.getObjects().size(), "one ALTER_TABLE ParsedObject for: " + stmt);
            assertTrue(result.getObjects().get(0).getTableOperations().stream()
                            .noneMatch(t -> "REFERENCE".equals(t.getOperation())),
                    "no FK edge expected for a non-FK constraint: " + stmt);
        }
    }
}
