package com.plsql.parser.multidb;

import com.plsql.parser.model.ForeignKeyInfo;
import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The multidb bridge must carry declared FOREIGN KEY constraints across for DB2 too.
 *
 * <p>DB2 previously kept these only as a rendered descriptor string in
 * {@code Db2RoutineInfo.alterActions} plus a bare list of referenced table names — the column
 * pairing, which is the whole content of the constraint, was discarded even though both lists
 * are present in the parse tree. Now the visitor builds a {@code Db2ForeignKeyInfo} and
 * {@link Db2ProcedureParser} — which owns the only conversion from the dialect model to the
 * shared {@link ParsedObject} that everything downstream consumes — copies it. This test is the
 * bridge itself, because a conversion that silently drops one field is precisely the failure the
 * dialect tests cannot see (they assert on the dialect model).
 */
class Db2ForeignKeyBridgeTest {

    private final Db2ProcedureParser parser = new Db2ProcedureParser();

    private static ParsedObject single(ParseResult r) {
        assertEquals(1, r.getObjects().size(), "expected exactly one parsed object");
        return r.getObjects().get(0);
    }

    @Test
    @DisplayName("named single-column FK reaches ParsedObject with both column lists")
    void namedForeignKeyCrossesTheBridge() {
        String src = """
                ALTER TABLE orders
                  ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id);
                """;
        ParsedObject obj = single(parser.parse(src, "alter_orders", "sales"));

        List<ForeignKeyInfo> fks = obj.getForeignKeys();
        assertEquals(1, fks.size(), "the FK must survive the dialect -> shared model conversion");

        ForeignKeyInfo fk = fks.get(0);
        assertEquals("fk_customer", fk.getConstraintName());
        assertEquals("ORDERS", fk.getTable());
        assertEquals("CUSTOMERS", fk.getReferencedTable());
        assertEquals(List.of("customer_id"), fk.getColumns());
        assertEquals(List.of("id"), fk.getReferencedColumns(),
                "the referenced columns are half the pairing — dropping them loses the constraint");
        assertTrue(fk.getLine() > 0, "a line number makes the constraint navigable in the source");
    }

    @Test
    @DisplayName("composite FK keeps every column, in declaration order")
    void compositeForeignKeyKeepsColumnOrder() {
        String src = """
                ALTER TABLE order_lines
                  ADD FOREIGN KEY (order_id, line_no) REFERENCES orders(id, line_no);
                """;
        ParsedObject obj = single(parser.parse(src, "alter_order_lines", "sales"));

        assertEquals(1, obj.getForeignKeys().size());
        ForeignKeyInfo fk = obj.getForeignKeys().get(0);
        assertEquals(List.of("order_id", "line_no"), fk.getColumns());
        assertEquals(List.of("id", "line_no"), fk.getReferencedColumns());
        assertNull(fk.getConstraintName(), "an unnamed constraint is still a constraint");
    }

    @Test
    @DisplayName("a procedure that declares no constraints yields an empty list, never null")
    void noForeignKeysIsEmptyNotNull() {
        String src = """
                CREATE PROCEDURE get_orders()
                BEGIN
                  SELECT * FROM orders;
                END
                """;
        ParsedObject obj = single(parser.parse(src, "get_orders", "sales"));
        assertNotNull(obj.getForeignKeys());
        assertTrue(obj.getForeignKeys().isEmpty());
    }

    /**
     * The shared list must be an independent copy. Handing the dialect model's own list across
     * would let anything that mutates the shared object reach back into the parse result.
     */
    @Test
    void sharedListIsIndependentOfTheDialectModel() {
        String src = "ALTER TABLE a ADD CONSTRAINT fk_b FOREIGN KEY (b_id) REFERENCES b(id);";
        ParsedObject obj = single(parser.parse(src, "alter_a", null));
        ForeignKeyInfo fk = obj.getForeignKeys().get(0);
        fk.getColumns().add("injected");
        assertEquals(2, fk.getColumns().size(),
                "the shared model owns its own mutable copy of the column list");
    }
}
