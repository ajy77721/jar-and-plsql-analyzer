package com.plsql.parser.multidb;

import com.plsql.parser.model.ForeignKeyInfo;
import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The multidb bridge must carry declared FOREIGN KEY constraints across for Postgres too.
 *
 * <p>Postgres previously kept an FK only as the bare string {@code "child->parent"}, which names
 * the two tables and discards the column pairing that is the constraint's actual content — even
 * though those columns were already in the parse tree. {@link PostgresProcedureParser} owns the
 * only conversion from the dialect model to the shared {@link ParsedObject} that everything
 * downstream consumes, so a field it does not copy is data that exists and is unreachable: no
 * artifact carries it, therefore no Mongo collection and no screen can.
 */
class PostgresForeignKeyBridgeTest {

    private final PostgresProcedureParser parser = new PostgresProcedureParser();

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
        assertEquals("orders", fk.getTable());
        assertEquals("customers", fk.getReferencedTable());
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
                CREATE PROCEDURE get_orders() AS $$
                BEGIN
                  SELECT * FROM orders;
                END;
                $$ LANGUAGE plpgsql;
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
