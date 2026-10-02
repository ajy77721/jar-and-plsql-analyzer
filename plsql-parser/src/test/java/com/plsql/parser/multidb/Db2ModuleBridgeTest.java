package com.plsql.parser.multidb;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Task #28 — DB2 modules through the shared model. Verifies that a module member carries the
 * module as its package slot (so the node keys as MODULE.SUB, mirroring Oracle PACKAGE.SUB) and
 * that module-level variables land in {@code packageVariables} (the globals facet's declaration
 * list).
 */
class Db2ModuleBridgeTest {

    private Db2ProcedureParser parser;

    @BeforeEach
    void setUp() {
        parser = new Db2ProcedureParser();
    }

    private ParsedObject single(ParseResult r) {
        assertEquals(1, r.getObjects().size());
        return r.getObjects().get(0);
    }

    @Test
    @DisplayName("#28: a module member carries the module as its package slot → MODULE.SUB key")
    void moduleMemberKeysAsModuleSub() {
        String src = """
                ALTER MODULE ERP_MOD_PRICING PUBLISH
                    FUNCTION NET_PRICE(p_product_id INTEGER)
                    RETURNS DECIMAL(16,2)
                    BEGIN
                        RETURN 0;
                    END
                """;
        ParsedObject obj = single(parser.parse(src, "NET_PRICE", "DB2INST1"));

        assertEquals("NET_PRICE", obj.getName());
        assertEquals("FUNCTION", obj.getType());
        assertEquals("ERP_MOD_PRICING", obj.getPackageName(),
                "module member carries its module as the package slot");

        // The node id mirrors Oracle's PACKAGE.SUB: SCHEMA$MODULE$SUB.
        FlowNode node = new FlowNode();
        node.setSchema(obj.getSchema());
        node.setPackageName(obj.getPackageName());
        node.setObjectName(obj.getName());
        assertEquals("DB2INST1$ERP_MOD_PRICING$NET_PRICE", node.buildShortId());
    }

    @Test
    @DisplayName("#28: module variables become packageVariables (globals facet declarations)")
    void moduleVariablesBecomePackageVariables() {
        String src = """
                CREATE MODULE ERP_MOD_PRICING;
                    CREATE VARIABLE ERP_MOD_PRICING.G_REPRICE_COUNT INTEGER DEFAULT 0;
                    CREATE VARIABLE ERP_MOD_PRICING.G_LAST_TENANT VARCHAR(20) DEFAULT 'DEFAULT';
                END MODULE
                """;
        ParsedObject obj = single(parser.parse(src, "ERP_MOD_PRICING", "DB2INST1"));

        assertEquals("MODULE", obj.getType());
        assertEquals("ERP_MOD_PRICING", obj.getName());
        assertNull(obj.getPackageName(), "the module container is not re-qualified by itself");
        assertEquals(2, obj.getPackageVariables().size());
        VariableInfo first = obj.getPackageVariables().get(0);
        assertEquals("G_REPRICE_COUNT", first.getName());
        assertEquals("INTEGER", first.getDataType());
        assertEquals("0", first.getDefaultValue());
    }

    @Test
    @DisplayName("#28: top-level CREATE VARIABLE is captured as a module/global variable")
    void topLevelCreateVariableBecomesPackageVariable() {
        ParsedObject obj = single(parser.parse(
                "CREATE VARIABLE ERP_GV_RUN_ID INTEGER DEFAULT 0", "ERP_GV_RUN_ID", "DB2INST1"));
        assertEquals(1, obj.getPackageVariables().size());
        assertEquals("ERP_GV_RUN_ID", obj.getPackageVariables().get(0).getName());
    }

    @Test
    @DisplayName("#28: a non-module routine is unchanged (no packageName)")
    void nonModuleRoutineUnchanged() {
        String src = "CREATE PROCEDURE get_orders()\nLANGUAGE SQL\nBEGIN\n  SELECT * FROM orders;\nEND";
        ParsedObject obj = single(parser.parse(src, "get_orders", "mydb"));
        assertNull(obj.getPackageName(), "standalone routine keeps no package slot");
        assertEquals("get_orders", obj.getName());
        assertEquals("PROCEDURE", obj.getType());
    }
}
