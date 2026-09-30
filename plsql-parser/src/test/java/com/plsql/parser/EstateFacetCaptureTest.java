package com.plsql.parser;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.VariableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The grammar-captured inputs behind the estate facets: a source-parsed trigger's WHEN clause,
 * ROW/STATEMENT level and body, and a package's global variable DECLARATIONS. These are the fields
 * {@code DependencyCrawler} copies onto the node and {@code ChunkedFlowWriter} persists into
 * {@code api/tables/index.json} / {@code api/nodes/*.json} for {@code EstateFacetReader}.
 */
public class EstateFacetCaptureTest extends ParserTestBase {

    private ParsedObject findObject(ParseResult result, String type, String name) {
        for (ParsedObject po : result.getObjects()) {
            if (type.equalsIgnoreCase(po.getType())
                    && (name == null || name.equalsIgnoreCase(po.getName()))) {
                return po;
            }
        }
        return null;
    }

    @Test
    @DisplayName("FOR EACH ROW trigger captures level=ROW, its WHEN condition and its body")
    void rowTriggerCapturesWhenLevelBody() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_orders_biu
                BEFORE INSERT OR UPDATE ON orders
                FOR EACH ROW
                WHEN (NEW.status = 'X')
                BEGIN
                  :NEW.last_modified := SYSDATE;
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FOR EACH ROW WHEN trigger");
        ParsedObject po = findObject(result, "TRIGGER", "TRG_ORDERS_BIU");
        assertNotNull(po, "trigger object must be captured");
        assertEquals("ROW", po.getTriggerLevel());
        assertEquals("NEW.status = 'X'", po.getTriggerWhenClause(),
                "the WHEN condition text, without the wrapping WHEN ( )");
        assertNotNull(po.getTriggerBody(), "trigger body must be captured");
        assertTrue(po.getTriggerBody().toUpperCase().contains("BEGIN"), "body carries the block");
    }

    @Test
    @DisplayName("statement-level trigger (no FOR EACH ROW) captures level=STATEMENT")
    void statementTriggerCapturesStatementLevel() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_orders_stmt
                AFTER INSERT ON orders
                BEGIN
                  INSERT INTO audit_log(action) VALUES ('INSERT');
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "statement trigger");
        ParsedObject po = findObject(result, "TRIGGER", "TRG_ORDERS_STMT");
        assertNotNull(po, "trigger object must be captured");
        assertEquals("STATEMENT", po.getTriggerLevel());
        assertNull(po.getTriggerWhenClause(), "no WHEN clause on this trigger");
        assertNotNull(po.getTriggerBody());
    }

    @Test
    @DisplayName("package body captures package-level variable declarations (name/type/default/constant)")
    void packageBodyCapturesGlobalDeclarations() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY pkg_globals AS
                  g_counter NUMBER := 7;
                  g_name CONSTANT VARCHAR2(20) := 'X';
                  PROCEDURE bump IS
                  BEGIN
                    g_counter := g_counter + 1;
                  END bump;
                END pkg_globals;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "package body with globals");
        ParsedObject po = findObject(result, "PACKAGE_BODY", "PKG_GLOBALS");
        assertNotNull(po, "package body object must be captured");

        VariableInfo counter = null, name = null;
        for (VariableInfo v : po.getPackageVariables()) {
            if ("G_COUNTER".equalsIgnoreCase(v.getName())) counter = v;
            if ("G_NAME".equalsIgnoreCase(v.getName())) name = v;
        }
        assertNotNull(counter, "G_COUNTER must be a package-level declaration");
        assertEquals("NUMBER", counter.getDataType());
        assertNotNull(counter.getDefaultValue());
        assertTrue(counter.getDefaultValue().contains("7"), "default value carries the 7");
        assertFalse(counter.isConstant());

        assertNotNull(name, "G_NAME must be a package-level declaration");
        assertTrue(name.isConstant(), "CONSTANT must be captured");
        assertNotNull(name.getDefaultValue());
        assertTrue(name.getDefaultValue().contains("'X'"), "default value carries the literal");
    }
}
