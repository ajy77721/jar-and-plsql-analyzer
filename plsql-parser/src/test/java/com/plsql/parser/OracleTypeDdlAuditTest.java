package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-19): CREATE TYPE / CREATE TYPE BODY support against the real Oracle
 * ANTLR grammar/visitor ({@link PlSqlParserEngine} -&gt; {@code PlSqlAnalysisVisitor}).
 *
 * <p>Grammar-level findings (plsql-parser/src/main/antlr4/com/plsql/parser/PlSqlParser.g4):
 * <ul>
 *   <li>{@code create_type} exists (around line 988):
 *       {@code CREATE (OR REPLACE)? (EDITIONABLE|NONEDITIONABLE)? TYPE (type_definition|type_body)}
 *       and IS wired into the top-level {@code unit_statement} alternation (around line 120,
 *       alphabetically between create_trigger and create_user). So structurally the grammar can
 *       parse OBJECT types, collection (TABLE OF) types, and TYPE BODY.</li>
 * </ul>
 *
 * <p>Visitor-level findings (plsql-parser/src/main/java/com/plsql/parser/visitor/
 * PlSqlAnalysisVisitor.java): grep for "CreateType", "TypeDefinition", "TypeBody" returns ZERO
 * matches. There is no {@code visitCreate_type}, {@code visitType_definition}, or
 * {@code visitType_body} override anywhere — unlike CREATE SEQUENCE/CREATE TRIGGER/CREATE VIEW,
 * which build a dedicated {@link ParsedObject}. So a standalone CREATE TYPE falls through to the
 * default ANTLR {@code visitChildren} traversal: it parses without error (grammar supports it)
 * but produces NO distinct TYPE ParsedObject in {@code ParseResult.getObjects()} — confirmed
 * empirically below.
 */
public class OracleTypeDdlAuditTest extends ParserTestBase {

    private ParsedObject findObject(ParseResult result, String type, String name) {
        for (ParsedObject po : result.getObjects()) {
            if (type.equalsIgnoreCase(po.getType())
                    && (name == null || name.equalsIgnoreCase(po.getName()))) {
                return po;
            }
        }
        return null;
    }

    private void dumpAllObjects(String label, ParseResult result) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + result.getErrors());
        if (result.getObjects() == null || result.getObjects().isEmpty()) {
            System.out.println("  (NO ParsedObject captured at all)");
            return;
        }
        for (ParsedObject po : result.getObjects()) {
            System.out.println("  captured object: type=" + po.getType() + " name=" + po.getName());
        }
    }

    @Test
    @DisplayName("1: CREATE TYPE t_address AS OBJECT (street VARCHAR2(100), city VARCHAR2(50))")
    void createObjectType() {
        String sql = """
                CREATE TYPE t_address AS OBJECT (
                    street VARCHAR2(100),
                    city VARCHAR2(50)
                );
                """;
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE TYPE ... AS OBJECT");
        dumpAllObjects("1: CREATE TYPE AS OBJECT", result);

        ParsedObject po = findObject(result, "TYPE", "T_ADDRESS");
        Assertions.assertNotNull(po, "CREATE TYPE ... AS OBJECT must produce a TYPE ParsedObject");
        Assertions.assertEquals("OBJECT", po.getTypeCategory());
        Assertions.assertNull(po.getTypeFinal(), "no FINAL/NOT FINAL modifier was written");
    }

    @Test
    @DisplayName("2: CREATE TYPE t_address_list AS TABLE OF t_address (collection type)")
    void createCollectionType() {
        String sql = "CREATE TYPE t_address_list AS TABLE OF t_address;\n";
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE TYPE ... AS TABLE OF");
        dumpAllObjects("2: CREATE TYPE AS TABLE OF", result);

        ParsedObject po = findObject(result, "TYPE", "T_ADDRESS_LIST");
        Assertions.assertNotNull(po, "CREATE TYPE ... AS TABLE OF must produce a TYPE ParsedObject");
        Assertions.assertEquals("NESTED_TABLE", po.getTypeCategory());
        Assertions.assertEquals("T_ADDRESS", po.getTypeElementType().toUpperCase());
    }

    @Test
    @DisplayName("3: CREATE TYPE t_shape AS OBJECT (...) NOT FINAL")
    void createNotFinalObjectType() {
        String sql = """
                CREATE TYPE t_shape AS OBJECT (
                    sides NUMBER
                ) NOT FINAL;
                """;
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE TYPE ... NOT FINAL");
        dumpAllObjects("3: CREATE TYPE NOT FINAL", result);

        ParsedObject po = findObject(result, "TYPE", "T_SHAPE");
        Assertions.assertNotNull(po, "CREATE TYPE ... NOT FINAL must produce a TYPE ParsedObject");
        Assertions.assertEquals("OBJECT", po.getTypeCategory());
        Assertions.assertEquals(Boolean.FALSE, po.getTypeFinal(), "NOT FINAL must be captured");
    }

    @Test
    @DisplayName("5: TYPE BODY with MEMBER PROCEDURE doing DML — subprogram table ops captured")
    void typeBodyMemberProcedureCapturesTableOps() {
        String sql = """
                CREATE TYPE BODY t_audit AS
                    MEMBER PROCEDURE log_it(p_msg VARCHAR2) IS
                    BEGIN
                        INSERT INTO audit_log (msg) VALUES (p_msg);
                    END;
                END;
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CREATE TYPE BODY with MEMBER PROCEDURE");
        dumpAllObjects("5: TYPE BODY MEMBER PROCEDURE", result);

        ParsedObject po = findObject(result, "TYPE_BODY", "T_AUDIT");
        Assertions.assertNotNull(po);
        Assertions.assertEquals(1, po.getSubprograms().size());
        SubprogramInfo si = po.getSubprograms().get(0);
        Assertions.assertEquals("LOG_IT", si.getName().toUpperCase());
        Assertions.assertEquals("PROCEDURE", si.getType());
        Assertions.assertEquals(1, si.getParameters().size());
        Assertions.assertTrue(si.getTableOperations().stream().anyMatch(t ->
                        "AUDIT_LOG".equalsIgnoreCase(t.getTableName())
                                && "INSERT".equalsIgnoreCase(t.getOperation())),
                "the member procedure's INSERT must be captured on the SubprogramInfo: "
                        + si.getTableOperations());
    }

    @Test
    @DisplayName("4: CREATE TYPE BODY t_address AS MEMBER FUNCTION ... END")
    void createTypeBody() {
        String sql = """
                CREATE TYPE BODY t_address AS
                    MEMBER FUNCTION full_address RETURN VARCHAR2 IS
                    BEGIN
                        RETURN street || ', ' || city;
                    END;
                END;
                """;
        ParseResult result = parse(sql);

        assertNoParsErrors(result, "CREATE TYPE BODY");
        dumpAllObjects("4: CREATE TYPE BODY", result);

        ParsedObject po = findObject(result, "TYPE_BODY", "T_ADDRESS");
        Assertions.assertNotNull(po, "CREATE TYPE BODY must produce a TYPE_BODY ParsedObject");
        Assertions.assertEquals(1, po.getSubprograms().size(),
                "the MEMBER FUNCTION must be discovered as a SubprogramInfo");
        SubprogramInfo si = po.getSubprograms().get(0);
        Assertions.assertEquals("FULL_ADDRESS", si.getName().toUpperCase());
        Assertions.assertEquals("FUNCTION", si.getType());
        Assertions.assertEquals("VARCHAR2", si.getReturnType().toUpperCase());
    }
}
