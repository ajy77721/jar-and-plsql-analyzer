package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2VariableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Task #28 — DB2 modules (the package analog), grammar-first.
 *
 * <p>Golden fixture mirrors the seed's module form: an empty {@code CREATE MODULE} header plus
 * {@code ALTER MODULE … PUBLISH FUNCTION|PROCEDURE} members (the shape SYSCAT.ROUTINES stores for
 * a module routine, with ROUTINEMODULENAME set), and module-scoped {@code CREATE VARIABLE}
 * declarations.
 */
class Db2ModuleTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    @Test
    @DisplayName("#28: ALTER MODULE PUBLISH FUNCTION carries the module name and member identity")
    void alterModulePublishFunction() {
        String src = """
                ALTER MODULE ERP_MOD_PRICING PUBLISH
                    FUNCTION NET_PRICE(p_product_id INTEGER, p_qty DECIMAL(16,2))
                    RETURNS DECIMAL(16,2)
                    BEGIN
                        DECLARE v_list DECIMAL(16,2) DEFAULT 0;
                        SET v_list = (SELECT COALESCE(list_price,0) FROM ERP_PRODUCT WHERE id = p_product_id);
                        RETURN v_list * COALESCE(p_qty,0);
                    END
                """;
        Db2RoutineInfo r = engine.parse(src, "NET_PRICE", "DB2INST1");

        assertEquals("NET_PRICE", r.getName());
        assertEquals(Db2RoutineInfo.RoutineType.FUNCTION, r.getType());
        assertEquals("ERP_MOD_PRICING", r.getModuleName(), "owning module captured");
        assertTrue(r.getErrors().isEmpty(), "module member parses cleanly: " + r.getErrors());
        // Body is still analyzed: the SELECT on ERP_PRODUCT is captured.
        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        "ERP_PRODUCT".equalsIgnoreCase(t.getTableName())),
                "member body table op captured");
    }

    @Test
    @DisplayName("#28: ALTER MODULE PUBLISH PROCEDURE carries the module name and member identity")
    void alterModulePublishProcedure() {
        String src = """
                ALTER MODULE ERP_MOD_PRICING PUBLISH
                    PROCEDURE REPRICE_ORDER(IN p_so_id INTEGER)
                    BEGIN
                        DECLARE v_pid INTEGER DEFAULT 0;
                        INSERT INTO ERP_AUDIT_LOG (msg) VALUES ('reprice');
                    END
                """;
        Db2RoutineInfo r = engine.parse(src, "REPRICE_ORDER", "DB2INST1");

        assertEquals("REPRICE_ORDER", r.getName());
        assertEquals(Db2RoutineInfo.RoutineType.PROCEDURE, r.getType());
        assertEquals("ERP_MOD_PRICING", r.getModuleName());
        assertTrue(r.getErrors().isEmpty(), "module member parses cleanly: " + r.getErrors());
    }

    @Test
    @DisplayName("#28: CREATE MODULE captures module identity and module variables")
    void createModuleWithVariables() {
        String src = """
                CREATE MODULE ERP_MOD_PRICING;
                    CREATE VARIABLE ERP_MOD_PRICING.G_REPRICE_COUNT INTEGER DEFAULT 0;
                    CREATE VARIABLE ERP_MOD_PRICING.G_LAST_TENANT VARCHAR(20) DEFAULT 'DEFAULT';
                END MODULE
                """;
        Db2RoutineInfo r = engine.parse(src, "ERP_MOD_PRICING", "DB2INST1");

        assertEquals(Db2RoutineInfo.RoutineType.MODULE, r.getType());
        assertEquals("ERP_MOD_PRICING", r.getName());
        assertEquals("ERP_MOD_PRICING", r.getModuleName());
        assertTrue(r.getErrors().isEmpty(), "module parses cleanly: " + r.getErrors());
        assertEquals(2, r.getModuleVariables().size(), "both module variables captured");
        Db2VariableInfo first = r.getModuleVariables().get(0);
        assertEquals("G_REPRICE_COUNT", first.getName());
        assertEquals("INTEGER", first.getDataType());
        assertEquals("0", first.getDefaultValue());
    }

    @Test
    @DisplayName("#28: empty CREATE MODULE header (seed form) parses cleanly")
    void emptyCreateModuleHeader() {
        Db2RoutineInfo r = engine.parse("CREATE MODULE ERP_MOD_PRICING", "ERP_MOD_PRICING", "DB2INST1");
        assertEquals(Db2RoutineInfo.RoutineType.MODULE, r.getType());
        assertEquals("ERP_MOD_PRICING", r.getName());
        assertTrue(r.getErrors().isEmpty(), "empty header parses cleanly: " + r.getErrors());
    }

    @Test
    @DisplayName("#28: top-level CREATE VARIABLE is captured as a module/global variable")
    void topLevelCreateVariable() {
        Db2RoutineInfo r = engine.parse(
                "CREATE VARIABLE ERP_GV_RUN_ID INTEGER DEFAULT 0", "ERP_GV_RUN_ID", "DB2INST1");
        assertEquals(1, r.getModuleVariables().size());
        assertEquals("ERP_GV_RUN_ID", r.getModuleVariables().get(0).getName());
        assertTrue(r.getErrors().isEmpty(), "variable parses cleanly: " + r.getErrors());
    }

    @Test
    @DisplayName("#28: ALTER MODULE PUBLISH VARIABLE is captured as a module variable")
    void alterModulePublishVariable() {
        Db2RoutineInfo r = engine.parse(
                "ALTER MODULE ERP_MOD_PRICING PUBLISH VARIABLE G_COUNT INTEGER DEFAULT 7",
                "ERP_MOD_PRICING", "DB2INST1");
        assertEquals(Db2RoutineInfo.RoutineType.MODULE, r.getType());
        assertEquals("ERP_MOD_PRICING", r.getModuleName());
        assertEquals(1, r.getModuleVariables().size());
        assertEquals("G_COUNT", r.getModuleVariables().get(0).getName());
        assertEquals("7", r.getModuleVariables().get(0).getDefaultValue());
        assertTrue(r.getErrors().isEmpty(), "variable parses cleanly: " + r.getErrors());
    }

    @Test
    @DisplayName("#28: MODULE/VARIABLE/PUBLISH stay usable as ordinary identifiers")
    void moduleWordsRemainIdentifiers() {
        // A column named MODULE / VARIABLE / PUBLISH must not break a routine that selects it.
        String src = """
                CREATE PROCEDURE USE_WORDS()
                BEGIN
                    SELECT module, variable, publish FROM t1;
                END
                """;
        Db2RoutineInfo r = engine.parse(src, "USE_WORDS", "DB2INST1");
        assertTrue(r.getErrors().isEmpty(), "keywords-as-identifiers parse cleanly: " + r.getErrors());
        assertTrue(r.getTableOps().stream().anyMatch(t -> "T1".equalsIgnoreCase(t.getTableName())));
    }
}
