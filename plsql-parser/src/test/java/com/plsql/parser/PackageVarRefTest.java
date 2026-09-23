package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for cross-package global-variable reference detection
 * (Step 1: standalone-procedure gap fix) and the structured
 * PackageVarRefInfo data model (Step 2: line number + read/write).
 */
public class PackageVarRefTest extends ParserTestBase {

    /**
     * Like ParserTestBase.findSub, but only considers PACKAGE_BODY objects, since
     * a package spec's forward-declared subprogram entry never carries body-only
     * data such as external package var refs, and would otherwise shadow the
     * real (body) entry when iteration order returns the spec first.
     */
    private SubprogramInfo findSubInBody(ParseResult result, String name) {
        for (ParsedObject obj : result.getObjects()) {
            if (!"PACKAGE_BODY".equals(obj.getType())) continue;
            for (SubprogramInfo sub : obj.getSubprograms()) {
                if (sub.getName() != null && sub.getName().equalsIgnoreCase(name)) {
                    return sub;
                }
            }
        }
        return null;
    }

    private ParsedObject findStandaloneObject(ParseResult result, String name) {
        for (ParsedObject obj : result.getObjects()) {
            if (obj.getName() != null && obj.getName().equalsIgnoreCase(name)
                    && (obj.getSubprograms() == null || obj.getSubprograms().isEmpty())) {
                return obj;
            }
        }
        return null;
    }

    /**
     * Step 1 regression: a STANDALONE procedure (not inside any package) reads
     * another package's global variable. Mirrors db-setup/seed/oracle.sql:1092
     * (erp_usp_finalize_gl_batch reading erp_pkg_planner.g_last_context).
     * Only detectable because erp_pkg_planner's spec/body appear earlier in the
     * same single-pass parse and register g_last_context as a known global.
     */
    @Test
    void standaloneProcedureReadingOtherPackageGlobal_isDetected() {
        String sql =
            "CREATE OR REPLACE PACKAGE erp_pkg_planner AS\n" +
            "  g_last_context VARCHAR2(60);\n" +
            "  PROCEDURE set_context(p_ctx VARCHAR2);\n" +
            "END erp_pkg_planner;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE BODY erp_pkg_planner AS\n" +
            "  PROCEDURE set_context(p_ctx VARCHAR2) IS\n" +
            "  BEGIN\n" +
            "    g_last_context := p_ctx;\n" +
            "  END set_context;\n" +
            "END erp_pkg_planner;\n" +
            "/\n" +
            "CREATE OR REPLACE PROCEDURE erp_usp_finalize_gl_batch(p_batch_id IN NUMBER) IS\n" +
            "  v_tot NUMBER := 0;\n" +
            "BEGIN\n" +
            "  erp_pkg_planner.set_context('CYCLE');\n" +
            "  DBMS_OUTPUT.PUT_LINE('ctx='||erp_pkg_planner.g_last_context);\n" +
            "END erp_usp_finalize_gl_batch;\n" +
            "/\n";

        ParseResult result = parse(sql);
        assertNoParsErrors(result, "standalone-package-global-read");

        ParsedObject standalone = findStandaloneObject(result, "erp_usp_finalize_gl_batch");
        assertNotNull(standalone, "standalone procedure should be parsed");

        assertTrue(standalone.getExternalPackageVarRefs().stream()
                .anyMatch(r -> r.equalsIgnoreCase("erp_pkg_planner.g_last_context")),
            "expected standalone procedure to record external package var ref, got: "
                + standalone.getExternalPackageVarRefs());

        PackageVarRefInfo detail = standalone.getExternalPackageVarRefDetails().stream()
            .filter(d -> "ERP_PKG_PLANNER".equalsIgnoreCase(d.getPackageName())
                    && "G_LAST_CONTEXT".equalsIgnoreCase(d.getVariableName()))
            .findFirst().orElse(null);
        assertNotNull(detail, "expected structured detail entry for the reference");
        assertEquals(PackageVarRefInfo.AccessType.READ, detail.getAccessType());
        assertTrue(detail.getLine() > 0);
    }

    /**
     * No-regression check: the existing package-to-package case (one package's
     * subprogram reading a DIFFERENT package's global) must keep working exactly
     * as before.
     */
    @Test
    void packageToPackageGlobalRead_stillDetected() {
        String sql =
            "CREATE OR REPLACE PACKAGE pkg_a AS\n" +
            "  g_flag VARCHAR2(1);\n" +
            "END pkg_a;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE BODY pkg_a AS\n" +
            "END pkg_a;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE pkg_b AS\n" +
            "  PROCEDURE use_flag;\n" +
            "END pkg_b;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE BODY pkg_b AS\n" +
            "  PROCEDURE use_flag IS\n" +
            "  BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE(pkg_a.g_flag);\n" +
            "  END use_flag;\n" +
            "END pkg_b;\n" +
            "/\n";

        ParseResult result = parse(sql);
        assertNoParsErrors(result, "package-to-package-global-read");

        SubprogramInfo useFlag = findSubInBody(result, "use_flag");
        assertNotNull(useFlag);
        assertTrue(useFlag.getExternalPackageVarRefs().stream()
                .anyMatch(r -> r.equalsIgnoreCase("pkg_a.g_flag")));
    }

    /**
     * Step 2 structured-data test: a package's subprogram both WRITES and READS
     * another package's global variable on different lines; confirm the
     * structured PackageVarRefInfo list carries correct line numbers and
     * correct READ/WRITE classification for each.
     */
    @Test
    void writeAndReadOfSameExternalGlobal_recordedWithCorrectLineAndAccessType() {
        String sql =
            "CREATE OR REPLACE PACKAGE pkg_a AS\n" +
            "  G_FLAG VARCHAR2(1);\n" +
            "END pkg_a;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE BODY pkg_a AS\n" +
            "END pkg_a;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE pkg_b AS\n" +
            "  v_use_flag VARCHAR2(1);\n" +
            "  PROCEDURE use_flag;\n" +
            "END pkg_b;\n" +
            "/\n" +
            "CREATE OR REPLACE PACKAGE BODY pkg_b AS\n" +
            "  PROCEDURE use_flag IS\n" +
            "  BEGIN\n" +
            "    pkg_a.g_flag := 'Y';\n" +
            "    IF pkg_a.g_flag = 'Y' THEN\n" +
            "      DBMS_OUTPUT.PUT_LINE(pkg_a.g_flag);\n" +
            "    END IF;\n" +
            "  END use_flag;\n" +
            "END pkg_b;\n" +
            "/\n";

        ParseResult result = parse(sql);
        assertNoParsErrors(result, "write-then-read-global");

        SubprogramInfo useFlag = findSubInBody(result, "use_flag");
        assertNotNull(useFlag);
        List<PackageVarRefInfo> details = useFlag.getExternalPackageVarRefDetails();
        assertTrue(details.size() >= 1, "expected at least one recorded reference, got: " + details);

        // The write on the ":=" line
        PackageVarRefInfo write = details.stream()
            .filter(d -> d.getAccessType() == PackageVarRefInfo.AccessType.WRITE)
            .findFirst().orElse(null);
        assertNotNull(write, "expected a WRITE entry, got: " + details);
        assertEquals("PKG_A", write.getPackageName());
        assertEquals("G_FLAG", write.getVariableName());

        // At least one read (from the IF condition or PUT_LINE call) at a later line than the write
        PackageVarRefInfo read = details.stream()
            .filter(d -> d.getAccessType() == PackageVarRefInfo.AccessType.READ)
            .findFirst().orElse(null);
        assertNotNull(read, "expected a READ entry, got: " + details);
        assertTrue(read.getLine() > write.getLine(),
            "expected read line (" + read.getLine() + ") after write line (" + write.getLine() + ")");
    }
}
