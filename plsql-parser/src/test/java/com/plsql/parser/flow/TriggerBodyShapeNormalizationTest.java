package com.plsql.parser.flow;

import com.plsqlanalyzer.config.DbType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The trigger-body SHAPE the live catalog returns is not the idealized fixture.
 *
 * <p>Oracle's {@code ALL_SOURCE} stores a trigger without the leading {@code CREATE OR REPLACE}:
 * the text begins at the {@code TRIGGER} keyword and carries the full header. The fixture used in
 * the earlier trigger tests was a bare {@code BEGIN … END} block, so the header shape parsed to
 * zero calls. These tests pin the normalization against the EXACT live body text (copied from the
 * catalog source cache {@code ANALYZER.ERP_TRG_SO_STMT_AD.TRIGGER.*.srccache}).
 */
@DisplayName("Trigger body shape normalization: live catalog bodies yield calls")
class TriggerBodyShapeNormalizationTest {

    private final TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);

    /** The exact live {@code ALL_SOURCE} text for ANALYZER.ERP_TRG_SO_STMT_AD. */
    private static final String LIVE_BODY =
            "TRIGGER erp_trg_so_stmt_ad\n"
          + "AFTER DELETE ON erp_sales_order\n"
          + "BEGIN\n"
          + "  erp_usp_write_audit_trail('SO_DELETE','ERP_SALES_ORDER','statement-level delete fired');\n"
          + "END erp_trg_so_stmt_ad;";

    @Test
    @DisplayName("the live ALL_SOURCE body (TRIGGER header, no CREATE) yields its call")
    void liveCatalogTriggerBodyYieldsCall() {
        List<Map<String, Object>> calls = discoverer.extractCallsFromBody(LIVE_BODY, DbType.ORACLE);
        assertEquals(1, calls.size(), "the live trigger body must yield one call: " + calls);
        assertEquals("ERP_USP_WRITE_AUDIT_TRAIL", calls.get(0).get("objectName"));
        assertEquals(1, calls.get(0).get("count"));
    }

    @Test
    @DisplayName("normalization strips the TRIGGER header and keeps the block")
    void normalizationStripsTriggerHeader() {
        String normalized = TriggerDiscoverer.normalizeTriggerBody(LIVE_BODY);
        assertTrue(normalized.stripLeading().toUpperCase().startsWith("BEGIN"),
                "header must be stripped, block kept: " + normalized);
        assertFalse(normalized.toUpperCase().contains("TRIGGER"),
                "the TRIGGER header must not survive normalization: " + normalized);
        assertEquals(1, countOccurrences(normalized.toUpperCase(), "BEGIN"),
                "exactly one synthetic BEGIN: " + normalized);
    }

    @Test
    @DisplayName("an already-wrapped BEGIN…END body is not double-wrapped")
    void alreadyWrappedBodyIsNotDoubleWrapped() {
        String body = "BEGIN\n  erp_usp_write_audit_trail('X');\nEND;";
        assertEquals(body, TriggerDiscoverer.normalizeTriggerBody(body),
                "a body that already has its own block must be returned unchanged");
        List<Map<String, Object>> calls = discoverer.extractCallsFromBody(body, DbType.ORACLE);
        assertEquals(1, calls.size(), "already-wrapped body still yields its call: " + calls);
    }

    @Test
    @DisplayName("a DECLARE…BEGIN body is already valid and left unchanged")
    void declareBlockBodyLeftUnchanged() {
        String body = "DECLARE\n  v NUMBER;\nBEGIN\n  erp_usp_write_audit_trail('X');\nEND;";
        assertEquals(body, TriggerDiscoverer.normalizeTriggerBody(body));
        assertEquals(1, discoverer.extractCallsFromBody(body, DbType.ORACLE).size());
    }

    @Test
    @DisplayName("bare statements with no block get a synthetic BEGIN…END wrapper")
    void bareStatementsGetWrapped() {
        String body = "erp_usp_write_audit_trail('SO_DELETE','ERP_SALES_ORDER','x');";
        String normalized = TriggerDiscoverer.normalizeTriggerBody(body);
        assertTrue(normalized.stripLeading().toUpperCase().startsWith("BEGIN"), normalized);
        List<Map<String, Object>> calls = discoverer.extractCallsFromBody(body, DbType.ORACLE);
        assertEquals(1, calls.size(), "bare-statement body must yield its call: " + calls);
        assertEquals("ERP_USP_WRITE_AUDIT_TRAIL", calls.get(0).get("objectName"));
    }

    @Test
    @DisplayName("a NULL (no-op) body yields zero calls, honestly")
    void nullBodyYieldsNoCalls() {
        assertTrue(discoverer.extractCallsFromBody("BEGIN NULL; END;", DbType.ORACLE).isEmpty());
        assertTrue(discoverer.extractCallsFromBody("NULL;", DbType.ORACLE).isEmpty());
        assertTrue(discoverer.extractCallsFromBody("", DbType.ORACLE).isEmpty());
        assertTrue(discoverer.extractCallsFromBody(null, DbType.ORACLE).isEmpty());
    }

    @Test
    @DisplayName("a nested-block body yields its call")
    void nestedBlockBodyYieldsCall() {
        String body = "BEGIN\n  BEGIN\n    erp_usp_write_audit_trail('NESTED');\n  END;\nEND;";
        List<Map<String, Object>> calls = discoverer.extractCallsFromBody(body, DbType.ORACLE);
        assertEquals(1, calls.size(), "nested-block body must yield its call: " + calls);
        assertEquals("ERP_USP_WRITE_AUDIT_TRAIL", calls.get(0).get("objectName"));
    }

    @Test
    @DisplayName("a full CREATE TRIGGER DDL is parsed as-is (no re-wrap)")
    void fullCreateTriggerDdlParsedAsIs() {
        String ddl = "CREATE OR REPLACE TRIGGER erp_trg_so_stmt_ad\n"
                + "AFTER DELETE ON erp_sales_order\n"
                + "BEGIN\n  erp_usp_write_audit_trail('SO_DELETE');\nEND;";
        assertEquals(ddl, TriggerDiscoverer.normalizeTriggerBody(ddl));
        assertEquals(1, discoverer.extractCallsFromBody(ddl, DbType.ORACLE).size());
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) { count++; idx += needle.length(); }
        return count;
    }
}
