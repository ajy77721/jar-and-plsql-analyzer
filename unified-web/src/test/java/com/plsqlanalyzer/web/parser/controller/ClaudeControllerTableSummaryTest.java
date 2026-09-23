package com.plsqlanalyzer.web.parser.controller;

import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.OperationVerification;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.TableVerificationResult;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-013 §60/§62 — {@link ClaudeController#summarizeTables}, checked against
 * {@code parser/js/claudeEnrichment.js:_renderOverviewBody}'s own per-table fold, the thing this
 * endpoint exists to replace: fetching the whole {@code /claude/result} document (measured
 * elsewhere at 138,301 bytes for a handful of useful numbers) just to reduce it to eight small
 * counts per table.
 */
class ClaudeControllerTableSummaryTest {

    private static OperationVerification op(String status, String userDecision) {
        OperationVerification v = new OperationVerification();
        v.status = status;
        v.userDecision = userDecision;
        return v;
    }

    private static TableVerificationResult table(String name, String overallStatus,
                                                  List<OperationVerification> ops) {
        TableVerificationResult t = new TableVerificationResult();
        t.tableName = name;
        t.overallStatus = overallStatus;
        t.claudeVerifications = ops;
        return t;
    }

    @Test
    void countsEachStatusAndDecisionIndependently() {
        VerificationResult r = new VerificationResult();
        r.tables.add(table("ORDERS", "PARTIALLY_CONFIRMED", List.of(
                op("CONFIRMED", "ACCEPTED"),
                op("CONFIRMED", null),
                op("REMOVED", "REJECTED"),
                op("NEW", null)
        )));

        List<Map<String, Object>> rows = ClaudeController.summarizeTables(r);
        assertEquals(1, rows.size());
        Map<String, Object> row = rows.get(0);
        assertEquals("ORDERS", row.get("tableName"));
        assertEquals("PARTIALLY_CONFIRMED", row.get("overallStatus"));
        assertEquals(4, row.get("total"));
        assertEquals(2, row.get("confirmed"));
        assertEquals(1, row.get("removed"));
        assertEquals(1, row.get("newOps"));
        // Status and decision are independent tallies: a null/blank userDecision falls to
        // "pending", exactly as `else tPending++` does in the JS — not "unmatched, so dropped".
        assertEquals(1, row.get("accepted"));
        assertEquals(1, row.get("rejected"));
        assertEquals(2, row.get("pending"));
    }

    @Test
    void aTableWithNoVerificationsIsDroppedEntirely() {
        // `if (vfs.length === 0) continue;` in the JS — a table present in the result but with
        // nothing verified must not appear as an all-zero row.
        VerificationResult r = new VerificationResult();
        r.tables.add(table("EMPTY_TABLE", "UNVERIFIED", List.of()));
        r.tables.add(table("REAL_TABLE", "CONFIRMED", List.of(op("CONFIRMED", "ACCEPTED"))));

        List<Map<String, Object>> rows = ClaudeController.summarizeTables(r);
        assertEquals(1, rows.size());
        assertEquals("REAL_TABLE", rows.get(0).get("tableName"));
    }

    @Test
    void statusAndDecisionMatchingIsCaseInsensitive() {
        // The JS upper-cases before comparing (`(vfs[vi].status || '').toUpperCase()`) — a
        // lower-cased status from an older document must still classify correctly.
        VerificationResult r = new VerificationResult();
        r.tables.add(table("T", "confirmed", List.of(op("confirmed", "accepted"))));

        Map<String, Object> row = ClaudeController.summarizeTables(r).get(0);
        assertEquals(1, row.get("confirmed"));
        assertEquals(1, row.get("accepted"));
        assertEquals(0, row.get("pending"));
    }

    @Test
    void nullFieldsDoNotThrowAndFallToTheirEmptyDefaults() {
        VerificationResult r = new VerificationResult();
        TableVerificationResult t = new TableVerificationResult();
        t.tableName = null;
        t.overallStatus = null;
        t.claudeVerifications = List.of(op(null, null));
        r.tables.add(t);

        Map<String, Object> row = ClaudeController.summarizeTables(r).get(0);
        assertEquals("", row.get("tableName"));
        assertEquals("", row.get("overallStatus"));
        assertEquals(1, row.get("pending"));
        assertEquals(0, row.get("confirmed"));
        assertEquals(0, row.get("removed"));
        assertEquals(0, row.get("newOps"));
    }

    @Test
    void emptyResultProducesAnEmptyList() {
        VerificationResult r = new VerificationResult();
        List<Map<String, Object>> rows = ClaudeController.summarizeTables(r);
        assertTrue(rows.isEmpty());
    }
}
