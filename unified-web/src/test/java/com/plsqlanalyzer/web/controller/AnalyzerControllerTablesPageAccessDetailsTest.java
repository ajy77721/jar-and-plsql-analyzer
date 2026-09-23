package com.plsqlanalyzer.web.controller;

import com.analyzer.storage.AnalysisRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.parser.model.SqlOperationType;
import com.plsqlanalyzer.web.controller.detail.TableQueryService;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * UI-002 (dedicated project, §133) — {@code /api/plsql/analysis/tables/page} no longer embeds each
 * row's own {@code accessDetails}/{@code _fullAccessDetails}: unlike the parser backend's own
 * `/tables/page` (already the compact {@code usedBy[]} wire form), this backend's row is the real
 * {@code TableOperationSummary} POJO, serializing one entry per access line on every row of every
 * page regardless of whether that row's panel is ever opened. {@code accessCount}/
 * {@code _fullAccessCount} stay untouched — they are already the authoritative scalar the count
 * column and "scoped" label read.
 */
class AnalyzerControllerTablesPageAccessDetailsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static TableOperationSummary table(String name, String schema, int accesses) {
        TableOperationSummary t = new TableOperationSummary(name, schema);
        for (int i = 0; i < accesses; i++) {
            TableOperationSummary.TableAccessDetail d = new TableOperationSummary.TableAccessDetail();
            d.setProcedureId("ERP$P" + i);
            d.setProcedureName("P" + i);
            d.setOperation(SqlOperationType.SELECT);
            d.setLineNumber(10 + i);
            t.getAccessDetails().add(d);
        }
        t.getOperations().add(SqlOperationType.SELECT);
        t.setAccessCount(accesses);
        return t;
    }

    private static AnalyzerController build(TableQueryService tableQueryService) throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (TableQueryService.class.equals(type)) args[i] = tableQueryService;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(MAPPER, 10000);
            else args[i] = mock(type);
        }
        return (AnalyzerController) ctor.newInstance(args);
    }

    @Test
    void pageRowsCarryNoAccessDetailsButKeepTheScalarCount() throws Exception {
        TableQueryService svc = mock(TableQueryService.class);
        when(svc.getTableOperations()).thenReturn(
                ResponseEntity.ok(List.of(table("ERP_ORDER", "ERP", 3), table("ERP_INVOICE", "ERP", 5))));

        AnalyzerController c = build(svc);
        var response = c.getTableOperationsPage(50, null, null, null, null, null, null, null, null);

        assertEquals(200, response.getStatusCode().value());
        JsonNode items = response.getBody().get("items");
        assertEquals(2, items.size());
        for (JsonNode row : items) {
            assertNull(row.get("accessDetails"), "accessDetails must not be on the page row: " + row);
            assertNull(row.get("_fullAccessDetails"), "_fullAccessDetails must not be on the page row: " + row);
            assertTrue(row.has("accessCount"), "accessCount must still be present");
        }
        JsonNode order = null;
        for (JsonNode row : items) if ("ERP_ORDER".equals(row.get("tableName").asText())) order = row;
        assertEquals(3, order.get("accessCount").asInt());
    }

    /**
     * Calibrated by removing the {@code stripAccessDetails} call at the `/tables/page` call site
     * and confirming this test failed with `accessDetails` present on the row, then restoring.
     */
    @Test
    void aScopedRowAlsoLosesFullAccessDetailsButKeepsFullAccessCount() throws Exception {
        TableOperationSummary t = table("ERP_ORDER", "ERP", 4);
        TableQueryService svc = mock(TableQueryService.class);
        when(svc.getTableOperations()).thenReturn(ResponseEntity.ok(List.of(t)));

        AnalyzerController c = build(svc);
        // directProcId scoped to the table's own first accessor — PlsqlScopeFilter.direct trims to
        // 1 of 4, and its own trim() sets _fullAccessDetails/_fullAccessCount before this strips them.
        var response = c.getTableOperationsPage(50, null, null, null, null, null, "ERP$P0", null, null);

        JsonNode items = response.getBody().get("items");
        assertEquals(1, items.size(), "only the row matching ERP$P0 survives PlsqlScopeFilter.direct");
        JsonNode row = items.get(0);
        assertNull(row.get("_fullAccessDetails"), "_fullAccessDetails must be stripped even on a scoped row");
        assertTrue(row.has("_fullAccessCount"), "_fullAccessCount must survive — it's the scalar the UI reads");
        assertEquals(4, row.get("_fullAccessCount").asInt());
        assertEquals(1, row.get("accessCount").asInt());
    }

    /**
     * Calibrated by removing the {@code claudeVerifiedCount} computation before stripping and
     * confirming this test failed (the count would silently read 0, going dark), then restoring.
     */
    @Test
    void claudeVerifiedCountSurvivesEvenThoughAccessDetailsIsStripped() throws Exception {
        TableOperationSummary t = table("ERP_ORDER", "ERP", 3);
        t.getAccessDetails().get(1).setSourceFile("claude-verified");
        TableQueryService svc = mock(TableQueryService.class);
        when(svc.getTableOperations()).thenReturn(ResponseEntity.ok(List.of(t)));

        AnalyzerController c = build(svc);
        var response = c.getTableOperationsPage(50, null, null, null, null, null, null, null, null);

        JsonNode row = response.getBody().get("items").get(0);
        assertEquals(1, row.get("claudeVerifiedCount").asLong());
    }

    @Test
    void theLegacyBareArrayPathIsAlsoStripped() throws Exception {
        TableQueryService svc = mock(TableQueryService.class);
        when(svc.getTableOperations()).thenReturn(ResponseEntity.ok(List.of(table("ERP_ORDER", "ERP", 2))));

        AnalyzerController c = build(svc);
        // No limit, no Accept header — legacy bare-array path.
        var response = c.getTableOperationsPage(null, null, null, null, null, null, null, null, null);

        JsonNode body = response.getBody();
        assertFalse(body.has("items"), "sanity check: this really is the legacy bare-array shape");
        assertTrue(body.isArray());
        assertNull(body.get(0).get("accessDetails"));
    }
}
