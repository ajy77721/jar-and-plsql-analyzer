package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * UI-024 — {@link TableTriggersService}, replacing `tableDetail.js`'s
 * `PA.nodeIndex.all()`-then-filter-by-`triggerTable` scan with a server-side filter. Fixture node
 * shape matches a real trigger node read off disk (`nodeId`/`objectType`/`triggerTable`/
 * `triggerEvent`/`triggerTiming`), confirmed against
 * `data/plsql-parse/ORACLE/.../ANALYZER$TRG$ERP_TRG_SO_STMT_AD.json`-equivalent index entries.
 */
class TableTriggersServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static String indexJson(String nodesJson) {
        return "{\"nodes\":" + nodesJson + "}";
    }

    private static String trigger(String id, String table, String event) {
        return "{\"nodeId\":\"" + id + "\",\"name\":\"TRG." + id + "\",\"objectName\":\"" + id + "\","
                + "\"schema\":\"ANALYZER\",\"objectType\":\"TRIGGER\",\"triggerTable\":\"" + table + "\","
                + "\"triggerEvent\":\"" + event + "\",\"triggerTiming\":\"AFTER\"}";
    }

    private static String procNode(String id) {
        return "{\"nodeId\":\"" + id + "\",\"objectType\":\"PROCEDURE\"}";
    }

    @Test
    void returnsOnlyTriggersMatchingTheRequestedTable() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + trigger("T1", "ERP_SALES_ORDER", "DELETE")
                + "," + trigger("T2", "ERP_INVOICE", "INSERT")
                + "," + procNode("ERP$P1") + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        TableTriggersService svc = new TableTriggersService(service, M);
        ResponseEntity<Object> response = svc.getTriggersForTable("A1", "ERP_SALES_ORDER");

        assertEquals(200, response.getStatusCode().value());
        ArrayNode body = (ArrayNode) response.getBody();
        assertEquals(1, body.size());
        assertEquals("T1", body.get(0).get("nodeId").asText());
        assertEquals("DELETE", body.get(0).get("triggerEvent").asText());
    }

    /**
     * Calibrated by removing the `trigTable.isEmpty()` guard — confirmed a trigger with no
     * `triggerTable` at all matched every table request before restoring (the exact "previous bug
     * showed all triggers on every table" the client's own comment warns about).
     */
    @Test
    void aTriggerWithNoTriggerTableMatchesNothing() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String noTableTrigger = "{\"nodeId\":\"T3\",\"objectType\":\"TRIGGER\",\"triggerTable\":\"\"}";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson("[" + noTableTrigger + "]")));

        TableTriggersService svc = new TableTriggersService(service, M);
        ArrayNode body = (ArrayNode) svc.getTriggersForTable("A1", "ERP_SALES_ORDER").getBody();
        assertEquals(0, body.size());
    }

    @Test
    void matchIsCaseInsensitive() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson("[" + trigger("T1", "erp_sales_order", "UPDATE") + "]")));

        TableTriggersService svc = new TableTriggersService(service, M);
        ArrayNode body = (ArrayNode) svc.getTriggersForTable("A1", "ERP_SALES_ORDER").getBody();
        assertEquals(1, body.size());
    }

    @Test
    void nonTriggerNodesAreIgnoredEvenIfTheyHappenToCarryATriggerTableField() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String impostor = "{\"nodeId\":\"P1\",\"objectType\":\"PROCEDURE\",\"triggerTable\":\"ERP_SALES_ORDER\"}";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson("[" + impostor + "]")));

        TableTriggersService svc = new TableTriggersService(service, M);
        ArrayNode body = (ArrayNode) svc.getTriggersForTable("A1", "ERP_SALES_ORDER").getBody();
        assertEquals(0, body.size());
    }

    @Test
    void blankTableNameIsBadRequest() {
        AnalysisService service = mock(AnalysisService.class);
        TableTriggersService svc = new TableTriggersService(service, M);
        assertEquals(400, svc.getTriggersForTable("A1", "").getStatusCode().value());
        assertEquals(400, svc.getTriggersForTable("A1", null).getStatusCode().value());
    }
}
