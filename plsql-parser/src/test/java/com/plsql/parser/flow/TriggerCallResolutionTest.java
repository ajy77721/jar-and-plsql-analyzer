package com.plsql.parser.flow;

import com.plsql.parser.model.CallInfo;
import com.plsql.parser.model.FlowNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The DB-discovered trigger path resolves its body's outbound calls against the crawled estate
 * (see {@code DependencyCrawler#resolveTriggerCallNodeIds}). A trigger is a catalog artefact, not a
 * crawled node, so its callees arrive unresolved; each is matched to the crawled routine it names
 * and stamped with that routine's node id for the UI cross-link. An unmatched callee is left
 * exactly as it arrived.
 */
class TriggerCallResolutionTest {

    private static FlowNode node(String id, String pkg, String objectName) {
        FlowNode n = new FlowNode();
        n.setNodeId(id);
        n.setPackageName(pkg);
        n.setObjectName(objectName);
        return n;
    }

    @Test
    @DisplayName("a standalone callee resolves to the crawled procedure's node id")
    void standaloneCalleeResolves() {
        Map<String, String> ids = new HashMap<>();
        DependencyCrawler.recordCrawledNodeId(ids, node("ERP$ERP_USP_WRITE_AUDIT_TRAIL",
                null, "erp_usp_write_audit_trail"));

        CallInfo c = new CallInfo();
        c.setName("erp_usp_write_audit_trail");
        c.setType("EXTERNAL");

        DependencyCrawler.resolveTriggerCallNodeIds(List.of(c), ids);

        assertEquals("ERP$ERP_USP_WRITE_AUDIT_TRAIL", c.getNodeId());
        assertEquals("EXTERNAL", c.getType(),
                "type is deliberately untouched — a trigger's namespace is TRG");
    }

    @Test
    @DisplayName("a package member callee resolves via PACKAGE.SUBPROGRAM")
    void packageMemberCalleeResolves() {
        Map<String, String> ids = new HashMap<>();
        DependencyCrawler.recordCrawledNodeId(ids, node("ERP$ERP_PKG_PLANNER$RUN", "erp_pkg_planner", "run"));

        CallInfo c = new CallInfo();
        c.setName("run");
        c.setPackageName("erp_pkg_planner");

        DependencyCrawler.resolveTriggerCallNodeIds(List.of(c), ids);

        assertEquals("ERP$ERP_PKG_PLANNER$RUN", c.getNodeId());
    }

    @Test
    @DisplayName("a callee absent from the crawl stays unresolved (no node id)")
    void unresolvedCalleeStaysExternal() {
        Map<String, String> ids = new HashMap<>();
        DependencyCrawler.recordCrawledNodeId(ids, node("ERP$KNOWN", null, "known"));

        CallInfo c = new CallInfo();
        c.setName("some_external_thing");
        c.setType("EXTERNAL");

        DependencyCrawler.resolveTriggerCallNodeIds(List.of(c), ids);

        assertNull(c.getNodeId());
        assertEquals("EXTERNAL", c.getType());
    }

    @Test
    @DisplayName("registration records both bare and PACKAGE.SUB keys, first writer wins")
    void registrationRecordsBothKeys() {
        Map<String, String> ids = new HashMap<>();
        DependencyCrawler.recordCrawledNodeId(ids, node("A", "PKG", "SUB"));
        // A later overload must not displace the canonical node.
        DependencyCrawler.recordCrawledNodeId(ids, node("B", "PKG", "SUB"));

        assertEquals("A", ids.get("PKG.SUB"));
        assertEquals("A", ids.get("SUB"));
    }

    @Test
    @DisplayName("blank names, null ids and null lists are ignored without throwing")
    void defensiveInputs() {
        Map<String, String> ids = new HashMap<>();
        DependencyCrawler.recordCrawledNodeId(ids, node(null, null, "x"));
        DependencyCrawler.recordCrawledNodeId(ids, node("id", null, " "));
        assertEquals(0, ids.size());

        DependencyCrawler.resolveTriggerCallNodeIds(new ArrayList<>(), ids);
        CallInfo blank = new CallInfo();
        blank.setName("  ");
        DependencyCrawler.resolveTriggerCallNodeIds(List.of(blank), ids);
        assertNull(blank.getNodeId());
    }
}
