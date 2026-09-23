package com.plsql.parser.flow;

import com.plsql.parser.model.FlowEdge;
import com.plsql.parser.model.FlowNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression: {@code call_graph.json}'s edges and the per-node artifacts
 * ({@code api/nodes/&lt;id&gt;.json} / {@code api/index.json}) must attribute the SAME schema to the
 * same routine.
 *
 * <p>Confirmed on the real {@code POSTGRES~POSTGRESDEMO~LOCAL~ERP_USP_RUN_CYCLE} corpus: the node
 * was correctly resolved to {@code PG_CATALOG$UNNEST} (the {@code pg_catalog.unnest(...)} builtin's
 * true owner), while every edge pointing at it still carried the call-site guess
 * {@code ERP$UNNEST} — the CALLER's schema — because {@code DependencyCrawler} derives an edge's
 * {@code toSchema}/{@code toNodeId} at enqueue time, long before the target's owner is resolved,
 * and nothing reconciled the two afterwards. The UI's Hierarchy tab reads the edge list, so it
 * showed the stale name indefinitely.
 *
 * <p>{@link ChunkedFlowWriter#reconcileEdgeSchemas} makes the resolved {@link FlowNode} the single
 * source of truth. These cases are dialect-agnostic on purpose — the same enqueue-then-resolve
 * ordering exists for every dialect (cross-schema standalone calls in Oracle/SQL Server/DB2/MySQL),
 * so this is not a PostgreSQL-only guard.
 */
class CallGraphNodeSchemaAgreementTest {

    private static FlowNode node(String schema, String pkg, String object) {
        FlowNode n = new FlowNode();
        n.setSchema(schema);
        n.setPackageName(pkg);
        n.setObjectName(object);
        n.generateNodeId();
        return n;
    }

    private static FlowEdge edge(String fromSchema, String fromObj, String toSchema, String toObj) {
        FlowEdge e = new FlowEdge();
        e.setFromSchema(fromSchema);
        e.setFromObject(fromObj);
        e.setFromNodeId((fromSchema + "$" + fromObj).toUpperCase());
        e.setToSchema(toSchema);
        e.setToObject(toObj);
        e.setToNodeId((toSchema + "$" + toObj).toUpperCase());
        return e;
    }

    /** The exact real-corpus case: a pg_catalog builtin mis-attributed to the caller's schema. */
    @Test
    void danglingEdgeAdoptsResolvedNodeSchema() {
        List<FlowNode> nodes = List.of(
                node("ERP", null, "erp_usp_run_cycle"),
                node("PG_CATALOG", null, "unnest"));
        List<FlowEdge> edges = new ArrayList<>(List.of(
                edge("ERP", "erp_usp_run_cycle", "ERP", "unnest")));

        ChunkedFlowWriter.reconcileEdgeSchemas(edges, nodes);

        FlowEdge e = edges.get(0);
        assertEquals("PG_CATALOG$UNNEST", e.getToNodeId());
        assertEquals("PG_CATALOG", e.getToSchema());
        // The caller end already resolved to a real node and must be untouched.
        assertEquals("ERP$ERP_USP_RUN_CYCLE", e.getFromNodeId());
        assertEquals("ERP", e.getFromSchema());
    }

    /** Every edge endpoint must name a node that actually exists in the node list. */
    @Test
    void everyEdgeEndpointResolvesToARealNode() {
        List<FlowNode> nodes = List.of(
                node("ERP", null, "erp_usp_run_cycle"),
                node("PG_CATALOG", null, "unnest"),
                node("PG_CATALOG", null, "set_config"));
        List<FlowEdge> edges = new ArrayList<>(List.of(
                edge("ERP", "erp_usp_run_cycle", "ERP", "unnest"),
                edge("ERP", "erp_usp_run_cycle", "ERP", "set_config")));

        ChunkedFlowWriter.reconcileEdgeSchemas(edges, nodes);

        List<String> ids = nodes.stream().map(FlowNode::getNodeId).toList();
        for (FlowEdge e : edges) {
            assertNotNull(e.getToNodeId());
            org.junit.jupiter.api.Assertions.assertTrue(ids.contains(e.getToNodeId()),
                    "edge target " + e.getToNodeId() + " is not a real node: " + ids);
            org.junit.jupiter.api.Assertions.assertTrue(ids.contains(e.getFromNodeId()),
                    "edge source " + e.getFromNodeId() + " is not a real node: " + ids);
        }
    }

    /** Cross-schema (Oracle-style OWNER.OBJECT) edges that already resolve stay byte-identical. */
    @Test
    void alreadyCorrectEdgesAreUntouched() {
        List<FlowNode> nodes = List.of(
                node("SALES", "PKG_ORDERS", "PROCESS"),
                node("ANALYZER_REF", null, "USP_REF_LOG"));
        FlowEdge e = new FlowEdge();
        e.setFromSchema("SALES");
        e.setFromPackage("PKG_ORDERS");
        e.setFromObject("PROCESS");
        e.setFromNodeId("SALES$PKG_ORDERS$PROCESS");
        e.setToSchema("ANALYZER_REF");
        e.setToObject("USP_REF_LOG");
        e.setToNodeId("ANALYZER_REF$USP_REF_LOG");
        List<FlowEdge> edges = new ArrayList<>(List.of(e));

        ChunkedFlowWriter.reconcileEdgeSchemas(edges, nodes);

        assertEquals("SALES$PKG_ORDERS$PROCESS", e.getFromNodeId());
        assertEquals("ANALYZER_REF$USP_REF_LOG", e.getToNodeId());
        assertEquals("ANALYZER_REF", e.getToSchema());
    }

    /** Genuinely ambiguous same-named routines in two schemas are left alone, never guessed at. */
    @Test
    void ambiguousCrossSchemaMatchIsLeftAlone() {
        List<FlowNode> nodes = List.of(
                node("SALES", null, "FN_X"),
                node("HR", null, "FN_X"));
        List<FlowEdge> edges = new ArrayList<>(List.of(edge("APP", "CALLER", "APP", "FN_X")));

        ChunkedFlowWriter.reconcileEdgeSchemas(edges, nodes);

        assertEquals("APP$FN_X", edges.get(0).getToNodeId());
        assertEquals("APP", edges.get(0).getToSchema());
    }

    /** An overloaded target keeps its {@code __<arity>} discriminator when re-pointed. */
    @Test
    void overloadDiscriminatorIsPreserved() {
        FlowNode n1 = node("SALES", "PKG", "COMPUTE_TOTAL");
        n1.setOverloadParamCount(1);
        n1.generateNodeId();
        FlowNode n2 = node("SALES", "PKG", "COMPUTE_TOTAL");
        n2.setOverloadParamCount(2);
        n2.generateNodeId();
        List<FlowNode> nodes = List.of(n1, n2);

        FlowEdge e = new FlowEdge();
        e.setFromSchema("APP");
        e.setFromObject("CALLER");
        e.setFromNodeId("APP$CALLER");
        e.setToSchema("APP");
        e.setToPackage("PKG");
        e.setToObject("COMPUTE_TOTAL");
        e.setToNodeId("APP$PKG$COMPUTE_TOTAL__2");
        List<FlowEdge> edges = new ArrayList<>(List.of(e));

        ChunkedFlowWriter.reconcileEdgeSchemas(edges, nodes);

        assertEquals("SALES$PKG$COMPUTE_TOTAL__2", e.getToNodeId());
        assertEquals("SALES", e.getToSchema());
    }
}
