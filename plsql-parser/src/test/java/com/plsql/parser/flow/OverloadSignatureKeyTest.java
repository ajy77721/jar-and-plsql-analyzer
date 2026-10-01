package com.plsql.parser.flow;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.FlowEdge;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.ParameterInfo;
import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.SubprogramInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * W1 overload handling: the node-id discriminator is a SIGNATURE (parameter TYPE list, kind-prefixed
 * for a FUNCTION/PROCEDURE same-name pair) instead of a bare arity count, with the arity count kept
 * as the fallback. These tests pin the key format, the byte-compatibility of non-overloaded ids, the
 * kind split, the call-resolution chain (exact signature → arity → first match, the last two counted)
 * and the edge-reconciliation path.
 */
class OverloadSignatureKeyTest extends ParserTestBase {

    private static ParameterInfo p(String type) {
        ParameterInfo pi = new ParameterInfo();
        pi.setDataType(type);
        pi.setDirection("IN");
        return pi;
    }

    private static SubprogramInfo sub(String kind, ParameterInfo... params) {
        SubprogramInfo s = new SubprogramInfo();
        s.setType(kind);
        s.setName("F");
        s.setParameters(new ArrayList<>(List.of(params)));
        return s;
    }

    // ── key format / byte-compatibility ──────────────────────────────────────────────────────

    @Test
    @DisplayName("non-overloaded id is byte-identical (no discriminator)")
    void nonOverloadedIdUnchanged() {
        FlowNode n = new FlowNode();
        n.setSchema("S");
        n.setPackageName("PKG");
        n.setObjectName("SUB");
        n.generateNodeId();
        assertEquals("S$PKG$SUB", n.getNodeId());
        assertNull(n.getOverloadDiscriminator());
    }

    @Test
    @DisplayName("legacy arity discriminator is preserved as the fallback")
    void legacyArityDiscriminatorPreserved() {
        FlowNode n = new FlowNode();
        n.setPackageName("PKG");
        n.setObjectName("SUB");
        n.setOverloadParamCount(2);
        n.generateNodeId();
        assertEquals("$PKG$SUB__2", n.getNodeId());
    }

    @Test
    @DisplayName("same-arity-different-type overloads resolve to DISTINCT nodes")
    void sameArityDifferentTypesDistinct() {
        FlowNode a = new FlowNode();
        a.setPackageName("PKG");
        a.setObjectName("PROCESS_ORDER");
        a.setOverloadSignature("NUMBER");
        a.generateNodeId();
        FlowNode b = new FlowNode();
        b.setPackageName("PKG");
        b.setObjectName("PROCESS_ORDER");
        b.setOverloadSignature("VARCHAR2");
        b.generateNodeId();
        assertEquals("$PKG$PROCESS_ORDER__NUMBER", a.getNodeId());
        assertEquals("$PKG$PROCESS_ORDER__VARCHAR2", b.getNodeId());
        assertNotEquals(a.getNodeId(), b.getNodeId());
    }

    @Test
    @DisplayName("a FUNCTION/PROCEDURE same-name pair splits (kind enters the key)")
    void functionProcedurePairSplits() {
        List<SubprogramInfo> overloads = List.of(
                sub("FUNCTION", p("NUMBER")),
                sub("PROCEDURE", p("NUMBER")));
        assertTrue(FlowNode.mixedKinds(overloads));
        String fn = FlowNode.discriminatorFor(overloads.get(0), true);
        String pr = FlowNode.discriminatorFor(overloads.get(1), true);
        assertEquals("FUNCTION_NUMBER", fn);
        assertEquals("PROCEDURE_NUMBER", pr);
        assertNotEquals(fn, pr);
    }

    // ── call-site resolution chain ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("resolution: exact signature → arity → first match (last two counted)")
    void resolutionFallbackChain() {
        List<SubprogramInfo> sameArity = List.of(
                sub("PROCEDURE", p("NUMBER")),
                sub("PROCEDURE", p("VARCHAR2")));
        AtomicLong arity = new AtomicLong();
        AtomicLong first = new AtomicLong();

        // exact signature: a VARCHAR2 argument picks the VARCHAR2 overload; no fallback counted.
        assertEquals("VARCHAR2",
                FlowNode.resolveOverloadDiscriminator(sameArity, 1, List.of("VARCHAR2"), arity, first));
        assertEquals(0, arity.get());
        assertEquals(0, first.get());

        // unknown argument types → arity is ambiguous (both 1-arg) → first match; both counted.
        assertEquals("NUMBER",
                FlowNode.resolveOverloadDiscriminator(sameArity, 1, null, arity, first));
        assertEquals(1, arity.get());
        assertEquals(1, first.get());

        // arity disambiguates when exactly one overload has that arity → arity fallback only.
        List<SubprogramInfo> mixedArity = List.of(
                sub("PROCEDURE", p("NUMBER")),
                sub("PROCEDURE", p("NUMBER"), p("NUMBER")));
        assertEquals("NUMBER_NUMBER",
                FlowNode.resolveOverloadDiscriminator(mixedArity, 2, null, arity, first));
        assertEquals(2, arity.get());
        assertEquals(1, first.get());

        // not genuinely overloaded → null (no discriminator).
        assertNull(FlowNode.resolveOverloadDiscriminator(
                List.of(sub("PROCEDURE", p("NUMBER"))), 1, null, arity, first));
        // unknown arity and no types → null (legacy no-split).
        assertNull(FlowNode.resolveOverloadDiscriminator(sameArity, -1, null, arity, first));
    }

    @Test
    @DisplayName("Oracle fixture: same-arity-different-type overloads yield distinct signatures")
    void parserFixtureDistinctSignatures() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ORDER_PKG AS
                  PROCEDURE PROCESS_ORDER(p_id NUMBER) IS
                  BEGIN
                    NULL;
                  END PROCESS_ORDER;

                  PROCEDURE PROCESS_ORDER(p_flag VARCHAR2) IS
                  BEGIN
                    NULL;
                  END PROCESS_ORDER;
                END ORDER_PKG;
                /
                """;
        ParseResult result = parse(sql);
        List<SubprogramInfo> overloads = new ArrayList<>();
        for (SubprogramInfo s : result.getObjects().get(0).getSubprograms()) {
            if ("PROCESS_ORDER".equalsIgnoreCase(s.getName())) overloads.add(s);
        }
        assertEquals(2, overloads.size(), "both PROCESS_ORDER overloads must be retained");
        List<String> discs = new ArrayList<>();
        for (SubprogramInfo s : overloads) discs.add(FlowNode.discriminatorFor(s, false));
        assertTrue(discs.contains("NUMBER") && discs.contains("VARCHAR2"),
                "distinct type signatures expected, got " + discs);
        assertEquals(2, discs.stream().distinct().count());
    }

    // ── edge reconciliation keeps the signature suffix ───────────────────────────────────────

    @Test
    @DisplayName("edge reconciliation preserves a signature discriminator")
    void reconcilePreservesSignatureDiscriminator() {
        FlowNode n1 = new FlowNode();
        n1.setSchema("SALES");
        n1.setPackageName("PKG");
        n1.setObjectName("F");
        n1.setOverloadSignature("NUMBER");
        n1.generateNodeId();
        FlowNode n2 = new FlowNode();
        n2.setSchema("SALES");
        n2.setPackageName("PKG");
        n2.setObjectName("F");
        n2.setOverloadSignature("VARCHAR2");
        n2.generateNodeId();
        List<FlowNode> nodes = List.of(n1, n2);

        FlowEdge e = new FlowEdge();
        e.setFromSchema("APP");
        e.setFromObject("CALLER");
        e.setFromNodeId("APP$CALLER");
        e.setToSchema("APP");
        e.setToPackage("PKG");
        e.setToObject("F");
        e.setToNodeId("APP$PKG$F__VARCHAR2");
        List<FlowEdge> edges = new ArrayList<>(List.of(e));

        ChunkedFlowWriter.reconcileEdgeSchemas(edges, nodes);

        assertEquals("SALES$PKG$F__VARCHAR2", e.getToNodeId());
        assertEquals("SALES", e.getToSchema());
    }
}
