package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Task #29 — nested-subprogram marking (Oracle).
 *
 * <p>Golden fixture: a package body with a top-level member ({@code OUTER_PROC}), a subprogram
 * declared inside its body ({@code LOCAL_HELPER}), one nested inside THAT ({@code DEEP_HELPER}),
 * and an ordinary sibling ({@code SIBLING_PROC}). The visitor must stamp
 * {@code nested=true}/{@code scopeDepth=N} on the nested entries and leave the top-level members
 * at {@code nested=false}/{@code scopeDepth=0}. The marker is metadata only — it never participates
 * in id generation, so non-nested ids stay byte-identical.
 */
public class OracleNestedSubprogramMarkingTest extends ParserTestBase {

    private static final String FIXTURE = """
            CREATE OR REPLACE PACKAGE BODY MY_PKG AS
              PROCEDURE OUTER_PROC IS
                PROCEDURE LOCAL_HELPER IS
                  PROCEDURE DEEP_HELPER IS
                  BEGIN
                    INSERT INTO deep_log (msg) VALUES ('deep');
                  END DEEP_HELPER;
                BEGIN
                  DEEP_HELPER;
                  INSERT INTO helper_log (msg) VALUES ('nested');
                END LOCAL_HELPER;
              BEGIN
                LOCAL_HELPER;
                INSERT INTO main_log (msg) VALUES ('outer');
              END OUTER_PROC;

              PROCEDURE SIBLING_PROC IS
              BEGIN
                INSERT INTO sibling_log (msg) VALUES ('sibling');
              END SIBLING_PROC;
            END MY_PKG;
            /
            """;

    @Test
    @DisplayName("#29: top-level package members are nested=false / scopeDepth=0")
    void topLevelMembersAreNotNested() {
        ParseResult result = parse(FIXTURE);
        assertNoParsErrors(result, "nested marking fixture");

        SubprogramInfo outer = findSub(result, "OUTER_PROC");
        SubprogramInfo sibling = findSub(result, "SIBLING_PROC");
        org.junit.jupiter.api.Assertions.assertNotNull(outer, "OUTER_PROC present");
        org.junit.jupiter.api.Assertions.assertNotNull(sibling, "SIBLING_PROC present");
        org.junit.jupiter.api.Assertions.assertFalse(outer.isNested(), "OUTER_PROC is top-level");
        org.junit.jupiter.api.Assertions.assertEquals(0, outer.getScopeDepth());
        org.junit.jupiter.api.Assertions.assertFalse(sibling.isNested(), "SIBLING_PROC is top-level");
        org.junit.jupiter.api.Assertions.assertEquals(0, sibling.getScopeDepth());
    }

    @Test
    @DisplayName("#29: nested subprograms carry nested=true and their scopeDepth")
    void nestedSubprogramsAreMarked() {
        ParseResult result = parse(FIXTURE);
        assertNoParsErrors(result, "nested marking fixture");

        SubprogramInfo local = findSub(result, "LOCAL_HELPER");
        SubprogramInfo deep = findSub(result, "DEEP_HELPER");
        org.junit.jupiter.api.Assertions.assertNotNull(local, "LOCAL_HELPER present");
        org.junit.jupiter.api.Assertions.assertNotNull(deep, "DEEP_HELPER present");
        org.junit.jupiter.api.Assertions.assertTrue(local.isNested(), "LOCAL_HELPER is nested");
        org.junit.jupiter.api.Assertions.assertEquals(1, local.getScopeDepth(),
                "LOCAL_HELPER is one body deep");
        org.junit.jupiter.api.Assertions.assertTrue(deep.isNested(), "DEEP_HELPER is nested");
        org.junit.jupiter.api.Assertions.assertEquals(2, deep.getScopeDepth(),
                "DEEP_HELPER is two bodies deep");
    }

    @Test
    @DisplayName("#29: nesting survives FlowNode and never changes the node id")
    void nestingPersistsThroughFlowNodeWithoutChangingIds() {
        FlowNode node = new FlowNode();
        node.setSchema("MY_SCHEMA");
        node.setPackageName("MY_PKG");
        node.setObjectName("OUTER_PROC");
        String idBefore = node.buildShortId();

        // Additive metadata must not participate in id generation.
        node.setNested(true);
        node.setScopeDepth(1);
        org.junit.jupiter.api.Assertions.assertEquals(idBefore, node.buildShortId(),
                "nested/scopeDepth must never alter the node id");
        org.junit.jupiter.api.Assertions.assertEquals("MY_SCHEMA$MY_PKG$OUTER_PROC", idBefore);

        // A default (non-nested) node is byte-identical to the pre-#29 shape.
        FlowNode plain = new FlowNode();
        plain.setSchema("MY_SCHEMA");
        plain.setPackageName("MY_PKG");
        plain.setObjectName("SIBLING_PROC");
        org.junit.jupiter.api.Assertions.assertFalse(plain.isNested());
        org.junit.jupiter.api.Assertions.assertEquals(0, plain.getScopeDepth());
        org.junit.jupiter.api.Assertions.assertEquals("MY_SCHEMA$MY_PKG$SIBLING_PROC", plain.buildShortId());
    }

    @Test
    @DisplayName("#29: SubprogramSummaryInfo carries the nesting marker (members[] path)")
    void summaryCarriesNesting() {
        SubprogramSummaryInfo summary = new SubprogramSummaryInfo(
                "LOCAL_HELPER", "PROCEDURE", 4, 10, true, 1);
        org.junit.jupiter.api.Assertions.assertTrue(summary.isNested());
        org.junit.jupiter.api.Assertions.assertEquals(1, summary.getScopeDepth());

        // The legacy 4-arg constructor keeps its old shape (non-nested).
        SubprogramSummaryInfo legacy = new SubprogramSummaryInfo("OUTER_PROC", "PROCEDURE", 3, 16);
        org.junit.jupiter.api.Assertions.assertFalse(legacy.isNested());
        org.junit.jupiter.api.Assertions.assertEquals(0, legacy.getScopeDepth());
    }
}
