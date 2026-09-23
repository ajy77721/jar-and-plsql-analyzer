package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2StatementInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep audit of DB2 LEAVE/ITERATE label capture across nested, sibling, and
 * variable-colliding labeled loops (added per follow-up review of the
 * flat-capture LEAVE/ITERATE implementation in Db2AnalysisVisitor).
 *
 * Verdict recorded here: the visitor performs NO scope resolution at all — it
 * simply records whatever identifier token follows LEAVE/ITERATE verbatim as
 * the statement's `label` field, with the bare type "LEAVE" / "ITERATE".
 * (The label is deliberately NOT concatenated into the type: doing so produced
 * one distinct statement-type key per labeled loop.) Since this is a flat structural
 * analyzer (not a control-flow simulator), that is exactly the correct and
 * sufficient behavior: the label named in the source IS the fact to capture,
 * regardless of which loop it lexically sits inside or how deeply nested it
 * is. These tests confirm there is no mis-attribution to the "nearest"
 * enclosing loop's label, no cross-contamination between sibling loops, and
 * no misclassification when a label collides with a variable/table name.
 */
class Db2LabeledLoopAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String body) {
        return engine.parse(
                "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n" + body + "\nEND", "p", null);
    }

    private List<String> statementTypes(Db2RoutineInfo r) {
        return r.getStatements().stream().map(Db2StatementInfo::getType).collect(Collectors.toList());
    }

    /** Renders "TYPE:label" purely for assertion convenience — this form is NOT emitted as a type. */
    private List<String> typeWithLabel(Db2RoutineInfo r) {
        return r.getStatements().stream()
                .map(s -> s.getLabel() == null ? s.getType() : s.getType() + ":" + s.getLabel())
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("nested loop: inner LEAVE targets the OUTER label verbatim, not the nearest (inner) loop")
    void innerLeaveTargetsOuterLabel() {
        Db2RoutineInfo r = parse(
                "outer_loop: LOOP\n"
              + "  inner_loop: LOOP\n"
              + "    IF 1 = 1 THEN\n"
              + "      LEAVE outer_loop;\n"
              + "    END IF;\n"
              + "  END LOOP inner_loop;\n"
              + "END LOOP outer_loop;");
        System.out.println("[nested-leave] errors=" + r.getErrors());
        List<String> types = statementTypes(r);
        System.out.println("[nested-leave] statements=" + types);
        assertTrue(types.contains("LEAVE"), "LEAVE must be emitted with the BARE type \"LEAVE\"");
        assertTrue(types.stream().noneMatch(t -> t.startsWith("LEAVE:")),
                "the loop label must never be concatenated into the statement TYPE");
        types = typeWithLabel(r);
        assertTrue(types.contains("LEAVE:outer_loop"),
                "LEAVE outer_loop from inside inner_loop must be captured with the exact label named, "
              + "not silently rewritten to the nearest enclosing loop's label");
        assertFalse(types.contains("LEAVE:inner_loop"),
                "LEAVE outer_loop must not ALSO be mis-attributed to inner_loop");
    }

    @Test
    @DisplayName("nested loop: ITERATE from inner loop targeting outer label is captured, not misclassified as a CALL/table-op")
    void innerIterateTargetsOuterLabel() {
        Db2RoutineInfo r = parse(
                "outer_loop: LOOP\n"
              + "  SELECT * FROM ERP_ORDERS;\n"
              + "  inner_loop: LOOP\n"
              + "    CALL some_proc();\n"
              + "    ITERATE outer_loop;\n"
              + "  END LOOP inner_loop;\n"
              + "END LOOP outer_loop;");
        System.out.println("[nested-iterate] errors=" + r.getErrors());
        List<String> types = statementTypes(r);
        System.out.println("[nested-iterate] statements=" + types);
        assertTrue(types.contains("ITERATE"), "ITERATE must be emitted with the BARE type \"ITERATE\"");
        assertTrue(types.stream().noneMatch(t -> t.startsWith("ITERATE:")),
                "the loop label must never be concatenated into the statement TYPE");
        assertTrue(typeWithLabel(r).contains("ITERATE:outer_loop"), "ITERATE outer_loop was not captured");
        assertFalse(r.getCalls().stream().anyMatch(c -> "outer_loop".equalsIgnoreCase(c.getName())),
                "ITERATE <label> must not be misclassified as a routine CALL");
        // Confirm capture continues normally around/after the ITERATE for a different label:
        // the real CALL and the real table op inside the labeled blocks must still be seen.
        assertTrue(r.getCalls().stream().anyMatch(c -> "some_proc".equalsIgnoreCase(c.getName())),
                "a genuine CALL inside a labeled loop containing an ITERATE for a different label was lost");
        assertFalse(r.getTableOps().isEmpty(),
                "a genuine table op inside a labeled loop was lost due to sibling LEAVE/ITERATE handling");
    }

    @Test
    @DisplayName("sibling labeled loops at the same nesting level: no cross-contamination between their LEAVE labels")
    void siblingLoopsNoCrossContamination() {
        Db2RoutineInfo r = parse(
                "loop_a: LOOP\n"
              + "  LEAVE loop_a;\n"
              + "END LOOP loop_a;\n"
              + "loop_b: LOOP\n"
              + "  LEAVE loop_b;\n"
              + "END LOOP loop_b;");
        System.out.println("[siblings] errors=" + r.getErrors());
        List<String> types = statementTypes(r);
        System.out.println("[siblings] statements=" + types);
        assertEquals(2, types.stream().filter(t -> t.equals("LEAVE")).count(),
                "two differently-labeled LEAVEs must collapse to two records of the SAME bare type \"LEAVE\"");
        assertEquals(1, types.stream().filter(t -> t.startsWith("LEAVE")).distinct().count(),
                "differently-labeled loops must not create distinct one-off statement-type keys");
        types = typeWithLabel(r);
        long leaveA = types.stream().filter(t -> t.equals("LEAVE:loop_a")).count();
        long leaveB = types.stream().filter(t -> t.equals("LEAVE:loop_b")).count();
        assertEquals(1, leaveA, "expected exactly one LEAVE:loop_a");
        assertEquals(1, leaveB, "expected exactly one LEAVE:loop_b");
        assertFalse(types.contains("LEAVE:loop_a") && types.contains("LEAVE:loop_b") && leaveA > 1,
                "sibling loop bodies must not bleed into each other's LEAVE capture");
    }

    @Test
    @DisplayName("label name colliding with a variable/table name elsewhere is not misclassified")
    void labelCollidingWithVariableName() {
        // "orders" is used both as a labeled loop AND as a table name elsewhere in the body.
        Db2RoutineInfo r = parse(
                "DECLARE orders INTEGER DEFAULT 0;\n"
              + "orders: LOOP\n"
              + "  SET orders = orders + 1;\n"
              + "  IF orders > 5 THEN\n"
              + "    LEAVE orders;\n"
              + "  END IF;\n"
              + "END LOOP orders;\n"
              + "SELECT * FROM orders;");
        System.out.println("[collision] errors=" + r.getErrors());
        List<String> types = statementTypes(r);
        System.out.println("[collision] statements=" + types);
        System.out.println("[collision] tableOps=" + r.getTableOps());
        assertTrue(types.contains("LEAVE"), "LEAVE must be emitted with the BARE type \"LEAVE\"");
        assertTrue(typeWithLabel(r).contains("LEAVE:orders"), "LEAVE orders (the loop label) was not captured");
        // The trailing SELECT * FROM orders must still be captured as a genuine table op,
        // proving the label token and the later table-name token are not cross-confused.
        assertTrue(r.getTableOps().stream().anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName())),
                "SELECT * FROM orders (a real table op) was lost/misclassified after a same-named label");
    }
    @Test
    @DisplayName("regression: many uniquely-labeled loops collapse to ONE \"LEAVE\" type key, labels preserved")
    void manyLabeledLoopsDoNotExplodeStatementTypes() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            b.append("LOOP_RC0").append(i).append(": LOOP\n")
             .append("  LEAVE LOOP_RC0").append(i).append(";\n")
             .append("END LOOP LOOP_RC0").append(i).append(";\n");
        }
        Db2RoutineInfo r = parse(b.toString());
        System.out.println("[explosion] errors=" + r.getErrors());
        List<String> types = statementTypes(r);
        System.out.println("[explosion] statements=" + types);
        assertEquals(5, types.stream().filter(t -> t.equals("LEAVE")).count(),
                "each labeled loop must contribute exactly one bare LEAVE record");
        assertEquals(1, types.stream().filter(t -> t.startsWith("LEAVE")).distinct().count(),
                "5 uniquely-labeled loops must yield ONE distinct statement-type key, not 5");
        List<String> labels = typeWithLabel(r);
        for (int i = 0; i < 5; i++) {
            assertTrue(labels.contains("LEAVE:LOOP_RC0" + i),
                    "label LOOP_RC0" + i + " was silently lost when moved off the type string");
        }
    }
}
