package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code call-tree.json} must be written compact.
 *
 * <p>It is the largest derived artifact under {@code endpoints/} — 36.07 MB across 317 endpoints —
 * and no human reads it: its only route expands the dictionary and re-serialises the result, so the
 * stored whitespace never reaches a response. Indentation is pure cost.
 *
 * <p>This test exists because an earlier attempt to compact it appeared to work (the file shrank
 * 36 → 22 MB) but that was a different subject JAR, not the change — the output was still indented.
 * A size measurement across two different inputs proves nothing; this asserts the actual bytes.
 */
class SidecarCompactnessTest {

    private CallNode node(String id, CallNode... children) {
        CallNode n = new CallNode();
        n.setId(id);
        n.setClassName("com.demo." + id);
        n.setMethodName("m" + id);
        if (children.length > 0) n.setChildren(java.util.List.of(children));
        return n;
    }

    @Test
    @DisplayName("the sidecar serialises without indentation")
    void sidecarIsCompact() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        CallNode root = node("A", node("B"), node("C"));

        String json = mapper.writeValueAsString(CallTreeFlattener.sidecar(mapper, root));

        assertFalse(json.contains("\n"),
                "a compact document has no newlines; got:\n" + json);
        assertFalse(json.contains("\" : "),
                "\" : \" is Jackson's default pretty printer; got:\n" + json);
        assertTrue(json.contains("\"callTreeFormat\":\"dag-v1\""),
                "and the content must still be the dag-v1 sidecar; got:\n" + json);
    }

    @Test
    @DisplayName("a pretty writer on the same mapper WOULD indent — so the choice is the writer")
    void prettyWriterIsTheDifference() throws Exception {
        // Pins where the cost comes from, so the compact call above is not "simplified" back to
        // writerWithDefaultPrettyPrinter to match its neighbours in EndpointOutputWriter.
        ObjectMapper mapper = new ObjectMapper();
        var sidecar = CallTreeFlattener.sidecar(mapper, node("A", node("B")));

        String compact = mapper.writeValueAsString(sidecar);
        String pretty = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sidecar);

        assertTrue(pretty.length() > compact.length() * 1.4,
                "indenting should cost substantially; compact=" + compact.length()
                        + " pretty=" + pretty.length());
        assertEquals(mapper.readTree(compact), mapper.readTree(pretty),
                "and the two must carry identical content");
    }
}
