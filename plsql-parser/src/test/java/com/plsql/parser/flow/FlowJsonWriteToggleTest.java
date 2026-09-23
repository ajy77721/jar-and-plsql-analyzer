package com.plsql.parser.flow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code flow.json} must stay off by default and be restorable by property alone.
 *
 * <p>The file is a merged view re-embedding every chunk, every edge file and {@code tables.json} —
 * 3.3 MB measured at 52 nodes, ~300 MB extrapolated at 5,000 — and nothing reads it. Writing it is
 * pure cost, and over a ~0.5 MB/s link to object storage that is minutes per analysis.
 *
 * <p>The revert path has to work without a code change, so it is worth pinning: if the property
 * stopped being honoured the only symptom would be a slow analysis, which is easy to misattribute.
 */
class FlowJsonWriteToggleTest {

    private static final String PROP = "plsql.write-flow-json";

    @AfterEach
    void clearProperty() {
        System.clearProperty(PROP);
    }

    private boolean enabled() throws Exception {
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("writeFlowJsonEnabled");
        m.setAccessible(true);
        return (boolean) m.invoke(null);
    }

    @Test
    @DisplayName("off by default")
    void offByDefault() throws Exception {
        System.clearProperty(PROP);
        assertFalse(enabled(), "an artifact with no reader must not be written by default");
    }

    @Test
    @DisplayName("the documented property restores it")
    void propertyRestoresIt() throws Exception {
        System.setProperty(PROP, "true");
        assertTrue(enabled(), "this is the revert path; it must work without a code change");
    }

    @Test
    @DisplayName("only an exact 'true' enables it")
    void onlyTrueEnables() throws Exception {
        // Boolean.parseBoolean is case-insensitive but not whitespace-tolerant, so record the
        // actual behaviour rather than assuming; a value that silently enabled a 300 MB write
        // would be an unpleasant surprise.
        for (String v : new String[]{"false", "", "yes", "1"}) {
            System.setProperty(PROP, v);
            assertFalse(enabled(), "'" + v + "' must not enable the write");
        }
        System.setProperty(PROP, "TRUE");
        assertTrue(enabled(), "case-insensitive 'true' is accepted");
    }

    @Test
    @DisplayName("api/** is still written when flow.json is skipped")
    void apiFilesAreNotConditional() throws Exception {
        // The UI reads api/**; only the unread merged view is optional. If the skip branch ever
        // swallowed writeApiFiles, an analysis would produce nothing the UI could display — and
        // the tests above would still pass, which is why this one reads the structure.
        String src = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/com/plsql/parser/flow/ChunkedFlowWriter.java"));
        // MID-026 (§177): writeApiFiles now also takes the explicit node-metrics map.
        int api = src.indexOf("writeApiFiles(result, nodeMetrics);");
        assertTrue(api > 0, "writeApiFiles call not found — did the method move?");
        String before = src.substring(0, api);
        int lastConditional = before.lastIndexOf("if (writeFlowJsonEnabled())");
        int lastBrace = before.lastIndexOf("}");
        assertTrue(lastBrace > lastConditional,
                "writeApiFiles must sit OUTSIDE the flow.json conditional");
    }
}
