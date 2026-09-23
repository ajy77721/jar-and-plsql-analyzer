package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * MID-015 — {@code GET /{id}} no longer serves the whole analysis body by default.
 *
 * <h2>What was wrong</h2>
 *
 * <p>A real {@code analysis.json} reaches 912 MB, and even streamed this endpoint still passes
 * through {@code classes} and {@code endpoints} — most of the document. Checked across every
 * client script in this repository: <b>nothing calls it</b>. Every screen moved to a narrower
 * endpoint (<code>/summary/header</code>, <code>/summary/endpoints</code>, ...) some time ago, and
 * no test exercised the old one either — it was declared and never wired to anything but itself,
 * the same shape as {@code am_txn_statement} and {@code IdCodec.dbColumn}.
 *
 * <h2>Why gated rather than deleted</h2>
 *
 * <p>An operator's own script outside this repository may still call it, and there is no way to
 * know from inside the repo whether one does. So the default path returns 410 naming the
 * replacements, and {@code ?full=true} is kept as an explicit, loud escape hatch — the same shape
 * MID-004 used for {@code /call-graph}.
 *
 * <h2>Why reflection builds the controller</h2>
 *
 * <p>The constructor takes 21 collaborators, and this test exercises none of them except
 * {@code ObjectMapper} — the gated path returns before {@code resolveProvider} is ever called. A
 * hand-written 21-argument call site would need updating every time an unrelated dependency is
 * added to the controller, for a test that does not use it. Reflection picks each parameter's mock
 * (or the real mapper) by type, so the constructor can grow without this test noticing.
 */
class AnalyzerControllerFullBodyGateTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static AnalyzerController build() throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            args[i] = ObjectMapper.class.equals(type) ? MAPPER : mock(type);
        }
        return (AnalyzerController) ctor.newInstance(args);
    }

    @Test
    void theDefaultRequestIsRefusedRatherThanServingTheWholeDocument() throws Exception {
        AnalyzerController c = build();
        MockHttpServletResponse resp = new MockHttpServletResponse();

        c.getAnalysis("some-jar", null, null, resp);

        assertEquals(410, resp.getStatus(), "the default request was not refused with 410 Gone");
        JsonNode body = MAPPER.readTree(resp.getContentAsByteArray());
        assertTrue(body.has("error"), "the 410 body does not say why");
        assertTrue(body.has("replacements"), "the 410 body does not name a replacement endpoint");
        assertTrue(body.path("replacements").has("header"),
                "the header replacement is not named");
    }

    @Test
    void fullFalseIsRefusedTheSameWayAsAbsent() throws Exception {
        // The parameter is a boxed Boolean specifically so "absent" and "explicitly false" are the
        // same case — a caller passing full=false must not accidentally get the old behaviour by
        // relying on Spring's null-vs-false handling of a primitive default.
        AnalyzerController c = build();
        MockHttpServletResponse resp = new MockHttpServletResponse();

        c.getAnalysis("some-jar", null, false, resp);

        assertEquals(410, resp.getStatus(), "full=false served the body instead of refusing");
    }

    @Test
    void fullTrueReachesTheStreamingPath() throws Exception {
        // Not a full round trip through resolveProvider — that needs a real AnalysisDataProvider —
        // but the boundary this test owns is that ?full=true does NOT take the 410 branch. Verified
        // by the absence of a 410, since resolveProvider on a mocked factory throws before any
        // content is written; catching that exception is the signal the gate was bypassed as asked.
        AnalyzerController c = build();
        MockHttpServletResponse resp = new MockHttpServletResponse();

        try {
            c.getAnalysis("some-jar", null, true, resp);
        } catch (RuntimeException | IOException expected) {
            // resolveProvider has nothing real to resolve against a mock factory; reaching it is
            // the point of this test, not succeeding past it.
        }

        assertFalse(resp.getStatus() == 410,
                "full=true still hit the 410 gate meant for the default path");
    }

    @Test
    void theEscapeHatchIsDocumentedInTheRefusalBody() throws Exception {
        AnalyzerController c = build();
        MockHttpServletResponse resp = new MockHttpServletResponse();

        c.getAnalysis("some-jar", null, null, resp);

        JsonNode body = MAPPER.readTree(resp.getContentAsByteArray());
        String hatch = body.path("escapeHatch").asText("");
        assertTrue(hatch.contains("full=true"),
                "the refusal does not say how to opt back into the old behaviour: " + hatch);
    }
}
