package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.AnalysisDataProvider;
import com.jaranalyzer.service.AnalysisDataProviderFactory;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * MID-016 — the JAR call-tree endpoint's default is now {@code dag}, and the WAR endpoint's is
 * deliberately unchanged.
 *
 * <h2>What is asserted, and why by dispatch rather than by response shape</h2>
 *
 * <p>Building a real endpoint's flattened DAG or expanded tree needs a real
 * {@code AnalysisDataProvider} over a real document — this test cares about which of
 * {@code streamCallTreeDag} / {@code streamCallTree} gets <b>called</b>, not what either produces,
 * since that is already covered by the JAR call-tree fixtures elsewhere in this suite. A mock
 * provider records the dispatch directly, which is both simpler and a more precise assertion of
 * the actual regression this row is about.
 *
 * <h2>The asymmetry this test exists to pin</h2>
 *
 * <p>The JAR client (`jar/js/api.js`) has a {@code JA.dag} inflation module and already asks for
 * {@code format=dag} explicitly on every call; its one unqualified fallback request was updated to
 * ask for {@code format=full} in the same change that flipped this default, so it keeps working for
 * documents written before the flat format existed. The WAR client (`war/js/api.js`) has no such
 * module — checked, not assumed, before deciding this — so {@code WarAnalyzerController}'s default
 * stays {@code full} and {@code dag} is added as an opt-in only. A test that only checked the JAR
 * side would have missed the exact same mistake on the WAR side.
 */
class CallTreeFormatDefaultTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static AnalysisDataProvider mockProvider() throws IOException {
        AnalysisDataProvider provider = mock(AnalysisDataProvider.class);
        when(provider.openStream()).thenAnswer(inv ->
                new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8)));
        return provider;
    }

    private static <T> T build(Class<T> controllerClass, AnalysisDataProvider provider)
            throws Exception {
        Constructor<?> ctor = controllerClass.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) {
                args[i] = MAPPER;
            } else if (AnalysisDataProviderFactory.class.equals(type)) {
                AnalysisDataProviderFactory factory = mock(AnalysisDataProviderFactory.class);
                when(factory.resolve(any(), any())).thenReturn(provider);
                args[i] = factory;
            } else {
                args[i] = mock(type);
            }
        }
        return controllerClass.cast(ctor.newInstance(args));
    }

    @Test
    void jarEndpointDefaultsToDag() throws Exception {
        AnalysisDataProvider provider = mockProvider();
        AnalyzerController c = build(AnalyzerController.class, provider);

        c.getEndpointCallTree("jar1", 0, null, null, new MockHttpServletResponse());

        verify(provider, times(1)).streamCallTreeDag(any(), any(), eq(0));
        verify(provider, times(0)).streamCallTree(any(), any(), anyInt());
    }

    @Test
    void jarEndpointFormatFullStillReachesTheExpandedForm() throws Exception {
        AnalysisDataProvider provider = mockProvider();
        AnalyzerController c = build(AnalyzerController.class, provider);

        c.getEndpointCallTree("jar1", 0, null, "full", new MockHttpServletResponse());

        verify(provider, times(1)).streamCallTree(any(), any(), eq(0));
        verify(provider, times(0)).streamCallTreeDag(any(), any(), anyInt());
    }

    @Test
    void jarEndpointFormatDagIsUnchanged() throws Exception {
        // The explicit form the JS client has always sent — must keep working identically to
        // before this row, since it is the primary path, not the one that changed.
        AnalysisDataProvider provider = mockProvider();
        AnalyzerController c = build(AnalyzerController.class, provider);

        c.getEndpointCallTree("jar1", 0, null, "dag", new MockHttpServletResponse());

        verify(provider, times(1)).streamCallTreeDag(any(), any(), eq(0));
    }

    /**
     * The WAR half, structurally rather than behaviourally.
     *
     * <p>{@code WarAnalyzerController.resolveProvider} does not go through
     * {@code AnalysisDataProviderFactory} at all — it inspects a {@code version} parameter and
     * constructs one of several concrete provider types itself from a persistence service this
     * controller does not expose for substitution. Reaching {@code getEndpointCallTree} through
     * that path in a unit test means faithfully reconstructing WAR's whole resolution chain, which
     * is disproportionate to what this test needs to prove: that the branch on {@code format} is
     * present and defaults the right way. Read from source instead, the same way this codebase's
     * gate tests already check for a pattern's presence when exercising it end to end is not
     * proportionate.
     */
    private static String warControllerSource() throws Exception {
        return new String(java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                "src/main/java/com/waranalyzer/controller/WarAnalyzerController.java")),
                StandardCharsets.UTF_8);
    }

    @Test
    void warEndpointDefaultsToFullNotDag() throws Exception {
        String src = warControllerSource();
        int at = src.indexOf("public void getEndpointCallTree");
        assertEquals(true, at > 0, "getEndpointCallTree was not found in WarAnalyzerController");
        String body = src.substring(at, src.indexOf("\n    }", at));
        assertEquals(true, body.contains("\"dag\".equalsIgnoreCase(format)"),
                "WAR's default flipped to match JAR's (!\"full\".equalsIgnoreCase) — this is the "
                        + "regression this test exists to catch: war/js/api.js has no JA.dag "
                        + "module to inflate the flat shape that default would return");
    }

    @Test
    void warEndpointOffersDagAsAnExplicitOptIn() throws Exception {
        String src = warControllerSource();
        int at = src.indexOf("public void getEndpointCallTree");
        String body = src.substring(at, src.indexOf("\n    }", at));
        assertEquals(true, body.contains("streamCallTreeDag"),
                "WAR's endpoint lost the dag branch entirely — it should still be reachable via "
                        + "?format=dag even though it is not the default");
    }
}
