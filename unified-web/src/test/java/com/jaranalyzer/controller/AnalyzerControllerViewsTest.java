package com.jaranalyzer.controller;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.AnalysisDataProvider;
import com.jaranalyzer.service.AnalysisDataProviderFactory;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.PersistenceService;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The JAR-side view summaries, at the endpoint boundary: {@code /summary/header} and {@code
 * /collections} serve a precomputed sidecar when one exists, and the sidecar is byte-identical to
 * what the legacy full-document path produced the first time. The provider is a mock whose {@code
 * openStream} is counted, so "served from the sidecar" is proved by the document never being opened
 * a second time — not merely by the bytes matching.
 */
class AnalyzerControllerViewsTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ID = "svc";

    private InMemoryArtifactStore store;
    private JarDataPaths paths;
    private AnalysisDataProvider provider;
    private AnalyzerController controller;

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemoryArtifactStore();
        paths = new JarDataPaths("data/jar", store);

        provider = mock(AnalysisDataProvider.class);
        when(provider.lastModifiedMillis()).thenReturn(1000L);
        when(provider.getSummaryCacheKey()).thenReturn(null); // stats degrade to absent, as designed

        PersistenceService persistence = mock(PersistenceService.class);
        when(persistence.getPaths()).thenReturn(paths);
        Map<String, Object> versionInfo = new LinkedHashMap<>();
        versionInfo.put("hasStatic", true);
        versionInfo.put("hasCorrected", false);
        when(persistence.getVersionInfo(ID)).thenReturn(versionInfo);

        AnalysisDataProviderFactory factory = mock(AnalysisDataProviderFactory.class);
        when(factory.resolve(eq(ID), any())).thenReturn(provider);

        controller = build(persistence, factory);
    }

    private AnalyzerController build(PersistenceService persistence,
                                     AnalysisDataProviderFactory factory) throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = M;
            else if (PersistenceService.class.equals(type)) args[i] = persistence;
            else if (AnalysisDataProviderFactory.class.equals(type)) args[i] = factory;
            else if (ArtifactStore.class.equals(type)) args[i] = store;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(M, 10000);
            else args[i] = mock(type);
        }
        return (AnalyzerController) ctor.newInstance(args);
    }

    private static void stubHeaderStream(AnalysisDataProvider provider) throws Exception {
        org.mockito.Mockito.doAnswer(inv -> {
            com.fasterxml.jackson.core.JsonGenerator gen = inv.getArgument(1);
            gen.writeStartObject();
            gen.writeStringField("jarName", "svc.jar");
            gen.writeNumberField("totalClasses", 1);
            gen.writeEndObject();
            return null;
        }).when(provider).streamSummaryHeader(any(), any());
    }

    private static void stubDoc(AnalysisDataProvider provider, String doc) throws Exception {
        when(provider.openStream()).thenAnswer(inv ->
                new ByteArrayInputStream(doc.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void summaryHeaderIsByteIdenticalFromSidecarAndTheDocumentIsNotReopened() throws Exception {
        stubHeaderStream(provider);
        stubDoc(provider, "{\"jarName\":\"svc.jar\",\"totalClasses\":1,\"endpoints\":[]}");

        MockHttpServletResponse first = new MockHttpServletResponse();
        controller.getSummaryHeader(ID, null, new MockHttpServletRequest(), first);
        byte[] legacy = first.getContentAsByteArray();

        assertTrue(store.exists(paths.summaryHeaderViewKey(ID)), "header sidecar not written");

        MockHttpServletResponse second = new MockHttpServletResponse();
        controller.getSummaryHeader(ID, null, new MockHttpServletRequest(), second);

        assertArrayEquals(legacy, second.getContentAsByteArray(),
                "sidecar header differs from the legacy header");
        verify(provider, times(1)).openStream();
    }

    @Test
    void aStaleSidecarIsRejectedByTheStampedMtimeAndTheDocumentIsRebuilt() throws Exception {
        stubHeaderStream(provider);
        stubDoc(provider, "{\"jarName\":\"svc.jar\",\"totalClasses\":1,\"endpoints\":[]}");
        controller.getSummaryHeader(ID, null, new MockHttpServletRequest(), new MockHttpServletResponse());
        verify(provider, times(1)).openStream();

        // Age the sidecar past the provider's own write stamp: the freshness check must reject it
        // and rebuild, so a re-analysed JAR can never serve the previous run's header.
        store.setLastModifiedForTest(paths.summaryHeaderViewKey(ID), 500L); // < provider's 1000
        controller.getSummaryHeader(ID, null, new MockHttpServletRequest(), new MockHttpServletResponse());
        verify(provider, times(2)).openStream();
    }

    @Test
    void collectionsIsByteIdenticalFromSidecarAndTheDocumentIsNotReopened() throws Exception {
        stubDoc(provider, "{\"jarName\":\"svc.jar\",\"endpoints\":["
                + "{\"controllerSimpleName\":\"Ctrl\",\"methodName\":\"m\",\"httpMethod\":\"GET\","
                + "\"fullPath\":\"/x\",\"aggregatedCollections\":[\"COLL_A\"],"
                + "\"aggregatedOperations\":{\"COLL_A\":\"READ\"}}]}");

        byte[] legacy = M.writeValueAsBytes(
                controller.getCollectionReport(ID, null, null, null, null).getBody());
        assertTrue(store.exists(paths.collectionsViewKey(ID)), "collections sidecar not written");

        byte[] cached = M.writeValueAsBytes(
                controller.getCollectionReport(ID, null, null, null, null).getBody());
        assertArrayEquals(legacy, cached, "sidecar collections report differs from the legacy one");
        // openStream: once for the legacy aggregate, never again once the sidecar exists.
        verify(provider, times(1)).openStream();
    }
}
