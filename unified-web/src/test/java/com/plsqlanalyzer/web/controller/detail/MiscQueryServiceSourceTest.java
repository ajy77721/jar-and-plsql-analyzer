package com.plsqlanalyzer.web.controller.detail;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.service.LazyAnalysisResult;
import com.plsqlanalyzer.web.service.PersistenceService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * MID-027 (§124) — end-to-end through {@code MiscQueryService.getSource}, not just
 * {@code PersistenceService} in isolation: proves the real call site threads through to the
 * streamed single-key read rather than {@code AnalysisResult.getSourceMap()}.
 */
class MiscQueryServiceSourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void getSourceAnswersFromOneKeyWithoutLoadingTheWholeMap() throws Exception {
        Map<String, List<String>> sourceMap = new LinkedHashMap<>();
        sourceMap.put("ERP.PKG_A", List.of("line1", "line2"));
        sourceMap.put("ERP.PKG_B", List.of("unrelated"));

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey file = AnalysisPaths.claudeFileKey("A1", "analysis-source.json");
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("sourceMap", sourceMap);
        try (OutputStream out = store.write(file)) {
            MAPPER.writeValue(out, wrapper);
        }
        PersistenceService persistence = new PersistenceService(store);
        LazyAnalysisResult lazy = new LazyAnalysisResult("A1", persistence);

        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.getLatestResult()).thenReturn(lazy);

        MiscQueryService svc = new MiscQueryService(analysisService, mock(PlsqlConfig.class),
                mock(DbSourceFetcher.class), mock(GraphMapHelper.class),
                mock(ObjectResolutionService.class), persistence);

        ResponseEntity<Map<String, Object>> response = svc.getSource("erp", "pkg_a");

        assertEquals(200, response.getStatusCode().value());
        assertEquals("line1\nline2", response.getBody().get("content"));
        assertFalse(lazy.isSourceLoaded(),
                "getSource for one object must not force the whole analysis's sourceMap resident");
    }

    @Test
    void getSourceReturns404ForAMissingObject() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey file = AnalysisPaths.claudeFileKey("A1", "analysis-source.json");
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("sourceMap", Map.of("ERP.PKG_A", List.of("x")));
        try (OutputStream out = store.write(file)) {
            MAPPER.writeValue(out, wrapper);
        }
        PersistenceService persistence = new PersistenceService(store);
        LazyAnalysisResult lazy = new LazyAnalysisResult("A1", persistence);

        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.getLatestResult()).thenReturn(lazy);

        MiscQueryService svc = new MiscQueryService(analysisService, mock(PlsqlConfig.class),
                mock(DbSourceFetcher.class), mock(GraphMapHelper.class),
                mock(ObjectResolutionService.class), persistence);

        ResponseEntity<Map<String, Object>> response = svc.getSource("erp", "not_there");
        assertEquals(404, response.getStatusCode().value());
    }
}
