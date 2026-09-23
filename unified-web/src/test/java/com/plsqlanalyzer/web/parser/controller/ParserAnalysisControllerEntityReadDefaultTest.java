package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.client.MongoDatabase;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import com.plsqlanalyzer.web.parser.service.EntityTablesReader;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Mode 4's mode-wide toggle (§159) — {@code useEntitySource}'s own resolution logic, exercised
 * through the real endpoint rather than as a unit test of a private method, since the whole point
 * is that all 7 facet endpoints share one decision, not seven copies of it.
 *
 * <p>Uses {@code tables} as the representative facet — the same one every other mode-4 pattern was
 * first proven against (§150) — since the resolution logic itself is facet-agnostic.
 */
class ParserAnalysisControllerEntityReadDefaultTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ParserAnalysisController build(
            AnalysisRepository repository,
            com.analyzer.repo.EntityIngestConfig.EntityIngestConnection connection,
            EntityTablesReader tablesReader,
            boolean entityReadDefault) throws Exception {
        Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (AnalysisRepository.class.equals(type)) args[i] = repository;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(MAPPER, 10000);
            else if (ParserScopeResolver.class.equals(type)) args[i] = new ParserScopeResolver(mock(AnalysisService.class));
            else if (JoinComplexityConfig.class.equals(type)) args[i] = new JoinComplexityConfig();
            else if (EntityTablesReader.class.equals(type)) args[i] = tablesReader;
            else if (com.analyzer.repo.EntityIngestConfig.EntityIngestConnection.class.equals(type)) args[i] = connection;
            else if (type == boolean.class) args[i] = entityReadDefault;
            else if (type.isPrimitive()) args[i] = 0;
            else args[i] = mock(type);
        }
        return (ParserAnalysisController) ctor.newInstance(args);
    }

    private static ArrayNode oneRow(String name) throws Exception {
        return (ArrayNode) MAPPER.readTree("[{\"name\":\"" + name + "\",\"schema\":\"S\","
                + "\"objectType\":\"TABLE\",\"allOperations\":[],\"usedBy\":[]}]");
    }

    /** With the default on and no explicit source param, entity data must be served. */
    @Test
    void defaultOnServesEntityWithNoSourceParam() throws Exception {
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(
                MAPPER.readTree("{\"tables\":" + oneRow("FROM_ARTIFACT") + "}"));
        var connection = mock(com.analyzer.repo.EntityIngestConfig.EntityIngestConnection.class);
        when(connection.enabled()).thenReturn(true);
        when(connection.database()).thenReturn(mock(MongoDatabase.class));
        EntityTablesReader tablesReader = mock(EntityTablesReader.class);
        when(tablesReader.readTables(any(), eq("A1"))).thenReturn(
                MAPPER.readTree("{\"tables\":" + oneRow("FROM_ENTITY") + "}"));

        ParserAnalysisController c = build(repo, connection, tablesReader, true);
        var response = c.getTablesPage("A1", 50, null, null, null, null, null, null, null,
                null, null, null, null);

        JsonNode items = response.getBody().get("items");
        assertEquals("FROM_ENTITY", items.get(0).get("name").asText(),
                "with the default on and no source param, entity data must be served");
    }

    /** An explicit source=artifact must override the default even when it's on. */
    @Test
    void explicitArtifactOverridesTheDefaultWhenOn() throws Exception {
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(
                MAPPER.readTree("{\"tables\":" + oneRow("FROM_ARTIFACT") + "}"));
        var connection = mock(com.analyzer.repo.EntityIngestConfig.EntityIngestConnection.class);
        when(connection.enabled()).thenReturn(true);
        when(connection.database()).thenReturn(mock(MongoDatabase.class));
        EntityTablesReader tablesReader = mock(EntityTablesReader.class);

        ParserAnalysisController c = build(repo, connection, tablesReader, true);
        var response = c.getTablesPage("A1", 50, null, null, null, null, null, null, null,
                null, null, "artifact", null);

        JsonNode items = response.getBody().get("items");
        assertEquals("FROM_ARTIFACT", items.get(0).get("name").asText(),
                "source=artifact must override the default, even with entity-read enabled");
        verify(tablesReader, never()).readTables(any(), any());
    }

    /** With the default off and no explicit source param, artifact-scan stays the default. */
    @Test
    void defaultOffKeepsArtifactScanAsDefault() throws Exception {
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(
                MAPPER.readTree("{\"tables\":" + oneRow("FROM_ARTIFACT") + "}"));
        var connection = mock(com.analyzer.repo.EntityIngestConfig.EntityIngestConnection.class);
        when(connection.enabled()).thenReturn(true);
        EntityTablesReader tablesReader = mock(EntityTablesReader.class);

        ParserAnalysisController c = build(repo, connection, tablesReader, false);
        var response = c.getTablesPage("A1", 50, null, null, null, null, null, null, null,
                null, null, null, null);

        JsonNode items = response.getBody().get("items");
        assertEquals("FROM_ARTIFACT", items.get(0).get("name").asText(),
                "with the default off and no source param, artifact-scan must stay the default");
        verify(tablesReader, never()).readTables(any(), any());
    }
}
