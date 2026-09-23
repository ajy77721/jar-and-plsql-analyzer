package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * UI-029 — {@code /procedures} gains real {@code scope}/{@code procId}, matching
 * tables/joins/cursors/sequences. Before this, {@code ParserFacetSpecs.PROCEDURES}'s
 * {@code procedureIds} was {@code row -> List.of()} — a stub that made every scope predicate
 * reject every row, because a row can never belong to an id in an empty list.
 */
class ProceduresScopeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode node(String id, String name) {
        return mapper.createObjectNode()
                .put("nodeId", id).put("name", name).put("schema", "S").put("objectType", "PROCEDURE");
    }

    /** A root calling one child — enough to distinguish direct/subtree/subflow. */
    private JsonNode tree(String rootId, String childId) {
        var root = mapper.createObjectNode().put("id", rootId);
        var children = root.putArray("children");
        children.add(mapper.createObjectNode().put("id", childId));
        return root;
    }

    @Test
    void procedureRowsScopeToThemselves() {
        // The regression this row exists to fix: with the old row -> List.of() stub, this would
        // return an empty list and the row could never match any scope predicate.
        JsonNode row = node("S$PROC_A", "PROC_A");
        assertEquals(List.of("S$PROC_A"), ParserFacetSpecs.PROCEDURES.procedureIds().apply(row));
    }

    @Test
    void directScopeKeepsOnlyTheSelectedProcedure() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        when(service.getCallTree(anyString(), anyString())).thenReturn(tree("S$ROOT", "S$CHILD"));
        ParserScopeResolver resolver = new ParserScopeResolver(service);

        Predicate<JsonNode> pred = resolver.predicate("any", "direct", "S$ROOT",
                ParserFacetSpecs.PROCEDURES.procedureIds());

        List<String> kept = new ArrayList<>();
        for (JsonNode row : List.of(node("S$ROOT", "ROOT"), node("S$CHILD", "CHILD"),
                node("S$OTHER", "OTHER"))) {
            if (pred.test(row)) kept.add(row.path("nodeId").asText());
        }
        assertEquals(List.of("S$ROOT"), kept, "direct scope must keep only the selected node");
    }

    @Test
    void subtreeScopeExcludesTheRootItself() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        when(service.getCallTree(anyString(), anyString())).thenReturn(tree("S$ROOT", "S$CHILD"));
        ParserScopeResolver resolver = new ParserScopeResolver(service);

        Predicate<JsonNode> pred = resolver.predicate("any", "subtree", "S$ROOT",
                ParserFacetSpecs.PROCEDURES.procedureIds());

        List<String> kept = new ArrayList<>();
        for (JsonNode row : List.of(node("S$ROOT", "ROOT"), node("S$CHILD", "CHILD"),
                node("S$OTHER", "OTHER"))) {
            if (pred.test(row)) kept.add(row.path("nodeId").asText());
        }
        assertEquals(List.of("S$CHILD"), kept, "subtree scope must exclude the root and keep only descendants");
    }

    @Test
    void fullScopeMeansNoFilterAtAll() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        ParserScopeResolver resolver = new ParserScopeResolver(service);
        Predicate<JsonNode> pred = resolver.predicate("any", "full", "S$ROOT",
                ParserFacetSpecs.PROCEDURES.procedureIds());
        assertEquals(null, pred, "full scope must skip the predicate entirely, not filter to nothing");
    }
}
