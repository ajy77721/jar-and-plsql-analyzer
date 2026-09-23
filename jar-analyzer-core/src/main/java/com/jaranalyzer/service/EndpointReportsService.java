package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * UI-026f — the new endpoint the client converts to (UI-026g), replacing {@code jar/js/summary.js}'s
 * {@code _render}/{@code processBatch} network-call-per-procedure loop and its
 * {@code requestAnimationFrame} progress bar with one response.
 *
 * <p>Streams the summary document exactly the way {@link SummaryStats#compute} already does
 * (same field skips, same reasoning: the document is up to 912 MB in a live run and is read once,
 * token by token, never materialised whole) and builds one {@link EndpointReportBuilder} report per
 * endpoint, threading a single {@code jarModuleMap} across the whole pass — {@link
 * EndpointReportBuilder}'s own ordering contract (§59): the same map instance, in document order,
 * standing in for the browser's single {@code JA.summary._jarModuleMap}.
 *
 * <p>The per-endpoint reports are necessarily held in memory afterward — {@link
 * EndpointCrossReportBuilder}'s five aggregations need the whole array, the same requirement
 * {@code summary.js}'s own {@code this._epReports} already has client-side today. Moving the
 * <em>parse</em> to a stream does not change that; it removes the browser's per-procedure network
 * round trips and the raw analysis payload that fed them, which is the actual cost this row exists
 * to cut.
 */
public final class EndpointReportsService {

    private EndpointReportsService() {
    }

    /**
     * @param parser positioned before the summary document's opening brace
     */
    public static ObjectNode buildAll(JsonParser parser, ObjectMapper mapper, SummaryStats.Config cfg,
                                      EndpointCrossReportBuilder.ComplexityRules complexityRules,
                                      Map<String, Object> sizeThresholds, List<Object> perfThresholds)
            throws IOException {
        List<JsonNode> epReports = new ArrayList<>();
        Map<String, String> jarModuleMap = new LinkedHashMap<>();
        String jarName = null;

        parser.nextToken(); // START_OBJECT
        while (parser.nextToken() != null) {
            JsonToken token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String field = parser.currentName();

            if (InvocationPool.POOL_FIELD.equals(field)) {
                // Same reasoning as SummaryStats.compute: skipped as a unit, or the loop's own
                // END_OBJECT check would end the pass at the pool's first closing brace.
                parser.nextToken();
                parser.skipChildren();
                continue;
            }
            if ("jarName".equals(field)) {
                parser.nextToken();
                jarName = parser.getValueAsString("");
                continue;
            }
            if (!"endpoints".equals(field)) {
                parser.nextToken();
                parser.skipChildren();
                continue;
            }

            parser.nextToken(); // START_ARRAY
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                JsonNode ep = mapper.readTree(parser);
                if (ep.has("callTree") && !ep.get("callTree").isNull()) {
                    // The branch EndpointReportBuilder does not cover (§57/§58/§67 — confirmed
                    // unreachable from this document in practice, since the summary writer strips
                    // callTree). Skipped defensively rather than silently mis-scoring it as if it
                    // had no callTree, in case a future writer regresses that guarantee.
                    continue;
                }
                epReports.add(EndpointReportBuilder.build(mapper, ep, cfg, sizeThresholds,
                        perfThresholds, jarModuleMap, jarName));
            }
        }

        ArrayNode vertReport = EndpointCrossReportBuilder.buildVerticalisation(mapper, epReports);
        ArrayNode viewsReport = EndpointCrossReportBuilder.buildViewsAnalysis(mapper, vertReport, complexityRules);
        ArrayNode distReport = EndpointCrossReportBuilder.buildDistributedTransactions(mapper, epReports);
        ArrayNode batchReport = EndpointCrossReportBuilder.buildBatchAnalysis(mapper, epReports, cfg.batchKeywords());
        ObjectNode extReport = EndpointCrossReportBuilder.buildExternalDeps(mapper, epReports, cfg, jarModuleMap, jarName);

        ObjectNode out = mapper.createObjectNode();
        ArrayNode epReportsOut = out.putArray("epReports");
        for (JsonNode r : epReports) epReportsOut.add(r);
        out.set("vertReport", vertReport);
        out.set("viewsReport", viewsReport);
        out.set("distReport", distReport);
        out.set("batchReport", batchReport);
        out.set("extReport", extReport);
        return out;
    }
}
