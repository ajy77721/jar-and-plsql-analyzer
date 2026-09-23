package com.jaranalyzer.report;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The collection report, built by streaming rather than by loading the analysis.
 *
 * <h2>The ratio that decides this</h2>
 *
 * <p>The endpoint called {@code persistenceService.load(id)} — which reads all of
 * {@code analysis.json}, measured at <b>49.4 MB</b> — to produce an aggregate measured at
 * <b>3.8 KB</b>: 14 collections over 155 (endpoint, collection) pairs. That is roughly
 * <b>13,000×</b> more input than output, held in heap, per request.
 *
 * <p>So paging the response was never the interesting half. A 14-row answer does not need paging;
 * it needs to stop reading 49 MB to compute it. This walks the endpoint array as a token stream and
 * keeps only the three fields the report actually reads from each endpoint.
 *
 * <h2>What is deliberately not reproduced</h2>
 *
 * <p>The original called {@code ep.computeAggregates()} when {@code aggregatedCollections} was
 * empty, as a fallback for data written before aggregation existed. That cannot be done from a
 * stream — it needs the whole endpoint object graph — and doing it would defeat the purpose. An
 * endpoint with no aggregated collections now contributes nothing, which is the same answer for
 * every analysis produced since aggregation was added. Older analyses would under-report, and that
 * is stated here rather than discovered later.
 */
public final class CollectionReportAggregator {

    /** One collection and the endpoints that touch it. */
    public record CollectionRow(String collection, String domain, List<String> operations,
                                List<Map<String, String>> endpoints, int endpointCount) {}

    private CollectionReportAggregator() {
    }

    /**
     * Streams {@code analysis.json} and returns the report, sorted by endpoint count descending.
     *
     * <p>Only the {@code endpoints} array is read; {@code classes} and {@code callTreeNodes} — which
     * are the bulk of the file — are skipped without being parsed into anything.
     */
    public static List<CollectionRow> aggregate(JsonParser parser, ObjectMapper mapper)
            throws IOException {
        Map<String, Builder> byCollection = new LinkedHashMap<>();

        if (parser.nextToken() != JsonToken.START_OBJECT) return List.of();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String field = parser.currentName();
            parser.nextToken();
            if (!"endpoints".equals(field)) {
                // The 49 MB this class exists to avoid reading.
                parser.skipChildren();
                continue;
            }
            if (parser.currentToken() != JsonToken.START_ARRAY) {
                parser.skipChildren();
                continue;
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                readEndpoint(mapper.readTree(parser), byCollection);
            }
        }

        List<CollectionRow> rows = new ArrayList<>(byCollection.size());
        for (Builder b : byCollection.values()) rows.add(b.build());
        rows.sort(Comparator.comparingInt(CollectionRow::endpointCount).reversed());
        return rows;
    }

    private static void readEndpoint(JsonNode ep, Map<String, Builder> byCollection) {
        JsonNode collections = ep.path("aggregatedCollections");
        if (!collections.isArray() || collections.isEmpty()) return;

        String label = ep.path("controllerSimpleName").asText("") + "."
                + ep.path("methodName").asText("");
        String httpMethod = ep.path("httpMethod").asText("");
        String path = (httpMethod.isEmpty() ? "" : httpMethod + " ") + ep.path("fullPath").asText("");

        JsonNode domains = ep.path("aggregatedCollectionDomains");
        JsonNode operations = ep.path("aggregatedOperations");

        for (JsonNode c : collections) {
            String name = c.asText(null);
            if (name == null || name.isEmpty()) continue;
            Builder b = byCollection.computeIfAbsent(name, Builder::new);
            // path() rather than get(): a collection absent from either side map is still a real
            // access, and dropping it because the domain is unknown would lose the row.
            b.domain(domains.path(name).asText(null));
            String op = operations.path(name).asText(null);
            b.endpoint(label, path, op == null ? "" : op);
            if (op != null && !op.isEmpty()) b.operation(op);
        }
    }

    private static final class Builder {
        private final String collection;
        private String domain;
        private final Set<String> operations = new LinkedHashSet<>();
        private final List<Map<String, String>> endpoints = new ArrayList<>();

        Builder(String collection) {
            this.collection = collection;
        }

        /** First non-null wins, matching the original's computeIfAbsent semantics. */
        void domain(String d) {
            if (domain == null && d != null && !d.isEmpty()) domain = d;
        }

        void operation(String op) {
            operations.add(op);
        }

        void endpoint(String label, String path, String operation) {
            endpoints.add(Map.of("label", label, "path", path, "operation", operation));
        }

        CollectionRow build() {
            return new CollectionRow(collection, domain == null ? "" : domain,
                    new ArrayList<>(operations), endpoints, endpoints.size());
        }
    }
}
