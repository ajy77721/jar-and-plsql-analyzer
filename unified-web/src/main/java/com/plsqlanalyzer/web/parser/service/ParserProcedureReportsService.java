package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.controller.detail.ProcedureCrossReportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * UI-030-BUG's fix, wired: assembles the same {@code procReports}/`txnReport`/`schemaSliceReport`/
 * `extReport`/`batchReport` payload UI-030c's live-crawl endpoint serves, but for the parser
 * backend, using {@link ParserProcedureReportBuilder} (built against this backend's real
 * `subtreeTables` shape, not a port of the broken `_analyzeProcedure` logic) and reusing
 * {@link ProcedureCrossReportBuilder} unmodified, since both builders emit the same report shape.
 */
@Service
public class ParserProcedureReportsService {

    private static final Logger log = LoggerFactory.getLogger(ParserProcedureReportsService.class);

    private final AnalysisService service;
    private final ObjectMapper mapper;

    public ParserProcedureReportsService(AnalysisService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    public ResponseEntity<Object> getProcedureReports(String name) {
        JsonNode index;
        try {
            index = service.getIndex(name);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        if (index == null || !index.has("nodes")) {
            return ResponseEntity.notFound().build();
        }
        String entrySchema = index.path("entrySchema").asText("");

        ArrayNode procReports = mapper.createArrayNode();
        for (JsonNode nodeRef : index.get("nodes")) {
            String nodeId = nodeRef.path("nodeId").asText(null);
            if (nodeId == null || nodeId.isBlank()) continue;
            try {
                JsonNode detail = service.getNodeDetail(name, nodeId + ".json");
                if (detail == null) continue;
                ObjectNode report = ParserProcedureReportBuilder.build(mapper, detail, entrySchema);
                procReports.add(report);
            } catch (Exception e) {
                log.warn("Skipping parser report for node '{}': {}", nodeId, e.getMessage());
            }
        }

        ObjectNode all = mapper.createObjectNode();
        all.set("procReports", procReports);
        all.set("txnReport", ProcedureCrossReportBuilder.buildTransactionReport(mapper, procReports));
        all.set("schemaSliceReport", ProcedureCrossReportBuilder.buildSchemaSliceReport(mapper, procReports));
        all.set("extReport", ProcedureCrossReportBuilder.buildExternalDepsReport(mapper, procReports));
        all.set("batchReport", ProcedureCrossReportBuilder.buildBatchReport(mapper, procReports));
        return ResponseEntity.ok(all);
    }
}
