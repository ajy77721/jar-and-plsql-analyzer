package com.plsqlanalyzer.web.controller.detail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * UI-030c — assembles all procedure reports plus the four cross-procedure reports server-side,
 * replacing {@code plsql/js/summary.js}'s client loop of one {@code getProcDetail} network call
 * per procedure (UI-030's original finding) with a single request.
 *
 * <h2>Why this is a batching fix, not a redesign (§82's finding, confirmed in §83/§84)</h2>
 *
 * <p>This service does not reimplement {@link ProcedureDetailService}'s per-node computation — it
 * calls it once per node, exactly as the client used to over the network, just without the network
 * round trip in between. {@link ProcedureReportBuilder} then builds each node's report from that
 * same detail shape, and {@link ProcedureCrossReportBuilder} aggregates across all of them. The
 * callTree-table-walk redundancy identified in §83 means no logic is duplicated beyond what
 * {@code ProcedureDetailService} already does per node.
 */
@Service
public class ProcedureReportsService {

    private static final Logger log = LoggerFactory.getLogger(ProcedureReportsService.class);

    private final AnalysisService analysisService;
    private final ProcedureDetailService procedureDetailService;
    private final ObjectMapper mapper;

    public ProcedureReportsService(AnalysisService analysisService, ProcedureDetailService procedureDetailService,
                                   ObjectMapper mapper) {
        this.analysisService = analysisService;
        this.procedureDetailService = procedureDetailService;
        this.mapper = mapper;
    }

    public ResponseEntity<Object> getProcedureReports() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        CallGraph graph = result.getCallGraph();
        String entrySchema = result.getEntrySchema();

        ArrayNode procReports = mapper.createArrayNode();
        for (CallGraphNode node : graph.getAllNodes()) {
            String nodeId = node.getId();
            if (nodeId == null) continue;
            try {
                ResponseEntity<Map<String, Object>> detailResponse = procedureDetailService.getProcedureDetail(nodeId);
                Map<String, Object> detailMap = detailResponse.getBody();
                if (detailMap == null) continue;
                JsonNode detail = mapper.valueToTree(detailMap);
                ObjectNode report = ProcedureReportBuilder.build(mapper, detail, entrySchema);
                procReports.add(report);
            } catch (Exception e) {
                log.warn("Skipping report for node '{}': {}", nodeId, e.getMessage());
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
