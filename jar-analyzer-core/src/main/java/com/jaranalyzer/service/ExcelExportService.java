package com.jaranalyzer.service;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Generates a styled .xlsx workbook from frontend report data using Apache POI.
 * Each public buildXxx method creates one sheet.
 */
@Service
public class ExcelExportService {

    private final ExcelStyleHelper style;

    public ExcelExportService(ExcelStyleHelper style) {
        this.style = style;
    }

    @SuppressWarnings("unchecked")
    public byte[] generateReport(Map<String, Object> data) throws IOException {
        try (XSSFWorkbook wb = new XSSFWorkbook()) {
            CellStyle hdr = style.headerStyle(wb);
            CellStyle ttl = style.titleStyle(wb);
            CellStyle sec = style.sectionStyle(wb);
            CellStyle norm = style.dataStyle(wb);
            CellStyle alt = style.altRowStyle(wb);
            CellStyle green = style.greenFill(wb);
            CellStyle orange = style.orangeFill(wb);
            CellStyle red = style.redFill(wb);
            CellStyle yellow = style.yellowFill(wb);
            CellStyle purple = style.purpleFill(wb);

            Styles s = new Styles(hdr, ttl, sec, norm, alt, green, orange, red, yellow, purple);

            List<Map<String, Object>> epReports = asList(data.get("epReports"));
            List<Map<String, Object>> vertReport = asList(data.get("vertReport"));
            List<Map<String, Object>> distReport = asList(data.get("distReport"));
            List<Map<String, Object>> batchReport = asList(data.get("batchReport"));
            List<Map<String, Object>> viewsReport = asList(data.get("viewsReport"));
            Map<String, Object> extReport = asMap(data.get("extReport"));
            Map<String, Object> vertVerReport = asMap(data.get("vertVerReport"));
            String jarName = str(data.get("jarName"));

            buildSummarySheet(wb, s, epReports, vertReport, distReport, batchReport, viewsReport, extReport, jarName);
            if (!epReports.isEmpty()) buildEndpointsSheet(wb, s, epReports);
            if (!epReports.isEmpty()) buildEndpointCollsSheet(wb, s, epReports);
            if (!vertReport.isEmpty()) buildCollectionsSheet(wb, s, vertReport);
            if (!vertReport.isEmpty()) buildCollSummarySheet(wb, s, vertReport, epReports);
            if (!epReports.isEmpty()) buildCollUsageDetailSheet(wb, s, epReports);
            if (!distReport.isEmpty()) buildTransactionsSheet(wb, s, distReport);
            if (!batchReport.isEmpty()) buildBatchSheet(wb, s, batchReport);
            if (!viewsReport.isEmpty()) buildViewsSheet(wb, s, viewsReport);
            if (extReport != null) buildExternalSheet(wb, s, extReport);
            if (!epReports.isEmpty()) buildExtCallsDetailSheet(wb, s, epReports);
            if (vertVerReport != null) {
                buildVertMethodCallsSheet(wb, s, vertVerReport);
                buildVertCrossDomainSheet(wb, s, vertVerReport);
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            wb.write(baos);
            return baos.toByteArray();
        }
    }

    record Styles(CellStyle hdr, CellStyle ttl, CellStyle sec, CellStyle norm, CellStyle alt,
                  CellStyle green, CellStyle orange, CellStyle red, CellStyle yellow, CellStyle purple) {}

    // ========== Sheet: Summary ==========

    private void buildSummarySheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> ep,
                                   List<Map<String, Object>> vert, List<Map<String, Object>> dist,
                                   List<Map<String, Object>> batch, List<Map<String, Object>> views,
                                   Map<String, Object> ext, String jarName) {
        Sheet sh = wb.createSheet("Summary");
        int row = 0;

        // Title
        style.writeHeaderRow(sh, s.ttl, row++, "JAR Analyzer — Analysis Report", "", "", "", "");
        style.writeHeaderRow(sh, s.hdr, row++, "JAR", jarName, "", "Generated", new Date().toString());
        row++;
        style.writeHeaderRow(sh, s.sec, row++, "KEY METRICS", "", "", "", "");
        style.writeHeaderRow(sh, s.hdr, row++, "Metric", "Count", "Details", "", "");

        Set<String> domains = new LinkedHashSet<>();
        vert.forEach(c -> { String d = str(c.get("domain")); if (!d.isEmpty() && !"Other".equals(d)) domains.add(d); });
        long txnReq = dist.stream().filter(d -> str(d.get("transactionRequirement")).startsWith("REQUIRED")).count();
        long crossEps = ep.stream().filter(r -> num(r.get("externalScopeCalls")) > 0).count();
        long collCount = vert.stream().filter(c -> !"VIEW".equals(str(c.get("type")))).count();
        List<Map<String, Object>> crossModules = ext != null ? asList(ext.get("crossModule")) : List.of();

        style.writeDataRow(sh, s.norm, s.alt, row++, "Total Endpoints", ep.size(), "REST + Batch endpoints analyzed", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "Total Collections", collCount, "MongoDB data collections", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "Total Views", views.size(), "MongoDB views detected", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "Domains", domains.size(), String.join(", ", domains), "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "Cross-Module Endpoints", crossEps, "Endpoints calling external modules", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "Transactions Required", txnReq, "Multi-domain write endpoints", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "Batch Jobs", batch.size(), "Scheduled/batch endpoint flows", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "External JAR Dependencies", crossModules.size(), "Internal dependency JARs called", "", "");

        row++;
        style.writeHeaderRow(sh, s.sec, row++, "SIZE DISTRIBUTION", "", "", "", "");
        style.writeHeaderRow(sh, s.hdr, row++, "Category", "Count", "Threshold", "", "");
        Map<String, Integer> sizes = new LinkedHashMap<>();
        sizes.put("S", 0); sizes.put("M", 0); sizes.put("L", 0); sizes.put("XL", 0);
        ep.forEach(r -> { String sc = str(r.get("sizeCategory")); sizes.merge(sc, 1, Integer::sum); });
        style.writeDataRow(sh, s.norm, s.alt, row++, "S (Small)", sizes.getOrDefault("S", 0), "<= 5 methods", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "M (Medium)", sizes.getOrDefault("M", 0), "6-20 methods", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "L (Large)", sizes.getOrDefault("L", 0), "21-50 methods", "", "");
        style.writeDataRow(sh, s.norm, s.alt, row++, "XL (Extra Large)", sizes.getOrDefault("XL", 0), "> 50 methods", "", "");

        style.setColumnWidths(sh, 28, 14, 44, 14, 24);
    }

    // ========== Sheet: Endpoints ==========

    private void buildEndpointsSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> ep) {
        Sheet sh = wb.createSheet("Endpoints");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "HTTP", "Path", "Endpoint Name", "Type", "ProcName",
                "Domain", "Collections", "Views", "DB Ops", "Methods", "LOC",
                "Internal", "External", "Operations", "Write Colls", "Read Colls",
                "Size", "Performance");
        for (int i = 0; i < ep.size(); i++) {
            Map<String, Object> r = ep.get(i);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(r.get("httpMethod")), str(r.get("fullPath")), str(r.get("endpointName")),
                    str(r.get("typeOfEndpoint")), str(r.get("procName")),
                    str(r.get("primaryDomain")), num(r.get("totalCollections")),
                    num(r.get("totalViews")), num(r.get("totalDbOperations")), num(r.get("totalMethods")),
                    num(r.get("totalLoc")),
                    num(r.get("inScopeCalls")), num(r.get("externalScopeCalls")),
                    joinList(r.get("operationTypes")),
                    joinList(r.get("writeCollections")), joinList(r.get("readCollections")),
                    str(r.get("sizeCategory")), str(r.get("performanceImplication")));

            // Conditional: size XL=red, L=orange
            Row row = sh.getRow(i + 1);
            String size = str(r.get("sizeCategory"));
            if ("XL".equals(size)) style.applyFill(row, 17, s.red);
            else if ("L".equals(size)) style.applyFill(row, 17, s.orange);
            if (num(r.get("externalScopeCalls")) > 0) style.applyFill(row, 13, s.orange);
            int loc = num(r.get("totalLoc"));
            if (loc > 500) style.applyFill(row, 11, s.red);
            else if (loc > 200) style.applyFill(row, 11, s.orange);
        }
        style.setColumnWidths(sh, 5, 8, 34, 28, 12, 18, 16, 8, 7, 8, 8, 8, 8, 8, 22, 26, 26, 6, 16);
        style.freezeHeader(sh);
        style.autoFilter(sh, 19);
    }

    // ========== Sheet: Endpoint-Collections ==========

    private void buildEndpointCollsSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> ep) {
        Sheet sh = wb.createSheet("Endpoint-Collections");
        style.writeHeaderRow(sh, s.hdr, 0,
                "Endpoint", "HTTP", "Path", "ProcName", "Collection", "Domain",
                "Detected Via", "Operation", "Type", "References");
        int row = 1;
        for (Map<String, Object> r : ep) {
            Map<String, Object> colls = asMap(r.get("collections"));
            if (colls == null) continue;
            for (Map.Entry<String, Object> entry : colls.entrySet()) {
                Map<String, Object> info = asMap(entry.getValue());
                if (info == null) continue;
                List<String> ops = asList(info.get("operations")).stream().map(o -> str(o)).toList();
                String detectedVia = joinList(info.get("detectedVia"));
                String refs = joinList(info.get("sources"));
                for (String op : (ops.isEmpty() ? List.of("") : ops)) {
                    style.writeDataRow(sh, s.norm, s.alt, row,
                            str(r.get("endpointName")), str(r.get("httpMethod")), str(r.get("fullPath")),
                            str(r.get("procName")), entry.getKey(), str(info.get("domain")),
                            detectedVia, op, str(info.get("type")), refs);
                    Row dataRow = sh.getRow(row);
                    String opUpper = op.toUpperCase();
                    if (opUpper.contains("WRITE") || opUpper.contains("UPDATE") || opUpper.contains("DELETE"))
                        style.applyFill(dataRow, 7, s.orange);
                    else if (opUpper.contains("AGGREGATE"))
                        style.applyFill(dataRow, 7, s.purple);
                    if ("VIEW".equals(str(info.get("type")))) style.applyFill(dataRow, 8, s.yellow);
                    row++;
                }
            }
        }
        style.setColumnWidths(sh, 28, 8, 32, 18, 24, 16, 20, 14, 12, 38);
        style.freezeHeader(sh);
        style.autoFilter(sh, 10);
    }

    // ========== Sheet: Collections ==========

    private void buildCollectionsSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> vert) {
        Sheet sh = wb.createSheet("Collections");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Collection", "Type", "Domain", "Status",
                "Detected Via", "Read Ops", "Write Ops", "All Operations",
                "Find", "Agg", "Save", "Update", "Delete",
                "Usage Count", "Endpoint Count", "Complexity", "Score",
                "Endpoints", "ProcNames", "References");
        for (int i = 0; i < vert.size(); i++) {
            Map<String, Object> c = vert.get(i);
            String verif = str(c.get("verification"));
            String verifLabel = "VERIFIED".equals(verif) || "CLAUDE_VERIFIED".equals(verif) ? "Verified"
                    : "NOT_IN_DB".equals(verif) ? "Ambiguous" : "Unknown";
            Map<String, Object> opCounts = asMap(c.get("opCounts"));
            int findCnt = num(opCounts != null ? opCounts.get("READ") : null) + num(opCounts != null ? opCounts.get("COUNT") : null);
            int aggCnt = num(opCounts != null ? opCounts.get("AGGREGATE") : null);
            int saveCnt = num(opCounts != null ? opCounts.get("WRITE") : null);
            int updateCnt = num(opCounts != null ? opCounts.get("UPDATE") : null);
            int deleteCnt = num(opCounts != null ? opCounts.get("DELETE") : null);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(c.get("name")), str(c.get("type")), str(c.get("domain")),
                    verifLabel,
                    joinList(c.get("detectedVia")),
                    joinList(c.get("readOps")), joinList(c.get("writeOps")),
                    joinList(c.get("operations")),
                    findCnt, aggCnt, saveCnt, updateCnt, deleteCnt,
                    num(c.get("usageCount")), num(c.get("endpointCount")),
                    str(c.get("_complexity")), num(c.get("_complexityScore")),
                    joinList(c.get("endpoints")), joinList(c.get("procNames")),
                    joinList(c.get("sources")));
            if ("VIEW".equals(str(c.get("type")))) style.applyFill(sh.getRow(i + 1), 2, s.yellow);
            if ("Verified".equals(verifLabel)) style.applyFill(sh.getRow(i + 1), 4, s.green);
            else if ("Ambiguous".equals(verifLabel)) style.applyFill(sh.getRow(i + 1), 4, s.orange);
        }
        style.setColumnWidths(sh, 5, 28, 12, 16, 12, 20, 20, 20, 22, 6, 6, 6, 7, 7, 10, 10, 12, 8, 38, 26, 38);
        style.freezeHeader(sh);
        style.autoFilter(sh, 21);
    }

    // ========== Sheet: Collection Summary (from vertReport opCounts) ==========

    private void buildCollSummarySheet(XSSFWorkbook wb, Styles s,
                                       List<Map<String, Object>> vert, List<Map<String, Object>> ep) {
        Sheet sh = wb.createSheet("Coll Summary");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Collection", "Type", "Domain", "Status", "Detected Via",
                "Find", "Agg", "Save", "Update", "Delete", "Total Ops",
                "Endpoints", "Usage", "Complexity", "Score");

        int row = 1;
        for (int i = 0; i < vert.size(); i++) {
            Map<String, Object> c = vert.get(i);
            Map<String, Object> opCounts = asMap(c.get("opCounts"));
            int findCnt = num(opCounts != null ? opCounts.get("READ") : null) + num(opCounts != null ? opCounts.get("COUNT") : null);
            int aggCnt = num(opCounts != null ? opCounts.get("AGGREGATE") : null);
            int saveCnt = num(opCounts != null ? opCounts.get("WRITE") : null);
            int updateCnt = num(opCounts != null ? opCounts.get("UPDATE") : null);
            int deleteCnt = num(opCounts != null ? opCounts.get("DELETE") : null);
            int totalOps = findCnt + aggCnt + saveCnt + updateCnt + deleteCnt;
            // Endpoint count: from endpoints array/list length (Sets serialized as arrays)
            int epCount = c.get("endpoints") instanceof List<?> epList ? epList.size() : 0;
            String verif = str(c.get("verification"));
            String verifLabel = "VERIFIED".equals(verif) || "CLAUDE_VERIFIED".equals(verif) ? "Verified"
                    : "NOT_IN_DB".equals(verif) ? "Ambiguous" : "Unknown";
            style.writeDataRow(sh, s.norm, s.alt, row,
                    i + 1, str(c.get("name")), str(c.get("type")), str(c.get("domain")),
                    verifLabel, joinList(c.get("detectedVia")),
                    findCnt, aggCnt, saveCnt, updateCnt, deleteCnt, totalOps,
                    epCount, num(c.get("usageCount")),
                    str(c.get("_complexity")), num(c.get("_complexityScore")));
            if ("VIEW".equals(str(c.get("type")))) style.applyFill(sh.getRow(row), 2, s.yellow);
            if ("Verified".equals(verifLabel)) style.applyFill(sh.getRow(row), 4, s.green);
            else if ("Ambiguous".equals(verifLabel)) style.applyFill(sh.getRow(row), 4, s.orange);
            if (epCount >= 10) style.applyFill(sh.getRow(row), 12, s.green);
            row++;
        }
        style.setColumnWidths(sh, 5, 28, 12, 16, 12, 20, 6, 6, 6, 7, 7, 8, 10, 8, 12, 8);
        style.freezeHeader(sh);
        style.autoFilter(sh, 16);
    }

    // ========== Sheet: Collection Usage Detail ==========

    private void buildCollUsageDetailSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> ep) {
        Sheet sh = wb.createSheet("Coll Usage Detail");
        style.writeHeaderRow(sh, s.hdr, 0,
                "Collection", "Domain", "Type", "Endpoint", "HTTP", "Path",
                "Operation", "Reference Class", "Detected Via");

        List<Object[]> rows = new ArrayList<>();
        for (Map<String, Object> r : ep) {
            Map<String, Object> colls = asMap(r.get("collections"));
            if (colls == null) continue;
            for (Map.Entry<String, Object> entry : colls.entrySet()) {
                Map<String, Object> info = asMap(entry.getValue());
                if (info == null) continue;
                List<String> ops = asList(info.get("operations")).stream().map(o -> str(o)).toList();
                List<String> refs = asList(info.get("sources")).stream().map(o -> str(o)).toList();
                String detectedVia = joinList(info.get("detectedVia"));
                for (String op : (ops.isEmpty() ? List.of("") : ops)) {
                    String refStr = refs.isEmpty() ? "" : String.join("; ", refs);
                    rows.add(new Object[]{
                            entry.getKey(), str(info.get("domain")), str(info.get("type")),
                            str(r.get("endpointName")), str(r.get("httpMethod")), str(r.get("fullPath")),
                            op, refStr, detectedVia});
                }
            }
        }
        rows.sort(Comparator.comparing(a -> (String) a[0]));

        for (int i = 0; i < rows.size(); i++) {
            style.writeDataRow(sh, s.norm, s.alt, i + 1, rows.get(i));
            String op = ((String) rows.get(i)[6]).toUpperCase();
            Row dataRow = sh.getRow(i + 1);
            if (op.contains("WRITE") || op.contains("UPDATE") || op.contains("DELETE"))
                style.applyFill(dataRow, 6, s.orange);
            else if (op.contains("AGGREGATE"))
                style.applyFill(dataRow, 6, s.purple);
        }
        style.setColumnWidths(sh, 26, 16, 12, 28, 8, 32, 14, 34, 18);
        style.freezeHeader(sh);
        style.autoFilter(sh, 9);
    }

    // ========== Sheet: Transactions ==========

    private void buildTransactionsSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> dist) {
        Sheet sh = wb.createSheet("Transactions");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Endpoint", "ProcName", "HTTP", "Path",
                "Domain", "Transaction Req", "Collections", "LOC", "Performance");
        for (int i = 0; i < dist.size(); i++) {
            Map<String, Object> d = dist.get(i);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(d.get("endpointName")), str(d.get("procName")),
                    str(d.get("httpMethod")), str(d.get("fullPath")),
                    str(d.get("primaryDomain")), str(d.get("transactionRequirement")),
                    num(d.get("totalCollections")), num(d.get("totalLoc")),
                    str(d.get("performanceImplication")));
            String txn = str(d.get("transactionRequirement"));
            if (txn.startsWith("REQUIRED")) style.applyFill(sh.getRow(i + 1), 6, s.red);
        }
        style.setColumnWidths(sh, 5, 28, 18, 8, 32, 16, 18, 10, 8, 16);
        style.freezeHeader(sh);
        style.autoFilter(sh, 10);
    }

    // ========== Sheet: Batch Jobs ==========

    private void buildBatchSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> batch) {
        Sheet sh = wb.createSheet("Batch Jobs");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Name", "ProcName", "URL", "Domain",
                "Collections", "Methods", "LOC", "Size", "Performance");
        for (int i = 0; i < batch.size(); i++) {
            Map<String, Object> b = batch.get(i);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(b.get("batchName")), str(b.get("procName")),
                    str(b.get("endpointUrl")), str(b.get("primaryDomain")),
                    num(b.get("totalCollections")), num(b.get("totalMethods")),
                    num(b.get("totalLoc")),
                    str(b.get("sizeCategory")), str(b.get("performanceImplication")));
            String size = str(b.get("sizeCategory"));
            if ("XL".equals(size)) style.applyFill(sh.getRow(i + 1), 8, s.red);
            else if ("L".equals(size)) style.applyFill(sh.getRow(i + 1), 8, s.orange);
        }
        style.setColumnWidths(sh, 5, 30, 18, 30, 16, 10, 10, 8, 6, 16);
        style.freezeHeader(sh);
        style.autoFilter(sh, 10);
    }

    // ========== Sheet: Views ==========

    private void buildViewsSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> views) {
        Sheet sh = wb.createSheet("Views");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "View Name", "Domain", "Complexity", "Usage",
                "Endpoints", "Possible Alternative");
        for (int i = 0; i < views.size(); i++) {
            Map<String, Object> v = views.get(i);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(v.get("viewName")), str(v.get("domain")),
                    str(v.get("complexity")), num(v.get("usageCount")),
                    joinList(v.get("usedByEndpoints")), str(v.get("possibleAlternative")));
        }
        style.setColumnWidths(sh, 5, 28, 16, 12, 10, 38, 30);
        style.freezeHeader(sh);
        style.autoFilter(sh, 7);
    }

    // ========== Sheet: External Deps ==========

    private void buildExternalSheet(XSSFWorkbook wb, Styles s, Map<String, Object> ext) {
        List<Map<String, Object>> modules = asList(ext.get("crossModule"));
        if (modules.isEmpty()) return;
        Sheet sh = wb.createSheet("External Deps");
        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Module", "Domain", "Classes", "Methods", "Endpoints", "Calls");
        for (int i = 0; i < modules.size(); i++) {
            Map<String, Object> m = modules.get(i);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(m.get("project")), str(m.get("domain")),
                    num(m.get("classCount")), num(m.get("methodCount")),
                    num(m.get("endpointCount")), num(m.get("count")));
        }
        style.setColumnWidths(sh, 5, 34, 16, 10, 10, 12, 10);
        style.freezeHeader(sh);
        style.autoFilter(sh, 7);
    }

    // ========== Sheet: External Calls Detail ==========

    private void buildExtCallsDetailSheet(XSSFWorkbook wb, Styles s, List<Map<String, Object>> ep) {
        Sheet sh = wb.createSheet("External Calls Detail");
        style.writeHeaderRow(sh, s.hdr, 0,
                "Endpoint", "HTTP", "Path", "Domain",
                "Call Type", "Class", "Method", "Stereotype", "Module", "URL");
        int row = 1;
        for (Map<String, Object> r : ep) {
            // Cross-module calls
            for (Map<String, Object> ext : asList(r.get("externalCalls"))) {
                style.writeDataRow(sh, s.norm, s.alt, row,
                        str(r.get("endpointName")), str(r.get("httpMethod")), str(r.get("fullPath")),
                        str(r.get("primaryDomain")),
                        "CROSS_MODULE", str(ext.get("simpleClassName")), str(ext.get("methodName")),
                        str(ext.get("stereotype")), str(ext.get("module")), "");
                row++;
            }
            // HTTP calls
            for (Map<String, Object> h : asList(r.get("httpCalls"))) {
                style.writeDataRow(sh, s.norm, s.alt, row,
                        str(r.get("endpointName")), str(r.get("httpMethod")), str(r.get("fullPath")),
                        str(r.get("primaryDomain")),
                        "HTTP_CALL", str(h.get("simpleClassName")), str(h.get("methodName")),
                        "", "", str(h.get("url")));
                style.applyFill(sh.getRow(row), 4, s.purple);
                row++;
            }
        }
        if (row <= 1) { wb.removeSheetAt(wb.getSheetIndex(sh)); return; }
        style.setColumnWidths(sh, 28, 8, 32, 16, 14, 24, 24, 14, 20, 34);
        style.freezeHeader(sh);
        style.autoFilter(sh, 10);
    }

    // ========== Sheet: Vert - Method Calls ==========

    private void buildVertMethodCallsSheet(XSSFWorkbook wb, Styles s, Map<String, Object> report) {
        List<Map<String, Object>> beans = asList(report.get("beans"));
        if (beans.isEmpty()) return;
        Sheet sh = wb.createSheet("Vert - Method Calls");

        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Full Class", "Simple Class", "Method", "Stereotype", "Owner Module", "Owner Domain",
                "Caller Domains", "Calls", "Endpoints", "Called By (All)", "Current", "Recommended");
        for (int i = 0; i < beans.size(); i++) {
            Map<String, Object> b = beans.get(i);
            style.writeDataRow(sh, s.norm, s.alt, i + 1,
                    i + 1, str(b.get("className")), str(b.get("simpleClassName")), str(b.get("methodName")),
                    str(b.get("stereotype")), str(b.get("sourceModule")), str(b.get("sourceDomain")),
                    joinList(b.get("callerDomains")),
                    num(b.get("callCount")), num(b.get("endpointCount")),
                    joinList(b.get("calledByEndpoints")),
                    "DIRECT", "REST API");
            style.applyFill(sh.getRow(i + 1), 12, s.orange);
        }
        style.setColumnWidths(sh, 5, 44, 30, 26, 14, 22, 18, 26, 8, 10, 50, 10, 14);
        style.freezeHeader(sh);
        style.autoFilter(sh, 13);
    }

    // ========== Sheet: Vert - Cross Domain ==========

    private void buildVertCrossDomainSheet(XSSFWorkbook wb, Styles s, Map<String, Object> report) {
        List<Map<String, Object>> collections = asList(report.get("collections"));
        if (collections.isEmpty()) return;
        Sheet sh = wb.createSheet("Vert - Cross Domain");
        String pd = str(report.get("primaryDomain"));

        style.writeHeaderRow(sh, s.hdr, 0,
                "#", "Collection", "Section", "Direction", "Type", "Owner Domain",
                "Accessed By Domains", "Operations", "Endpoints", "Accessed By (All)", "Current", "Recommended");

        // Split into 4 groups: C2E colls, C2E ops, E2C colls, E2C ops
        List<Map<String, Object>> c2e = new ArrayList<>(), e2c = new ArrayList<>();
        for (Map<String, Object> c : collections) {
            String owner = str(c.get("ownerDomain"));
            if (owner.equals(pd)) e2c.add(c);
            else c2e.add(c);
        }

        int row = 1, n = 0;
        // C2E Collections
        for (Map<String, Object> c : c2e) {
            List<String> ops = asList(c.get("operations")).stream().map(o -> str(o)).toList();
            boolean hasWrite = ops.stream().anyMatch(o -> o.contains("WRITE") || o.contains("UPDATE"));
            style.writeDataRow(sh, s.norm, s.alt, row,
                    ++n, str(c.get("name")), pd + "→Ext Collections", pd + "→External",
                    str(c.get("type")), str(c.get("ownerDomain")),
                    joinList(c.get("accessedByDomains")), String.join(", ", ops),
                    num(c.get("endpointCount")), joinList(c.get("accessedByEndpoints")),
                    "DIRECT", hasWrite ? "REST API" : "EVALUATE");
            style.applyFill(sh.getRow(row), 11, hasWrite ? s.orange : s.yellow);
            row++;
        }
        // C2E Operations (flattened)
        for (Map<String, Object> c : c2e) {
            List<String> ops = asList(c.get("operations")).stream().map(o -> str(o)).toList();
            for (String op : ops) {
                boolean hasWrite = op.contains("WRITE") || op.contains("UPDATE");
                style.writeDataRow(sh, s.norm, s.alt, row,
                        ++n, str(c.get("name")), pd + "→Ext Operations", pd + "→External",
                        str(c.get("type")), str(c.get("ownerDomain")),
                        joinList(c.get("accessedByDomains")), op,
                        num(c.get("endpointCount")), joinList(c.get("accessedByEndpoints")),
                        "DIRECT", hasWrite ? "REST API" : "EVALUATE");
                style.applyFill(sh.getRow(row), 11, hasWrite ? s.orange : s.yellow);
                row++;
            }
        }
        // E2C Collections
        for (Map<String, Object> c : e2c) {
            List<String> ops = asList(c.get("operations")).stream().map(o -> str(o)).toList();
            boolean hasWrite = ops.stream().anyMatch(o -> o.contains("WRITE") || o.contains("UPDATE"));
            style.writeDataRow(sh, s.norm, s.alt, row,
                    ++n, str(c.get("name")), "Ext→" + pd + " Collections", "External→" + pd,
                    str(c.get("type")), str(c.get("ownerDomain")),
                    joinList(c.get("accessedByDomains")), String.join(", ", ops),
                    num(c.get("endpointCount")), joinList(c.get("accessedByEndpoints")),
                    "DIRECT", hasWrite ? "REST API" : "EVALUATE");
            style.applyFill(sh.getRow(row), 11, hasWrite ? s.orange : s.yellow);
            row++;
        }
        // E2C Operations (flattened)
        for (Map<String, Object> c : e2c) {
            List<String> ops = asList(c.get("operations")).stream().map(o -> str(o)).toList();
            for (String op : ops) {
                boolean hasWrite = op.contains("WRITE") || op.contains("UPDATE");
                style.writeDataRow(sh, s.norm, s.alt, row,
                        ++n, str(c.get("name")), "Ext→" + pd + " Operations", "External→" + pd,
                        str(c.get("type")), str(c.get("ownerDomain")),
                        joinList(c.get("accessedByDomains")), op,
                        num(c.get("endpointCount")), joinList(c.get("accessedByEndpoints")),
                        "DIRECT", hasWrite ? "REST API" : "EVALUATE");
                style.applyFill(sh.getRow(row), 11, hasWrite ? s.orange : s.yellow);
                row++;
            }
        }

        style.setColumnWidths(sh, 5, 30, 24, 20, 12, 18, 26, 22, 10, 50, 10, 14);
        style.freezeHeader(sh);
        style.autoFilter(sh, 12);
    }

    // ========== Utility ==========

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> asList(Object obj) {
        if (obj instanceof List<?> list) {
            List<Map<String, Object>> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) result.add((Map<String, Object>) m);
            }
            return result;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object obj) {
        if (obj instanceof Map<?, ?> m) return (Map<String, Object>) m;
        return null;
    }

    private String str(Object obj) {
        if (obj == null) return "";
        return String.valueOf(obj);
    }

    private int num(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number n) return n.intValue();
        try { return Integer.parseInt(String.valueOf(obj)); } catch (Exception e) { return 0; }
    }

    private String joinList(Object obj) {
        if (obj instanceof List<?> list) return list.stream().map(String::valueOf).reduce((a, b) -> a + ", " + b).orElse("");
        if (obj instanceof String s) return s;
        return "";
    }
}
