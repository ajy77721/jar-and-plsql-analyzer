package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.web.service.ClaudeVerificationService.TableVerificationResult;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * Generates the improvement/run report markdown file for a verification run.
 * Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeRunReporter {

    private static final Logger log = LoggerFactory.getLogger(ClaudeRunReporter.class);

    private final ArtifactStore store;

    ClaudeRunReporter(ArtifactStore store) {
        this.store = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
    }

    void generateRunReport(VerificationResult vr, int totalChunks, List<ChunkResult> chunkResults) {
        try {
            ArtifactKey improvDir = ArtifactKey.of(AnalysisPaths.CLAUDE_ROOT, "improvement");

            String timestamp = LocalDateTime.now().toString().replace(":", "-").substring(0, 19);
            String safeName = (vr.analysisName != null ? vr.analysisName : "unknown")
                    .replaceAll("[^a-zA-Z0-9_-]", "_");
            ArtifactKey reportFile = improvDir.resolve("run_" + safeName + "_" + timestamp + ".md");

            int errorChunks = (int) chunkResults.stream().filter(c -> c.error != null).count();
            int tablesVerified = 0, tablesUnverified = 0, tablesModified = 0;
            for (var t : (vr.tables != null ? vr.tables : Collections.<TableVerificationResult>emptyList())) {
                if ("UNVERIFIED".equals(t.overallStatus)) tablesUnverified++;
                else if ("MODIFIED".equals(t.overallStatus) || "PARTIAL".equals(t.overallStatus)) tablesModified++;
                else tablesVerified++;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("# Claude Verification Run Report\n\n");
            sb.append("- **Analysis:** ").append(vr.analysisName).append("\n");
            sb.append("- **Timestamp:** ").append(vr.timestamp).append("\n");
            sb.append("- **Total Chunks:** ").append(totalChunks).append("\n");
            sb.append("- **Error Chunks:** ").append(errorChunks).append("\n\n");

            sb.append("## Results Summary\n");
            sb.append("| Metric | Count |\n|--------|-------|\n");
            sb.append("| Confirmed operations | ").append(vr.confirmedCount).append(" |\n");
            sb.append("| New findings (missed by static) | ").append(vr.newCount).append(" |\n");
            sb.append("| Removed (false positives) | ").append(vr.removedCount).append(" |\n");
            sb.append("| Tables verified | ").append(tablesVerified).append(" |\n");
            sb.append("| Tables modified | ").append(tablesModified).append(" |\n");
            sb.append("| Tables unverified | ").append(tablesUnverified).append(" |\n\n");

            // List NEW findings (what static missed)
            if (vr.newCount > 0) {
                sb.append("## New Findings (Static Analysis Gaps)\n");
                sb.append("| Table | Operation | Procedure | Line |\n|-------|-----------|-----------|------|\n");
                for (var t : (vr.tables != null ? vr.tables : Collections.<TableVerificationResult>emptyList())) {
                    for (var v : t.claudeVerifications) {
                        if ("NEW".equalsIgnoreCase(v.status)) {
                            sb.append("| ").append(t.tableName != null ? t.tableName : "?");
                            sb.append(" | ").append(v.operation != null ? v.operation : "?");
                            sb.append(" | ").append(v.procedureName != null ? v.procedureName : "?");
                            sb.append(" | ").append(v.lineNumber > 0 ? String.valueOf(v.lineNumber) : "-");
                            sb.append(" |\n");
                        }
                    }
                }
                sb.append("\n");
            }

            // List REMOVED findings (false positives in static)
            if (vr.removedCount > 0) {
                sb.append("## Removed Findings (Static False Positives)\n");
                sb.append("| Table | Operation | Procedure | Reason |\n|-------|-----------|-----------|--------|\n");
                for (var t : (vr.tables != null ? vr.tables : Collections.<TableVerificationResult>emptyList())) {
                    for (var v : t.claudeVerifications) {
                        if ("REMOVED".equalsIgnoreCase(v.status)) {
                            sb.append("| ").append(t.tableName != null ? t.tableName : "?");
                            sb.append(" | ").append(v.operation != null ? v.operation : "?");
                            sb.append(" | ").append(v.procedureName != null ? v.procedureName : "?");
                            sb.append(" | ").append(v.reason != null ? v.reason : "-");
                            sb.append(" |\n");
                        }
                    }
                }
                sb.append("\n");
            }

            byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
            try (OutputStream out = store.write(reportFile, body.length)) {
                out.write(body);
            }
            log.info("Generated run report: {}", reportFile);
        } catch (Exception e) {
            log.warn("Failed to generate run report: {}", e.getMessage());
        }
    }
}
