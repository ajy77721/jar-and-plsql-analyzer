package com.jaranalyzer.crossref;

import com.jaranalyzer.crossref.model.*;

import java.util.List;

/**
 * Generates a human-readable Markdown report from a CrossRefResult.
 * Completely stateless — no Spring bean, instantiated directly.
 */
public class CrossRefReportGenerator {

    public String generate(CrossRefResult result) {
        StringBuilder sb = new StringBuilder();
        CrossRefSummary s = result.getSummary();

        sb.append("# Cross-Reference Report\n\n");
        sb.append("| Field | Value |\n|---|---|\n");
        sb.append("| JAR | `").append(result.getJarId()).append("` |\n");
        sb.append("| Endpoint | `").append(result.getEndpointPath()).append("` |\n");
        sb.append("| Oracle Proc | `").append(result.getProcName()).append("` |\n");
        if (result.getSchema() != null) sb.append("| Schema | `").append(result.getSchema()).append("` |\n");
        sb.append("| Mode | **").append(result.getMode()).append("** |\n");
        if (result.getPlsqlSessionId() != null)
            sb.append("| PL/SQL Session | `").append(result.getPlsqlSessionId()).append("` |\n");
        sb.append("\n");

        if (s != null) {
            sb.append("## Summary\n\n");
            sb.append("| Metric | Count |\n|---|---|\n");
            sb.append("| Total Java Nodes | ").append(s.getTotalJavaNodes()).append(" |\n");
            sb.append("| High Confidence Links | ").append(s.getHighConfidenceLinks()).append(" |\n");
            sb.append("| Medium Confidence Links | ").append(s.getMediumConfidenceLinks()).append(" |\n");
            sb.append("| Infrastructure Nodes (excluded) | ").append(s.getUnmatchedJavaNodes()).append(" |\n");
            if (s.getPlsqlNodesFound() > 0)
                sb.append("| PL/SQL Nodes Confirmed | ").append(s.getPlsqlNodesFound()).append(" |\n");
            sb.append("| Total Collections | ").append(s.getTotalJavaCollections()).append(" |\n");
            sb.append("| Matched Tables | ").append(s.getMatchedTables()).append(" |\n");
            sb.append("\n");
        }

        // ── Gap Analysis (most important — show first) ────────────────────────
        if (!result.getJavaOnlyProcs().isEmpty() || !result.getOracleOnlyProcs().isEmpty()
                || !result.getOperationGaps().isEmpty()) {
            sb.append("## Gap Analysis\n\n");
            sb.append("> The cross-reference has found the following gaps between Java and Oracle.\n\n");

            if (!result.getJavaOnlyProcs().isEmpty()) {
                sb.append("### Java-Only (not converted to Oracle)\n\n");
                sb.append("These Java methods access collections with no matching Oracle proc found.\n\n");
                sb.append("| Java Class | Method | Op | Collections | Reason |\n|---|---|---|---|---|\n");
                for (CrossRefResult.JavaOnlyProc p : result.getJavaOnlyProcs()) {
                    sb.append("| `").append(simpleClass(p.getJavaClass())).append("` | ");
                    sb.append(p.getJavaMethod()).append(" | ");
                    sb.append(orDash(p.getOperationType())).append(" | ");
                    sb.append("`").append(String.join(", ", p.getTables())).append("` | ");
                    sb.append(orDash(p.getReason())).append(" |\n");
                }
                sb.append("\n");
            }

            if (!result.getOracleOnlyProcs().isEmpty()) {
                sb.append("### Oracle-Only (no Java caller found)\n\n");
                sb.append("These Oracle procs (depth 0-2) have no corresponding Java caller.\n\n");
                sb.append("| Oracle Proc | Depth | Tables |\n|---|---|---|\n");
                for (CrossRefResult.OracleOnlyProc p : result.getOracleOnlyProcs()) {
                    sb.append("| `").append(orDash(p.getProcRef())).append("` | ");
                    sb.append(p.getDepth()).append(" | ");
                    sb.append(p.getTables().isEmpty() ? "-" : String.join(", ", p.getTables())).append(" |\n");
                }
                sb.append("\n");
            }

            if (!result.getOperationGaps().isEmpty()) {
                sb.append("### Operation Gaps\n\n");
                sb.append("| Table | Java Op | Oracle Op | Java Node | Oracle Proc | Type |\n|---|---|---|---|---|---|\n");
                for (CrossRefResult.OperationGap g : result.getOperationGaps()) {
                    sb.append("| `").append(g.getTable()).append("` | ");
                    sb.append(orDash(g.getJavaOperation())).append(" | ");
                    sb.append(orDash(g.getOracleOperation())).append(" | ");
                    sb.append(orDash(g.getJavaNode())).append(" | ");
                    sb.append(orDash(g.getOracleProc())).append(" | ");
                    sb.append(orDash(g.getGapType())).append(" |\n");
                }
                sb.append("\n");
            }
        }

        sb.append("## Confirmed Proc Links (HIGH + MEDIUM confidence)\n\n");
        for (CrossRefProcLink link : result.getProcLinks()) {
            sb.append("### `").append(simpleClass(link.getJavaClass()))
              .append(".").append(link.getJavaMethod()).append("`\n\n");
            sb.append("| Field | Value |\n|---|---|\n");
            sb.append("| Oracle Proc | `").append(orDash(link.getResolvedProcRef())).append("` |\n");
            sb.append("| Confidence | ").append(link.getMatchConfidence()).append(" |\n");
            if (link.getOracleProcRef() != null)
                sb.append("| Bytecode Ref | `").append(link.getOracleProcRef()).append("` |\n");
            sb.append("| Op Type | ").append(orDash(link.getJavaOperationType())).append(" |\n");
            if (link.isPlsqlNodeFound()) sb.append("| PL/SQL Node | ✓ Confirmed |\n");
            sb.append("\n");

            if (!link.getTableMatches().isEmpty()) {
                sb.append("**Table/Collection Mapping:**\n\n");
                sb.append("| Collection (Java) | Oracle Table | Java Op | Oracle Op | Status |\n");
                sb.append("|---|---|---|---|---|\n");
                for (CrossRefTableMatch tm : link.getTableMatches()) {
                    sb.append("| `").append(orDash(tm.getMongoCollection())).append("` | ");
                    sb.append("`").append(orDash(tm.getOracleTable())).append("` | ");
                    sb.append(orDash(tm.getJavaOperation())).append(" | ");
                    sb.append(orDash(tm.getOracleOperation())).append(" | ");
                    sb.append(statusBadge(tm.getMatchStatus())).append(" |\n");
                }
                sb.append("\n");
            }

            List<CrossRefParamMatch> params = link.getParameterMatches();
            if (!params.isEmpty()) {
                sb.append("**Parameter Mapping:**\n\n");
                sb.append("| Java Literal | Oracle Param | Status |\n|---|---|---|\n");
                for (CrossRefParamMatch pm : params) {
                    String status = pm.getMatched() == null ? "INFERRED"
                            : Boolean.TRUE.equals(pm.getMatched()) ? "MATCHED" : "NOT_FOUND";
                    sb.append("| `").append(pm.getJavaLiteral()).append("` | ");
                    sb.append("`").append(pm.getOracleParam()).append("` | ");
                    sb.append(status).append(" |\n");
                }
                sb.append("\n");
            }
        }

        if (!result.getInfraNodes().isEmpty()) {
            sb.append("## Infrastructure Nodes (No Oracle Equivalent)\n\n");
            sb.append("| Java Class | Method | Reason |\n|---|---|---|\n");
            for (CrossRefResult.InfraNode n : result.getInfraNodes()) {
                sb.append("| `").append(simpleClass(n.getJavaClass())).append("` | ");
                sb.append(n.getJavaMethod()).append(" | ");
                sb.append(n.getReason()).append(" |\n");
            }
            sb.append("\n");
        }

        if (!result.getLowConfidenceHints().isEmpty()) {
            sb.append("## Low-Confidence Hints (unconfirmed — for reference only)\n\n");
            sb.append("> These links are based on fuzzy matching or zone inheritance. ");
            sb.append("Do not treat these as confirmed mappings.\n\n");
            sb.append("| Java Class | Method | Inferred Oracle | Zone | Confidence |\n|---|---|---|---|---|\n");
            for (CrossRefProcLink h : result.getLowConfidenceHints()) {
                sb.append("| `").append(simpleClass(h.getJavaClass())).append("` | ");
                sb.append(h.getJavaMethod()).append(" | ");
                sb.append("`").append(orDash(h.getResolvedProcRef())).append("` | ");
                sb.append(orDash(h.getZoneSource())).append(" | ");
                sb.append(orDash(h.getMatchConfidence())).append(" |\n");
            }
            sb.append("\n");
        }

        if ("STATIC_INFERENCE".equals(result.getMode())) {
            sb.append("---\n> **Note:** This report uses Static Inference mode. ");
            sb.append("Oracle proc names are inferred from Java class naming conventions. ");
            sb.append("Run PL/SQL analysis for `").append(result.getProcName());
            sb.append("` to enable Full Comparison mode with operation-level verification.\n");
        }

        return sb.toString();
    }

    private String simpleClass(String fqn) {
        if (fqn == null) return "-";
        return fqn.contains(".") ? fqn.substring(fqn.lastIndexOf('.') + 1) : fqn;
    }

    private String orDash(String s) { return (s == null || s.isBlank()) ? "-" : s; }

    private String statusBadge(String status) {
        if (status == null) return "-";
        return switch (status) {
            case "MATCHED"      -> "✓ MATCHED";
            case "INFERRED"     -> "~ INFERRED";
            case "JAVA_ONLY"    -> "⚠ JAVA_ONLY";
            case "ORACLE_ONLY"  -> "⚠ ORACLE_ONLY";
            case "OP_MISMATCH"  -> "✗ OP_MISMATCH";
            default             -> status;
        };
    }
}
