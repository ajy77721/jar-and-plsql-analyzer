package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

import static com.plsqlanalyzer.web.parser.service.StaticAnalysisReader.*;

/**
 * Builds the package-level virtual call tree for a package that was crawled as a SINGLE
 * aggregated {@code FlowNode} (a bare package name given as the entry point — see
 * {@code DependencyCrawler}'s package-as-entry-point aggregation branch, and
 * {@code FlowNode#getMembers}).
 *
 * <p>There is only one real crawled node here (depth 0); every procedure/function in the package
 * is a synthetic child built from that one node's own {@code members} list (from its detail
 * file), with each child's own call/table counts derived by filtering the SAME node's
 * {@code calls}/{@code tables} lists on {@code subprogramName == member.name} (see
 * {@code ChunkedFlowWriter#groupCalls}/{@code #groupTableOps}, which tag each grouped row with
 * the subprogram it came from whenever the crawl populated it). There is no per-procedure
 * output folder to read anymore.
 */
class PackageTreeBuilder {

    private final StaticAnalysisReader r;

    PackageTreeBuilder(StaticAnalysisReader r) {
        this.r = r;
    }

    /**
     * Cheap check using only the already-loaded index (no detail file read): is the depth-0 node
     * a package? Only packages crawled via the aggregation branch ever reach this true — a
     * package whose specific requested subprogram could not be resolved keeps objectType
     * "PACKAGE" too, but with an empty {@code members} list, so {@link #buildPackageVirtualTree}
     * degrades gracefully to a childless tree in that case.
     */
    boolean isPackageEntry(JsonNode packageIndex) {
        JsonNode depth0 = findDepth0(packageIndex);
        return depth0 != null && "PACKAGE".equalsIgnoreCase(textOrNull(depth0, "objectType"));
    }

    private JsonNode findDepth0(JsonNode packageIndex) {
        JsonNode nodesArr = packageIndex.get("nodes");
        if (nodesArr != null && nodesArr.isArray()) {
            for (JsonNode n : nodesArr) {
                if (intOrDefault(n, "depth", -1) == 0) return n;
            }
        }
        return null;
    }

    /**
     * Build a virtual tree for a package: root = the package's own (single, aggregated) node,
     * children = one synthetic entry per member of {@code FlowNode#getMembers}.
     */
    JsonNode buildPackageVirtualTree(String name, JsonNode packageIndex) {
        ObjectMapper mapper = r.mapper;
        ObjectNode root = mapper.createObjectNode();
        String pkgName = textOrDefault(packageIndex, "entryPoint", name);
        String schema  = textOrDefault(packageIndex, "entrySchema", "");
        root.put("id",   pkgName.toUpperCase().replace(".", "_").replace("/", "_").replace("\\", "_"));
        root.put("name", pkgName);
        root.put("schemaName", schema);
        root.put("packageName", "");
        root.put("objectType", "PACKAGE");
        root.put("unitType", "PACKAGE");
        root.put("callType", "ROOT");
        root.put("callLineNumber", 0);
        root.put("startLine", 0); root.put("endLine", 0);
        root.put("readable", true);

        ArrayNode children = mapper.createArrayNode();

        JsonNode depth0 = findDepth0(packageIndex);
        String sourceFile = depth0 != null ? textOrNull(depth0, "sourceFile") : null;
        String detailFile = depth0 != null ? textOrNull(depth0, "detailFile") : null;

        JsonNode detail = readDetail(name, detailFile);
        JsonNode members = detail != null ? detail.get("members") : null;
        if (members != null && members.isArray()) {
            JsonNode tables = detail.get("tables");
            JsonNode calls = detail.get("calls");
            for (JsonNode member : members) {
                children.add(buildMemberNode(mapper, pkgName, schema, sourceFile, member, tables, calls));
            }
        }
        root.set("children", children);
        return root;
    }

    /** One synthetic leaf for a package member, counts filtered from the package's own node. */
    private ObjectNode buildMemberNode(ObjectMapper mapper, String pkgName, String schema,
                                       String sourceFile, JsonNode member,
                                       JsonNode tables, JsonNode calls) {
        String memberName = textOrNull(member, "name");
        String memberType = textOrDefault(member, "type", "PROCEDURE");
        int lineStart = intOrDefault(member, "lineStart", 0);
        int lineEnd = intOrDefault(member, "lineEnd", 0);

        ObjectNode child = mapper.createObjectNode();
        String childId = (schema + "$" + pkgName + "$" + memberName)
                .toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
        child.put("id", childId);
        child.put("name", memberName);
        child.put("schemaName", schema);
        child.put("packageName", pkgName);
        child.put("objectType", memberType);
        child.put("unitType", memberType);
        child.put("callType", "INTERNAL");
        child.put("callLineNumber", 0);
        child.put("startLine", lineStart);
        child.put("endLine", lineEnd);
        child.put("sourceFile", sourceFile != null ? sourceFile : "");
        child.put("linesOfCode", (lineEnd > 0 && lineStart > 0 && lineEnd >= lineStart)
                ? (lineEnd - lineStart + 1) : 0);
        child.put("readable", true);
        // Not a real crawled node — lets a caller that already resolved this child recognise it
        // as one member of the single package result rather than a subfolder/independent node.
        child.put("subprogramName", memberName);
        child.set("counts", buildMemberCounts(mapper, tables, calls, memberName));
        child.set("children", mapper.createArrayNode());
        return child;
    }

    /** Tables/calls counts for one member, filtered by the subprogramName tag on each grouped row. */
    private ObjectNode buildMemberCounts(ObjectMapper mapper, JsonNode tables, JsonNode calls, String memberName) {
        ObjectNode counts = mapper.createObjectNode();
        int tableCount = 0, callsOut = 0, internalCalls = 0, externalCalls = 0;
        if (tables != null && tables.isArray()) {
            for (JsonNode t : tables) {
                if (memberName.equalsIgnoreCase(textOrNull(t, "subprogramName"))) tableCount++;
            }
        }
        if (calls != null && calls.isArray()) {
            for (JsonNode c : calls) {
                if (!memberName.equalsIgnoreCase(textOrNull(c, "subprogramName"))) continue;
                callsOut++;
                if ("INTERNAL".equalsIgnoreCase(textOrDefault(c, "type", ""))) internalCalls++;
                else externalCalls++;
            }
        }
        counts.put("tables", tableCount);
        counts.put("callsOut", callsOut);
        counts.put("internalCalls", internalCalls);
        counts.put("externalCalls", externalCalls);
        return counts;
    }

    /** {@code detailFile} is e.g. {@code "nodes/SCHEMA$PKG.json"} — {@link StaticAnalysisReader#getNodeDetail} wants just the filename. */
    private JsonNode readDetail(String name, String detailFile) {
        if (detailFile == null) return null;
        int slash = detailFile.lastIndexOf('/');
        String fileName = slash >= 0 ? detailFile.substring(slash + 1) : detailFile;
        try {
            return r.getNodeDetail(name, fileName);
        } catch (IOException e) {
            return null;
        }
    }
}
