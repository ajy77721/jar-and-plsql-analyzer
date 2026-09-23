package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.CallEdge;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CallGraphBuilder {

    private static final Logger log = LoggerFactory.getLogger(CallGraphBuilder.class);

    private Map<String, String> packageSchemaMap = Collections.emptyMap();

    public CallGraph buildGraph(List<PlsqlUnit> units) {
        return buildGraph(units, Collections.emptyMap());
    }

    public CallGraph buildGraph(List<PlsqlUnit> units, Map<String, String> sourceMapSchemaLookup) {
        log.info("Building call graph from {} units", units.size());
        this.packageSchemaMap = sourceMapSchemaLookup;
        CallGraph graph = new CallGraph();

        // Detect overloaded procedure IDs (same SCHEMA.PACKAGE.PROC, different params)
        Map<String, List<PlsqlProcedure>> idToProcs = new LinkedHashMap<>();
        Map<String, PlsqlUnit> procToUnit = new LinkedHashMap<>();
        for (PlsqlUnit unit : units) {
            boolean sa = unit.getUnitType() == PlsqlUnitType.PROCEDURE
                    || unit.getUnitType() == PlsqlUnitType.FUNCTION;
            for (PlsqlProcedure proc : unit.getProcedures()) {
                String baseId = CallGraphNode.buildId(unit.getSchemaName(), sa ? null : unit.getName(), proc.getName());
                idToProcs.computeIfAbsent(baseId, k -> new ArrayList<>()).add(proc);
                procToUnit.put(baseId + ":" + proc.getStartLine(), unit);
            }
        }
        Set<String> overloadedIds = new HashSet<>();
        for (var entry : idToProcs.entrySet()) {
            if (entry.getValue().size() > 1) overloadedIds.add(entry.getKey());
        }
        if (!overloadedIds.isEmpty()) {
            log.info("Detected {} overloaded procedure IDs requiring parameter-based disambiguation", overloadedIds.size());
        }

        // Map: proc (by startLine) → unique node ID (used for edge resolution later)
        Map<String, String> procLineToNodeId = new LinkedHashMap<>();

        // First pass: register all nodes
        for (PlsqlUnit unit : units) {
            log.debug("Processing unit: {}", unit.getName());
            for (PlsqlProcedure proc : unit.getProcedures()) {
                CallGraphNode node = new CallGraphNode();
                node.setSchemaName(unit.getSchemaName());
                boolean standalone = unit.getUnitType() == PlsqlUnitType.PROCEDURE
                        || unit.getUnitType() == PlsqlUnitType.FUNCTION;
                String pkgName = standalone ? null : unit.getName();
                node.setPackageName(pkgName);
                node.setProcedureName(proc.getName());
                String baseId = CallGraphNode.buildId(unit.getSchemaName(), pkgName, proc.getName());
                node.setBaseId(baseId);
                node.setUnitType(proc.getType());
                node.setSourceFile(unit.getSourceFile());
                node.setStartLine(proc.getStartLine());
                node.setEndLine(proc.getEndLine());

                // Build parameter signature for all procs, use it in ID only for overloads
                String paramSig = buildParamSignature(proc);
                node.setParamSignature(paramSig);
                node.setParamCount(proc.getParameters().size());

                if (overloadedIds.contains(baseId)) {
                    node.setId(baseId + "/" + paramSig);
                } else {
                    node.setId(baseId);
                }

                procLineToNodeId.put(baseId + ":" + proc.getStartLine(), node.getId());
                graph.addNode(node);
            }
        }

        // Second pass: register edges from calls
        for (PlsqlUnit unit : units) {
            boolean saUnit = unit.getUnitType() == PlsqlUnitType.PROCEDURE
                    || unit.getUnitType() == PlsqlUnitType.FUNCTION;
            for (PlsqlProcedure proc : unit.getProcedures()) {
                String baseFromId = CallGraphNode.buildId(unit.getSchemaName(), saUnit ? null : unit.getName(), proc.getName());
                String fromId = procLineToNodeId.getOrDefault(baseFromId + ":" + proc.getStartLine(), baseFromId);

                for (ProcedureCall call : proc.getCalls()) {
                    String toId = resolveCallTarget(call, unit, graph);
                    CallEdge edge = new CallEdge(fromId, toId, call.getLineNumber(), call.isDynamicSql());
                    graph.addEdge(edge);

                    // Ensure target node exists (even as unresolved placeholder)
                    if (graph.getNode(toId) == null) {
                        log.warn("Unresolved call target: {} -> {}", fromId, toId);
                        CallGraphNode placeholder = new CallGraphNode();
                        placeholder.setId(toId);
                        String pSchema = call.getSchemaName();
                        if (pSchema == null && call.getPackageName() != null) {
                            pSchema = packageSchemaMap.get(call.getPackageName().toUpperCase());
                        }
                        if (pSchema == null) pSchema = unit.getSchemaName();
                        String pPkg = call.getPackageName();
                        placeholder.setSchemaName(pSchema);
                        placeholder.setPackageName(pPkg);
                        placeholder.setProcedureName(call.getProcedureName());
                        placeholder.setPlaceholder(true);
                        if (pSchema != null && pPkg != null) {
                            placeholder.setSourceFile(pSchema.toUpperCase() + "." + pPkg.toUpperCase());
                        } else if (pSchema != null) {
                            placeholder.setSourceFile(pSchema.toUpperCase() + "." +
                                (call.getProcedureName() != null ? call.getProcedureName().toUpperCase() : ""));
                        }
                        graph.addNode(placeholder);
                    }
                }
            }
        }

        // Third pass: create synthetic package-level root nodes
        // For each PACKAGE_BODY unit with procedures, create a SCHEMA.PKG node
        // with edges to all child procs. This allows package-alone analysis to find a root.
        Map<String, List<String>> pkgProcs = new LinkedHashMap<>();
        Map<String, PlsqlUnit> pkgUnits = new LinkedHashMap<>();
        for (PlsqlUnit unit : units) {
            if (unit.getUnitType() == PlsqlUnitType.PACKAGE_BODY && unit.getProcedures().size() > 0) {
                String pkgId = (unit.getSchemaName() != null ? unit.getSchemaName().toUpperCase() + "." : "")
                        + unit.getName().toUpperCase();
                if (graph.getNode(pkgId) == null) {
                    List<String> childIds = new ArrayList<>();
                    for (PlsqlProcedure proc : unit.getProcedures()) {
                        String baseId = CallGraphNode.buildId(unit.getSchemaName(), unit.getName(), proc.getName());
                        String nodeId = procLineToNodeId.getOrDefault(baseId + ":" + proc.getStartLine(), baseId);
                        childIds.add(nodeId);
                    }
                    pkgProcs.put(pkgId, childIds);
                    pkgUnits.put(pkgId, unit);
                }
            }
        }
        for (Map.Entry<String, List<String>> entry : pkgProcs.entrySet()) {
            String pkgId = entry.getKey();
            PlsqlUnit unit = pkgUnits.get(pkgId);
            CallGraphNode pkgNode = new CallGraphNode();
            pkgNode.setId(pkgId);
            pkgNode.setBaseId(pkgId);
            pkgNode.setSchemaName(unit.getSchemaName());
            pkgNode.setPackageName(unit.getName());
            pkgNode.setProcedureName(null);
            pkgNode.setUnitType(PlsqlUnitType.PACKAGE_BODY);
            pkgNode.setSourceFile(unit.getSourceFile());
            pkgNode.setStartLine(unit.getStartLine());
            pkgNode.setEndLine(unit.getEndLine());
            graph.addNode(pkgNode);
            for (String childId : entry.getValue()) {
                graph.addEdge(new CallEdge(pkgId, childId, 0, false));
            }
            log.debug("Created synthetic package root node: {} with {} child procs", pkgId, entry.getValue().size());
        }

        log.info("Call graph built: {} nodes, {} edges (including {} overloaded proc groups)",
                graph.getNodeCount(), graph.getEdgeCount(), overloadedIds.size());
        return graph;
    }

    private String resolveCallTarget(ProcedureCall call, PlsqlUnit currentUnit, CallGraph graph) {
        // Try exact qualified name first
        String exactId = call.getQualifiedName().toUpperCase();
        if (graph.getNode(exactId) != null) {
            return exactId;
        }

        // Try within current package
        if (call.getPackageName() == null && currentUnit.getName() != null) {
            String withinPackage = CallGraphNode.buildId(
                    currentUnit.getSchemaName(), currentUnit.getName(), call.getProcedureName());
            if (graph.getNode(withinPackage) != null) {
                return withinPackage;
            }
        }

        // Try prefixing with current unit's schema (call says PKG.PROC, node is SCHEMA.PKG.PROC)
        if (call.getSchemaName() == null && currentUnit.getSchemaName() != null) {
            String schemaQualified = currentUnit.getSchemaName().toUpperCase() + "." + exactId;
            if (graph.getNode(schemaQualified) != null) {
                return schemaQualified;
            }
        }

        // For overloaded procs, match any node whose baseId matches
        // (call sites don't carry param info so we link to the first overload)
        for (CallGraphNode node : graph.getAllNodes()) {
            if (exactId.equals(node.getBaseId()) && !exactId.equals(node.getId())) {
                return node.getId();
            }
            // Also try schema-prefixed baseId match for overloads
            if (call.getSchemaName() == null && currentUnit.getSchemaName() != null) {
                String schemaQualified = currentUnit.getSchemaName().toUpperCase() + "." + exactId;
                if (schemaQualified.equals(node.getBaseId()) && !schemaQualified.equals(node.getId())) {
                    return node.getId();
                }
            }
        }

        // Suffix match: if a node ID ends with ".exactId", use it
        for (CallGraphNode node : graph.getAllNodes()) {
            if (node.getId().endsWith("." + exactId)) {
                return node.getId();
            }
        }

        // Look up the package's actual owner from the source map
        if (call.getSchemaName() == null && call.getPackageName() != null) {
            String realSchema = packageSchemaMap.get(call.getPackageName().toUpperCase());
            if (realSchema != null) {
                return realSchema.toUpperCase() + "." + exactId;
            }
        }

        // Fall back to the calling unit's schema
        if (call.getSchemaName() == null && currentUnit.getSchemaName() != null) {
            return currentUnit.getSchemaName().toUpperCase() + "." + exactId;
        }
        return exactId;
    }

    private String buildParamSignature(PlsqlProcedure proc) {
        List<ProcedureParameter> params = proc.getParameters();
        boolean isFn = proc.getType() == PlsqlUnitType.FUNCTION;

        if (params == null || params.isEmpty()) {
            return isFn ? "FN" : "0";
        }

        StringBuilder sb = new StringBuilder();
        if (isFn) sb.append("FN_");
        for (int i = 0; i < params.size(); i++) {
            ProcedureParameter p = params.get(i);
            if (i > 0) sb.append("_");
            String mode = (p.getMode() != null) ? p.getMode().toUpperCase().trim() : "IN";
            if ("OUT".equals(mode)) sb.append("O_");
            else if ("IN OUT".equals(mode)) sb.append("IO_");
            String name = (p.getName() != null) ? p.getName().toUpperCase().replaceAll("[^A-Z0-9_]", "") : ("P" + i);
            sb.append(name);
        }
        return sb.toString();
    }
}
