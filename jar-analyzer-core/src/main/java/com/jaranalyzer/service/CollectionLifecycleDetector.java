package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Detects multi-step "collection lifecycle workflows" (e.g. a blue-green collection swap:
 * create temp -> copy data -> index temp -> rename live<->temp -> drop backup) that span MANY
 * separate call-tree nodes scattered under one orchestrating method.
 *
 * Two-phase design:
 *   Phase A ({@link #detectStages}) — called per-method from CollectionResolver, alongside the
 *     existing Mongo/Aggregation detection loops that already scan method.invocations. Matches
 *     REAL MongoDB driver/Spring Data method names (createCollection, createIndex,
 *     renameCollection, drop) against resolved bytecode invocations, and real pipeline-stage
 *     tokens ($merge/$out) against AggregationDetector's already-computed PipelineDetail.stages.
 *     Deliberately does NOT match against application-defined wrapper method names (e.g.
 *     "mergeCollectionsInBatches", "createCollectionIfNotExists") — those are free to be renamed
 *     at any time and are not a reliable detection signal.
 *   Phase B ({@link #aggregate}) — called post-build from the same three call sites that invoke
 *     EndpointInfo.computeAggregates(). Walks the already-tagged tree (CallNode.lifecycleStageTags,
 *     set by Phase A) and groups scattered stage-tagged nodes into workflow instances, keyed by
 *     the nearest SERVICE/COMPONENT/CONTROLLER ancestor that is not itself a stage match.
 */
@Component
class CollectionLifecycleDetector {

    private final DomainConfigLoader configLoader;

    // Owner-class substrings recognized as MongoDB driver/Spring Data classes. Deliberately a
    // small, self-contained check (not shared with MongoMethodDetector's private constants) so
    // this detector has no coupling to another detector's internals.
    private static final List<String> MONGO_OWNER_HINTS = List.of(
            "MongoDatabase", "MongoTemplate", "MongoOperations", "MongoCollection",
            "InterceptableMongoDatabase"
    );

    CollectionLifecycleDetector(DomainConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    // ========== Phase A: per-method stage tagging ==========

    /**
     * Scans this method's resolved invocations (and the already-computed aggregation pipeline
     * detail) for real MongoDB API calls matching any configured workflow's stage definitions.
     * Returns workflow-type-qualified stage tags (e.g. "BLUE_GREEN_COLLECTION_SWAP:CREATE_TEMP")
     * found on THIS method — usually zero or one, occasionally more if a single method performs
     * more than one stage's worth of work.
     */
    List<String> detectStages(CallGraphIndex.IndexedMethod method, AggregationDetector.PipelineDetail pipelineDetail) {
        Map<String, DomainConfigLoader.WorkflowConfig> workflows = configLoader.getCollectionLifecycleWorkflows();
        if (workflows == null || workflows.isEmpty()) return List.of();

        List<String> tags = new ArrayList<>();
        for (DomainConfigLoader.WorkflowConfig workflow : workflows.values()) {
            for (DomainConfigLoader.StageConfig stage : workflow.stages()) {
                if (matchesApiMethod(method, stage) || matchesPipelineStageToken(pipelineDetail, stage)) {
                    tags.add(workflow.typeName() + ":" + stage.name());
                }
            }
        }
        return tags;
    }

    private boolean matchesApiMethod(CallGraphIndex.IndexedMethod method, DomainConfigLoader.StageConfig stage) {
        if (stage.apiMethods() == null || stage.apiMethods().isEmpty()) return false;
        for (InvocationRef inv : method.invocations) {
            if (stage.apiMethods().contains(inv.methodName()) && isMongoOwner(inv.ownerClass())) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesPipelineStageToken(AggregationDetector.PipelineDetail pipelineDetail, DomainConfigLoader.StageConfig stage) {
        if (stage.pipelineStageTokens() == null || stage.pipelineStageTokens().isEmpty()) return false;
        if (pipelineDetail == null || pipelineDetail.stages == null || pipelineDetail.stages.isEmpty()) return false;
        for (Map<String, Object> stageEntry : pipelineDetail.stages) {
            Object stageToken = stageEntry.get("stage");
            if (stageToken != null && stage.pipelineStageTokens().contains(stageToken.toString())) {
                return true;
            }
        }
        return false;
    }

    private boolean isMongoOwner(String owner) {
        if (owner == null) return false;
        for (String hint : MONGO_OWNER_HINTS) {
            if (owner.contains(hint)) return true;
        }
        return false;
    }

    // ========== Phase B: cross-node structural aggregation ==========

    private record StageMatch(String workflowType, String stageName, CallNode node, int dfsIndex, List<String> breadcrumb) {}

    /**
     * Walks the already-tagged call tree and groups scattered lifecycleStageTags into workflow
     * instances. Each instance is rooted at the nearest SERVICE/COMPONENT/CONTROLLER ancestor of
     * its matched stages (never a stage-tagged node itself), and kept only if it meets the
     * configured workflow's minStagesMatched threshold.
     */
    List<Map<String, Object>> aggregate(CallNode root, Map<String, List<CallNode>> fragments) {
        Map<String, DomainConfigLoader.WorkflowConfig> workflows = configLoader.getCollectionLifecycleWorkflows();
        if (workflows == null || workflows.isEmpty() || root == null) return List.of();

        Map<CallNode, List<StageMatch>> byRoot = new IdentityHashMap<>();
        Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        int[] dfsCounter = {0};
        walk(root, root, dfsCounter, byRoot, visited, fragments, new ArrayList<>());

        List<Map<String, Object>> results = new ArrayList<>();
        for (Map.Entry<CallNode, List<StageMatch>> entry : byRoot.entrySet()) {
            CallNode instanceRoot = entry.getKey();
            // Group this root's matches by workflow type — one root can host instances of
            // more than one configured workflow type simultaneously.
            Map<String, List<StageMatch>> byWorkflowType = new LinkedHashMap<>();
            for (StageMatch m : entry.getValue()) {
                byWorkflowType.computeIfAbsent(m.workflowType(), k -> new ArrayList<>()).add(m);
            }
            for (Map.Entry<String, List<StageMatch>> wfEntry : byWorkflowType.entrySet()) {
                DomainConfigLoader.WorkflowConfig workflow = workflows.get(wfEntry.getKey());
                if (workflow == null) continue;
                List<StageMatch> matches = wfEntry.getValue();
                Set<String> distinctStages = new LinkedHashSet<>();
                for (StageMatch m : matches) distinctStages.add(m.stageName());
                if (distinctStages.size() < workflow.minStagesMatched()) continue;

                matches.sort(Comparator.comparingInt(StageMatch::dfsIndex));
                Map<String, Object> instance = new LinkedHashMap<>();
                instance.put("workflowType", workflow.typeName());
                instance.put("rootClassName", safe(instanceRoot.getClassName()));
                instance.put("rootSimpleClassName", safe(instanceRoot.getSimpleClassName()));
                instance.put("rootMethodName", safe(instanceRoot.getMethodName()));
                instance.put("matchedStageCount", distinctStages.size());
                instance.put("totalConfiguredStages", workflow.stages().size());
                instance.put("complete", distinctStages.size() == workflow.stages().size());
                instance.put("sourceJar", safe(instanceRoot.getSourceJar()));
                instance.put("jarName", safe(instanceRoot.getJarName()));
                instance.put("sourcePath", safe(instanceRoot.getSourcePath()));

                List<Map<String, Object>> steps = new ArrayList<>();
                for (StageMatch m : matches) {
                    Map<String, Object> step = new LinkedHashMap<>();
                    step.put("stageName", m.stageName());
                    step.put("className", safe(m.node().getClassName()));
                    step.put("simpleClassName", safe(m.node().getSimpleClassName()));
                    step.put("methodName", safe(m.node().getMethodName()));
                    step.put("operationType", m.node().getOperationType());
                    step.put("breadcrumb", m.breadcrumb());
                    String hint = findDynamicVariableHint(m.node(), m.stageName());
                    if (hint != null) step.put("dynamicVariableHint", hint);
                    steps.add(step);
                }
                instance.put("steps", steps);
                results.add(instance);
            }
        }
        return results;
    }

    private void walk(CallNode node, CallNode nearestOrchestrator, int[] dfsCounter,
                       Map<CallNode, List<StageMatch>> byRoot, Set<CallNode> visited,
                       Map<String, List<CallNode>> fragments, List<String> breadcrumbPath) {
        if (node == null || !visited.add(node)) return;

        List<String> bcPath = new ArrayList<>(breadcrumbPath);
        bcPath.add(safe(node.getSimpleClassName()) + "." + safe(node.getMethodName()) + "()");
        if (bcPath.size() > 6) bcPath = bcPath.subList(bcPath.size() - 6, bcPath.size());

        List<String> tags = node.getLifecycleStageTags();
        boolean hasStageTags = tags != null && !tags.isEmpty();
        if (hasStageTags) {
            int dfsIndex = dfsCounter[0]++;
            for (String tag : tags) {
                int sep = tag.indexOf(':');
                if (sep < 0) continue;
                String workflowType = tag.substring(0, sep);
                String stageName = tag.substring(sep + 1);
                byRoot.computeIfAbsent(nearestOrchestrator, k -> new ArrayList<>())
                        .add(new StageMatch(workflowType, stageName, node, dfsIndex, bcPath));
            }
        } else {
            dfsCounter[0]++;
        }

        // A stage-tagged node is never promoted to be its own workflow's orchestrator root —
        // only a non-stage-match SERVICE/COMPONENT/CONTROLLER node is promoted. Additionally, a
        // SERVICE/COMPONENT node is only promoted when it is the OUTERMOST node in a contiguous
        // run of SERVICE/COMPONENT ancestors — i.e. do NOT re-promote to a nested SERVICE node
        // when the current nearestOrchestrator is already SERVICE/COMPONENT-stereotyped. This
        // keeps the topmost orchestrator stable for everything nested beneath it (e.g.
        // processWithBatch -> cpyIndexAndValidation -> createCollectionIfNotExists should all
        // group under processWithBatch, not fragment into a separate root at
        // cpyIndexAndValidation). CONTROLLER is always treated as a hard boundary and is promoted
        // unconditionally, since it's always a real entry-point boundary.
        CallNode childOrchestrator = nearestOrchestrator;
        if (!hasStageTags) {
            String stereotype = node.getStereotype();
            if ("CONTROLLER".equals(stereotype)) {
                childOrchestrator = node;
            } else if ("SERVICE".equals(stereotype) || "COMPONENT".equals(stereotype)) {
                String currentStereotype = nearestOrchestrator != null ? nearestOrchestrator.getStereotype() : null;
                boolean currentIsServiceLike = "SERVICE".equals(currentStereotype) || "COMPONENT".equals(currentStereotype);
                if (!currentIsServiceLike) {
                    childOrchestrator = node;
                }
            }
        }

        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                walk(child, childOrchestrator, dfsCounter, byRoot, visited, fragments, bcPath);
            }
        }
        if (node.getFragmentRef() != null && fragments != null) {
            List<CallNode> overflow = fragments.get(node.getFragmentRef());
            if (overflow != null) {
                for (CallNode child : overflow) {
                    walk(child, childOrchestrator, dfsCounter, byRoot, visited, fragments, bcPath);
                }
            }
        }
    }

    /**
     * Optional enrichment: if this node also captured a dynamic (variable-sourced) collection
     * reference whose variable name hints at this stage's role (e.g. "temp" for CREATE_TEMP/COPY,
     * "rename" for RENAME), surface it for extra UI context. Absence never disqualifies the
     * already-found stage match — this is enrichment only.
     */
    private String findDynamicVariableHint(CallNode node, String stageName) {
        List<Map<String, Object>> refs = node.getDynamicCollectionRefs();
        if (refs == null || refs.isEmpty()) return null;
        String hintSubstring = switch (stageName) {
            case "CREATE_TEMP", "COPY" -> "temp";
            case "RENAME" -> "rename";
            default -> null;
        };
        if (hintSubstring == null) return null;
        for (Map<String, Object> ref : refs) {
            Object varName = ref.get("variableName");
            if (varName != null && varName.toString().toLowerCase().contains(hintSubstring)) {
                return varName.toString();
            }
        }
        return null;
    }

    private static String safe(String s) { return s != null ? s : ""; }
}
