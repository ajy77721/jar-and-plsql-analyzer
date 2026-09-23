package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.page.FacetSpec;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Resolves the Direct / Subtree / SubFlow / Full selector into a row predicate, server-side.
 *
 * <p>This has to exist for the parser tables to be paged at all. The selector used to be applied
 * in the browser over the whole facet array; once only one page is in memory, a client-side scope
 * filter would filter that page and show a plausible-looking, wrong list. So the scope travels
 * with the page request, exactly like the search box and the sort.
 *
 * <p><b>The semantics are the client's, deliberately reproduced rather than improved.</b>
 * {@code parser/js/scope.js} is the definition of what these four words mean to a user, and the
 * acceptance criterion for this work is that a converted screen renders the same rows. In
 * particular {@code direct} means "this node only", {@code subtree} means "everything it calls but
 * not itself", and the id set includes the {@code $}-form aliases that package analyses use, which
 * is why the node-id map is consulted.
 */
@Component
public class ParserScopeResolver {

    /** The client's sentinel for "a package root is selected", where scope does not apply. */
    private static final String PACKAGE_ROOT = "__PKG_ROOT__";

    private final AnalysisService service;

    public ParserScopeResolver(AnalysisService service) {
        this.service = service;
    }

    /**
     * @param analysis the analysis name
     * @param scope    {@code direct} | {@code subtree} | {@code subflow} | {@code full}; anything
     *                 else, including null, means no filtering
     * @param procId   the selected node, or null
     * @param rowProcedureIds the procedures a row belongs to — see
     *                        {@link FacetSpec#accessDetailProcedureIds}
     * @return null when no filtering applies, so callers can skip the predicate entirely
     * @throws IllegalArgumentException on a scope name that is not one of the four. An unknown
     *         enum is a 400, never a silently unfiltered list.
     */
    public Predicate<JsonNode> predicate(String analysis, String scope, String procId,
                                         Function<JsonNode, List<String>> rowProcedureIds) {
        if (scope == null || scope.isBlank()) {
            // §260 — `procId` alone, with no companion `scope`, used to mean "no filtering at
            // all": a caller asking to be scoped to one procedure silently got every row in the
            // analysis back instead, a 200-OK correctness trap (found independently on the
            // cursors and dynamic-sql facets, on two different dialects). `directProcId` already
            // means "this node only" one parameter earlier in the caller — `procId` alone means
            // the same thing here now, matching that discoverable, single-parameter contract
            // instead of silently requiring an undocumented `scope=direct` companion.
            if (procId == null || procId.isBlank()) return null;
            scope = "direct";
        }
        String mode = scope.toLowerCase(Locale.ROOT);
        if (!Set.of("direct", "subtree", "subflow", "full").contains(mode)) {
            throw new IllegalArgumentException(
                    "Unknown scope '" + scope + "'. Allowed: direct, subtree, subflow, full");
        }
        if ("full".equals(mode) || procId == null || procId.isBlank()
                || PACKAGE_ROOT.equalsIgnoreCase(procId)) {
            return null;
        }

        JsonNode tree;
        try {
            tree = service.getCallTree(analysis, procId);
        } catch (Exception e) {
            // No call tree means no way to answer the question honestly. Returning an unfiltered
            // list would show the whole analysis while the toolbar says "Direct".
            throw new IllegalStateException(
                    "Cannot resolve scope '" + mode + "' for " + procId + ": call tree unavailable",
                    e);
        }
        if (tree == null || tree.isNull() || tree.isMissingNode()) {
            throw new IllegalStateException(
                    "Cannot resolve scope '" + mode + "' for " + procId + ": no call tree");
        }

        Map<String, String> idAliases = nodeIdAliases(analysis);
        Set<String> treeIds = new HashSet<>();
        collectTreeIds(tree, treeIds, idAliases);
        String rootId = tree.path("id").asText("");
        if (rootId.isEmpty()) rootId = procId;
        final String currentId = rootId.toUpperCase(Locale.ROOT);
        final int treeSize = treeIds.size();

        return row -> {
            for (String pid : rowProcedureIds.apply(row)) {
                if (matches(mode, pid, currentId, treeIds, treeSize)) return true;
            }
            return false;
        };
    }

    private static boolean matches(String mode, String pid, String currentId, Set<String> treeIds,
                                   int treeSize) {
        switch (mode) {
            case "direct":
                // "this node only". The treeSize==1 clause is the client's: when the tree has a
                // single node, that node IS the direct scope even if its id differs in form.
                return pid.equals(currentId) || (treeIds.contains(pid) && treeSize == 1);
            case "subtree":
                return !pid.equals(currentId) && treeIds.contains(pid);
            default: // subflow — this node plus everything below it
                return pid.equals(currentId) || treeIds.contains(pid);
        }
    }

    private static void collectTreeIds(JsonNode node, Set<String> out, Map<String, String> aliases) {
        String raw = node.path("id").asText("");
        if (raw.isEmpty()) raw = node.path("nodeId").asText("");
        if (!raw.isEmpty()) {
            String upper = raw.toUpperCase(Locale.ROOT);
            out.add(upper);
            String alias = aliases.get(upper);
            if (alias != null) out.add(alias);
        }
        // `circular` marks a cycle the renderer stops at; descending would not terminate.
        if (node.path("circular").asBoolean(false)) return;
        JsonNode children = node.path("children");
        if (children.isArray()) {
            for (JsonNode child : children) collectTreeIds(child, out, aliases);
        }
    }

    /**
     * Underscore id → {@code $} id, taken from each node's {@code detailFile}.
     *
     * <p>Package analyses address the same node both ways; the client builds this map for exactly
     * this reason, and omitting it would make Subtree lose rows in package analyses only — the
     * kind of gap that survives every small-fixture test.
     */
    private Map<String, String> nodeIdAliases(String analysis) {
        Map<String, String> map = new HashMap<>();
        JsonNode procedures;
        try {
            procedures = service.getProcedures(analysis);
        } catch (Exception e) {
            return map;
        }
        JsonNode nodes = procedures == null ? null
                : (procedures.isArray() ? procedures : procedures.path("nodes"));
        if (nodes == null || !nodes.isArray()) return map;
        for (JsonNode n : nodes) {
            String underscore = n.path("nodeId").asText("").toUpperCase(Locale.ROOT);
            String detailFile = n.path("detailFile").asText("");
            if (underscore.isEmpty() || detailFile.isEmpty()) continue;
            int slash = detailFile.lastIndexOf('/');
            String base = slash >= 0 ? detailFile.substring(slash + 1) : detailFile;
            if (!base.endsWith(".json")) continue;
            String dollar = base.substring(0, base.length() - 5).toUpperCase(Locale.ROOT);
            if (!dollar.equals(underscore)) map.put(underscore, dollar);
        }
        return map;
    }
}
