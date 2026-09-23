package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Post-pass over one jar/war's classesFile (JSONL of ClassInfo, one class per line) that
 * transitively resolves stereotype/repositoryEntityType for multi-level Spring Data repository
 * interface chains, e.g.:
 *
 * <pre>
 * interface A extends MongoRepository&lt;Entity, String&gt; { }   // tagged REPOSITORY directly today
 * interface B extends A { }                                    // stays OTHER today -- this is the gap
 * interface C extends B { }                                    // stays OTHER today -- this is the gap
 * </pre>
 *
 * {@link BytecodeClassParser} parses one class at a time with zero visibility into other classes,
 * so it can only classify a class by its OWN direct {@code interfaces} list. This resolver runs
 * once all classes in the jar are known, and walks each unresolved class's interface chain to see
 * if it transitively reaches a Spring Data marker interface -- using the exact same per-hop rule
 * BytecodeClassParser already uses for the one-hop case, so nothing that already worked changes.
 *
 * <p>Must run BEFORE classesFile is streamed into the call-graph index
 * ({@code CallGraphService.addToIndex}) or persisted to analysis.json
 * ({@code PersistenceService.writeAnalysisStreaming}) -- both read classesFile directly and have
 * no other way to see corrected values.
 */
@Component
public class RepositoryStereotypeResolver {

    private static final Logger log = LoggerFactory.getLogger(RepositoryStereotypeResolver.class);

    /** Defensive only -- real interface chains are never this deep; guards against pathological
     * bytecode without relying solely on the cycle guard. */
    private static final int MAX_CHAIN_DEPTH = 20;

    private static final byte[] NEWLINE = new byte[]{'\n'};

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JarParserService parserService;

    public RepositoryStereotypeResolver(JarParserService parserService) {
        this.parserService = parserService;
    }

    /** Lightweight per-class snapshot used for resolution -- deliberately excludes
     * methods/fields/annotations so memory stays well below what building the full call-graph
     * index already holds for the same jar. */
    private record LightClass(String stereotype, List<String> interfaces, String repositoryEntityType) {
    }

    private record Resolution(String stereotype, String repositoryEntityType) {
    }

    /**
     * Returns a path to a classesFile with corrected stereotype/repositoryEntityType values. If
     * no class needed correcting, returns the SAME path unchanged (no rewrite performed).
     */
    public Path resolveTransitiveRepositoryStereotypes(Path classesFile) throws IOException {
        Map<String, LightClass> byFqn = new HashMap<>();
        parserService.streamClasses(classesFile, ci -> {
            String fqn = ci.getFullyQualifiedName();
            if (fqn != null) {
                byFqn.put(fqn, new LightClass(ci.getStereotype(), ci.getInterfaces(),
                        ci.getRepositoryEntityType()));
            }
        });

        Map<String, Resolution> corrections = new HashMap<>();
        Map<String, Resolution> memo = new HashMap<>();
        for (Map.Entry<String, LightClass> entry : byFqn.entrySet()) {
            if (isRepoStereotype(entry.getValue().stereotype())) continue;
            Resolution r = resolve(entry.getKey(), byFqn, memo, new HashSet<>(), 0);
            if (r != null) {
                corrections.put(entry.getKey(), r);
            }
        }

        if (corrections.isEmpty()) {
            return classesFile;
        }

        log.info("RepositoryStereotypeResolver: {} class(es) reclassified via multi-level "
                + "repository interface chains", corrections.size());

        Path patched = Files.createTempFile("jar-classes-repo-patched-", ".jsonl");
        try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(patched), 65536)) {
            parserService.streamClasses(classesFile, ci -> {
                Resolution r = corrections.get(ci.getFullyQualifiedName());
                if (r != null) {
                    ci.setStereotype(r.stereotype());
                    if (ci.getRepositoryEntityType() == null) {
                        ci.setRepositoryEntityType(r.repositoryEntityType());
                    }
                }
                out.write(objectMapper.writeValueAsBytes(ci));
                out.write(NEWLINE);
            });
        }
        Files.deleteIfExists(classesFile);
        return patched;
    }

    private static boolean isRepoStereotype(String stereotype) {
        return "REPOSITORY".equals(stereotype) || "SPRING_DATA".equals(stereotype);
    }

    /**
     * Walks {@code fqn}'s direct interfaces looking for a path to a Spring Data marker interface.
     * Memoized per-fqn (shared ancestors across many classes are only resolved once); a class
     * still mid-resolution on the current path is tracked in {@code visited} so a cyclic interface
     * reference resolves to failure instead of infinite recursion.
     */
    private Resolution resolve(String fqn, Map<String, LightClass> byFqn, Map<String, Resolution> memo,
                                Set<String> visited, int depth) {
        if (memo.containsKey(fqn)) {
            return memo.get(fqn);
        }
        if (depth >= MAX_CHAIN_DEPTH || !visited.add(fqn)) {
            return null;
        }

        LightClass lc = byFqn.get(fqn);
        Resolution result = null;
        if (lc != null) {
            for (String iface : lc.interfaces()) {
                Resolution hop = resolveHop(iface, byFqn, memo, visited, depth);
                if (hop != null) {
                    result = hop;
                    break;
                }
            }
        }

        visited.remove(fqn);
        memo.put(fqn, result);
        return result;
    }

    /**
     * Resolves ONE hop: is {@code iface} itself a terminal success (a literal Spring Data marker
     * interface, or a name that already looks repository-ish -- the exact same one-hop heuristic
     * BytecodeClassParser uses), or should we recurse into it because it's an in-project interface
     * we have data for?
     */
    private Resolution resolveHop(String iface, Map<String, LightClass> byFqn, Map<String, Resolution> memo,
                                   Set<String> visited, int depth) {
        if (SpringDataRepositoryInterfaces.MARKER_INTERFACES.contains(iface)
                || SpringDataRepositoryInterfaces.looksLikeRepositoryByName(iface)) {
            LightClass ifaceInfo = byFqn.get(iface);
            String entityType = ifaceInfo != null ? ifaceInfo.repositoryEntityType() : null;
            return new Resolution("REPOSITORY", entityType);
        }
        if (byFqn.containsKey(iface)) {
            Resolution deeper = resolve(iface, byFqn, memo, visited, depth + 1);
            if (deeper != null) {
                // Prefer this hop's OWN repositoryEntityType (nearer the leaf) if it has one,
                // else fall back to whatever was found further up the chain.
                LightClass ifaceInfo = byFqn.get(iface);
                String entityType = (ifaceInfo != null && ifaceInfo.repositoryEntityType() != null)
                        ? ifaceInfo.repositoryEntityType() : deeper.repositoryEntityType();
                return new Resolution("REPOSITORY", entityType);
            }
        }
        return null;
    }
}
