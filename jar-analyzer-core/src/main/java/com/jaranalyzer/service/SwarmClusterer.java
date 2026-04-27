package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ACO-inspired swarm clustering for endpoint grouping.
 * <p>
 * Ant Colony Optimization (ACO) applied to endpoint grouping:
 * <p>
 * 1. PHEROMONE MODEL: Each dependency (class, collection, domain) gets a pheromone
 *    level inversely proportional to how many endpoints reference it.
 *    Rare dependencies = high pheromone = more discriminating for clustering.
 * <p>
 * 2. ANT WALK: Each "ant" starts at an unassigned endpoint (seed), then greedily
 *    walks to the nearest unassigned endpoint by pheromone-weighted similarity.
 * <p>
 * 3. SEMANTIC CHUNKING: Clusters are ordered by internal cohesion (tightest first).
 * <p>
 * 4. CHUNK SIZE: Each cluster = one Claude session. Max METHODS_PER_SESSION endpoints.
 */
@Component
class SwarmClusterer {

    private static final Logger log = LoggerFactory.getLogger(SwarmClusterer.class);
    static final int METHODS_PER_SESSION = 25;
    static final int MAX_CLUSTER_CONTEXT_CHARS = 8_000;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TreeChunker treeChunker;

    SwarmClusterer(TreeChunker treeChunker) {
        this.treeChunker = treeChunker;
    }

    /**
     * ACO-inspired clustering: pheromone-weighted similarity with greedy ant walk.
     */
    List<List<EndpointInfo>> clusterEndpoints(List<EndpointInfo> endpoints) {
        if (endpoints.size() <= 1) return List.of(new ArrayList<>(endpoints));

        // Phase 1: Build dependency fingerprints
        Map<EndpointInfo, Set<String>> depSets = new LinkedHashMap<>();
        Map<String, Integer> depFrequency = new HashMap<>();

        for (EndpointInfo ep : endpoints) {
            Set<String> deps = new LinkedHashSet<>();
            collectDependencies(ep.getCallTree(), deps);
            depSets.put(ep, deps);
            for (String dep : deps) {
                depFrequency.merge(dep, 1, Integer::sum);
            }
        }

        // Phase 2: Compute pheromone levels (inverse document frequency)
        int N = endpoints.size();
        Map<String, Double> pheromone = new HashMap<>();
        for (Map.Entry<String, Integer> entry : depFrequency.entrySet()) {
            int freq = entry.getValue();
            double idf = Math.max(0.1, Math.log((double) N / freq));
            String dep = entry.getKey();
            if (dep.startsWith("coll:")) idf *= 2.0;
            else if (dep.startsWith("domain:")) idf *= 1.5;
            pheromone.put(dep, idf);
        }

        log.info("  ACO pheromone: {} unique dependencies, N={}", pheromone.size(), N);

        // Phase 3: Ant walk -- greedy clustering with pheromone-weighted similarity
        List<List<EndpointInfo>> clusters = new ArrayList<>();
        Set<EndpointInfo> assigned = new HashSet<>();

        List<EndpointInfo> sortedByDeps = new ArrayList<>(endpoints);
        sortedByDeps.sort((a, b) -> Integer.compare(depSets.get(b).size(), depSets.get(a).size()));

        for (EndpointInfo seed : sortedByDeps) {
            if (assigned.contains(seed)) continue;

            List<EndpointInfo> cluster = new ArrayList<>();
            cluster.add(seed);
            assigned.add(seed);

            while (cluster.size() < METHODS_PER_SESSION) {
                EndpointInfo bestNeighbor = null;
                double bestScore = 0;

                for (EndpointInfo candidate : endpoints) {
                    if (assigned.contains(candidate)) continue;

                    double score = pheromoneWeightedSimilarity(
                            depSets.get(seed), depSets.get(candidate), pheromone);

                    if (seed.getControllerSimpleName() != null
                            && seed.getControllerSimpleName().equals(candidate.getControllerSimpleName())) {
                        score += 0.3;
                    }

                    if (score > bestScore) {
                        bestScore = score;
                        bestNeighbor = candidate;
                    }
                }

                if (bestNeighbor == null || bestScore < 0.15) break;

                cluster.add(bestNeighbor);
                assigned.add(bestNeighbor);
            }

            clusters.add(cluster);
        }

        // Phase 4: Sort clusters by internal cohesion (tightest clusters first)
        clusters.sort((a, b) -> {
            double cohA = clusterCohesion(a, depSets, pheromone);
            double cohB = clusterCohesion(b, depSets, pheromone);
            return Double.compare(cohB, cohA);
        });

        // Log cluster summary
        for (int i = 0; i < clusters.size(); i++) {
            List<EndpointInfo> c = clusters.get(i);
            double coh = clusterCohesion(c, depSets, pheromone);
            String members = c.stream()
                    .map(ep -> ep.getControllerSimpleName() + "." + ep.getMethodName()
                            + "(" + (ep.getParameters() != null ? ep.getParameters().size() : 0) + " params)")
                    .collect(Collectors.joining(", "));
            log.info("  Cluster {}: {} endpoints, cohesion={} [{}]",
                    i + 1, c.size(), String.format("%.3f", coh), members);
        }

        return clusters;
    }

    /**
     * Pheromone-weighted similarity between two dependency sets.
     */
    double pheromoneWeightedSimilarity(Set<String> depsA, Set<String> depsB,
                                       Map<String, Double> pheromone) {
        double sharedPheromone = 0;
        double totalPheromone = 0;

        Set<String> union = new LinkedHashSet<>(depsA);
        union.addAll(depsB);

        for (String dep : union) {
            double p = pheromone.getOrDefault(dep, 0.1);
            totalPheromone += p;
            if (depsA.contains(dep) && depsB.contains(dep)) {
                sharedPheromone += p;
            }
        }

        return totalPheromone > 0 ? sharedPheromone / totalPheromone : 0;
    }

    /**
     * Measure internal cohesion of a cluster: average pairwise pheromone similarity.
     */
    double clusterCohesion(List<EndpointInfo> cluster,
                           Map<EndpointInfo, Set<String>> depSets,
                           Map<String, Double> pheromone) {
        if (cluster.size() <= 1) return 1.0;
        double totalSim = 0;
        int pairs = 0;
        for (int i = 0; i < cluster.size(); i++) {
            for (int j = i + 1; j < cluster.size(); j++) {
                totalSim += pheromoneWeightedSimilarity(
                        depSets.get(cluster.get(i)), depSets.get(cluster.get(j)), pheromone);
                pairs++;
            }
        }
        return pairs > 0 ? totalSim / pairs : 0;
    }

    /**
     * Collect all dependency identifiers from a call tree.
     * Classes, stereotypes, domains, and collections form the fingerprint.
     */
    void collectDependencies(CallNode node, Set<String> deps) {
        if (node == null) return;
        deps.add("method:" + treeChunker.nodeSignature(node));
        if (node.getClassName() != null) deps.add("class:" + node.getClassName());
        if (node.getStereotype() != null) deps.add("stereo:" + node.getStereotype());
        if (node.getDomain() != null) deps.add("domain:" + node.getDomain());
        if (node.getCollectionsAccessed() != null) {
            for (String coll : node.getCollectionsAccessed()) deps.add("coll:" + coll);
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                collectDependencies(child, deps);
            }
        }
    }

    /**
     * Build a compact summary of shared context for a cluster.
     */
    String buildClusterContext(List<EndpointInfo> cluster) {
        try {
            Set<String> sharedClasses = new LinkedHashSet<>();
            Set<String> sharedCollections = new LinkedHashSet<>();
            Set<String> sharedDomains = new LinkedHashSet<>();
            List<Map<String, String>> members = new ArrayList<>();

            for (EndpointInfo ep : cluster) {
                members.add(Map.of(
                        "controller", ep.getControllerSimpleName(),
                        "method", ep.getMethodName(),
                        "path", ep.getFullPath() != null ? ep.getFullPath() : ""
                ));
                Set<String> deps = new LinkedHashSet<>();
                collectDependencies(ep.getCallTree(), deps);
                for (String dep : deps) {
                    if (dep.startsWith("class:")) sharedClasses.add(dep.substring(6));
                    else if (dep.startsWith("coll:")) sharedCollections.add(dep.substring(5));
                    else if (dep.startsWith("domain:")) sharedDomains.add(dep.substring(7));
                }
            }

            Map<String, Object> ctx = new LinkedHashMap<>();
            ctx.put("clusterSize", cluster.size());
            ctx.put("endpoints", members);
            ctx.put("sharedDomains", sharedDomains);
            ctx.put("sharedCollections", sharedCollections);

            // Cap sharedServices to keep context under limit
            Set<String> services = sharedClasses;
            if (services.size() > 50) {
                services = new LinkedHashSet<>(new ArrayList<>(services).subList(0, 50));
            }
            ctx.put("sharedServices", services);

            PromptTemplates.DbTechnology tech = PromptTemplates.detectTechnology(cluster);
            ctx.put("complexityRules", PromptTemplates.buildComplexityRules(tech));

            String result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ctx);
            if (result.length() > MAX_CLUSTER_CONTEXT_CHARS) {
                log.info("Cluster context exceeds {}KB ({} chars), trimming sharedServices",
                        MAX_CLUSTER_CONTEXT_CHARS / 1000, result.length());
                // Progressively reduce services until under limit
                List<String> serviceList = new ArrayList<>(sharedClasses);
                int maxServices = serviceList.size();
                while (result.length() > MAX_CLUSTER_CONTEXT_CHARS && maxServices > 5) {
                    maxServices = maxServices / 2;
                    ctx.put("sharedServices", new LinkedHashSet<>(serviceList.subList(0, Math.min(maxServices, serviceList.size()))));
                    result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ctx);
                }
            }
            return result;
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * Write the cluster plan as a fragment for visibility.
     */
    void writeClusterPlan(java.nio.file.Path workDir, List<List<EndpointInfo>> clusters,
                          FragmentStore fragmentStore) {
        try {
            // Rebuild pheromone for the plan output
            Map<EndpointInfo, Set<String>> depSets = new LinkedHashMap<>();
            Map<String, Integer> depFreq = new HashMap<>();
            for (List<EndpointInfo> cluster : clusters) {
                for (EndpointInfo ep : cluster) {
                    Set<String> deps = new LinkedHashSet<>();
                    collectDependencies(ep.getCallTree(), deps);
                    depSets.put(ep, deps);
                    for (String d : deps) depFreq.merge(d, 1, Integer::sum);
                }
            }
            int total = depSets.size();
            Map<String, Double> pheromone = new HashMap<>();
            for (Map.Entry<String, Integer> e : depFreq.entrySet()) {
                double idf = Math.max(0.1, Math.log((double) total / e.getValue()));
                if (e.getKey().startsWith("coll:")) idf *= 2.0;
                else if (e.getKey().startsWith("domain:")) idf *= 1.5;
                pheromone.put(e.getKey(), idf);
            }

            List<Map<String, Object>> plan = new ArrayList<>();
            for (int i = 0; i < clusters.size(); i++) {
                List<EndpointInfo> cluster = clusters.get(i);
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("cluster", i + 1);
                entry.put("size", cluster.size());
                entry.put("cohesion", String.format("%.3f", clusterCohesion(cluster, depSets, pheromone)));
                entry.put("endpoints", cluster.stream()
                        .map(ep -> ep.getControllerSimpleName() + "." + ep.getMethodName())
                        .toList());

                Set<String> allDeps = new LinkedHashSet<>();
                for (EndpointInfo ep : cluster) {
                    allDeps.addAll(depSets.get(ep));
                }
                entry.put("totalDependencies", allDeps.size());

                List<String> topDeps = allDeps.stream()
                        .sorted((a, b) -> Double.compare(
                                pheromone.getOrDefault(b, 0.0),
                                pheromone.getOrDefault(a, 0.0)))
                        .limit(10)
                        .toList();
                entry.put("topPheromone", topDeps);
                plan.add(entry);
            }

            Map<String, Object> output = new LinkedHashMap<>();
            output.put("algorithm", "ACO-SwarmCluster");
            output.put("totalEndpoints", depSets.size());
            output.put("totalClusters", clusters.size());
            output.put("uniqueDependencies", depFreq.size());
            output.put("maxClusterSize", METHODS_PER_SESSION);
            output.put("clusters", plan);

            fragmentStore.writeFragment(workDir, "_cluster_plan.json",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(output));
        } catch (IOException e) {
            log.debug("Failed to write cluster plan: {}", e.getMessage());
        }
    }
}
