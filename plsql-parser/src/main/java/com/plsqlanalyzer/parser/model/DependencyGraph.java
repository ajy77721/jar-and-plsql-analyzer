package com.plsqlanalyzer.parser.model;

import com.plsqlanalyzer.parser.service.OracleDictionaryService.DependencyRecord;

import java.util.*;

/**
 * Holds the result of recursive dependency traversal via Oracle ALL_DEPENDENCIES.
 * Maps each "OWNER.OBJECT_NAME" to its list of direct dependencies.
 */
public class DependencyGraph {

    private final Map<String, List<DependencyRecord>> adjacencyList;
    private final String rootKey;

    public DependencyGraph(String rootKey, Map<String, List<DependencyRecord>> adjacencyList) {
        this.rootKey = rootKey;
        this.adjacencyList = adjacencyList;
    }

    public String getRootKey() {
        return rootKey;
    }

    public Map<String, List<DependencyRecord>> getAdjacencyList() {
        return adjacencyList;
    }

    /**
     * Get all unique object keys (OWNER.NAME) in the graph.
     */
    public Set<String> getAllObjects() {
        Set<String> objects = new LinkedHashSet<>(adjacencyList.keySet());
        for (List<DependencyRecord> deps : adjacencyList.values()) {
            for (DependencyRecord dep : deps) {
                objects.add(dep.referencedOwner() + "." + dep.referencedName());
            }
        }
        return objects;
    }

    /**
     * Get all PL/SQL object keys (packages, procedures, functions, triggers).
     */
    public Set<String> getAllPLSQLObjects() {
        Set<String> objects = new LinkedHashSet<>();
        for (Map.Entry<String, List<DependencyRecord>> entry : adjacencyList.entrySet()) {
            objects.add(entry.getKey());
            for (DependencyRecord dep : entry.getValue()) {
                String type = dep.referencedType();
                if (type != null && (type.contains("PACKAGE") || type.equals("PROCEDURE")
                        || type.equals("FUNCTION") || type.equals("TRIGGER"))) {
                    objects.add(dep.referencedOwner() + "." + dep.referencedName());
                }
            }
        }
        return objects;
    }

    /**
     * Get all table/view keys referenced in the dependency graph.
     */
    public Set<String> getAllTableReferences() {
        Set<String> tables = new LinkedHashSet<>();
        for (List<DependencyRecord> deps : adjacencyList.values()) {
            for (DependencyRecord dep : deps) {
                if ("TABLE".equals(dep.referencedType()) || "VIEW".equals(dep.referencedType())) {
                    tables.add(dep.referencedOwner() + "." + dep.referencedName());
                }
            }
        }
        return tables;
    }

    /**
     * Get direct dependencies for a given object key.
     */
    public List<DependencyRecord> getDependencies(String objectKey) {
        return adjacencyList.getOrDefault(objectKey, Collections.emptyList());
    }

    public int size() {
        return adjacencyList.size();
    }
}
