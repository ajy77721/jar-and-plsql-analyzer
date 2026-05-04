package com.jaranalyzer.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CallNode {
    private String id;
    private String className;
    private String simpleClassName;
    private String methodName;
    private String returnType;
    private List<String> parameterTypes = new ArrayList<>();
    private String stereotype;
    private List<String> annotations = new ArrayList<>();
    private List<CallNode> children = new ArrayList<>();
    private boolean recursive;
    private String sourceJar;

    // Enriched analysis fields
    private String module;
    private String domain;
    private boolean crossModule;
    private List<String> collectionsAccessed = new ArrayList<>();
    private String operationType;
    private Set<String> operationTypes = new LinkedHashSet<>();
    private String callType;

    // Dispatch resolution metadata — how this node was resolved from bytecode invocation
    private String dispatchType;    // DIRECT | DYNAMIC_DISPATCH | QUALIFIED | HEURISTIC | INTERFACE_FALLBACK
    private String resolvedFrom;    // FQN of interface/abstract originally invoked (null for DIRECT)
    private String qualifierHint;   // @Qualifier value or field name that caused narrowing

    // Full annotation data (name + attributes map) for each annotation on this method
    private List<Map<String, Object>> annotationDetails = new ArrayList<>();

    // String literals found in this method's bytecode (collection names, constants)
    private List<String> stringLiterals = new ArrayList<>();

    // Collection domain mapping (collection name -> domain)
    private Map<String, String> collectionDomains = new LinkedHashMap<>();

    // Collection detection source (collection name -> how it was detected)
    // Values: REPOSITORY_MAPPING, DOCUMENT_ANNOTATION, STRING_LITERAL, FIELD_CONSTANT, QUERY_ANNOTATION, PIPELINE_ANNOTATION
    private Map<String, String> collectionSources = new LinkedHashMap<>();

    // Lines of code for this method (endLine - startLine + 1, 0 if unknown)
    private int lineCount;

    // Actual decompiled Java source code for this method (from CFR).
    // This is REAL code, not synthetic — accurate analysis requires actual code at every level.
    private String sourceCode;

    // Entity field mappings for collections accessed by this node.
    // Map<collectionName, Map<javaFieldName, mongoFieldName>>
    // Only populated when the node accesses collections with @Field/@BsonProperty/@Id mappings.
    private Map<String, Map<String, String>> entityFieldMappings;

    // Verification status per collection from live MongoDB catalog.
    // Values: VERIFIED (exists in DB), NOT_IN_DB (detected but not in real DB), NO_CATALOG (catalog unavailable)
    private Map<String, String> collectionVerification;

    // True when this method invokes MongoTemplate/MongoOperations/native driver methods,
    // even if the target collection couldn't be resolved. Prevents cleanOperationTypes
    // from clearing a valid DB operation type on leaf nodes with unresolved collections.
    private boolean hasDbInteraction;

    // Spring Cache annotation metadata extracted from @Cacheable, @CacheEvict, @CachePut, @Caching.
    // Each entry: {type: "CACHEABLE"|"CACHE_EVICT"|"CACHE_PUT", cacheNames: [...], key: "...", ...}
    private List<Map<String, Object>> cacheOperations;

    // Rich aggregation pipeline metadata: ordered stages, lookup targets with fields,
    // match predicates, group accumulators, detection sources per stage.
    private Map<String, Object> aggregationPipeline;

    // SQL/JPQL statement text captured from JDBC Template, EntityManager, or @Query annotations.
    // Stored as list to handle methods that execute more than one query.
    private List<String> sqlStatements;

    public CallNode() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    public String getSimpleClassName() { return simpleClassName; }
    public void setSimpleClassName(String simpleClassName) { this.simpleClassName = simpleClassName; }
    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }
    public String getReturnType() { return returnType; }
    public void setReturnType(String returnType) { this.returnType = returnType; }
    public List<String> getParameterTypes() { return parameterTypes; }
    public void setParameterTypes(List<String> parameterTypes) { this.parameterTypes = parameterTypes; }
    public String getStereotype() { return stereotype; }
    public void setStereotype(String stereotype) { this.stereotype = stereotype; }
    public List<String> getAnnotations() { return annotations; }
    public void setAnnotations(List<String> annotations) { this.annotations = annotations; }
    public List<CallNode> getChildren() { return children; }
    public void setChildren(List<CallNode> children) { this.children = children; }
    public boolean isRecursive() { return recursive; }
    public void setRecursive(boolean recursive) { this.recursive = recursive; }
    public String getSourceJar() { return sourceJar; }
    public void setSourceJar(String sourceJar) { this.sourceJar = sourceJar; }

    public String getModule() { return module; }
    public void setModule(String module) { this.module = module; }
    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }
    public boolean isCrossModule() { return crossModule; }
    public void setCrossModule(boolean crossModule) { this.crossModule = crossModule; }
    public List<String> getCollectionsAccessed() { return collectionsAccessed; }
    public void setCollectionsAccessed(List<String> collectionsAccessed) { this.collectionsAccessed = collectionsAccessed; }
    public String getOperationType() { return operationType; }
    public void setOperationType(String operationType) { this.operationType = operationType; }
    public Set<String> getOperationTypes() { return operationTypes; }
    public void setOperationTypes(Set<String> operationTypes) { this.operationTypes = operationTypes; }
    public String getCallType() { return callType; }
    public void setCallType(String callType) { this.callType = callType; }
    public String getDispatchType() { return dispatchType; }
    public void setDispatchType(String dispatchType) { this.dispatchType = dispatchType; }
    public String getResolvedFrom() { return resolvedFrom; }
    public void setResolvedFrom(String resolvedFrom) { this.resolvedFrom = resolvedFrom; }
    public String getQualifierHint() { return qualifierHint; }
    public void setQualifierHint(String qualifierHint) { this.qualifierHint = qualifierHint; }

    public List<Map<String, Object>> getAnnotationDetails() { return annotationDetails; }
    public void setAnnotationDetails(List<Map<String, Object>> annotationDetails) { this.annotationDetails = annotationDetails; }
    public List<String> getStringLiterals() { return stringLiterals; }
    public void setStringLiterals(List<String> stringLiterals) { this.stringLiterals = stringLiterals; }
    public Map<String, String> getCollectionDomains() { return collectionDomains; }
    public void setCollectionDomains(Map<String, String> collectionDomains) { this.collectionDomains = collectionDomains; }
    public Map<String, String> getCollectionSources() { return collectionSources; }
    public void setCollectionSources(Map<String, String> collectionSources) { this.collectionSources = collectionSources; }
    public int getLineCount() { return lineCount; }
    public void setLineCount(int lineCount) { this.lineCount = lineCount; }
    public String getSourceCode() { return sourceCode; }
    public void setSourceCode(String sourceCode) { this.sourceCode = sourceCode; }
    public Map<String, Map<String, String>> getEntityFieldMappings() { return entityFieldMappings; }
    public void setEntityFieldMappings(Map<String, Map<String, String>> entityFieldMappings) { this.entityFieldMappings = entityFieldMappings; }
    public Map<String, String> getCollectionVerification() { return collectionVerification; }
    public void setCollectionVerification(Map<String, String> collectionVerification) { this.collectionVerification = collectionVerification; }
    public boolean isHasDbInteraction() { return hasDbInteraction; }
    public void setHasDbInteraction(boolean hasDbInteraction) { this.hasDbInteraction = hasDbInteraction; }
    public List<Map<String, Object>> getCacheOperations() { return cacheOperations; }
    public void setCacheOperations(List<Map<String, Object>> cacheOperations) { this.cacheOperations = cacheOperations; }
    public Map<String, Object> getAggregationPipeline() { return aggregationPipeline; }
    public void setAggregationPipeline(Map<String, Object> aggregationPipeline) { this.aggregationPipeline = aggregationPipeline; }
    public List<String> getSqlStatements() { return sqlStatements; }
    public void setSqlStatements(List<String> sqlStatements) { this.sqlStatements = sqlStatements; }

    /**
     * Clear all transient/heavy data from this node and its children.
     * Called after Claude processing to free RAM during long enrichment runs.
     */
    public void clearTransient() {
        this.sourceCode = null;
        this.stringLiterals = null;
        this.annotationDetails = null;
        this.entityFieldMappings = null;
        this.cacheOperations = null;
        this.aggregationPipeline = null;
        if (this.children != null) {
            for (CallNode child : this.children) {
                child.clearTransient();
            }
        }
    }
}
