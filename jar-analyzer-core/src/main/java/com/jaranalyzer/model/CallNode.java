package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * §273 follow-up — {@code CallTreeInflater} (jar-analyzer-core) writes some fields (e.g.
 * {@code childrenTruncated}/{@code omittedChildCount}) directly via a raw {@code JsonGenerator}
 * stream, bypassing this class entirely, so a field could exist in real, already-written JSON
 * without ever being declared here — and deserializing that JSON back into {@code CallNode}
 * (e.g. a Claude verification/correction scan reading a previously-inflated call tree) then fails
 * the WHOLE read with "Unrecognized field ...", not just that one field. Both currently-known
 * fields are now declared for real (see below), but {@code ignoreUnknown = true} is kept as a
 * backstop: a future field added the same streaming way, once, should degrade to "field silently
 * dropped on read" — recoverable — rather than "the entire scan fails outright," which is what
 * actually happened here in production before this fix.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
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
    private String jarName;
    private String sourcePath;

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

    /**
     * Real reflective invocations found in THIS method's actual bytecode — e.g.
     * "java.lang.Class.forName", "java.lang.reflect.Method.invoke". Populated by
     * CallTreeBuilder from real InvocationRef entries whose owner class/method name match known
     * reflection APIs, NOT by scanning string-literal constants for matching substrings (the
     * previous approach in AbstractAnalysisDataProvider, which both missed real reflection calls
     * that don't happen to share text with any string literal in the method, and falsely flagged
     * any string literal — e.g. a log message — that merely happened to contain matching text).
     */
    private List<String> reflectiveCalls = new ArrayList<>();

    /**
     * Real third-party library dependencies invoked from THIS method — e.g. "com.google.common
     * .collect.Lists". Audit #10: the existing "External Dependency" facet (crossModule/
     * httpTarget) only ever reports intra-repo cross-module calls and outbound HTTP, because
     * third-party classes are excluded from tree recursion before any external-dependency flag
     * could be set. Rather than repurpose that facet's existing name/consumers, this is a new,
     * additive field recording the actual excluded third-party classes a method calls — JDK-core
     * classes (java./javax./jdk./sun.) are deliberately NOT included here, since those aren't
     * "dependencies" in the sense a user cares about; only genuine third-party libraries are.
     */
    private List<String> thirdPartyDependencies = new ArrayList<>();

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

    // Dynamic (unresolvable-at-analysis-time) collection references — call sites where a
    // Mongo/BulkWriteCollector/aggregation API method was invoked with a VARIABLE (local var,
    // method param, instance field, or static constant) instead of a literal collection name.
    // NEVER merged into collectionsAccessed/collectionSources/collectionDomains — those three
    // fields must remain "resolved collection name" ground truth for aggregation/rollup/
    // verification. Each entry: {variableName, varKind, pattern, source, operationType}.
    private List<Map<String, Object>> dynamicCollectionRefs = new ArrayList<>();

    // Collection-lifecycle-workflow stage tags matched on THIS method's own resolved MongoDB
    // API invocations (e.g. "BLUE_GREEN_COLLECTION_SWAP:CREATE_TEMP"), set by
    // CollectionLifecycleDetector.detectStages() in CollectionResolver. Consumed by
    // CollectionLifecycleDetector.aggregate() to group scattered stage-tagged nodes across the
    // tree into workflow instances. Additive — never affects operationType/collectionsAccessed.
    private List<String> lifecycleStageTags = new ArrayList<>();

    // Spring Cache annotation metadata extracted from @Cacheable, @CacheEvict, @CachePut, @Caching.
    // Each entry: {type: "CACHEABLE"|"CACHE_EVICT"|"CACHE_PUT", cacheNames: [...], key: "...", ...}
    private List<Map<String, Object>> cacheOperations;

    // Rich aggregation pipeline metadata: ordered stages, lookup targets with fields,
    // match predicates, group accumulators, detection sources per stage.
    private Map<String, Object> aggregationPipeline;

    // SQL/JPQL statement text captured from JDBC Template, EntityManager, or @Query annotations.
    // Stored as list to handle methods that execute more than one query.
    private List<String> sqlStatements;

    // Oracle stored-procedure reference resolved from bytecode (withCatalogName/withProcedureName chain).
    // Format: "PROC:PKG_NAME.PROC_NAME" or "FUNC:PKG_NAME.FUNC_NAME".
    // Null when this node has no SimpleJdbcCall/StoredProcedure wiring.
    private String oracleProcRef;

    // Oracle schema resolved from withSchemaName() in the SimpleJdbcCall chain (e.g. "CUSTOMER").
    private String oracleSchema;

    // Set when this node's full children list exceeded calltree.max-children-per-node or the
    // tree exceeded calltree.max-nodes-per-tree while building this node's remaining children.
    // Points to a continuation fragment id (see EndpointInfo.fragments / FragmentBuilder) holding
    // the rest of the children so they are never silently dropped. Null when no overflow occurred.
    private String fragmentRef;

    // Number of additional children available in the fragment referenced by fragmentRef.
    // Null when fragmentRef is null.
    private Integer truncatedChildCount;

    // Companion flag `CallTreeInflater` streams alongside `omittedChildCount` (below) — written
    // via a raw JsonGenerator.writeBooleanField call that bypasses this class entirely, so it was
    // never declared here. Any later deserialization of that same JSON back into CallNode (e.g.
    // the Claude verification/correction scan) failed outright with "Unrecognized field
    // 'childrenTruncated'" the instant a truncated node appeared anywhere in the tree. Declared
    // for real now so round-tripping this class works, not just one-way streaming writes.
    private Boolean childrenTruncated;

    // Paired with `childrenTruncated` above by the same streaming writer (`CallTreeInflater`,
    // `gen.writeNumberField("omittedChildCount", omitted)`) — a DIFFERENT truncation mechanism
    // from `truncatedChildCount`/`fragmentRef` above (that pair is for the fragment-continuation
    // overflow path; this pair is for a collapsed-subtree count), also never declared here for
    // the same reason.
    private Integer omittedChildCount;

    // Resolved HTTP method (GET/POST/PUT/DELETE/PATCH) for HTTP_CALL/FEIGN leaf nodes — derived
    // from the actual RestTemplate/WebClient method invoked or the Feign interface method's
    // mapping annotation, NOT guessed from the calling Java method's name. Source of truth for
    // cross-JAR endpoint matching (see ExternalApiLinkService). Null for non-HTTP nodes.
    private String httpTargetMethod;

    // Resolved path/URL template for this specific HTTP call site (bound to the actual argument
    // passed at this invocation, not any string literal found anywhere in the calling method).
    // Null for non-HTTP nodes or when no path/URL argument could be resolved.
    private String httpTargetPath;

    // Best-effort target service/module identifier: @FeignClient name/url, RestTemplate/WebClient
    // base-URL host, or null if nothing could be inferred. Used as a tie-breaker when multiple
    // analyzed JARs expose the same HTTP method + path.
    private String httpTargetModuleHint;

    // CALL-010: provenance of the resolved httpTargetPath — "DIRECT_LITERAL" (same-call-site
    // literal, the original/control behavior), "CALL_SITE_PARAM_PROPAGATION" (single- or
    // bounded-multi-hop caller-argument resolution across ancestorCallChain, mirroring CALL-001/
    // CALL-002's Mongo/JDBC mechanisms), "CALL_SITE_CONCAT_PROPAGATION" (a `literal + param`/
    // `param + literal` string-concat fold, mirroring CALL-003), or "CALLER_METHOD_LITERAL_SCAN"
    // (the pre-existing best-effort fallback scan of the calling method's own string literals).
    // Null when httpTargetPath itself is null. Lets callers/reports distinguish a high-confidence
    // same-call-site literal from a traced/propagated value, the same way collectionSources
    // already does for Mongo/JDBC.
    private String httpTargetPathSource;

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
    public String getJarName() { return jarName; }
    public void setJarName(String jarName) { this.jarName = jarName; }
    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }

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
    public List<String> getReflectiveCalls() { return reflectiveCalls; }
    public void setReflectiveCalls(List<String> reflectiveCalls) { this.reflectiveCalls = reflectiveCalls; }
    public List<String> getThirdPartyDependencies() { return thirdPartyDependencies; }
    public void setThirdPartyDependencies(List<String> thirdPartyDependencies) { this.thirdPartyDependencies = thirdPartyDependencies; }
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
    public List<Map<String, Object>> getDynamicCollectionRefs() { return dynamicCollectionRefs; }
    public void setDynamicCollectionRefs(List<Map<String, Object>> dynamicCollectionRefs) { this.dynamicCollectionRefs = dynamicCollectionRefs; }
    public List<String> getLifecycleStageTags() { return lifecycleStageTags; }
    public void setLifecycleStageTags(List<String> lifecycleStageTags) { this.lifecycleStageTags = lifecycleStageTags; }
    public List<Map<String, Object>> getCacheOperations() { return cacheOperations; }
    public void setCacheOperations(List<Map<String, Object>> cacheOperations) { this.cacheOperations = cacheOperations; }
    public Map<String, Object> getAggregationPipeline() { return aggregationPipeline; }
    public void setAggregationPipeline(Map<String, Object> aggregationPipeline) { this.aggregationPipeline = aggregationPipeline; }
    public List<String> getSqlStatements() { return sqlStatements; }
    public void setSqlStatements(List<String> sqlStatements) { this.sqlStatements = sqlStatements; }
    public String getOracleProcRef() { return oracleProcRef; }
    public void setOracleProcRef(String oracleProcRef) { this.oracleProcRef = oracleProcRef; }
    public String getOracleSchema() { return oracleSchema; }
    public void setOracleSchema(String oracleSchema) { this.oracleSchema = oracleSchema; }
    public String getFragmentRef() { return fragmentRef; }
    public void setFragmentRef(String fragmentRef) { this.fragmentRef = fragmentRef; }
    public Integer getTruncatedChildCount() { return truncatedChildCount; }
    public void setTruncatedChildCount(Integer truncatedChildCount) { this.truncatedChildCount = truncatedChildCount; }
    public Boolean getChildrenTruncated() { return childrenTruncated; }
    public void setChildrenTruncated(Boolean childrenTruncated) { this.childrenTruncated = childrenTruncated; }
    public Integer getOmittedChildCount() { return omittedChildCount; }
    public void setOmittedChildCount(Integer omittedChildCount) { this.omittedChildCount = omittedChildCount; }
    public String getHttpTargetMethod() { return httpTargetMethod; }
    public void setHttpTargetMethod(String httpTargetMethod) { this.httpTargetMethod = httpTargetMethod; }
    public String getHttpTargetPath() { return httpTargetPath; }
    public void setHttpTargetPath(String httpTargetPath) { this.httpTargetPath = httpTargetPath; }
    public String getHttpTargetModuleHint() { return httpTargetModuleHint; }
    public void setHttpTargetModuleHint(String httpTargetModuleHint) { this.httpTargetModuleHint = httpTargetModuleHint; }
    public String getHttpTargetPathSource() { return httpTargetPathSource; }
    public void setHttpTargetPathSource(String httpTargetPathSource) { this.httpTargetPathSource = httpTargetPathSource; }

    /**
     * Shallow copy for call-site-specific tagging (see CollectionResolver.tagCallSiteCollection).
     * buildCallTree's memoization can return the SAME node instance to multiple unrelated call
     * sites (memo keyed by callee method identity, shared across every caller of that method) —
     * mutating that instance in place would corrupt the memo entry for every other caller. This
     * copy shares the immutable/structural fields (children, sourceCode, etc.) by reference but
     * gives collection-related fields fresh mutable copies, so a caller can tag ITS OWN call-site
     * collection onto the copy without affecting the original memoized node or other callers.
     */
    public CallNode copyForCallSiteTagging() {
        CallNode copy = new CallNode();
        copy.id = this.id;
        copy.className = this.className;
        copy.simpleClassName = this.simpleClassName;
        copy.methodName = this.methodName;
        copy.returnType = this.returnType;
        copy.parameterTypes = this.parameterTypes;
        copy.stereotype = this.stereotype;
        copy.annotations = this.annotations;
        copy.children = this.children;
        copy.recursive = this.recursive;
        copy.sourceJar = this.sourceJar;
        copy.jarName = this.jarName;
        copy.sourcePath = this.sourcePath;
        copy.module = this.module;
        copy.domain = this.domain;
        copy.crossModule = this.crossModule;
        copy.collectionsAccessed = new ArrayList<>(this.collectionsAccessed);
        copy.operationType = this.operationType;
        copy.operationTypes = new LinkedHashSet<>(this.operationTypes);
        copy.callType = this.callType;
        copy.dispatchType = this.dispatchType;
        copy.resolvedFrom = this.resolvedFrom;
        copy.qualifierHint = this.qualifierHint;
        copy.annotationDetails = this.annotationDetails;
        copy.stringLiterals = this.stringLiterals;
        copy.collectionDomains = new LinkedHashMap<>(this.collectionDomains);
        copy.collectionSources = new LinkedHashMap<>(this.collectionSources);
        copy.lineCount = this.lineCount;
        copy.sourceCode = this.sourceCode;
        copy.entityFieldMappings = this.entityFieldMappings;
        copy.collectionVerification = this.collectionVerification;
        copy.hasDbInteraction = this.hasDbInteraction;
        copy.dynamicCollectionRefs = new ArrayList<>(this.dynamicCollectionRefs);
        copy.lifecycleStageTags = this.lifecycleStageTags;
        copy.cacheOperations = this.cacheOperations;
        copy.aggregationPipeline = this.aggregationPipeline;
        copy.sqlStatements = this.sqlStatements;
        copy.oracleProcRef = this.oracleProcRef;
        copy.oracleSchema = this.oracleSchema;
        copy.fragmentRef = this.fragmentRef;
        copy.truncatedChildCount = this.truncatedChildCount;
        copy.childrenTruncated = this.childrenTruncated;
        copy.omittedChildCount = this.omittedChildCount;
        copy.httpTargetMethod = this.httpTargetMethod;
        copy.httpTargetPath = this.httpTargetPath;
        copy.httpTargetModuleHint = this.httpTargetModuleHint;
        copy.httpTargetPathSource = this.httpTargetPathSource;
        return copy;
    }

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
