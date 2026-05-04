package com.jaranalyzer.service;

import com.jaranalyzer.model.*;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Package-private index data structures used during call-graph construction.
 * Extracted from CallTreeBuilder to keep individual files focused.
 */
class CallGraphIndex {

    // ========== Indexed wrappers ==========

    /** DI injection metadata for a field: type, qualifier value, how it was injected,
     *  the generic element type for collection-typed fields, and any custom qualifier annotations. */
    record FieldDIInfo(String fieldName, String fieldType, String qualifierValue, String injectionType,
                       String collectionElementType, List<AnnotationData> customQualifiers) {}

    static class IndexedClass {
        final String fqn;
        final String simpleName;
        final String stereotype;
        final boolean isInterface;
        final boolean isAbstract;
        final boolean isEnum;
        final String superClass;
        final List<String> interfaces;
        final String sourceJar;
        final Map<String, IndexedMethod> methods = new HashMap<>();
        boolean isPrimary = false;

        /** Explicit bean name from @Service("name"), @Component("name"), etc. */
        String explicitBeanName;

        /** Custom qualifier annotations on this class (non-standard annotations with a value attribute).
         *  Used for cross-matching against injection-point custom qualifiers (e.g. @AnalysisType). */
        final List<AnnotationData> customQualifierAnnotations = new ArrayList<>();

        /** Known Spring/Jakarta/standard annotations — excluded from custom qualifier extraction. */
        private static final Set<String> STANDARD_DI_ANNOTATIONS = Set.of(
            "Autowired", "Inject", "Qualifier", "Named", "Resource", "Primary", "Value",
            "Component", "Service", "Repository", "Controller", "RestController", "Configuration",
            "Bean", "Scope", "Lazy", "Conditional", "ConditionalOnProperty", "ConditionalOnClass",
            "ConditionalOnMissingBean", "ConditionalOnBean", "Profile", "Import", "ImportResource",
            "PropertySource", "ComponentScan", "EnableAutoConfiguration", "SpringBootApplication",
            "SpringBootTest", "TestConfiguration", "MockBean", "SpyBean",
            "Transactional", "Async", "Cacheable", "CachePut", "CacheEvict",
            "EventListener", "TransactionalEventListener",
            "Document", "Entity", "Table", "Column", "Id", "Field", "BsonId", "BsonProperty",
            "JoinTable", "JoinColumn", "MappedSuperclass", "Embedded", "EmbeddedId",
            "RequestMapping", "GetMapping", "PostMapping", "PutMapping",
            "DeleteMapping", "PatchMapping", "PathVariable", "RequestBody", "ResponseBody",
            "RequestParam", "RequestHeader", "ModelAttribute", "SessionAttributes",
            "PostConstruct", "PreDestroy",
            "RabbitListener", "KafkaListener", "MessageMapping", "Scheduled",
            "FeignClient", "LoadBalanced", "Override", "SuppressWarnings",
            "Deprecated", "FunctionalInterface", "SafeVarargs"
        );

        /** Stereotype annotations that carry an explicit bean name via their value attribute. */
        private static final Set<String> STEREOTYPE_ANNOTATION_NAMES = Set.of(
            "Service", "Component", "Repository", "Controller", "RestController", "Named"
        );

        String documentCollection;
        /** JPA @Table name for @Entity classes (null if not JPA or no @Table annotation). */
        String jpaTableName;
        /** JPA @Table schema (null if not specified). */
        String jpaTableSchema;
        /** True if this class is annotated with @Entity (JPA). */
        boolean isJpaEntity;

        /** For repository classes: FQN of the entity type from generic signature (e.g. MongoRepository&lt;Entity, ID&gt;). */
        final String repositoryEntityType;
        final List<String> fieldConstants = new ArrayList<>();

        /** Java field name → effective MongoDB field name for @Document entity classes.
         *  Built from @Field, @BsonProperty, @JsonProperty, @Id annotations. */
        final Map<String, String> fieldMappings = new LinkedHashMap<>();

        /** Java field name → JPA column name for @Entity classes.
         *  Built from @Column annotations. */
        final Map<String, String> jpaColumnMappings = new LinkedHashMap<>();

        /** Table names from @JoinTable annotations on this entity. */
        final List<String> joinTableNames = new ArrayList<>();

        /** Property keys from @Value annotations that may hold collection names.
         *  e.g. "${mongo.collection.claims}" -> key is "mongo.collection.claims" */
        final List<String> valuePropertyKeys = new ArrayList<>();

        /** All string constant values from this class (for enum/constants class resolution). */
        final List<String> allStringConstants = new ArrayList<>();

        /**
         * SimpleJdbcCall field name → "PROC:procedureName" or "FUNC:functionName".
         * Built from constructor invocations of withProcedureName/withFunctionName so that
         * when wrapper methods call field.execute(), we can resolve the stored procedure name.
         */
        final Map<String, String> simpleJdbcCallProcedures = new LinkedHashMap<>();

        /**
         * Stored procedure name for classes that extend Spring's StoredProcedure directly.
         * Pattern: class extends StoredProcedure and calls super(ds, "PKG.PROC_NAME") in constructor.
         * Populated during indexClass() from the INVOKESPECIAL to the superclass constructor.
         */
        String storedProcedureName;

        /** DI-injected fields: field name → injection metadata (type, @Qualifier, injection pattern).
         *  Built from all 4 injection patterns: field, constructor, setter, method. */
        final Map<String, FieldDIInfo> injectedFields = new LinkedHashMap<>();

        IndexedClass(ClassInfo ci, Pattern collPattern) {
            this.fqn = ci.getFullyQualifiedName();
            this.simpleName = ci.getSimpleName();
            this.stereotype = ci.getStereotype();
            this.isInterface = ci.getIsInterface();
            this.isAbstract = ci.getIsAbstract();
            this.isEnum = ci.getIsEnum();
            this.superClass = ci.getSuperClass();
            this.interfaces = ci.getInterfaces();
            this.sourceJar = ci.getSourceJar();
            this.repositoryEntityType = ci.getRepositoryEntityType();

            boolean hasDocAnnotation = false;
            for (AnnotationInfo ann : ci.getAnnotations()) {
                if ("Document".equals(ann.getName()) || "Collection".equals(ann.getName())) {
                    hasDocAnnotation = true;
                    Object coll = ann.getAttributes().get("collection");
                    if (coll == null) coll = ann.getAttributes().get("value");
                    // Skip SpEL expressions like #{@bean.collectionName} — cannot resolve statically
                    if (coll instanceof String s && !s.isBlank() && !s.contains("#{")) {
                        this.documentCollection = s;
                    }
                }
                if ("Primary".equals(ann.getName())) {
                    this.isPrimary = true;
                }
                // Explicit stereotype bean name: @Service("myBean") etc.
                if (STEREOTYPE_ANNOTATION_NAMES.contains(ann.getName())) {
                    Object val = ann.getAttributes().get("value");
                    if (val instanceof String s && !s.isBlank()) {
                        this.explicitBeanName = s;
                    }
                }
                // Custom qualifier annotation: any non-standard annotation with a value on the class
                if (!STANDARD_DI_ANNOTATIONS.contains(ann.getName()) && !ann.getAttributes().isEmpty()) {
                    Object val = ann.getAttributes().get("value");
                    if (val instanceof String s && !s.isBlank()) {
                        customQualifierAnnotations.add(new AnnotationData(ann.getName(), ann.getAttributes()));
                    }
                }
            }
            // @Document without explicit collection → Spring Data defaults to lcfirst(simpleName)
            if (hasDocAnnotation && this.documentCollection == null && this.simpleName != null && !this.simpleName.isEmpty()) {
                this.documentCollection = Character.toLowerCase(this.simpleName.charAt(0)) + this.simpleName.substring(1);
            }
            // JPA @Entity / @Table detection
            for (AnnotationInfo ann : ci.getAnnotations()) {
                if ("Entity".equals(ann.getName()) && !hasDocAnnotation) {
                    this.isJpaEntity = true;
                }
                if ("Table".equals(ann.getName())) {
                    Object tName = ann.getAttributes().get("name");
                    if (tName instanceof String s && !s.isBlank()) this.jpaTableName = s;
                    Object tSchema = ann.getAttributes().get("schema");
                    if (tSchema instanceof String s && !s.isBlank()) this.jpaTableSchema = s;
                }
            }
            // @Entity without @Table → JPA defaults to class simple name (unquoted)
            if (this.isJpaEntity && this.jpaTableName == null && this.simpleName != null) {
                this.jpaTableName = this.simpleName;
            }
            // Build field-name lookup for Passes 2 and 3 (collectionElementType propagation)
            Map<String, FieldInfo> fieldInfoByName = new HashMap<>();
            for (FieldInfo fi : ci.getFields()) fieldInfoByName.put(fi.getName(), fi);

            for (FieldInfo fi : ci.getFields()) {
                if (fi.getConstantValue() != null) {
                    allStringConstants.add(fi.getConstantValue());
                    if (fi.getConstantValue().contains("_")
                            && collPattern.matcher(fi.getConstantValue()).matches()) {
                        fieldConstants.add(fi.getConstantValue());
                    }
                }
                // Build Java→MongoDB field name mapping
                String effectiveName = fi.effectiveMongoName();
                if (effectiveName != null && !effectiveName.equals(fi.getName())) {
                    fieldMappings.put(fi.getName(), effectiveName);
                }
                // Build Java→JPA column name mapping
                String jpaCol = fi.effectiveJpaColumnName();
                if (jpaCol != null && !jpaCol.equals(fi.getName())) {
                    jpaColumnMappings.put(fi.getName(), jpaCol);
                }
                // Capture @JoinTable names
                for (AnnotationInfo ann : fi.getAnnotations()) {
                    if ("JoinTable".equals(ann.getName())) {
                        Object jtName = ann.getAttributes().get("name");
                        if (jtName instanceof String s && !s.isBlank()) joinTableNames.add(s);
                    }
                }
                // Capture @Value property keys (e.g. "${mongo.collection.claims}")
                for (AnnotationInfo ann : fi.getAnnotations()) {
                    if ("Value".equals(ann.getName())) {
                        Object val = ann.getAttributes().get("value");
                        if (val instanceof String s && s.contains("${")) {
                            int start = s.indexOf("${") + 2;
                            int end = s.indexOf("}", start);
                            if (end > start) {
                                String key = s.substring(start, end);
                                // Handle default values: ${key:default}
                                int colon = key.indexOf(':');
                                if (colon > 0) key = key.substring(0, colon);
                                valuePropertyKeys.add(key);
                            }
                        }
                    }
                }
                // Pass 1: Field Injection — @Autowired/@Inject + @Qualifier/@Named/@Resource on field
                boolean isInjected = false;
                String fieldQualifier = null;
                for (AnnotationInfo ann : fi.getAnnotations()) {
                    String aName = ann.getName();
                    if ("Autowired".equals(aName) || "Inject".equals(aName)) isInjected = true;
                    if ("Qualifier".equals(aName) || "Named".equals(aName)) {
                        Object qv = ann.getAttributes().get("value");
                        if (qv instanceof String s && !s.isBlank()) fieldQualifier = s;
                    }
                    if ("Resource".equals(aName)) {
                        isInjected = true;
                        Object nv = ann.getAttributes().get("name");
                        if (nv instanceof String s && !s.isBlank()) fieldQualifier = s;
                    }
                }
                if (isInjected) {
                    List<AnnotationData> customQuals = extractCustomQualifiers(fi.getAnnotations());
                    String collElem = fi.getGenericElementType();
                    injectedFields.put(fi.getName(),
                            new FieldDIInfo(fi.getName(), fi.getType(), fieldQualifier, "FIELD",
                                    collElem, customQuals));
                }
            }

            // Pass 2: Constructor Injection — @Qualifier on constructor params, correlated to fields
            for (MethodInfo mi : ci.getMethods()) {
                if (!"<init>".equals(mi.getName())) continue;
                // Track which param indices have already been matched to prevent two fields
                // of the same type (e.g. two AnalysisStrategy fields) both mapping to the
                // same first matching parameter.
                Set<Integer> usedParamIndices = new HashSet<>();
                for (FieldAccessInfo fa : mi.getFieldAccesses()) {
                    if (!"PUT".equals(fa.getAccessType())) continue;
                    if (!fa.getOwnerClass().equals(this.fqn)) continue;
                    String fieldName = fa.getFieldName();
                    if (injectedFields.containsKey(fieldName)) continue;
                    // Prefer param whose name matches the field name; fall back to first
                    // unused param of the matching type. Never reuse an already-assigned index.
                    Integer bestIdx = null;
                    for (int idx = 0; idx < mi.getParameters().size(); idx++) {
                        if (usedParamIndices.contains(idx)) continue;
                        ParameterInfo param = mi.getParameters().get(idx);
                        if (!param.getType().equals(fa.getFieldType())) continue;
                        if (fieldName.equals(param.getName())) { bestIdx = idx; break; }
                        if (bestIdx == null) bestIdx = idx;
                    }
                    if (bestIdx != null) {
                        ParameterInfo param = mi.getParameters().get(bestIdx);
                        String q = extractQualifier(param.getAnnotations());
                        List<AnnotationData> pCustomQuals = extractCustomQualifiers(param.getAnnotations());
                        FieldInfo fi2 = fieldInfoByName.get(fieldName);
                        String collElem2 = fi2 != null ? fi2.getGenericElementType() : null;
                        injectedFields.put(fieldName,
                                new FieldDIInfo(fieldName, fa.getFieldType(), q, "CONSTRUCTOR",
                                        collElem2, pCustomQuals));
                        usedParamIndices.add(bestIdx);
                    }
                }
            }

            // Pass 3: Setter Injection — @Autowired on setXxx methods with @Qualifier on param
            for (MethodInfo mi : ci.getMethods()) {
                boolean hasAutowired = mi.getAnnotations().stream()
                        .anyMatch(a -> "Autowired".equals(a.getName()) || "Inject".equals(a.getName()));
                if (!hasAutowired || !mi.getName().startsWith("set") || mi.getName().length() < 4
                        || mi.getParameters().size() != 1) continue;
                ParameterInfo param = mi.getParameters().get(0);
                String q = extractQualifier(param.getAnnotations());
                List<AnnotationData> pCustomQuals = extractCustomQualifiers(param.getAnnotations());
                String fieldName = Character.toLowerCase(mi.getName().charAt(3)) + mi.getName().substring(4);
                if (!injectedFields.containsKey(fieldName)) {
                    FieldInfo fi2 = fieldInfoByName.get(fieldName);
                    String collElem2 = fi2 != null ? fi2.getGenericElementType() : null;
                    injectedFields.put(fieldName,
                            new FieldDIInfo(fieldName, param.getType(), q, "SETTER",
                                    collElem2, pCustomQuals));
                }
            }
        }

        private static String extractQualifier(List<AnnotationInfo> annotations) {
            for (AnnotationInfo ann : annotations) {
                String aName = ann.getName();
                if ("Qualifier".equals(aName) || "Named".equals(aName)) {
                    Object val = ann.getAttributes().get("value");
                    return val instanceof String s && !s.isBlank() ? s : null;
                }
                if ("Resource".equals(aName)) {
                    Object val = ann.getAttributes().get("name");
                    return val instanceof String s && !s.isBlank() ? s : null;
                }
            }
            return null;
        }

        /** Collect non-standard annotations with a value attribute — these are custom qualifiers
         *  (e.g. @AnalysisType("statistical")) that Spring resolves via meta-annotation matching. */
        private static List<AnnotationData> extractCustomQualifiers(List<AnnotationInfo> annotations) {
            List<AnnotationData> result = new ArrayList<>();
            for (AnnotationInfo ann : annotations) {
                if (!STANDARD_DI_ANNOTATIONS.contains(ann.getName())) {
                    Object val = ann.getAttributes().get("value");
                    if (val instanceof String s && !s.isBlank()) {
                        result.add(new AnnotationData(ann.getName(), ann.getAttributes()));
                    }
                }
            }
            return result;
        }
    }

    static class IndexedMethod {
        final String name;
        final String descriptor;
        final String returnType;
        final boolean isStatic;
        final String httpMethod;
        final String path;
        final List<String> paramTypes;
        final List<String> annotationNames;
        final List<InvocationRef> invocations;
        final List<String> stringLiterals;
        final List<AnnotationData> annotationDetailList;
        final Set<String> staticFieldRefOwners; // owner classes of GETSTATIC operations
        final int startLine;
        final int endLine;

        IndexedMethod(MethodInfo mi) {
            this.name = mi.getName();
            this.descriptor = mi.getDescriptor();
            this.httpMethod = mi.getHttpMethod();
            this.path = mi.getPath();
            this.isStatic = (mi.getAccessFlags() & 0x0008) != 0; // ACC_STATIC
            this.returnType = mi.getReturnType();
            this.startLine = mi.getStartLine();
            this.endLine = mi.getEndLine();
            this.paramTypes = new ArrayList<>();
            for (ParameterInfo p : mi.getParameters()) paramTypes.add(p.getType());

            this.annotationNames = new ArrayList<>();
            this.annotationDetailList = new ArrayList<>();
            for (AnnotationInfo a : mi.getAnnotations()) {
                annotationNames.add("@" + a.getName());
                annotationDetailList.add(new AnnotationData(a.getName(), a.getAttributes()));
            }

            this.invocations = new ArrayList<>();
            for (MethodCallInfo mci : mi.getInvocations()) {
                invocations.add(new InvocationRef(mci.getOwnerClass(), mci.getMethodName(),
                        mci.getDescriptor(), mci.getReturnType(),
                        mci.getRecentStringArgs() != null ? mci.getRecentStringArgs() : List.of(),
                        mci.getOpcode(), mci.getReceiverFieldName(),
                        mci.isLambdaInvocation(), mci.getLambdaTargetClass(),
                        mci.getLambdaTargetMethod(), mci.getLambdaTargetDescriptor()));
            }
            this.stringLiterals = mi.getStringLiterals() != null ? new ArrayList<>(mi.getStringLiterals()) : List.of();

            // Collect owner classes from GETSTATIC field accesses (for external constant resolution)
            this.staticFieldRefOwners = new HashSet<>();
            if (mi.getFieldAccesses() != null) {
                for (FieldAccessInfo fa : mi.getFieldAccesses()) {
                    if ("GET_STATIC".equals(fa.getAccessType()) && fa.getOwnerClass() != null) {
                        staticFieldRefOwners.add(fa.getOwnerClass());
                    }
                }
            }
        }
    }

    record AnnotationData(String name, Map<String, Object> attributes) {}
    record InvocationRef(String ownerClass, String methodName, String descriptor, String returnType,
                         List<String> stringArgs, int opcode, String receiverFieldName,
                         boolean isLambdaInvocation, String lambdaTargetClass,
                         String lambdaTargetMethod, String lambdaTargetDescriptor) {
        /** Backwards-compatible constructor for non-lambda invocations. */
        InvocationRef(String ownerClass, String methodName, String descriptor, String returnType,
                      List<String> stringArgs, int opcode, String receiverFieldName) {
            this(ownerClass, methodName, descriptor, returnType, stringArgs, opcode,
                 receiverFieldName, false, null, null, null);
        }
    }

    /** Reference to a method annotated with @EventListener or @TransactionalEventListener. */
    record EventListenerRef(IndexedClass cls, IndexedMethod method, boolean isTransactional) {}

    static class IndexedEndpoint {
        final String httpMethod;
        final String path;
        final String controllerFqn;
        final String controllerSimpleName;
        final String methodName;
        final String returnType;
        final List<ParameterInfo> parameters;
        final String methodKey;

        IndexedEndpoint(MethodInfo mi, ClassInfo cls) {
            this.httpMethod = mi.getHttpMethod();
            this.path = mi.getPath() != null ? mi.getPath() : "";
            this.controllerFqn = cls.getFullyQualifiedName();
            this.controllerSimpleName = cls.getSimpleName();
            this.methodName = mi.getName();
            this.returnType = mi.getReturnType();
            this.parameters = mi.getParameters();
            this.methodKey = mi.getName() + mi.getDescriptor();
        }

        IndexedEndpoint(String httpMethod, String path, IndexedClass ic, IndexedMethod im) {
            this.httpMethod = httpMethod;
            this.path = path != null ? path : "";
            this.controllerFqn = ic.fqn;
            this.controllerSimpleName = ic.simpleName;
            this.methodName = im.name;
            this.returnType = im.returnType;
            this.parameters = List.of();
            this.methodKey = (im.isStatic ? "S:" : "") + im.name + im.descriptor;
        }
    }

    // ========== Resolution context (mutable state during a single analysis run) ==========

    static class ResolutionContext {
        final Map<String, IndexedClass> classMap = new HashMap<>();
        final Map<String, List<String>> interfaceImplMap = new HashMap<>();
        final Map<String, String> superClassMap = new HashMap<>();
        final List<IndexedEndpoint> endpoints = new ArrayList<>();
        final Map<String, String> entityCollectionMap = new HashMap<>();
        final Map<String, String> repoCollectionMap = new HashMap<>();

        /** JPA @Entity FQN/simpleName → table name (from @Table or class name). */
        final Map<String, String> entityTableMap = new HashMap<>();
        /** JPA Repository FQN/simpleName → table name (resolved via entity type). */
        final Map<String, String> repoTableMap = new HashMap<>();
        /** JAR filename → POM artifactId (set during parse phase) */
        Map<String, String> jarArtifactMap = Map.of();

        /** Collection name → entity field mappings (Java name → MongoDB name).
         *  Built from @Document entities' @Field/@BsonProperty/@Id annotations.
         *  Used to confirm field names in $lookup/pipeline resolution. */
        final Map<String, Map<String, String>> collectionFieldMappings = new HashMap<>();

        /** All known MongoDB field names from @Field/@BsonProperty annotations.
         *  These are NOT collection names — used to filter false positives
         *  when UPPER_CASE field names like PART_ID look like collection names. */
        final Set<String> knownEntityFieldNames = new HashSet<>();

        /** Real MongoDB collection/view names from live catalog (null if no catalog available).
         *  Used to verify detected collections: VERIFIED if in set, NOT_IN_DB if absent. */
        Set<String> mongoCatalogCollections;

        /** Auto-detected base packages (e.g. "com.allianz") for cross-JAR class resolution.
         *  Classes sharing these prefixes are treated as application classes even if not indexed. */
        List<String> detectedBasePackages;

        /** Properties extracted from application config files (key->value).
         *  Used to resolve @Value("${key}") property placeholders to collection names. */
        Map<String, String> configProperties = Map.of();

        /** Enum/constants class FQN -> all string constant values.
         *  Used to resolve GETSTATIC on enum fields to their actual values. */
        final Map<String, List<String>> enumConstantsMap = new HashMap<>();

        /** @Bean method name → concrete implementation FQN.
         *  Built from @Configuration classes scanning @Bean factory methods. */
        final Map<String, String> beanNameToImplMap = new HashMap<>();

        /** Reverse superclass index: parent FQN → list of child FQNs.
         *  Built once after indexing, used by resolveAllMethods and isApplicationClass. */
        final Map<String, List<String>> childClassMap = new HashMap<>();

        /** Event class FQN → list of @EventListener/@TransactionalEventListener methods.
         *  Built during indexClass() from method annotations. Used to connect
         *  publishEvent() calls to their listener methods in call tree building. */
        final Map<String, List<EventListenerRef>> eventListenerMap = new HashMap<>();

        /** Named query name → JPQL/SQL text (from @NamedQuery / @NamedNativeQuery on @Entity classes).
         *  Populated during indexClass(); used to resolve createNamedQuery("name") calls. */
        final Map<String, String> namedQueryMap = new HashMap<>();

        /** Explicit stereotype bean name → concrete class FQN.
         *  Built from @Service("name"), @Component("name"), @Repository("name"), etc.
         *  Complements beanNameToImplMap (which only covers @Bean factory methods). */
        final Map<String, String> stereotypeBeanNameMap = new HashMap<>();

        /** Class FQN → custom qualifier annotations declared on the class.
         *  Used to cross-match injection-point custom qualifiers (e.g. @AnalysisType) with
         *  the matching bean class that carries the same annotation and value. */
        final Map<String, List<AnnotationData>> classCustomQualifiers = new HashMap<>();
    }
}
