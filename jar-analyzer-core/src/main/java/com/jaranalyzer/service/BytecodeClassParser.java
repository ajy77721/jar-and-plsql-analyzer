package com.jaranalyzer.service;

import com.jaranalyzer.model.*;
import com.jaranalyzer.util.SpringAnnotations;
import com.jaranalyzer.util.TypeUtils;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.*;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Stateless bytecode parser: accepts raw class bytes and returns a ClassInfo.
 * All annotation, endpoint, and path helpers live here.
 */
@Component
public class BytecodeClassParser {

    public ClassInfo parseClass(byte[] bytecode) {
        try {
            org.objectweb.asm.ClassReader reader = new org.objectweb.asm.ClassReader(bytecode);
            ClassNode classNode = new ClassNode();
            reader.accept(classNode, 0);

            if ((classNode.access & Opcodes.ACC_SYNTHETIC) != 0) return null;

            ClassInfo info = new ClassInfo();
            String fqn = classNode.name.replace('/', '.');

            info.setFullyQualifiedName(fqn);
            int lastDot = fqn.lastIndexOf('.');
            info.setPackageName(lastDot > 0 ? fqn.substring(0, lastDot) : "");
            info.setSimpleName(lastDot > 0 ? fqn.substring(lastDot + 1) : fqn);
            info.setSuperClass(classNode.superName != null ? classNode.superName.replace('/', '.') : null);
            info.setAccessFlags(classNode.access);
            info.setIsInterface((classNode.access & Opcodes.ACC_INTERFACE) != 0);
            info.setIsAbstract((classNode.access & Opcodes.ACC_ABSTRACT) != 0);
            info.setIsEnum((classNode.access & Opcodes.ACC_ENUM) != 0);

            if (classNode.interfaces != null) {
                for (String iface : classNode.interfaces) {
                    info.getInterfaces().add(iface.replace('/', '.'));
                }
            }

            List<AnnotationInfo> classAnnotations = parseAnnotationNodes(classNode.visibleAnnotations);
            info.setAnnotations(classAnnotations);
            info.setStereotype(determineStereotype(classAnnotations, info.getInterfaces()));

            // Extract entity type from generic signature for repository interfaces
            // e.g. MongoRepository<ClaimEntity, String> → "com.example.model.ClaimEntity"
            if ("REPOSITORY".equals(info.getStereotype()) && classNode.signature != null) {
                String entityType = extractRepositoryEntityType(classNode.signature, info.getInterfaces());
                if (entityType != null) info.setRepositoryEntityType(entityType);
            }

            String basePath = extractBasePath(classNode.visibleAnnotations);

            // Fields
            if (classNode.fields != null) {
                for (FieldNode field : classNode.fields) {
                    if ((field.access & Opcodes.ACC_SYNTHETIC) != 0) continue;
                    FieldInfo fi = new FieldInfo();
                    fi.setName(field.name);
                    fi.setType(TypeUtils.cleanTypeName(Type.getType(field.desc).getClassName()));
                    fi.setAccessFlags(field.access);
                    fi.setAnnotations(parseAnnotationNodes(field.visibleAnnotations));
                    // Extract generic element type for collection fields (List<T>, Set<T>, Map<K,V>)
                    if (field.signature != null) {
                        String elem = extractCollectionElementType(field.desc, field.signature);
                        if (elem != null) fi.setGenericElementType(elem);
                    }
                    // Capture static final String constant values (collection names, etc.)
                    if (field.value instanceof String) {
                        fi.setConstantValue((String) field.value);
                    }
                    // Extract MongoDB field name from @Field, @BsonProperty, @JsonProperty
                    extractMongoFieldName(fi);
                    // Extract JPA column name from @Column
                    extractJpaColumnName(fi);
                    info.getFields().add(fi);
                }
            }

            // Methods
            if (classNode.methods != null) {
                for (MethodNode method : classNode.methods) {
                    if ("<clinit>".equals(method.name)) continue;
                    if ((method.access & Opcodes.ACC_BRIDGE) != 0) continue;

                    MethodInfo mi = new MethodInfo();
                    mi.setName(method.name);
                    mi.setDescriptor(method.desc);
                    mi.setAccessFlags(method.access);
                    mi.setReturnType(TypeUtils.cleanTypeName(Type.getReturnType(method.desc).getClassName()));

                    Type[] paramTypes = Type.getArgumentTypes(method.desc);
                    List<ParameterInfo> params = new ArrayList<>();
                    for (int i = 0; i < paramTypes.length; i++) {
                        ParameterInfo pi = new ParameterInfo();
                        pi.setType(TypeUtils.cleanTypeName(paramTypes[i].getClassName()));
                        pi.setName("arg" + i);
                        params.add(pi);
                    }

                    if (method.localVariables != null) {
                        int slotIndex = (method.access & Opcodes.ACC_STATIC) != 0 ? 0 : 1;
                        for (int i = 0; i < paramTypes.length && i < params.size(); i++) {
                            int slot = slotIndex;
                            for (LocalVariableNode lv : method.localVariables) {
                                if (lv.index == slot) {
                                    params.get(i).setName(lv.name);
                                    break;
                                }
                            }
                            slotIndex += paramTypes[i].getSize();
                        }
                    }

                    if (method.visibleParameterAnnotations != null) {
                        for (int i = 0; i < method.visibleParameterAnnotations.length && i < params.size(); i++) {
                            List<AnnotationNode> paramAnns = method.visibleParameterAnnotations[i];
                            if (paramAnns != null) {
                                params.get(i).setAnnotations(parseAnnotationNodes(paramAnns));
                            }
                        }
                    }

                    mi.setParameters(params);
                    mi.setAnnotations(parseAnnotationNodes(method.visibleAnnotations));
                    extractEndpointInfo(mi, basePath);

                    if (method.instructions != null) {
                        List<String> stringLits = new ArrayList<>();
                        List<FieldAccessInfo> fieldAccesses = new ArrayList<>();
                        // Track GETFIELD on 'this' to link interface calls to their DI-injected field
                        String pendingReceiverField = null;
                        // Declared type of the pending receiver field (used to prevent builder-chain
                        // INVOKEVIRTUAL calls from consuming the pending receiver prematurely)
                        String pendingReceiverType = null;
                        // Rolling window of LDC strings between consecutive method calls.
                        // Attached to each MethodCallInfo so we can correlate string args
                        // with specific invocations (e.g. BulkWriteCollector.addInsert("COLL", ...))
                        List<String> pendingStringArgs = new ArrayList<>();
                        int currentLine = 0;
                        int minLine = Integer.MAX_VALUE, maxLine = 0;

                        for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null; insn = insn.getNext()) {
                            // Track current line number from LineNumberNode
                            if (insn instanceof LineNumberNode lineNode) {
                                currentLine = lineNode.line;
                                if (currentLine < minLine) minLine = currentLine;
                                if (currentLine > maxLine) maxLine = currentLine;
                                continue;
                            }

                            // Capture LDC constants (strings and class literals)
                            if (insn instanceof LdcInsnNode ldc) {
                                if (ldc.cst instanceof String s) {
                                    // Collection-like strings (UPPER_CASE) go to the method's stringLiterals
                                    if (s.length() >= 4 && s.length() <= 60
                                            && s.matches("[A-Z][A-Z0-9_]+")) {
                                        stringLits.add(s);
                                    }
                                    // SQL strings (any SQL keyword prefix) also go to stringLiterals for fallback scanning
                                    if (s.length() >= 6) {
                                        String up = s.stripLeading().toUpperCase();
                                        if (up.startsWith("SELECT ") || up.startsWith("INSERT ")
                                                || up.startsWith("UPDATE ") || up.startsWith("DELETE ")
                                                || up.startsWith("MERGE ") || up.startsWith("WITH ")
                                                || up.startsWith("BEGIN ") || up.startsWith("CALL ")
                                                || up.startsWith("{CALL") || up.startsWith("EXEC ")) {
                                            stringLits.add(s);
                                        }
                                    }
                                    // All non-trivial strings go to the pending args window for next call
                                    // Limit raised to 8000 to capture multi-line SQL text blocks
                                    if (s.length() >= 2 && s.length() <= 8000) {
                                        pendingStringArgs.add(s);
                                    }
                                } else if (ldc.cst instanceof Type t && t.getSort() == Type.OBJECT) {
                                    // Class literal (Bean.class) → add FQN to pending args
                                    // Enables MongoMethodDetector to resolve mongoOps.updateMulti(q, u, Bean.class)
                                    String className = t.getClassName();
                                    if (className != null && className.contains(".")) {
                                        pendingStringArgs.add("__class:" + className);
                                    }
                                }
                            }

                            // Capture field accesses (GETFIELD, PUTFIELD, GETSTATIC, PUTSTATIC)
                            if (insn instanceof FieldInsnNode fieldInsn) {
                                FieldAccessInfo fai = new FieldAccessInfo();
                                fai.setOwnerClass(fieldInsn.owner.replace('/', '.'));
                                fai.setFieldName(fieldInsn.name);
                                fai.setFieldType(TypeUtils.cleanTypeName(Type.getType(fieldInsn.desc).getClassName()));
                                fai.setLineNumber(currentLine);
                                switch (fieldInsn.getOpcode()) {
                                    case Opcodes.GETFIELD -> fai.setAccessType("GET");
                                    case Opcodes.PUTFIELD -> fai.setAccessType("PUT");
                                    case Opcodes.GETSTATIC -> fai.setAccessType("GET_STATIC");
                                    case Opcodes.PUTSTATIC -> fai.setAccessType("PUT_STATIC");
                                }
                                fieldAccesses.add(fai);
                                // Track GETFIELD on 'this' as pending receiver for next interface/virtual call.
                                // Record the field's declared type so we can reject intermediate builder-chain
                                // INVOKEVIRTUAL calls (e.g. RequestBuilder.metric()) that are not the target.
                                // Only PUTFIELD / GETSTATIC / PUTSTATIC reset the pending state; a GETFIELD on a
                                // non-this object does NOT clear it (it just pushes another value to the stack
                                // as an argument and the original receiver is still underneath).
                                if (fieldInsn.getOpcode() == Opcodes.GETFIELD
                                        && fieldInsn.owner.replace('/', '.').equals(fqn)) {
                                    pendingReceiverField = fieldInsn.name;
                                    pendingReceiverType = Type.getType(fieldInsn.desc).getClassName().replace('/', '.');
                                } else if (fieldInsn.getOpcode() == Opcodes.PUTFIELD
                                        || fieldInsn.getOpcode() == Opcodes.GETSTATIC
                                        || fieldInsn.getOpcode() == Opcodes.PUTSTATIC) {
                                    pendingReceiverField = null;
                                    pendingReceiverType = null;
                                }
                                // GETFIELD on a non-this object: leave the pending receiver intact
                            }

                            if (insn instanceof MethodInsnNode call) {
                                MethodCallInfo mci = new MethodCallInfo();
                                mci.setOwnerClass(call.owner.replace('/', '.'));
                                mci.setMethodName(call.name);
                                mci.setDescriptor(call.desc);
                                mci.setReturnType(TypeUtils.cleanTypeName(Type.getReturnType(call.desc).getClassName()));
                                mci.setOpcode(call.getOpcode());
                                mci.setLineNumber(currentLine);
                                // Link interface/virtual calls to the DI-injected field loaded via GETFIELD.
                                // INVOKESPECIAL (opcode 183) is used for constructors (e.g. new MapSqlParameterSource())
                                // called as arguments to the field method — do NOT clear pendingReceiverField for these,
                                // so it survives to the actual INVOKEINTERFACE/INVOKEVIRTUAL on the field object.
                                // Only INVOKEINTERFACE / INVOKEVIRTUAL consume the field object from the stack
                                // and therefore clear the pending receiver.
                                // INVOKESPECIAL (<init>) and INVOKESTATIC are called to build arguments —
                                // they do not consume the field object, so the pending receiver survives.
                                boolean isVirtualOrInterface = call.getOpcode() == Opcodes.INVOKEINTERFACE
                                        || call.getOpcode() == Opcodes.INVOKEVIRTUAL;
                                if (pendingReceiverField != null && isVirtualOrInterface) {
                                    String callOwner = call.owner.replace('/', '.');
                                    // Only consume the pending receiver when the call's owner matches the
                                    // field's declared type. Builder-chain calls (e.g. RequestBuilder.metric())
                                    // have a different owner and must leave the pending receiver intact so it
                                    // survives to the actual target INVOKEINTERFACE/INVOKEVIRTUAL.
                                    if (pendingReceiverType == null || callOwner.equals(pendingReceiverType)) {
                                        mci.setReceiverFieldName(pendingReceiverField);
                                        pendingReceiverField = null;
                                        pendingReceiverType = null;
                                    }
                                    // else: type mismatch — builder-chain call; leave pending receiver intact
                                }
                                if (!pendingStringArgs.isEmpty()) {
                                    mci.setRecentStringArgs(new ArrayList<>(pendingStringArgs));
                                    pendingStringArgs.clear();
                                }
                                mi.getInvocations().add(mci);
                            } else if (insn instanceof InvokeDynamicInsnNode dynInsn) {
                                pendingReceiverField = null;
                                pendingReceiverType = null;
                                if (dynInsn.bsmArgs != null) {
                                    for (Object arg : dynInsn.bsmArgs) {
                                        if (arg instanceof Handle handle) {
                                            // Map ASM Handle tag to the corresponding invoke opcode
                                            // so CallTreeBuilder can build the correct method key
                                            int resolvedOpcode = switch (handle.getTag()) {
                                                case Opcodes.H_INVOKESTATIC    -> Opcodes.INVOKESTATIC;    // 184
                                                case Opcodes.H_INVOKEVIRTUAL   -> Opcodes.INVOKEVIRTUAL;   // 182
                                                case Opcodes.H_INVOKEINTERFACE -> Opcodes.INVOKEINTERFACE; // 185
                                                case Opcodes.H_INVOKESPECIAL,
                                                     Opcodes.H_NEWINVOKESPECIAL -> Opcodes.INVOKESPECIAL;  // 183
                                                default -> -1;
                                            };
                                            MethodCallInfo mci = new MethodCallInfo();
                                            mci.setOwnerClass(handle.getOwner().replace('/', '.'));
                                            mci.setMethodName(handle.getName());
                                            mci.setDescriptor(handle.getDesc());
                                            mci.setReturnType(TypeUtils.cleanTypeName(
                                                    Type.getReturnType(handle.getDesc()).getClassName()));
                                            mci.setOpcode(resolvedOpcode);
                                            mci.setLineNumber(currentLine);
                                            // Mark as lambda/method-reference invocation with target info
                                            mci.setLambdaInvocation(true);
                                            mci.setLambdaTargetClass(handle.getOwner().replace('/', '.'));
                                            mci.setLambdaTargetMethod(handle.getName());
                                            mci.setLambdaTargetDescriptor(handle.getDesc());
                                            if (!pendingStringArgs.isEmpty()) {
                                                mci.setRecentStringArgs(new ArrayList<>(pendingStringArgs));
                                                pendingStringArgs.clear();
                                            }
                                            mi.getInvocations().add(mci);
                                        }
                                    }
                                }
                                pendingStringArgs.clear();
                            }
                        }
                        mi.setStringLiterals(stringLits);
                        mi.setFieldAccesses(fieldAccesses);
                        if (minLine != Integer.MAX_VALUE) {
                            mi.setStartLine(minLine);
                            mi.setEndLine(maxLine);
                        }
                    }

                    // Capture local variable table (name -> type)
                    if (method.localVariables != null) {
                        Map<String, String> localVars = new LinkedHashMap<>();
                        boolean isStatic = (method.access & Opcodes.ACC_STATIC) != 0;
                        for (LocalVariableNode lv : method.localVariables) {
                            if (lv.index == 0 && !isStatic) continue; // skip 'this'
                            // Skip parameters (already captured in parameters list)
                            boolean isParam = false;
                            int slotIdx = isStatic ? 0 : 1;
                            Type[] ptypes = Type.getArgumentTypes(method.desc);
                            for (Type pt : ptypes) {
                                if (lv.index == slotIdx) { isParam = true; break; }
                                slotIdx += pt.getSize();
                            }
                            if (!isParam) {
                                localVars.put(lv.name, TypeUtils.cleanTypeName(Type.getType(lv.desc).getClassName()));
                            }
                        }
                        mi.setLocalVariables(localVars);
                    }

                    info.getMethods().add(mi);
                }
            }

            return info;
        } catch (Exception e) {
            return null;
        }
    }

    /* ---- annotation helpers ---- */

    private List<AnnotationInfo> parseAnnotationNodes(List<AnnotationNode> annotationNodes) {
        if (annotationNodes == null) return new ArrayList<>();
        List<AnnotationInfo> result = new ArrayList<>();
        for (AnnotationNode ann : annotationNodes) {
            AnnotationInfo ai = new AnnotationInfo();
            String readable = SpringAnnotations.resolve(ann.desc);
            if (readable != null) {
                ai.setName(readable);
            } else {
                String typeName = Type.getType(ann.desc).getClassName();
                ai.setName(typeName.substring(typeName.lastIndexOf('.') + 1));
            }
            if (ann.values != null) {
                for (int i = 0; i < ann.values.size(); i += 2) {
                    String key = (String) ann.values.get(i);
                    Object value = ann.values.get(i + 1);
                    ai.getAttributes().put(key, convertAnnotationValue(value));
                }
            }
            result.add(ai);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Object convertAnnotationValue(Object value) {
        if (value instanceof String[]) {
            String[] arr = (String[]) value;
            return arr.length > 1 ? arr[1] : Arrays.toString(arr);
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> result = new ArrayList<>();
            for (Object item : list) { result.add(convertAnnotationValue(item)); }
            return result;
        }
        if (value instanceof AnnotationNode nested) {
            Map<String, Object> map = new LinkedHashMap<>();
            String typeName = Type.getType(nested.desc).getClassName();
            map.put("@type", typeName.substring(typeName.lastIndexOf('.') + 1));
            if (nested.values != null) {
                for (int i = 0; i < nested.values.size(); i += 2) {
                    map.put((String) nested.values.get(i), convertAnnotationValue(nested.values.get(i + 1)));
                }
            }
            return map;
        }
        if (value instanceof Type) { return TypeUtils.cleanTypeName(((Type) value).getClassName()); }
        return value;
    }

    private static final Set<String> SPRING_DATA_REPO_INTERFACES = Set.of(
            "org.springframework.data.repository.CrudRepository",
            "org.springframework.data.repository.PagingAndSortingRepository",
            "org.springframework.data.repository.reactive.ReactiveCrudRepository",
            "org.springframework.data.repository.reactive.ReactiveSortingRepository",
            "org.springframework.data.jpa.repository.JpaRepository",
            "org.springframework.data.mongodb.repository.MongoRepository",
            "org.springframework.data.mongodb.repository.ReactiveMongoRepository",
            "org.springframework.data.repository.Repository"
    );

    private String determineStereotype(List<AnnotationInfo> annotations, List<String> interfaces) {
        for (AnnotationInfo ann : annotations) {
            switch (ann.getName()) {
                case "RestController": return "REST_CONTROLLER";
                case "Controller": return "CONTROLLER";
                case "Service": return "SERVICE";
                case "Repository": return "REPOSITORY";
                case "Component": return "COMPONENT";
                case "Configuration": return "CONFIGURATION";
                case "Entity": return "ENTITY";
                case "Document": return "ENTITY";  // MongoDB @Document beans are data classes
                case "FeignClient": return "HTTP_CLIENT";  // declarative HTTP client interface
            }
        }
        // Detect Spring Data repository interfaces (often used without @Repository)
        if (interfaces != null) {
            for (String iface : interfaces) {
                if (SPRING_DATA_REPO_INTERFACES.contains(iface)
                        || iface.endsWith("Repository") || iface.endsWith("CrudRepository")) {
                    return "REPOSITORY";
                }
            }
        }
        return "OTHER";
    }

    private String extractBasePath(List<AnnotationNode> annotations) {
        if (annotations == null) return "";
        for (AnnotationNode ann : annotations) {
            String name = SpringAnnotations.resolve(ann.desc);
            if ("RequestMapping".equals(name)) return extractPathFromAnnotationNode(ann);
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private String extractPathFromAnnotationNode(AnnotationNode ann) {
        if (ann.values == null) return "";
        for (int i = 0; i < ann.values.size(); i += 2) {
            String key = (String) ann.values.get(i);
            if ("value".equals(key) || "path".equals(key)) {
                Object val = ann.values.get(i + 1);
                if (val instanceof List) {
                    List<?> list = (List<?>) val;
                    return list.isEmpty() ? "" : list.get(0).toString();
                }
                return val.toString();
            }
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private void extractEndpointInfo(MethodInfo method, String basePath) {
        for (AnnotationInfo ann : method.getAnnotations()) {
            String httpMethod = SpringAnnotations.httpMethodFor(ann.getName());
            if (httpMethod != null) {
                method.setHttpMethod(httpMethod);
                String path = extractPathFromAttributes(ann.getAttributes());
                method.setPath(normalizePath(basePath) + normalizePath(path));
                return;
            }
            if ("RequestMapping".equals(ann.getName())) {
                Object methodAttr = ann.getAttributes().get("method");
                String hm = "ALL";
                if (methodAttr instanceof List && !((List<?>) methodAttr).isEmpty()) {
                    hm = ((List<?>) methodAttr).get(0).toString();
                } else if (methodAttr instanceof String) { hm = (String) methodAttr; }
                method.setHttpMethod(hm);
                String path = extractPathFromAttributes(ann.getAttributes());
                method.setPath(normalizePath(basePath) + normalizePath(path));
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private String extractPathFromAttributes(Map<String, Object> attrs) {
        Object pathVal = attrs.get("value");
        if (pathVal == null) pathVal = attrs.get("path");
        if (pathVal instanceof List && !((List<?>) pathVal).isEmpty()) {
            return ((List<?>) pathVal).get(0).toString();
        } else if (pathVal instanceof String) { return (String) pathVal; }
        return "";
    }

    private String normalizePath(String path) {
        if (path == null || path.isEmpty()) return "";
        if (!path.startsWith("/")) path = "/" + path;
        if (path.endsWith("/") && path.length() > 1) path = path.substring(0, path.length() - 1);
        return path;
    }

    /**
     * Extract MongoDB field name from field-level annotations.
     * Priority: @Field > @BsonProperty > @JsonProperty (first non-blank wins).
     * Also detects @Id / @BsonId for _id mapping.
     */
    private void extractMongoFieldName(FieldInfo fi) {
        // Annotations that map Java field name → MongoDB field name
        // @Field("PART_ID"), @Field(name="PART_ID"), @Field(value="PART_ID")
        // @BsonProperty("_id"), @BsonProperty(value="_id")
        // @JsonProperty("access_token"), @JsonProperty(value="access_token")
        String fieldName = null;
        boolean isId = false;

        for (AnnotationInfo ann : fi.getAnnotations()) {
            String name = ann.getName();
            Map<String, Object> attrs = ann.getAttributes();

            switch (name) {
                case "Field" -> {
                    // Spring Data MongoDB: @Field("name") or @Field(name="name") or @Field(value="name")
                    String val = extractStringAttr(attrs, "value");
                    if (val == null) val = extractStringAttr(attrs, "name");
                    if (val != null && !val.isBlank() && fieldName == null) fieldName = val;
                }
                case "BsonProperty" -> {
                    String val = extractStringAttr(attrs, "value");
                    if (val != null && !val.isBlank() && fieldName == null) fieldName = val;
                }
                case "JsonProperty" -> {
                    String val = extractStringAttr(attrs, "value");
                    if (val != null && !val.isBlank() && fieldName == null) fieldName = val;
                }
                case "Id", "BsonId" -> isId = true;
                case "Indexed" -> {
                    // @Indexed doesn't change field name, but confirms it's a DB field
                }
            }
        }

        if (fieldName != null) fi.setMongoFieldName(fieldName);
        if (isId) fi.setIdField(true);
    }

    /**
     * Parses a bytecode field signature to extract the element/value type from collection generics.
     * Handles List&lt;T&gt;, Set&lt;T&gt;, Collection&lt;T&gt;, Map&lt;K,V&gt; (returns V), and ? extends T.
     * Returns null for non-collection types, raw types, java.* element types, or arrays.
     */
    private static String extractCollectionElementType(String desc, String signature) {
        String rawType;
        try { rawType = Type.getType(desc).getClassName(); } catch (Exception e) { return null; }
        boolean isList = rawType.equals("java.util.List") || rawType.equals("java.util.Set")
                || rawType.equals("java.util.Collection") || rawType.equals("java.util.Queue")
                || rawType.equals("java.util.Deque") || rawType.equals("java.util.SortedSet")
                || rawType.equals("java.util.NavigableSet") || rawType.equals("java.util.LinkedList")
                || rawType.equals("java.util.ArrayList") || rawType.equals("java.util.HashSet")
                || rawType.equals("java.util.LinkedHashSet") || rawType.equals("java.util.TreeSet");
        boolean isMap = rawType.equals("java.util.Map") || rawType.equals("java.util.SortedMap")
                || rawType.equals("java.util.NavigableMap") || rawType.equals("java.util.LinkedHashMap")
                || rawType.equals("java.util.HashMap") || rawType.equals("java.util.TreeMap");
        if (!isList && !isMap) return null;

        int ltPos = signature.indexOf('<');
        if (ltPos < 0) return null;
        int typeStart = ltPos + 1;

        if (isMap) {
            // Skip the key type (first type param) to get to the value type
            if (typeStart >= signature.length() || signature.charAt(typeStart) != 'L') return null;
            int firstEnd = signature.indexOf(';', typeStart);
            if (firstEnd < 0) return null;
            typeStart = firstEnd + 1;
        }

        // Skip wildcard indicators: + (? extends) or - (? super)
        if (typeStart < signature.length()
                && (signature.charAt(typeStart) == '+' || signature.charAt(typeStart) == '-')) {
            typeStart++;
        }
        if (typeStart >= signature.length() || signature.charAt(typeStart) != 'L') return null;
        typeStart++; // skip the 'L' object descriptor prefix
        int typeEnd = signature.indexOf(';', typeStart);
        if (typeEnd < 0) return null;
        String internal = signature.substring(typeStart, typeEnd);
        // Skip parameterised nested types (e.g. List<Map<K,V>>)
        if (internal.isEmpty() || internal.contains("<") || internal.contains(">")) return null;
        String fqn = internal.replace('/', '.');
        // Only return application types — skip java.*, javax.*, org.springframework.*
        if (fqn.startsWith("java.") || fqn.startsWith("javax.")
                || fqn.startsWith("org.springframework.") || fqn.startsWith("org.slf4j.")) return null;
        return fqn;
    }

    private String extractStringAttr(Map<String, Object> attrs, String key) {
        if (attrs == null) return null;
        Object val = attrs.get(key);
        if (val instanceof String s) return s;
        return null;
    }

    private void extractJpaColumnName(FieldInfo fi) {
        for (AnnotationInfo ann : fi.getAnnotations()) {
            if ("Column".equals(ann.getName())) {
                String val = extractStringAttr(ann.getAttributes(), "name");
                if (val != null && !val.isBlank()) {
                    fi.setJpaColumnName(val);
                    return;
                }
            }
            if ("JoinColumn".equals(ann.getName())) {
                String val = extractStringAttr(ann.getAttributes(), "name");
                if (val != null && !val.isBlank()) {
                    fi.setJpaColumnName(val);
                    return;
                }
            }
        }
    }

    // Repository interface FQNs whose first type parameter is the entity type
    private static final Set<String> REPO_INTERFACES_WITH_ENTITY = Set.of(
            "org.springframework.data.repository.CrudRepository",
            "org.springframework.data.repository.PagingAndSortingRepository",
            "org.springframework.data.repository.reactive.ReactiveCrudRepository",
            "org.springframework.data.repository.reactive.ReactiveSortingRepository",
            "org.springframework.data.jpa.repository.JpaRepository",
            "org.springframework.data.mongodb.repository.MongoRepository",
            "org.springframework.data.mongodb.repository.ReactiveMongoRepository",
            "org.springframework.data.repository.Repository"
    );

    /**
     * Parse generic signature to extract the entity type from MongoRepository&lt;Entity, ID&gt;.
     * ASM signature format: "Ljava/lang/Object;Lorg/springframework/data/.../MongoRepository&lt;Lcom/example/Entity;Ljava/lang/String;&gt;;"
     * We look for a known repo interface in the signature, then extract the first type parameter.
     */
    private String extractRepositoryEntityType(String signature, List<String> interfaces) {
        if (signature == null) return null;

        for (String iface : interfaces) {
            if (!REPO_INTERFACES_WITH_ENTITY.contains(iface)
                    && !iface.endsWith("Repository") && !iface.endsWith("CrudRepository")) {
                continue;
            }
            // Convert interface FQN to internal format for signature matching
            String internalName = iface.replace('.', '/');
            // Look for "L<internalName><...;" pattern in signature
            int idx = signature.indexOf("L" + internalName + "<");
            if (idx < 0) continue;

            // Extract first type parameter: everything between first '<L' and next ';'
            int typeStart = idx + ("L" + internalName + "<L").length();
            if (typeStart >= signature.length()) continue;
            int typeEnd = signature.indexOf(';', typeStart);
            if (typeEnd < 0) continue;

            String entityInternal = signature.substring(typeStart, typeEnd);
            return entityInternal.replace('/', '.');
        }
        return null;
    }
}
