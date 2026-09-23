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

    // Mirrors MongoMethodDetector.BULK_COLLECTOR_METHODS — duplicated here (rather than a shared
    // dependency) since this parser is intentionally stateless/dependency-free; both sets must be
    // kept in sync if BulkWriteCollector's buffering method names ever change.
    private static final Set<String> BULK_COLLECTOR_ADD_METHODS = Set.of("addInsert", "addUpdate", "addDelete");

    /**
     * CALL-003: the narrow, safe result of a two-operand string concatenation
     * (`literal + param` / `param + literal`) recorded for the local-variable slot that receives
     * it, so the live ALOAD-tracking loop below can report a richer dynamic-arg source instead of
     * a bare unresolved LOCAL_VAR. {@code literalPart} is the known text, {@code literalFirst}
     * records which side of the `+` it was on, and {@code paramName} is the OTHER (same-method
     * parameter) operand's name — resolved further up the call chain by CollectionResolver.
     */
    /**
     * CALL-008 (§209/§213 follow-on): {@code paramName2} is non-null exclusively for the
     * `param + param` two-operand shape (neither operand a same-method literal — e.g. §204
     * Pattern A's real fixture, {@code prefix + region} inside {@code RegionAppender.append},
     * where BOTH operands are still-unresolved parameters at this point). When non-null,
     * {@code literalPart}/{@code literalFirst} are unused (kept null/false) — the two operands
     * must each be resolved independently, one hop at a time, up each side's own ancestor chain;
     * see {@code CollectionResolver.foldTwoParamConcatIfPossible}.
     */
    private record ConcatSlotInfo(String literalPart, boolean literalFirst, String paramName, String paramName2) {
        ConcatSlotInfo(String literalPart, boolean literalFirst, String paramName) {
            this(literalPart, literalFirst, paramName, null);
        }
    }

    /** Marks an ALOAD-of-a-same-method-parameter operand pushed for a potential concatenation. */
    private record ParamOperand(String name) {}

    /** Sentinel for an operand push whose provenance the narrow CALL-003 scan does not track
     *  (e.g. ALOAD of a plain local, GETFIELD, a method-call return value) — occupies a window
     *  slot so a real literal/param pair from before it can never be mistaken for adjacency. */
    private static final Object NON_OPERAND = new Object();

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
            String pkg = lastDot > 0 ? fqn.substring(0, lastDot) : "";
            info.setPackageName(pkg);
            info.setSimpleName(lastDot > 0 ? fqn.substring(lastDot + 1) : fqn);
            info.setSuperClass(classNode.superName != null ? classNode.superName.replace('/', '.') : null);
            info.setAccessFlags(classNode.access);

            // Reconstruct source path from SourceFile bytecode attribute + package
            if (classNode.sourceFile != null && !classNode.sourceFile.isEmpty()) {
                String pkgPath = pkg.isEmpty() ? "" : pkg.replace('.', '/') + "/";
                info.setSourcePath("src/main/java/" + pkgPath + classNode.sourceFile);
            }
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

            // Same-class static final String constants, keyed by field name — used to resolve
            // GETSTATIC-loaded call arguments (e.g. bulkCollector.addInsert(SOME_CONSTANT, doc))
            // inline as if they were LDC literals, without needing the cross-class enumConstantsMap
            // resolution that CollectionResolver performs later for constants declared elsewhere.
            Map<String, String> classConstantsByFieldName = new HashMap<>();
            for (FieldInfo fi : info.getFields()) {
                if (fi.getConstantValue() != null) {
                    classConstantsByFieldName.put(fi.getName(), fi.getConstantValue());
                }
            }

            // §216/CALL-015: whole-class scan for INSTANCE fields that are, in practice, a single
            // constant string even though they are not `static final` in bytecode (so they don't
            // show up in classConstantsByFieldName above) — the common Spring
            // "private String baseUrl = "http://...";" / constructor-literal-assigned field idiom
            // this class's own concat-fold window scan (below, per-method) needs to treat a
            // `this.baseUrl + somePath` GETFIELD read as if it were a plain literal push, the same
            // way the pre-existing GETSTATIC same-class-constant override does a few lines below
            // (see bulkCollectorLiteralOverride's own GETSTATIC handling). Scoped deliberately
            // narrowly and safely per this task's own instructions:
            //   - only a PUTFIELD whose value was pushed by an LDC STRING *directly and adjacently*
            //     before it counts (mirrors this file's other adjacency-only literal tracking, e.g.
            //     the concatSlotValue window below) — `this.baseUrl = baseUrl;` (a constructor
            //     PARAMETER assignment, the real `@Value`-injected case) is correctly NEVER captured
            //     here, since no LDC precedes that PUTFIELD; such a field stays unresolved, exactly
            //     as required.
            //   - a field assigned MORE THAN ONE distinct literal value anywhere in the class (e.g.
            //     different literals in different constructor overloads), or EVER assigned from a
            //     non-literal source anywhere in the class, is excluded entirely — never guessed at.
            //   - scans every method in the class (constructors included), not just the method
            //     currently being parsed by the outer per-method loop, since the field's value may be
            //     set in a constructor while read in a completely different method.
            Map<String, String> fieldSingleLiteralValue = new HashMap<>();
            if (classNode.methods != null) {
                Map<String, Set<String>> literalsSeenByField = new HashMap<>();
                Set<String> fieldsSeenNonLiteral = new HashSet<>();
                for (MethodNode scanMethod : classNode.methods) {
                    String pendingLdcForField = null;
                    for (AbstractInsnNode scanInsn = scanMethod.instructions.getFirst(); scanInsn != null; scanInsn = scanInsn.getNext()) {
                        if (scanInsn instanceof LineNumberNode || scanInsn instanceof LabelNode || scanInsn instanceof FrameNode) {
                            continue; // pure positional bookkeeping — never breaks LDC/PUTFIELD adjacency
                        }
                        if (scanInsn instanceof LdcInsnNode scanLdc2 && scanLdc2.cst instanceof String s2) {
                            pendingLdcForField = s2;
                        } else if (scanInsn instanceof FieldInsnNode putFieldInsn
                                && putFieldInsn.getOpcode() == Opcodes.PUTFIELD
                                && putFieldInsn.owner.replace('/', '.').equals(fqn)) {
                            if (pendingLdcForField != null) {
                                literalsSeenByField.computeIfAbsent(putFieldInsn.name, k -> new HashSet<>()).add(pendingLdcForField);
                            } else {
                                fieldsSeenNonLiteral.add(putFieldInsn.name);
                            }
                            pendingLdcForField = null;
                        } else {
                            pendingLdcForField = null;
                        }
                    }
                }
                for (Map.Entry<String, Set<String>> e : literalsSeenByField.entrySet()) {
                    if (e.getValue().size() == 1 && !fieldsSeenNonLiteral.contains(e.getKey())) {
                        fieldSingleLiteralValue.put(e.getKey(), e.getValue().iterator().next());
                    }
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

                    // Map local-variable-table slot index -> parameter slot, so ALOAD tracking
                    // below can tell a method-parameter load (PARAM) apart from a true local
                    // variable load (LOCAL_VAR) using the exact same slot arithmetic as the
                    // parameter-naming loop above.
                    Set<Integer> paramSlots = new HashSet<>();
                    if (method.localVariables != null) {
                        int slotIndex = (method.access & Opcodes.ACC_STATIC) != 0 ? 0 : 1;
                        for (int i = 0; i < paramTypes.length && i < params.size(); i++) {
                            int slot = slotIndex;
                            paramSlots.add(slot);
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
                        // Stack of saved pendingStringArgs snapshots, pushed at each NEW (object
                        // construction start) and popped at the matching INVOKESPECIAL <init> call.
                        // Needed so a NESTED constructor's own LDC literals (e.g. the inner
                        // `new Document("into", targetCollectionName)` in
                        // `new Document("$merge", new Document("into", targetCollectionName))`)
                        // are scoped separately from literals already pending for the OUTER
                        // constructor call that hasn't been invoked yet (e.g. the outer's own
                        // "$merge" literal). Without this, the inner <init> call greedily drains
                        // ALL currently-pending literals — including the outer's "$merge" pushed
                        // before the inner NEW — producing a bogus ["$merge","into"] on the inner
                        // call and nothing left for the outer call.
                        Deque<List<String>> pendingStringArgsScopeStack = new ArrayDeque<>();
                        // Collection-name string arg captured from a getCollection("X") call, held
                        // until the immediately-chained method call on its return value (e.g.
                        // db.getCollection("X").insertOne(doc)) so that chained call also sees "X".
                        // Without this, getCollection and insertOne are two separate InvocationRefs
                        // with no automatic linkage (each MethodInsnNode produces its own
                        // MethodCallInfo, and pendingStringArgs is drained by getCollection itself),
                        // so MongoMethodDetector's insertOne/aggregate/etc. detection never resolves
                        // a collection name for the common fluent-chaining idiom.
                        String pendingChainedCollectionArg = null;
                        // CALL-005: captured target of an invokedynamic (lambda/method-ref) that has
                        // not yet been consumed by an immediately-following PUTFIELD/PUTSTATIC. Set
                        // when an invokedynamic is seen, consumed (recorded as a field->target
                        // association on fieldAccesses) when the very next instruction is a field
                        // store, and cleared by any other instruction in between (a field write is
                        // only attributed to the method reference when it stores it DIRECTLY —
                        // `Supplier<X> s = repo::findAll; this.loader = s;` is intentionally not
                        // followed since that is a two-step local-variable case, not this task's
                        // single-field-write scope).
                        String pendingLambdaTargetClass = null;
                        String pendingLambdaTargetMethod = null;
                        String pendingLambdaTargetDescriptor = null;
                        // Non-literal (ALOAD/GETFIELD/GETSTATIC) argument sources captured between
                        // the previous call and this one — parallel to pendingStringArgs, but for
                        // values whose origin is a variable/field rather than an LDC. Consumed the
                        // same way, at each MethodInsnNode, so detectors can still report WHICH
                        // variable was used when the collection-name argument isn't a literal.
                        List<MethodCallInfo.DynamicArgSource> pendingDynamicArgs = new ArrayList<>();
                        // Parallel to pendingChainedCollectionArg, for the case where getCollection(x)
                        // was called with a variable/field rather than a literal — propagates the
                        // dynamic source onto the immediately-chained call (e.g.
                        // db.getCollection(sourceCollection).aggregate(pipeline)). Mutually exclusive
                        // with pendingChainedCollectionArg: getCollection's argument is either a
                        // resolved literal or a dynamic source, never both.
                        MethodCallInfo.DynamicArgSource pendingChainedDynamicArg = null;
                        // Declared return type of the getCollection(...) call that produced
                        // pendingChainedCollectionArg/pendingChainedDynamicArg (e.g.
                        // "com.mongodb.client.MongoCollection") — the receiver type the chained
                        // call must actually be invoked ON. Needed because a fluent BUILDER
                        // argument constructed in between (e.g. `new BulkWriteOptions().ordered
                        // (false)` as bulkWrite's 2nd arg in `getCollection(x).bulkWrite(ops, new
                        // BulkWriteOptions().ordered(false))`) is ALSO an INVOKEVIRTUAL — so the
                        // opcode-only check isn't enough to identify the real chained call;
                        // `.ordered(false)` would otherwise consume and clear the pending arg
                        // before bulkWrite (the actual MongoCollection-receiver call) is reached.
                        String pendingChainedReceiverType = null;

                        // Pre-scan: count ASTORE occurrences per local-variable slot, and record the
                        // last LDC-derived literal string stored to each slot. Used below so an ALOAD
                        // of a slot with EXACTLY ONE ASTORE in the whole method can be resolved inline
                        // as a real literal (common "String coll = "X"; ...; use(coll);" idiom),
                        // while a slot reassigned more than once (e.g. across if/else branches) is
                        // correctly left as a dynamic reference rather than guessing which branch ran.
                        Map<Integer, Integer> astoreCountBySlot = new HashMap<>();
                        Map<Integer, String> literalOnlySlotValue = new HashMap<>();
                        Set<Integer> slotEverStoredFromNonLiteral = new HashSet<>();
                        // CALL-003: per-slot record of a narrow, safe two-operand string
                        // concatenation (`literal + param` / `param + literal`) detected
                        // immediately before the ASTORE that stores its result — see
                        // ConcatSlotInfo's own docs. Populated in the SAME forward scan as
                        // literalOnlySlotValue above (so a param's own slot state is already
                        // known when a later concat references it), using a small sliding
                        // "operand window" that only survives a genuinely-adjacent run of
                        // LDC-string/ALOAD-of-parameter pushes — any other instruction in
                        // between (a method call, a field access, a branch, ...) clears it, so
                        // this can never misattribute a stale, non-adjacent push as an operand.
                        Map<Integer, ConcatSlotInfo> concatSlotValue = new HashMap<>();
                        // CALL-018: per-slot record of a local ASTORE'd DIRECTLY (adjacently — see
                        // pendingCallForReturn below) from a method call's own return value —
                        // `String x = someMethod();` — captured so the ALOAD-tracking loop further
                        // below can report a RETURN_VALUE-kind dynamic-arg source (owner/method of
                        // the call) instead of a bare unresolved LOCAL_VAR, letting
                        // CollectionResolver look up whether the CALLED method itself has a single,
                        // unambiguous literal return (see singleReturnLiteral/areturn tracking
                        // below) — single-hop only, mirroring CALL-001's own single-hop discipline
                        // for caller-argument provenance, but in the opposite (return-value) direction.
                        Map<Integer, MethodInsnNode> returnCallSlotValue = new HashMap<>();
                        // CALL-018: whole-method scan for THIS method's own return shape — resolved
                        // only when there is EXACTLY ONE ARETURN in the method and it is directly
                        // preceded (adjacency, same discipline as the concat-fold window above) by an
                        // LDC string push. Any second ARETURN, or an ARETURN not fed by a literal
                        // (e.g. returning a field, a param, or another call's result), leaves this
                        // method's return value correctly unresolved — never guessed across branches.
                        int[] areturnCount = {0};
                        String[] singleReturnLiteral = {null};
                        boolean[] areturnAmbiguous = {false};
                        // CALL-019: parallel single-hop tracking for a PASS-THROUGH return
                        // (`return other.foo();` — no literal, ASTORE, field, or param feeding the
                        // ARETURN, just another call's own return value directly). Mutually
                        // exclusive with singleReturnLiteral above — only one of the two is ever
                        // set for a given method, matching "exactly one ARETURN, unambiguous shape".
                        String[] singleReturnCallOwner = {null};
                        String[] singleReturnCallName = {null};
                        {
                            String pendingLiteral = null;
                            ConcatSlotInfo pendingConcatInfo = null;
                            MethodInsnNode pendingCallForReturn = null;
                            List<Object> concatWindow = new ArrayList<>();
                            for (AbstractInsnNode scanInsn = method.instructions.getFirst(); scanInsn != null; scanInsn = scanInsn.getNext()) {
                                if (scanInsn instanceof LineNumberNode || scanInsn instanceof LabelNode || scanInsn instanceof FrameNode) {
                                    continue; // pure positional bookkeeping — never breaks operand adjacency
                                }
                                if (scanInsn.getOpcode() == Opcodes.ARETURN) {
                                    areturnCount[0]++;
                                    if (pendingLiteral != null) {
                                        if (singleReturnLiteral[0] == null && singleReturnCallOwner[0] == null) {
                                            singleReturnLiteral[0] = pendingLiteral;
                                        } else if (!pendingLiteral.equals(singleReturnLiteral[0])) {
                                            areturnAmbiguous[0] = true;
                                        }
                                    } else if (pendingCallForReturn != null) {
                                        // CALL-019: this ARETURN is a pure pass-through of another
                                        // call's own return value — no literal/field/param/ASTORE
                                        // fed it. Record the callee so a later hop can be walked.
                                        String calleeOwner = pendingCallForReturn.owner.replace('/', '.');
                                        String calleeName = pendingCallForReturn.name;
                                        if (singleReturnLiteral[0] == null && singleReturnCallOwner[0] == null) {
                                            singleReturnCallOwner[0] = calleeOwner;
                                            singleReturnCallName[0] = calleeName;
                                        } else if (!(calleeOwner.equals(singleReturnCallOwner[0])
                                                && calleeName.equals(singleReturnCallName[0]))) {
                                            areturnAmbiguous[0] = true;
                                        }
                                    } else {
                                        areturnAmbiguous[0] = true;
                                    }
                                    pendingLiteral = null;
                                    pendingConcatInfo = null;
                                    pendingCallForReturn = null;
                                    concatWindow.clear();
                                } else if (scanInsn instanceof LdcInsnNode scanLdc && scanLdc.cst instanceof String s) {
                                    pendingLiteral = s;
                                    pendingCallForReturn = null;
                                    concatWindow.add(s);
                                    if (concatWindow.size() > 2) concatWindow.remove(0);
                                } else if (scanInsn instanceof VarInsnNode scanVarLoad && scanVarLoad.getOpcode() == Opcodes.ALOAD) {
                                    String pname = paramSlots.contains(scanVarLoad.var)
                                            ? resolveLocalVarName(method, scanVarLoad.var, true, scanInsn) : null;
                                    concatWindow.add(pname != null ? new ParamOperand(pname) : NON_OPERAND);
                                    if (concatWindow.size() > 2) concatWindow.remove(0);
                                    pendingCallForReturn = null;
                                } else if (scanInsn instanceof FieldInsnNode scanField && scanField.getOpcode() == Opcodes.GETFIELD
                                        && scanField.owner.replace('/', '.').equals(fqn)
                                        && fieldSingleLiteralValue.get(scanField.name) != null) {
                                    // §216/CALL-015: a GETFIELD read of a same-class field whose value
                                    // is provably a single literal across the whole class (see the
                                    // fieldSingleLiteralValue prescan above) — treat it exactly like an
                                    // LDC string push so the existing literal+param fold logic below
                                    // (tryFoldStringConcat) picks it up with zero changes to that method
                                    // or to ConcatSlotInfo. An UNRESOLVABLE field GETFIELD (not in this
                                    // map) deliberately falls through to the generic `else` branch below,
                                    // which clears the window — leaving the concatenation correctly
                                    // unresolved rather than guessed at.
                                    String fieldLiteral = fieldSingleLiteralValue.get(scanField.name);
                                    pendingLiteral = fieldLiteral;
                                    pendingCallForReturn = null;
                                    concatWindow.add(fieldLiteral);
                                    if (concatWindow.size() > 2) concatWindow.remove(0);
                                } else if (scanInsn instanceof InvokeDynamicInsnNode scanDyn) {
                                    ConcatFold fold = tryFoldStringConcat(scanDyn, concatWindow);
                                    if (fold != null && fold.fullLiteral() != null) {
                                        pendingLiteral = fold.fullLiteral();
                                        pendingConcatInfo = null;
                                    } else if (fold != null && fold.partial() != null) {
                                        pendingLiteral = null;
                                        pendingConcatInfo = fold.partial();
                                    } else {
                                        pendingLiteral = null;
                                        pendingConcatInfo = null;
                                    }
                                    pendingCallForReturn = null;
                                    concatWindow.clear();
                                } else if (scanInsn instanceof VarInsnNode scanVarStore && scanVarStore.getOpcode() == Opcodes.ASTORE) {
                                    astoreCountBySlot.merge(scanVarStore.var, 1, Integer::sum);
                                    if (pendingLiteral != null) {
                                        literalOnlySlotValue.put(scanVarStore.var, pendingLiteral);
                                    } else {
                                        // Stored value did not come directly from an LDC (e.g. a method
                                        // call result) — this slot can never be trusted as literal-only.
                                        slotEverStoredFromNonLiteral.add(scanVarStore.var);
                                    }
                                    if (pendingConcatInfo != null) {
                                        concatSlotValue.put(scanVarStore.var, pendingConcatInfo);
                                    }
                                    if (pendingLiteral == null && pendingConcatInfo == null && pendingCallForReturn != null) {
                                        // CALL-018: value stored here came DIRECTLY (adjacently) from a
                                        // method call's return — the classic `String x = other.foo();`
                                        // idiom. Recorded per-slot; whether it's actually usable is
                                        // decided later by the single-assignment ("exactly one ASTORE")
                                        // check in the ALOAD-tracking loop below, same discipline as
                                        // literalOnlySlotValue/concatSlotValue.
                                        returnCallSlotValue.put(scanVarStore.var, pendingCallForReturn);
                                    }
                                    pendingLiteral = null;
                                    pendingConcatInfo = null;
                                    pendingCallForReturn = null;
                                    concatWindow.clear();
                                } else if (scanInsn instanceof MethodInsnNode scanCallInsn) {
                                    pendingLiteral = null;
                                    pendingConcatInfo = null;
                                    concatWindow.clear();
                                    pendingCallForReturn = scanCallInsn;
                                } else {
                                    // Anything else breaks operand adjacency for the narrow concat
                                    // shape (pendingLiteral's own broader semantics, used elsewhere,
                                    // are untouched by this task).
                                    concatWindow.clear();
                                    pendingCallForReturn = null;
                                }
                            }
                        }
                        // CALL-018: this method's own return value resolves to a single, unambiguous
                        // literal ONLY when there was exactly one ARETURN, it was fed directly by an
                        // LDC string, and the method actually returns String (never guessed for a
                        // non-String return type, even if the bytecode shape happened to match).
                        if (areturnCount[0] == 1 && !areturnAmbiguous[0] && singleReturnLiteral[0] != null
                                && "java.lang.String".equals(Type.getReturnType(method.desc).getClassName())) {
                            mi.setSingleLiteralReturnValue(singleReturnLiteral[0]);
                        } else if (areturnCount[0] == 1 && !areturnAmbiguous[0] && singleReturnCallOwner[0] != null
                                && "java.lang.String".equals(Type.getReturnType(method.desc).getClassName())) {
                            // CALL-019: recorded as a pass-through target only — CollectionResolver
                            // walks this at resolution time (classMap is fully indexed by then),
                            // bounded by MAX_HOP_DEPTH so a cyclical/very-long chain can't loop forever.
                            mi.setSingleReturnPassThrough(singleReturnCallOwner[0], singleReturnCallName[0]);
                        }

                        // Pre-scan: for each addInsert/addUpdate/addDelete call on a BulkWriteCollector
                        // owner, walk BACKWARD to the nearest preceding LDC string literal, stopping at
                        // any other MethodInsnNode/ASTORE/NEW in between (those signal an argument was
                        // built by a nested call, not this literal). This directly answers "what literal
                        // did THIS specific call push as its collection-name arg", independent of how
                        // many non-consuming argument-builder calls (e.g. this.buildDoc(...)) run between
                        // the LDC and the actual addInsert/addUpdate invocation — those calls evaluate
                        // AFTER the collection-name arg is pushed but BEFORE addInsert/addUpdate itself
                        // (JVM stack-based argument evaluation order), and would otherwise steal the
                        // pending literal via the generic pendingStringArgs draining below.
                        Map<MethodInsnNode, String> bulkCollectorLiteralOverride = new IdentityHashMap<>();
                        {
                            String pendingLdc = null;
                            for (AbstractInsnNode scanInsn = method.instructions.getFirst(); scanInsn != null; scanInsn = scanInsn.getNext()) {
                                if (scanInsn instanceof LdcInsnNode scanLdc && scanLdc.cst instanceof String s) {
                                    pendingLdc = s;
                                } else if (scanInsn instanceof FieldInsnNode scanField && scanField.getOpcode() == Opcodes.GETSTATIC
                                        && scanField.owner.replace('/', '.').equals(fqn)) {
                                    // Same-class static-final constant (e.g. `private static final
                                    // String WIP_TABLE = "WIP_UWFI_..."`) used directly as the arg —
                                    // resolves the same way the main loop's GETSTATIC handling below does.
                                    String constVal = classConstantsByFieldName.get(scanField.name);
                                    if (constVal != null) pendingLdc = constVal;
                                } else if (scanInsn instanceof MethodInsnNode scanCall) {
                                    if (pendingLdc != null && BULK_COLLECTOR_ADD_METHODS.contains(scanCall.name)
                                            && scanCall.owner.toLowerCase().contains("bulkwritecollector")) {
                                        bulkCollectorLiteralOverride.put(scanCall, pendingLdc);
                                        pendingLdc = null;
                                    }
                                    // else: leave pendingLdc pending — a non-bulk-collector call in
                                    // between (e.g. this.convertDoc(bean) building addInsert's 2nd
                                    // arg) is an argument-BUILDER call, not a consumer of the literal;
                                    // it must not clear the literal before the real addInsert/addUpdate
                                    // call (which comes right after it in bytecode) gets a chance to
                                    // match. Only ASTORE/NEW/a fresh LDC end this literal's candidacy.
                                } else if (scanInsn instanceof VarInsnNode scanVar && scanVar.getOpcode() == Opcodes.ASTORE) {
                                    pendingLdc = null;
                                } else if (scanInsn instanceof TypeInsnNode scanType && scanType.getOpcode() == Opcodes.NEW) {
                                    pendingLdc = null;
                                }
                            }
                        }

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

                            // Track NEW (object construction start) so a nested constructor call's
                            // own LDC literals (e.g. inner `new Document("into", ...)`) are scoped
                            // separately from literals already pending for an outer constructor call
                            // that hasn't been INVOKESPECIAL <init>'d yet (e.g. outer's "$merge").
                            // Save-and-clear here; restored on the matching <init> below.
                            if (insn instanceof TypeInsnNode typeInsn && typeInsn.getOpcode() == Opcodes.NEW) {
                                pendingStringArgsScopeStack.push(new ArrayList<>(pendingStringArgs));
                                pendingStringArgs.clear();
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

                            // Capture ALOAD of a local variable/parameter as a potential dynamic
                            // (non-literal) call argument — e.g. bulkCollector.addInsert(coll, doc)
                            // where 'coll' is a loop variable, parameter, or local. Slot 0 on an
                            // instance method is 'this' and is never a collection-name variable.
                            if (insn instanceof VarInsnNode varInsn && varInsn.getOpcode() == Opcodes.ALOAD) {
                                boolean isStaticMethod = (method.access & Opcodes.ACC_STATIC) != 0;
                                if (isStaticMethod || varInsn.var != 0) {
                                    // Same-method single-assignment literal resolution: if this slot
                                    // was ASTORE'd from an LDC exactly once in the whole method, treat
                                    // it as if the literal itself were loaded here (safe: a slot
                                    // reassigned across branches is excluded by the ASTORE-count/
                                    // non-literal-store checks below, so we never guess between branches).
                                    Integer storeCount = astoreCountBySlot.get(varInsn.var);
                                    String resolvedLiteral = literalOnlySlotValue.get(varInsn.var);
                                    if (resolvedLiteral != null && storeCount != null && storeCount == 1
                                            && !slotEverStoredFromNonLiteral.contains(varInsn.var)) {
                                        pendingStringArgs.add(resolvedLiteral);
                                    } else if (storeCount != null && storeCount == 1
                                            && concatSlotValue.get(varInsn.var) != null) {
                                        // CALL-003: this slot's single assignment was a recognized
                                        // `literal + param` / `param + literal` concatenation — report
                                        // BOTH pieces so CollectionResolver can fold them once the
                                        // param side is resolved (kind encodes which side the literal
                                        // was on; ownerClass is repurposed to carry the literal text,
                                        // matching how STATIC_FIELD already repurposes it for its
                                        // owner class — see MethodCallInfo.DynamicArgSource's javadoc).
                                        ConcatSlotInfo ci = concatSlotValue.get(varInsn.var);
                                        if (ci.paramName2() != null) {
                                            // CALL-008: `param + param` shape — carry
                                            // BOTH operand names (ownerClass repurposed to hold
                                            // the SECOND operand's name, same pattern STATIC_FIELD/
                                            // CONCAT_LITERAL_PARAM already use to repurpose this
                                            // field for a second piece of data).
                                            pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(
                                                    "CONCAT_PARAM_PARAM", ci.paramName(), ci.paramName2()));
                                        } else {
                                            String kind = ci.literalFirst() ? "CONCAT_LITERAL_PARAM" : "CONCAT_PARAM_LITERAL";
                                            pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(kind, ci.paramName(), ci.literalPart()));
                                        }
                                    } else if (storeCount != null && storeCount == 1
                                            && returnCallSlotValue.get(varInsn.var) != null
                                            && "java.lang.String".equals(
                                                    Type.getReturnType(returnCallSlotValue.get(varInsn.var).desc).getClassName())) {
                                        // CALL-018: this slot's single assignment came directly from a
                                        // method call's return value (`String x = other.foo();`) — report
                                        // the CALLED method's owner/name so CollectionResolver can look up
                                        // whether THAT method has a single, unambiguous literal return
                                        // (see mi.getSingleLiteralReturnValue()/the whole-method ARETURN
                                        // scan above), the single-hop return-value mirror of the
                                        // PARAM/CONCAT_* argument-flow kinds above. ownerClass carries the
                                        // called method's owner class, matching how STATIC_FIELD/CONCAT_*
                                        // already repurpose this field for a second piece of data.
                                        MethodInsnNode retCall = returnCallSlotValue.get(varInsn.var);
                                        pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(
                                                "RETURN_VALUE", retCall.name, retCall.owner.replace('/', '.')));
                                    } else {
                                        // Only track String-typed locals/params as dynamic ARGUMENT
                                        // candidates — same reasoning as the GETFIELD filter above:
                                        // a MongoTemplate/Repository-typed local can never be String,
                                        // so this cannot mistake a receiver variable for an argument.
                                        String varName = resolveLocalVarName(method, varInsn.var, true, varInsn);
                                        if (varName != null) {
                                            String kind = paramSlots.contains(varInsn.var) ? "PARAM" : "LOCAL_VAR";
                                            pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(kind, varName, null));
                                        } else {
                                            // CALL-004: not a String — check whether it's a
                                            // Class<?>-typed local/param instead (an entity class
                                            // threaded down through 2+ calls before being used as a
                                            // Mongo collection-derivation argument). Reported with a
                                            // distinct kind (PARAM_CLASS/LOCAL_VAR_CLASS) so
                                            // CollectionResolver's resolvers never confuse it with a
                                            // String-shaped PARAM/LOCAL_VAR candidate.
                                            String classVarName = resolveLocalVarNameClassTyped(method, varInsn.var, varInsn);
                                            if (classVarName != null) {
                                                String kind = paramSlots.contains(varInsn.var) ? "PARAM_CLASS" : "LOCAL_VAR_CLASS";
                                                pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(kind, classVarName, null));
                                            }
                                        }
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
                                // CALL-005: a PUTFIELD/PUTSTATIC directly consuming an invokedynamic's
                                // result (`this.loader = repo::findAll;`) records the field->target
                                // association on THIS field access so CallGraphIndex can build a
                                // per-class field->method-reference map (mirrors injectedFields'
                                // shape/consumption pattern). Any other instruction between the
                                // invokedynamic and this field store already cleared the pending
                                // fields below, so this only fires for the direct, single-hop case.
                                if (pendingLambdaTargetClass != null
                                        && (fieldInsn.getOpcode() == Opcodes.PUTFIELD
                                            || fieldInsn.getOpcode() == Opcodes.PUTSTATIC)) {
                                    fai.setLambdaTargetClass(pendingLambdaTargetClass);
                                    fai.setLambdaTargetMethod(pendingLambdaTargetMethod);
                                    fai.setLambdaTargetDescriptor(pendingLambdaTargetDescriptor);
                                }
                                pendingLambdaTargetClass = null;
                                pendingLambdaTargetMethod = null;
                                pendingLambdaTargetDescriptor = null;
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
                                    pendingChainedCollectionArg = null;
                                    pendingChainedDynamicArg = null;
                                    pendingChainedReceiverType = null;
                                }
                                // GETFIELD on a non-this object: leave the pending receiver intact

                                // Additionally (independent of the receiver-linkage logic above) record
                                // GETFIELD/GETSTATIC as a potential dynamic call-argument source — e.g.
                                // bulkCollector.addInsert(this.targetCollection, doc) or
                                // addInsert(SomeConstants.COLLECTION_NAME, doc). This may overlap with the
                                // receiver field captured above for the SAME field access (we can't cheaply
                                // disambiguate "receiver" vs "argument" without a full stack simulator); any
                                // false-positive capture here is harmless because detectors only consult
                                // this list on call sites already known (by method name + owner) to expect
                                // a collection-name argument, so unrelated field captures are simply ignored.
                                if (fieldInsn.getOpcode() == Opcodes.GETSTATIC) {
                                    String ownerFqn = fieldInsn.owner.replace('/', '.');
                                    String sameClassConst = ownerFqn.equals(fqn)
                                            ? classConstantsByFieldName.get(fieldInsn.name) : null;
                                    if (sameClassConst != null) {
                                        // Same-class static final String constant — resolve inline as a
                                        // literal, exactly as if it had been loaded via LDC.
                                        pendingStringArgs.add(sameClassConst);
                                    } else {
                                        // Cross-class (or non-constant) static field — leave for
                                        // CollectionResolver's enumConstantsMap resolution attempt.
                                        pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(
                                                "STATIC_FIELD", fieldInsn.name, ownerFqn));
                                    }
                                } else if (fieldInsn.getOpcode() == Opcodes.GETFIELD
                                        && isStringDesc(fieldInsn.desc)) {
                                    // Only track String-typed fields as dynamic ARGUMENT candidates.
                                    // This is what disambiguates a receiver field load (e.g.
                                    // this.mongoTemplate, type MongoTemplate) from a genuine
                                    // collection-name-holding field (e.g. this.targetCollection, type
                                    // String) without needing a full stack simulator: a
                                    // MongoTemplate/Repository-typed field can never be String-typed,
                                    // so this filter cannot mistake a receiver for an argument.
                                    pendingDynamicArgs.add(new MethodCallInfo.DynamicArgSource(
                                            "FIELD", fieldInsn.name, fieldInsn.owner.replace('/', '.')));
                                }
                            }

                            if (insn instanceof MethodInsnNode call) {
                                // A method call between an invokedynamic and any later field store
                                // breaks the direct "captured lambda stored straight into a field"
                                // pattern (CALL-005 scope) — clear so an unrelated later PUTFIELD
                                // isn't wrongly attributed to this invokedynamic's target.
                                pendingLambdaTargetClass = null;
                                pendingLambdaTargetMethod = null;
                                pendingLambdaTargetDescriptor = null;
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
                                // Bulk-collector call-site override (see bulkCollectorLiteralOverride
                                // pre-scan above): collector.addInsert("WIP_X", this.buildDoc(...)) pushes
                                // "WIP_X" via LDC, then evaluates buildDoc(...) — an argument-BUILDER call
                                // that runs BEFORE addInsert itself per JVM stack-based argument order and
                                // greedily steals "WIP_X" from pendingStringArgs above. The identity-keyed
                                // pre-scan below sidesteps that ambiguity by directly matching each LDC to
                                // its enclosing addInsert/addUpdate/addDelete call in the raw instruction
                                // stream, so the override always wins regardless of what pendingStringArgs
                                // got drained into on the way there.
                                String bulkOverride = bulkCollectorLiteralOverride.get(call);
                                if (bulkOverride != null) {
                                    List<String> args = mci.getRecentStringArgs();
                                    if (args == null) {
                                        mci.setRecentStringArgs(new ArrayList<>(List.of(bulkOverride)));
                                    } else if (!args.contains(bulkOverride)) {
                                        args.add(bulkOverride);
                                    }
                                }
                                // Matching <init> for a NEW pushed earlier — restore the caller's
                                // (outer) pendingStringArgs scope now that this constructor call has
                                // consumed its own literals above. This is what keeps a nested
                                // `new Document("into", ...)` constructor's "into" literal from
                                // leaking onto the outer `new Document("$merge", ...)` constructor
                                // call (and vice versa) — see pendingStringArgsScopeStack declaration.
                                if ("<init>".equals(call.name) && !pendingStringArgsScopeStack.isEmpty()) {
                                    pendingStringArgs.addAll(pendingStringArgsScopeStack.pop());
                                }
                                if (!pendingDynamicArgs.isEmpty()) {
                                    mci.setDynamicArgSources(new ArrayList<>(pendingDynamicArgs));
                                    pendingDynamicArgs.clear();
                                }
                                // Propagate a collection name captured from an immediately-preceding
                                // getCollection("X") call onto THIS call — the fluent-chain idiom
                                // db.getCollection("X").insertOne(doc) produces two separate
                                // MethodInsnNodes (getCollection, then insertOne on its return
                                // value), so without this the collection name never reaches the
                                // insertOne/aggregate/etc. invocation that actually needs it.
                                // Consumed exactly once — always cleared after this point, so it
                                // never leaks past the single call chained directly onto getCollection.
                                //
                                // Only INVOKEINTERFACE/INVOKEVIRTUAL calls actually consume the
                                // MongoCollection reference off the stack and are eligible to be the
                                // "chained" call. Require the call's owner to match the declared return
                                // type of the getCollection(...) call that produced the pending arg, so
                                // an intermediate builder-chain call on a differently-typed argument
                                // (e.g. `new BulkWriteOptions().ordered(false)`) doesn't steal it.
                                boolean isChainConsumingCall = (call.getOpcode() == Opcodes.INVOKEINTERFACE
                                        || call.getOpcode() == Opcodes.INVOKEVIRTUAL)
                                        && (pendingChainedReceiverType == null
                                            || call.owner.replace('/', '.').equals(pendingChainedReceiverType));
                                if (isChainConsumingCall && pendingChainedCollectionArg != null) {
                                    List<String> args = mci.getRecentStringArgs();
                                    if (args == null) {
                                        mci.setRecentStringArgs(new ArrayList<>(List.of(pendingChainedCollectionArg)));
                                    } else if (!args.contains(pendingChainedCollectionArg)) {
                                        args.add(pendingChainedCollectionArg);
                                    }
                                    pendingChainedCollectionArg = null;
                                    pendingChainedReceiverType = null;
                                }
                                // Same propagation for the dynamic (variable-sourced) case — e.g.
                                // db.getCollection(sourceCollection).aggregate(pipeline) where
                                // sourceCollection is a method parameter. Only applies when getCollection
                                // did NOT resolve a literal (the two chains are mutually exclusive).
                                if (isChainConsumingCall && pendingChainedDynamicArg != null) {
                                    List<MethodCallInfo.DynamicArgSource> dynArgs = mci.getDynamicArgSources();
                                    if (dynArgs == null) {
                                        mci.setDynamicArgSources(new ArrayList<>(List.of(pendingChainedDynamicArg)));
                                    } else if (!dynArgs.contains(pendingChainedDynamicArg)) {
                                        dynArgs.add(pendingChainedDynamicArg);
                                    }
                                    pendingChainedDynamicArg = null;
                                    pendingChainedReceiverType = null;
                                }
                                // If THIS call is getCollection("X"), remember "X" for the next call
                                // that's actually invoked on its return value (the MongoCollection).
                                // Use the call's DECLARED RETURN TYPE (mci.getReturnType(), already
                                // cleaned by TypeUtils above) as the expected receiver type for the
                                // chained call — NOT call.owner, which is getCollection's OWNER
                                // (e.g. MongoTemplate), not what it returns.
                                if ("getCollection".equals(call.name) && mci.getRecentStringArgs() != null
                                        && !mci.getRecentStringArgs().isEmpty()) {
                                    pendingChainedCollectionArg = mci.getRecentStringArgs().get(0);
                                    pendingChainedReceiverType = mci.getReturnType();
                                } else if ("getCollection".equals(call.name)
                                        && (mci.getRecentStringArgs() == null || mci.getRecentStringArgs().isEmpty())
                                        && mci.getDynamicArgSources() != null && !mci.getDynamicArgSources().isEmpty()) {
                                    // getCollection(variable) — no literal resolved, but a dynamic
                                    // source was captured; propagate that instead.
                                    pendingChainedDynamicArg = mci.getDynamicArgSources().get(0);
                                    pendingChainedReceiverType = mci.getReturnType();
                                }
                                mi.getInvocations().add(mci);
                            } else if (insn instanceof InvokeDynamicInsnNode dynInsn) {
                                pendingReceiverField = null;
                                pendingReceiverType = null;
                                pendingChainedCollectionArg = null;
                                pendingChainedDynamicArg = null;
                                pendingDynamicArgs.clear();
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
                                            // CALL-005: remember this invokedynamic's target in case the
                                            // VERY NEXT instruction is a PUTFIELD/PUTSTATIC storing it
                                            // straight into a field (see FieldInsnNode handling above).
                                            pendingLambdaTargetClass = mci.getLambdaTargetClass();
                                            pendingLambdaTargetMethod = mci.getLambdaTargetMethod();
                                            pendingLambdaTargetDescriptor = mci.getLambdaTargetDescriptor();
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

    /* ---- dynamic (variable-sourced) argument helpers ---- */

    /** True when a field/local-variable bytecode descriptor is exactly java.lang.String. */
    private boolean isStringDesc(String desc) {
        return "Ljava/lang/String;".equals(desc);
    }

    /** True when a field/local-variable bytecode descriptor is exactly java.lang.Class (raw or
     *  generic-erased {@code Class<?>} — generics don't survive into the descriptor, so this
     *  matches both). CALL-004: used the same way {@link #isStringDesc} already disambiguates a
     *  collection-name-holding String from a receiver/service-typed variable — a
     *  MongoTemplate/Repository-typed variable can never be {@code Class}-typed, so this cannot
     *  mistake a receiver for a propagated entity-class argument. */
    private boolean isClassDesc(String desc) {
        return "Ljava/lang/Class;".equals(desc);
    }

    /** CALL-003: result of attempting to fold a two-operand string concatenation. Exactly one of
     *  the two fields is ever non-null (or both null when the shape didn't qualify at all — the
     *  caller treats a null {@code ConcatFold} the same as a fully-non-qualifying attempt). */
    private record ConcatFold(String fullLiteral, ConcatSlotInfo partial) {}

    /**
     * CALL-003: recognizes the narrow, safe `literal + param` / `param + literal` shape at an
     * {@code invokedynamic} that bootstraps through {@code StringConcatFactory} — the ONLY shape
     * javac emits for {@code +} string concatenation on this codebase's target bytecode level
     * (confirmed against real compiled fixtures: no {@code StringBuilder.append} chain is ever
     * produced; javac also constant-folds an ALL-literal concatenation away entirely before this
     * point, so that trivial case never even reaches here). Two distinct call shapes exist:
     * <ul>
     *   <li>BOTH operands are dynamic (descriptor takes 2 String args, nothing folded into the
     *       recipe) — read them straight off the operand window.</li>
     *   <li>ONE operand was a compile-time constant that javac folded directly into the
     *       bootstrap's recipe string (descriptor takes only 1 String arg) — the recipe uses
     *       {@code } as the placeholder for the single dynamic operand; the literal text is
     *       whatever surrounds it. Rejected (kept safely unresolved) when the recipe contains
     *       {@code } (an escaped literal constant — signals unusual/complex text this task's
     *       narrow scope doesn't attempt) or has literal text on BOTH sides of the placeholder
     *       (that shape implies 3+ original operands, e.g. {@code "a" + x + "b"}, out of scope).
     * </ul>
     * Any other shape (3+ descriptor args, ambiguous/missing window entries, etc.) returns null —
     * callers fall back to today's plain unresolved LOCAL_VAR behavior, exactly as before this
     * task, never fabricating a value.
     */
    private ConcatFold tryFoldStringConcat(InvokeDynamicInsnNode dyn, List<Object> window) {
        if (dyn.bsm == null || !"java/lang/invoke/StringConcatFactory".equals(dyn.bsm.getOwner())
                || !"makeConcatWithConstants".equals(dyn.bsm.getName())) {
            return null;
        }
        Type[] argTypes = Type.getArgumentTypes(dyn.desc);
        if (!Type.getReturnType(dyn.desc).getClassName().equals("java.lang.String")) return null;

        if (argTypes.length == 2) {
            if (!isStringDesc(argTypes[0].getDescriptor()) || !isStringDesc(argTypes[1].getDescriptor())) return null;
            if (window.size() != 2) return null;
            Object op1 = window.get(0), op2 = window.get(1);
            if (op1 instanceof String l1 && op2 instanceof String l2) {
                return new ConcatFold(l1 + l2, null);
            } else if (op1 instanceof String lit && op2 instanceof ParamOperand p) {
                return new ConcatFold(null, new ConcatSlotInfo(lit, true, p.name()));
            } else if (op1 instanceof ParamOperand p && op2 instanceof String lit) {
                return new ConcatFold(null, new ConcatSlotInfo(lit, false, p.name()));
            } else if (op1 instanceof ParamOperand p1 && op2 instanceof ParamOperand p2) {
                // CALL-008 (§209/§213): `param + param` — neither operand is a
                // same-method literal (§204 Pattern A's real shape). Both must be chased up
                // their own ancestor chain independently; see ConcatSlotInfo's own docs.
                return new ConcatFold(null, new ConcatSlotInfo(null, false, p1.name(), p2.name()));
            }
            return null; // an untracked NON_OPERAND on either side — out of scope
        }

        if (argTypes.length == 1) {
            if (!isStringDesc(argTypes[0].getDescriptor())) return null;
            if (window.isEmpty()) return null;
            Object op = window.get(window.size() - 1);
            String recipe = null;
            if (dyn.bsmArgs != null) {
                for (Object a : dyn.bsmArgs) {
                    if (a instanceof String s) { recipe = s; break; }
                }
            }
            if (recipe == null || recipe.indexOf('\u0002') >= 0) return null;
            int placeholder = recipe.indexOf('\u0001');
            if (placeholder < 0 || recipe.indexOf('\u0001', placeholder + 1) >= 0) return null; // 0 or 2+ placeholders
            String before = recipe.substring(0, placeholder);
            String after = recipe.substring(placeholder + 1);
            if (!before.isEmpty() && !after.isEmpty()) return null; // "a" + x + "b" shape — 3 operands, out of scope
            if (before.isEmpty() && after.isEmpty()) return null; // no literal text at all — nothing to fold
            String literalPart = before.isEmpty() ? after : before;
            boolean literalFirst = !before.isEmpty();
            if (op instanceof String lit) {
                return new ConcatFold(literalFirst ? literalPart + lit : lit + literalPart, null);
            } else if (op instanceof ParamOperand p) {
                return new ConcatFold(null, new ConcatSlotInfo(literalPart, literalFirst, p.name()));
            }
            return null;
        }
        return null; // 0 or 3+ operands — arbitrary/complex chain, explicitly out of this task's scope
    }

    /**
     * Resolves a local-variable-table slot index to its declared name, restricted to slots
     * whose type is String (so a MongoTemplate/Repository-typed receiver variable is never
     * mistaken for a collection-name-holding argument). Slot-index matching only (no
     * start/end-label scope check).
     *
     * @deprecated Slot-only matching is unsafe when a single slot is reused for two DIFFERENT
     * (non-overlapping-scope) variables in the same method. Prefer
     * {@link #resolveLocalVarName(MethodNode, int, boolean, AbstractInsnNode)} which
     * disambiguates using the local-variable-table's start/end label range around the actual
     * ALOAD site. Kept only for the ASTORE pre-scan's whole-method ambiguity checks, which
     * intentionally look at the slot in the abstract (not at any specific instruction).
     */
    private String resolveLocalVarName(MethodNode method, int slot, boolean requireStringType) {
        if (method.localVariables == null) return null;
        for (LocalVariableNode lv : method.localVariables) {
            if (lv.index == slot) {
                if (requireStringType && !isStringDesc(lv.desc)) return null;
                return lv.name;
            }
        }
        return null;
    }

    /**
     * Resolves a local-variable-table slot index to its declared name AT A SPECIFIC ALOAD SITE,
     * disambiguating slot reuse by checking the local-variable-table entry's [start, end) label
     * range against the ALOAD instruction's position in the method.
     *
     * Without this, a slot reused for two unrelated variables in different (non-overlapping)
     * scopes of the same method resolves EVERY ALOAD of that slot to whichever LocalVariableNode
     * happens to appear first in method.localVariables, regardless of which scope the ALOAD site
     * is actually in. That produces a bogus dynamic-arg capture (the stale out-of-scope variable
     * name) attached to a call site that has nothing to do with it.
     *
     * Falls back to the slot-only match (old behavior) if the local-variable-table has no usable
     * start/end labels for a candidate entry (defensive — shouldn't happen for javac-compiled
     * classes, but avoids regressing to "no match" for edge-case bytecode).
     */
    private String resolveLocalVarName(MethodNode method, int slot, boolean requireStringType,
                                        AbstractInsnNode site) {
        if (method.localVariables == null) return null;
        List<LocalVariableNode> candidates = new ArrayList<>();
        for (LocalVariableNode lv : method.localVariables) {
            if (lv.index == slot) candidates.add(lv);
        }
        if (candidates.isEmpty()) return null;
        if (candidates.size() == 1) {
            LocalVariableNode lv = candidates.get(0);
            if (requireStringType && !isStringDesc(lv.desc)) return null;
            return lv.name;
        }
        // Multiple LocalVariableNode entries share this slot (scope reuse) — find the one whose
        // [start, end) range actually contains the ALOAD site, walking the instruction list once.
        for (LocalVariableNode lv : candidates) {
            if (isWithinRange(lv.start, lv.end, site)) {
                if (requireStringType && !isStringDesc(lv.desc)) return null;
                return lv.name;
            }
        }
        // No candidate's range contained the site (shouldn't normally happen) — be conservative
        // and report nothing rather than guessing the wrong scope.
        return null;
    }

    /**
     * CALL-004: same slot-reuse-safe resolution as {@link #resolveLocalVarName(MethodNode, int,
     * boolean, AbstractInsnNode)}, but requiring the local-variable-table entry's declared type be
     * exactly {@code java.lang.Class} rather than {@code String} — used to capture an ALOAD of a
     * {@code Class<?>}-typed parameter/local (an entity class threaded down through 2+ method
     * calls before being used at a Mongo call site) as a dynamic argument candidate. Deliberately
     * a separate method rather than parameterizing the String one with a predicate — keeps the
     * existing, already-verified CALL-001/002/003/005 String path completely untouched.
     */
    private String resolveLocalVarNameClassTyped(MethodNode method, int slot, AbstractInsnNode site) {
        if (method.localVariables == null) return null;
        List<LocalVariableNode> candidates = new ArrayList<>();
        for (LocalVariableNode lv : method.localVariables) {
            if (lv.index == slot) candidates.add(lv);
        }
        if (candidates.isEmpty()) return null;
        if (candidates.size() == 1) {
            LocalVariableNode lv = candidates.get(0);
            return isClassDesc(lv.desc) ? lv.name : null;
        }
        for (LocalVariableNode lv : candidates) {
            if (isWithinRange(lv.start, lv.end, site)) {
                return isClassDesc(lv.desc) ? lv.name : null;
            }
        }
        return null;
    }

    /** True when `site` appears at or after `start` and before `end` in the instruction list. */
    private boolean isWithinRange(LabelNode start, LabelNode end, AbstractInsnNode site) {
        if (start == null) return false;
        boolean afterStart = false;
        for (AbstractInsnNode n = start; n != null; n = n.getNext()) {
            if (n == site) { afterStart = true; break; }
            if (n == end) break; // reached end label before finding site
        }
        return afterStart;
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

    private String determineStereotype(List<AnnotationInfo> annotations, List<String> interfaces) {
        // @Aspect wins over the stereotype annotations, and is therefore tested before the loop
        // rather than as another case in it. An aspect is nearly always also a @Component, and the
        // loop below returns on the first *annotation* in bytecode order, not by case order — so a
        // case inside it would make the result depend on how the compiler happened to emit them.
        // Reporting an aspect as COMPONENT hides that it is cross-cutting advice which appears in
        // no call tree; AdviceDetector carries the detail.
        for (AnnotationInfo ann : annotations) {
            if ("Aspect".equals(ann.getName())) return "ASPECT";
        }
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
        // Detect Spring Data repository interfaces (often used without @Repository). Direct
        // (one-hop) case only -- multi-level chains (interface B extends A extends
        // MongoRepository<...>) are handled transitively by RepositoryStereotypeResolver, which
        // runs as a post-pass over the whole classesFile using this exact same per-hop rule.
        if (interfaces != null) {
            for (String iface : interfaces) {
                if (SpringDataRepositoryInterfaces.MARKER_INTERFACES.contains(iface)
                        || SpringDataRepositoryInterfaces.looksLikeRepositoryByName(iface)) {
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

    /**
     * Parse generic signature to extract the entity type from MongoRepository&lt;Entity, ID&gt;.
     * ASM signature format: "Ljava/lang/Object;Lorg/springframework/data/.../MongoRepository&lt;Lcom/example/Entity;Ljava/lang/String;&gt;;"
     * We look for a known repo interface in the signature, then extract the first type parameter.
     * Direct (one-hop) case only -- see RepositoryStereotypeResolver for the multi-level chain case.
     */
    private String extractRepositoryEntityType(String signature, List<String> interfaces) {
        if (signature == null) return null;

        for (String iface : interfaces) {
            if (!SpringDataRepositoryInterfaces.MARKER_INTERFACES.contains(iface)
                    && !SpringDataRepositoryInterfaces.looksLikeRepositoryByName(iface)) {
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
