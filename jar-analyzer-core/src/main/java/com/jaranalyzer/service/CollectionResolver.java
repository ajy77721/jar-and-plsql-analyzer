package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.service.CallGraphIndex.*;
import com.jaranalyzer.util.SqlStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Resolves repository-to-collection mappings and populates collection data on CallNodes.
 * Extracted from CallTreeBuilder to keep files focused and under 300 lines.
 */
@Component
class CollectionResolver {

    private static final Logger log = LoggerFactory.getLogger(CollectionResolver.class);

    private final DomainConfigLoader configLoader;
    private final MongoMethodDetector mongoMethodDetector;
    private final AggregationDetector aggregationDetector;
    private final JpaMethodDetector jpaMethodDetector;
    private final MyBatisMethodDetector myBatisMethodDetector;
    private final RedisMethodDetector redisMethodDetector;
    private final CassandraMethodDetector cassandraMethodDetector;
    private final ElasticsearchMethodDetector elasticsearchMethodDetector;
    private final CollectionLifecycleDetector collectionLifecycleDetector;

    CollectionResolver(DomainConfigLoader configLoader, MongoMethodDetector mongoMethodDetector,
                       AggregationDetector aggregationDetector, JpaMethodDetector jpaMethodDetector,
                       MyBatisMethodDetector myBatisMethodDetector, RedisMethodDetector redisMethodDetector,
                       CassandraMethodDetector cassandraMethodDetector,
                       ElasticsearchMethodDetector elasticsearchMethodDetector,
                       CollectionLifecycleDetector collectionLifecycleDetector) {
        this.configLoader = configLoader;
        this.mongoMethodDetector = mongoMethodDetector;
        this.aggregationDetector = aggregationDetector;
        this.jpaMethodDetector = jpaMethodDetector;
        this.myBatisMethodDetector = myBatisMethodDetector;
        this.redisMethodDetector = redisMethodDetector;
        this.cassandraMethodDetector = cassandraMethodDetector;
        this.elasticsearchMethodDetector = elasticsearchMethodDetector;
        this.collectionLifecycleDetector = collectionLifecycleDetector;
    }

    /**
     * Tag a just-built CHILD node with the collection resolved at the CALL SITE that produced it
     * (as opposed to collectCollections above, which resolves collections from signals found
     * INSIDE a method's own body). Needed for methods like BulkWriteCollector.addInsert/addUpdate
     * whose own body never references the collection at all — it only arrives as a parameter, so
     * collectCollections finds nothing for that node's OWN method; the collection name is only
     * visible at the caller's literal/resolved argument, one level up in the tree. Additive only:
     * never overwrites collections already found on the node by its own body (e.g. a wrapper
     * method that both calls addInsert AND does other DB work in the same body).
     *
     * Returns null when the call is neither a BulkWriteCollector buffering call nor a general
     * (CALL-001) single-hop PARAM-propagation candidate (the common case — callers should keep
     * the original node), or the caller-tagged CallNode otherwise. The returned node is always
     * CallNode.copyForCallSiteTagging()'d off the original BEFORE this method mutates it — see
     * that method's docs for why buildCallTree's memoization makes this mandatory (the original
     * may be shared across unrelated call sites via the memo map).
     */
    CallNode tagCallSiteCollection(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain) {
        return tagCallSiteCollection(original, call, ancestorCallChain, null);
    }

    CallNode tagCallSiteCollection(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain,
                                    ResolutionContext ctx) {
        MongoMethodDetector.CallSiteCollection csc = mongoMethodDetector.resolveBulkCollectorCallSite(call);
        if (csc != null) {
            CallNode child = original.copyForCallSiteTagging();
            if (csc.collectionName() != null) {
                if (!child.getCollectionsAccessed().contains(csc.collectionName())) {
                    child.getCollectionsAccessed().add(csc.collectionName());
                    child.getCollectionDomains().put(csc.collectionName(), configLoader.detectCollectionDomain(csc.collectionName()));
                    child.getCollectionSources().put(csc.collectionName(), "BULK_WRITE_COLLECTOR_CALL_SITE");
                }
            } else if (csc.dynamicArg() != null) {
                Map<String, Object> ref = new LinkedHashMap<>();
                ref.put("variableName", csc.dynamicArg().name());
                ref.put("varKind", csc.dynamicArg().kind());
                ref.put("pattern", "BULK_WRITE_COLLECTOR_CALL_SITE_DYNAMIC");
                ref.put("source", "BULK_WRITE_COLLECTOR");
                ref.put("operationType", csc.operationType());
                if (!child.getDynamicCollectionRefs().contains(ref)) {
                    child.getDynamicCollectionRefs().add(ref);
                }
            }
            if (csc.operationType() != null) {
                child.getOperationTypes().add(csc.operationType());
                if (child.getOperationType() == null) child.setOperationType(csc.operationType());
            }
            child.setHasDbInteraction(true);
            return tagGeneralSqlCallSite(child, call, ancestorCallChain, ctx);
        }
        CallNode mongoTagged = tagGeneralParamCallSite(original, call, ancestorCallChain, ctx);
        CallNode jdbcTagged = tagGeneralSqlCallSite(mongoTagged != null ? mongoTagged : original, call, ancestorCallChain, ctx);
        CallNode base = jdbcTagged != null ? jdbcTagged : mongoTagged;
        CallNode esTagged = tagGeneralEsCallSite(base != null ? base : original, call, ancestorCallChain, ctx);
        return esTagged != null ? esTagged : base;
    }

    /**
     * CALL-002: bounded multi-hop walk-up bound. A single hop (CALL-001) only resolves the case
     * where the IMMEDIATE caller passes a literal/already-resolved value into the callee. When the
     * immediate caller instead passes ITS OWN unresolved PARAM-kind value (i.e. the caller received
     * it from further up the chain and is just forwarding it), CALL-001 alone gives up. This bound
     * caps how many call sites total (the original call site PLUS its ancestors, one hop each) are
     * consulted before giving up for good — matches the user's own real-world report of a value
     * threading through roughly 3 intermediate methods before reaching its real use, plus one
     * margin level. Named constant per this task's requirement — not sprinkled as a magic number.
     */
    static final int MAX_HOP_DEPTH = 4;

    /**
     * CALL-001: generalizes the mechanism above beyond BulkWriteCollector to ANY callee, single
     * hop only (caller → immediate callee — no walking further up, that is CALL-002). Fires when
     * the just-built CHILD node's own method body already recorded an unresolved PARAM-kind
     * dynamicCollectionRefs entry (i.e. collectCollections, walking the CALLEE's own body, found
     * that one of its parameters is used as a Mongo collection-name argument, but couldn't
     * resolve the parameter to a literal because that requires knowing what the CALLER passed).
     * When that's the case, resolve THIS call site's actual argument the same way
     * resolveBulkCollectorCallSite already does (literal string arg first, else a same-method
     * dynamic/variable source) and tag the resolved value onto the child, removing the now-stale
     * unresolved PARAM entry it replaces. Position-agnostic (first-match), matching the existing
     * BulkWriteCollector mechanism's own semantics — see resolveGenericCallSiteArg's docs.
     *
     * CALL-002 extension: if THIS call site's own argument is itself an unresolved PARAM-kind
     * pass-through (the immediate caller is just forwarding a value it received as ITS OWN
     * parameter from further up the tree), don't give up immediately — consult the caller's own
     * caller (and so on), via {@code ancestorCallChain} (the call sites that produced the current
     * frame, nearest ancestor first, threaded down through CallTreeBuilder's existing recursion),
     * up to {@link #MAX_HOP_DEPTH} call sites total. Returns null when the callee has no
     * unresolved PARAM entry, or when the walk-up exhausts its bound / the ancestor chain without
     * resolving to anything (leaves the existing unresolved PARAM ref as-is — same as today, never
     * crashes or loops unboundedly).
     */
    private CallNode tagGeneralParamCallSite(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain,
                                              ResolutionContext ctx) {
        List<Map<String, Object>> existingRefs = original.getDynamicCollectionRefs();
        if (existingRefs == null || existingRefs.isEmpty()) return null;
        Map<String, Object> paramRef = null;
        Map<String, Object> paramClassRef = null;
        for (Map<String, Object> ref : existingRefs) {
            if ("PARAM".equals(ref.get("varKind"))) {
                paramRef = ref;
                break;
            }
            if (paramClassRef == null && "PARAM_CLASS".equals(ref.get("varKind"))) {
                paramClassRef = ref;
            }
        }
        if (paramRef == null) {
            // CALL-004: no plain String PARAM pass-through — check for a Class<?>-typed one
            // (an entity class threaded down through 2+ calls, resolved to a collection name via
            // the SAME @Document-annotation → collection-name map AggregationDetector/
            // MongoMethodDetector already use for the direct-call case, see resolveClassAcrossHops).
            if (paramClassRef == null || ctx == null) return null;
            Map<String, String> entityMap = ctx.entityCollectionMap != null ? ctx.entityCollectionMap : Map.of();
            ClassResolution resolution = resolveClassAcrossHops(call, ancestorCallChain, entityMap);
            if (resolution == null) return null;
            String coll = resolution.collectionName();

            CallNode classChild = original.copyForCallSiteTagging();
            if (!classChild.getCollectionsAccessed().contains(coll)) {
                classChild.getCollectionsAccessed().add(coll);
                classChild.getCollectionDomains().put(coll, configLoader.detectCollectionDomain(coll));
                classChild.getCollectionSources().put(coll, resolution.source());
            }
            Object paramClassVarName = paramClassRef.get("variableName");
            classChild.getDynamicCollectionRefs().removeIf(r ->
                    "PARAM_CLASS".equals(r.get("varKind")) && Objects.equals(r.get("variableName"), paramClassVarName));
            Object classOpType = paramClassRef.get("operationType");
            if (classOpType instanceof String opStr) {
                classChild.getOperationTypes().add(opStr);
                if (classChild.getOperationType() == null) classChild.setOperationType(opStr);
            }
            classChild.setHasDbInteraction(true);
            return classChild;
        }

        MongoMethodDetector.CallSiteCollection csc = resolveAcrossHops(call, ancestorCallChain);
        // CALL-003: if what CALL-002's walk found is itself a `literal + param`/`param + literal`
        // concatenation (rather than a plain unresolved PARAM pass-through), attempt to fold it —
        // see foldConcatIfPossible's own docs for exactly how far this walks and why it refuses
        // to guess when the ancestor call site is ambiguous.
        boolean wasConcatShape = isConcatShape(csc);
        csc = foldConcatIfPossible(csc, ancestorCallChain);
        if (csc == null || csc.collectionName() == null) return null; // still unresolved — nothing gained

        CallNode child = original.copyForCallSiteTagging();
        String coll = csc.collectionName();
        if (!child.getCollectionsAccessed().contains(coll)) {
            child.getCollectionsAccessed().add(coll);
            child.getCollectionDomains().put(coll, configLoader.detectCollectionDomain(coll));
            child.getCollectionSources().put(coll, wasConcatShape
                    ? "CALL_SITE_CONCAT_PROPAGATION" : "CALL_SITE_PARAM_PROPAGATION");
        }
        // The PARAM entry (or entries — the SAME parameter can be captured more than once by
        // collectCollections, e.g. once at the getCollection(...) call itself and again via the
        // immediately-chained find()/insertOne()/etc. propagation) is now stale — remove every
        // PARAM ref for that SAME variable name so the node doesn't report both a resolved
        // collection AND a leftover unresolved dynamic ref for the very same underlying value.
        // Scoped to the matched variableName (not "all PARAM refs") so an unrelated second
        // parameter used as a different dynamic collection reference on the same node, if one
        // ever exists, is left untouched rather than being silently discarded.
        Object paramVarName = paramRef.get("variableName");
        child.getDynamicCollectionRefs().removeIf(r ->
                "PARAM".equals(r.get("varKind")) && Objects.equals(r.get("variableName"), paramVarName));

        Object opType = paramRef.get("operationType");
        if (opType instanceof String opStr) {
            child.getOperationTypes().add(opStr);
            if (child.getOperationType() == null) child.setOperationType(opStr);
        }
        child.setHasDbInteraction(true);
        return child;
    }

    // ── CALL-009: JDBC/SQL table-name resolution ────────────────────────────────────────────
    // Parallel to the Mongo mechanisms above (single-hop, bounded multi-hop, concat-fold,
    // Class-derivation) rather than a generalization of them: JDBC's traced value is a raw SQL
    // STRING that must still be parsed for its table name(s) once resolved to a literal, whereas
    // Mongo's traced value already IS the collection name. That one extra step (SQL parsing after
    // resolution) is a real structural difference, not just a naming one — folding it into the
    // Mongo-shaped mechanisms above would mean threading a "how do I turn a resolved value into a
    // reportable name" strategy parameter through every one of them, a materially bigger and
    // riskier refactor than this task's own established small-increment discipline calls for.
    // Deliberately keyed off distinct "SQL_"-prefixed varKind tags (see
    // JpaMethodDetector.captureDynamicSql/captureDynamicClass) so this never collides with, or
    // races, the Mongo-specific handling above on the SAME dynamicCollectionRefs list.

    /**
     * CALL-009: JDBC-parallel of {@link #tagGeneralParamCallSite} — resolves an unresolved
     * {@code SQL_PARAM}-kind (SQL-string) or {@code SQL_PARAM_CLASS}-kind (entity Class&lt;?&gt;)
     * dynamicCollectionRefs entry left on the child node by JpaMethodDetector, using this call
     * site's own argument (single-hop), walking up {@code ancestorCallChain} when that argument is
     * itself an unresolved pass-through (bounded multi-hop, same {@link #MAX_HOP_DEPTH}), and
     * folding a narrow `literal + param`/`param + literal` SQL-string concatenation shape when
     * present. Returns null when nothing changed (mirrors every other tag* method's contract).
     */
    private CallNode tagGeneralSqlCallSite(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain,
                                            ResolutionContext ctx) {
        List<Map<String, Object>> existingRefs = original.getDynamicCollectionRefs();
        if (existingRefs == null || existingRefs.isEmpty()) return null;
        Map<String, Object> sqlRef = null, classRef = null;
        for (Map<String, Object> ref : existingRefs) {
            Object kind = ref.get("varKind");
            if (kind instanceof String ks && ks.startsWith("SQL_")) {
                if (ks.endsWith("_CLASS")) {
                    if (classRef == null) classRef = ref;
                } else if (sqlRef == null) {
                    sqlRef = ref;
                }
            }
        }
        if (sqlRef == null) {
            if (classRef == null) return null;
            return tagSqlClassCallSite(original, call, ancestorCallChain, classRef, ctx);
        }

        // CALL-009 concat shapes (same defect the ES path had, fixed there first): the CHILD's own
        // SQL_ ref is where a `literal + param` SQL concatenation is recorded (the callee's body
        // built the SQL from its parameter — ownerClass repurposed to carry the literal SQL text).
        // resolveSqlAcrossHops consults only the CALL SITE's own args and walks ANCESTORS for
        // pass-throughs — it never sees the concat operands, so a caller-supplied table name can
        // never be folded into it and the ref stayed unresolved forever. When the child's ref is a
        // CONCAT_* kind, reconstruct the concat-shaped CallSiteSql from the ref and let
        // foldSqlConcatIfPossible walk the ancestors for the operand value. The eager resolution
        // path stays correct for the plain SQL_PARAM pass-through shape.
        String sqlRefKind = String.valueOf(sqlRef.get("varKind"));
        JpaMethodDetector.CallSiteSql css;
        if (sqlRefKind.startsWith("SQL_CONCAT")) {
            css = new JpaMethodDetector.CallSiteSql(null,
                    new MethodCallInfo.DynamicArgSource(sqlRefKind.substring(4),
                            String.valueOf(sqlRef.get("variableName")),
                            String.valueOf(sqlRef.get("ownerClass"))));
        } else {
            css = resolveSqlAcrossHops(call, ancestorCallChain);
        }
        boolean wasConcat = isSqlConcatShape(css);
        css = foldSqlConcatIfPossible(css, ancestorCallChain);
        if (css == null || css.literalSql() == null) return null; // still unresolved — nothing gained

        Set<String> tables = SqlStatementParser.extractTableNames(css.literalSql());
        tables.removeIf(t -> t == null || t.isBlank() || "?".equals(t));
        if (tables.isEmpty()) return null;
        String opType = SqlStatementParser.inferOperationType(css.literalSql());

        CallNode child = original.copyForCallSiteTagging();
        for (String table : tables) {
            if (!child.getCollectionsAccessed().contains(table)) {
                child.getCollectionsAccessed().add(table);
                child.getCollectionDomains().put(table, configLoader.detectCollectionDomain(table));
                child.getCollectionSources().put(table, wasConcat
                        ? "CALL_SITE_SQL_CONCAT_PROPAGATION" : "CALL_SITE_SQL_PARAM_PROPAGATION");
            }
        }
        Object refVarName = sqlRef.get("variableName");
        child.getDynamicCollectionRefs().removeIf(r -> {
            Object k = r.get("varKind");
            return k instanceof String ks && ks.startsWith("SQL_") && !ks.endsWith("_CLASS")
                    && Objects.equals(r.get("variableName"), refVarName);
        });
        if (opType != null) {
            child.getOperationTypes().add(opType);
            if (child.getOperationType() == null) child.setOperationType(opType);
        }
        child.setHasDbInteraction(true);
        return child;
    }

    /** CALL-009/CALL-014 (§215's own documented residual, closed): Class&lt;?&gt;-typed counterpart
     *  of the SQL-string resolution above — mirrors {@link #resolveClassAcrossHops}. Now that
     *  {@code ctx} is threaded through from {@link #tagCallSiteCollection}, the CONFIRMED
     *  {@code @Table}/{@code @Entity} tier ({@code ctx.entityTableMap}, the SAME ground-truth map
     *  {@code resolveRepositoryCollections}/{@code JpaMethodDetector.detect}'s own direct-call-site
     *  case already use) is checked FIRST at each hop, exactly matching CALL-004's Mongo tiering
     *  behavior (confirmed annotation match before falling back to a naming-convention guess) —
     *  falling back to the lower-confidence naming-convention heuristic only when the class
     *  genuinely has no annotation entry in {@code ctx.entityTableMap}. */
    private CallNode tagSqlClassCallSite(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain,
                                          Map<String, Object> classRef, ResolutionContext ctx) {
        InvocationRef current = call;
        int hop = 1, idx = 0;
        String resolvedFqn = null;
        while (current != null) {
            JpaMethodDetector.CallSiteClass csc = jpaMethodDetector.resolveGenericClassCallSiteArg(current);
            if (csc == null) return null;
            if (csc.classFqn() != null) { resolvedFqn = csc.classFqn(); break; }
            MethodCallInfo.DynamicArgSource dyn = csc.dynamicArg();
            if (dyn == null || !"PARAM_CLASS".equals(dyn.kind())) return null;
            if (hop >= MAX_HOP_DEPTH || ancestorCallChain == null || idx >= ancestorCallChain.size()) return null;
            current = ancestorCallChain.get(idx);
            idx++;
            hop++;
        }
        if (resolvedFqn == null) return null;

        Map<String, String> entityTableMap = (ctx != null && ctx.entityTableMap != null)
                ? ctx.entityTableMap : Map.of();
        String table = resolveEntityFqnToCollection(resolvedFqn, entityTableMap);
        String source = "CALL_SITE_SQL_CLASS_PROPAGATION";
        if (table == null) {
            table = deriveTableNameByNamingConvention(resolvedFqn);
            source = "CALL_SITE_SQL_CLASS_NAMING_HEURISTIC";
        }
        if (table == null) return null;

        CallNode child = original.copyForCallSiteTagging();
        if (!child.getCollectionsAccessed().contains(table)) {
            child.getCollectionsAccessed().add(table);
            child.getCollectionDomains().put(table, configLoader.detectCollectionDomain(table));
            child.getCollectionSources().put(table, source);
        }
        Object varName = classRef.get("variableName");
        child.getDynamicCollectionRefs().removeIf(r -> {
            Object k = r.get("varKind");
            return k instanceof String ks && ks.startsWith("SQL_") && ks.endsWith("_CLASS")
                    && Objects.equals(r.get("variableName"), varName);
        });
        Object opType = classRef.get("operationType");
        if (opType instanceof String opStr) {
            child.getOperationTypes().add(opStr);
            if (child.getOperationType() == null) child.setOperationType(opStr);
        }
        child.setHasDbInteraction(true);
        return child;
    }

    /** CALL-009: lower-confidence naming-convention derivation for a JPA entity class with no
     *  reachable {@code @Table}/{@code @Entity} name at this call site (ctx.entityTableMap is not
     *  threaded through the ancestor-chain walk here — see {@link #tagSqlClassCallSite}'s docs) —
     *  CamelCase entity simple name minus a common suffix, snake_cased and pluralized. Mirrors
     *  {@code MongoMethodDetector}'s CamelCase → UPPER_SNAKE convention but lowercase/snake_case,
     *  matching typical relational-table naming (e.g. {@code OrderItemEntity} -> {@code order_items}). */
    private static String deriveTableNameByNamingConvention(String fqn) {
        if (fqn == null || fqn.isBlank()) return null;
        int dot = fqn.lastIndexOf('.');
        String simple = dot > 0 ? fqn.substring(dot + 1) : fqn;
        String stem = simple.replaceAll("(Entity|Model|Dto|DO)$", "");
        if (stem.isBlank()) return null;
        String snake = stem.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT);
        return pluralize(snake);
    }

    /** CALL-009: JDBC-parallel of {@link #resolveAcrossHops} — bounded multi-hop walk resolving a
     *  SQL-string call-site argument, stopping as soon as a literal is found, a non-PARAM/null
     *  result is hit, or {@link #MAX_HOP_DEPTH} is reached. */
    private JpaMethodDetector.CallSiteSql resolveSqlAcrossHops(InvocationRef call, List<InvocationRef> ancestorCallChain) {
        JpaMethodDetector.CallSiteSql css = jpaMethodDetector.resolveGenericSqlCallSiteArg(call);
        int hop = 1, idx = 0;
        while (isUnresolvedSqlParamPassThrough(css) && hop < MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            css = jpaMethodDetector.resolveGenericSqlCallSiteArg(ancestorCallChain.get(idx));
            idx++;
            hop++;
        }
        return css;
    }

    private static boolean isUnresolvedSqlParamPassThrough(JpaMethodDetector.CallSiteSql css) {
        return css != null && css.literalSql() == null && css.dynamicArg() != null
                && "PARAM".equals(css.dynamicArg().kind());
    }

    private static boolean isSqlConcatShape(JpaMethodDetector.CallSiteSql css) {
        if (css == null || css.literalSql() != null || css.dynamicArg() == null) return false;
        String kind = css.dynamicArg().kind();
        return "CONCAT_LITERAL_PARAM".equals(kind) || "CONCAT_PARAM_LITERAL".equals(kind)
                || "CONCAT_PARAM_PARAM".equals(kind);
    }

    /**
     * CALL-012 (§215's own documented residual, closed): JDBC-parallel of
     * {@link #foldTwoParamConcatIfPossible} — folds the `param + param` SQL-string concatenation
     * shape (neither operand a same-method literal), chasing both operand names independently up
     * {@code ancestorCallChain}. Same by-name-only disambiguation discipline, same partial-fold
     * honesty contract when one operand's chain runs out but the other resolves. See
     * {@link #foldTwoParamConcatIfPossible}'s own docs for the full walk-through — this is the
     * exact same technique, adapted for {@link JpaMethodDetector.CallSiteSql}.
     */
    private JpaMethodDetector.CallSiteSql foldTwoParamSqlConcatIfPossible(
            JpaMethodDetector.CallSiteSql css, List<InvocationRef> ancestorCallChain) {
        String name1 = css.dynamicArg().name();
        String name2 = css.dynamicArg().ownerClass(); // repurposed field — see DynamicArgSource javadoc
        String resolved1 = null, resolved2 = null;

        int hop = 1, idx = 0;
        while ((resolved1 == null || resolved2 == null) && hop < MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();

            Set<String> paramNames = new LinkedHashSet<>();
            if (dynArgs != null) {
                for (MethodCallInfo.DynamicArgSource dyn : dynArgs) {
                    if (!"PARAM".equals(dyn.kind())) return css;
                    boolean matchesOperand = (resolved1 == null && name1.equals(dyn.name()))
                            || (resolved2 == null && name2.equals(dyn.name()));
                    if (!matchesOperand) return css;
                    paramNames.add(dyn.name());
                }
            }
            if (paramNames.size() > 2) return css;

            boolean n1StillForwarded = resolved1 == null && paramNames.contains(name1);
            boolean n2StillForwarded = resolved2 == null && paramNames.contains(name2);

            List<String> distinctLiterals = literalArgs == null ? List.of()
                    : literalArgs.stream().distinct().toList();
            long distinctLiteralCount = distinctLiterals.size();

            if (resolved1 == null && resolved2 == null && paramNames.isEmpty() && distinctLiteralCount == 2) {
                resolved1 = distinctLiterals.get(0);
                resolved2 = distinctLiterals.get(1);
                continue;
            }

            if (n1StillForwarded && n2StillForwarded) {
                if (distinctLiteralCount != 0) return css;
                continue;
            }
            if (n1StillForwarded) {
                if (resolved2 != null || distinctLiteralCount != 1) return css;
                resolved2 = literalArgs.get(0);
                continue;
            }
            if (n2StillForwarded) {
                if (resolved1 != null || distinctLiteralCount != 1) return css;
                resolved1 = literalArgs.get(0);
                continue;
            }
            return css;
        }

        if (resolved1 == null && resolved2 == null) return css;
        if (resolved1 != null && resolved2 != null) {
            return new JpaMethodDetector.CallSiteSql(resolved1 + resolved2, null);
        }
        String partial = resolved1 != null
                ? resolved1 + "{" + name2 + "}"
                : "{" + name1 + "}" + resolved2;
        return new JpaMethodDetector.CallSiteSql(partial, null);
    }

    /**
     * CALL-009: JDBC-parallel of {@link #foldConcatIfPossible}, handling both the single-operand
     * `literal + param`/`param + literal` SQL-string shape and (CALL-012, §215's own documented
     * residual, now closed) the `param + param` shape via
     * {@link #foldTwoParamSqlConcatIfPossible}. Never guesses: only resolves when an ancestor call
     * site is unambiguous (exactly one literal / one PARAM pass-through).
     */
    private JpaMethodDetector.CallSiteSql foldSqlConcatIfPossible(
            JpaMethodDetector.CallSiteSql css, List<InvocationRef> ancestorCallChain) {
        if (!isSqlConcatShape(css)) return css;
        if ("CONCAT_PARAM_PARAM".equals(css.dynamicArg().kind())) {
            return foldTwoParamSqlConcatIfPossible(css, ancestorCallChain);
        }
        boolean literalFirst = "CONCAT_LITERAL_PARAM".equals(css.dynamicArg().kind());
        String literalPart = css.dynamicArg().ownerClass(); // repurposed field — see DynamicArgSource javadoc

        int hop = 1, idx = 0;
        while (hop < MAX_HOP_DEPTH && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();
            boolean hasOneLiteral = literalArgs != null && !literalArgs.isEmpty()
                    && literalArgs.stream().distinct().count() == 1;
            boolean hasNoDynamic = dynArgs == null || dynArgs.isEmpty();
            boolean hasOneDynamic = dynArgs != null && dynArgs.size() == 1;
            boolean hasNoLiteral = literalArgs == null || literalArgs.isEmpty();

            if (hasOneLiteral && hasNoDynamic) {
                String paramValue = literalArgs.get(0);
                String finalSql = literalFirst ? literalPart + paramValue : paramValue + literalPart;
                return new JpaMethodDetector.CallSiteSql(finalSql, null);
            }
            if (hasOneDynamic && hasNoLiteral) {
                if ("PARAM".equals(dynArgs.get(0).kind())) {
                    continue; // unambiguous single pass-through — keep walking
                }
                return css; // resolved-but-non-literal — stop
            }
            return css; // ambiguous — stay unresolved
        }
        return css; // bound reached / ancestor chain exhausted
    }

    // ── CALL-011: Elasticsearch index-name resolution ───────────────────────────────────────
    // Parallel to the Mongo mechanism above (NOT the JDBC one) — Elasticsearch's traced value (an
    // index name) already IS the reportable value, exactly like Mongo's collection name, with no
    // post-resolution parsing step needed (unlike JDBC's raw-SQL-string case). Deliberately keyed
    // off distinct "ES_"-prefixed varKind tags (see ElasticsearchMethodDetector.detectWithDynamic)
    // so this never collides with, or races, the Mongo/JDBC-specific handling above on the SAME
    // dynamicCollectionRefs list.

    /** CALL-011: ES-parallel of {@link #tagGeneralParamCallSite} — resolves an unresolved
     *  {@code ES_}-prefixed dynamicCollectionRefs entry left on the child node by
     *  ElasticsearchMethodDetector, using this call site's own argument (single-hop), walking up
     *  {@code ancestorCallChain} when that argument is itself an unresolved pass-through (bounded
     *  multi-hop, same {@link #MAX_HOP_DEPTH}), and folding a narrow `literal + param`/
     *  `param + literal` index-name concatenation shape when present. */
    private CallNode tagGeneralEsCallSite(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain,
                                           ResolutionContext ctx) {
        List<Map<String, Object>> existingRefs = original.getDynamicCollectionRefs();
        if (existingRefs == null || existingRefs.isEmpty()) return null;
        Map<String, Object> esRef = null, esClassRef = null;
        for (Map<String, Object> ref : existingRefs) {
            Object kind = ref.get("varKind");
            if (kind instanceof String ks && ks.startsWith("ES_")) {
                if (ks.endsWith("_CLASS")) {
                    if (esClassRef == null) esClassRef = ref;
                } else if (esRef == null) {
                    esRef = ref;
                }
            }
        }
        if (esRef == null) {
            // CALL-017: no plain ES_PARAM/ES_LOCAL_VAR pass-through — check for an
            // ES_PARAM_CLASS/ES_LOCAL_VAR_CLASS one (an entity Class<?> threaded down through 2+
            // calls before being used to derive an Elasticsearch index name).
            if (esClassRef == null) return null;
            return tagEsClassCallSite(original, call, ancestorCallChain, esClassRef, ctx);
        }

        Map<String, String> entityMap = (ctx != null && ctx.entityCollectionMap != null) ? ctx.entityCollectionMap : Map.of();

        // CALL-011/016 concat shapes: the CHILD's own ES_ ref is where a `literal + param` /
        // `param + param` index concatenation is recorded (the callee's body built the index name
        // from its parameter). In that case the caller's literal — which resolveEsAcrossHops would
        // eagerly walk up and return as the WHOLE index name — is only one OPERAND of the child's
        // concatenation: feeding it to the eager path reported the bare operand ("east") as the
        // resolved index instead of the folded value ("orders_east"). So when the child's ref is a
        // CONCAT_* kind, reconstruct the concat-shaped CallSiteIndex from the ref (both operands
        // ride on it: variableName = the param, ownerClass = the repurposed literal part) and let
        // foldEsConcatIfPossible walk the ancestors for the operand value itself. The eager
        // literal path stays correct for the plain ES_PARAM pass-through shape.
        String esRefKind = String.valueOf(esRef.get("varKind"));
        ElasticsearchMethodDetector.CallSiteIndex csi;
        if (esRefKind.startsWith("ES_CONCAT")) {
            csi = new ElasticsearchMethodDetector.CallSiteIndex(null,
                    new MethodCallInfo.DynamicArgSource(esRefKind.substring(3),
                            String.valueOf(esRef.get("variableName")),
                            String.valueOf(esRef.get("ownerClass"))));
        } else {
            csi = resolveEsAcrossHops(call, ancestorCallChain, entityMap);
        }
        boolean wasConcat = isEsConcatShape(csi);
        csi = foldEsConcatIfPossible(csi, ancestorCallChain);
        if (csi == null || csi.indexName() == null) return null; // still unresolved — nothing gained

        String index = csi.indexName();
        CallNode child = original.copyForCallSiteTagging();
        if (!child.getCollectionsAccessed().contains(index)) {
            child.getCollectionsAccessed().add(index);
            child.getCollectionDomains().put(index, configLoader.detectCollectionDomain(index));
            child.getCollectionSources().put(index, wasConcat
                    ? "CALL_SITE_ES_CONCAT_PROPAGATION" : "CALL_SITE_ES_PARAM_PROPAGATION");
        }
        Object refVarName = esRef.get("variableName");
        child.getDynamicCollectionRefs().removeIf(r -> {
            Object k = r.get("varKind");
            return k instanceof String ks && ks.startsWith("ES_") && Objects.equals(r.get("variableName"), refVarName);
        });
        Object opType = esRef.get("operationType");
        if (opType instanceof String opStr) {
            child.getOperationTypes().add(opStr);
            if (child.getOperationType() == null) child.setOperationType(opStr);
        }
        child.setHasDbInteraction(true);
        return child;
    }

    /**
     * CALL-017: Elasticsearch-parallel of {@link #tagSqlClassCallSite}/{@link #resolveClassAcrossHops}
     * — resolves an unresolved {@code ES_PARAM_CLASS}/{@code ES_LOCAL_VAR_CLASS}-kind
     * dynamicCollectionRefs entry left on the child node (an entity {@code Class<?>} threaded
     * through 2+ method calls before reaching an ES {@code search}/{@code save}/... call site).
     * Walks {@code ancestorCallChain} one hop at a time using the SAME domain-agnostic
     * {@code __class:FQN}-literal / {@code PARAM_CLASS} pass-through detection Mongo's
     * {@link #resolveClassAcrossHops} already uses (via {@link #firstClassLiteralArg}/
     * {@link #firstClassDynamicArg} — no new capture machinery, reused verbatim), bounded at the
     * same {@link #MAX_HOP_DEPTH}. Once a Class literal is found, resolves it in two tiers, exactly
     * mirroring CALL-004's Mongo tiering and CALL-014's JDBC tiering:
     * <ol>
     *   <li>CONFIRMED: {@code ctx.entityIndexMap} — populated from
     *       {@code @Document(indexName = "...")} specifically (CALL-017's own new map, distinct
     *       from Mongo's {@code entityCollectionMap}/{@code collection} attribute on the SAME
     *       annotation type) — tagged {@code CALL_SITE_ES_CLASS_PROPAGATION}.</li>
     *   <li>HEURISTIC fallback when no annotation ground truth exists: the SAME
     *       {@link #deriveCollectionNameByNamingConvention} naming heuristic Mongo's CALL-008
     *       already established (CamelCase entity name minus a common suffix, lowercased,
     *       pluralized) — reused verbatim, not reinvented — tagged
     *       {@code CALL_SITE_ES_CLASS_NAMING_HEURISTIC}.</li>
     * </ol>
     * Never guesses beyond these two tiers; returns null (leaves the ref unresolved) when neither
     * tier produces a name or the walk runs out.
     */
    private CallNode tagEsClassCallSite(CallNode original, InvocationRef call, List<InvocationRef> ancestorCallChain,
                                         Map<String, Object> classRef, ResolutionContext ctx) {
        InvocationRef current = call;
        int hop = 1, idx = 0;
        String resolvedFqn = null;
        while (current != null) {
            String fqn = firstClassLiteralArg(current.stringArgs());
            if (fqn != null) { resolvedFqn = fqn; break; }
            MethodCallInfo.DynamicArgSource dyn = firstClassDynamicArg(current.dynamicArgSources());
            if (dyn == null) return null; // not an unambiguous class pass-through
            if (hop >= MAX_HOP_DEPTH || ancestorCallChain == null || idx >= ancestorCallChain.size()) return null;
            current = ancestorCallChain.get(idx);
            idx++;
            hop++;
        }
        if (resolvedFqn == null) return null;

        Map<String, String> entityIndexMap = (ctx != null && ctx.entityIndexMap != null)
                ? ctx.entityIndexMap : Map.of();
        String index = resolveEntityFqnToCollection(resolvedFqn, entityIndexMap);
        String source = "CALL_SITE_ES_CLASS_PROPAGATION";
        if (index == null) {
            index = deriveCollectionNameByNamingConvention(resolvedFqn);
            source = "CALL_SITE_ES_CLASS_NAMING_HEURISTIC";
        }
        if (index == null) return null;

        CallNode child = original.copyForCallSiteTagging();
        if (!child.getCollectionsAccessed().contains(index)) {
            child.getCollectionsAccessed().add(index);
            child.getCollectionDomains().put(index, configLoader.detectCollectionDomain(index));
            child.getCollectionSources().put(index, source);
        }
        Object varName = classRef.get("variableName");
        child.getDynamicCollectionRefs().removeIf(r -> {
            Object k = r.get("varKind");
            return k instanceof String ks && ks.startsWith("ES_") && ks.endsWith("_CLASS")
                    && Objects.equals(r.get("variableName"), varName);
        });
        Object opType = classRef.get("operationType");
        if (opType instanceof String opStr) {
            child.getOperationTypes().add(opStr);
            if (child.getOperationType() == null) child.setOperationType(opStr);
        }
        child.setHasDbInteraction(true);
        return child;
    }

    private ElasticsearchMethodDetector.CallSiteIndex resolveEsAcrossHops(
            InvocationRef call, List<InvocationRef> ancestorCallChain, Map<String, String> entityMap) {
        ElasticsearchMethodDetector.CallSiteIndex csi = elasticsearchMethodDetector.resolveGenericCallSiteArg(call, entityMap);
        int hop = 1, idx = 0;
        while (isUnresolvedEsParamPassThrough(csi) && hop < MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            csi = elasticsearchMethodDetector.resolveGenericCallSiteArg(ancestorCallChain.get(idx), entityMap);
            idx++;
            hop++;
        }
        return csi;
    }

    private static boolean isUnresolvedEsParamPassThrough(ElasticsearchMethodDetector.CallSiteIndex csi) {
        return csi != null && csi.indexName() == null && csi.dynamicArg() != null
                && "PARAM".equals(csi.dynamicArg().kind());
    }

    private static boolean isEsConcatShape(ElasticsearchMethodDetector.CallSiteIndex csi) {
        if (csi == null || csi.indexName() != null || csi.dynamicArg() == null) return false;
        String kind = csi.dynamicArg().kind();
        return "CONCAT_LITERAL_PARAM".equals(kind) || "CONCAT_PARAM_LITERAL".equals(kind)
                || "CONCAT_PARAM_PARAM".equals(kind);
    }

    /**
     * CALL-016 (§221's own documented completeness-audit fix): ES-parallel of
     * {@link #foldTwoParamConcatIfPossible}/{@link #foldTwoParamSqlConcatIfPossible} — folds the
     * `param + param` index-name concatenation shape (neither operand a same-method literal),
     * chasing both operand names independently up {@code ancestorCallChain}. Exact same
     * by-name-only disambiguation discipline and partial-fold honesty contract as the Mongo/JDBC
     * originals; this was the one documented asymmetry left over from CALL-011 (§217/§218), which
     * only ported the single-operand fold.
     */
    private ElasticsearchMethodDetector.CallSiteIndex foldTwoParamEsConcatIfPossible(
            ElasticsearchMethodDetector.CallSiteIndex csi, List<InvocationRef> ancestorCallChain) {
        String name1 = csi.dynamicArg().name();
        String name2 = csi.dynamicArg().ownerClass(); // repurposed field — see DynamicArgSource javadoc
        String resolved1 = null, resolved2 = null;

        int hop = 1, idx = 0;
        while ((resolved1 == null || resolved2 == null) && hop < MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();

            Set<String> paramNames = new LinkedHashSet<>();
            if (dynArgs != null) {
                for (MethodCallInfo.DynamicArgSource dyn : dynArgs) {
                    if (!"PARAM".equals(dyn.kind())) return csi;
                    boolean matchesOperand = (resolved1 == null && name1.equals(dyn.name()))
                            || (resolved2 == null && name2.equals(dyn.name()));
                    if (!matchesOperand) return csi;
                    paramNames.add(dyn.name());
                }
            }
            if (paramNames.size() > 2) return csi;

            boolean n1StillForwarded = resolved1 == null && paramNames.contains(name1);
            boolean n2StillForwarded = resolved2 == null && paramNames.contains(name2);

            List<String> distinctLiterals = literalArgs == null ? List.of()
                    : literalArgs.stream().distinct().toList();
            long distinctLiteralCount = distinctLiterals.size();

            if (resolved1 == null && resolved2 == null && paramNames.isEmpty() && distinctLiteralCount == 2) {
                resolved1 = distinctLiterals.get(0);
                resolved2 = distinctLiterals.get(1);
                continue;
            }

            if (n1StillForwarded && n2StillForwarded) {
                if (distinctLiteralCount != 0) return csi;
                continue;
            }
            if (n1StillForwarded) {
                if (resolved2 != null || distinctLiteralCount != 1) return csi;
                resolved2 = literalArgs.get(0);
                continue;
            }
            if (n2StillForwarded) {
                if (resolved1 != null || distinctLiteralCount != 1) return csi;
                resolved1 = literalArgs.get(0);
                continue;
            }
            return csi;
        }

        if (resolved1 == null && resolved2 == null) return csi;
        if (resolved1 != null && resolved2 != null) {
            return new ElasticsearchMethodDetector.CallSiteIndex(resolved1 + resolved2, null);
        }
        String partial = resolved1 != null
                ? resolved1 + "{" + name2 + "}"
                : "{" + name1 + "}" + resolved2;
        return new ElasticsearchMethodDetector.CallSiteIndex(partial, null);
    }

    /** CALL-011: ES-parallel of {@link #foldSqlConcatIfPossible}, narrowed to the single-operand
     *  `literal + param`/`param + literal` index-name shape (CALL-016: now also dispatches the
     *  `param + param` shape to {@link #foldTwoParamEsConcatIfPossible}). */
    private ElasticsearchMethodDetector.CallSiteIndex foldEsConcatIfPossible(
            ElasticsearchMethodDetector.CallSiteIndex csi, List<InvocationRef> ancestorCallChain) {
        if (!isEsConcatShape(csi)) return csi;
        if ("CONCAT_PARAM_PARAM".equals(csi.dynamicArg().kind())) {
            return foldTwoParamEsConcatIfPossible(csi, ancestorCallChain);
        }
        boolean literalFirst = "CONCAT_LITERAL_PARAM".equals(csi.dynamicArg().kind());
        String literalPart = csi.dynamicArg().ownerClass(); // repurposed field — see DynamicArgSource javadoc

        int hop = 1, idx = 0;
        while (hop < MAX_HOP_DEPTH && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();
            boolean hasOneLiteral = literalArgs != null && !literalArgs.isEmpty()
                    && literalArgs.stream().distinct().count() == 1;
            boolean hasNoDynamic = dynArgs == null || dynArgs.isEmpty();
            boolean hasOneDynamic = dynArgs != null && dynArgs.size() == 1;
            boolean hasNoLiteral = literalArgs == null || literalArgs.isEmpty();

            if (hasOneLiteral && hasNoDynamic) {
                String paramValue = literalArgs.get(0);
                String finalIndex = literalFirst ? literalPart + paramValue : paramValue + literalPart;
                return new ElasticsearchMethodDetector.CallSiteIndex(finalIndex, null);
            }
            if (hasOneDynamic && hasNoLiteral) {
                if ("PARAM".equals(dynArgs.get(0).kind())) {
                    continue; // unambiguous single pass-through — keep walking
                }
                return csi; // resolved-but-non-literal — stop
            }
            return csi; // ambiguous — stay unresolved
        }
        return csi; // bound reached / ancestor chain exhausted
    }

    /**
     * CALL-002: resolve a call site's argument, walking up through {@code ancestorCallChain} one
     * hop at a time when the current attempt resolves to an unresolved PARAM-kind pass-through
     * (the caller is just forwarding a value it itself received as a parameter, rather than
     * supplying a literal/constant). Each element of {@code ancestorCallChain} is the call site
     * that produced the current stack frame, nearest ancestor first (index 0 = the call that
     * invoked the current caller; index 1 = the call that invoked THAT caller; etc.) — threaded
     * down through CallTreeBuilder's existing top-down recursion, so no separate upward tree walk
     * or extra bytecode pass is needed.
     *
     * Bounded at {@link #MAX_HOP_DEPTH} total call sites consulted (the original call site counts
     * as hop 1). Stops and returns whatever the last attempt produced (resolved or not) as soon as
     * either: a literal/constant is found, a non-PARAM (or null) result is hit — nothing further
     * up the chain could possibly resolve it any better via this same mechanism — the bound is
     * reached, or the ancestor chain runs out (shallower real call chain than the bound). Never
     * recurses beyond the supplied chain and never grows it — CallTreeBuilder is solely
     * responsible for capping how many ancestors it threads down.
     */
    private MongoMethodDetector.CallSiteCollection resolveAcrossHops(InvocationRef call, List<InvocationRef> ancestorCallChain) {
        MongoMethodDetector.CallSiteCollection csc = mongoMethodDetector.resolveGenericCallSiteArg(call);
        int hop = 1;
        int idx = 0;
        while (isUnresolvedParamPassThrough(csc) && hop < MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            csc = mongoMethodDetector.resolveGenericCallSiteArg(ancestorCallChain.get(idx));
            idx++;
            hop++;
        }
        return csc;
    }

    /**
     * CALL-004: walks {@code ancestorCallChain} the same bounded way {@link #resolveAcrossHops}
     * does for a String PARAM pass-through, but for a {@code Class<?>}-typed value (kind
     * {@code PARAM_CLASS}): at each call site, either (a) the argument is a direct
     * {@code __class:FQN} literal (a {@code .class} literal passed at that hop) — resolve it
     * immediately through {@code entityCollectionMap}, the exact same @Document-annotation →
     * collection-name lookup {@code AggregationDetector.resolveClassLiteralCollection}/
     * {@code MongoMethodDetector}'s own inline block already use for the direct-call case (FQN
     * first, then simple name fallback) — reused here rather than duplicated logic invented fresh;
     * or (b) the argument is itself an unresolved {@code PARAM_CLASS} pass-through — keep walking,
     * up to {@link #MAX_HOP_DEPTH} total call sites; anything else (no recognizable class-typed
     * argument at that hop) stops the walk unresolved. Deliberately does not attempt the
     * no-@Document naming-convention heuristic (§204 Pattern B's {@code InvoiceEntity} →
     * {@code "invoices"} derivation) — that is a materially different, riskier guess with no
     * annotation ground truth and is intentionally left out of this mechanism's scope.
     */
    private ClassResolution resolveClassAcrossHops(InvocationRef call, List<InvocationRef> ancestorCallChain,
                                                    Map<String, String> entityCollectionMap) {
        InvocationRef current = call;
        int hop = 1;
        int idx = 0;
        while (current != null) {
            String fqn = firstClassLiteralArg(current.stringArgs());
            if (fqn != null) {
                String annotated = resolveEntityFqnToCollection(fqn, entityCollectionMap);
                if (annotated != null) return new ClassResolution(annotated, "CALL_SITE_CLASS_PROPAGATION");
                // CALL-008 (§204 Pattern B's own residual gap): the class carries no
                // @Document annotation ground truth — fall back to the SAME lower-confidence
                // naming-convention heuristic below, tagged distinctly so callers/reports can
                // tell a confirmed annotation match apart from a guessed one.
                String heuristic = deriveCollectionNameByNamingConvention(fqn);
                return heuristic == null ? null : new ClassResolution(heuristic, "CALL_SITE_CLASS_NAMING_HEURISTIC");
            }
            MethodCallInfo.DynamicArgSource dyn = firstClassDynamicArg(current.dynamicArgSources());
            if (dyn == null || !"PARAM_CLASS".equals(dyn.kind())) return null; // not an unambiguous pass-through
            if (hop >= MAX_HOP_DEPTH || ancestorCallChain == null || idx >= ancestorCallChain.size()) return null;
            current = ancestorCallChain.get(idx);
            idx++;
            hop++;
        }
        return null;
    }

    /** CALL-008: pairs a resolved collection name with WHICH mechanism resolved it —
     *  the pre-existing, higher-confidence {@code @Document}-annotation lookup
     *  ({@code CALL_SITE_CLASS_PROPAGATION}) or the newer, lower-confidence bare naming-convention
     *  heuristic ({@code CALL_SITE_CLASS_NAMING_HEURISTIC}) — so {@code collectionSources} always
     *  reflects the true confidence of a Class&lt;?&gt;-derived collection name. */
    private record ClassResolution(String collectionName, String source) {}

    /**
     * CALL-008 (§204 Pattern B's own residual gap): when a propagated
     * {@code Class<?>} carries NO {@code @Document} annotation (so there is no ground truth to
     * look up), derive a best-effort collection name purely from the class's own simple name —
     * the exact heuristic §204 Pattern B's own fixture uses ({@code InvoiceEntity} →
     * {@code "invoices"}): strip a common entity-class suffix using the SAME regex this file
     * already applies elsewhere for the analogous JPA-repository naming-convention case (see
     * {@code resolveRepositoryCollections}'s "CamelCase → UPPER_SNAKE" Priority-1.5 block,
     * {@code entitySimple.replaceAll("(Entity|Document|Model|Dto|DO)$", "")}), lowercase it, then
     * apply simple English pluralization (append {@code "s"}, or {@code "es"} after
     * s/x/z/ch/sh, or {@code y -> ies} after a consonant+y) — deliberately NOT a full
     * pluralization library, matching this task's own narrow scope. Marked with a distinct,
     * lower-confidence source tag by the caller ({@code CALL_SITE_CLASS_NAMING_HEURISTIC}) so this
     * guessed result is never confused with a confirmed {@code @Document} match.
     */
    private static String deriveCollectionNameByNamingConvention(String fqn) {
        if (fqn == null || fqn.isBlank()) return null;
        int dot = fqn.lastIndexOf('.');
        String simple = dot > 0 ? fqn.substring(dot + 1) : fqn;
        String stem = simple.replaceAll("(Entity|Document|Model|Dto|DO)$", "");
        if (stem.isBlank()) return null;
        String lower = stem.toLowerCase(Locale.ROOT);
        return pluralize(lower);
    }

    /** Deliberately simple English pluralization — not a library, just the handful of common
     *  suffix rules needed for collection-name-shaped words (see
     *  {@link #deriveCollectionNameByNamingConvention}'s own docs for scope). */
    private static String pluralize(String word) {
        if (word.endsWith("s") || word.endsWith("x") || word.endsWith("z")
                || word.endsWith("ch") || word.endsWith("sh")) {
            return word + "es";
        }
        if (word.endsWith("y") && word.length() > 1
                && "aeiou".indexOf(word.charAt(word.length() - 2)) < 0) {
            return word.substring(0, word.length() - 1) + "ies";
        }
        return word + "s";
    }

    /** First {@code __class:FQN} literal argument, if any (the FQN itself, prefix stripped). */
    private static String firstClassLiteralArg(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (arg != null && arg.startsWith("__class:")) return arg.substring(8);
        }
        return null;
    }

    /** First {@code PARAM_CLASS}/{@code LOCAL_VAR_CLASS}-kind dynamic argument, if any. */
    private static MethodCallInfo.DynamicArgSource firstClassDynamicArg(List<MethodCallInfo.DynamicArgSource> args) {
        if (args == null) return null;
        for (MethodCallInfo.DynamicArgSource arg : args) {
            if ("PARAM_CLASS".equals(arg.kind()) || "LOCAL_VAR_CLASS".equals(arg.kind())) return arg;
        }
        return null;
    }

    /** Resolve a {@code Class<?>}'s FQN to its @Document collection name, FQN first then simple
     *  name — identical lookup shape to {@code AggregationDetector.resolveClassLiteralCollection}
     *  and {@code MongoMethodDetector}'s own inline {@code __class:} block, against the SAME
     *  {@code ctx.entityCollectionMap} CollectionResolver already reads elsewhere in this file
     *  (see {@code resolveRepositoryCollections}) — no new map, no new population logic. */
    private static String resolveEntityFqnToCollection(String fqn, Map<String, String> entityCollectionMap) {
        String coll = entityCollectionMap.get(fqn);
        if (coll == null) {
            int dot = fqn.lastIndexOf('.');
            String simple = dot > 0 ? fqn.substring(dot + 1) : fqn;
            coll = entityCollectionMap.get(simple);
        }
        return coll;
    }

    /** True when the resolution attempt found nothing but "this is itself just the caller's own
     *  unresolved parameter being forwarded" — the one case worth walking one hop further up for. */
    private static boolean isUnresolvedParamPassThrough(MongoMethodDetector.CallSiteCollection csc) {
        return csc != null && csc.collectionName() == null && csc.dynamicArg() != null
                && "PARAM".equals(csc.dynamicArg().kind());
    }

    /** True when {@code csc} is one of {@code BytecodeClassParser}'s CALL-003/CALL-008-follow-on
     *  {@code CONCAT_LITERAL_PARAM}/{@code CONCAT_PARAM_LITERAL}/{@code CONCAT_PARAM_PARAM}
     *  dynamic-arg kinds. */
    private static boolean isConcatShape(MongoMethodDetector.CallSiteCollection csc) {
        if (csc == null || csc.collectionName() != null || csc.dynamicArg() == null) return false;
        String kind = csc.dynamicArg().kind();
        return "CONCAT_LITERAL_PARAM".equals(kind) || "CONCAT_PARAM_LITERAL".equals(kind)
                || "CONCAT_PARAM_PARAM".equals(kind);
    }

    /**
     * CALL-003: folds a `literal + param` / `param + literal` concatenation (see
     * {@code BytecodeClassParser.ConcatSlotInfo}) once the PARAM side's own value can be found.
     * Deliberately does NOT reuse {@code resolveGenericCallSiteArg}'s existing "first dynamic arg
     * wins" simplification here — with TWO of a callee's own parameters both forwarded unchanged
     * at the same call site (a real, common shape — see §204 Pattern A's
     * {@code regionAppender.append(prefix, region)}), picking "whichever came first" could fold
     * in the WRONG sibling parameter's value and fabricate an incorrect collection name, which
     * this task explicitly must never do. Instead, this only ever trusts an ancestor call site
     * when it is UNAMBIGUOUS: exactly one literal argument and no dynamic ones (resolves
     * immediately), or exactly one dynamic argument and no literal ones AND that argument is
     * itself an unresolved PARAM pass-through (keep walking — same PARAM semantics CALL-002 already
     * established), up to {@link #MAX_HOP_DEPTH} call sites total. Any other shape at an ancestor
     * (zero candidates, 2+ candidates of either kind, or a resolved-but-non-literal single
     * candidate) stops the walk and returns the ORIGINAL (still-unresolved) csc unchanged — never
     * guesses, never fabricates.
     */
    private MongoMethodDetector.CallSiteCollection foldConcatIfPossible(
            MongoMethodDetector.CallSiteCollection csc, List<InvocationRef> ancestorCallChain) {
        if (!isConcatShape(csc)) return csc;
        if ("CONCAT_PARAM_PARAM".equals(csc.dynamicArg().kind())) {
            return foldTwoParamConcatIfPossible(csc, ancestorCallChain);
        }
        boolean literalFirst = "CONCAT_LITERAL_PARAM".equals(csc.dynamicArg().kind());
        String literalPart = csc.dynamicArg().ownerClass(); // repurposed field — see DynamicArgSource javadoc
        String originalOpType = csc.operationType();

        int hop = 1, idx = 0;
        while (hop < MAX_HOP_DEPTH && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();
            // Same-value duplicates are common here (the SAME single-assignment literal ALOAD can
            // be captured both by the generic every-LDC scan and again by the literal-slot-
            // resolution ALOAD path — a pre-existing, harmless quirk elsewhere in this file's own
            // capture pipeline) — distinct-count, not raw size, is what actually signals ambiguity.
            boolean hasOneLiteral = literalArgs != null && !literalArgs.isEmpty()
                    && literalArgs.stream().distinct().count() == 1;
            boolean hasNoDynamic = dynArgs == null || dynArgs.isEmpty();
            boolean hasOneDynamic = dynArgs != null && dynArgs.size() == 1;
            boolean hasNoLiteral = literalArgs == null || literalArgs.isEmpty();

            if (hasOneLiteral && hasNoDynamic) {
                String paramValue = literalArgs.get(0);
                String finalName = literalFirst ? literalPart + paramValue : paramValue + literalPart;
                return new MongoMethodDetector.CallSiteCollection(finalName, null, originalOpType);
            }
            if (hasOneDynamic && hasNoLiteral) {
                if ("PARAM".equals(dynArgs.get(0).kind())) {
                    continue; // unambiguous single pass-through — keep walking
                }
                return csc; // resolved-but-non-literal (FIELD/LOCAL_VAR/CONCAT_*/STATIC_FIELD) — stop
            }
            return csc; // ambiguous (0 or 2+ candidates, or a literal+dynamic mix) — stay unresolved
        }
        return csc; // bound reached / ancestor chain exhausted — still unresolved, never fabricated
    }

    /**
     * CALL-008 (§209/§213): folds the `param + param` concatenation shape — §204
     * Pattern A's real gap, where NEITHER operand is a same-method literal at the point of
     * concatenation (e.g. {@code prefix + region} inside {@code RegionAppender.append}, both
     * still-unresolved parameters of THAT method). Unlike {@link #foldConcatIfPossible}'s
     * single-operand walk (which already knows one side's literal text and only needs to resolve
     * the other), this must chase BOTH operand names up {@code ancestorCallChain}
     * INDEPENDENTLY, since either one (or both) may still be several hops from a real literal.
     *
     * Kept deliberately narrow and name-based, matching this mechanism's existing safety
     * discipline: at each ancestor call site, this only ever commits to resolving ONE of the two
     * still-unresolved operands to a literal when that hop shows EXACTLY one distinct literal
     * argument and EXACTLY one dynamic argument, AND that dynamic argument's captured variable
     * name equals the OTHER (still-unresolved) operand's current name — i.e. the shape
     * unambiguously identifies which operand is which by name, not by guessing argument
     * position. Any other shape at a hop (0/2+ literals, 0/2+ dynamic args, or a dynamic name
     * that doesn't match either operand) stops the walk for good and returns the ORIGINAL,
     * still-unresolved {@code csc} — same "never fabricate" contract as every other mechanism in
     * this file. Bounded at the same {@link #MAX_HOP_DEPTH} call sites total (hop 1 is the
     * original call site that carried the {@code CONCAT_PARAM_PARAM} entry itself; the walk below
     * starts at hop 2, the nearest ancestor).
     */
    private MongoMethodDetector.CallSiteCollection foldTwoParamConcatIfPossible(
            MongoMethodDetector.CallSiteCollection csc, List<InvocationRef> ancestorCallChain) {
        String name1 = csc.dynamicArg().name();
        String name2 = csc.dynamicArg().ownerClass(); // repurposed field — see DynamicArgSource javadoc
        String originalOpType = csc.operationType();
        String resolved1 = null, resolved2 = null;

        int hop = 1, idx = 0;
        while ((resolved1 == null || resolved2 == null) && hop < MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();

            // Only ever trust PARAM-kind dynamic args at this hop, and only when EVERY one of
            // them carries a name matching one of our two still-unresolved operands — anything
            // else (an unrelated third dynamic arg, a non-PARAM kind, a duplicate name) means
            // this hop's shape is not unambiguously ours to interpret, so refuse to guess.
            Set<String> paramNames = new LinkedHashSet<>();
            if (dynArgs != null) {
                for (MethodCallInfo.DynamicArgSource dyn : dynArgs) {
                    if (!"PARAM".equals(dyn.kind())) return csc;
                    boolean matchesOperand = (resolved1 == null && name1.equals(dyn.name()))
                            || (resolved2 == null && name2.equals(dyn.name()));
                    if (!matchesOperand) return csc;
                    paramNames.add(dyn.name());
                }
            }
            if (paramNames.size() > 2) return csc; // shouldn't happen given the filter above, but be safe

            boolean n1StillForwarded = resolved1 == null && paramNames.contains(name1);
            boolean n2StillForwarded = resolved2 == null && paramNames.contains(name2);

            // Distinct-count, not raw size, signals real ambiguity here — the SAME
            // single-assignment literal ALOAD is routinely captured twice (the generic
            // every-LDC scan AND the literal-slot-resolution ALOAD path), a pre-existing,
            // harmless quirk elsewhere in this file's own capture pipeline (see
            // foldConcatIfPossible's own docs for the identical reasoning).
            List<String> distinctLiterals = literalArgs == null ? List.of()
                    : literalArgs.stream().distinct().toList();
            long distinctLiteralCount = distinctLiterals.size();

            if (resolved1 == null && resolved2 == null && paramNames.isEmpty() && distinctLiteralCount == 2) {
                // Both operands terminate in a real literal at THIS SAME hop, with no dynamic
                // arg at all left to disambiguate by name — the only remaining unambiguous
                // signal is bytecode argument PUSH ORDER, which the distinct literal list still
                // preserves for a call site with exactly two distinct literal String arguments
                // and zero dynamic ones (no interleaved dynamic arg to have disturbed that
                // order). Safe because this is the ONLY shape left once both names have
                // exhausted every other disambiguating signal above.
                resolved1 = distinctLiterals.get(0);
                resolved2 = distinctLiterals.get(1);
                continue;
            }

            if (n1StillForwarded && n2StillForwarded) {
                if (distinctLiteralCount != 0) return csc; // unaccounted literal at this hop — ambiguous
                continue; // both still pass-through, unchanged — walk further up
            }
            if (n1StillForwarded) {
                // Operand 1 still forwarding; operand 2 must resolve to THIS hop's one literal.
                if (resolved2 != null || distinctLiteralCount != 1) return csc;
                resolved2 = literalArgs.get(0);
                continue;
            }
            if (n2StillForwarded) {
                if (resolved1 != null || distinctLiteralCount != 1) return csc;
                resolved1 = literalArgs.get(0);
                continue;
            }
            return csc; // neither still-unresolved operand's name appears — refuse to guess
        }

        if (resolved1 == null && resolved2 == null) return csc; // neither side ever resolved
        if (resolved1 != null && resolved2 != null) {
            return new MongoMethodDetector.CallSiteCollection(resolved1 + resolved2, null, originalOpType);
        }
        // PARTIAL fold: the ancestor chain genuinely ran out (e.g. the still-unresolved operand
        // originates at a @PathVariable/@RequestParam on the endpoint method itself — a real
        // runtime-only value with no compile-time literal ANYWHERE up the chain, §204 Pattern A's
        // own exact fixture shape) while the OTHER operand DID resolve. Rather than reporting
        // nothing at all (today's pre-fix behavior) OR fabricating a made-up value for the
        // genuinely-unknowable side, report the resolvable part plus an explicit `{name}`
        // placeholder for the part that can only be known at runtime — never a guessed literal,
        // always visibly a placeholder. Distinguishable from a fully-resolved fold via the
        // `{`/`}` markers, so callers can tell "fully known" apart from "known except a runtime
        // parameter" if that distinction ever matters downstream.
        String partial = resolved1 != null
                ? resolved1 + "{" + name2 + "}"
                : "{" + name1 + "}" + resolved2;
        return new MongoMethodDetector.CallSiteCollection(partial, null, originalOpType);
    }

    // ========== Base Package Auto-Detection ==========

    /**
     * Fallback: detect the most common top-level package (e.g. "com.acme") from indexed classes.
     * Uses the first two segments of class FQNs and picks the most frequent one.
     */
    List<String> autoDetectBasePackages(ResolutionContext ctx) {
        Map<String, Integer> pkgCount = new HashMap<>();
        for (String fqn : ctx.classMap.keySet()) {
            String[] parts = fqn.split("\\.");
            if (parts.length >= 2) {
                String base = parts[0] + "." + parts[1];
                pkgCount.merge(base, 1, Integer::sum);
            }
        }
        if (pkgCount.isEmpty()) return List.of();

        String topPkg = pkgCount.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);

        if (topPkg != null) {
            log.info("Auto-detected base package '{}' from {} indexed classes", topPkg, pkgCount.get(topPkg));
            return List.of(topPkg);
        }
        return List.of();
    }

    // ========== Repository -> Collection Resolution ==========

    void resolveRepositoryCollections(ResolutionContext ctx) {
        for (IndexedClass ic : ctx.classMap.values()) {
            if (!"REPOSITORY".equals(ic.stereotype) && !"SPRING_DATA".equals(ic.stereotype)) continue;

            // Priority 1: Direct entity type from generic signature (MongoRepository<Entity, ID>)
            if (ic.repositoryEntityType != null) {
                String coll = ctx.entityCollectionMap.get(ic.repositoryEntityType);
                // Also try simple name
                if (coll == null) {
                    String simpleName = ic.repositoryEntityType.contains(".")
                            ? ic.repositoryEntityType.substring(ic.repositoryEntityType.lastIndexOf('.') + 1)
                            : ic.repositoryEntityType;
                    coll = ctx.entityCollectionMap.get(simpleName);
                }
                if (coll != null) {
                    ctx.repoCollectionMap.put(ic.fqn, coll);
                    ctx.repoCollectionMap.put(ic.simpleName, coll);
                    log.debug("Repo {} → entity {} → collection {} (generic signature)",
                            ic.simpleName, ic.repositoryEntityType, coll);
                }
            }

            // Priority 1.5: Entity type known but not in entityCollectionMap —
            // try CamelCase → UPPER_SNAKE convention (e.g., ClmTransEntity → CLM_TRANS)
            if (!ctx.repoCollectionMap.containsKey(ic.simpleName) && ic.repositoryEntityType != null) {
                String entitySimple = ic.repositoryEntityType.contains(".")
                        ? ic.repositoryEntityType.substring(ic.repositoryEntityType.lastIndexOf('.') + 1)
                        : ic.repositoryEntityType;
                String entityStem = entitySimple.replaceAll("(Entity|Document|Model|Dto|DO)$", "");
                String upperSnake = entityStem.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
                if (configLoader.isLikelyCollectionName(upperSnake)) {
                    ctx.repoCollectionMap.put(ic.fqn, upperSnake);
                    ctx.repoCollectionMap.put(ic.simpleName, upperSnake);
                    log.debug("Repo {} → entity {} → derived collection {} (name convention)",
                            ic.simpleName, ic.repositoryEntityType, upperSnake);
                }
            }

            // Priority 2: Stem-based matching (ClaimRepository → Claim → entityCollectionMap)
            if (!ctx.repoCollectionMap.containsKey(ic.simpleName)) {
                List<String> stems = new ArrayList<>();
                for (String suffix : configLoader.getRepositorySuffixes()) {
                    if (ic.simpleName.endsWith(suffix)) {
                        stems.add(ic.simpleName.substring(0, ic.simpleName.length() - suffix.length()));
                    }
                }
                for (String stem : stems) {
                    for (Map.Entry<String, String> entry : ctx.entityCollectionMap.entrySet()) {
                        if (entry.getKey().endsWith(stem) || entry.getKey().equals(stem)) {
                            ctx.repoCollectionMap.put(ic.fqn, entry.getValue());
                            ctx.repoCollectionMap.put(ic.simpleName, entry.getValue());
                            break;
                        }
                    }
                    if (ctx.repoCollectionMap.containsKey(ic.simpleName)) break;
                }
            }

            for (IndexedMethod m : ic.methods.values()) {
                for (AnnotationData ad : m.annotationDetailList) {
                    if (configLoader.getQueryAnnotations().contains(ad.name())) {
                        for (Object val : ad.attributes().values()) {
                            for (String s : configLoader.flattenAnnotationValue(val)) {
                                // Only use targeted pipeline extraction (from/into/coll contexts)
                                // Do NOT use extractCollectionRefs — it matches foreignField, $project aliases, etc.
                                for (String ref : configLoader.extractPipelineCollectionRefs(s)) {
                                    if (!"Other".equals(configLoader.detectCollectionDomain(ref))) {
                                        ctx.repoCollectionMap.putIfAbsent(ic.fqn, ref);
                                        ctx.repoCollectionMap.putIfAbsent(ic.simpleName, ref);
                                    }
                                }
                            }
                        }
                    }
                }
                for (String lit : m.stringLiterals) {
                    if (lit.contains("_") && !"Other".equals(configLoader.detectCollectionDomain(lit))) {
                        ctx.repoCollectionMap.putIfAbsent(ic.fqn, lit);
                        ctx.repoCollectionMap.putIfAbsent(ic.simpleName, lit);
                    }
                }
            }
            for (String fc : ic.fieldConstants) {
                if (!"Other".equals(configLoader.detectCollectionDomain(fc))) {
                    ctx.repoCollectionMap.putIfAbsent(ic.fqn, fc);
                    ctx.repoCollectionMap.putIfAbsent(ic.simpleName, fc);
                }
            }
        }

        // JPA Repository → Table resolution (parallel to MongoDB repo-collection above)
        for (IndexedClass ic : ctx.classMap.values()) {
            if (!"REPOSITORY".equals(ic.stereotype)) continue;
            if (ctx.repoTableMap.containsKey(ic.simpleName)) continue;
            if (ic.repositoryEntityType != null && !ctx.entityTableMap.isEmpty()) {
                String table = ctx.entityTableMap.get(ic.repositoryEntityType);
                if (table == null) {
                    String simpleName = ic.repositoryEntityType.contains(".")
                            ? ic.repositoryEntityType.substring(ic.repositoryEntityType.lastIndexOf('.') + 1)
                            : ic.repositoryEntityType;
                    table = ctx.entityTableMap.get(simpleName);
                }
                if (table != null) {
                    ctx.repoTableMap.put(ic.fqn, table);
                    ctx.repoTableMap.put(ic.simpleName, table);
                    log.debug("JPA Repo {} → entity {} → table {} (generic signature)",
                            ic.simpleName, ic.repositoryEntityType, table);
                }
            }
        }

        log.info("Resolved {} entity-collection, {} repo-collection, {} entity-table, {} repo-table mappings",
                ctx.entityCollectionMap.size(), ctx.repoCollectionMap.size(),
                ctx.entityTableMap.size(), ctx.repoTableMap.size());
    }

    // ========== Populate collection data on a CallNode ==========

    // Methods whose string arguments are NOT collection names (sequence IDs, log strings, etc.)
    private static final Set<String> NON_COLLECTION_METHODS = Set.of(
            // Sequence generators
            "getSequenceId", "getNextSequenceValue", "getNextVal", "nextVal",
            "getNextId", "generateSequence", "incrementAndGet",
            // Extension/API handlers
            "handleClaimAPIExtensions", "handleAPIExtensions",
            // Logging
            "info", "debug", "warn", "error", "trace", "log", "logError", "logWarning", "logInfo",
            // String formatting / IO
            "format", "printf", "println", "append", "write", "print",
            // Reflection / metadata access (not DB operations)
            "getCollectionName", "getCollectionNameFromClass",
            // In-memory utility methods
            "createBatches", "createSavepoint", "partitionList",
            // Object utilities
            "toString", "hashCode", "equals", "valueOf", "compareTo", "fromString", "parse",
            // Property/config access
            "getProperty", "setProperty", "containsKey", "containsValue",
            "getConfig", "loadProperty", "resolveProperty", "getValue", "getEnv",
            // Cache/session operations
            "put", "get", "remove", "evict", "putIfAbsent", "getIfPresent",
            // In-memory membership checks — e.g. List.of("A", COLL_LIKE_CONST, "B").contains(x) is a
            // pure guard-clause comparison, not a DB call; never a real Mongo/JPA collection method.
            // The literals themselves are captured as List.of/Set.of's OWN stringArgs (the receiver
            // collection-literal construction), not contains()'s — "of" is what actually needs
            // excluding; "contains"/"containsAll" kept too in case of alternate bytecode shapes.
            "contains", "containsAll", "of",
            // Event/messaging (string args are event names, not collections)
            "sendMessage", "publishEvent", "emit", "publish", "send", "broadcast",
            "addEventListener", "addListener", "removeListener", "removeEventListener",
            // Validation/exception
            "validate", "check", "throwException", "assertNotNull", "requireNonNull",
            // Enum/constant lookup
            "getEnumValue", "lookup", "resolve", "forName",
            // Annotation/metadata
            "annotation", "with", "metadata", "getAttribute", "setAttribute"
    );

    // Suffixes that disqualify a string literal from being a collection name.
    // E.g., "CLM_TRANS_TIMEOUT", "CLM_TRANS_ERROR" — these are config/error constants, not collections.
    private static final Set<String> NON_COLLECTION_SUFFIXES = Set.of(
            "_TIMEOUT", "_CONFIG", "_PROPERTY", "_PROPERTIES", "_MESSAGE", "_MSG",
            "_EVENT", "_EXCEPTION", "_ERROR", "_ERRORS", "_CONSTANT", "_CACHE",
            "_SESSION", "_KEY", "_KEYS", "_PREFIX", "_SUFFIX", "_FORMAT",
            "_LOG", "_LOGGER", "_HANDLER", "_LISTENER", "_FACTORY", "_BUILDER",
            "_TYPE", "_STATUS", "_STATE", "_FLAG", "_MODE", "_LEVEL",
            "_FAILED", "_SUCCESS", "_ACTIVE", "_INACTIVE", "_PENDING",
            "_COUNT", "_LIMIT", "_SIZE", "_LENGTH", "_INDEX", "_OFFSET",
            "_PATH", "_URL", "_URI", "_HOST", "_PORT", "_NAME", "_LABEL",
            "_TEMPLATE", "_PATTERN", "_REGEX", "_SEPARATOR", "_DELIMITER",
            "_QUERY", "_CONTEXT", "_CRITERIA", "_FILTER", "_SORT", "_PROJECTION",
            "_HEADER", "_FOOTER", "_BODY", "_PAYLOAD", "_RESPONSE", "_REQUEST",
            "_DESCRIPTION", "_REASON", "_DETAIL", "_DETAILS", "_REPORT"
    );

    void collectCollections(IndexedClass cls, IndexedMethod method, CallNode node, ResolutionContext ctx) {
        List<String> collections = new ArrayList<>();
        Map<String, String> collDomains = new LinkedHashMap<>();
        Map<String, String> collSources = new LinkedHashMap<>();
        Set<String> detectedOpTypes = new HashSet<>();

        // Known entity field names (from @Field/@BsonProperty) — these are NOT collections.
        Set<String> fieldNameExclusions = ctx.knownEntityFieldNames;

        // Build exclusion set: strings used as arguments to sequence/logging/utility methods
        Set<String> nonCollectionArgs = buildNonCollectionArgs(method);

        // From repository mapping
        if ("REPOSITORY".equals(cls.stereotype) || "SPRING_DATA".equals(cls.stereotype)) {
            String coll = ctx.repoCollectionMap.get(cls.fqn);
            if (coll == null) coll = ctx.repoCollectionMap.get(cls.simpleName);
            if (coll != null && !collections.contains(coll) && !fieldNameExclusions.contains(coll)) {
                collections.add(coll);
                collDomains.put(coll, configLoader.detectCollectionDomain(coll));
                collSources.put(coll, "REPOSITORY_MAPPING");
            }
        }

        // From @Document on class — always trust this (explicit annotation)
        if (cls.documentCollection != null && !collections.contains(cls.documentCollection)) {
            collections.add(cls.documentCollection);
            collDomains.put(cls.documentCollection, configLoader.detectCollectionDomain(cls.documentCollection));
            collSources.put(cls.documentCollection, "DOCUMENT_ANNOTATION");
        }

        // From method string literals -- only if they match a known domain prefix (not "Other")
        // AND are not known entity field names AND not arguments to sequence/log methods
        // AND don't end with a non-collection suffix (e.g., _TIMEOUT, _ERROR, _CONFIG)
        for (String lit : method.stringLiterals) {
            if (lit.contains("_") && !collections.contains(lit)
                    && !fieldNameExclusions.contains(lit) && !nonCollectionArgs.contains(lit)
                    && !hasNonCollectionSuffix(lit)) {
                String domain = configLoader.detectCollectionDomain(lit);
                if (!"Other".equals(domain)) {
                    collections.add(lit);
                    collDomains.put(lit, domain);
                    collSources.put(lit, "STRING_LITERAL");
                }
            }
        }

        // From class field constants — only for REPOSITORY/SPRING_DATA classes where
        // static final String constants genuinely hold collection names.
        // Skip for SERVICE/COMPONENT classes where constants may be used for other purposes
        // (e.g. field names, config keys) and leak as false positives to all methods.
        if ("REPOSITORY".equals(cls.stereotype) || "SPRING_DATA".equals(cls.stereotype)) {
            for (String fc : cls.fieldConstants) {
                if (!collections.contains(fc) && !fieldNameExclusions.contains(fc)) {
                    String domain = configLoader.detectCollectionDomain(fc);
                    if (!"Other".equals(domain)) {
                        collections.add(fc);
                        collDomains.put(fc, domain);
                        collSources.put(fc, "FIELD_CONSTANT");
                    }
                }
            }
        }

        // From annotation values (@Query, @Aggregation, etc.)
        // ONLY use extractPipelineCollectionRefs (targeted: from/into/coll contexts).
        // Do NOT use extractCollectionRefs here — it matches ANY UPPER_CASE_STRING
        // which causes false positives from foreignField, localField, $project aliases, etc.
        for (AnnotationData ad : method.annotationDetailList) {
            for (Object val : ad.attributes().values()) {
                for (String s : configLoader.flattenAnnotationValue(val)) {
                    for (String ref : configLoader.extractPipelineCollectionRefs(s)) {
                        if (!collections.contains(ref) && !fieldNameExclusions.contains(ref)) {
                            String domain = configLoader.detectCollectionDomain(ref);
                            if (!"Other".equals(domain)) {
                                collections.add(ref);
                                collDomains.put(ref, domain);
                                collSources.put(ref, "PIPELINE_ANNOTATION");
                            }
                        }
                    }
                }
            }
        }

        // From MongoMethodDetector: BulkWriteCollector, MongoTemplate, native driver, reactive.
        // Unlike the string-literal/field-constant scans above (which need the domain-prefix
        // filter to reject false positives like foreignField/$project aliases), these collection
        // names come from actual resolved API invocations (addInsert/addUpdate/getCollection/etc.)
        // so they are added unconditionally — matching the JpaMethodDetector loop below. Domain is
        // still computed for display/grouping but "Other" must not drop the collection, or any
        // collection whose prefix isn't yet in domain-config.json's module_types (e.g. WIP_*)
        // silently vanishes even though detection correctly found it.
        // Dynamic (variable-sourced) collection references — collected from both detectors below,
        // merged onto CallNode.dynamicCollectionRefs, and NEVER folded into collections/
        // collDomains/collSources (those three must stay "resolved literal" ground truth).
        List<Map<String, Object>> dynamicRefs = new ArrayList<>();

        MongoMethodDetector.DetectionResult mongoResult = mongoMethodDetector.detect(method, cls, ctx.entityCollectionMap);
        for (MongoMethodDetector.DetectedCollection dc : mongoResult.resolved()) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            // dc.collectionName() is null for NATIVE_COMMAND_DEFAULT (runCommand default-READ
            // classification, e.g. collStats/admin commands) — that entry exists purely to carry
            // an operationType signal (handled above) and must never be added to
            // collectionsAccessed as a bogus "null" entry.
            if (dc.collectionName() == null) continue;
            if (!collections.contains(dc.collectionName()) && !fieldNameExclusions.contains(dc.collectionName())) {
                collections.add(dc.collectionName());
                collDomains.put(dc.collectionName(), configLoader.detectCollectionDomain(dc.collectionName()));
                collSources.put(dc.collectionName(), dc.source());
            }
        }
        for (CallGraphIndex.DetectedDynamicCollection ddc : mongoResult.dynamic()) {
            mergeDynamicResult(ddc, ctx, collections, collDomains, collSources, fieldNameExclusions,
                    dynamicRefs, detectedOpTypes);
        }

        // From AggregationDetector: pipeline stages ($lookup, $merge, $out), Spring Aggregation API.
        // Same reasoning as the MongoMethodDetector loop above: these are resolved invocations,
        // not heuristic string scans, so they must not be dropped for having an unmapped prefix.
        AggregationDetector.PipelineDetail pipelineDetail = new AggregationDetector.PipelineDetail();
        for (AggregationDetector.DetectedPipelineCollection dc : aggregationDetector.detect(method, cls, ctx, pipelineDetail)) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            if (!collections.contains(dc.collectionName()) && !fieldNameExclusions.contains(dc.collectionName())) {
                collections.add(dc.collectionName());
                collDomains.put(dc.collectionName(), configLoader.detectCollectionDomain(dc.collectionName()));
                collSources.put(dc.collectionName(), dc.source());
            }
        }
        for (CallGraphIndex.DetectedDynamicCollection ddc : pipelineDetail.dynamicRefs) {
            mergeDynamicResult(ddc, ctx, collections, collDomains, collSources, fieldNameExclusions,
                    dynamicRefs, detectedOpTypes);
        }
        // Attach rich pipeline detail to CallNode if any stages were detected
        if (!pipelineDetail.stages.isEmpty() || !pipelineDetail.lookupTargets.isEmpty()
                || !pipelineDetail.pipelineSourceMethods.isEmpty()) {
            Map<String, Object> pipeline = new LinkedHashMap<>();
            if (pipelineDetail.baseCollection != null)
                pipeline.put("baseCollection", pipelineDetail.baseCollection);
            pipeline.put("stages", pipelineDetail.stages);
            if (!pipelineDetail.lookupTargets.isEmpty())
                pipeline.put("lookupTargets", pipelineDetail.lookupTargets);
            if (!pipelineDetail.matchFields.isEmpty())
                pipeline.put("matchFields", new ArrayList<>(pipelineDetail.matchFields));
            if (!pipelineDetail.groupFields.isEmpty())
                pipeline.put("groupFields", new ArrayList<>(pipelineDetail.groupFields));
            if (!pipelineDetail.projectionFields.isEmpty())
                pipeline.put("projectionFields", new ArrayList<>(pipelineDetail.projectionFields));
            if (!pipelineDetail.sortFields.isEmpty())
                pipeline.put("sortFields", new ArrayList<>(pipelineDetail.sortFields));
            if (!pipelineDetail.companionUsage.isEmpty())
                pipeline.put("companionUsage", pipelineDetail.companionUsage);
            if (!pipelineDetail.detectionSources.isEmpty())
                pipeline.put("detectionSources", new ArrayList<>(pipelineDetail.detectionSources));
            if (pipelineDetail.isDynamic)
                pipeline.put("isDynamic", true);
            if (!pipelineDetail.pipelineSourceMethods.isEmpty())
                pipeline.put("pipelineSourceMethods", pipelineDetail.pipelineSourceMethods);
            node.setAggregationPipeline(pipeline);
        }

        // Collection-lifecycle-workflow stage tagging (e.g. blue-green collection swap). Matches
        // real MongoDB driver method invocations (createCollection/createIndex/renameCollection/
        // drop) already scanned above via method.invocations, plus the pipelineDetail.stages
        // just populated by aggregationDetector — deliberately independent of whatever
        // application-defined wrapper method this node's own methodName is, since that can be
        // renamed freely. See CollectionLifecycleDetector for the aggregation pass that groups
        // these tags across the tree into workflow instances.
        List<String> lifecycleStageTags = collectionLifecycleDetector.detectStages(method, pipelineDetail);
        if (!lifecycleStageTags.isEmpty()) {
            node.setLifecycleStageTags(lifecycleStageTags);
        }

        // From MyBatisMethodDetector: SqlSession, @Mapper annotations
        for (MyBatisMethodDetector.DetectedTable dt : myBatisMethodDetector.detect(method, cls, ctx.namedQueryMap)) {
            if (dt.operationType() != null) detectedOpTypes.add(dt.operationType());
            if (!"?".equals(dt.tableName()) && !collections.contains(dt.tableName())) {
                collections.add(dt.tableName());
                String domain = configLoader.detectCollectionDomain(dt.tableName());
                collDomains.put(dt.tableName(), domain);
                collSources.put(dt.tableName(), dt.source());
            }
        }

        // From RedisMethodDetector: RedisTemplate, StringRedisTemplate operations
        for (RedisMethodDetector.DetectedCollection dc : redisMethodDetector.detect(cls, method, method.invocations)) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            // Redis keys are not "collection names" in the MongoDB/JPA sense — we skip domain filtering
            // and always include them when detected. They use source=REDIS_TEMPLATE.
            if (!collections.contains(dc.collectionName())) {
                collections.add(dc.collectionName());
                collDomains.put(dc.collectionName(), "Redis");
                collSources.put(dc.collectionName(), dc.source());
            }
        }

        // From CassandraMethodDetector: CassandraTemplate, CqlSession operations
        for (CassandraMethodDetector.DetectedCollection dc : cassandraMethodDetector.detect(cls, method, method.invocations)) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            if (!collections.contains(dc.collectionName()) && !fieldNameExclusions.contains(dc.collectionName())) {
                String domain = configLoader.detectCollectionDomain(dc.collectionName());
                collections.add(dc.collectionName());
                collDomains.put(dc.collectionName(), domain);
                collSources.put(dc.collectionName(), dc.source());
            }
        }

        // From ElasticsearchMethodDetector: ElasticsearchOperations, RestHighLevelClient
        // CALL-011: detectWithDynamic also captures unresolved dynamic (variable/field-sourced)
        // index-name arguments instead of silently falling back to "es:unknown-index" — those are
        // merged the SAME way Mongo's own dynamic refs are (never folded into collections/
        // collDomains/collSources directly).
        ElasticsearchMethodDetector.DetectionResult esResult =
                elasticsearchMethodDetector.detectWithDynamic(cls, method, method.invocations,
                        ctx.entityCollectionMap, ctx.entityIndexMap);
        for (ElasticsearchMethodDetector.DetectedCollection dc : esResult.resolved()) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            if (!collections.contains(dc.collectionName()) && !fieldNameExclusions.contains(dc.collectionName())) {
                String domain = configLoader.detectCollectionDomain(dc.collectionName());
                collections.add(dc.collectionName());
                collDomains.put(dc.collectionName(), domain);
                collSources.put(dc.collectionName(), dc.source());
            }
        }
        for (CallGraphIndex.DetectedDynamicCollection ddc : esResult.dynamic()) {
            if (ddc.operationType() != null) detectedOpTypes.add(ddc.operationType());
            dynamicRefs.add(toDynamicRefMap(ddc));
        }

        // From JpaMethodDetector: JdbcTemplate, EntityManager, @Query, derived queries
        List<String> capturedSql = new ArrayList<>();
        JpaMethodDetector.DetectionResult jpaResult = jpaMethodDetector.detect(method, cls, ctx.entityTableMap, ctx.namedQueryMap);
        for (JpaMethodDetector.DetectedTable dt : jpaResult.resolved()) {
            if (dt.operationType() != null) detectedOpTypes.add(dt.operationType());
            if (!collections.contains(dt.tableName())) {
                collections.add(dt.tableName());
                String domain = configLoader.detectCollectionDomain(dt.tableName());
                collDomains.put(dt.tableName(), domain);
                collSources.put(dt.tableName(), dt.source());
            }
            // Capture raw SQL text for display in UI
            if (dt.sqlText() != null && !dt.sqlText().isBlank() && !capturedSql.contains(dt.sqlText())) {
                capturedSql.add(dt.sqlText());
            }
        }
        if (!capturedSql.isEmpty()) node.setSqlStatements(capturedSql);
        // CALL-009: dynamic (variable-sourced) JDBC/SQL references — same "never fold into
        // collections/collDomains/collSources" contract as the Mongo dynamicRefs above.
        for (CallGraphIndex.DetectedDynamicCollection ddc : jpaResult.dynamic()) {
            if (ddc.operationType() != null) detectedOpTypes.add(ddc.operationType());
            dynamicRefs.add(toDynamicRefMap(ddc));
        }

        // From JPA repository mapping (parallel to MongoDB repo mapping above)
        if ("REPOSITORY".equals(cls.stereotype)) {
            String table = ctx.repoTableMap.get(cls.fqn);
            if (table == null) table = ctx.repoTableMap.get(cls.simpleName);
            if (table != null && !collections.contains(table)) {
                collections.add(table);
                collDomains.put(table, configLoader.detectCollectionDomain(table));
                collSources.put(table, "JPA_REPOSITORY_MAPPING");
            }
        }

        // From @Entity @Table on the class itself
        if (cls.isJpaEntity && cls.jpaTableName != null && !collections.contains(cls.jpaTableName)) {
            collections.add(cls.jpaTableName);
            collDomains.put(cls.jpaTableName, configLoader.detectCollectionDomain(cls.jpaTableName));
            collSources.put(cls.jpaTableName, "JPA_TABLE_ANNOTATION");
        }

        // Mark node as DB-interacting when collections were resolved OR when direct
        // MongoTemplate/JDBC/JPA calls were detected. The guard on collections.isEmpty()
        // was incorrect: Oracle JDBC nodes detect stored-procedure collections (e.g.
        // PROC_ARCHIVE_ORDERS) so the list is never empty, yet the flag was never set.
        // Also true for a BulkWriteCollector's own addInsert/addUpdate/addDelete methods —
        // these buffer an operation for a LATER flush (execute()/runCommand()) rather than
        // calling Mongo directly, so neither collections nor mongoMethodDetector.hasDbInvocations
        // would otherwise catch them, letting the post-build cleanup pass null out or
        // re-derive their operationType from unrelated child calls.
        // Also true when AggregationDetector saw pipeline-builder-class usage (MatchOperation,
        // GroupOperation, SortOperation, Filters/Projections/Sorts/Accumulators companions, etc.)
        // even though no individual stage carried a resolvable collection-name string — e.g. a
        // helper method that only assembles $match/$group criteria for a pipeline executed
        // elsewhere. Without this, such helpers have zero collections and get their AGGREGATE
        // operationType wiped as a false positive by the cleanup pass.
        // Also true when a dynamic (variable-sourced) collection reference was captured —
        // a real DB call happened, we just couldn't resolve the literal collection name.
        // Also true when this method passes a lambda/method-reference DIRECTLY as an argument
        // to a higher-order method and that lambda's own body itself does real DB work — see
        // hasDirectLambdaDbInteraction below for the deliberately narrow (single-hop) scope.
        node.setHasDbInteraction(!collections.isEmpty()
                || mongoMethodDetector.hasDbInvocations(method)
                || jpaMethodDetector.hasJpaInvocations(method)
                || myBatisMethodDetector.hasMyBatisInvocations(method)
                || redisMethodDetector.hasRedisInvocations(method)
                || cassandraMethodDetector.hasCassandraInvocations(method)
                || elasticsearchMethodDetector.hasElasticsearchInvocations(method)
                || mongoMethodDetector.isBulkCollectorOwnMethod(cls, method)
                || pipelineDetail.hasPipelineInvolvement
                || !dynamicRefs.isEmpty()
                || hasDirectLambdaDbInteraction(method, ctx));

        // From @Value property keys resolved via config files
        if (!cls.valuePropertyKeys.isEmpty() && ctx.configProperties != null && !ctx.configProperties.isEmpty()) {
            for (String key : cls.valuePropertyKeys) {
                String resolved = ctx.configProperties.get(key);
                if (resolved != null && !collections.contains(resolved) && !fieldNameExclusions.contains(resolved)) {
                    String domain = configLoader.detectCollectionDomain(resolved);
                    if (!"Other".equals(domain)) {
                        collections.add(resolved);
                        collDomains.put(resolved, domain);
                        collSources.put(resolved, "VALUE_PROPERTY");
                    }
                }
            }
        }

        // From Environment.getProperty() calls — resolve property key from config
        if (ctx.configProperties != null && !ctx.configProperties.isEmpty()) {
            for (InvocationRef inv : method.invocations) {
                if ("getProperty".equals(inv.methodName())
                        && inv.ownerClass() != null
                        && (inv.ownerClass().endsWith("Environment") || inv.ownerClass().endsWith("ConfigurableEnvironment"))) {
                    for (String arg : inv.stringArgs()) {
                        String resolved = ctx.configProperties.get(arg);
                        if (resolved != null && !collections.contains(resolved) && !fieldNameExclusions.contains(resolved)) {
                            String domain = configLoader.detectCollectionDomain(resolved);
                            if (!"Other".equals(domain)) {
                                collections.add(resolved);
                                collDomains.put(resolved, domain);
                                collSources.put(resolved, "ENV_PROPERTY");
                            }
                        }
                    }
                }
            }
        }

        // From enum/constants class references — resolve GETSTATIC on enum fields to their values
        if (!ctx.enumConstantsMap.isEmpty()) {
            // Check method invocation targets (calls to enum/constants class methods)
            Set<String> constantOwners = new HashSet<>();
            for (InvocationRef inv : method.invocations) {
                if (ctx.enumConstantsMap.containsKey(inv.ownerClass())) {
                    constantOwners.add(inv.ownerClass());
                }
            }
            // Check GETSTATIC field references (direct access to constants fields)
            if (method.staticFieldRefOwners != null) {
                for (String owner : method.staticFieldRefOwners) {
                    if (ctx.enumConstantsMap.containsKey(owner)) {
                        constantOwners.add(owner);
                    }
                }
            }
            for (String owner : constantOwners) {
                for (String val : ctx.enumConstantsMap.get(owner)) {
                    if (!collections.contains(val) && !fieldNameExclusions.contains(val)) {
                        String domain = configLoader.detectCollectionDomain(val);
                        if (!"Other".equals(domain)) {
                            collections.add(val);
                            collDomains.put(val, domain);
                            collSources.put(val, "ENUM_CONSTANT");
                        }
                    }
                }
            }
        }

        // From implicit collection mappings (known cross-JAR patterns like validators, sequence service)
        Map<String, List<Map<String, String>>> implicitMap = configLoader.getImplicitCollections();
        if (implicitMap != null && !implicitMap.isEmpty()) {
            // Check current node: ClassName.methodName
            String nodeKey = cls.simpleName + "." + method.name;
            addImplicitCollections(nodeKey, implicitMap, collections, collDomains, collSources, fieldNameExclusions);
            // Check invocations: if this method calls a known implicit method
            for (InvocationRef inv : method.invocations) {
                String owner = inv.ownerClass();
                if (owner == null || owner.isEmpty()) continue;
                String simpleName = owner.contains(".")
                        ? owner.substring(owner.lastIndexOf('.') + 1) : owner;
                String invKey = simpleName + "." + inv.methodName();
                addImplicitCollections(invKey, implicitMap, collections, collDomains, collSources, fieldNameExclusions);
            }
        }

        // From repository interface invocations — inherit the repo's collection.
        // Handles service.search() → repo.customMethod() patterns not reachable in the call tree,
        // and service.raiseSalary() → repo.bulkUpdate() delegations.
        if (!ctx.repoCollectionMap.isEmpty()) {
            for (InvocationRef inv : method.invocations) {
                String owner = inv.ownerClass();
                if (owner == null || owner.isEmpty()) continue;
                String simpleOwner = owner.contains(".") ? owner.substring(owner.lastIndexOf('.') + 1) : owner;
                String repoColl = ctx.repoCollectionMap.get(simpleOwner);
                if (repoColl == null) repoColl = ctx.repoCollectionMap.get(owner);
                if (repoColl != null && !collections.contains(repoColl) && !fieldNameExclusions.contains(repoColl)) {
                    collections.add(repoColl);
                    collDomains.put(repoColl, configLoader.detectCollectionDomain(repoColl));
                    collSources.put(repoColl, "REPOSITORY_CALL");
                }
            }
        }

        // When MongoTemplate calls detected but collection unresolved (e.g. insertAll(List<T>)
        // with generic type erased), infer from injected repository fields on the enclosing class.
        // e.g. EmployeeService has field EmployeeRepository → "employees"
        // Skipped when we already captured a dynamic reference for this method — a named
        // variable/field source is strictly better information than a repo-field guess, and
        // guessing here would silently overwrite/mask the dynamic ref with an unrelated
        // collection (the same class of bug fixed for the WIP_* domain-filter case).
        if (collections.isEmpty() && dynamicRefs.isEmpty()
                && mongoMethodDetector.hasDbInvocations(method) && !ctx.repoCollectionMap.isEmpty()) {
            for (CallGraphIndex.FieldDIInfo fi : cls.injectedFields.values()) {
                String simpleType = fi.fieldType().contains(".")
                        ? fi.fieldType().substring(fi.fieldType().lastIndexOf('.') + 1) : fi.fieldType();
                String coll = ctx.repoCollectionMap.get(simpleType);
                if (coll == null) coll = ctx.repoCollectionMap.get(fi.fieldType());
                if (coll != null && !fieldNameExclusions.contains(coll)) {
                    collections.add(coll);
                    collDomains.put(coll, configLoader.detectCollectionDomain(coll));
                    collSources.put(coll, "INJECTED_REPO_INFERENCE");
                }
            }
        }

        // Post-filter: when catalog is available, remove STRING_LITERAL-sourced items
        // that are NOT in the live database (sequence names, log strings, etc.)
        if (ctx.mongoCatalogCollections != null && !collections.isEmpty()) {
            List<String> toRemove = new ArrayList<>();
            for (String coll : collections) {
                String source = collSources.get(coll);
                if (!catalogContains(ctx.mongoCatalogCollections, coll)
                        && ("STRING_LITERAL".equals(source) || "FIELD_CONSTANT".equals(source)
                            || "ENUM_CONSTANT".equals(source))) {
                    toRemove.add(coll);
                }
            }
            for (String coll : toRemove) {
                collections.remove(coll);
                collDomains.remove(coll);
                collSources.remove(coll);
            }
        }

        node.setCollectionsAccessed(collections);
        node.setCollectionDomains(collDomains);
        node.setCollectionSources(collSources);
        if (!dynamicRefs.isEmpty()) {
            node.setDynamicCollectionRefs(dynamicRefs);
        }

        // Pipeline-builder-only involvement (no specific WRITE/AGGREGATE op resolved above via a
        // named stage keyword like $out/$merge, e.g. a method that only builds MatchOperation/
        // GroupOperation stages) still counts as an AGGREGATE operation on this node.
        if (pipelineDetail.hasPipelineInvolvement) {
            detectedOpTypes.add("AGGREGATE");
        }

        // Always add all detected operation types to the node's set
        node.getOperationTypes().addAll(detectedOpTypes);
        // Set primary operationType if not already set
        if (node.getOperationType() == null && !detectedOpTypes.isEmpty()) {
            node.setOperationType(pickHighestPriorityOp(detectedOpTypes));
        }

        // Verify collections against live MongoDB catalog (supplementary — not a gate)
        // Uses case-insensitive matching: @Document("clm_child_locks") matches catalog "CLM_CHILD_LOCKS"
        if (!collections.isEmpty()) {
            Map<String, String> verification = new LinkedHashMap<>();
            if (ctx.mongoCatalogCollections != null) {
                for (String coll : collections) {
                    verification.put(coll, catalogContains(ctx.mongoCatalogCollections, coll) ? "VERIFIED" : "NOT_IN_DB");
                }
            } else {
                for (String coll : collections) {
                    verification.put(coll, "NO_CATALOG");
                }
            }
            node.setCollectionVerification(verification);
        }

        // Attach entity field mappings for collections that have them
        Map<String, Map<String, String>> fieldMaps = null;
        for (String coll : collections) {
            Map<String, String> mapping = ctx.collectionFieldMappings.get(coll);
            if (mapping != null && !mapping.isEmpty()) {
                if (fieldMaps == null) fieldMaps = new LinkedHashMap<>();
                fieldMaps.put(coll, mapping);
            }
        }
        if (fieldMaps != null) node.setEntityFieldMappings(fieldMaps);
    }

    /** Add implicit collections from known cross-JAR patterns (validators, sequence service, etc.). */
    private void addImplicitCollections(String key, Map<String, List<Map<String, String>>> implicitMap,
                                         List<String> collections, Map<String, String> collDomains,
                                         Map<String, String> collSources, Set<String> fieldNameExclusions) {
        List<Map<String, String>> mappings = implicitMap.get(key);
        if (mappings == null) return;
        for (Map<String, String> mapping : mappings) {
            String coll = mapping.get("collection");
            String source = mapping.get("source");
            if (coll != null && !collections.contains(coll) && !fieldNameExclusions.contains(coll)) {
                collections.add(coll);
                collDomains.put(coll, configLoader.detectCollectionDomain(coll));
                collSources.put(coll, source != null ? source : "IMPLICIT");
            }
        }
    }

    /** Check if a string literal ends with a non-collection suffix (e.g., _TIMEOUT, _ERROR). */
    private boolean hasNonCollectionSuffix(String literal) {
        String upper = literal.toUpperCase();
        for (String suffix : NON_COLLECTION_SUFFIXES) {
            if (upper.endsWith(suffix)) return true;
        }
        return false;
    }

    /** Build set of strings that are arguments to sequence/log/utility methods (not collections). */
    private Set<String> buildNonCollectionArgs(IndexedMethod method) {
        Set<String> excluded = new HashSet<>();
        for (InvocationRef inv : method.invocations) {
            if (NON_COLLECTION_METHODS.contains(inv.methodName())) {
                for (String arg : inv.stringArgs()) {
                    if (!arg.startsWith("__class:")) excluded.add(arg);
                }
            }
        }
        return excluded;
    }

    /** Case-insensitive check if catalog contains a collection name. */
    private static boolean catalogContains(Set<String> catalog, String collName) {
        if (catalog.contains(collName)) return true;
        return catalog.contains(collName.toUpperCase()) || catalog.contains(collName.toLowerCase());
    }

    /** Pick highest-priority operation type from a set of detected types.
     *  Order must stay in sync with EndpointInfo.opPri(): WRITE > UPDATE > DELETE > UPSERT > AGGREGATE > READ > COUNT */
    private static String pickHighestPriorityOp(Set<String> ops) {
        for (String op : List.of("WRITE", "UPDATE", "DELETE", "UPSERT", "AGGREGATE", "READ", "COUNT")) {
            if (ops.contains(op)) return op;
        }
        return null;
    }

    /**
     * Merge a single dynamic (variable-sourced) detection: attempt one last cheap resolution
     * via ctx.enumConstantsMap for cross-class STATIC_FIELD refs (same-class constants and
     * single-assignment locals were already resolved inline by BytecodeClassParser, so only
     * cross-class constant-holder references reach here unresolved). If that resolves to
     * exactly one literal, promote it into the resolved collections/domains/sources maps —
     * otherwise record it as a genuine dynamic reference.
     */
    private void mergeDynamicResult(CallGraphIndex.DetectedDynamicCollection ddc, ResolutionContext ctx,
                                     List<String> collections, Map<String, String> collDomains,
                                     Map<String, String> collSources, Set<String> fieldNameExclusions,
                                     List<Map<String, Object>> dynamicRefs, Set<String> detectedOpTypes) {
        if (ddc.operationType() != null) detectedOpTypes.add(ddc.operationType());
        String resolved = tryResolveDynamicViaEnumConstants(ddc, ctx);
        boolean viaReturnValue = false;
        if (resolved == null) {
            resolved = tryResolveSingleHopReturnValue(ddc, ctx);
            viaReturnValue = resolved != null;
        }
        // Gate the resolved value through the same "does this actually look like a collection
        // name" check used for every other resolution path (isCollectionName/isLikelyCollectionName)
        // — a resolved static-final-String value could just as easily be an unrelated constant
        // (an error message, a header name, etc.), and this call site has no other content filter.
        if (resolved != null && configLoader.isLikelyCollectionName(resolved)) {
            if (!collections.contains(resolved) && !fieldNameExclusions.contains(resolved)) {
                collections.add(resolved);
                collDomains.put(resolved, configLoader.detectCollectionDomain(resolved));
                // CALL-018: distinctly tagged (never reusing an existing CALL_SITE_*/argument-flow
                // tag) so return-value provenance stays auditable separately from the
                // caller-argument provenance CALL-001..017 already tag.
                collSources.put(resolved, viaReturnValue ? "RETURN_VALUE_SINGLE_HOP_RESOLVED" : "ENUM_CONSTANT_DYNAMIC_RESOLVED");
            }
        } else {
            dynamicRefs.add(toDynamicRefMap(ddc));
        }
    }

    /**
     * CALL-018: single-hop return-value resolution. Fires when {@code ddc} is a
     * {@code RETURN_VALUE}-kind dynamic reference (see {@code BytecodeClassParser}'s
     * {@code returnCallSlotValue}/whole-method ARETURN scan) — i.e. the CALLER's own body did
     * {@code String x = other.someMethod(); ...use(x)...} and could not resolve {@code x} itself.
     * Rather than walking the caller's OWN ancestor call chain (that answers "who called ME with
     * this value", the wrong direction here), this looks DOWN into the CALLED method
     * ({@code ddc.ownerClass()}/{@code ddc.variableName()}) via {@code ctx.classMap}, which is
     * fully indexed before any resolution runs, and checks whether that method's own body was
     * classified (at parse time) as having exactly one, unambiguous literal return. CALL-019
     * extended this to walk FURTHER down when the called method's own return is itself just
     * another method call's return (a pure pass-through, e.g. {@code return other.foo();}) —
     * bounded at {@link #MAX_HOP_DEPTH} total hops, same convention as CALL-001/002's own
     * (opposite-direction) ancestor walk; a chain longer than that, or any hop whose return is
     * genuinely computed/ambiguous/non-String, correctly stays unresolved. Matched by simple method name + zero declared parameters (a plain
     * "getter"-shaped call) — narrow but sufficient for this task's scope; overload-safe exact
     * matching would need the call's descriptor threaded through DynamicArgSource, which only
     * carries 3 fields (kind/name/ownerClass), matching how STATIC_FIELD/CONCAT_* already accept
     * that same narrow-fields trade-off elsewhere in this file.
     */
    private String tryResolveSingleHopReturnValue(CallGraphIndex.DetectedDynamicCollection ddc, ResolutionContext ctx) {
        if (!"RETURN_VALUE".equals(ddc.varKind())) return null;
        if (ddc.ownerClass() == null || ddc.variableName() == null || ctx == null || ctx.classMap == null) return null;
        String ownerClass = ddc.ownerClass();
        String methodName = ddc.variableName();
        // CALL-019: bounded multi-hop walk. Hop 1 is the direct callee (ddc.ownerClass()/
        // ddc.variableName()) — the exact single-hop lookup CALL-018 already did. If that
        // method's own return is instead a PURE pass-through of a FURTHER call
        // (singleReturnPassThroughOwner/Method), walk down into THAT method next, and so on, up
        // to MAX_HOP_DEPTH total hops (same bound/convention CALL-001/002 use for their own,
        // opposite-direction ancestor walk). Stops immediately (returns null) the moment a hop's
        // target class isn't indexed, no matching zero-arg method is found, or the bound is hit —
        // never guesses past what's provably a straight-line chain of single, unambiguous returns.
        for (int hop = 0; hop < MAX_HOP_DEPTH; hop++) {
            IndexedClass calleeCls = ctx.classMap.get(ownerClass);
            if (calleeCls == null) return null;
            IndexedMethod match = null;
            for (IndexedMethod m : calleeCls.methods.values()) {
                if (m.name.equals(methodName) && (m.paramTypes == null || m.paramTypes.isEmpty())) {
                    match = m;
                    break;
                }
            }
            if (match == null) return null;
            if (match.singleLiteralReturnValue != null) return match.singleLiteralReturnValue;
            if (match.singleReturnPassThroughOwner != null && match.singleReturnPassThroughMethod != null) {
                ownerClass = match.singleReturnPassThroughOwner;
                methodName = match.singleReturnPassThroughMethod;
                continue; // one more hop down
            }
            return null; // genuinely unresolved (computed, ambiguous, or non-String) at this hop
        }
        return null; // bound exceeded — chain longer than MAX_HOP_DEPTH stays unresolved
    }

    /** Attempt to resolve a cross-class STATIC_FIELD dynamic reference to its literal value.
     *  Tries the EXACT field first via ctx.fieldConstantsMap (ownerClass -> fieldName -> value,
     *  built for every indexed class) — this is unambiguous regardless of how many other
     *  constants the owner class declares. Falls back to the coarser ctx.enumConstantsMap
     *  (owner class -> ALL its string constants) ONLY when the owner class maps to EXACTLY one
     *  literal value overall — an ambiguous (multi-value) owner with no exact-field match is left
     *  as a dynamic reference rather than guessing which constant applies. */
    private String tryResolveDynamicViaEnumConstants(CallGraphIndex.DetectedDynamicCollection ddc,
                                                       ResolutionContext ctx) {
        if (!"STATIC_FIELD".equals(ddc.varKind())) return null;
        if (ddc.ownerClass() != null && ctx.fieldConstantsMap != null) {
            Map<String, String> byField = ctx.fieldConstantsMap.get(ddc.ownerClass());
            if (byField != null) {
                String exact = byField.get(ddc.variableName());
                if (exact != null) return exact;
            }
        }
        if (ctx.enumConstantsMap == null || ctx.enumConstantsMap.isEmpty() || ddc.ownerClass() == null) return null;
        List<String> values = ctx.enumConstantsMap.get(ddc.ownerClass());
        if (values != null && values.size() == 1) return values.get(0);
        return null;
    }

    /** Build the JSON-serializable shape for a dynamic collection reference (see CallNode.dynamicCollectionRefs). */
    private Map<String, Object> toDynamicRefMap(CallGraphIndex.DetectedDynamicCollection ddc) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("variableName", ddc.variableName());
        m.put("varKind", ddc.varKind());
        m.put("pattern", ddc.pattern());
        m.put("source", ddc.source());
        m.put("operationType", ddc.operationType());
        // CONCAT_* kinds repurpose ownerClass to carry the concat's literal operand (see
        // DynamicArgSource's javadoc) — the operand tagGeneralEsCallSite's fold needs. Null for
        // every other kind, so existing ref shapes are byte-identical.
        if (ddc.ownerClass() != null) m.put("ownerClass", ddc.ownerClass());
        return m;
    }

    /**
     * True when this method invokes a higher-order method with a lambda/method-reference
     * argument (invokedynamic-sourced InvocationRef with isLambdaInvocation()=true), and that
     * lambda's own target method (resolved the same way CallTreeBuilder resolves lambda targets
     * for tree-building) itself performs real DB work.
     *
     * Deliberately scoped to a SINGLE hop: only the DIRECT lambda target method is inspected via
     * mongoMethodDetector.hasDbInvocations/jpaMethodDetector.hasJpaInvocations/collection
     * detection — it does not recurse into what THAT method calls. This covers the concrete bug
     * (a wrapper passing a lambda straight into a higher-order bypass/executor method) without
     * the added complexity/regression risk of a full transitive DB-interaction propagation
     * across arbitrary call depth.
     */
    private boolean hasDirectLambdaDbInteraction(IndexedMethod method, ResolutionContext ctx) {
        for (InvocationRef inv : method.invocations) {
            if (!inv.isLambdaInvocation() || inv.lambdaTargetClass() == null) continue;
            String lambdaTargetCls = inv.lambdaTargetClass();
            String lambdaTargetMtd = inv.lambdaTargetMethod();
            String lambdaTargetDesc = inv.lambdaTargetDescriptor();
            if (lambdaTargetMtd == null || lambdaTargetDesc == null) continue;

            IndexedClass lambdaCls = ctx.classMap.get(lambdaTargetCls);
            if (lambdaCls == null) continue;

            boolean isSyntheticLambda = lambdaTargetMtd.startsWith("lambda$");
            // Synthetic lambda bodies are typically static; method-reference targets can be
            // either — try both key forms the same way CallTreeBuilder's lambda resolution does.
            IndexedMethod lambdaMethod = lambdaCls.methods.get((isSyntheticLambda ? "S:" : "") + lambdaTargetMtd + lambdaTargetDesc);
            if (lambdaMethod == null) {
                lambdaMethod = lambdaCls.methods.get((isSyntheticLambda ? "" : "S:") + lambdaTargetMtd + lambdaTargetDesc);
            }
            if (lambdaMethod == null) continue;

            if (mongoMethodDetector.hasDbInvocations(lambdaMethod)
                    || jpaMethodDetector.hasJpaInvocations(lambdaMethod)
                    || mongoMethodDetector.isBulkCollectorOwnMethod(lambdaCls, lambdaMethod)) {
                return true;
            }
        }
        return false;
    }
}
