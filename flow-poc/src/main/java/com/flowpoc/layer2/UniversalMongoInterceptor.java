package com.flowpoc.layer2;

import com.mongodb.client.MongoClient;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.*;
import net.bytebuddy.matcher.ElementMatchers;
import org.bson.Document;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * ByteBuddy proxy over MongoTemplate that captures EVERY public method call.
 *
 * Safety contract — real DB is NEVER mutated:
 *   READ  ops → superCall executed → real data fetched → shadow-first (shadow → real DB fallback)
 *   WRITE ops → superCall BLOCKED → MongoWriteMocks returns safe mock response
 *               BEFORE blocking: matching real-DB docs are cloned into shadow so
 *               subsequent reads in the same flow see the post-write state.
 *               Impact count (matched doc count) is recorded in CapturedCall.
 *
 * Pre-seed (fetch-before-mutate):
 *   When a write arrives and no shadow docs match the filter, the interceptor
 *   queries the REAL DB with that filter, seeds the results into shadow, then
 *   applies the write.  This means:
 *     • impactCount on the CapturedCall = real docs that would have been touched
 *     • snapshotBefore = copies of those docs pre-mutation (for test-data gen)
 *     • shadow holds the post-write state for downstream reads in the same flow
 *
 * Real DB access for pre-seeding is optional: pass a non-null MongoClient to
 * the constructor.  Without it the engine still works — writes buffer in shadow
 * but impact count stays 0 and pre-seeding is skipped.
 */
public class UniversalMongoInterceptor {

    private final CollectingQueryInterceptor interceptor;
    private final MongoClient                realClient;
    private final String                     realDbName;

    public UniversalMongoInterceptor(CollectingQueryInterceptor interceptor) {
        this(interceptor, null, null);
    }

    public UniversalMongoInterceptor(CollectingQueryInterceptor interceptor,
                                     MongoClient realClient, String realDbName) {
        this.interceptor = interceptor;
        this.realClient  = realClient;
        this.realDbName  = realDbName;
    }

    public Class<?> buildProxyClass(Class<?> mongoTemplateClass) {
        return new ByteBuddy()
                .subclass(mongoTemplateClass)
                .method(ElementMatchers.isPublic()
                        .and(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class))))
                .intercept(MethodDelegation.to(new Dispatcher(interceptor, realClient, realDbName)))
                .make()
                .load(mongoTemplateClass.getClassLoader(),
                        ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dispatcher
    // ─────────────────────────────────────────────────────────────────────────

    public static class Dispatcher {

        private final CollectingQueryInterceptor interceptor;
        private final ShadowMongoStore           shadow;
        private final MongoClient                realClient;
        private final String                     realDbName;

        public Dispatcher(CollectingQueryInterceptor interceptor,
                          MongoClient realClient, String realDbName) {
            this.interceptor = interceptor;
            this.shadow      = interceptor.getShadowStore();
            this.realClient  = realClient;
            this.realDbName  = realDbName;
        }

        @RuntimeType
        public Object intercept(@Origin Method method,
                                @AllArguments Object[] args,
                                @SuperCall Callable<?> superCall) throws Exception {
            String  methodName = method.getName();
            MongoOp op         = MongoOp.fromMethodName(methodName);
            String  collection = extractCollection(args, method.getReturnType());
            Object  input      = extractInput(op, args);
            Object  result;

            if (op.isMutation()) {
                // ── WRITE BLOCKED — pre-seed shadow, apply locally, record impact ──
                Document filterDoc = toFilterDoc(input);
                preSeedIfNeeded(collection, filterDoc);

                ShadowMongoStore.WriteImpact impact =
                        shadow.applyWriteWithImpact(op, collection, input, args);

                result = MongoWriteMocks.forReturnType(method.getReturnType(), args);
                interceptor.onMongoWrite(collection, op, input, impact);

            } else if (op.isFetchable()) {
                // ── READ — shadow store first, real DB as fallback ────────────
                result = shadowFirst(op, collection, input, superCall);
                interceptor.onMongoOperation(collection, op, input, toObjectList(result));

            } else {
                // ── META / PLUMBING / getCollection / bulkOps ─────────────────
                result = null;
                try { result = superCall.call(); } catch (Exception ignored) {}

                if (op == MongoOp.GET_COLLECTION && result != null) {
                    result = wrapMongoCollection(result, collection);
                } else if (op == MongoOp.BULK_WRITE && result != null) {
                    result = wrapBulkOperations(result, collection);
                } else if (op != MongoOp.GET_COLLECTION && op != MongoOp.UNKNOWN) {
                    interceptor.onMongoOperation(collection, op, input, toObjectList(result));
                }
            }

            return result;
        }

        // ── Pre-seed: clone real docs into shadow before applying a write ──

        /**
         * If shadow has no documents matching filterDoc for this collection,
         * fetch matching docs from the real DB and seed them into shadow.
         * This ensures:
         *   1. applyWriteWithImpact() can find and count the affected docs
         *   2. subsequent reads in the same flow return the post-write state
         */
        private void preSeedIfNeeded(String collection, Document filterDoc) {
            if (realClient == null || realDbName == null) return;
            try {
                long shadowMatches = shadow.countMatching(collection, filterDoc);
                if (shadowMatches == 0) {
                    var col = realClient.getDatabase(realDbName).getCollection(collection);
                    var cursor = (filterDoc != null && !filterDoc.isEmpty())
                            ? col.find(filterDoc) : col.find();
                    List<Document> docs = new ArrayList<>();
                    cursor.limit(200).forEach(docs::add);
                    if (!docs.isEmpty()) shadow.seedCollection(collection, docs);
                }
            } catch (Exception ignored) {}
        }

        private Document toFilterDoc(Object input) {
            if (input instanceof Document d) return d;
            if (input instanceof Map<?,?> m) {
                Document doc = new Document();
                m.forEach((k, v) -> doc.put(String.valueOf(k), v));
                return doc;
            }
            return null;
        }

        // ── Shadow-first read helper ──────────────────────────────────────

        private Object shadowFirst(MongoOp op, String collection, Object input,
                                    Callable<?> superCall) {
            List<Map<String, Object>> shadowDocs;
            if (op == MongoOp.AGGREGATE) {
                String pipeline = input != null ? input.toString() : "[]";
                shadowDocs = shadow.aggregate(collection, pipeline, 100);
            } else if (op == MongoOp.COUNT) {
                if (shadow.hasCollection(collection))
                    return shadow.count(collection, input);
                try { return superCall.call(); } catch (Exception e) { return 0L; }
            } else if (op == MongoOp.EXISTS) {
                if (shadow.hasCollection(collection))
                    return shadow.exists(collection, input);
                try { return superCall.call(); } catch (Exception e) { return false; }
            } else {
                shadowDocs = shadow.query(collection, input, 100);
            }

            if (!shadowDocs.isEmpty()) return new ArrayList<>(shadowDocs);
            try { return superCall.call(); } catch (Exception e) { return null; }
        }

        // ── MongoCollection proxy ─────────────────────────────────────────

        private Object wrapMongoCollection(Object raw, String collectionName) {
            Class<?>[] ifaces = allInterfaces(raw.getClass());
            if (ifaces.length == 0) return raw;
            try {
                return Proxy.newProxyInstance(
                        raw.getClass().getClassLoader(), ifaces,
                        (proxy, method, args) -> {
                            MongoOp op    = MongoOp.fromMethodName(method.getName());
                            Object  input = extractInputFromCollectionArgs(op, args);
                            Object  result;

                            if (op.isMutation()) {
                                Document filterDoc = toFilterDoc(input);
                                preSeedIfNeeded(collectionName, filterDoc);
                                ShadowMongoStore.WriteImpact impact =
                                        shadow.applyWriteWithImpact(op, collectionName, input, args);
                                result = MongoWriteMocks.forReturnType(method.getReturnType(), args);
                                interceptor.onMongoWrite(collectionName, op, input, impact);
                            } else if (op.isFetchable()) {
                                final Object[] finalArgs = args;
                                result = shadowFirst(op, collectionName, input,
                                        () -> method.invoke(raw, finalArgs));
                                interceptor.onMongoOperation(collectionName, op, input,
                                        toObjectList(result));
                            } else {
                                result = null;
                                try { result = method.invoke(raw, args); } catch (Exception ignored) {}
                                if (op != MongoOp.UNKNOWN) {
                                    interceptor.onMongoOperation(collectionName, op, input,
                                            toObjectList(result));
                                }
                            }
                            return result;
                        });
            } catch (Exception e) {
                return raw;
            }
        }

        // ── BulkOperations proxy ──────────────────────────────────────────

        private Object wrapBulkOperations(Object raw, String collectionName) {
            Class<?>[] ifaces = allInterfaces(raw.getClass());
            if (ifaces.length == 0) return raw;
            List<String> pendingOps = new ArrayList<>();
            try {
                return Proxy.newProxyInstance(
                        raw.getClass().getClassLoader(), ifaces,
                        (proxy, method, args) -> {
                            String mn = method.getName().toLowerCase();

                            if (mn.equals("execute")) {
                                interceptor.onMongoOperation(collectionName,
                                        MongoOp.BULK_WRITE, new ArrayList<>(pendingOps),
                                        Collections.emptyList());
                                return MongoWriteMocks.forReturnType(method.getReturnType(), args);
                            }

                            if (mn.equals("insert") || mn.equals("update") || mn.equals("upsert")
                                    || mn.equals("remove") || mn.equals("replaceone")
                                    || mn.equals("updateone") || mn.equals("updatefirst")
                                    || mn.equals("updatemovelti") || mn.equals("remove")) {
                                pendingOps.add(method.getName() + "(" + argsToString(args) + ")");
                            }

                            Object result = null;
                            try { result = method.invoke(raw, args); } catch (Exception ignored) {}
                            return result == raw ? proxy : result;
                        });
            } catch (Exception e) {
                return raw;
            }
        }

        // ── extraction helpers ────────────────────────────────────────────

        private String extractCollection(Object[] args, Class<?> returnType) {
            if (args == null) return "unknown";
            for (Object arg : args) {
                if (arg instanceof String s && !s.isBlank()
                        && s.length() < 128 && !s.contains(" ")) return s;
            }
            for (Object arg : args) {
                if (arg instanceof Class<?> c && c != Object.class)
                    return c.getSimpleName().toLowerCase();
            }
            return "unknown";
        }

        private Object extractInput(MongoOp op, Object[] args) {
            if (args == null || args.length == 0) return null;
            if (op == MongoOp.AGGREGATE || op == MongoOp.MAP_REDUCE) return extractPipeline(args);
            return extractFilter(args[0]);
        }

        private Object extractInputFromCollectionArgs(MongoOp op, Object[] args) {
            if (args == null || args.length == 0) return null;
            if (op == MongoOp.AGGREGATE) return extractPipeline(args);
            return args[0] != null ? args[0].toString() : null;
        }

        private Object extractFilter(Object first) {
            if (first == null) return null;
            try { return first.getClass().getMethod("getQueryObject").invoke(first); }
            catch (Exception ignored) {}
            return first.toString();
        }

        private Object extractPipeline(Object[] args) {
            for (Object arg : args) {
                if (arg == null) continue;
                try {
                    var m = arg.getClass().getMethod("toPipeline",
                            org.bson.codecs.configuration.CodecRegistry.class);
                    return m.invoke(arg,
                            com.mongodb.MongoClientSettings.getDefaultCodecRegistry());
                } catch (Exception ignored) {}
                try {
                    return arg.getClass().getMethod("getPipeline").invoke(arg);
                } catch (Exception ignored) {}
                String s = arg.toString();
                if (s.contains("$match") || s.contains("$group") || s.contains("$lookup")
                        || s.contains("Aggregation") || s.contains("pipeline")) return s;
            }
            return null;
        }

        private List<Object> toObjectList(Object result) {
            if (result == null) return Collections.emptyList();
            if (result instanceof List<?> list) return new ArrayList<>(list);
            return Collections.singletonList(result);
        }

        private Class<?>[] allInterfaces(Class<?> clazz) {
            Set<Class<?>> ifaces = new LinkedHashSet<>();
            Class<?> c = clazz;
            while (c != null && c != Object.class) {
                Collections.addAll(ifaces, c.getInterfaces());
                c = c.getSuperclass();
            }
            return ifaces.toArray(new Class[0]);
        }

        private String argsToString(Object[] args) {
            if (args == null) return "";
            StringBuilder sb = new StringBuilder();
            for (Object a : args) { if (sb.length() > 0) sb.append(", "); sb.append(a); }
            return sb.toString();
        }
    }
}
