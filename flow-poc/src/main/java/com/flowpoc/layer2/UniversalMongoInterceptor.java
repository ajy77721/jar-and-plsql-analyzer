package com.flowpoc.layer2;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.*;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * ByteBuddy proxy over MongoTemplate that captures EVERY public method call.
 *
 * Safety contract — real DB is NEVER mutated:
 *   READ  ops (find*, aggregate, count, exists, stream, mapReduce, FIND_AND_*)
 *         → superCall executed → real data fetched → captured
 *   WRITE ops (insert*, save*, update*, delete*, remove*, replace*, bulkWrite)
 *         → superCall BLOCKED → MongoWriteMocks returns safe mock response
 *         → intent (filter + document) captured for analysis only
 *
 * Additional wrapping:
 *   getCollection() → returned MongoCollection wrapped in Java dynamic proxy
 *                     so raw collection calls (insertOne, aggregate, bulkWrite…) also go through
 *                     the same read/write safety contract.
 *   bulkOps()       → returned BulkOperations wrapped so queued ops + execute() are captured
 *                     without touching the DB.
 */
public class UniversalMongoInterceptor {

    private final CollectingQueryInterceptor interceptor;

    public UniversalMongoInterceptor(CollectingQueryInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public Class<?> buildProxyClass(Class<?> mongoTemplateClass) {
        return new ByteBuddy()
                .subclass(mongoTemplateClass)
                .method(ElementMatchers.isPublic()
                        .and(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class))))
                .intercept(MethodDelegation.to(new Dispatcher(interceptor)))
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

        public Dispatcher(CollectingQueryInterceptor interceptor) {
            this.interceptor = interceptor;
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
                // ── WRITE BLOCKED ──────────────────────────────────────────
                // Return a mock success object so the calling code doesn't
                // throw NPE and can proceed to the next step.
                result = MongoWriteMocks.forReturnType(method.getReturnType(), args);
                interceptor.onMongoOperation(collection, op, input, Collections.emptyList());

            } else {
                // ── READ / META / PLUMBING ─────────────────────────────────
                result = null;
                try { result = superCall.call(); } catch (Exception ignored) {}

                if (op == MongoOp.GET_COLLECTION && result != null) {
                    // Wrap raw collection so direct collection-level calls are also captured
                    result = wrapMongoCollection(result, collection);
                } else if (op == MongoOp.BULK_WRITE && result != null) {
                    result = wrapBulkOperations(result, collection);
                } else if (op != MongoOp.GET_COLLECTION && op != MongoOp.UNKNOWN) {
                    interceptor.onMongoOperation(collection, op, input, toObjectList(result));
                }
            }

            return result;
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
                                result = MongoWriteMocks.forReturnType(method.getReturnType(), args);
                                interceptor.onMongoOperation(collectionName, op, input,
                                        Collections.emptyList());
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

        /**
         * Wraps the BulkOperations object so:
         *   - Individual insert/update/remove calls are queued (not executed)
         *   - execute() captures all pending ops and returns a mock BulkWriteResult
         */
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
                                // Fire the capture event — no real execution
                                interceptor.onMongoOperation(collectionName,
                                        MongoOp.BULK_WRITE, new ArrayList<>(pendingOps),
                                        Collections.emptyList());
                                return MongoWriteMocks.forReturnType(method.getReturnType(), args);
                            }

                            // Queue the individual op description
                            if (mn.equals("insert") || mn.equals("update") || mn.equals("upsert")
                                    || mn.equals("remove") || mn.equals("replaceone")
                                    || mn.equals("updateone") || mn.equals("updatefirst")
                                    || mn.equals("updatemovelti") || mn.equals("remove")) {
                                pendingOps.add(method.getName() + "(" + argsToString(args) + ")");
                            }

                            // Return proxy so fluent chaining keeps going through proxy
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
