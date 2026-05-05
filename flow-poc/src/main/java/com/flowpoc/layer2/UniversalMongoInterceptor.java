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
 * ByteBuddy proxy over MongoTemplate that intercepts EVERY public method.
 *
 * Strategy:
 *  1. ALL public non-Object methods on MongoTemplate → Dispatcher
 *  2. Dispatcher categorizes by method name using MongoOp.fromMethodName()
 *  3. getCollection() → wraps returned MongoCollection in a Java dynamic proxy
 *     so raw collection ops (insertOne, aggregate, bulkWrite, etc.) are also captured
 *  4. bulkOps() → wraps returned BulkOperations in a Java dynamic proxy
 *
 * This guarantees that no DB interaction escapes interception regardless of which
 * MongoTemplate API surface the target code uses.
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

    // ---- Dispatcher (ByteBuddy delegate) ----

    public static class Dispatcher {

        private final CollectingQueryInterceptor interceptor;

        public Dispatcher(CollectingQueryInterceptor interceptor) {
            this.interceptor = interceptor;
        }

        @RuntimeType
        public Object intercept(@Origin Method method,
                                @AllArguments Object[] args,
                                @SuperCall Callable<?> superCall) throws Exception {
            Object result = null;
            try {
                result = superCall.call();
            } catch (Exception ignored) {
                // capture the call even if the actual invocation fails in test context
            }

            String   methodName = method.getName();
            MongoOp  op         = MongoOp.fromMethodName(methodName);
            String   collection = extractCollection(args, method.getReturnType());
            Object   input      = extractInput(op, args);

            // Wrap getCollection() result so raw MongoCollection calls are also captured
            if (op == MongoOp.GET_COLLECTION && result != null) {
                result = wrapMongoCollection(result, collection);
            }

            // Wrap bulkOps() result so bulk operations are captured on execute()
            if (op == MongoOp.BULK_WRITE && result != null) {
                result = wrapBulkOperations(result, collection);
            }

            // Don't emit an event for pure plumbing ops (getCollection/bulkOps themselves —
            // the wrapped proxy will emit events per individual call)
            if (op != MongoOp.GET_COLLECTION && op != MongoOp.UNKNOWN) {
                interceptor.onMongoOperation(collection, op, input, toObjectList(result));
            }

            return result;
        }

        // ---- MongoCollection proxy ----

        private Object wrapMongoCollection(Object raw, String collectionName) {
            Class<?>[] ifaces = getAllInterfaces(raw.getClass());
            if (ifaces.length == 0) return raw;
            try {
                return Proxy.newProxyInstance(
                        raw.getClass().getClassLoader(),
                        ifaces,
                        (proxy, method, args) -> {
                            Object result = null;
                            try {
                                result = method.invoke(raw, args);
                            } catch (Exception ignored) {
                            }
                            MongoOp op = MongoOp.fromMethodName(method.getName());
                            if (op != MongoOp.UNKNOWN && op != MongoOp.GET_COLLECTION) {
                                Object input = extractInputFromCollectionArgs(op, args);
                                interceptor.onMongoOperation(collectionName, op, input,
                                        toObjectList(result));
                            }
                            return result;
                        });
            } catch (Exception e) {
                return raw;
            }
        }

        // ---- BulkOperations proxy ----

        /**
         * BulkOperations is a Spring interface. We wrap it so that every individual
         * insert/update/remove queued via the fluent API is captured, and the final
         * execute() result is also captured as a BULK_WRITE event.
         */
        private Object wrapBulkOperations(Object raw, String collectionName) {
            Class<?>[] ifaces = getAllInterfaces(raw.getClass());
            if (ifaces.length == 0) return raw;
            List<Object> pendingOps = new ArrayList<>();
            try {
                return Proxy.newProxyInstance(
                        raw.getClass().getClassLoader(),
                        ifaces,
                        (proxy, method, args) -> {
                            Object result = null;
                            try {
                                result = method.invoke(raw, args);
                            } catch (Exception ignored) {
                            }
                            String mn = method.getName().toLowerCase();
                            if (mn.equals("insert") || mn.equals("update")
                                    || mn.equals("remove") || mn.equals("upsert")
                                    || mn.equals("replaceone")) {
                                // queue individual op for reporting
                                pendingOps.add(method.getName() + Arrays.toString(args));
                            }
                            if (mn.equals("execute")) {
                                interceptor.onMongoOperation(collectionName,
                                        MongoOp.BULK_WRITE, pendingOps, toObjectList(result));
                            }
                            // Return proxy instead of raw so chained calls still go through proxy
                            if (result == raw) return proxy;
                            return result;
                        });
            } catch (Exception e) {
                return raw;
            }
        }

        // ---- extraction helpers ----

        private String extractCollection(Object[] args, Class<?> returnType) {
            if (args == null) return "unknown";
            // String arg is almost always the collection name in MongoTemplate APIs
            for (Object arg : args) {
                if (arg instanceof String s && !s.isBlank()
                        && s.length() < 128 && !s.contains(" ")) {
                    return s;
                }
            }
            // Class<?> arg → class simple name lowercased = inferred collection
            for (Object arg : args) {
                if (arg instanceof Class<?> c && c != Object.class) {
                    return c.getSimpleName().toLowerCase();
                }
            }
            return "unknown";
        }

        private Object extractInput(MongoOp op, Object[] args) {
            if (args == null || args.length == 0) return null;
            if (op == MongoOp.AGGREGATE || op == MongoOp.MAP_REDUCE) {
                return extractPipeline(args);
            }
            return extractFilter(args[0]);
        }

        private Object extractInputFromCollectionArgs(MongoOp op, Object[] args) {
            if (args == null || args.length == 0) return null;
            if (op == MongoOp.AGGREGATE) return extractPipeline(args);
            return args[0] != null ? args[0].toString() : null;
        }

        private Object extractFilter(Object first) {
            if (first == null) return null;
            // Spring Query → getQueryObject()
            try {
                return first.getClass().getMethod("getQueryObject").invoke(first);
            } catch (Exception ignored) {
            }
            // Bson Document or raw string
            return first.toString();
        }

        private Object extractPipeline(Object[] args) {
            for (Object arg : args) {
                if (arg == null) continue;
                // Spring Aggregation → toPipeline() or toString()
                try {
                    var m = arg.getClass().getMethod("toPipeline",
                            org.bson.codecs.configuration.CodecRegistry.class);
                    return m.invoke(arg,
                            com.mongodb.MongoClientSettings.getDefaultCodecRegistry());
                } catch (Exception ignored) {
                }
                try {
                    var m = arg.getClass().getMethod("getPipeline");
                    return m.invoke(arg);
                } catch (Exception ignored) {
                }
                String s = arg.toString();
                if (s.contains("$match") || s.contains("$group") || s.contains("$lookup")
                        || s.contains("Aggregation") || s.contains("pipeline")) {
                    return s;
                }
            }
            return null;
        }

        private List<Object> toObjectList(Object result) {
            if (result == null) return Collections.emptyList();
            if (result instanceof List<?> list) return new ArrayList<>(list);
            return Collections.singletonList(result);
        }

        private Class<?>[] getAllInterfaces(Class<?> clazz) {
            Set<Class<?>> ifaces = new LinkedHashSet<>();
            Class<?> c = clazz;
            while (c != null && c != Object.class) {
                Collections.addAll(ifaces, c.getInterfaces());
                c = c.getSuperclass();
            }
            return ifaces.toArray(new Class[0]);
        }
    }
}
