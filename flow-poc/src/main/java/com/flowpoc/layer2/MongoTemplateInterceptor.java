package com.flowpoc.layer2;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * ByteBuddy proxy over MongoTemplate that intercepts:
 *   find, insert, updateFirst, updateMulti, remove, aggregate
 *
 * For aggregate(): the pipeline stages are captured as their toString()
 * representations since the Aggregation object from the target classloader
 * cannot be cast to our classloader's types.
 */
public class MongoTemplateInterceptor {

    private final CollectingQueryInterceptor interceptor;

    public MongoTemplateInterceptor(CollectingQueryInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public Class<?> buildProxyClass(Class<?> mongoTemplateClass) {
        return new ByteBuddy()
                .subclass(mongoTemplateClass)
                .method(ElementMatchers.namedOneOf(
                        "find", "insert", "updateFirst", "updateMulti", "remove", "aggregate")
                        .and(ElementMatchers.isPublic()))
                .intercept(MethodDelegation.to(new Dispatcher(interceptor)))
                .make()
                .load(mongoTemplateClass.getClassLoader(),
                        ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
    }

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
            }

            String methodName  = method.getName();
            String collection  = extractCollectionName(args);
            Object filter      = extractFilter(args);

            switch (methodName) {
                case "find" -> {
                    interceptor.onMongoFind(collection, filter, toObjectList(result));
                }
                case "aggregate" -> {
                    List<Object> stages = extractPipelineStages(args);
                    interceptor.onMongoAggregate(collection, stages, toObjectList(result));
                }
                case "insert" -> interceptor.onMongoInsert(collection, args.length > 0 ? args[0] : null);
                case "updateFirst", "updateMulti" ->
                        interceptor.onMongoUpdate(collection, filter, extractUpdate(args));
                case "remove" -> interceptor.onMongoUpdate(collection, filter, null);
            }

            return result;
        }

        // ---- extraction helpers ----

        private String extractCollectionName(Object[] args) {
            if (args == null) return "unknown";
            for (Object arg : args) {
                if (arg instanceof String s && !s.isBlank()) return s;
            }
            // MongoTemplate.aggregate(Aggregation, Class<?> inputType, ...) — try inputType.getSimpleName
            for (Object arg : args) {
                if (arg instanceof Class<?> c) {
                    return c.getSimpleName().toLowerCase();
                }
            }
            return "unknown";
        }

        private Object extractFilter(Object[] args) {
            if (args == null || args.length == 0) return null;
            Object first = args[0];
            if (first == null) return null;
            try {
                Method getCriteria = first.getClass().getMethod("getQueryObject");
                return getCriteria.invoke(first);
            } catch (Exception ignored) {
            }
            return first.toString();
        }

        private Object extractUpdate(Object[] args) {
            if (args == null || args.length < 2) return null;
            Object second = args[1];
            if (second == null) return null;
            try {
                Method getUpdate = second.getClass().getMethod("getUpdateObject");
                return getUpdate.invoke(second);
            } catch (Exception ignored) {
            }
            return second.toString();
        }

        /**
         * Extracts pipeline stages from the Aggregation argument.
         * MongoTemplate.aggregate(Aggregation agg, String collection, Class<?> output)
         * The Aggregation object carries AggregationOperations (stages).
         * We use reflection to call toPipeline() or getPipeline() if available,
         * otherwise fall back to toString().
         */
        private List<Object> extractPipelineStages(Object[] args) {
            if (args == null) return Collections.emptyList();
            for (Object arg : args) {
                if (arg == null) continue;
                Class<?> cls = arg.getClass();
                // Try toPipeline() — returns List<Document>
                try {
                    Method toPipeline = cls.getMethod("toPipeline",
                            org.bson.codecs.configuration.CodecRegistry.class);
                    Object pipeline = toPipeline.invoke(arg,
                            com.mongodb.MongoClientSettings.getDefaultCodecRegistry());
                    if (pipeline instanceof List<?> list) return new ArrayList<>(list);
                } catch (Exception ignored) {
                }
                // Try getPipeline()
                try {
                    Method getPipeline = cls.getMethod("getPipeline");
                    Object pipeline = getPipeline.invoke(arg);
                    if (pipeline instanceof List<?> list) return new ArrayList<>(list);
                } catch (Exception ignored) {
                }
                // Fallback: toString representation of the aggregation
                String s = arg.toString();
                if (s.contains("$match") || s.contains("$group") || s.contains("$lookup")) {
                    return Collections.singletonList(s);
                }
            }
            return Collections.emptyList();
        }

        private List<Object> toObjectList(Object result) {
            if (result == null) return Collections.emptyList();
            if (result instanceof List<?> list) return new ArrayList<>(list);
            return Collections.singletonList(result);
        }
    }
}
