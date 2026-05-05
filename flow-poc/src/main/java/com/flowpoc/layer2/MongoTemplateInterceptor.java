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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class MongoTemplateInterceptor {

    private final CollectingQueryInterceptor interceptor;

    public MongoTemplateInterceptor(CollectingQueryInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public Class<?> buildProxyClass(Class<?> mongoTemplateClass) {
        return new ByteBuddy()
                .subclass(mongoTemplateClass)
                .method(ElementMatchers.namedOneOf("find", "insert", "updateFirst", "remove")
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

            String methodName = method.getName();
            String collection = extractCollectionName(args);
            Object filter = extractFilter(args);

            switch (methodName) {
                case "find" -> {
                    List<Object> resultList = toObjectList(result);
                    interceptor.onMongoFind(collection, filter, resultList);
                }
                case "insert" -> interceptor.onMongoInsert(collection, args.length > 0 ? args[0] : null);
                case "updateFirst" -> interceptor.onMongoUpdate(collection, filter, extractUpdate(args));
                case "remove" -> interceptor.onMongoUpdate(collection, filter, null);
            }

            return result;
        }

        private String extractCollectionName(Object[] args) {
            for (Object arg : args) {
                if (arg instanceof String s && !s.isEmpty()) return s;
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

        private List<Object> toObjectList(Object result) {
            if (result == null) return Collections.emptyList();
            if (result instanceof List<?> list) return new ArrayList<>(list);
            return Collections.singletonList(result);
        }
    }
}
