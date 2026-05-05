package com.flowpoc.layer2;

import com.flowpoc.config.PocConfig;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import org.springframework.context.support.GenericApplicationContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class DynamicFlowExecutor {

    private final BeanFactoryLoader beanFactoryLoader;
    private final CollectingQueryInterceptor interceptor;
    private final PocConfig config;

    public DynamicFlowExecutor(BeanFactoryLoader beanFactoryLoader,
                               CollectingQueryInterceptor interceptor,
                               PocConfig config) {
        this.beanFactoryLoader = beanFactoryLoader;
        this.interceptor       = interceptor;
        this.config            = config;
    }

    public List<CollectingQueryInterceptor.CapturedCall> execute(FlowResult flowResult) {
        List<CollectingQueryInterceptor.CapturedCall> allCaptured = new ArrayList<>();
        GenericApplicationContext context = null;

        try {
            context = beanFactoryLoader.buildContext();
            context.refresh();
        } catch (Exception e) {
            return allCaptured;
        }

        try {
            for (FlowStep step : flowResult.allSteps()) {
                try {
                    interceptor.clear();
                    invokeStep(step, context);
                    List<CollectingQueryInterceptor.CapturedCall> calls = new ArrayList<>(interceptor.getCaptured());
                    attachCapturedToStep(step, calls);
                    allCaptured.addAll(calls);
                } catch (Exception ignored) {
                }
            }
        } finally {
            try {
                context.close();
            } catch (Exception ignored) {
            }
        }

        return allCaptured;
    }

    private void invokeStep(FlowStep step, GenericApplicationContext context) {
        String className  = step.getClassName();
        String methodName = step.getMethodName();
        if (className == null || className.isBlank() || methodName == null || methodName.isBlank()) return;

        try {
            String simpleName = className.contains(".")
                    ? className.substring(className.lastIndexOf('.') + 1)
                    : className;

            Object bean = null;
            try {
                bean = context.getBean(simpleName);
            } catch (Exception ignored) {
            }
            if (bean == null) return;

            Method target = findMethod(bean.getClass(), methodName);
            if (target == null) return;

            Object[] args = buildArgs(target);
            target.setAccessible(true);
            target.invoke(bean, args);
        } catch (Exception ignored) {
        }
    }

    private Method findMethod(Class<?> clazz, String methodName) {
        for (Method m : clazz.getMethods()) {
            if (m.getName().equals(methodName)) return m;
        }
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.getName().equals(methodName)) return m;
        }
        return null;
    }

    private Object[] buildArgs(Method method) {
        Class<?>[] params = method.getParameterTypes();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            args[i] = defaultFor(params[i]);
        }
        return args;
    }

    private Object defaultFor(Class<?> type) {
        if (type == boolean.class || type == Boolean.class) return false;
        if (type == int.class || type == Integer.class) return 0;
        if (type == long.class || type == Long.class) return 0L;
        if (type == double.class || type == Double.class) return 0.0;
        if (type == String.class) return "";
        return null;
    }

    private void attachCapturedToStep(FlowStep step,
                                      List<CollectingQueryInterceptor.CapturedCall> calls) {
        for (CollectingQueryInterceptor.CapturedCall call : calls) {
            String rawSql = buildDynamicLabel(call);
            ExtractedQuery eq = new ExtractedQuery(
                    rawSql,
                    call.type().contains("SQL") ? ExtractedQuery.QueryType.SELECT
                            : ExtractedQuery.QueryType.UNKNOWN,
                    call.collection(),
                    step.getClassName(),
                    step.getMethodName());
            step.addQuery(eq);
            step.setBoundSql(rawSql);
            if (call.params() != null) step.setBoundParams(new ArrayList<>(call.params()));
        }
    }

    private String buildDynamicLabel(CollectingQueryInterceptor.CapturedCall call) {
        if (call.sql() != null) return "[DYNAMIC] " + call.sql();
        String filter = call.filter() != null ? call.filter().toString() : "{}";
        return "[DYNAMIC] " + call.type() + " " + call.collection() + " FILTER " + filter;
    }
}
