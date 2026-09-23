/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  org.springframework.context.support.GenericApplicationContext
 */
package com.flowpoc.layer2;

import com.flowpoc.config.PocConfig;
import com.flowpoc.layer2.BeanFactoryLoader;
import com.flowpoc.layer2.CollectingQueryInterceptor;
import com.flowpoc.layer2.MongoOp;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.support.GenericApplicationContext;

public class DynamicFlowExecutor {
    private final BeanFactoryLoader beanFactoryLoader;
    private final CollectingQueryInterceptor interceptor;
    private final PocConfig config;

    public DynamicFlowExecutor(BeanFactoryLoader beanFactoryLoader, CollectingQueryInterceptor interceptor, PocConfig config) {
        this.beanFactoryLoader = beanFactoryLoader;
        this.interceptor = interceptor;
        this.config = config;
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public List<CollectingQueryInterceptor.CapturedCall> execute(FlowResult flowResult) {
        GenericApplicationContext context;
        ArrayList<CollectingQueryInterceptor.CapturedCall> allCaptured = new ArrayList<CollectingQueryInterceptor.CapturedCall>();
        try {
            context = this.beanFactoryLoader.buildContext();
            context.refresh();
        }
        catch (Exception e) {
            return allCaptured;
        }
        this.interceptor.clearAll();
        LinkedHashMap<String, Object> forwardCtx = new LinkedHashMap<String, Object>();
        try {
            for (FlowStep step : flowResult.allSteps()) {
                try {
                    this.interceptor.clear();
                    this.invokeStep(step, context, forwardCtx);
                    ArrayList<CollectingQueryInterceptor.CapturedCall> calls = new ArrayList<CollectingQueryInterceptor.CapturedCall>(this.interceptor.getCaptured());
                    this.attachCapturedToStep(step, calls);
                    allCaptured.addAll(calls);
                    this.updateForwardContext(forwardCtx, calls);
                }
                catch (Exception exception) {}
            }
        }
        finally {
            try {
                context.close();
            }
            catch (Exception exception) {}
        }
        return allCaptured;
    }

    private void invokeStep(FlowStep step, GenericApplicationContext ctx, Map<String, Object> forwardCtx) {
        String className = step.getClassName();
        String methodName = step.getMethodName();
        if (className == null || className.isBlank() || methodName == null || methodName.isBlank()) {
            return;
        }
        try {
            String simpleName = className.contains(".") ? className.substring(className.lastIndexOf(46) + 1) : className;
            Object bean = null;
            try {
                bean = ctx.getBean(simpleName);
            }
            catch (Exception exception) {
                // empty catch block
            }
            if (bean == null) {
                return;
            }
            Method target = this.findMethod(bean.getClass(), methodName);
            if (target == null) {
                return;
            }
            target.setAccessible(true);
            target.invoke(bean, this.buildArgs(target, forwardCtx));
        }
        catch (Exception exception) {
            // empty catch block
        }
    }

    private Method findMethod(Class<?> clazz, String methodName) {
        for (Method m : clazz.getMethods()) {
            if (!m.getName().equals(methodName)) continue;
            return m;
        }
        for (Method m : clazz.getDeclaredMethods()) {
            if (!m.getName().equals(methodName)) continue;
            return m;
        }
        return null;
    }

    private Object[] buildArgs(Method method, Map<String, Object> ctx) {
        Class<?>[] params = method.getParameterTypes();
        Object[] args = new Object[params.length];
        ArrayList<Object> ctxValues = new ArrayList<Object>(ctx.values());
        for (int i = 0; i < params.length; ++i) {
            Object match = this.findCompatibleValue(params[i], ctxValues);
            args[i] = match != null ? match : this.defaultFor(params[i]);
        }
        return args;
    }

    private Object findCompatibleValue(Class<?> type, List<Object> values) {
        for (Object v : values) {
            Number n;
            if (v == null) continue;
            if (type.isInstance(v)) {
                return v;
            }
            if (type == String.class) {
                return String.valueOf(v);
            }
            if ((type == Long.TYPE || type == Long.class) && v instanceof Number) {
                n = (Number)v;
                return n.longValue();
            }
            if ((type == Integer.TYPE || type == Integer.class) && v instanceof Number) {
                n = (Number)v;
                return n.intValue();
            }
            if ((type == Double.TYPE || type == Double.class) && v instanceof Number) {
                n = (Number)v;
                return n.doubleValue();
            }
            if ((type == Boolean.TYPE || type == Boolean.class) && v instanceof Boolean) {
                Boolean b = (Boolean)v;
                return b;
            }
            if (!type.getSimpleName().equals("ObjectId") || !(v instanceof String)) continue;
            String s = (String)v;
            try {
                return type.getConstructor(String.class).newInstance(s);
            }
            catch (Exception exception) {
            }
        }
        return null;
    }

    private Object defaultFor(Class<?> type) {
        if (type == Boolean.TYPE || type == Boolean.class) {
            return false;
        }
        if (type == Integer.TYPE || type == Integer.class) {
            return 0;
        }
        if (type == Long.TYPE || type == Long.class) {
            return 0L;
        }
        if (type == Double.TYPE || type == Double.class) {
            return 0.0;
        }
        if (type == String.class) {
            return "";
        }
        return null;
    }

    private void updateForwardContext(Map<String, Object> ctx, List<CollectingQueryInterceptor.CapturedCall> calls) {
        for (CollectingQueryInterceptor.CapturedCall call : calls) {
            if (!call.mongoOp().isRead() || call.results().isEmpty()) continue;
            Object firstResult = call.results().get(0);
            Map<String, Object> doc = null;
            if (firstResult instanceof Map) {
                Map<String, Object> m;
                doc = m = (Map<String, Object>)firstResult;
            } else if (firstResult != null) {
                doc = this.extractFieldsReflective(firstResult);
            }
            if (doc == null || doc.isEmpty()) continue;
            doc.forEach(ctx::put);
            if (call.collection() == null || call.collection().equals("unknown")) continue;
            String prefix = call.collection() + ".";
            doc.forEach((k, v) -> ctx.put(prefix + k, v));
        }
    }

    private Map<String, Object> extractFieldsReflective(Object obj) {
        LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
        try {
            for (Method m : obj.getClass().getMethods()) {
                String n = m.getName();
                if (m.getParameterCount() != 0 || !n.startsWith("get") || n.length() <= 3 || n.equals("getClass")) continue;
                try {
                    Object val = m.invoke(obj, new Object[0]);
                    if (val == null) continue;
                    result.put(this.lowerFirst(n.substring(3)), val);
                }
                catch (Exception exception) {
                    // empty catch block
                }
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
        return result;
    }

    private String lowerFirst(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    private void attachCapturedToStep(FlowStep step, List<CollectingQueryInterceptor.CapturedCall> calls) {
        for (CollectingQueryInterceptor.CapturedCall call : calls) {
            Object object;
            String rawLabel = this.buildDynamicLabel(call);
            ExtractedQuery.QueryType qtype = call.isSql() ? ExtractedQuery.QueryType.SELECT : this.mongoOpToQueryType(call.mongoOp());
            ExtractedQuery eq = new ExtractedQuery(rawLabel, qtype, call.collection(), step.getClassName(), step.getMethodName());
            if (call.mongoOp() == MongoOp.AGGREGATE && (object = call.input()) instanceof List) {
                List stages = (List)object;
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < stages.size(); ++i) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(stages.get(i));
                }
                sb.append("]");
                eq.setAggregationPipeline(sb.toString());
            }
            step.addQuery(eq);
            step.setBoundSql(rawLabel);
            if (call.params() == null) continue;
            step.setBoundParams(new ArrayList<Object>(call.params()));
        }
    }

    private String buildDynamicLabel(CollectingQueryInterceptor.CapturedCall call) {
        String inp;
        if (call.isSql()) {
            return "[DYNAMIC:SQL] " + call.sql();
        }
        String op = call.mongoOp().name();
        String col = call.collection() != null ? call.collection() : "unknown";
        String string = inp = call.input() != null ? call.input().toString() : "{}";
        if (call.mongoOp() == MongoOp.AGGREGATE) {
            return "[DYNAMIC:AGGREGATE] " + col + " PIPELINE " + inp;
        }
        if (call.mongoOp().isMutation()) {
            String impact = call.hasImpact() ? " IMPACT=" + call.impactCount() + " docs" : " IMPACT=unknown (no real DB)";
            return "[DYNAMIC:WRITE-BLOCKED:" + op + "] " + col + " INTENT " + inp + impact;
        }
        return "[DYNAMIC:" + op + "] " + col + " INPUT " + inp;
    }

    private ExtractedQuery.QueryType mongoOpToQueryType(MongoOp op) {
        if (op == null) {
            return ExtractedQuery.QueryType.UNKNOWN;
        }
        if (op.isRead()) {
            return ExtractedQuery.QueryType.SELECT;
        }
        return switch (op) {
            case INSERT, INSERT_MANY, SAVE, SAVE_ALL -> ExtractedQuery.QueryType.INSERT;
            case UPDATE, UPDATE_MANY, UPSERT -> ExtractedQuery.QueryType.UPDATE;
            case DELETE, DELETE_MANY, FIND_ALL_AND_REMOVE -> ExtractedQuery.QueryType.DELETE;
            case REPLACE -> ExtractedQuery.QueryType.UPDATE;
            default -> ExtractedQuery.QueryType.UNKNOWN;
        };
    }
}

