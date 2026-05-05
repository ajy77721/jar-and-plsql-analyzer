package com.flowpoc.layer2;

import com.flowpoc.config.PocConfig;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import org.springframework.context.support.GenericApplicationContext;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Executes each step of a FlowResult against the loaded JAR's Spring context.
 *
 * Forward flow (predicate propagation at runtime):
 *   - Step N executes → read results captured by interceptor
 *   - Field values from those results are extracted into a "context map"
 *   - Step N+1 method arguments are built using that context (String/long/int/etc.)
 *     so the actual bound values flow naturally through the call chain
 *
 * Safety:
 *   - Write operations (INSERT/UPDATE/DELETE/BULK) are blocked by UniversalMongoInterceptor
 *     and never reach the real DB — only read operations execute for real
 */
public class DynamicFlowExecutor {

    private final BeanFactoryLoader          beanFactoryLoader;
    private final CollectingQueryInterceptor interceptor;
    private final PocConfig                  config;

    public DynamicFlowExecutor(BeanFactoryLoader beanFactoryLoader,
                               CollectingQueryInterceptor interceptor,
                               PocConfig config) {
        this.beanFactoryLoader = beanFactoryLoader;
        this.interceptor       = interceptor;
        this.config            = config;
    }

    public List<CollectingQueryInterceptor.CapturedCall> execute(FlowResult flowResult) {
        List<CollectingQueryInterceptor.CapturedCall> allCaptured = new ArrayList<>();
        GenericApplicationContext context;

        try {
            context = beanFactoryLoader.buildContext();
            context.refresh();
        } catch (Exception e) {
            return allCaptured;
        }

        // Clear shadow store + captured calls from any previous flow execution
        interceptor.clearAll();

        // Forward context: field values from prior step's DB results → next step's method args
        Map<String, Object> forwardCtx = new LinkedHashMap<>();

        try {
            for (FlowStep step : flowResult.allSteps()) {
                try {
                    interceptor.clear();   // clears captured only — shadow store persists across steps
                    invokeStep(step, context, forwardCtx);

                    List<CollectingQueryInterceptor.CapturedCall> calls =
                            new ArrayList<>(interceptor.getCaptured());
                    attachCapturedToStep(step, calls);
                    allCaptured.addAll(calls);

                    // Propagate read results into context for next step
                    updateForwardContext(forwardCtx, calls);

                } catch (Exception ignored) {
                }
            }
        } finally {
            try { context.close(); } catch (Exception ignored) {}
        }

        return allCaptured;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Step invocation
    // ─────────────────────────────────────────────────────────────────────────

    private void invokeStep(FlowStep step, GenericApplicationContext ctx,
                            Map<String, Object> forwardCtx) {
        String className  = step.getClassName();
        String methodName = step.getMethodName();
        if (className == null || className.isBlank() || methodName == null || methodName.isBlank())
            return;

        try {
            String simpleName = className.contains(".")
                    ? className.substring(className.lastIndexOf('.') + 1)
                    : className;
            Object bean = null;
            try { bean = ctx.getBean(simpleName); } catch (Exception ignored) {}
            if (bean == null) return;

            Method target = findMethod(bean.getClass(), methodName);
            if (target == null) return;

            target.setAccessible(true);
            target.invoke(bean, buildArgs(target, forwardCtx));
        } catch (Exception ignored) {
        }
    }

    private Method findMethod(Class<?> clazz, String methodName) {
        for (Method m : clazz.getMethods())         { if (m.getName().equals(methodName)) return m; }
        for (Method m : clazz.getDeclaredMethods()) { if (m.getName().equals(methodName)) return m; }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Argument building — uses forward context from prior step's DB results
    // ─────────────────────────────────────────────────────────────────────────

    private Object[] buildArgs(Method method, Map<String, Object> ctx) {
        Class<?>[] params = method.getParameterTypes();
        Object[] args = new Object[params.length];
        List<Object> ctxValues = new ArrayList<>(ctx.values());

        for (int i = 0; i < params.length; i++) {
            Object match = findCompatibleValue(params[i], ctxValues);
            args[i] = match != null ? match : defaultFor(params[i]);
        }
        return args;
    }

    /**
     * Finds the first value in the context that is assignment-compatible with the
     * required parameter type, including common coercions (Number → int/long/double,
     * Object → String, String → ObjectId).
     */
    private Object findCompatibleValue(Class<?> type, List<Object> values) {
        for (Object v : values) {
            if (v == null) continue;
            if (type.isInstance(v))                                              return v;
            if (type == String.class)                                            return String.valueOf(v);
            if ((type == long.class   || type == Long.class)
                    && v instanceof Number n)                                    return n.longValue();
            if ((type == int.class    || type == Integer.class)
                    && v instanceof Number n)                                    return n.intValue();
            if ((type == double.class || type == Double.class)
                    && v instanceof Number n)                                    return n.doubleValue();
            if ((type == boolean.class || type == Boolean.class)
                    && v instanceof Boolean b)                                   return b;
            // ObjectId from String
            if (type.getSimpleName().equals("ObjectId") && v instanceof String s) {
                try {
                    return type.getConstructor(String.class).newInstance(s);
                } catch (Exception ignored) {}
            }
        }
        return null;
    }

    private Object defaultFor(Class<?> type) {
        if (type == boolean.class || type == Boolean.class) return false;
        if (type == int.class    || type == Integer.class)  return 0;
        if (type == long.class   || type == Long.class)     return 0L;
        if (type == double.class || type == Double.class)   return 0.0;
        if (type == String.class)                           return "";
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Forward context update — extracts field values from read results
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Populates the forward context with all field→value entries from the first
     * document returned by any read operation in this step.
     * Values are keyed both plain ("userId") and with collection prefix ("users.userId")
     * to allow unambiguous downstream resolution.
     */
    @SuppressWarnings("unchecked")
    private void updateForwardContext(Map<String, Object> ctx,
                                      List<CollectingQueryInterceptor.CapturedCall> calls) {
        for (CollectingQueryInterceptor.CapturedCall call : calls) {
            if (!call.mongoOp().isRead()) continue;
            if (call.results().isEmpty()) continue;

            Object firstResult = call.results().get(0);
            Map<String, Object> doc = null;

            if (firstResult instanceof Map<?,?> m) {
                doc = (Map<String, Object>) m;
            } else if (firstResult != null) {
                // Try to extract fields via reflection (POJO / Spring Data entity)
                doc = extractFieldsReflective(firstResult);
            }

            if (doc == null || doc.isEmpty()) continue;

            doc.forEach(ctx::put);
            if (call.collection() != null && !call.collection().equals("unknown")) {
                String prefix = call.collection() + ".";
                doc.forEach((k, v) -> ctx.put(prefix + k, v));
            }
        }
    }

    private Map<String, Object> extractFieldsReflective(Object obj) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            for (Method m : obj.getClass().getMethods()) {
                String n = m.getName();
                if (m.getParameterCount() != 0) continue;
                if (n.startsWith("get") && n.length() > 3 && !n.equals("getClass")) {
                    try {
                        Object val = m.invoke(obj);
                        if (val != null) result.put(lowerFirst(n.substring(3)), val);
                    } catch (Exception ignored) {}
                }
            }
        } catch (Exception ignored) {}
        return result;
    }

    private String lowerFirst(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Attach captured calls to the flow step for reporting
    // ─────────────────────────────────────────────────────────────────────────

    private void attachCapturedToStep(FlowStep step,
                                      List<CollectingQueryInterceptor.CapturedCall> calls) {
        for (CollectingQueryInterceptor.CapturedCall call : calls) {
            String rawLabel = buildDynamicLabel(call);
            ExtractedQuery.QueryType qtype = call.isSql()
                    ? ExtractedQuery.QueryType.SELECT
                    : mongoOpToQueryType(call.mongoOp());

            ExtractedQuery eq = new ExtractedQuery(
                    rawLabel, qtype, call.collection(),
                    step.getClassName(), step.getMethodName());

            if (call.mongoOp() == MongoOp.AGGREGATE && call.input() instanceof List<?> stages) {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < stages.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append(stages.get(i));
                }
                sb.append("]");
                eq.setAggregationPipeline(sb.toString());
            }

            step.addQuery(eq);
            step.setBoundSql(rawLabel);
            if (call.params() != null) step.setBoundParams(new ArrayList<>(call.params()));
        }
    }

    private String buildDynamicLabel(CollectingQueryInterceptor.CapturedCall call) {
        if (call.isSql()) return "[DYNAMIC:SQL] " + call.sql();
        String op  = call.mongoOp().name();
        String col = call.collection() != null ? call.collection() : "unknown";
        String inp = call.input() != null ? call.input().toString() : "{}";
        if (call.mongoOp() == MongoOp.AGGREGATE)
            return "[DYNAMIC:AGGREGATE] " + col + " PIPELINE " + inp;
        if (call.mongoOp().isMutation())
            return "[DYNAMIC:WRITE-BLOCKED:" + op + "] " + col + " INTENT " + inp;
        return "[DYNAMIC:" + op + "] " + col + " INPUT " + inp;
    }

    private ExtractedQuery.QueryType mongoOpToQueryType(MongoOp op) {
        if (op == null) return ExtractedQuery.QueryType.UNKNOWN;
        if (op.isRead())  return ExtractedQuery.QueryType.SELECT;
        return switch (op) {
            case INSERT, INSERT_MANY, SAVE, SAVE_ALL -> ExtractedQuery.QueryType.INSERT;
            case UPDATE, UPDATE_MANY, UPSERT         -> ExtractedQuery.QueryType.UPDATE;
            case DELETE, DELETE_MANY,
                 FIND_ALL_AND_REMOVE                 -> ExtractedQuery.QueryType.DELETE;
            case REPLACE                             -> ExtractedQuery.QueryType.UPDATE;
            default                                  -> ExtractedQuery.QueryType.UNKNOWN;
        };
    }
}
