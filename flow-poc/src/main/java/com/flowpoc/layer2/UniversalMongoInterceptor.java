/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.mongodb.MongoClientSettings
 *  com.mongodb.client.FindIterable
 *  com.mongodb.client.MongoClient
 *  com.mongodb.client.MongoCollection
 *  net.bytebuddy.ByteBuddy
 *  net.bytebuddy.dynamic.loading.ClassLoadingStrategy
 *  net.bytebuddy.dynamic.loading.ClassLoadingStrategy$Default
 *  net.bytebuddy.implementation.Implementation
 *  net.bytebuddy.implementation.MethodDelegation
 *  net.bytebuddy.implementation.bind.annotation.AllArguments
 *  net.bytebuddy.implementation.bind.annotation.Origin
 *  net.bytebuddy.implementation.bind.annotation.RuntimeType
 *  net.bytebuddy.implementation.bind.annotation.SuperCall
 *  net.bytebuddy.matcher.ElementMatcher
 *  net.bytebuddy.matcher.ElementMatchers
 *  org.bson.Document
 *  org.bson.codecs.configuration.CodecRegistry
 *  org.bson.conversions.Bson
 */
package com.flowpoc.layer2;

import com.flowpoc.layer2.CollectingQueryInterceptor;
import com.flowpoc.layer2.MongoOp;
import com.flowpoc.layer2.MongoWriteMocks;
import com.flowpoc.layer2.ShadowMongoStore;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class UniversalMongoInterceptor {
    private final CollectingQueryInterceptor interceptor;
    private final MongoClient realClient;
    private final String realDbName;

    public UniversalMongoInterceptor(CollectingQueryInterceptor interceptor) {
        this(interceptor, null, null);
    }

    public UniversalMongoInterceptor(CollectingQueryInterceptor interceptor, MongoClient realClient, String realDbName) {
        this.interceptor = interceptor;
        this.realClient = realClient;
        this.realDbName = realDbName;
    }

    public Class<?> buildProxyClass(Class<?> mongoTemplateClass) {
        return new ByteBuddy().subclass(mongoTemplateClass).method((ElementMatcher)ElementMatchers.isPublic().and((ElementMatcher)ElementMatchers.not((ElementMatcher)ElementMatchers.isDeclaredBy(Object.class)))).intercept((Implementation)MethodDelegation.to((Object)new Dispatcher(this.interceptor, this.realClient, this.realDbName))).make().load(mongoTemplateClass.getClassLoader(), (ClassLoadingStrategy)ClassLoadingStrategy.Default.WRAPPER).getLoaded();
    }

    public static class Dispatcher {
        private final CollectingQueryInterceptor interceptor;
        private final ShadowMongoStore shadow;
        private final MongoClient realClient;
        private final String realDbName;

        public Dispatcher(CollectingQueryInterceptor interceptor, MongoClient realClient, String realDbName) {
            this.interceptor = interceptor;
            this.shadow = interceptor.getShadowStore();
            this.realClient = realClient;
            this.realDbName = realDbName;
        }

        @RuntimeType
        public Object intercept(@Origin Method method, @AllArguments Object[] args, @SuperCall Callable<?> superCall) throws Exception {
            Object result;
            String methodName = method.getName();
            MongoOp op = MongoOp.fromMethodName(methodName);
            String collection = this.extractCollection(args, method.getReturnType());
            Object input = this.extractInput(op, args);
            if (op.isMutation()) {
                Document filterDoc;
                Document document = filterDoc = op.isFilterWrite() ? this.toFilterDoc(input) : null;
                if (filterDoc != null) {
                    this.preSeedIfNeeded(collection, filterDoc);
                }
                ShadowMongoStore.WriteImpact impact = this.shadow.applyWriteWithImpact(op, collection, input, args);
                result = MongoWriteMocks.forReturnType(method.getReturnType(), args);
                this.interceptor.onMongoWrite(collection, op, input, impact);
            } else if (op.isFetchable()) {
                result = this.shadowFirst(op, collection, input, superCall);
                this.interceptor.onMongoOperation(collection, op, input, this.toObjectList(result));
            } else {
                result = null;
                try {
                    result = superCall.call();
                }
                catch (Exception exception) {
                    // empty catch block
                }
                if (op == MongoOp.GET_COLLECTION && result != null) {
                    result = this.wrapMongoCollection(result, collection);
                } else if (op == MongoOp.BULK_WRITE && result != null) {
                    result = this.wrapBulkOperations(result, collection);
                } else if (op != MongoOp.GET_COLLECTION && op != MongoOp.UNKNOWN) {
                    this.interceptor.onMongoOperation(collection, op, input, this.toObjectList(result));
                }
            }
            return result;
        }

        private void preSeedIfNeeded(String collection, Document filterDoc) {
            if (this.realClient == null || this.realDbName == null) {
                return;
            }
            try {
                long shadowMatches = this.shadow.countMatching(collection, filterDoc);
                if (shadowMatches == 0L) {
                    MongoCollection<Document> col = this.realClient.getDatabase(this.realDbName).getCollection(collection);
                    FindIterable<Document> cursor = filterDoc != null && !filterDoc.isEmpty() ? col.find((Bson)filterDoc) : col.find();
                    ArrayList<Document> docs = new ArrayList<Document>();
                    cursor.limit(200).forEach(docs::add);
                    if (!docs.isEmpty()) {
                        this.shadow.seedCollection(collection, docs);
                    }
                }
            }
            catch (Exception exception) {
                // empty catch block
            }
        }

        private Document toFilterDoc(Object input) {
            String s;
            if (input instanceof Document) {
                Document d = (Document)input;
                return d;
            }
            if (input instanceof Map) {
                Map m = (Map)input;
                Document doc = new Document();
                m.forEach((k, v) -> doc.put(String.valueOf(k), v));
                return doc;
            }
            if (input instanceof String && !(s = (String)input).isBlank()) {
                try {
                    return Document.parse((String)s);
                }
                catch (Exception exception) {
                    // empty catch block
                }
            }
            return null;
        }

        private Object shadowFirst(MongoOp op, String collection, Object input, Callable<?> superCall) {
            List<Map<String, Object>> shadowDocs;
            if (op == MongoOp.AGGREGATE) {
                String pipeline = input != null ? input.toString() : "[]";
                shadowDocs = this.shadow.aggregate(collection, pipeline, 100);
            } else {
                if (op == MongoOp.COUNT) {
                    if (this.shadow.hasCollection(collection)) {
                        return this.shadow.count(collection, input);
                    }
                    try {
                        return superCall.call();
                    }
                    catch (Exception e) {
                        return 0L;
                    }
                }
                if (op == MongoOp.EXISTS) {
                    if (this.shadow.hasCollection(collection)) {
                        return this.shadow.exists(collection, input);
                    }
                    try {
                        return superCall.call();
                    }
                    catch (Exception e) {
                        return false;
                    }
                }
                shadowDocs = this.shadow.query(collection, input, 100);
            }
            if (!shadowDocs.isEmpty()) {
                return new ArrayList<Map<String, Object>>(shadowDocs);
            }
            try {
                return superCall.call();
            }
            catch (Exception e) {
                return null;
            }
        }

        private Object wrapMongoCollection(Object raw, String collectionName) {
            Class<?>[] ifaces = this.allInterfaces(raw.getClass());
            if (ifaces.length == 0) {
                return raw;
            }
            try {
                return Proxy.newProxyInstance(raw.getClass().getClassLoader(), ifaces, (proxy, method, args) -> {
                    Object result;
                    MongoOp op = MongoOp.fromMethodName(method.getName());
                    Object input = this.extractInputFromCollectionArgs(op, args);
                    if (op.isMutation()) {
                        Document filterDoc;
                        Document document = filterDoc = op.isFilterWrite() ? this.toFilterDoc(input) : null;
                        if (filterDoc != null) {
                            this.preSeedIfNeeded(collectionName, filterDoc);
                        }
                        ShadowMongoStore.WriteImpact impact = this.shadow.applyWriteWithImpact(op, collectionName, input, args);
                        result = MongoWriteMocks.forReturnType(method.getReturnType(), args);
                        this.interceptor.onMongoWrite(collectionName, op, input, impact);
                    } else if (op.isFetchable()) {
                        Object[] finalArgs = args;
                        result = this.shadowFirst(op, collectionName, input, () -> method.invoke(raw, finalArgs));
                        this.interceptor.onMongoOperation(collectionName, op, input, this.toObjectList(result));
                    } else {
                        result = null;
                        try {
                            result = method.invoke(raw, args);
                        }
                        catch (Exception exception) {
                            // empty catch block
                        }
                        if (op != MongoOp.UNKNOWN) {
                            this.interceptor.onMongoOperation(collectionName, op, input, this.toObjectList(result));
                        }
                    }
                    return result;
                });
            }
            catch (Exception e) {
                return raw;
            }
        }

        private Object wrapBulkOperations(Object raw, String collectionName) {
            Class<?>[] ifaces = this.allInterfaces(raw.getClass());
            if (ifaces.length == 0) {
                return raw;
            }
            ArrayList pendingOps = new ArrayList();
            try {
                return Proxy.newProxyInstance(raw.getClass().getClassLoader(), ifaces, (proxy, method, args) -> {
                    String mn = method.getName().toLowerCase();
                    if (mn.equals("execute")) {
                        this.interceptor.onMongoOperation(collectionName, MongoOp.BULK_WRITE, new ArrayList(pendingOps), Collections.emptyList());
                        return MongoWriteMocks.forReturnType(method.getReturnType(), args);
                    }
                    if (mn.equals("insert") || mn.equals("update") || mn.equals("upsert") || mn.equals("remove") || mn.equals("replaceone") || mn.equals("updateone") || mn.equals("updatefirst") || mn.equals("updatemovelti") || mn.equals("remove")) {
                        pendingOps.add(method.getName() + "(" + this.argsToString(args) + ")");
                    }
                    Object result = null;
                    try {
                        result = method.invoke(raw, args);
                    }
                    catch (Exception exception) {
                        // empty catch block
                    }
                    return result == raw ? proxy : result;
                });
            }
            catch (Exception e) {
                return raw;
            }
        }

        private String extractCollection(Object[] args, Class<?> returnType) {
            if (args == null) {
                return "unknown";
            }
            for (Object arg : args) {
                String s;
                if (!(arg instanceof String) || (s = (String)arg).isBlank() || s.length() >= 128 || s.contains(" ")) continue;
                return s;
            }
            for (Object arg : args) {
                Class c;
                if (!(arg instanceof Class) || (c = (Class)arg) == Object.class) continue;
                return c.getSimpleName().toLowerCase();
            }
            return "unknown";
        }

        private Object extractInput(MongoOp op, Object[] args) {
            if (args == null || args.length == 0) {
                return null;
            }
            if (op == MongoOp.AGGREGATE || op == MongoOp.MAP_REDUCE) {
                return this.extractPipeline(args);
            }
            return this.extractFilter(args[0]);
        }

        private Object extractInputFromCollectionArgs(MongoOp op, Object[] args) {
            if (args == null || args.length == 0) {
                return null;
            }
            if (op == MongoOp.AGGREGATE) {
                return this.extractPipeline(args);
            }
            return args[0] != null ? args[0].toString() : null;
        }

        private Object extractFilter(Object first) {
            if (first == null) {
                return null;
            }
            try {
                return first.getClass().getMethod("getQueryObject", new Class[0]).invoke(first, new Object[0]);
            }
            catch (Exception exception) {
                return first.toString();
            }
        }

        private Object extractPipeline(Object[] args) {
            for (Object arg : args) {
                if (arg == null) continue;
                try {
                    Method m2 = arg.getClass().getMethod("toPipeline", CodecRegistry.class);
                    return m2.invoke(arg, MongoClientSettings.getDefaultCodecRegistry());
                }
                catch (Exception e1) {
                    try {
                        return arg.getClass().getMethod("getPipeline", new Class[0]).invoke(arg, new Object[0]);
                    }
                    catch (Exception e2) {
                        String s = arg.toString();
                        if (!s.contains("$match") && !s.contains("$group") && !s.contains("$lookup") && !s.contains("Aggregation") && !s.contains("pipeline")) continue;
                        return s;
                    }
                }
            }
            return null;
        }

        private List<Object> toObjectList(Object result) {
            if (result == null) {
                return Collections.emptyList();
            }
            if (result instanceof List) {
                List list = (List)result;
                return new ArrayList<Object>(list);
            }
            return Collections.singletonList(result);
        }

        private Class<?>[] allInterfaces(Class<?> clazz) {
            LinkedHashSet<Class<?>> ifaces = new LinkedHashSet<Class<?>>();
            for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
                Collections.addAll(ifaces, c.getInterfaces());
            }
            return ifaces.toArray(new Class<?>[0]);
        }

        private String argsToString(Object[] args) {
            if (args == null) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (Object a : args) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(a);
            }
            return sb.toString();
        }
    }
}

