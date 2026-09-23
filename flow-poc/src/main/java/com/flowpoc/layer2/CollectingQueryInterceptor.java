/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  org.bson.Document
 */
package com.flowpoc.layer2;

import com.flowpoc.layer2.MongoOp;
import com.flowpoc.layer2.QueryInterceptor;
import com.flowpoc.layer2.ShadowMongoStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.bson.Document;

public class CollectingQueryInterceptor
implements QueryInterceptor {
    private final List<CapturedCall> captured = new ArrayList<CapturedCall>();
    private final ShadowMongoStore shadowStore = new ShadowMongoStore();

    @Override
    public void onMongoOperation(String collection, MongoOp op, Object input, List<Object> results) {
        this.captured.add(new CapturedCall(collection, op, input, null, Collections.emptyList(), results));
    }

    public void onMongoWrite(String collection, MongoOp op, Object input, ShadowMongoStore.WriteImpact impact) {
        this.captured.add(new CapturedCall(collection, op, input, null, Collections.emptyList(), Collections.emptyList(), impact.matchedCount(), impact.snapshotBefore()));
    }

    public ShadowMongoStore getShadowStore() {
        return this.shadowStore;
    }

    @Override
    public void onSqlQuery(String sql, List<Object> params, List<Map<String, Object>> results) {
        this.captured.add(new CapturedCall(null, MongoOp.UNKNOWN, null, sql, params, (List<Object>)(List<?>)results));
    }

    public List<CapturedCall> getCaptured() {
        return Collections.unmodifiableList(this.captured);
    }

    public void clear() {
        this.captured.clear();
    }

    public void clearAll() {
        this.captured.clear();
        this.shadowStore.clear();
    }

    public record CapturedCall(String collection, MongoOp mongoOp, Object input, String sql, List<Object> params, List<Object> results, long impactCount, List<Document> snapshotBefore) {
        public CapturedCall(String collection, MongoOp mongoOp, Object input, String sql, List<Object> params, List<Object> results) {
            this(collection, mongoOp, input, sql, params, results, 0L, Collections.emptyList());
        }

        public boolean isMongo() {
            return this.mongoOp != MongoOp.UNKNOWN;
        }

        public boolean isSql() {
            return this.sql != null;
        }

        public boolean isWrite() {
            return this.mongoOp != null && this.mongoOp.isMutation();
        }

        public boolean hasImpact() {
            return this.impactCount > 0L;
        }
    }
}

