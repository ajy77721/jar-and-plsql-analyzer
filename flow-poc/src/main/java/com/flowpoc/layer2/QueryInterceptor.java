/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.layer2;

import com.flowpoc.layer2.MongoOp;
import java.util.List;
import java.util.Map;

public interface QueryInterceptor {
    public void onMongoOperation(String var1, MongoOp var2, Object var3, List<Object> var4);

    public void onSqlQuery(String var1, List<Object> var2, List<Map<String, Object>> var3);
}

