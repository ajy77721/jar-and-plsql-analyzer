package com.flowpoc.layer2;

import java.util.List;
import java.util.Map;

public interface QueryInterceptor {
    void onMongoFind(String collection, Object filter, List<Object> results);
    void onMongoAggregate(String collection, List<Object> pipeline, List<Object> results);
    void onMongoInsert(String collection, Object document);
    void onMongoUpdate(String collection, Object filter, Object update);
    void onSqlQuery(String sql, List<Object> params, List<Map<String, Object>> results);
}
