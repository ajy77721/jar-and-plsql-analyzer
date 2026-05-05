package com.flowpoc.layer2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CollectingQueryInterceptor implements QueryInterceptor {

    public record CapturedCall(
            String type,
            String collection,
            Object filter,
            String sql,
            List<Object> params,
            List<Map<String, Object>> results
    ) {}

    private final List<CapturedCall> captured = new ArrayList<>();

    @Override
    public void onMongoFind(String collection, Object filter, List<Object> results) {
        captured.add(new CapturedCall("MONGO_FIND", collection, filter, null,
                Collections.emptyList(), results));
    }

    @Override
    public void onMongoInsert(String collection, Object document) {
        captured.add(new CapturedCall("MONGO_INSERT", collection, document, null,
                Collections.emptyList(), Collections.emptyList()));
    }

    @Override
    public void onMongoUpdate(String collection, Object filter, Object update) {
        captured.add(new CapturedCall("MONGO_UPDATE", collection, filter, null,
                Collections.emptyList(), Collections.emptyList()));
    }

    @Override
    public void onSqlQuery(String sql, List<Object> params, List<Map<String, Object>> results) {
        captured.add(new CapturedCall("SQL", null, null, sql, params, results));
    }

    public List<CapturedCall> getCaptured() {
        return Collections.unmodifiableList(captured);
    }

    public void clear() {
        captured.clear();
    }
}
