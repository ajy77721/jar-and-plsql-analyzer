package com.codeanalyzer.ingest.mongo.write;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Per-collection counters, so ingest can be reconciled against the source rather than trusted. */
public final class WriteStats {

    private final Map<String, LongAdder> upserts = new ConcurrentHashMap<>();

    public void record(String collection, int n) {
        upserts.computeIfAbsent(collection, k -> new LongAdder()).add(n);
    }

    public long count(String collection) {
        LongAdder a = upserts.get(collection);
        return a == null ? 0 : a.sum();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> out = new java.util.TreeMap<>();
        upserts.forEach((k, v) -> out.put(k, v.sum()));
        return out;
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }
}
