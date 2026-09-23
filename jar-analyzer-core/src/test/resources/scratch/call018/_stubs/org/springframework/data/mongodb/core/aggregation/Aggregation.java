package org.springframework.data.mongodb.core.aggregation;

/**
 * Minimal same-FQN stub for spring-data-mongodb's Aggregation API (§204's precedent). The
 * AggregationDetector matches the $lookup/$match stages by the static factory shapes below.
 */
public class Aggregation {
    public static Aggregation lookup(String from, String localField, String foreignField, String as) {
        return new Aggregation();
    }
    public static Aggregation match(Object criteria) { return new Aggregation(); }
    public static Aggregation newAggregation(Aggregation... stages) { return new Aggregation(); }
}
