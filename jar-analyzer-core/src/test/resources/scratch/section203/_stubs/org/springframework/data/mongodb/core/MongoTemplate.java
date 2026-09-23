package org.springframework.data.mongodb.core;

/**
 * Minimal same-FQN stub for spring-data-mongodb's MongoTemplate (§204's own precedent — same FQNs,
 * not on this module's test classpath). MongoMethodDetector matches by owner simple name; the
 * collection-name string is the third arg of find(Query, Class, String) / second of aggregate().
 */
public class MongoTemplate {
    public Object find(Object query, Class<?> entity, String collectionName) { return null; }
    public Object find(Object query, Class<?> entity) { return null; }
    public Object aggregate(Object aggregation, String collectionName, Class<?> viewType) { return null; }
    public Object aggregate(Object aggregation, Class<?> entity, Class<?> viewType) { return null; }
}
