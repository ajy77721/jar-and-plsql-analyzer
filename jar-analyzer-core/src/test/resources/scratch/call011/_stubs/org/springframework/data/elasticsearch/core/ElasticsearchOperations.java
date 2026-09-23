package org.springframework.data.elasticsearch.core;

/**
 * Minimal same-FQN stub for spring-data-elasticsearch's ElasticsearchOperations — that library is
 * not on this module's test classpath, but the fixtures must compile against the type the
 * ElasticsearchMethodDetector matches by owner simple name. Only the shapes the scratch apps
 * actually call exist here; everything returns null (never invoked — the analyzer reads bytecode,
 * it does not run the app).
 */
public class ElasticsearchOperations {
    public Object search(Object criteria, Class<?> entity, String index) { return null; }
    public Object get(String index, Class<?> entity, Object id) { return null; }
    public Object count(Object criteria, Class<?> entity, String index) { return null; }
}
