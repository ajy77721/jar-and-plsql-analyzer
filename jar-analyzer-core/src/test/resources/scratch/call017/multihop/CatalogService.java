package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

public class CatalogService {
    private final ElasticsearchOperations es;
    public CatalogService(ElasticsearchOperations es) { this.es = es; }

    public void levelOne(Class<?> entityType) {
        levelTwo(entityType);
    }

    public void levelTwo(Class<?> entityType) {
        es.search(null, entityType);
    }
}
