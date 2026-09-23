package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

/** Wrapper one: passes the parameter through untouched to wrapper two. */
public class EsService {
    private final ElasticsearchOperations es;
    public EsService(ElasticsearchOperations es) { this.es = es; }

    public void wrapperOne(String indexName) {
        wrapperTwo(indexName);
    }

    public void wrapperTwo(String indexName) {
        this.searchIndex(indexName);
    }

    public void searchIndex(String indexName) {
        es.search(null, null, indexName);
    }
}
