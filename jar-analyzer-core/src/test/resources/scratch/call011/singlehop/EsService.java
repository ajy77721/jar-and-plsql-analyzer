package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

/** The callee's own body only sees the unresolved PARAMETER; resolution must come from the caller. */
public class EsService {
    private final ElasticsearchOperations es;
    public EsService(ElasticsearchOperations es) { this.es = es; }

    public void searchIndex(String indexName) {
        es.search(null, null, indexName);
    }
}
