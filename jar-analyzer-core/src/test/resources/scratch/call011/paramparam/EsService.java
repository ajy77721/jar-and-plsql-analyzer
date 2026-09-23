package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

/**
 * NEITHER operand of the concat is a literal in this method — both arrive as parameters and must
 * resolve from the caller for "orders_east" to appear. Single-assignment local first (the shape
 * the parser's CALL-008 CONCAT_PARAM_PARAM capture recognizes).
 */
public class EsService {
    private final ElasticsearchOperations es;
    public EsService(ElasticsearchOperations es) { this.es = es; }

    public void fetch(String prefix, String region) {
        String index = prefix + region;
        es.search(null, null, index);
    }
}
