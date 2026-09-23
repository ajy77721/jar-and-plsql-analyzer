package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

/**
 * The literal operand ("orders_") lives in the SAME method as the ES call; the other operand is a
 * parameter resolvable from the caller — the fold must happen once the param side resolves.
 * The concat is assigned to a single-assignment local first: that is the shape the bytecode
 * parser's CALL-003 capture recognizes (single ASTORE from the makeConcatWithConstants indy,
 * then an ALOAD of that slot as the call argument).
 */
public class EsService {
    private final ElasticsearchOperations es;
    public EsService(ElasticsearchOperations es) { this.es = es; }

    public void searchRegion(String region) {
        String index = "orders_" + region;
        es.search(null, null, index);
    }
}
