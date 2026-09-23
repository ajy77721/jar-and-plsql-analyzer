package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-017 direct: Class<?> literal AT the call site; @Document(indexName) ground truth. */
@RestController
public class ProductController {
    private final ElasticsearchOperations es;
    public ProductController(ElasticsearchOperations es) { this.es = es; }

    @GetMapping("/products")
    public void products() {
        es.search(null, Product.class);
    }
}
