package demo;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-011 control: the index name is a literal right at the ES call site, IN the endpoint itself. */
@RestController
public class EsController {
    private final ElasticsearchOperations es;
    public EsController(ElasticsearchOperations es) { this.es = es; }

    @GetMapping("/control")
    public void control() {
        es.search(null, null, "catalog_index");
    }
}
