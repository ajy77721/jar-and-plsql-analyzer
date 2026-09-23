package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-011 single-hop: the caller supplies the index name as a literal argument. */
@RestController
public class EsController {
    private final EsService service;
    public EsController(EsService service) { this.service = service; }

    @GetMapping("/singlehop")
    public void singlehop() {
        service.searchIndex("customers_index");
    }
}
