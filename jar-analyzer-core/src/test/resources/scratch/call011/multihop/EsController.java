package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-011 multi-hop: the literal is threaded UNCHANGED through 2 wrapper methods. */
@RestController
public class EsController {
    private final EsService service;
    public EsController(EsService service) { this.service = service; }

    @GetMapping("/multihop")
    public void multihop() {
        service.wrapperOne("invoices_index");
    }
}
