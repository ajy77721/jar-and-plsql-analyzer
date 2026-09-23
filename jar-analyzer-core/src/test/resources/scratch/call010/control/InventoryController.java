package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/** CALL-010 control: the literal URL sits right at the RestTemplate call site, in the endpoint. */
@RestController
public class InventoryController {
    private final RestTemplate rest;
    public InventoryController(RestTemplate rest) { this.rest = rest; }

    @GetMapping("/fetch")
    public void fetch() {
        rest.getForObject("http://inventory-service/api/orders", String.class);
    }
}
