package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class CustomerController {
    private final RestTemplate rest;
    public CustomerController(RestTemplate rest) { this.rest = rest; }

    @GetMapping("/customers")
    public void customers() {
        loadCustomers("/api/customers");
    }

    /** The callee's own body only sees the unresolved URL parameter. */
    private void loadCustomers(String path) {
        rest.getForObject(path, String.class);
    }
}
