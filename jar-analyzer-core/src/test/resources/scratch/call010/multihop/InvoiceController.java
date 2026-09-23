package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class InvoiceController {
    private final RestTemplate rest;
    public InvoiceController(RestTemplate rest) { this.rest = rest; }

    @GetMapping("/invoices")
    public void invoices() {
        hopOne("/api/invoices");
    }

    public void hopOne(String path) {
        hopTwo(path);
    }

    public void hopTwo(String path) {
        hopThree(path);
    }

    public void hopThree(String path) {
        rest.getForObject(path, String.class);
    }
}
