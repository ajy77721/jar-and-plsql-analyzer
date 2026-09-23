package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/**
 * CALL-015: `this.baseUrl + path` where baseUrl is a SINGLE-LITERAL-assigned instance field
 * (assigned once, directly, never reassigned) and the path literal comes from the caller —
 * so the concat resolves via CALL_SITE_CONCAT_PROPAGATION, the URL's host becomes the module hint.
 */
@RestController
public class OrderClient {
    private final String baseUrl = "http://order-svc";
    private final RestTemplate rest;

    public OrderClient(RestTemplate rest) { this.rest = rest; }

    @GetMapping("/orders")
    public void orders() {
        fetchOrders("/api/orders");
    }

    private void fetchOrders(String path) {
        String url = baseUrl + path;
        rest.getForObject(url, String.class);
    }
}
