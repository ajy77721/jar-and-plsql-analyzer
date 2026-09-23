package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/**
 * CALL-015 negative: baseUrl comes from a CONSTRUCTOR PARAMETER (the @Value-injected shape) —
 * genuinely unresolvable at compile time; must stay unresolved, not guessed.
 */
@RestController
public class ShippingClient {
    private final String baseUrl;
    private final RestTemplate rest;

    public ShippingClient(String baseUrl, RestTemplate rest) {
        this.baseUrl = baseUrl;
        this.rest = rest;
    }

    @GetMapping("/ship")
    public void ship() {
        fetchQuote(rest);
    }

    private void fetchQuote(RestTemplate rest) {
        String path = baseUrl + "/api/quotes";
        rest.getForObject(path, String.class);
    }
}
