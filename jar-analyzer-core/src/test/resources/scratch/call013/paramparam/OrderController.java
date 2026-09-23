package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-013: both URL-concat operands arrive from the endpoint's own literals. */
@RestController
public class OrderController {
    private final OrderClient client;
    public OrderController(OrderClient client) { this.client = client; }

    @GetMapping("/orders")
    public void orders() {
        client.fetch("/api/orders/", "east");
    }
}
