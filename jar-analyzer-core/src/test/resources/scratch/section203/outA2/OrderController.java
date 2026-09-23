package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** §204 Pattern A: region is a genuine runtime value — no literal for it exists ANYWHERE. */
@RestController
public class OrderController {
    private final OrderService service;
    public OrderController(OrderService service) { this.service = service; }

    @GetMapping("/orders/{region}")
    public void fetchOrders(@PathVariable String region) {
        service.fetchOrders("orders_", region);
    }
}
