package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** The ONLY path to the DB access runs through the field-stored method reference. */
@RestController
public class OrderController {
    private final OrderQueryHandler handler;
    public OrderController(OrderQueryHandler handler) { this.handler = handler; }

    @GetMapping("/orders")
    public String listOrders() {
        handler.runQuery();
        return "ok";
    }
}
