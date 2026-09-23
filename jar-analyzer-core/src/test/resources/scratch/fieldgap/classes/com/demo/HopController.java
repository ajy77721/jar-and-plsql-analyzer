package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-002: "orders_hop" threads unchanged through 2 wrapper methods into the mongo call. */
@RestController
public class HopController {
    private final HopServiceA a;
    public HopController(HopServiceA a) { this.a = a; }

    @GetMapping("/orders-hop")
    public String listOrdersHop() {
        a.levelOne("orders_hop");
        return "ok";
    }
}
