package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-008: BOTH operands terminate in real literals up the chain — full fold to orders_east. */
@RestController
public class TwoLiteralConcatController {
    private final TwoParamConcatService service;
    public TwoLiteralConcatController(TwoParamConcatService service) { this.service = service; }

    @GetMapping("/orders-twoliteral")
    public String listOrdersTwoLiteral() {
        service.buildQuery("orders_", "east");
        return "ok";
    }
}
