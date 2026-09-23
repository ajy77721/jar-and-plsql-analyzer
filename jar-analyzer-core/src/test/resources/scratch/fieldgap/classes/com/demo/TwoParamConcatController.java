package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** CALL-006: prefix is a literal; region is a genuinely runtime @PathVariable value. */
@RestController
public class TwoParamConcatController {
    private final TwoParamConcatService service;
    public TwoParamConcatController(TwoParamConcatService service) { this.service = service; }

    @GetMapping("/orders-twoparam/{region}")
    public String listOrdersTwoParam(@PathVariable String region) {
        service.buildQuery("orders_", region);
        return "ok";
    }
}
