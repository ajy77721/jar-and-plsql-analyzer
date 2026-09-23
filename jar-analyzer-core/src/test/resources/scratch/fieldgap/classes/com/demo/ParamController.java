package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-001: the collection name is a literal passed as a caller argument. */
@RestController
public class ParamController {
    private final ParamRepositoryChild child;
    public ParamController(ParamRepositoryChild child) { this.child = child; }

    @GetMapping("/orders-param")
    public String listOrdersParam() {
        child.execute("orders_param");
        return "ok";
    }
}
