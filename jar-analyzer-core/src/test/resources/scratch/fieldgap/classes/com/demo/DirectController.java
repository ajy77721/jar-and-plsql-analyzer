package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** §203 control: reaches OrderRepo.findAll() DIRECTLY — no field-stored indirection. */
@RestController
public class DirectController {
    private final OrderRepo repo;
    public DirectController(OrderRepo repo) { this.repo = repo; }

    @GetMapping("/orders-direct")
    public String listOrdersDirect() {
        return repo.findAll().toString();
    }
}
