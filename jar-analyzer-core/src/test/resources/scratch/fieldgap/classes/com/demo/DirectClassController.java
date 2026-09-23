package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-004 control: Class<?> literal directly at the repo call. */
@RestController
public class DirectClassController {
    private final DirectClassRepo repo;
    public DirectClassController(DirectClassRepo repo) { this.repo = repo; }

    @GetMapping("/invoices-direct-class")
    public String listInvoicesDirectClass() {
        repo.findByEntity(AnnotatedInvoiceEntity.class);
        return "ok";
    }
}
