package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-004: Class<?> threads through 2 hops before the mongo call. */
@RestController
public class MultiHopClassController {
    private final ClassServiceMid mid;
    public MultiHopClassController(ClassServiceMid mid) { this.mid = mid; }

    @GetMapping("/invoices-multihop-class")
    public String getInvoicesMultiHop() {
        mid.fetch(AnnotatedInvoiceEntity.class);
        return "ok";
    }
}
