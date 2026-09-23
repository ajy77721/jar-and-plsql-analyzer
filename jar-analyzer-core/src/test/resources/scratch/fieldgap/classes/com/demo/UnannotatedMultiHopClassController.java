package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-008: un-annotated InvoiceEntity threads 2 hops; the naming heuristic resolves it. */
@RestController
public class UnannotatedMultiHopClassController {
    private final UnannotatedClassServiceMid mid;
    public UnannotatedMultiHopClassController(UnannotatedClassServiceMid mid) { this.mid = mid; }

    @GetMapping("/invoices-multihop-unannotated")
    public String getInvoicesMultiHopUnannotated() {
        mid.fetch(InvoiceEntity.class);
        return "ok";
    }
}
