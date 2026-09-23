package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InvoiceController {
    private final InvoiceQueryFacade facade;
    public InvoiceController(InvoiceQueryFacade facade) { this.facade = facade; }

    @GetMapping("/invoices-derived")
    public void invoices() {
        facade.findInvoice(InvoiceEntity.class);
    }
}
