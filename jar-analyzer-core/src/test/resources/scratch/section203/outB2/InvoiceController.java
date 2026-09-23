package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** §204 Pattern B: Class<?> threads down the chain; the name derives by naming convention. */
@RestController
public class InvoiceController {
    private final InvoiceService service;
    public InvoiceController(InvoiceService service) { this.service = service; }

    @GetMapping("/invoices")
    public void invoices() {
        service.fetchInvoices(InvoiceEntity.class);
    }
}
