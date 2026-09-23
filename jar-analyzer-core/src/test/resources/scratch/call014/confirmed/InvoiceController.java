package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InvoiceController {
    private final InvoiceFacade facade;
    public InvoiceController(InvoiceFacade facade) { this.facade = facade; }

    @GetMapping("/invoices")
    public void invoices() {
        facade.findInvoice(Invoice.class);
    }
}
