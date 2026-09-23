package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InvoiceController {
    private final InvoiceService service;
    public InvoiceController(InvoiceService service) { this.service = service; }

    @GetMapping("/invoices")
    public void invoices() {
        service.levelOne("SELECT * FROM invoices");
    }
}
