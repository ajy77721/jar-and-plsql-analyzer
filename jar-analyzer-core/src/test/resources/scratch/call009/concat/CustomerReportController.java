package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CustomerReportController {
    private final CustomerReportFacade facade;
    public CustomerReportController(CustomerReportFacade facade) { this.facade = facade; }

    @GetMapping("/customer-report")
    public void report() {
        facade.runReport("customers");
    }
}
