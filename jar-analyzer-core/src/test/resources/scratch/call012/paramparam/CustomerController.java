package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-012: both SQL-concat operands arrive from the endpoint's own literals. */
@RestController
public class CustomerController {
    private final CustomerFacade facade;
    public CustomerController(CustomerFacade facade) { this.facade = facade; }

    @GetMapping("/customers")
    public void customers() {
        facade.run("SELECT * FROM ", "customers");
    }
}
