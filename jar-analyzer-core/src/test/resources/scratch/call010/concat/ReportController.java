package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-010 concat-fold: the endpoint supplies the region literal. */
@RestController
public class ReportController {
    private final ReportFacade facade;
    public ReportController(ReportFacade facade) { this.facade = facade; }

    @GetMapping("/reports")
    public void reports() {
        facade.run("east");
    }
}
