package demo;

/** Intermediate pass-through hop: both operands thread down untouched. */
public class CustomerFacade {
    private final CustomerService service;
    public CustomerFacade(CustomerService service) { this.service = service; }

    public void run(String sqlPrefix, String table) {
        service.buildReport(sqlPrefix, table);
    }
}
