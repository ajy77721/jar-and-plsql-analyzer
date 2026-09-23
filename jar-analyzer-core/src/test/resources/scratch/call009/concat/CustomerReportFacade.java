package demo;

/** Intermediate pass-through hop (the caller whose literal feeds the concat one level down). */
public class CustomerReportFacade {
    private final CustomerReportService service;
    public CustomerReportFacade(CustomerReportService service) { this.service = service; }

    public void runReport(String tableName) {
        service.buildReport(tableName);
    }
}
