package demo;

/** Pass-through hop. */
public class ReportFacade {
    private final ReportClient client;
    public ReportFacade(ReportClient client) { this.client = client; }

    public void run(String region) {
        client.fetch(region);
    }
}
