package demo;

/** Intermediate pass-through: both operands thread down untouched. */
public class QueryPlanner {
    private final EsService service;
    public QueryPlanner(EsService service) { this.service = service; }

    public void query(String prefix, String region) {
        service.fetch(prefix, region);
    }
}
