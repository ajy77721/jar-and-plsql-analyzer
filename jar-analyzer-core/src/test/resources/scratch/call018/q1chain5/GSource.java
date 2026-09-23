package demo;

/** The ONLY place the literal exists — 5 hops from the DB call. */
public class GSource {
    public String resolveCollectionName() {
        return "invoices5";
    }
}
