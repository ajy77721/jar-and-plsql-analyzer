package demo;

/** The ONLY place the literal exists — returned, never passed as a parameter. */
public class CBuilder {
    public String resolveCollectionName() {
        return "invoices";
    }
}
