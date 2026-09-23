package demo;

/** Hop 1: forwards the Class<?> untouched. */
public class InvoiceService {
    private final InvoiceRepoChild child;
    public InvoiceService(InvoiceRepoChild child) { this.child = child; }

    public void fetchInvoices(Class<?> entityType) {
        child.execute(entityType);
    }
}
