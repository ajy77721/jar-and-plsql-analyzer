package demo;

/** Intermediate pass-through hop for the Class<?> value. */
public class InvoiceQueryFacade {
    private final InvoiceQueryService service;
    public InvoiceQueryFacade(InvoiceQueryService service) { this.service = service; }

    public void findInvoice(Class<?> entityType) {
        service.findByEntity(entityType);
    }
}
