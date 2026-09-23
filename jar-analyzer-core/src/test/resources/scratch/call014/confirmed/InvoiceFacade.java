package demo;

/** Intermediate pass-through hop for the Class<?> value. */
public class InvoiceFacade {
    private final InvoiceService service;
    public InvoiceFacade(InvoiceService service) { this.service = service; }

    public void findInvoice(Class<?> entityType) {
        service.findByEntity(entityType);
    }
}
