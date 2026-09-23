package demo;

import jakarta.persistence.EntityManager;

public class InvoiceService {
    private final EntityManager em;
    public InvoiceService(EntityManager em) { this.em = em; }

    public void findByEntity(Class<?> entityType) {
        em.find(entityType, null);
    }
}
