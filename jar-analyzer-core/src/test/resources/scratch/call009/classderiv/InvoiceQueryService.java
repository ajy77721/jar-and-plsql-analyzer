package demo;

import jakarta.persistence.EntityManager;

/** Threads the Class<?> down one wrapper — no @Table/@Entity ground truth anywhere. */
public class InvoiceQueryService {
    private final EntityManager em;
    public InvoiceQueryService(EntityManager em) { this.em = em; }

    public void findByEntity(Class<?> entityType) {
        em.find(entityType, null);
    }
}
