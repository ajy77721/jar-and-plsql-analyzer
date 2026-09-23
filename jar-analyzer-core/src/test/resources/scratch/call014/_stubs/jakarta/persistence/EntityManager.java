package jakarta.persistence;

/**
 * Minimal same-FQN stub for jakarta.persistence.EntityManager — persistence API is not on this
 * module's test classpath. EM_ENTITY_METHODS/EM_QUERY_METHODS capture by owner simple name.
 */
public class EntityManager {
    public Object find(Class<?> entityClass, Object id) { return null; }
    public Object createNativeQuery(String sql, Class<?> resultClass) { return null; }
    public Object createQuery(String jpql, Class<?> resultClass) { return null; }
}
