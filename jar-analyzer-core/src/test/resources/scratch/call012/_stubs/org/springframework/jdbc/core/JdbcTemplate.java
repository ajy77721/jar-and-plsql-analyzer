package org.springframework.jdbc.core;

/**
 * Minimal same-FQN stub for spring-jdbc's JdbcTemplate — spring-jdbc is not on this module's
 * test classpath, but the fixtures must compile against the type JpaMethodDetector matches by
 * owner simple name (JDBC_TEMPLATE_OWNERS). Never invoked: the analyzer reads bytecode.
 */
public class JdbcTemplate {
    public Object query(String sql, Object rowMapper) { return null; }
    public int update(String sql) { return 0; }
    public int update(String sql, Object[] args) { return 0; }
    public void execute(String sql) {}
}
