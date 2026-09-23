package demo;

import org.springframework.jdbc.core.JdbcTemplate;

/** The callee's own body only sees the unresolved SQL-string parameter. */
public class CustomerService {
    private final JdbcTemplate jdbc;
    public CustomerService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void loadCustomers(String sql) {
        jdbc.query(sql, null);
    }
}
