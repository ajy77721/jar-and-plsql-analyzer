package demo;

import org.springframework.jdbc.core.JdbcTemplate;

/** NEITHER operand of the concat is a same-method literal — both resolve from the caller. */
public class CustomerService {
    private final JdbcTemplate jdbc;
    public CustomerService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void buildReport(String sqlPrefix, String table) {
        String sql = sqlPrefix + table;
        jdbc.query(sql, null);
    }
}
