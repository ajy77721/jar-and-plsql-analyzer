package demo;

import org.springframework.jdbc.core.JdbcTemplate;

public class InvoiceService {
    private final JdbcTemplate jdbc;
    public InvoiceService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void levelOne(String sql) {
        levelTwo(sql);
    }

    public void levelTwo(String sql) {
        levelThree(sql);
    }

    public void levelThree(String sql) {
        jdbc.query(sql, null);
    }
}
