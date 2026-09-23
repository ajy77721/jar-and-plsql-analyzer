package demo;

import org.springframework.jdbc.core.JdbcTemplate;

public class OrderService {
    private final JdbcTemplate jdbc;
    public OrderService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void loadOrders() {
        jdbc.query("SELECT * FROM orders", null);
    }
}
