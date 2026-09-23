package demo;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The concat is assigned to a single-assignment local first — the shape the bytecode parser's
 * CALL-003 capture recognizes (single ASTORE from the makeConcatWithConstants indy, then an
 * ALOAD of that slot as the call argument).
 */
public class CustomerReportService {
    private final JdbcTemplate jdbc;
    public CustomerReportService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void buildReport(String tableName) {
        String sql = "SELECT * FROM " + tableName;
        jdbc.query(sql, null);
    }
}
