package com.plsqlanalyzer.web.parser.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The Cache Manager's datasource filter derives its options from the dialects the server reports.
 * {@link AnalysisService#dialectOf} is what feeds that report; before this test it omitted
 * PostgreSQL, so a Postgres connection was reported {@code UNKNOWN}, dropped from the derived
 * filter, and the dropdown offered four of the five dialects.
 *
 * <p>Pinned against the exact JDBC URL shapes each driver produces. Kept in lock-step with the two
 * other copies of this scan ({@code HistoryFacet.dbType}, {@code AdminController.dialectOf}).
 */
class AnalysisServiceDialectTest {

    @Test
    @DisplayName("every dialect the app supports is recognised from its JDBC URL")
    void recognisesEveryDialect() {
        assertEquals("ORACLE",    AnalysisService.dialectOf("jdbc:oracle:thin:@//localhost:1521/FREEPDB1"));
        assertEquals("SQLSERVER", AnalysisService.dialectOf("jdbc:sqlserver://localhost:1433;databaseName=x"));
        assertEquals("MYSQL",     AnalysisService.dialectOf("jdbc:mysql://localhost:3307/anl"));
        assertEquals("DB2",       AnalysisService.dialectOf("jdbc:db2://localhost:50000/anl"));
        // The regression: PostgreSQL once fell through to UNKNOWN here.
        assertEquals("POSTGRES",  AnalysisService.dialectOf("jdbc:postgresql://localhost:5432/anl"));
    }

    @Test
    @DisplayName("an unknown or absent URL stays UNKNOWN rather than guessing a dialect")
    void unknownStaysUnknown() {
        assertEquals("UNKNOWN", AnalysisService.dialectOf("jdbc:h2:mem:test"));
        assertEquals("UNKNOWN", AnalysisService.dialectOf(null));
        assertEquals("UNKNOWN", AnalysisService.dialectOf(""));
    }
}
