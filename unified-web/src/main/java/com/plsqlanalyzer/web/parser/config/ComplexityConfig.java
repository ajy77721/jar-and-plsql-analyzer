package com.plsqlanalyzer.web.parser.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("parserComplexityConfig")
@ConfigurationProperties(prefix = "complexity")
public class ComplexityConfig {

    private double loc = 0.3;
    private double tables = 15;
    private double callsOut = 10;
    private double cursors = 8;
    private double exceptionHandlers = 5;
    private double totalStatements = 0.1;
    private double dynamicSql = 20;
    private double depth = 5;

    private int thresholdMedium = 50;
    private int thresholdHigh = 150;

    public double getLoc() { return loc; }
    public void setLoc(double loc) { this.loc = loc; }

    public double getTables() { return tables; }
    public void setTables(double tables) { this.tables = tables; }

    public double getCallsOut() { return callsOut; }
    public void setCallsOut(double callsOut) { this.callsOut = callsOut; }

    public double getCursors() { return cursors; }
    public void setCursors(double cursors) { this.cursors = cursors; }

    public double getExceptionHandlers() { return exceptionHandlers; }
    public void setExceptionHandlers(double exceptionHandlers) { this.exceptionHandlers = exceptionHandlers; }

    public double getTotalStatements() { return totalStatements; }
    public void setTotalStatements(double totalStatements) { this.totalStatements = totalStatements; }

    public double getDynamicSql() { return dynamicSql; }
    public void setDynamicSql(double dynamicSql) { this.dynamicSql = dynamicSql; }

    public double getDepth() { return depth; }
    public void setDepth(double depth) { this.depth = depth; }

    public int getThresholdMedium() { return thresholdMedium; }
    public void setThresholdMedium(int thresholdMedium) { this.thresholdMedium = thresholdMedium; }

    public int getThresholdHigh() { return thresholdHigh; }
    public void setThresholdHigh(int thresholdHigh) { this.thresholdHigh = thresholdHigh; }
}
