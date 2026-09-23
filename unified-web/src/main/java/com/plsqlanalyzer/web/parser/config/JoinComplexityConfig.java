package com.plsqlanalyzer.web.parser.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("parserJoinComplexityConfig")
@ConfigurationProperties(prefix = "join-complexity")
public class JoinComplexityConfig {

    private double baseScore = 1.0;
    private double outerJoinPenalty = 0.5;
    private double crossFullJoinPenalty = 1.0;
    private double multiPredBonus = 0.5;
    private double highPredBonus = 1.0;
    private double noPredPenalty = 0.5;
    private int multiPredThreshold = 2;
    private int highPredThreshold = 3;
    private double thresholdMedium = 2.0;
    private double thresholdHigh = 3.0;

    public double getBaseScore() { return baseScore; }
    public void setBaseScore(double baseScore) { this.baseScore = baseScore; }

    public double getOuterJoinPenalty() { return outerJoinPenalty; }
    public void setOuterJoinPenalty(double outerJoinPenalty) { this.outerJoinPenalty = outerJoinPenalty; }

    public double getCrossFullJoinPenalty() { return crossFullJoinPenalty; }
    public void setCrossFullJoinPenalty(double crossFullJoinPenalty) { this.crossFullJoinPenalty = crossFullJoinPenalty; }

    public double getMultiPredBonus() { return multiPredBonus; }
    public void setMultiPredBonus(double multiPredBonus) { this.multiPredBonus = multiPredBonus; }

    public double getHighPredBonus() { return highPredBonus; }
    public void setHighPredBonus(double highPredBonus) { this.highPredBonus = highPredBonus; }

    public double getNoPredPenalty() { return noPredPenalty; }
    public void setNoPredPenalty(double noPredPenalty) { this.noPredPenalty = noPredPenalty; }

    public int getMultiPredThreshold() { return multiPredThreshold; }
    public void setMultiPredThreshold(int multiPredThreshold) { this.multiPredThreshold = multiPredThreshold; }

    public int getHighPredThreshold() { return highPredThreshold; }
    public void setHighPredThreshold(int highPredThreshold) { this.highPredThreshold = highPredThreshold; }

    public double getThresholdMedium() { return thresholdMedium; }
    public void setThresholdMedium(double thresholdMedium) { this.thresholdMedium = thresholdMedium; }

    public double getThresholdHigh() { return thresholdHigh; }
    public void setThresholdHigh(double thresholdHigh) { this.thresholdHigh = thresholdHigh; }
}
