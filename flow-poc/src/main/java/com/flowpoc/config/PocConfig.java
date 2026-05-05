package com.flowpoc.config;

/**
 * Immutable configuration for one POC run.
 * Constructed via Builder — no setters (immutability + Builder pattern).
 */
public final class PocConfig {

    private final String  analysisJsonPath;
    private final int     sampleSize;
    private final boolean enableMongo;
    private final String  mongoUri;
    private final String  mongoDatabase;
    private final boolean enableOracle;    // stub — not implemented in v1
    private final String  oracleJdbcUrl;
    private final int     maxEndpoints;    // 0 = all
    private final boolean layer2Enabled;
    private final String  jarFilePath;
    private final String  claudeApiKey;   // Anthropic API key; null = Claude analysis skipped
    private final String  reportOutputDir; // directory for .md report; null = stdout JSON only

    private PocConfig(Builder b) {
        this.analysisJsonPath = b.analysisJsonPath;
        this.sampleSize       = b.sampleSize;
        this.enableMongo      = b.enableMongo;
        this.mongoUri         = b.mongoUri;
        this.mongoDatabase    = b.mongoDatabase;
        this.enableOracle     = b.enableOracle;
        this.oracleJdbcUrl    = b.oracleJdbcUrl;
        this.maxEndpoints     = b.maxEndpoints;
        this.layer2Enabled    = b.layer2Enabled;
        this.jarFilePath      = b.jarFilePath;
        this.claudeApiKey     = b.claudeApiKey;
        this.reportOutputDir  = b.reportOutputDir;
    }

    public String  getAnalysisJsonPath() { return analysisJsonPath; }
    public int     getSampleSize()       { return sampleSize; }
    public boolean isEnableMongo()       { return enableMongo; }
    public String  getMongoUri()         { return mongoUri; }
    public String  getMongoDatabase()    { return mongoDatabase; }
    public boolean isEnableOracle()      { return enableOracle; }
    public String  getOracleJdbcUrl()    { return oracleJdbcUrl; }
    public int     getMaxEndpoints()     { return maxEndpoints; }
    public boolean isLayer2Enabled()     { return layer2Enabled; }
    public String  getJarFilePath()      { return jarFilePath; }
    public String  getClaudeApiKey()     { return claudeApiKey; }
    public String  getReportOutputDir()  { return reportOutputDir; }
    public boolean isClaudeEnabled()     { return claudeApiKey != null && !claudeApiKey.isBlank(); }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String  analysisJsonPath;
        private int     sampleSize    = 10;
        private boolean enableMongo   = true;
        private String  mongoUri      = "mongodb://localhost:27017";
        private String  mongoDatabase;
        private boolean enableOracle  = false;
        private String  oracleJdbcUrl;
        private int     maxEndpoints  = 0;
        private boolean layer2Enabled = false;
        private String  jarFilePath;
        private String  claudeApiKey;
        private String  reportOutputDir;

        public Builder analysisJson(String path)    { this.analysisJsonPath = path; return this; }
        public Builder sampleSize(int n)            { this.sampleSize = n; return this; }
        public Builder mongo(String uri, String db) { this.mongoUri = uri; this.mongoDatabase = db; this.enableMongo = true; return this; }
        public Builder oracle(String jdbcUrl)       { this.oracleJdbcUrl = jdbcUrl; this.enableOracle = true; return this; }
        public Builder maxEndpoints(int n)          { this.maxEndpoints = n; return this; }
        public Builder layer2(String jarFilePath)   { this.layer2Enabled = true; this.jarFilePath = jarFilePath; return this; }
        public Builder claude(String apiKey)        { this.claudeApiKey = apiKey; return this; }
        public Builder reportDir(String dir)        { this.reportOutputDir = dir; return this; }

        public PocConfig build() {
            if (analysisJsonPath == null) throw new IllegalStateException("analysisJsonPath required");
            return new PocConfig(this);
        }
    }
}
