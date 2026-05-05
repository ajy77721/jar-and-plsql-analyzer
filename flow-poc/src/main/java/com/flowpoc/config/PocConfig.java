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

    private PocConfig(Builder b) {
        this.analysisJsonPath = b.analysisJsonPath;
        this.sampleSize       = b.sampleSize;
        this.enableMongo      = b.enableMongo;
        this.mongoUri         = b.mongoUri;
        this.mongoDatabase    = b.mongoDatabase;
        this.enableOracle     = b.enableOracle;
        this.oracleJdbcUrl    = b.oracleJdbcUrl;
        this.maxEndpoints     = b.maxEndpoints;
    }

    public String  getAnalysisJsonPath() { return analysisJsonPath; }
    public int     getSampleSize()       { return sampleSize; }
    public boolean isEnableMongo()       { return enableMongo; }
    public String  getMongoUri()         { return mongoUri; }
    public String  getMongoDatabase()    { return mongoDatabase; }
    public boolean isEnableOracle()      { return enableOracle; }
    public String  getOracleJdbcUrl()    { return oracleJdbcUrl; }
    public int     getMaxEndpoints()     { return maxEndpoints; }

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

        public Builder analysisJson(String path)    { this.analysisJsonPath = path; return this; }
        public Builder sampleSize(int n)            { this.sampleSize = n; return this; }
        public Builder mongo(String uri, String db) { this.mongoUri = uri; this.mongoDatabase = db; this.enableMongo = true; return this; }
        public Builder oracle(String jdbcUrl)       { this.oracleJdbcUrl = jdbcUrl; this.enableOracle = true; return this; }
        public Builder maxEndpoints(int n)          { this.maxEndpoints = n; return this; }

        public PocConfig build() {
            if (analysisJsonPath == null) throw new IllegalStateException("analysisJsonPath required");
            return new PocConfig(this);
        }
    }
}
