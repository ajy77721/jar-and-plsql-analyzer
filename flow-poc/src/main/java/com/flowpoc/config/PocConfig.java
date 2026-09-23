/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.analyzer.storage.ArtifactKey
 *  com.analyzer.storage.ArtifactStore
 */
package com.flowpoc.config;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.flowpoc.ClaudeInvoker;
import com.flowpoc.ProgressCallback;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class PocConfig {
    private final ArtifactStore store;
    private final ArtifactKey analysisKey;
    private final int sampleSize;
    private final boolean enableMongo;
    private final String mongoUri;
    private final String mongoDatabase;
    private final boolean enableOracle;
    private final String oracleJdbcUrl;
    private final int maxEndpoints;
    private final boolean layer2Enabled;
    private final ArtifactKey jarKey;
    private final String claudeApiKey;
    private final ClaudeInvoker claudeInvoker;
    private final ArtifactKey reportKey;
    private final ProgressCallback progressCallback;
    private final Set<String> selectedEndpoints;

    private PocConfig(Builder b) {
        this.store = b.store;
        this.analysisKey = b.analysisKey;
        this.sampleSize = b.sampleSize;
        this.enableMongo = b.enableMongo;
        this.mongoUri = b.mongoUri;
        this.mongoDatabase = b.mongoDatabase;
        this.enableOracle = b.enableOracle;
        this.oracleJdbcUrl = b.oracleJdbcUrl;
        this.maxEndpoints = b.maxEndpoints;
        this.layer2Enabled = b.layer2Enabled;
        this.jarKey = b.jarKey;
        this.claudeApiKey = b.claudeApiKey;
        this.claudeInvoker = b.claudeInvoker;
        this.reportKey = b.reportKey;
        this.progressCallback = b.progressCallback;
        this.selectedEndpoints = b.selectedEndpoints != null ? Collections.unmodifiableSet(b.selectedEndpoints) : Collections.emptySet();
    }

    public ArtifactStore getStore() {
        return this.store;
    }

    public ArtifactKey getAnalysisKey() {
        return this.analysisKey;
    }

    public int getSampleSize() {
        return this.sampleSize;
    }

    public boolean isEnableMongo() {
        return this.enableMongo;
    }

    public String getMongoUri() {
        return this.mongoUri;
    }

    public String getMongoDatabase() {
        return this.mongoDatabase;
    }

    public boolean isEnableOracle() {
        return this.enableOracle;
    }

    public String getOracleJdbcUrl() {
        return this.oracleJdbcUrl;
    }

    public int getMaxEndpoints() {
        return this.maxEndpoints;
    }

    public boolean isLayer2Enabled() {
        return this.layer2Enabled;
    }

    public ArtifactKey getJarKey() {
        return this.jarKey;
    }

    public String getClaudeApiKey() {
        return this.claudeApiKey;
    }

    public ClaudeInvoker getClaudeInvoker() {
        return this.claudeInvoker;
    }

    public ArtifactKey getReportKey() {
        return this.reportKey;
    }

    public boolean isClaudeEnabled() {
        return this.claudeInvoker != null || this.claudeApiKey != null && !this.claudeApiKey.isBlank();
    }

    public ProgressCallback getProgressCallback() {
        return this.progressCallback;
    }

    public Set<String> getSelectedEndpoints() {
        return this.selectedEndpoints;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ArtifactStore store;
        private ArtifactKey analysisKey;
        private int sampleSize = 10;
        private boolean enableMongo = true;
        private String mongoUri = "mongodb://localhost:27017";
        private String mongoDatabase;
        private boolean enableOracle = false;
        private String oracleJdbcUrl;
        private int maxEndpoints = 0;
        private boolean layer2Enabled = false;
        private ArtifactKey jarKey;
        private String claudeApiKey;
        private ClaudeInvoker claudeInvoker;
        private ArtifactKey reportKey;
        private ProgressCallback progressCallback;
        private Set<String> selectedEndpoints;

        public Builder analysis(ArtifactStore store, ArtifactKey key) {
            this.store = store;
            this.analysisKey = key;
            return this;
        }

        public Builder sampleSize(int n) {
            this.sampleSize = n;
            return this;
        }

        public Builder mongo(String uri, String db) {
            this.mongoUri = uri;
            this.mongoDatabase = db;
            this.enableMongo = true;
            return this;
        }

        public Builder oracle(String jdbcUrl) {
            this.oracleJdbcUrl = jdbcUrl;
            this.enableOracle = true;
            return this;
        }

        public Builder maxEndpoints(int n) {
            this.maxEndpoints = n;
            return this;
        }

        public Builder layer2(ArtifactKey jarKey) {
            this.layer2Enabled = true;
            this.jarKey = jarKey;
            return this;
        }

        public Builder claude(String apiKey) {
            this.claudeApiKey = apiKey;
            return this;
        }

        public Builder claudeInvoker(ClaudeInvoker inv) {
            this.claudeInvoker = inv;
            return this;
        }

        public Builder reportKey(ArtifactKey key) {
            this.reportKey = key;
            return this;
        }

        public Builder progressCallback(ProgressCallback cb) {
            this.progressCallback = cb;
            return this;
        }

        public Builder selectedEndpoints(Collection<String> paths) {
            this.selectedEndpoints = new HashSet<String>(paths);
            return this;
        }

        public PocConfig build() {
            if (this.store == null || this.analysisKey == null) {
                throw new IllegalStateException("analysis(store, key) is required");
            }
            return new PocConfig(this);
        }
    }
}

