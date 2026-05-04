package com.plsqlanalyzer.web.config;

import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.ConfigLoader;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.service.AnalysisJobService;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import com.plsqlanalyzer.web.service.ClaudeSessionManager;
import com.plsqlanalyzer.web.service.ClaudeVerificationService;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.service.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    @Value("${plsql.threads.source-fetch:8}")
    private int sourceFetchThreads;

    @Value("${plsql.threads.trigger-resolve:4}")
    private int triggerResolveThreads;

    @Value("${plsql.threads.metadata:4}")
    private int metadataThreads;

    @Value("${plsql.threads.claude-parallel:3}")
    private int claudeParallelChunks;

    @Value("${claude.bedrock.enabled:false}")
    private boolean bedrockEnabled;

    @Value("${claude.bedrock.profile:ClaudeCode}")
    private String bedrockProfile;

    @Value("${claude.bedrock.region:eu-central-1}")
    private String bedrockRegion;

    @Bean
    public PlsqlConfig plsqlConfig(ConfigDirService dirs) {
        Path configFile = dirs.resolve("plsql-config.yaml");
        if (Files.exists(configFile)) {
            try {
                PlsqlConfig cfg = ConfigLoader.loadFromFile(configFile);
                log.info("Loaded PL/SQL config from {}", configFile);
                return cfg;
            } catch (Exception e) {
                log.warn("Failed to load config from {}: {}", configFile, e.getMessage());
            }
        }
        log.info("Using default PL/SQL config (classpath)");
        return ConfigLoader.loadDefault();
    }

    @Bean("plsqlConfigFilePath")
    public Path plsqlConfigFilePath(ConfigDirService dirs) {
        return dirs.resolve("plsql-config.yaml");
    }

    @Bean
    public AnalysisService analysisService() {
        AnalysisService svc = new AnalysisService();
        svc.setSourceFetchThreads(sourceFetchThreads);
        svc.setTriggerThreads(triggerResolveThreads);
        svc.setMetadataThreads(metadataThreads);
        return svc;
    }

    @Bean("plsqlPersistenceService")
    public PersistenceService persistenceService(ConfigDirService dirs) {
        return new PersistenceService(dirs.getPlsqlDataDir().toString());
    }

    @Bean
    public AnalysisJobService analysisJobService() {
        return new AnalysisJobService();
    }

    @Bean
    public DbSourceFetcher dbSourceFetcher(PlsqlConfig config) {
        String jdbcUrl = config.getJdbcUrl();
        if (jdbcUrl == null) {
            jdbcUrl = "jdbc:oracle:thin:@//localhost:1521/ORCL";
        }
        return new DbSourceFetcher(jdbcUrl);
    }

    @Bean("plsqlClaudeProcessRunner")
    public ClaudeProcessRunner claudeProcessRunner() {
        ClaudeProcessRunner runner = new ClaudeProcessRunner();
        if (bedrockEnabled) {
            runner.setEnv("CLAUDE_CODE_USE_BEDROCK", "1");
            runner.setEnv("AWS_PROFILE", bedrockProfile);
            runner.setEnv("AWS_REGION", bedrockRegion);
            log.info("Claude: using AWS Bedrock profile={} region={}", bedrockProfile, bedrockRegion);
        } else {
            log.info("Claude: using local CLI authentication (claude login)");
        }
        return runner;
    }

    @Bean("plsqlClaudeSessionManager")
    public ClaudeSessionManager claudeSessionManager() {
        return new ClaudeSessionManager();
    }

    @Bean
    public ClaudeVerificationService claudeVerificationService(
            @org.springframework.beans.factory.annotation.Qualifier("plsqlClaudeProcessRunner") ClaudeProcessRunner processRunner,
            ConfigDirService dirs) {
        ClaudeVerificationService svc = new ClaudeVerificationService(processRunner, dirs.getPlsqlDataDir().toString());
        svc.setParallelChunks(claudeParallelChunks);
        return svc;
    }
}
