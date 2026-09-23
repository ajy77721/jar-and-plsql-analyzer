package com.plsqlanalyzer.web.config;

import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves app.data-dir to absolute paths BEFORE any @Value injection.
 *
 * The whole product keeps ALL mutable runtime state under a single data dir so a server
 * install is one folder to ship/back up.  app.data-dir=data resolves to:
 *   app.data-dir       -> {abs}/data/jar    (for jar-analyzer-core services)
 *   plsql.data-dir     -> {abs}/data/plsql  (for plsql services)
 *   app.data-dir.base  -> {abs}/data        (for ConfigDirService)
 *   app.config-dir     -> {abs}/data/config (connections, prompts — seeded from the jar)
 *   app.parser-config-path -> {abs}/data/config/plsql-config.yaml
 *   LOG_PATH           -> {abs}/data/logs   (consumed by logback-spring.xml)
 *
 * The pristine masters live inside the jar (resources/); the config dir is a working copy
 * seeded on first boot and then owned by the app (CRUD writes land there).
 */
public class DirPropertyResolver implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {

    // Run AFTER application.properties is loaded (HIGHEST_PRECEDENCE+10) but BEFORE Spring Boot's
    // LoggingApplicationListener (HIGHEST_PRECEDENCE+20) initialises logback, so LOG_PATH is set
    // as a system property in time for logback-spring.xml to pick it up.
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 15;
    }

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        ConfigurableEnvironment env = event.getEnvironment();
        Map<String, Object> overrides = new HashMap<>();

        String dataDir = env.getProperty("app.data-dir", "data");
        Path resolvedData = resolveDir(dataDir);

        // Base dir for ConfigDirService
        overrides.put("app.data-dir.base", resolvedData.toString());
        // JAR services expect app.data-dir to be their own root
        overrides.put("app.data-dir", resolvedData.resolve("jar").toString());
        // PL/SQL data dir
        overrides.put("plsql.data-dir", resolvedData.resolve("plsql").toString());

        // Logging dir lives under the data base; logback-spring.xml reads ${LOG_PATH}.
        // Set it as a SYSTEM property (not just the Spring env) because logback resolves
        // ${LOG_PATH} against system properties when it initialises.
        String logPath = resolvedData.resolve("logs").toString();
        overrides.put("LOG_PATH", logPath);
        System.setProperty("LOG_PATH", logPath);
        String logFile = env.getProperty("logging.file.name");
        if (logFile != null && logFile.startsWith("data/")) {
            overrides.put("logging.file.name", resolvedData.resolve(logFile.substring(5)).toString());
        }

        // Config dir is a subfolder of the data base so the entire product state is one tree.
        // (Pristine defaults ship inside the jar and are seeded into here on first boot.)
        Path resolvedConfig = resolvedData.resolve("config");
        overrides.put("app.config-dir", resolvedConfig.toString());
        // Keep the standalone parser config path in sync with the config dir.
        overrides.put("app.parser-config-path", resolvedConfig.resolve("plsql-config.yaml").toString());

        env.getPropertySources().addFirst(new MapPropertySource("resolvedDirs", overrides));
    }

    private Path resolveDir(String dirProp) {
        Path p = Path.of(dirProp).toAbsolutePath().normalize();
        if (Files.isDirectory(p)) return p;
        Path parent = p.getParent();
        if (parent != null) {
            Path up = parent.getParent();
            if (up != null) {
                Path upDir = up.resolve(dirProp);
                if (Files.isDirectory(upDir)) return upDir;
            }
        }
        return p;
    }
}
