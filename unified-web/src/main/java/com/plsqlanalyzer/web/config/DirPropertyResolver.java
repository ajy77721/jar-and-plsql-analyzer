package com.plsqlanalyzer.web.config;

import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves app.data-dir to absolute paths BEFORE any @Value injection.
 *
 * app.data-dir=data  resolves to:
 *   app.data-dir  -> {abs}/data/jar   (for jar-analyzer-core services)
 *   plsql.data-dir -> {abs}/data/plsql (for plsql services)
 *   app.data-dir.base -> {abs}/data    (for ConfigDirService)
 */
public class DirPropertyResolver implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

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

        // Resolve logging dir relative to data base
        String logFile = env.getProperty("logging.file.name");
        if (logFile != null && logFile.startsWith("data/")) {
            overrides.put("logging.file.name", resolvedData.resolve(logFile.substring(5)).toString());
        }

        // Resolve config dir
        String configDir = env.getProperty("app.config-dir", "config");
        Path resolvedConfig = resolveDir(configDir);
        overrides.put("app.config-dir", resolvedConfig.toString());

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
