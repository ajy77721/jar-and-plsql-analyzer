package com.plsqlanalyzer.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConfigLoader {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);

    public static PlsqlConfig loadFromFile(Path path) throws IOException {
        try (InputStream is = Files.newInputStream(path)) {
            return YAML_MAPPER.readValue(is, PlsqlConfig.class);
        }
    }

    public static PlsqlConfig loadFromClasspath(String resourcePath) throws IOException {
        try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Config resource not found: " + resourcePath);
            }
            return YAML_MAPPER.readValue(is, PlsqlConfig.class);
        }
    }

    public static PlsqlConfig loadDefault() {
        try {
            return loadFromClasspath("plsql-config.yaml");
        } catch (IOException e) {
            return new PlsqlConfig();
        }
    }

    public static void saveToFile(PlsqlConfig config, Path path) throws IOException {
        Path abs = path.toAbsolutePath();
        if (abs.getParent() != null) {
            Files.createDirectories(abs.getParent());
        }
        YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(abs.toFile(), config);
    }
}
