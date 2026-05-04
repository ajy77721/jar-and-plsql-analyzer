package com.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
    "com.analyzer",
    "com.jaranalyzer",
    "com.waranalyzer",
    "com.plsqlanalyzer.web"
})
public class UnifiedAnalyzerApplication {

    public static void main(String[] args) {
        SpringApplication.run(UnifiedAnalyzerApplication.class, args);
    }
}
