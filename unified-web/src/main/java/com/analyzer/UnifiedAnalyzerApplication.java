package com.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {
    "com.analyzer",
    "com.jaranalyzer",
    "com.waranalyzer",
    "com.plsqlanalyzer.web"
})
// Required by LogArchiveService's @Scheduled sweep. Without it the annotation is inert and the
// archiving silently never runs — logs would just accumulate locally with no error anywhere.
@EnableScheduling
public class UnifiedAnalyzerApplication {

    public static void main(String[] args) {
        SpringApplication.run(UnifiedAnalyzerApplication.class, args);
    }
}
