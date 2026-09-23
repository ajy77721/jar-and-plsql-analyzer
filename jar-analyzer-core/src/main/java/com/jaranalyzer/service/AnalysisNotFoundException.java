package com.jaranalyzer.service;

public class AnalysisNotFoundException extends RuntimeException {
    public AnalysisNotFoundException(String message) {
        super(message);
    }
}
