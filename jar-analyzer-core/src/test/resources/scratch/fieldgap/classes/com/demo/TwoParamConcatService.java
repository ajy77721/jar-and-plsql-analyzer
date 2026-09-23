package com.demo;

/** Hop 1: forwards both values untouched. */
public class TwoParamConcatService {
    private final TwoParamConcatAppender appender;
    public TwoParamConcatService(TwoParamConcatAppender appender) { this.appender = appender; }

    public void buildQuery(String prefix, String region) {
        appender.append(prefix, region);
    }
}
