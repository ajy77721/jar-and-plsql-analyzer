package com.flowpoc.report;

import com.flowpoc.model.FlowResult;

import java.io.IOException;
import java.util.List;

/**
 * Strategy interface for report output.
 * Implementations: JsonReporter, HtmlReporter (future).
 */
public interface FlowReporter {
    void write(List<FlowResult> results, java.io.OutputStream out) throws IOException;
}
