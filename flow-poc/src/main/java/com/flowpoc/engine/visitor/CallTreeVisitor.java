package com.flowpoc.engine.visitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowpoc.model.FlowStep;

/**
 * Visitor interface (Visitor pattern) for processing individual call-tree nodes.
 * Separate implementations handle different extraction concerns independently (SRP).
 */
public interface CallTreeVisitor {
    void visit(JsonNode node, FlowStep step);
}
