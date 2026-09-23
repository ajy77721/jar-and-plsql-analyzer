/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 */
package com.flowpoc.engine.visitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowpoc.model.FlowStep;

public interface CallTreeVisitor {
    public void visit(JsonNode var1, FlowStep var2);
}

