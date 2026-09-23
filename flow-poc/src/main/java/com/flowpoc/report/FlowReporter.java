/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.report;

import com.flowpoc.model.FlowResult;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface FlowReporter {
    public void write(List<FlowResult> var1, OutputStream var2) throws IOException;
}

