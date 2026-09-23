/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.fetch;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.TestDataSet;
import java.util.List;

public interface DataFetcher
extends AutoCloseable {
    public boolean supports(ExtractedQuery var1);

    public TestDataSet fetch(ExtractedQuery var1, int var2);

    public List<TestDataSet> fetchChain(List<ExtractedQuery> var1, int var2);
}

