package com.flowpoc.fetch;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.TestDataSet;

import java.util.List;

/**
 * Strategy interface (ISP + DIP) for fetching representative records from a live DB
 * using the predicates extracted from a query.
 *
 * Concrete implementations: MongoDataFetcher, OracleDataFetcher (stub).
 * The flow engine depends on this abstraction, not on any specific driver.
 */
public interface DataFetcher extends AutoCloseable {
    boolean supports(ExtractedQuery query);
    TestDataSet fetch(ExtractedQuery query, int sampleSize);
    List<TestDataSet> fetchChain(List<ExtractedQuery> orderedQueries, int sampleSize);
}
