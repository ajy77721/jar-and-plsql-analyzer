package com.flowpoc;

import com.flowpoc.analyzer.AnalyzerPipeline;
import com.flowpoc.config.PocConfig;
import com.flowpoc.engine.FlowWalker;
import com.flowpoc.engine.SqlPredicateExtractor;
import com.flowpoc.engine.visitor.CallTreeVisitor;
import com.flowpoc.engine.visitor.MongoOperationVisitor;
import com.flowpoc.engine.visitor.SqlOperationVisitor;
import com.flowpoc.fetch.DataFetcher;
import com.flowpoc.fetch.MongoDataFetcher;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.TestDataSet;
import com.flowpoc.report.FlowReporter;
import com.flowpoc.report.JsonReporter;
import com.mongodb.client.MongoClients;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Main orchestrator (Facade pattern): wires engine + analyzers + fetcher + reporter.
 *
 * Typical usage:
 *
 *   PocConfig config = PocConfig.builder()
 *       .analysisJson("/data/analysis.json")
 *       .mongo("mongodb://localhost:27017", "mydb")
 *       .sampleSize(10)
 *       .build();
 *
 *   new PocRunner(config).run(System.out);
 */
public class PocRunner {

    private final PocConfig config;

    public PocRunner(PocConfig config) {
        this.config = config;
    }

    public void run(OutputStream out) throws Exception {
        // 1. Build visitor list based on config
        List<CallTreeVisitor> visitors = new ArrayList<>();
        visitors.add(new MongoOperationVisitor());
        visitors.add(new SqlOperationVisitor(new SqlPredicateExtractor()));

        // 2. Walk all endpoint call trees
        FlowWalker walker = new FlowWalker(visitors);
        List<FlowResult> results = walker.walk(new File(config.getAnalysisJsonPath()));

        // Limit if configured
        if (config.getMaxEndpoints() > 0 && results.size() > config.getMaxEndpoints()) {
            results = results.subList(0, config.getMaxEndpoints());
        }

        // 3. Run optimization analyzers on each flow
        AnalyzerPipeline pipeline = AnalyzerPipeline.builder()
                .withMongoDefaults()
                .build();

        for (FlowResult r : results) {
            pipeline.runAndAttach(r);
        }

        // 4. Fetch real test data (MongoDB) if enabled
        if (config.isEnableMongo() && config.getMongoDatabase() != null) {
            try (DataFetcher fetcher = new MongoDataFetcher(
                    MongoClients.create(config.getMongoUri()), config.getMongoDatabase())) {
                for (FlowResult r : results) {
                    fetchAndAttach(r, fetcher);
                }
            }
        }

        // 5. Write report
        FlowReporter reporter = new JsonReporter();
        reporter.write(results, out);
    }

    private void fetchAndAttach(FlowResult result, DataFetcher fetcher) {
        // Collect all queries in flow order
        List<ExtractedQuery> orderedQueries = result.allSteps().stream()
                .flatMap(s -> s.getQueries().stream())
                .filter(fetcher::supports)
                .collect(Collectors.toList());

        if (orderedQueries.isEmpty()) return;

        // Chain fetch: each step's output feeds the next step's predicates
        List<TestDataSet> chain = fetcher.fetchChain(orderedQueries, config.getSampleSize());
        chain.forEach(result::addTestDataSet);

        // Also back-fill step.queries with sample rows for reporting
        int i = 0;
        outer:
        for (FlowStep step : result.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (i >= chain.size()) break outer;
                if (fetcher.supports(eq)) {
                    eq.setFetchedSample(chain.get(i++).getSampleRows());
                }
            }
        }
    }

    // --- convenience static entry point ---

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PocRunner <analysis.json> <mongo-db-name> [mongo-uri] [max-endpoints]");
            System.exit(1);
        }
        String jsonPath  = args[0];
        String mongoDb   = args[1];
        String mongoUri  = args.length > 2 ? args[2] : "mongodb://localhost:27017";
        int    maxEp     = args.length > 3 ? Integer.parseInt(args[3]) : 0;

        PocConfig config = PocConfig.builder()
                .analysisJson(jsonPath)
                .mongo(mongoUri, mongoDb)
                .sampleSize(10)
                .maxEndpoints(maxEp)
                .build();

        new PocRunner(config).run(System.out);
    }
}
