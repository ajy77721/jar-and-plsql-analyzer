/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.analyzer.storage.ArtifactKey
 *  com.analyzer.storage.ArtifactStore
 *  com.analyzer.storage.LocalHandle
 *  com.analyzer.storage.StorageFactory
 *  com.analyzer.storage.StorageFactory$Settings
 *  com.mongodb.client.MongoClient
 *  com.mongodb.client.MongoClients
 */
package com.flowpoc;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.analyzer.storage.StorageFactory;
import com.flowpoc.ProgressCallback;
import com.flowpoc.analyzer.AnalyzerPipeline;
import com.flowpoc.config.PocConfig;
import com.flowpoc.engine.FlowWalker;
import com.flowpoc.engine.SqlPredicateExtractor;
import com.flowpoc.engine.visitor.CallTreeVisitor;
import com.flowpoc.engine.visitor.MongoOperationVisitor;
import com.flowpoc.engine.visitor.SqlOperationVisitor;
import com.flowpoc.fetch.DataFetcher;
import com.flowpoc.fetch.MongoDataFetcher;
import com.flowpoc.layer2.BeanFactoryLoader;
import com.flowpoc.layer2.CollectingQueryInterceptor;
import com.flowpoc.layer2.DynamicFlowExecutor;
import com.flowpoc.layer2.JarClassLoader;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.TestDataSet;
import com.flowpoc.report.JsonReporter;
import com.flowpoc.report.MarkdownReporter;
import com.flowpoc.testdata.EntitySchemaBuilder;
import com.flowpoc.testdata.TestDataBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PocRunner {
    private final PocConfig config;

    public PocRunner(PocConfig config) {
        this.config = config;
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public List<FlowResult> runAndReturn() throws Exception {
        Map<String, Map<String, Object>> entitySamples;
        List<FlowResult> results;
        ArrayList<CallTreeVisitor> visitors = new ArrayList<CallTreeVisitor>();
        visitors.add(new MongoOperationVisitor());
        visitors.add(new SqlOperationVisitor(new SqlPredicateExtractor()));
        FlowWalker walker = new FlowWalker(visitors);
        try (InputStream in = this.openAnalysis();){
            results = walker.walk(in, this.config.getSelectedEndpoints());
        }
        if (this.config.getMaxEndpoints() > 0 && results.size() > this.config.getMaxEndpoints()) {
            results = results.subList(0, this.config.getMaxEndpoints());
        }
        ProgressCallback cb = this.config.getProgressCallback();
        AnalyzerPipeline pipeline = this.config.getClaudeInvoker() != null ? AnalyzerPipeline.builder().withMongoDefaultsAndClaude(this.config.getClaudeInvoker(), cb).build() : (this.config.isClaudeEnabled() ? AnalyzerPipeline.builder().withMongoDefaultsAndClaude(this.config.getClaudeApiKey(), cb).build() : AnalyzerPipeline.builder().withMongoDefaults().build());
        int total = results.size();
        for (int i = 0; i < results.size(); ++i) {
            FlowResult r = results.get(i);
            String ep = r.getEndpointMethod() + " " + r.getEndpointPath();
            if (cb != null) {
                cb.onEndpointStart(i + 1, total, ep);
            }
            pipeline.runAndAttach(r);
            if (cb == null) continue;
            cb.onEndpointDone(i + 1, total, ep, r);
        }
        if (this.config.isEnableMongo() && this.config.getMongoDatabase() != null) {
            try (MongoDataFetcher fetcher = new MongoDataFetcher(MongoClients.create((String)this.config.getMongoUri()), this.config.getMongoDatabase());){
                for (FlowResult r : results) {
                    this.fetchAndAttach(r, fetcher);
                }
            }
        }
        try (InputStream in = this.openAnalysis();){
            entitySamples = EntitySchemaBuilder.buildCollectionSamples(in);
        }
        for (FlowResult r : results) {
            for (TestDataSet tds : r.getTestDataSets()) {
                if (!tds.getTestCases().isEmpty()) continue;
                Map<String, Object> sample = entitySamples.get(tds.getTable());
                TestDataBuilder.appendStaticEdgeCases(tds, tds.getAppliedPredicates(), sample);
            }
        }
        if (this.config.isLayer2Enabled() && this.config.getJarKey() != null) {
            try (MongoClient preReadClient = this.config.isEnableMongo() && this.config.getMongoUri() != null ? MongoClients.create((String)this.config.getMongoUri()) : null;
                 LocalHandle jar = this.config.getStore().materialize(this.config.getJarKey());){
                JarClassLoader jarClassLoader = new JarClassLoader(jar.path().toFile());
                CollectingQueryInterceptor queryInterceptor = new CollectingQueryInterceptor();
                BeanFactoryLoader bfl = new BeanFactoryLoader(jarClassLoader, Collections.emptyList(), queryInterceptor, preReadClient, this.config.getMongoDatabase());
                DynamicFlowExecutor executor = new DynamicFlowExecutor(bfl, queryInterceptor, this.config);
                for (FlowResult r : results) {
                    executor.execute(r);
                }
            }
        }
        return results;
    }

    public void run(OutputStream out) throws Exception {
        List<FlowResult> results = this.runAndReturn();
        new JsonReporter().write(results, out);
        ArtifactKey reportKey = this.config.getReportKey();
        if (reportKey != null) {
            try (OutputStream md = this.config.getStore().write(reportKey);){
                new MarkdownReporter().write(results, md);
            }
            catch (Exception exception) {
                // empty catch block
            }
        }
    }

    private InputStream openAnalysis() throws IOException {
        return this.config.getStore().read(this.config.getAnalysisKey());
    }

    private void fetchAndAttach(FlowResult result, DataFetcher fetcher) {
        List<ExtractedQuery> orderedQueries = result.allSteps().stream().flatMap(s -> s.getQueries().stream()).filter(fetcher::supports).collect(Collectors.toList());
        if (orderedQueries.isEmpty()) {
            return;
        }
        List<TestDataSet> chain = fetcher.fetchChain(orderedQueries, this.config.getSampleSize());
        chain.forEach(result::addTestDataSet);
        int i = 0;
        block0: for (FlowStep step : result.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (i >= chain.size()) break block0;
                if (!fetcher.supports(eq)) continue;
                eq.setFetchedSample(chain.get(i++).getSampleRows());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: PocRunner <data-root> <analysis-key> <mongo-db-name> [mongo-uri] [max-endpoints]");
            System.err.println("  <analysis-key> is '/'-separated and relative to <data-root>, e.g. jar/my-app/analysis.json");
            System.exit(1);
            return;
        }
        String dataRoot = args[0];
        String keyText = args[1];
        String mongoDb = args[2];
        String mongoUri = args.length > 3 ? args[3] : "mongodb://localhost:27017";
        int maxEp = args.length > 4 ? Integer.parseInt(args[4]) : 0;
        ArtifactStore store = StorageFactory.create((StorageFactory.Settings)StorageFactory.settingsFor((String)System.getProperty("app.storage.mode"), (Path)Path.of(dataRoot, new String[0])));
        PocConfig config = PocConfig.builder().analysis(store, PocRunner.keyOf(keyText)).mongo(mongoUri, mongoDb).sampleSize(10).maxEndpoints(maxEp).build();
        new PocRunner(config).run(System.out);
    }

    static ArtifactKey keyOf(String text) {
        ArrayList<String> segs = new ArrayList<String>();
        for (String raw : text.replace('\\', '/').split("/")) {
            if (raw.isBlank()) continue;
            if (raw.equals(".") || raw.equals("..")) {
                throw new IllegalArgumentException("Key escapes the data root: " + text);
            }
            segs.add(raw);
        }
        if (segs.isEmpty()) {
            throw new IllegalArgumentException("Key is empty");
        }
        return ArtifactKey.of((String[])segs.toArray(new String[0]));
    }
}

