package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

@Service("parserAnalysisService")
public class AnalysisService {

    private final String flowOutputDir;

    @Value("${app.parser-config-path:config/plsql-config.yaml}")
    private String configPath;

    private final AnalysisReaderFactory readerFactory;
    private final StaticAnalysisReader staticReader;

    public AnalysisService(@Value("${app.data-dir.base:data}") String dataDir,
                           AnalysisReaderFactory readerFactory,
                           StaticAnalysisReader staticReader) {
        this.flowOutputDir = Path.of(dataDir, "plsql-parse").toString();
        this.readerFactory = readerFactory;
        this.staticReader = staticReader;
    }

    public List<AnalysisInfo> listAnalyses() throws IOException {
        Path root = Path.of(flowOutputDir);
        if (!Files.isDirectory(root)) return Collections.emptyList();

        List<AnalysisInfo> results = new ArrayList<>();
        try (Stream<Path> dirs = Files.list(root)) {
            dirs.filter(Files::isDirectory).sorted().forEach(dir -> {
                try {
                    AnalysisInfo info = buildInfo(dir);
                    if (info != null) results.add(info);
                } catch (IOException ignored) {}
            });
        }
        return results;
    }

    public JsonNode getIndex(String name) throws IOException {
        return readerFactory.getReader(name).getIndex(name);
    }

    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        return readerFactory.getReader(name).getNodeDetail(name, fileName);
    }

    public JsonNode getTables(String name) throws IOException {
        return readerFactory.getReader(name).getTables(name);
    }

    public JsonNode getCallGraph(String name) throws IOException {
        return readerFactory.getReader(name).getCallGraph(name);
    }

    public String getSource(String name, String fileName) throws IOException {
        return readerFactory.getReader(name).getSource(name, fileName);
    }

    public JsonNode getResolver(String name, String type) throws IOException {
        return readerFactory.getReader(name).getResolver(name, type);
    }

    public JsonNode getProcedures(String name) throws IOException {
        return readerFactory.getReader(name).getProcedures(name);
    }

    public JsonNode getJoins(String name) throws IOException {
        return readerFactory.getReader(name).getJoins(name);
    }

    public JsonNode getCursors(String name) throws IOException {
        return readerFactory.getReader(name).getCursors(name);
    }

    public JsonNode getSequences(String name) throws IOException {
        return readerFactory.getReader(name).getSequences(name);
    }

    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        return readerFactory.getReader(name).getCallTree(name, nodeId);
    }

    public JsonNode getCallers(String name, String nodeId) throws IOException {
        return readerFactory.getReader(name).getCallers(name, nodeId);
    }

    public String runAnalysis(String entryPoint) throws Exception {
        return runAnalysis(entryPoint, null, null);
    }

    public String runAnalysis(String entryPoint, String owner, String objectType) throws Exception {
        String target = entryPoint;
        if (owner != null && !owner.isBlank() && !entryPoint.contains(".")) {
            target = owner.toUpperCase() + "." + entryPoint;
        }

        String baseFolderName = target.toUpperCase()
                .replace(".", "_")
                .replace("/", "_")
                .replace("\\", "_");

        Path existingDir = Path.of(flowOutputDir, baseFolderName);
        if (Files.isDirectory(existingDir)) {
            String oldTs = java.time.LocalDateTime.now()
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            Path archived = Path.of(flowOutputDir, baseFolderName + "_" + oldTs);
            Files.move(existingDir, archived, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            staticReader.evictCache(baseFolderName);
        }

        java.util.List<String> argList = new java.util.ArrayList<>();
        argList.add("--flow");
        argList.add("--config"); argList.add(configPath);
        argList.add("--entry"); argList.add(target);
        argList.add("--output-dir"); argList.add(flowOutputDir);
        argList.add("--pretty");

        com.plsql.parser.flow.FlowAnalysisMain.run(argList.toArray(new String[0]));

        staticReader.evictCache(baseFolderName);

        return baseFolderName;
    }

    // --- helpers ---

    private AnalysisInfo buildInfo(Path dir) throws IOException {
        Path indexFile = dir.resolve("api/index.json");
        if (!Files.isRegularFile(indexFile)) return null;

        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        JsonNode root = mapper.readTree(indexFile.toFile());

        AnalysisInfo info = new AnalysisInfo();
        info.setName(dir.getFileName().toString());
        info.setEntryPoint(textOrDefault(root, "entryPoint", dir.getFileName().toString()));
        info.setEntrySchema(textOrDefault(root, "entrySchema", ""));
        info.setTotalNodes(intOrDefault(root, "totalNodes", 0));
        info.setTotalTables(intOrDefault(root, "totalTables", 0));
        info.setTotalEdges(intOrDefault(root, "totalEdges", 0));
        info.setTotalLinesOfCode(intOrDefault(root, "totalLinesOfCode", 0));
        info.setMaxDepth(intOrDefault(root, "maxDepth", 0));
        info.setCrawlTimeMs(longOrDefault(root, "crawlTimeMs", 0));
        info.setDbCallCount(intOrDefault(root, "dbCallCount", 0));

        List<String> errors = new ArrayList<>();
        JsonNode errNode = root.get("errors");
        if (errNode != null && errNode.isArray()) {
            errNode.forEach(e -> errors.add(e.asText()));
        }
        info.setErrors(errors);

        return info;
    }

    private static String textOrDefault(JsonNode node, String field, String def) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : def;
    }

    private static int intOrDefault(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }

    private static long longOrDefault(JsonNode node, String field, long def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asLong() : def;
    }
}
