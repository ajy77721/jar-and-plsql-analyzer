# JAR Analyzer Core

Java bytecode analysis engine for static inspection of JAR files. Extracts structural
metadata, REST endpoints, call graphs, class hierarchies, and complexity metrics from
compiled Java archives without requiring source code.

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| Bytecode reading | OW2 ASM (asm + asm-tree) | 9.7 |
| Decompilation | CFR | 0.152 |
| Excel export | Apache POI | 5.2.5 |
| Catalog storage | MongoDB Driver Sync | 4.11.2 |
| Runtime | Spring Boot | 3.2.5 |
| Java | JDK | 17 |

## Package Structure

```
com.jaranalyzer
  model/        Data classes: ClassInfo, MethodInfo, EndpointInfo, CallNode,
                FieldInfo, AnnotationInfo, JarAnalysis, ParameterInfo, etc.
  service/      Analysis pipeline and supporting services (56 classes)
  util/         SpringAnnotations, SqlStatementParser, TypeUtils
```

## Key Services

- **JarParserService** -- Orchestrates end-to-end JAR analysis.
- **BytecodeClassParser** -- ASM-based visitor that extracts class/method/field metadata.
- **DecompilerService** -- CFR wrapper for on-demand source decompilation.
- **CallGraphService / CallGraphIndex / CallTreeBuilder** -- Builds and queries method-level call graphs.
- **ExcelExportService** -- Generates multi-sheet XLSX reports with styled output.
- **MongoCatalogService** -- Persists analysis results to MongoDB.
- **ProgressService** -- Tracks analysis progress for SSE streaming.

## Claude AI Enrichment

The module includes a full Claude AI integration layer for enriching static analysis
results with natural-language descriptions and correction suggestions:

- **ClaudeAnalysisService** -- Manages enrichment sessions (chunking, verification, merging).
- **ClaudeProcessRunner** -- Invokes Claude CLI as an external process.
- **ClaudeResultMerger / CorrectionMerger** -- Merges AI-generated corrections back into analysis data.
- **ClaudeSessionManager / ClaudeEnrichmentTracker** -- Session lifecycle and progress tracking.
- **TreeChunker / SwarmClusterer** -- Splits large call trees into Claude-friendly chunks.
- **FragmentStore / CorrectionPersistence** -- Caches fragments and persists corrections to disk.

## Analysis Outputs

- REST endpoint inventory (path, HTTP method, controller class)
- Class hierarchy with interfaces, superclasses, and annotations
- Method signatures with parameter types and return types
- Intra-JAR call graph with depth-limited tree expansion
- Cyclomatic complexity and method-level metrics
- JPA and MongoDB data-access method detection
- Domain-specific clustering via configurable rules

## Build

This module is built as part of the parent `plsql-jar-analyzer` multi-module project:

```bash
mvn clean install -pl jar-analyzer-core
```

The module has no standalone entry point; it is consumed as a dependency by `unified-web`.
