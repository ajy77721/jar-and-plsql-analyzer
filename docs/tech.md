# Technology Reference

> **Version:** 1.0
> **Last Updated:** 2026-05-17

Full dependency inventory for the `plsql-jar-analyzer` multi-module Maven project.  
Versions are from the root POM (`1.0.0-SNAPSHOT` built on Spring Boot `3.2.5`, Java 17).

---

## Core Framework

| Library | Version | Used For |
|---|---|---|
| `spring-boot-starter-parent` | 3.2.5 | Root parent BOM — manages all Spring versions |
| `spring-boot-starter-web` | (managed) | REST controllers, HTTP server (Tomcat embedded) |
| `spring-boot-starter-jdbc` | (managed) | JDBC template for Oracle queries in `unified-web` |
| `spring-boot-maven-plugin` | (managed) | Fat-JAR packaging; JVM args: `-Xms512m -Xmx4g -XX:+UseG1GC` |

---

## Bytecode Analysis

| Library | Version | Used For |
|---|---|---|
| `asm` | 9.7 | Core ASM bytecode reading — `ClassReader`, `ClassNode` |
| `asm-tree` | 9.7 | Tree API (`ClassNode`, `MethodNode`, `FieldNode`) used in `BytecodeClassParser` |
| `cfr` | 0.152 | Java decompiler (CFR) — produces readable source from `.class` bytecode in `DecompilerService` |

---

## Parser / Compiler

| Library | Version | Used For |
|---|---|---|
| `antlr4-runtime` | 4.13.1 | ANTLR4 runtime — runs generated lexer/parser for all five dialects at analysis time |
| `antlr4-maven-plugin` | 4.13.1 | Build-time grammar compilation — generates lexer/parser from the `.g4` files in each parser module |
| `jsqlparser` | 5.0 | SQL statement parsing — used to supplement ANTLR AST with structured DML/table-operation details |

> **Dialect grammars:** `plsql-parser` (`PlSqlLexer.g4` / `PlSqlParser.g4`, Oracle), `mysql-parser`
> (`MySqlLexer.g4` / `MySqlParser.g4`), `sqlserver-parser` (`SqlServerLexer.g4` / `SqlServerParser.g4`,
> T-SQL), `db2-parser` (`Db2Lexer.g4` / `Db2Parser.g4`, SQL PL), and `postgres-parser`
> (`PostgresLexer.g4` / `PostgresParser.g4`, PL/pgSQL). All target ANTLR4 4.13.1.

### Database JDBC Drivers (source fetching)

| Library | Used For |
|---|---|
| `ojdbc11` (23.3.0.23.09) | Oracle — `ALL_SOURCE` / `DBMS_METADATA.GET_DDL` |
| `mysql-connector-j` (managed) | MySQL — routine source via `information_schema` / `SHOW CREATE` |
| `mssql-jdbc` (managed) | Microsoft SQL Server — `OBJECT_DEFINITION` / `sys.sql_modules` |
| `com.ibm.db2:jcc` (managed) | IBM Db2 — routine text from the catalog |
| `org.postgresql:postgresql` (42.7.3) | PostgreSQL — routine text from the catalog, on Maven Central (no manual install needed, unlike `ojdbc11`) |

---

## AI Integration

| Library | Version | Used For |
|---|---|---|
| `claude` (CLI subprocess) | — | Claude AI enrichment — invoked as a subprocess via `ClaudeProcessRunner`; no SDK dependency, communicates over stdin/stdout |

---

## Data / Storage

| Library | Version | Used For |
|---|---|---|
| `jackson-databind` | (managed) | Core JSON serialisation/deserialisation for `analysis.json` and all REST responses |
| `jackson-dataformat-yaml` | (managed) | YAML parsing for `plsql-config.yaml` (connection config) |
| `jackson-datatype-jsr310` | (managed) | Java 8 time type support (`LocalDateTime`) in JSON output |
| `jackson-annotations` | (managed) | `@JsonInclude`, `@JsonIgnoreProperties` on model classes |
| `snakeyaml` | (managed) | YAML loading in `plsql-parser` module |
| `ojdbc11` | 23.3.0.23.09 | Oracle JDBC driver — `ALL_SOURCE`, `ALL_DEPENDENCIES`, `DBMS_METADATA` queries |
| `mongodb-driver-sync` | 4.11.2 | MongoDB catalog verification — lists actual collections to cross-check against analysis |
| `poi-ooxml` | 5.2.5 | Excel export (`ExcelExportService`) — produces `.xlsx` endpoint/table reports |

---

## Frontend

| Technology | Version | Used For |
|---|---|---|
| Vanilla JS + HTML/CSS | — | Single-page app served from `src/main/resources/static/` — no build tool, no framework |
| Server-Sent Events (SSE) | — | Real-time progress streaming from queue jobs to browser |

---

## Build / Tooling

| Tool | Version | Used For |
|---|---|---|
| Maven | 3.x | Multi-module build orchestration |
| Java | 17 | Compile and runtime target |
| JUnit Jupiter | (managed) | Unit tests in `plsql-parser` module |
| Mockito | (managed) | Mocking in unit tests |

---

## Module Dependency Graph

```
plsql-jar-analyzer (root POM)
├── plsql-config          — YAML config models, connection config loader
├── plsql-parser          → plsql-config (Oracle grammar + multidb adapters for
│                            MySQL/SqlServer/Db2/Postgres)
├── mysql-parser, sqlserver-parser, db2-parser, postgres-parser
│                         → plsql-config (each: own ANTLR4 grammar + analysis visitor)
├── plsql-analyzer-core   → plsql-parser, plsql-config
├── jar-analyzer-core     — ASM, CFR, POI, MongoDB driver — shared by both the JAR
│                            (`/api/jar/jars`) and WAR (`/api/war/wars`) controllers
├── flow-poc              — Dynamic flow analysis POC
└── unified-web           → jar-analyzer-core, plsql-analyzer-core,
                            plsql-parser, flow-poc
                            (Oracle JDBC, Jackson YAML/JSR310)
```

---

## Tips

- **ASM versions**: `asm` and `asm-tree` must stay in sync — mismatched versions cause `ClassReader` API breakage. Both are pinned to `9.7` via `${asm.version}` in the root POM.
- **CFR 0.152**: Does not support Java 21 bytecode features fully. If target JARs were compiled with Java 21+, decompilation may return incomplete source for some classes.
- **ojdbc11 23.3**: Requires JDK 11+. Using `ojdbc8` on Java 17 will produce deprecation warnings and may fail with certain SSL configurations.
- **MongoDB driver**: Used only for catalog verification (listing collection names). No MongoDB dependency is required for core JAR/PL/SQL analysis — if no MongoDB config is found in the JAR's resources, the catalog step is silently skipped.
- **No Spring Data**: The JAR analyzer uses `mongodb-driver-sync` directly, not Spring Data MongoDB, to keep the module lightweight and avoid auto-configuration of a MongoDB connection on startup.
