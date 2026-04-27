# Configuration Properties Reference

Complete reference for every property in `application.properties` for the Unified PL/SQL + JAR Analyzer.

---

## Server & Application Identity

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `server.port` | `8083` | The HTTP port the Spring Boot application listens on. | Change when port 8083 is already in use by another process, or when deploying multiple instances on the same host. | Changing this requires updating any bookmarks, frontend proxy configs, and monitoring tools that reference the old port. |
| `spring.application.name` | `unified-analyzer` | The logical name of the application used in logs, actuator endpoints, and service discovery. | Change when running multiple analyzer instances that need distinct names for operational identification. | Affects log output prefixes and any infrastructure that identifies services by name. No runtime behavior change. |

---

## Dynamic Directory Configuration

These properties control where the application reads configuration files and writes analysis output.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `app.config-dir` | `./config` (resolved to absolute path) | Root directory for configuration files: `plsql-config.yaml`, `domain-config.json`, and `prompts/*.txt` templates. | Change when deploying to a different machine or when config files live outside the project tree (e.g., a shared config volume). | If the path is wrong or inaccessible, the application will fail to load PL/SQL database connections, domain mappings, and Claude prompt templates. |
| `app.data-dir` | `./data` (resolved to absolute path) | Root directory for all output: JAR analyses, PL/SQL parse results, logs, cache files, and Claude enrichment artifacts. | Change when the default disk lacks space, when pointing to a shared/network drive, or when separating data across environments. | All persistent output uses this root. Changing it after analyses exist means prior results become invisible unless you also move the files. |

---

## JAR Upload & Multipart Settings

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `spring.servlet.multipart.max-file-size` | `2GB` | Maximum size of a single uploaded file (JAR) via the multipart POST endpoint. | Increase if you need to analyze JAR files larger than 2 GB. Decrease to restrict uploads in resource-constrained environments. | Uploads exceeding this limit are rejected with a `413 Payload Too Large` error before any processing begins. |
| `spring.servlet.multipart.max-request-size` | `2GB` | Maximum size of the entire multipart HTTP request, including all parts (file + form fields + headers). | Should be at least as large as `max-file-size`. Increase if multiple files are uploaded simultaneously. | Requests exceeding this are rejected at the Tomcat level before reaching the controller. |

---

## JSON & Compression

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `spring.jackson.serialization.indent-output` | `false` | Whether JSON responses are pretty-printed with indentation. | Set to `true` during development/debugging to make JSON responses human-readable. Keep `false` in production. | Enabling adds significant overhead to large analysis JSON payloads (analysis files can be 100+ MB). Pretty-printing can double response size. |
| `server.compression.enabled` | `true` | Enables gzip compression on HTTP responses. | Disable only if a reverse proxy (nginx, Cloudflare) already handles compression to avoid double-encoding. | When enabled, large JSON responses (analysis summaries, call graphs) are compressed before transmission, reducing bandwidth by 70-90%. |
| `server.compression.mime-types` | `application/json` | Which content types are eligible for compression. | Add `text/html,text/plain,text/css,application/javascript` if serving significant non-JSON content. | Only responses with these MIME types are compressed. Other content types are sent uncompressed. |
| `server.compression.min-response-size` | `1024` | Minimum response body size (in bytes) before compression is applied. | Increase if CPU usage from compression is a concern for many small responses. Decrease if bandwidth is the bottleneck. | Responses smaller than this threshold are sent uncompressed since the compression overhead would outweigh the size savings. |

---

## Async Request Timeout

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `spring.mvc.async.request-timeout` | `-1` (no timeout) | Timeout in milliseconds for async (deferred) HTTP requests including SSE streams. `-1` disables the timeout entirely. | Set to a positive value (e.g., `3600000` for 1 hour) if you need to enforce a hard upper bound on long-running SSE connections. | With `-1`, SSE event streams for queue progress and Claude verification remain open indefinitely until the client disconnects or the server shuts down. Setting a timeout will close idle SSE connections after the specified duration. |

---

## Claude CLI Analysis Configuration

These properties control how the Claude AI enrichment pipeline operates.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `claude.analysis.max-endpoints` | `-1` (no limit) | Maximum number of endpoints (JAR) or procedures (PL/SQL) to analyze with Claude in a single session. `-1` means analyze all. | Set to a positive number (e.g., `10`) for testing or cost control. Useful when onboarding a new JAR to preview Claude output before committing to a full scan. | Limits how many endpoint chunks are sent to Claude CLI. Remaining endpoints retain only static analysis data. |
| `claude.analysis.parallel-chunks` | `5` | Number of Claude CLI processes that execute concurrently during enrichment. | Increase (e.g., `10`) on machines with many cores and sufficient API quota. Decrease (e.g., `2`) if hitting rate limits or on resource-constrained machines. | Higher values speed up enrichment linearly but consume more CPU, memory, and API quota. Each process is a separate Claude CLI invocation. |
| `claude.allowed-tools` | `Read,Grep,Glob,Bash,Write,Edit` | Comma-separated list of tools that Claude CLI is permitted to use during analysis. | Remove tools (e.g., remove `Bash,Write,Edit`) to restrict Claude to read-only operations for safety in sensitive environments. | Restricting tools limits Claude's ability to inspect the codebase. For example, removing `Grep` prevents Claude from searching across files for cross-references. |

---

## Call Tree Building Limits

These limits prevent runaway recursion and memory exhaustion when building call trees for deeply nested codebases.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `calltree.max-depth` | `20` | Maximum depth of call tree traversal (JAR analyzer). Once a call chain exceeds this depth, further children are not expanded. | Increase for deeply layered architectures (e.g., microservice frameworks with many decorators). Decrease for faster tree generation. | Deeper trees produce more complete analysis but consume more memory and CPU. Circular references are already detected separately. |
| `calltree.max-children-per-node` | `30` | Maximum number of child method calls shown per node in the call tree. | Increase if methods have many outgoing calls (e.g., builder patterns, initialization methods). | Excess children are truncated with a "...and N more" indicator. This prevents tree views from becoming unmanageably wide. |
| `calltree.max-nodes-per-tree` | `2000` | Absolute maximum number of nodes in a single call tree. Tree expansion stops when this limit is reached. | Increase for very large codebases. Decrease for faster rendering on the frontend. | This is a safety valve. Once reached, a `[TRUNCATED]` marker appears. Overly large trees can cause browser rendering issues in the UI. |

---

## Chunking & Prompt Size Limits

These properties control how analysis data is split into chunks for Claude CLI processing.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `claude.chunking.max-chunk-chars` | `50000` | Maximum character count for a single chunk of analysis data sent to Claude. Endpoints/tables are grouped until this limit is reached. | Decrease if Claude responses are being truncated or if you get context-window errors. Increase if chunks are too granular. | Smaller chunks mean more Claude invocations (slower but safer). Larger chunks risk hitting Claude's context window limit or producing less focused results. |
| `claude.chunking.max-tree-nodes` | `500` | Maximum number of call-tree nodes included per chunk. Large call trees are pruned to this limit before being sent. | Increase for more complete call-tree context per chunk. Decrease if prompts are too long. | Affects the quality of Claude's analysis -- more nodes give Claude better context about the execution flow, but increase prompt size. |
| `claude.chunking.max-chunk-depth` | `3` | Maximum call-tree depth included in a chunk. Only the top N levels of the call tree are sent. | Increase if Claude needs to see deeper call chains for accurate analysis. | Higher depth means more complete context but larger prompts. Most meaningful analysis data is in the first 2-3 levels. |
| `claude.chunking.max-prompt-chars` | `180000` | Absolute maximum character count for the entire prompt sent to a single Claude CLI invocation (chunk data + prompt template + system instructions). | Decrease if using a model with a smaller context window. This should stay well below the model's token limit. | If a prompt exceeds this, it is rejected before sending to Claude. This prevents wasted API calls on prompts that would fail anyway. |

---

## Timeout Configuration

All timeouts are in **seconds** unless otherwise noted.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `claude.timeout.per-endpoint` | `7200` (2 hours) | Maximum time allowed for a single Claude CLI invocation analyzing one endpoint/chunk. | Increase for extremely complex endpoints. Decrease if you prefer to fail fast and retry. | If exceeded, the Claude process is killed and the chunk is marked as FAILED. The endpoint retains its static analysis data. |
| `claude.timeout.version-check` | `30` | Timeout for the `claude --version` check that verifies CLI availability at startup or before sessions. | Increase on slow machines or when Claude CLI is on a network drive. | If the version check times out, Claude features are marked as unavailable and all enrichment endpoints return 503. |
| `claude.timeout.stream-drain` | `3000` | Time allowed to drain stdout/stderr streams after a Claude CLI process finishes or is killed. | Increase if you see incomplete output in fragment files. | If streams are not fully drained, the last portion of Claude's response may be lost, leading to JSON parse errors in the output. |
| `claude.timeout.executor-shutdown` | `60` | Grace period for the parallel executor thread pool to finish all running tasks during application shutdown. | Increase if Claude tasks are still running when the app shuts down and you want them to complete. | After this timeout, any still-running Claude processes are force-killed. In-progress analyses are marked as FAILED. |
| `mongo.timeout.connect` | `30` | Connection timeout (seconds) for MongoDB catalog fetching (JAR analyzer feature). | Increase if the MongoDB server is on a high-latency network. | If the connection cannot be established within this time, catalog fetch fails and collection verification is unavailable. |
| `mongo.timeout.server-selection` | `30` | Server selection timeout for MongoDB driver. Time to wait for a suitable server in a replica set. | Increase for replica sets with slow failover. | Affects only the MongoDB catalog feature. If exceeded, returns an error for catalog operations. |

---

## PL/SQL Analyzer Configuration

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `server.tomcat.max-http-form-post-size` | `50MB` | Maximum size of HTTP POST form data (non-multipart). Used for large analysis request bodies. | Increase if analysis request payloads with embedded source code exceed 50 MB. | Requests exceeding this limit are rejected by Tomcat before reaching Spring controllers. |
| `spring.autoconfigure.exclude` | `DataSourceAutoConfiguration` | Disables Spring Boot's automatic DataSource configuration. The application manages its own JDBC connections via `plsql-config.yaml`. | Do not change. Removing this exclusion causes startup failure because there is no `spring.datasource.*` configuration. | The application uses manual JDBC connection management through `DbSourceFetcher` and `PlsqlConfig` instead of a Spring-managed `DataSource`. |
| `plsql.threads.source-fetch` | `8` | Number of threads in the pool used for fetching PL/SQL source code from Oracle `ALL_SOURCE` in parallel. | Increase for faster analysis of packages with many dependencies. Decrease if the Oracle database has limited connection capacity. | More threads = faster source fetching but more concurrent database connections. Each thread holds its own JDBC connection during fetch. |
| `plsql.threads.trigger-resolve` | `4` | Number of threads for parallel trigger resolution (querying `ALL_TRIGGERS` and parsing trigger bodies). | Increase if the schema has hundreds of triggers. | More threads speed up trigger analysis but add database load. Trigger resolution runs after the main BFS crawl completes. |
| `plsql.threads.metadata` | `4` | Number of threads for fetching table metadata (columns, constraints, indexes) from `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS`, etc. | Increase for schemas with many tables referenced by the analysis. | Parallel metadata fetching reduces wall-clock time for the metadata enrichment phase. |
| `plsql.threads.claude-parallel` | `5` | Number of parallel Claude CLI processes for PL/SQL verification (equivalent to `claude.analysis.parallel-chunks` for PL/SQL). | Same guidance as `claude.analysis.parallel-chunks`. | Controls parallelism of the PL/SQL Claude verification pipeline specifically. |
| `plsql.tree.max-depth` | `50` | Maximum call-tree depth for the PL/SQL analyzer. PL/SQL call chains tend to be deeper than Java. | Increase for extremely nested PL/SQL packages. | Deeper trees capture more of the execution flow but increase memory usage and response times. |
| `plsql.tree.max-nodes` | `2000` | Maximum total nodes in a PL/SQL call tree. | Same guidance as `calltree.max-nodes-per-tree`. | Safety limit to prevent memory exhaustion on pathologically recursive PL/SQL code. |
| `app.parser-config-path` | `{app.config-dir}/plsql-config.yaml` | Path to the PL/SQL parser configuration YAML file containing ANTLR grammar settings and BFS crawler options. | Change when the YAML config is stored outside the default config directory. | If this file is missing or unreadable, the parser falls back to defaults but may miss schema mappings and custom type resolution rules. |

---

## Chat Configuration

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `chat.classic.enabled` | `true` | Enables the classic chat interface (full-page chat view with session management). | Set to `false` to hide the classic chat tab from the UI. | When disabled, the classic chat tab is not shown and the `POST /api/chat/sessions` endpoint still exists but the frontend does not expose it. |
| `chat.chatbox.enabled` | `true` | Enables the floating chatbox widget that appears on analysis pages. | Set to `false` to disable the floating chatbox overlay. | When disabled, the chatbox button does not appear on analysis pages. Users can still use the classic chat if enabled. |

---

## Resource Caching (Development Mode)

These settings disable browser caching for static resources, ensuring that changes to HTML/CSS/JS are immediately visible during development.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `spring.web.resources.cache.period` | `0` | Cache duration in seconds for static resources. `0` means no caching. | Set to `3600` (1 hour) or higher in production to leverage browser caching and reduce server load. | With `0`, every page load re-fetches all static resources from the server. This is ideal for development but wasteful in production. |
| `spring.web.resources.cache.cachecontrol.no-cache` | `true` | Adds `Cache-Control: no-cache` header to static resource responses. | Set to `false` in production along with a positive `cache.period`. | Forces browsers to revalidate with the server on every request, even if the resource was recently fetched. |
| `spring.web.resources.cache.cachecontrol.no-store` | `true` | Adds `Cache-Control: no-store` header, preventing browsers and proxies from storing any copy of the response. | Set to `false` in production. | Most aggressive no-caching directive. Ensures nothing is stored anywhere. In production, this wastes bandwidth and increases latency. |

---

## Tomcat Tuning

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `server.tomcat.threads.max` | `50` | Maximum number of Tomcat worker threads for handling HTTP requests. | Increase if the application handles many concurrent users or long-running SSE connections that tie up threads. Decrease for resource-constrained deployments. | Each SSE connection consumes one thread. With 50 max threads and many open SSE streams, new HTTP requests may be queued. Consider increasing for heavy SSE usage. |
| `server.tomcat.threads.min-spare` | `4` | Minimum number of idle Tomcat threads kept in the pool. | Increase if the application receives bursty traffic and you want faster initial response times. | These threads are pre-allocated and waiting for requests. Keeps response latency low for the first few concurrent requests. |
| `server.tomcat.connection-timeout` | `60s` | Time Tomcat waits for a client to send a complete request after the TCP connection is established. | Increase if clients are on slow networks and uploads sometimes time out. Decrease to free up connections faster. | If a client does not send request headers within this time, the connection is closed. Does not affect response time -- only the initial request phase. |

---

## Logging Configuration

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `logging.file.name` | `{app.data-dir}/unified-analyzer.log` | Path to the application log file. Also served by the in-app log viewer at `GET /api/jar/logs`. | Change to redirect logs to a different location (e.g., `/var/log/analyzer/`). | The log viewer endpoint reads directly from this file. If changed, ensure the in-app viewer still has read access. |
| `logging.logback.rollingpolicy.max-file-size` | `10MB` | Maximum size of a single log file before rotation occurs. | Increase for verbose logging or to reduce rotation frequency. Decrease if disk space is limited. | When the current log file exceeds this size, it is renamed with a timestamp and a new empty file is created. |
| `logging.logback.rollingpolicy.max-history` | `10` | Number of rotated log files to retain. | Increase to keep more history for debugging. Decrease to save disk space. | Older rotated files beyond this count are automatically deleted. With 10 MB per file and 10 history, up to ~110 MB of logs are retained. |
| `logging.logback.rollingpolicy.total-size-cap` | `500MB` | Absolute maximum disk space for all log files combined (current + rotated). | Decrease in disk-constrained environments. Increase if extensive log history is needed. | Once total log storage exceeds this cap, the oldest rotated files are deleted regardless of `max-history`. This is the hard upper bound. |
| `logging.logback.rollingpolicy.clean-history-on-start` | `true` | Whether to delete old rotated log files when the application starts. | Set to `false` if you want to preserve logs from previous runs across restarts. | When `true`, stale rotated logs from previous runs are cleaned up on startup, ensuring a fresh log directory. Useful for development where old logs are rarely needed. |

---

## Polling Intervals (Server-Side Defaults)

These properties are served to the frontend via `GET /api/config/polling` so that all polling intervals are configurable server-side without redeploying the UI.

| Property | Default | What It Controls | When to Change | Impact |
|----------|---------|------------------|----------------|--------|
| `app.poll.session-ms` | `30000` (30s) | How often the frontend polls for Claude session status updates. | Decrease for more responsive session status. Increase to reduce server load. | Lower values give faster feedback when sessions complete or fail, but increase HTTP request volume. |
| `app.poll.claude-progress-ms` | `30000` (30s) | How often the frontend polls Claude enrichment/verification progress. | Decrease during active Claude scans for near-real-time progress. Increase when Claude is idle. | Affects the responsiveness of progress bars and completion notifications in the UI. |
| `app.poll.job-status-ms` | `10000` (10s) | How often the frontend polls the queue for job status updates. | Decrease for faster job completion notifications. | This is the fallback when SSE is unavailable. SSE provides instant updates when connected. |
| `app.poll.log-refresh-ms` | `10000` (10s) | How often the in-app log viewer refreshes its content. | Decrease for near-real-time log tailing. Increase to reduce I/O. | Each refresh reads the tail of the log file from disk. Frequent refreshes on large log files can add I/O overhead. |
| `app.poll.correction-ms` | `30000` (30s) | How often the frontend polls for Claude correction progress (JAR analyzer). | Same guidance as `claude-progress-ms`. | Affects responsiveness of the correction progress display. |
