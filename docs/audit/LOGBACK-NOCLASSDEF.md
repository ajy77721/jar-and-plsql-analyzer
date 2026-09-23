# NoClassDefFoundError: ch/qos/logback/classic/spi/ThrowableProxy

Date: 2026-09-04. Instance: port 8083.

## Verdict

Root cause: **the Spring Boot fat jar was rebuilt while an instance launched from it was still
running.** This is a runtime/lifecycle problem, not a dependency or packaging problem. No code fix
is warranted; the fix is a workflow rule.

## Evidence

1. The class IS packaged. `unzip -l` on the fat jar only lists nested jars, so grepping the fat jar
   for `ThrowableProxy` returns nothing — that is expected and not a finding. Extracting the nested
   jar shows the class:

   ```
   unzip -o -q -j unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
     "BOOT-INF/lib/logback-classic-1.4.14.jar" -d /tmp/lb
   unzip -l /tmp/lb/logback-classic-1.4.14.jar | grep ThrowableProxy
   #  5578  ch/qos/logback/classic/spi/ThrowableProxy.class
   ```

2. Timestamps line up exactly.

   ```
   ps -o lstart,pid,command -p 81946
   #  Fri Sep  4 12:13:59 2026  81946  java -jar .../unified-web-1.0.0-SNAPSHOT.jar   (LISTEN :8083)
   ls -la unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
   #  84643586  4 Sep 12:38
   ```

   The process started at 12:13:59; the jar it is executing was overwritten at 12:38 — ~24 minutes
   into its life. A Boot fat jar is read lazily: the JVM keeps the archive open and resolves each
   nested class on first use. Replacing the file underneath it invalidates the central directory /
   offsets the open handle is using, so every class not already loaded becomes unresolvable.
   `ThrowableProxy` is only touched the first time an exception is logged, which is why it is the
   class that surfaces.

3. Dependency graph is clean — no alternative explanation.

   ```
   mvn -pl unified-web dependency:tree | grep -iE 'logback|slf4j|log4j|logging'
   spring-boot-starter-logging:3.2.5
     ch.qos.logback:logback-classic:1.4.14 -> logback-core:1.4.14
     org.apache.logging.log4j:log4j-to-slf4j:2.21.1
     org.slf4j:jul-to-slf4j:2.0.13
   org.slf4j:slf4j-api:2.0.13   (single binding)
   ```

   One logback version, one SLF4J binding, `jul-to-slf4j` present as intended, no
   `spring-boot-starter-logging` exclusion in any pom. The only `<exclusion>` in the repo is in
   `analyzer-storage/pom.xml` (AWS `netty-nio-client`) and is unrelated.

## Where SLF4JBridgeHandler comes from

Nothing in this repo calls `SLF4JBridgeHandler.install()`. Spring Boot's `LogbackLoggingSystem`
installs it automatically because `jul-to-slf4j` is on the classpath. `logback-spring.xml` declares
`LevelChangePropagator` with `resetJUL=true`; its previous comment claimed this prevented the
`ThrowableProxy` error. That claim is wrong (Boot re-installs the bridge after configuration is
loaded, and no JUL setting can make an unreadable jar readable). The comment has been corrected —
that is the only file change made. `LevelChangePropagator` itself is kept: it is a legitimate
performance optimisation for JUL→SLF4J filtering.

## Why the stack trace is misleading

Tomcat's `NioEndpoint$SocketProcessor` caught some *original* failure on the request thread and
called `DirectJDKLog.error(...)` to report it. That goes JUL → `SLF4JBridgeHandler` → logback →
`new LoggingEvent(...)` → `new ThrowableProxy(...)` → `NoClassDefFoundError`. The logging path
itself needed the missing class, so the logging attempt threw and the original exception was never
printed. Every frame in the trace is the *reporting* machinery; none of it is the real fault.
Secondary consequence: once the jar is overwritten, **all** error reporting from that JVM is dead,
so the process can be failing silently in other ways too.

## Harmful or cosmetic?

- Jar overwritten under a running JVM (this case): **harmful.** The process is permanently damaged.
  Any code path that has not yet loaded all its classes will fail unpredictably (missing
  controllers, missing JDBC driver classes, half-initialised beans), and no error will be logged.
  Restart is mandatory; nothing else recovers it.
- Bridge left installed at shutdown (the other scenario worth ruling out): **cosmetic.** If logback
  stops its context while JUL still routes through the bridge, late shutdown records can trip a
  similar error. That is a one-off noise line during exit, no functional impact — and it is not what
  happened here, since the trace comes from an active `http-nio-8083-exec` worker thread mid-request,
  not from a shutdown hook.

## Recommended fix (workflow, not code)

Stop the running instance before rebuilding. `mvn install`/`package` writes straight over
`unified-web/target/unified-web-1.0.0-SNAPSHOT.jar`, and on macOS/Linux nothing prevents overwriting
a file another process has open.

```bash
# before any rebuild
lsof -nP -iTCP:8083 -sTCP:LISTEN          # find the pid holding the port
kill <pid>                                 # or: pkill -f unified-web-1.0.0-SNAPSHOT.jar
mvn -q install                             # then rebuild
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
```

For day-to-day development, prefer a mode where a rebuild cannot corrupt the live process:

```bash
mvn -pl unified-web spring-boot:run        # runs from target/classes + ~/.m2, not from the fat jar
```

Rejected options and why:
- A Maven profile/plugin that refuses to overwrite a held-open jar: adds build machinery to enforce
  something a one-line `pkill` in the start script already covers, and `lsof`-based detection is
  platform-specific and racy.
- Logback config changes to stop the cascade: cannot help. The failure is the classloader, and any
  fallback appender would need to load classes from the same broken archive.

If a guard is wanted anyway, the right place is `start.sh` / `start.bat` (kill any existing instance
before launching), not the Maven build.
