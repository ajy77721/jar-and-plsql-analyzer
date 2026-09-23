# ═══════════════════════════════════════════════════════════════
# Unified Analyzer — build + runtime image
# Modeled on the hello-flask template's Dockerfile pattern:
# thin build stage → slim JRE runtime, config via env vars.
# ═══════════════════════════════════════════════════════════════

# ── Build stage: Maven + JDK 17 ──
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

# Copy poms first so dependency download is cached independently of source
COPY pom.xml .
COPY plsql-config/pom.xml      plsql-config/
COPY plsql-parser/pom.xml      plsql-parser/
COPY flow-poc/pom.xml          flow-poc/
COPY mysql-parser/pom.xml      mysql-parser/
COPY sqlserver-parser/pom.xml  sqlserver-parser/
COPY db2-parser/pom.xml        db2-parser/
COPY postgres-parser/pom.xml   postgres-parser/
COPY dialect-common/pom.xml    dialect-common/
COPY plsql-analyzer-core/pom.xml plsql-analyzer-core/
COPY jar-analyzer-core/pom.xml jar-analyzer-core/
COPY analysis-mongo-ingest/pom.xml analysis-mongo-ingest/
COPY analyzer-storage/pom.xml  analyzer-storage/
COPY unified-web/pom.xml       unified-web/

# Best-effort dependency prefetch; a partial cache is fine, the real
# resolve happens during package below
RUN mvn -B -q dependency:go-offline -DskipTests || true

# Now copy sources and build the fat jar
COPY . .
RUN mvn -B -pl unified-web -am package -DskipTests

# ── Runtime stage: JRE only ──
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Single runtime tree — config/ and logs/ derive from DATA_DIR automatically
# (DirPropertyResolver), mirroring start.sh's directory layout
RUN mkdir -p /data/config/prompts /data/jar /data/plsql /data/plsql-parse \
             /data/cache-plsql /data/claude-chatbot /data/logs

COPY --from=build /build/unified-web/target/unified-web-1.0.0-SNAPSHOT.jar app.jar

ENV PORT=8083 \
    DATA_DIR=/data \
    JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC"

EXPOSE 8083
VOLUME /data

# Shell form so PORT / DATA_DIR / JAVA_OPTS are honored at run time
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar --server.port=${PORT} --app.data-dir=${DATA_DIR}"]
