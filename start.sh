#!/usr/bin/env bash
set -e

# ═══════════════════════════════════════════════════════════════
# Unified Analyzer — JAR + PL/SQL + PL/SQL Parser
# ═══════════════════════════════════════════════════════════════

# ── Paths (override via command line: ./start.sh --server.port=9090) ──
CONFIG_DIR="${CONFIG_DIR:-config}"
DATA_DIR="${DATA_DIR:-data}"
JAR_FILE="unified-web/target/unified-web-1.0.0-SNAPSHOT.jar"
PORT="${PORT:-8083}"

# ── JVM tuning ──
JVM_OPTS="-Xms512m -Xmx4g -XX:MaxMetaspaceSize=256m -XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:+UseStringDeduplication -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${DATA_DIR}/heap-dump.hprof"

# ── Application properties ──
APP_OPTS="--server.port=${PORT} --app.config-dir=${CONFIG_DIR} --app.data-dir=${DATA_DIR}"

# ── Ensure required directories exist ──
mkdir -p "${CONFIG_DIR}/prompts"
mkdir -p "${DATA_DIR}/jar"
mkdir -p "${DATA_DIR}/plsql"
mkdir -p "${DATA_DIR}/plsql-parse"
mkdir -p "${DATA_DIR}/cache-plsql"
mkdir -p "${DATA_DIR}/claude-chatbot"

# ── Check JAR exists ──
if [ ! -f "$JAR_FILE" ]; then
    echo ""
    echo "ERROR: $JAR_FILE not found."
    echo "Run: mvn clean package -DskipTests"
    echo ""
    exit 1
fi

# ── Launch ──
echo "═══════════════════════════════════════════════════════════════"
echo " Unified Analyzer"
echo "═══════════════════════════════════════════════════════════════"
echo " JAR:     $JAR_FILE"
echo " Port:    $PORT"
echo " Config:  $CONFIG_DIR"
echo " Data:    $DATA_DIR"
echo " JVM:     $JVM_OPTS"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo " Home:       http://localhost:${PORT}/"
echo " JAR:        http://localhost:${PORT}/jar/"
echo " PL/SQL:     http://localhost:${PORT}/plsql/"
echo " Parser:     http://localhost:${PORT}/parser/"
echo "═══════════════════════════════════════════════════════════════"
echo ""

exec java $JVM_OPTS -jar "$JAR_FILE" $APP_OPTS "$@"
