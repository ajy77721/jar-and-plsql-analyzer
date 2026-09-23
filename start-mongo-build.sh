#!/usr/bin/env bash
set -e

# ═══════════════════════════════════════════════════════════════
# Unified Analyzer — Build with Maven, then Start in MongoDB storage mode
# ═══════════════════════════════════════════════════════════════
#
# Full mode: builds the jar with `mvn package`, then launches with
# --app.storage.mode=mongodb (analysis artifacts go to MongoDB, not local
# disk — see docs/manual-properties.md "Storage Mode & Setup"). config/,
# logs/, prompts/, claude-chatbot/ still live locally in every mode; only
# data/jar and data/plsql are skipped here since mongodb mode never uses them.

# ── Paths (override via env or CLI: MONGO_URI=... ./start-mongo-build.sh) ──
CONFIG_DIR="${CONFIG_DIR:-config}"
DATA_DIR="${DATA_DIR:-data}"
JAR_FILE="unified-web/target/unified-web-1.0.0-SNAPSHOT.jar"
PORT="${PORT:-8083}"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
MONGO_DATABASE="${MONGO_DATABASE:-analyzer}"

echo "═══════════════════════════════════════════════════════════════"
echo " UNIFIED ANALYZER — Build & Start (MongoDB mode)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ── CHECK: Java ──
echo "[1/4] Checking Java..."
if ! command -v java >/dev/null 2>&1; then
    echo ""
    echo "ERROR: Java not found. Java 17+ is required."
    echo "Install: https://adoptium.net/"
    exit 1
fi
java -version 2>&1 | head -1

# ── CHECK: Maven ──
echo "[2/4] Checking Maven..."
if ! command -v mvn >/dev/null 2>&1; then
    echo ""
    echo "ERROR: Maven not found. Maven 3.8+ is required to build."
    echo "Install: https://maven.apache.org/download.cgi"
    echo "(Or use start.sh / a pre-built jar if you don't need to rebuild.)"
    exit 1
fi
mvn --version 2>&1 | head -1

# ── CHECK: MongoDB reachable ──
echo "[3/4] Checking MongoDB reachability at ${MONGO_URI}..."
MONGO_HOSTPORT=$(echo "$MONGO_URI" | sed -E 's#^mongodb://##; s#^.*@##; s#/.*$##; s#,.*$##')
MONGO_HOST=$(echo "$MONGO_HOSTPORT" | cut -d: -f1)
MONGO_PORT=$(echo "$MONGO_HOSTPORT" | cut -d: -f2)
MONGO_PORT=${MONGO_PORT:-27017}
if command -v nc >/dev/null 2>&1 && ! nc -z -w2 "$MONGO_HOST" "$MONGO_PORT" 2>/dev/null; then
    echo ""
    echo "WARNING: could not reach ${MONGO_HOST}:${MONGO_PORT}. The app will still start,"
    echo "         but MongoDB-backed storage calls will fail until it's reachable."
    echo ""
else
    echo "       MongoDB reachable at ${MONGO_HOST}:${MONGO_PORT}"
fi

# ── BUILD: stamp assets + mvn package ──
echo "[4/4] Building with Maven..."
echo ""
echo "---------------------------------------------------------------"
echo "  python3 scripts/stamp-assets.py && mvn -pl unified-web -am package -DskipTests"
echo "---------------------------------------------------------------"
echo ""

# Stop any running instance first — replacing the jar under a live JVM leaves it unable to
# load classes it hasn't already touched (including its own logging classes), so failures go
# unreported and requests hang. See docs/audit/LOGBACK-NOCLASSDEF.md.
RUNNING_PID=$(lsof -ti:"$PORT" 2>/dev/null || true)
if [ -n "$RUNNING_PID" ]; then
    echo "NOTE: a process is already listening on port ${PORT} (pid ${RUNNING_PID})."
    echo "      Stop it before rebuilding, or it will be replaced under a live JVM."
    read -p "Kill it and continue? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kill -9 "$RUNNING_PID"
        sleep 1
    else
        exit 1
    fi
fi

if [ -f scripts/stamp-assets.py ]; then
    python3 scripts/stamp-assets.py
fi
mvn -pl unified-web -am package -DskipTests

if [ ! -f "$JAR_FILE" ]; then
    echo ""
    echo "ERROR: Build succeeded but $JAR_FILE not found."
    exit 1
fi
echo ""
echo "  Build successful: $JAR_FILE"
echo ""

# ── SETUP: directories that are ALWAYS local, regardless of storage mode ──
# data/jar and data/plsql are deliberately NOT created here — in mongodb mode
# ConfigDirService skips them too, since analysis artifacts live in Mongo, not disk.
mkdir -p "${CONFIG_DIR}/prompts"
mkdir -p "${DATA_DIR}/claude-chatbot"

# ── JVM tuning (full mode — same as start.sh) ──
JVM_OPTS="-Xms512m -Xmx8g -XX:MaxMetaspaceSize=512m -XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:+UseStringDeduplication -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${DATA_DIR}/heap-dump.hprof"

# ── Application properties — MongoDB storage mode ──
APP_OPTS="--server.port=${PORT} --app.config-dir=${CONFIG_DIR} --app.data-dir=${DATA_DIR}"
APP_OPTS="${APP_OPTS} --app.storage.mode=mongodb"
APP_OPTS="${APP_OPTS} --app.storage.mongodb.uri=${MONGO_URI}"
APP_OPTS="${APP_OPTS} --app.storage.mongodb.database=${MONGO_DATABASE}"
APP_OPTS="${APP_OPTS} --app.storage.entity-ingest.enabled=true"
APP_OPTS="${APP_OPTS} --app.storage.entity-read.enabled=true"

# ── LAUNCH ──
echo "═══════════════════════════════════════════════════════════════"
echo " Unified Analyzer — MongoDB mode"
echo "═══════════════════════════════════════════════════════════════"
echo " JAR:      $JAR_FILE"
echo " Port:     $PORT"
echo " Config:   $CONFIG_DIR   (always local)"
echo " Data:     $DATA_DIR     (always local: logs/prompts/claude-chatbot only)"
echo " Mongo:    $MONGO_URI / $MONGO_DATABASE"
echo " JVM:      $JVM_OPTS"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo " Home:       http://localhost:${PORT}/"
echo " JAR:        http://localhost:${PORT}/jar/"
echo " WAR:        http://localhost:${PORT}/war/"
echo " PL/SQL:     http://localhost:${PORT}/plsql/"
echo " Parser:     http://localhost:${PORT}/parser/"
echo "═══════════════════════════════════════════════════════════════"
echo ""

exec java $JVM_OPTS -jar "$JAR_FILE" $APP_OPTS "$@"
