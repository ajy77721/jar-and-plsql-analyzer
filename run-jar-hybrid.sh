#!/usr/bin/env bash
set -euo pipefail

# Launches the app with BOTH features in S3 storage mode:
#   - EVERY artifact file (sources, analysis JSONs, uploaded JARs, everything) -> S3
#   - Mongo holds ONLY the am_* detail collections, populated by entity-ingest
#     (app.storage.entity-ingest.enabled=true in application.properties)
# parser.mode and jar.mode stay SEPARATE properties — change one without the other.
#run-jar-hybrid.sh
# Nothing secret is hardcoded here. Set these in your shell BEFORE running
# this script:
#   AWS_PROFILE               AWS credentials profile with S3 read/write on the bucket below
#   APP_STORAGE_MONGODB_URI   MongoDB connection string (credentials embedded in the URI)
#
# Optional:
#   AWS_DEFAULT_REGION        defaults to us-east-1 if unset
#   APP_PORT                  defaults to 8083 if unset

: "${AWS_PROFILE:Services.User-331472312345?Set AWS_PROFILE to a credentials profile with S3 write access before running this script}"
#: "${APP_STORAGE_MONGODB_URI:mongodb://localhost:27017/?Set APP_STORAGE_MONGODB_URI to the MongoDB connection string before running this script}"

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
APP_PORT="${APP_PORT:-8083}"

cd "$(dirname "$0")"
mvn clean install -DskipTests
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port="$APP_PORT" \
  --app.config-dir=config \
  --app.data-dir=data \
  --app.storage.jar.mode=s3 \
  --app.storage.parser.mode=s3 \
  --app.storage.s3.bucket=amp-analyzer-parse-jar \
  --app.storage.s3.region="$AWS_DEFAULT_REGION" \
  --app.storage.mongodb.prefix=amp-analyzer/data \
  --app.storage.mongodb.database=amp-parse \
  --app.storage.hybrid.big-threshold=1048576 \
  --app.storage.mongodb.inline-threshold=1048576 \
  --app.storage.mongodb.uri="mongodb://localhost:27017/" \
  > "data/app-${APP_PORT}-jar-hybrid.log" 2>&1 &

echo "Started PID $!, logging to data/app-${APP_PORT}-jar-hybrid.log"
