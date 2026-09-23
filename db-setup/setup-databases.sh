#!/usr/bin/env bash
# ============================================================================
# setup-databases.sh
# One-command bring-up of all 5 analyzer demo databases (Docker), seeded with
# the ERP sample schema/tables/procedures/data.
#
# Usage:   ./setup-databases.sh            # all 5 DBs
#          ./setup-databases.sh postgres   # one or more by name:
#                                          #   oracle mysql db2 sqlserver postgres
#
# Safe to re-run: existing containers are (re)started, seeds are idempotent.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEED_DIR="$SCRIPT_DIR/seed"

# ------------------------------------------------------------------ styling
info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[1;32m[ OK ]\033[0m  %s\n' "$*"; }
warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
err()   { printf '\033[1;31m[FAIL]\033[0m  %s\n' "$*" >&2; }

# Databases that failed to set up.
#
# `err` deliberately still returns 0: `set -e` is on and several call sites emit two messages before
# their own `exit 1`, so making it return non-zero would abort mid-sentence. That left the script
# reporting every failure and then exiting 0 — and print_summary went on to claim everything was
# seeded. Failures are recorded here instead, and main exits on them.
FAILED_TARGETS=()
step()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# ------------------------------------------------------------------ 1. docker on PATH
ensure_docker_binary() {
  if ! command -v docker >/dev/null 2>&1; then
    # macOS: Docker Desktop binary is often not on PATH
    local mac_bin="/Applications/Docker.app/Contents/Resources/bin"
    if [[ -x "$mac_bin/docker" ]]; then
      info "docker not on PATH; adding $mac_bin"
      export PATH="$mac_bin:$PATH"
    fi
  fi
  if ! command -v docker >/dev/null 2>&1; then
    err "Docker is not installed (or not found on PATH)."
    err "Install Docker Desktop (macOS/Windows) or the docker engine (Linux) and re-run."
    exit 1
  fi
  ok "docker found: $(command -v docker)"
}

# ------------------------------------------------------------------ 2. docker daemon running
ensure_docker_running() {
  if docker info >/dev/null 2>&1; then
    ok "Docker daemon is running."
    return
  fi
  warn "Docker daemon not running - attempting to start it..."
  local os="$(uname -s)"
  case "$os" in
    Darwin)  open -a Docker || true ;;
    Linux)   sudo systemctl start docker || true ;;
    *)       warn "Unknown OS '$os'; please start Docker manually." ;;
  esac
  info "Waiting for Docker daemon (up to 120s)..."
  local i=0
  while ! docker info >/dev/null 2>&1; do
    i=$((i+1))
    if [[ $i -ge 60 ]]; then
      err "Docker daemon did not become ready in time. Start Docker and re-run."
      exit 1
    fi
    sleep 2
  done
  ok "Docker daemon is running."
}

# ------------------------------------------------------------------ helpers
container_exists() { docker ps -a --format '{{.Names}}' | grep -qx "$1"; }
container_running() { docker ps --format '{{.Names}}' | grep -qx "$1"; }

# start existing container, or run a new one via the provided creator function
ensure_container() {
  local name="$1"; local creator="$2"
  if container_exists "$name"; then
    if container_running "$name"; then
      ok "Container '$name' already running."
    else
      info "Starting existing container '$name'..."
      docker start "$name" >/dev/null
      ok "Started '$name'."
    fi
  else
    info "Creating container '$name'..."
    "$creator"
    ok "Created '$name'."
  fi
}

# generic TCP port readiness fallback (host side)
wait_for_port() {
  local host="$1" port="$2" timeout="${3:-120}" i=0
  while ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    i=$((i+1)); [[ $i -ge $timeout ]] && return 1; sleep 1
  done
  exec 3>&- 2>/dev/null || true
  return 0
}

# wait until a docker-exec'd command succeeds
wait_for_cmd() {
  local name="$1" timeout="$2"; shift 2; local i=0
  while ! docker exec "$name" "$@" >/dev/null 2>&1; do
    i=$((i+1))
    if [[ $i -ge $timeout ]]; then return 1; fi
    sleep 2
  done
  return 0
}

# =====================================================================================
# PostgreSQL
# =====================================================================================
setup_postgres() {
  step "PostgreSQL (postgres16)"
  local NAME=postgres16
  create_postgres() {
    docker run -d --name "$NAME" --platform linux/amd64 \
      -e POSTGRES_PASSWORD='Password123!' \
      -e POSTGRES_DB=analyzerdb \
      -p 5432:5432 \
      postgres:16 >/dev/null
  }
  ensure_container "$NAME" create_postgres
  info "Waiting for PostgreSQL to accept connections..."
  if wait_for_cmd "$NAME" 60 pg_isready -U postgres -d analyzerdb; then
    ok "PostgreSQL ready."
  else
    warn "pg_isready timed out; trying TCP port check..."
    wait_for_port localhost 5432 60 || { err "PostgreSQL not reachable."; return 1; }
  fi
  # Seed + VERIFY + retry. pg_isready can report ready during initdb's bootstrap
  # phase (temp server), so a single seed can land on the wrong server and create
  # nothing. Re-run until the routines are actually present.
  info "Seeding PostgreSQL (verify + retry until routines land)..."
  local pg_ok=0 cnt
  for attempt in 1 2 3 4 5 6; do
    docker exec -i "$NAME" psql -v ON_ERROR_STOP=0 -U postgres -d analyzerdb < "$SEED_DIR/postgres.sql" >/dev/null 2>&1 || true
    cnt=$(docker exec "$NAME" psql -U postgres -d analyzerdb -tAc \
      "select count(*) from information_schema.routines where routine_schema not in ('pg_catalog','information_schema')" 2>/dev/null | tr -dc '0-9')
    if [[ "${cnt:-0}" -gt 0 ]]; then ok "PostgreSQL seeded ($cnt routines)."; pg_ok=1; break; fi
    warn "PostgreSQL not seeded yet (attempt $attempt) - init still finishing, retrying..."; sleep 6
  done
  [[ $pg_ok -eq 1 ]] || { err "PostgreSQL seed created no routines (check $SEED_DIR/postgres.sql)."; return 1; }
}

# =====================================================================================
# MySQL
# =====================================================================================
setup_mysql() {
  step "MySQL (mysql8)"
  local NAME=mysql8
  create_mysql() {
    docker run -d --name "$NAME" --platform linux/amd64 \
      -e MYSQL_ROOT_PASSWORD='Password123!' \
      -e MYSQL_DATABASE=analyzerdb \
      -p 3307:3306 \
      mysql:8 --log-bin-trust-function-creators=1 >/dev/null
  }
  ensure_container "$NAME" create_mysql
  info "Waiting for MySQL to accept connections..."
  if wait_for_cmd "$NAME" 90 mysqladmin ping -uroot -pPassword123! --silent; then
    ok "MySQL ready."
  else
    warn "mysqladmin ping timed out; trying TCP port check..."
    wait_for_port localhost 3307 60 || { err "MySQL not reachable."; return 1; }
  fi
  # Seed + VERIFY + retry. mysqladmin ping can report ready against the temporary
  # init server the entrypoint runs during first boot, so a single seed can be lost.
  # Re-run until the routines are actually present in analyzerdb.
  info "Seeding MySQL (verify + retry until routines land)..."
  local my_ok=0 cnt
  for attempt in 1 2 3 4 5 6; do
    # allow non-DBA function creation just in case log_bin is on
    docker exec -i "$NAME" mysql -uroot -pPassword123! \
      -e "SET GLOBAL log_bin_trust_function_creators = 1;" 2>/dev/null || true
    docker exec -i "$NAME" mysql -uroot -pPassword123! < "$SEED_DIR/mysql.sql" 2>/dev/null || true
    cnt=$(docker exec "$NAME" mysql -uroot -pPassword123! -N \
      -e "select count(*) from information_schema.routines where routine_schema='analyzerdb';" 2>/dev/null | tr -dc '0-9')
    if [[ "${cnt:-0}" -gt 0 ]]; then ok "MySQL seeded ($cnt routines)."; my_ok=1; break; fi
    warn "MySQL not seeded yet (attempt $attempt) - init still finishing, retrying..."; sleep 6
  done
  [[ $my_ok -eq 1 ]] || { err "MySQL seed created no routines (check $SEED_DIR/mysql.sql)."; return 1; }
}

# =====================================================================================
# Oracle Free
# =====================================================================================
setup_oracle() {
  step "Oracle Free (oracle-free)"
  local NAME=oracle-free
  create_oracle() {
    docker run -d --name "$NAME" --platform linux/amd64 \
      -e ORACLE_PASSWORD='Password123!' \
      -e APP_USER=analyzer \
      -e APP_USER_PASSWORD='Password123!' \
      -p 1521:1521 \
      gvenzl/oracle-free >/dev/null
  }
  ensure_container "$NAME" create_oracle
  info "Waiting for Oracle to open (this can take several minutes on first run)..."
  # gvenzl images expose 'healthcheck.sh'; fall back to sqlplus / port
  local ready=0 i=0
  while [[ $i -lt 150 ]]; do
    if docker exec "$NAME" bash -lc 'echo "SELECT 1 FROM dual;" | sqlplus -s system/Password123!@//localhost:1521/FREEPDB1' 2>/dev/null | grep -q '1'; then
      ready=1; break
    fi
    i=$((i+1)); sleep 4
  done
  if [[ $ready -eq 1 ]]; then ok "Oracle ready."; else
    warn "Oracle readiness check timed out; trying TCP port..."
    wait_for_port localhost 1521 60 || { err "Oracle not reachable."; return 1; }
  fi
  # Seed + VERIFY + retry until the entry procedure ERP_USP_RUN_CYCLE actually
  # exists in schema ANALYZER (same self-healing pattern as the other engines).
  info "Seeding Oracle (verify + retry until entry procedure lands)..."
  docker cp "$SEED_DIR/oracle.sql" "$NAME:/tmp/oracle.sql" >/dev/null
  local or_ok=0 cnt
  for attempt in 1 2 3 4 5 6; do
    docker exec "$NAME" bash -lc \
      "sqlplus -s analyzer/Password123!@//localhost:1521/FREEPDB1 @/tmp/oracle.sql" >/dev/null 2>&1 || true
    cnt=$(docker exec "$NAME" bash -lc \
      "echo \"set heading off feedback off pagesize 0 verify off; select count(*) from user_objects where object_name='ERP_USP_RUN_CYCLE' and object_type='PROCEDURE';\" | sqlplus -s analyzer/Password123!@//localhost:1521/FREEPDB1" 2>/dev/null | tr -dc '0-9')
    if [[ "${cnt:-0}" -gt 0 ]]; then ok "Oracle seeded (entry procedure ERP_USP_RUN_CYCLE present)."; or_ok=1; break; fi
    warn "Oracle entry procedure not present yet (attempt $attempt) - reseeding..."; sleep 6
  done
  [[ $or_ok -eq 1 ]] || err "Oracle seed did not create ERP_USP_RUN_CYCLE (check $SEED_DIR/oracle.sql)."
}

# =====================================================================================
# SQL Server
# =====================================================================================
setup_sqlserver() {
  step "SQL Server 2019 (sqlserver2019)"
  local NAME=sqlserver2019
  create_sqlserver() {
    docker run -d --name "$NAME" --platform linux/amd64 \
      -e ACCEPT_EULA=Y \
      -e MSSQL_SA_PASSWORD='Password123!' \
      -e SA_PASSWORD='Password123!' \
      -p 1433:1433 \
      mcr.microsoft.com/mssql/server:2019-latest >/dev/null
  }
  ensure_container "$NAME" create_sqlserver

  # locate sqlcmd inside the container (path differs across image versions)
  local SQLCMD=""
  info "Waiting for SQL Server to accept connections..."
  local i=0
  while [[ $i -lt 60 ]]; do
    for cand in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
      if docker exec "$NAME" test -x "$cand" 2>/dev/null; then
        if docker exec "$NAME" "$cand" -S localhost -U sa -P 'Password123!' -C -Q "SELECT 1" >/dev/null 2>&1; then
          SQLCMD="$cand"; break
        fi
      fi
    done
    [[ -n "$SQLCMD" ]] && break
    i=$((i+1)); sleep 3
  done
  if [[ -z "$SQLCMD" ]]; then
    warn "sqlcmd readiness timed out; trying TCP port..."
    wait_for_port localhost 1433 60 || { err "SQL Server not reachable."; return 1; }
    SQLCMD=/opt/mssql-tools18/bin/sqlcmd
  fi
  ok "SQL Server ready (sqlcmd: $SQLCMD)."
  # Seed + VERIFY + retry until the entry procedure erp.USP_RUNMRPCYCLE actually
  # exists in database analyzer-db (same self-healing pattern as the other engines).
  info "Seeding SQL Server (verify + retry until entry procedure lands)..."
  docker cp "$SEED_DIR/sqlserver.sql" "$NAME:/tmp/sqlserver.sql" >/dev/null
  local ss_ok=0 cnt
  for attempt in 1 2 3 4 5 6; do
    docker exec "$NAME" "$SQLCMD" -S localhost -U sa -P 'Password123!' -C -i /tmp/sqlserver.sql >/dev/null 2>&1 || true
    cnt=$(docker exec "$NAME" "$SQLCMD" -S localhost -U sa -P 'Password123!' -C -h -1 -W \
      -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [analyzer-db].sys.procedures p JOIN [analyzer-db].sys.schemas s ON p.schema_id=s.schema_id WHERE s.name='erp' AND p.name='USP_RUNMRPCYCLE'" 2>/dev/null | tr -dc '0-9')
    if [[ "${cnt:-0}" -gt 0 ]]; then ok "SQL Server seeded (entry procedure USP_RUNMRPCYCLE present)."; ss_ok=1; break; fi
    warn "SQL Server entry procedure not present yet (attempt $attempt) - reseeding..."; sleep 6
  done
  [[ $ss_ok -eq 1 ]] || err "SQL Server seed did not create erp.USP_RUNMRPCYCLE (check $SEED_DIR/sqlserver.sql)."
}

# =====================================================================================
# Db2
# =====================================================================================
setup_db2() {
  step "Db2 (db2)"
  local NAME=db2
  create_db2() {
    docker run -d --name "$NAME" --platform linux/amd64 --privileged=true \
      -e LICENSE=accept \
      -e DB2INST1_PASSWORD='Password123' \
      -e DBNAME=anlzdb \
      -p 50000:50000 \
      icr.io/db2_community/db2:11.5.9.0 >/dev/null
  }
  ensure_container "$NAME" create_db2
  info "Waiting for Db2 to initialise (this can take several minutes on first run)..."
  local ready=0 i=0
  while [[ $i -lt 150 ]]; do
    if docker exec "$NAME" su - db2inst1 -c "db2 connect to anlzdb" >/dev/null 2>&1; then
      ready=1; break
    fi
    i=$((i+1)); sleep 4
  done
  if [[ $ready -eq 1 ]]; then ok "Db2 ready."; else
    warn "Db2 readiness check timed out; trying TCP port..."
    wait_for_port localhost 50000 60 || { err "Db2 not reachable."; return 1; }
  fi
  # Seed + VERIFY + retry. Prepend CONNECT so it runs in the SAME db2 CLP process as
  # the file (a separate 'db2 connect' does NOT share its connection with a later
  # 'db2 -f' -> SQL1024N on every statement). A single pass can also miss procedures
  # whose dependencies are created later in the file; a second pass then compiles
  # them, so we re-run until the entry procedure ERP_USP_RUN_CYCLE actually exists.
  info "Seeding Db2 (verify + retry until entry procedure lands)..."
  docker cp "$SEED_DIR/db2.sql" "$NAME:/tmp/db2.sql" >/dev/null
  local db2_ok=0 cnt
  for attempt in 1 2 3 4 5 6; do
    docker exec "$NAME" su - db2inst1 -c \
      "{ echo 'CONNECT TO anlzdb@'; cat /tmp/db2.sql; } > /tmp/db2_seed.sql && db2 -td@ -f /tmp/db2_seed.sql" >/dev/null 2>&1 || true
    cnt=$(docker exec "$NAME" su - db2inst1 -c \
      "db2 connect to anlzdb >/dev/null 2>&1; db2 -x \"select count(*) from syscat.procedures where procschema='DB2INST1' and procname='ERP_USP_RUN_CYCLE'\"" 2>/dev/null | tr -dc '0-9')
    if [[ "${cnt:-0}" -gt 0 ]]; then ok "Db2 seeded (entry procedure ERP_USP_RUN_CYCLE present)."; db2_ok=1; break; fi
    warn "Db2 entry procedure not present yet (attempt $attempt) - reseeding..."; sleep 6
  done
  [[ $db2_ok -eq 1 ]] || err "Db2 seed did not create ERP_USP_RUN_CYCLE (check $SEED_DIR/db2.sql)."
}

# =====================================================================================
# summary
# =====================================================================================
print_summary() {
  step "SUMMARY - demo databases"
  printf '\n'
  printf '%-14s %-42s %-16s %-6s %-14s %-11s %-13s %-20s\n' \
    "CONTAINER" "IMAGE" "HOST" "PORT" "DB/SERVICE" "USER" "PASSWORD" "ENTRY PROCEDURE"
  printf '%s\n' "--------------------------------------------------------------------------------------------------------------------------------------------------------"
  printf '%-14s %-42s %-16s %-6s %-14s %-11s %-13s %-20s\n' \
    "oracle-free" "gvenzl/oracle-free" "localhost" "1521" "FREEPDB1" "analyzer" "Password123!" "ERP_USP_RUN_CYCLE"
  printf '%-14s %-42s %-16s %-6s %-14s %-11s %-13s %-20s\n' \
    "mysql8" "mysql:8" "localhost" "3307" "analyzerdb" "root" "Password123!" "ERP_USP_RUN_CYCLE"
  printf '%-14s %-42s %-16s %-6s %-14s %-11s %-13s %-20s\n' \
    "db2" "icr.io/db2_community/db2" "localhost" "50000" "anlzdb" "db2inst1" "Password123" "ERP_USP_RUN_CYCLE"
  printf '%-14s %-42s %-16s %-6s %-14s %-11s %-13s %-20s\n' \
    "sqlserver2019" "mcr.microsoft.com/mssql/server:2019-latest" "localhost" "1433" "analyzer-db" "sa" "Password123!" "USP_RUNMRPCYCLE"
  printf '%-14s %-42s %-16s %-6s %-14s %-11s %-13s %-20s\n' \
    "postgres16" "postgres:16" "localhost" "5432" "analyzerdb" "postgres" "Password123!" "usp_run_cycle"
  printf '\n'
  if [[ ${#FAILED_TARGETS[@]} -gt 0 ]]; then
    err "These did NOT set up successfully: ${FAILED_TARGETS[*]}"
    err "The rows above describe what was requested, not what is running."
  else
    ok "All requested databases are up and seeded. Point the analyzer's demo projects at the rows above."
  fi
}

# =====================================================================================
# main
# =====================================================================================
main() {
  step "Analyzer database setup"
  ensure_docker_binary
  ensure_docker_running

  local targets=("$@")
  if [[ ${#targets[@]} -eq 0 ]]; then
    targets=(oracle mysql db2 sqlserver postgres)
  fi

  for t in "${targets[@]}"; do
    case "$t" in
      oracle)    setup_oracle    || { err "Oracle setup failed."; FAILED_TARGETS+=("oracle"); } ;;
      mysql)     setup_mysql     || { err "MySQL setup failed."; FAILED_TARGETS+=("mysql"); } ;;
      db2)       setup_db2       || { err "Db2 setup failed."; FAILED_TARGETS+=("db2"); } ;;
      sqlserver) setup_sqlserver || { err "SQL Server setup failed."; FAILED_TARGETS+=("sqlserver"); } ;;
      postgres)  setup_postgres  || { err "PostgreSQL setup failed."; FAILED_TARGETS+=("postgres"); } ;;
      *)         warn "Unknown target '$t' (expected: oracle mysql db2 sqlserver postgres)" ;;
    esac
  done

  print_summary
  # Exit non-zero when anything failed, so a caller's `|| exit 1` can actually fire. setup.sh:61
  # has exactly that check and it could never trigger.
  [[ ${#FAILED_TARGETS[@]} -eq 0 ]]
}

main "$@"
