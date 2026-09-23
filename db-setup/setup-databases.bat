@echo off
REM ============================================================================
REM setup-databases.bat  (Windows)
REM One-command bring-up of all 5 analyzer demo databases (Docker), seeded with
REM the ERP sample schema/tables/procedures/data.
REM
REM Usage:   setup-databases.bat              (all 5 DBs)
REM          setup-databases.bat postgres     (one or more of:
REM                                            oracle mysql db2 sqlserver postgres)
REM
REM Safe to re-run: existing containers are (re)started, seeds are idempotent.
REM ============================================================================
setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "SEED_DIR=%SCRIPT_DIR%seed"

echo.
echo ==^> Analyzer database setup

REM ------------------------------------------------------------ 1. docker binary
where docker >nul 2>&1
if errorlevel 1 (
  if exist "%ProgramFiles%\Docker\Docker\resources\bin\docker.exe" (
    set "PATH=%ProgramFiles%\Docker\Docker\resources\bin;%PATH%"
  )
)
where docker >nul 2>&1
if errorlevel 1 (
  echo [FAIL] Docker is not installed or not on PATH. Install Docker Desktop and re-run.
  exit /b 1
)
echo [ OK ] docker found.

REM ------------------------------------------------------------ 2. docker daemon
docker info >nul 2>&1
if errorlevel 1 (
  echo [WARN] Docker daemon not running - attempting to start Docker Desktop...
  start "" "%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
  echo [INFO] Waiting for Docker daemon ^(up to 120s^)...
  set /a _i=0
  :WAIT_DAEMON
  docker info >nul 2>&1
  if not errorlevel 1 goto DAEMON_OK
  set /a _i+=1
  if !_i! GEQ 60 (
    echo [FAIL] Docker daemon did not become ready. Start Docker and re-run.
    exit /b 1
  )
  timeout /t 2 /nobreak >nul
  goto WAIT_DAEMON
)
:DAEMON_OK
echo [ OK ] Docker daemon is running.

REM ------------------------------------------------------------ targets
set "TARGETS=%*"
if "%TARGETS%"=="" set "TARGETS=oracle mysql db2 sqlserver postgres"

for %%T in (%TARGETS%) do (
  if /i "%%T"=="oracle"    call :SETUP_ORACLE
  if /i "%%T"=="mysql"     call :SETUP_MYSQL
  if /i "%%T"=="db2"       call :SETUP_DB2
  if /i "%%T"=="sqlserver" call :SETUP_SQLSERVER
  if /i "%%T"=="postgres"  call :SETUP_POSTGRES
)

call :PRINT_SUMMARY
endlocal
exit /b 0

REM ============================================================================
REM helper: ensure a container exists (start) or create it via label CREATE_%1
REM ============================================================================
:ENSURE_CONTAINER
REM %1 = container name, %2 = :label to call for creation
docker ps -a --format "{{.Names}}" | findstr /x "%~1" >nul 2>&1
if not errorlevel 1 (
  docker ps --format "{{.Names}}" | findstr /x "%~1" >nul 2>&1
  if not errorlevel 1 (
    echo [ OK ] Container '%~1' already running.
  ) else (
    echo [INFO] Starting existing container '%~1'...
    docker start %~1 >nul
    echo [ OK ] Started '%~1'.
  )
) else (
  echo [INFO] Creating container '%~1'...
  call %~2
  echo [ OK ] Created '%~1'.
)
goto :eof

REM ============================================================================
:SETUP_POSTGRES
echo.
echo ==^> PostgreSQL (postgres16)
call :ENSURE_CONTAINER postgres16 :CREATE_POSTGRES
echo [INFO] Waiting for PostgreSQL...
set /a _i=0
:PG_WAIT
docker exec postgres16 pg_isready -U postgres -d analyzerdb >nul 2>&1
if not errorlevel 1 goto PG_READY
set /a _i+=1
if !_i! GEQ 60 ( echo [WARN] PostgreSQL wait timed out. & goto PG_SEED )
timeout /t 2 /nobreak >nul
goto PG_WAIT
:PG_READY
echo [ OK ] PostgreSQL ready.
:PG_SEED
echo [INFO] Seeding PostgreSQL (verify + retry until routines land)...
set /a _a=0
:PG_SEED_LOOP
docker exec -i postgres16 psql -v ON_ERROR_STOP=0 -U postgres -d analyzerdb < "%SEED_DIR%\postgres.sql" >nul 2>&1
for /f %%N in ('docker exec postgres16 psql -U postgres -d analyzerdb -tAc "select count(*) from information_schema.routines where routine_schema not in ('pg_catalog','information_schema')" 2^>nul') do set "_PGCNT=%%N"
echo %_PGCNT%| findstr /r "^[1-9][0-9]*$" >nul && ( echo [ OK ] PostgreSQL seeded ^(%_PGCNT% routines^). & goto :eof )
set /a _a+=1
if !_a! GEQ 6 ( echo [WARN] PostgreSQL seed created no routines. & goto :eof )
echo [WARN] PostgreSQL not seeded yet ^(attempt !_a!^) - retrying...
timeout /t 6 /nobreak >nul
goto PG_SEED_LOOP
:CREATE_POSTGRES
docker run -d --platform linux/amd64 --name postgres16 -e POSTGRES_PASSWORD=Password123! -e POSTGRES_DB=analyzerdb -p 5432:5432 postgres:16 >nul
goto :eof

REM ============================================================================
:SETUP_MYSQL
echo.
echo ==^> MySQL (mysql8)
call :ENSURE_CONTAINER mysql8 :CREATE_MYSQL
echo [INFO] Waiting for MySQL...
set /a _i=0
:MY_WAIT
docker exec mysql8 mysqladmin ping -uroot -pPassword123! --silent >nul 2>&1
if not errorlevel 1 goto MY_READY
set /a _i+=1
if !_i! GEQ 90 ( echo [WARN] MySQL wait timed out. & goto MY_SEED )
timeout /t 2 /nobreak >nul
goto MY_WAIT
:MY_READY
echo [ OK ] MySQL ready.
:MY_SEED
echo [INFO] Seeding MySQL (verify + retry until routines land)...
set /a _a=0
:MY_SEED_LOOP
docker exec mysql8 mysql -uroot -pPassword123! -e "SET GLOBAL log_bin_trust_function_creators = 1;" 2>nul
docker exec -i mysql8 mysql -uroot -pPassword123! < "%SEED_DIR%\mysql.sql" 2>nul
for /f %%N in ('docker exec mysql8 mysql -uroot -pPassword123! -N -e "select count(*) from information_schema.routines where routine_schema='analyzerdb';" 2^>nul') do set "_MYCNT=%%N"
echo %_MYCNT%| findstr /r "^[1-9][0-9]*$" >nul && ( echo [ OK ] MySQL seeded ^(%_MYCNT% routines^). & goto :eof )
set /a _a+=1
if !_a! GEQ 6 ( echo [WARN] MySQL seed created no routines. & goto :eof )
echo [WARN] MySQL not seeded yet ^(attempt !_a!^) - retrying...
timeout /t 6 /nobreak >nul
goto MY_SEED_LOOP
:CREATE_MYSQL
docker run -d --platform linux/amd64 --name mysql8 -e MYSQL_ROOT_PASSWORD=Password123! -e MYSQL_DATABASE=analyzerdb -p 3307:3306 mysql:8 --log-bin-trust-function-creators=1 >nul
goto :eof

REM ============================================================================
:SETUP_ORACLE
echo.
echo ==^> Oracle Free (oracle-free)
call :ENSURE_CONTAINER oracle-free :CREATE_ORACLE
echo [INFO] Waiting for Oracle (can take several minutes on first run)...
set /a _i=0
:OR_WAIT
docker exec oracle-free bash -lc "echo 'SELECT 1 FROM dual;' | sqlplus -s system/Password123!@//localhost:1521/FREEPDB1" 2>nul | findstr /r "1" >nul 2>&1
if not errorlevel 1 goto OR_READY
set /a _i+=1
if !_i! GEQ 150 ( echo [WARN] Oracle wait timed out. & goto OR_SEED )
timeout /t 4 /nobreak >nul
goto OR_WAIT
:OR_READY
echo [ OK ] Oracle ready.
:OR_SEED
echo [INFO] Seeding Oracle (verify + retry until entry procedure lands)...
docker cp "%SEED_DIR%\oracle.sql" oracle-free:/tmp/oracle.sql >nul
set /a _a=0
:OR_SEED_LOOP
docker exec oracle-free bash -lc "sqlplus -s analyzer/Password123!@//localhost:1521/FREEPDB1 @/tmp/oracle.sql" >nul 2>&1
for /f %%N in ('docker exec oracle-free bash -lc "echo \"set heading off feedback off pagesize 0 verify off; select count(*) from user_objects where object_name='ERP_USP_RUN_CYCLE' and object_type='PROCEDURE';\" ^| sqlplus -s analyzer/Password123!@//localhost:1521/FREEPDB1" 2^>nul') do set "_ORCNT=%%N"
echo %_ORCNT%| findstr /r "^[1-9][0-9]*$" >nul && ( echo [ OK ] Oracle seeded ^(entry procedure ERP_USP_RUN_CYCLE present^). & goto :eof )
set /a _a+=1
if !_a! GEQ 6 ( echo [WARN] Oracle seed did not create ERP_USP_RUN_CYCLE. & goto :eof )
echo [WARN] Oracle entry procedure not present yet ^(attempt !_a!^) - reseeding...
timeout /t 6 /nobreak >nul
goto OR_SEED_LOOP
:CREATE_ORACLE
docker run -d --platform linux/amd64 --name oracle-free -e ORACLE_PASSWORD=Password123! -e APP_USER=analyzer -e APP_USER_PASSWORD=Password123! -p 1521:1521 gvenzl/oracle-free >nul
goto :eof

REM ============================================================================
:SETUP_SQLSERVER
echo.
echo ==^> SQL Server 2019 (sqlserver2019)
call :ENSURE_CONTAINER sqlserver2019 :CREATE_SQLSERVER
echo [INFO] Waiting for SQL Server...
set "SQLCMD=/opt/mssql-tools18/bin/sqlcmd"
set /a _i=0
:SS_WAIT
docker exec sqlserver2019 %SQLCMD% -S localhost -U sa -P Password123! -C -Q "SELECT 1" >nul 2>&1
if not errorlevel 1 goto SS_READY
docker exec sqlserver2019 /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Password123! -Q "SELECT 1" >nul 2>&1
if not errorlevel 1 ( set "SQLCMD=/opt/mssql-tools/bin/sqlcmd" & goto SS_READY )
set /a _i+=1
if !_i! GEQ 60 ( echo [WARN] SQL Server wait timed out. & goto SS_SEED )
timeout /t 3 /nobreak >nul
goto SS_WAIT
:SS_READY
echo [ OK ] SQL Server ready.
:SS_SEED
echo [INFO] Seeding SQL Server (verify + retry until entry procedure lands)...
docker cp "%SEED_DIR%\sqlserver.sql" sqlserver2019:/tmp/sqlserver.sql >nul
set /a _a=0
:SS_SEED_LOOP
docker exec sqlserver2019 %SQLCMD% -S localhost -U sa -P Password123! -C -i /tmp/sqlserver.sql >nul 2>&1
for /f %%N in ('docker exec sqlserver2019 %SQLCMD% -S localhost -U sa -P Password123! -C -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [analyzer-db].sys.procedures p JOIN [analyzer-db].sys.schemas s ON p.schema_id=s.schema_id WHERE s.name='erp' AND p.name='USP_RUNMRPCYCLE'" 2^>nul') do set "_SSCNT=%%N"
echo %_SSCNT%| findstr /r "^[1-9][0-9]*$" >nul && ( echo [ OK ] SQL Server seeded ^(entry procedure USP_RUNMRPCYCLE present^). & goto :eof )
set /a _a+=1
if !_a! GEQ 6 ( echo [WARN] SQL Server seed did not create erp.USP_RUNMRPCYCLE. & goto :eof )
echo [WARN] SQL Server entry procedure not present yet ^(attempt !_a!^) - reseeding...
timeout /t 6 /nobreak >nul
goto SS_SEED_LOOP
:CREATE_SQLSERVER
docker run -d --platform linux/amd64 --name sqlserver2019 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Password123! -e SA_PASSWORD=Password123! -p 1433:1433 mcr.microsoft.com/mssql/server:2019-latest >nul
goto :eof

REM ============================================================================
:SETUP_DB2
echo.
echo ==^> Db2 (db2)
call :ENSURE_CONTAINER db2 :CREATE_DB2
echo [INFO] Waiting for Db2 (can take several minutes on first run)...
set /a _i=0
:D2_WAIT
docker exec db2 su - db2inst1 -c "db2 connect to anlzdb" >nul 2>&1
if not errorlevel 1 goto D2_READY
set /a _i+=1
if !_i! GEQ 150 ( echo [WARN] Db2 wait timed out. & goto D2_SEED )
timeout /t 4 /nobreak >nul
goto D2_WAIT
:D2_READY
echo [ OK ] Db2 ready.
:D2_SEED
echo [INFO] Seeding Db2 (verify + retry until entry procedure lands)...
docker cp "%SEED_DIR%\db2.sql" db2:/tmp/db2.sql >nul
set /a _a=0
:D2_SEED_LOOP
docker exec db2 su - db2inst1 -c "{ echo 'CONNECT TO anlzdb@'; cat /tmp/db2.sql; } > /tmp/db2_seed.sql && db2 -td@ -f /tmp/db2_seed.sql" >nul 2>&1
for /f %%N in ('docker exec db2 su - db2inst1 -c "db2 connect to anlzdb >/dev/null 2>&1; db2 -x \"select count(*) from syscat.procedures where procschema='DB2INST1' and procname='ERP_USP_RUN_CYCLE'\"" 2^>nul') do set "_D2CNT=%%N"
echo %_D2CNT%| findstr /r "^[1-9][0-9]*$" >nul && ( echo [ OK ] Db2 seeded ^(entry procedure ERP_USP_RUN_CYCLE present^). & goto :eof )
set /a _a+=1
if !_a! GEQ 6 ( echo [WARN] Db2 seed did not create ERP_USP_RUN_CYCLE. & goto :eof )
echo [WARN] Db2 entry procedure not present yet ^(attempt !_a!^) - reseeding...
timeout /t 6 /nobreak >nul
goto D2_SEED_LOOP
:CREATE_DB2
docker run -d --platform linux/amd64 --name db2 --privileged=true -e LICENSE=accept -e DB2INST1_PASSWORD=Password123 -e DBNAME=anlzdb -p 50000:50000 icr.io/db2_community/db2:11.5.9.0 >nul
goto :eof

REM ============================================================================
:PRINT_SUMMARY
echo.
echo ==^> SUMMARY - demo databases
echo.
echo CONTAINER      IMAGE                                        HOST       PORT   DB/SERVICE    USER       PASSWORD      ENTRY PROCEDURE
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo oracle-free    gvenzl/oracle-free                           localhost  1521   FREEPDB1      analyzer   Password123!  ERP_USP_RUN_CYCLE
echo mysql8         mysql:8                                      localhost  3307   analyzerdb    root       Password123!  ERP_USP_RUN_CYCLE
echo db2            icr.io/db2_community/db2                     localhost  50000  anlzdb        db2inst1   Password123   ERP_USP_RUN_CYCLE
echo sqlserver2019  mcr.microsoft.com/mssql/server:2019-latest   localhost  1433   analyzer-db   sa         Password123!  USP_RUNMRPCYCLE
echo postgres16     postgres:16                                  localhost  5432   analyzerdb    postgres   Password123!  usp_run_cycle
echo.
echo [ OK ] All requested databases are up and seeded.
goto :eof
