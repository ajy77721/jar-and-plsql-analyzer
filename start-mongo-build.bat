@echo off
setlocal enabledelayedexpansion
title Unified Analyzer - Build and Start (MongoDB mode)

:: ═══════════════════════════════════════════════════════════════
::  Unified Analyzer — Build with Maven, then Start in MongoDB storage mode
::  Full mode: mvn package, then --app.storage.mode=mongodb.
::  config/, logs/, prompts/, claude-chatbot/ still live locally in every
::  mode; data/jar and data/plsql are skipped here since mongodb mode
::  never writes analysis artifacts to local disk.
:: ═══════════════════════════════════════════════════════════════

if "%CONFIG_DIR%"=="" set CONFIG_DIR=config
if "%DATA_DIR%"=="" set DATA_DIR=data
if "%PORT%"=="" set PORT=8083
if "%MONGO_URI%"=="" set MONGO_URI=mongodb://localhost:27017
if "%MONGO_DATABASE%"=="" set MONGO_DATABASE=analyzer
set JAR_FILE=unified-web\target\unified-web-1.0.0-SNAPSHOT.jar
set CLAUDE_OK=0

echo.
echo ===============================================================
echo   UNIFIED ANALYZER — Build ^& Start (MongoDB mode)
echo ===============================================================
echo.

:: ────────────────────────────────────────────────────────────────
:: CHECK 1: Java
:: ────────────────────────────────────────────────────────────────
echo [1/5] Checking Java...
java -version >nul 2>&1
if errorlevel 1 (
    echo.
    echo     JAVA NOT FOUND — Java 17+ is required.
    echo     Install: https://adoptium.net/
    echo.
    pause
    exit /b 1
)
for /f "tokens=3" %%v in ('java -version 2^>^&1 ^| findstr /i "version"') do set JAVA_VER=%%~v
echo        Java found: %JAVA_VER%

:: ────────────────────────────────────────────────────────────────
:: CHECK 2: Maven
:: ────────────────────────────────────────────────────────────────
echo [2/5] Checking Maven...
where mvn >nul 2>&1
if errorlevel 1 (
    echo.
    echo     MAVEN NOT FOUND — Maven 3.8+ is required to build.
    echo     Install: https://maven.apache.org/download.cgi
    echo     (Or use start.bat / a pre-built jar if you don't need to rebuild.)
    echo.
    pause
    exit /b 1
)
for /f "tokens=3" %%m in ('mvn --version 2^>^&1 ^| findstr /i "Apache Maven"') do set MVN_VER=%%m
echo        Maven found: %MVN_VER%

:: ────────────────────────────────────────────────────────────────
:: CHECK 3: Claude CLI (optional — warn if missing)
:: ────────────────────────────────────────────────────────────────
echo [3/5] Checking Claude CLI...
where claude >nul 2>&1
if errorlevel 1 (
    set CLAUDE_OK=0
    echo        WARNING: Claude CLI not found — AI features will be unavailable.
) else (
    set CLAUDE_OK=1
    for /f "tokens=*" %%c in ('claude --version 2^>^&1') do set CLAUDE_VER=%%c
    echo        Claude found: !CLAUDE_VER!
)

:: ────────────────────────────────────────────────────────────────
:: CHECK 4: MongoDB reachable (best-effort, does not block)
:: ────────────────────────────────────────────────────────────────
echo [4/5] Checking MongoDB reachability...
echo        (best-effort only — start will proceed regardless)

:: ────────────────────────────────────────────────────────────────
:: BUILD: stamp assets + mvn package
:: ────────────────────────────────────────────────────────────────
echo [5/5] Building with Maven...
echo.
echo ---------------------------------------------------------------
echo   mvn -pl unified-web -am package -DskipTests
echo ---------------------------------------------------------------
echo.

tasklist /FI "IMAGENAME eq java.exe" /FO CSV 2>nul | findstr /I "java.exe" >nul
if not errorlevel 1 (
    echo   NOTE: a java process is running. If it is the analyzer, stop it before building -
    echo         rebuilding now would corrupt it rather than update it.
    echo         Stop it with:  taskkill /F /IM java.exe
    echo.
    choice /C YN /M "Continue anyway"
    if errorlevel 2 exit /b 1
)

if exist scripts\stamp-assets.py (
    python scripts\stamp-assets.py
)

call mvn -pl unified-web -am package -DskipTests
if errorlevel 1 (
    echo.
    echo     BUILD FAILED — check the errors above.
    echo.
    pause
    exit /b 1
)

if not exist "%JAR_FILE%" (
    echo.
    echo  ERROR: Build succeeded but JAR not found at %JAR_FILE%
    echo.
    pause
    exit /b 1
)
echo.
echo   Build successful: %JAR_FILE%
echo.

:: ────────────────────────────────────────────────────────────────
:: SETUP: directories that are ALWAYS local, regardless of storage mode
:: data\jar and data\plsql are deliberately NOT created here — mongodb
:: mode never writes analysis artifacts to local disk.
:: ────────────────────────────────────────────────────────────────
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"
if not exist "%CONFIG_DIR%\prompts" mkdir "%CONFIG_DIR%\prompts"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%DATA_DIR%\claude-chatbot" mkdir "%DATA_DIR%\claude-chatbot"

:: ────────────────────────────────────────────────────────────────
:: JVM tuning (full mode — same as start.bat's larger-heap variant)
:: ────────────────────────────────────────────────────────────────
set JVM_OPTS=-Xms512m -Xmx8g -XX:MaxMetaspaceSize=512m -XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:+UseStringDeduplication -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DATA_DIR%/heap-dump.hprof

:: ────────────────────────────────────────────────────────────────
:: Application properties — MongoDB storage mode
:: ────────────────────────────────────────────────────────────────
set APP_OPTS=--server.port=%PORT%
set APP_OPTS=%APP_OPTS% --app.config-dir=%CONFIG_DIR%
set APP_OPTS=%APP_OPTS% --app.data-dir=%DATA_DIR%
set APP_OPTS=%APP_OPTS% --app.storage.mode=mongodb
set APP_OPTS=%APP_OPTS% --app.storage.mongodb.uri=%MONGO_URI%
set APP_OPTS=%APP_OPTS% --app.storage.mongodb.database=%MONGO_DATABASE%
set APP_OPTS=%APP_OPTS% --app.storage.entity-ingest.enabled=true
set APP_OPTS=%APP_OPTS% --app.storage.entity-read.enabled=true

:: ────────────────────────────────────────────────────────────────
:: LAUNCH
:: ────────────────────────────────────────────────────────────────
echo.
echo ===============================================================
echo   UNIFIED ANALYZER — Starting (MongoDB mode)
echo ===============================================================
echo.
echo   JAR:       %JAR_FILE%
echo   Port:      %PORT%
echo   Config:    %CONFIG_DIR%   (always local)
echo   Data:      %DATA_DIR%     (always local: logs/prompts/claude-chatbot only)
echo   Mongo:     %MONGO_URI% / %MONGO_DATABASE%
echo   Java:      %JAVA_VER%
echo   Maven:     %MVN_VER%
if "%CLAUDE_OK%"=="1" (
    echo   Claude:    !CLAUDE_VER!
) else (
    echo   Claude:    NOT AVAILABLE — static analysis only
)
echo.
echo ===============================================================
echo.
echo   Home:      http://localhost:%PORT%/
echo   JAR:       http://localhost:%PORT%/jar/
echo   WAR:       http://localhost:%PORT%/war/
echo   Parser:    http://localhost:%PORT%/parser/
echo.
echo ===============================================================
echo.

java %JVM_OPTS% -jar %JAR_FILE% %APP_OPTS% %*
