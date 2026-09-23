@echo off
setlocal enabledelayedexpansion
title Unified Analyzer - Start

:: ═══════════════════════════════════════════════════════════════
::  Unified Analyzer — Start from pre-built JAR
::  Validates: Java, JAR file, Claude CLI
:: ═══════════════════════════════════════════════════════════════

set CONFIG_DIR=config
set DATA_DIR=data
set JAR_FILE=unified-web\target\unified-web-1.0.0-SNAPSHOT.jar
set PORT=8083
set CLAUDE_OK=0

echo.
echo ===============================================================
echo   UNIFIED ANALYZER — Pre-flight Checks
echo ===============================================================
echo.

:: ────────────────────────────────────────────────────────────────
:: CHECK 1: Java
:: ────────────────────────────────────────────────────────────────
echo [1/3] Checking Java...
java -version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  ============================================================
    echo.
    echo     JAVA NOT FOUND
    echo.
    echo     Java is required to run this application.
    echo     Install Java 17+ and add it to your PATH.
    echo.
    echo     Download: https://adoptium.net/
    echo.
    echo  ============================================================
    echo.
    pause
    exit /b 1
)
for /f "tokens=3" %%v in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    set JAVA_VER=%%~v
)
echo        Java found: %JAVA_VER%

:: ────────────────────────────────────────────────────────────────
:: CHECK 2: JAR file
:: ────────────────────────────────────────────────────────────────
echo [2/3] Checking application JAR...
if not exist "%JAR_FILE%" (
    echo.
    echo  ============================================================
    echo.
    echo     JAR FILE NOT FOUND
    echo.
    echo     Expected: %JAR_FILE%
    echo.
    echo     Either:
    echo       1. Run start-mvn-build.bat to build first
    echo       2. Run: mvn clean package -DskipTests
    echo.
    echo  ============================================================
    echo.
    pause
    exit /b 1
)
for %%F in ("%JAR_FILE%") do set JAR_SIZE=%%~zF
set /a JAR_MB=%JAR_SIZE% / 1048576
echo        JAR found: %JAR_FILE% (%JAR_MB% MB)

:: ────────────────────────────────────────────────────────────────
:: CHECK 3: Claude CLI (optional — warn if missing)
:: ────────────────────────────────────────────────────────────────
echo [3/3] Checking Claude CLI...
where claude >nul 2>&1
if errorlevel 1 (
    set CLAUDE_OK=0
    echo.
    echo  ============================================================
    echo.
    echo     WARNING: Claude CLI not found in PATH
    echo.
    echo     The following features will NOT work:
    echo       - Claude AI enrichment for JAR analysis
    echo       - Claude AI verification for PL/SQL analysis
    echo       - AI chatbot conversations
    echo       - Claude corrections and insights
    echo.
    echo     Static analysis will work normally.
    echo.
    echo     To enable AI features, install Claude CLI:
    echo       npm install -g @anthropic-ai/claude-code
    echo.
    echo  ============================================================
    echo.
) else (
    set CLAUDE_OK=1
    for /f "tokens=*" %%c in ('claude --version 2^>^&1') do set CLAUDE_VER=%%c
    echo        Claude found: !CLAUDE_VER!
)

:: ────────────────────────────────────────────────────────────────
:: SETUP: Directories
:: ────────────────────────────────────────────────────────────────
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"
if not exist "%CONFIG_DIR%\prompts" mkdir "%CONFIG_DIR%\prompts"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%DATA_DIR%\jar" mkdir "%DATA_DIR%\jar"
if not exist "%DATA_DIR%\plsql" mkdir "%DATA_DIR%\plsql"
if not exist "%DATA_DIR%\plsql-parse" mkdir "%DATA_DIR%\plsql-parse"
if not exist "%DATA_DIR%\cache-plsql" mkdir "%DATA_DIR%\cache-plsql"
if not exist "%DATA_DIR%\claude-chatbot" mkdir "%DATA_DIR%\claude-chatbot"

:: ────────────────────────────────────────────────────────────────
:: JVM tuning
:: ────────────────────────────────────────────────────────────────
set JVM_OPTS=-Xms512m -Xmx4g -XX:MaxMetaspaceSize=256m -XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:+UseStringDeduplication -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DATA_DIR%/heap-dump.hprof

:: ────────────────────────────────────────────────────────────────
:: Application properties
:: ────────────────────────────────────────────────────────────────
set APP_OPTS=--server.port=%PORT%
set APP_OPTS=%APP_OPTS% --app.config-dir=%CONFIG_DIR%
set APP_OPTS=%APP_OPTS% --app.data-dir=%DATA_DIR%

:: ────────────────────────────────────────────────────────────────
:: LAUNCH
:: ────────────────────────────────────────────────────────────────
echo.
echo ===============================================================
echo   UNIFIED ANALYZER — Starting
echo ===============================================================
echo.
echo   JAR:       %JAR_FILE%
echo   Port:      %PORT%
echo   Config:    %CONFIG_DIR%
echo   Data:      %DATA_DIR%
echo   Java:      %JAVA_VER%
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
echo   Parser:    http://localhost:%PORT%/parser/
echo   Manual:    http://localhost:%PORT%/  (User Manual card)
echo.
echo ===============================================================
echo.

java %JVM_OPTS% -jar %JAR_FILE% %APP_OPTS% %*
