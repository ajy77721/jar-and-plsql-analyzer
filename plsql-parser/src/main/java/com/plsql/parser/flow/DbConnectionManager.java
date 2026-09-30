package com.plsql.parser.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages Oracle JDBC connections to all configured schemas.
 *
 * Connection model:
 *   - Per DB user: max POOL_SIZE_PER_USER (3) live connections.
 *   - Total max = 3 users × 3 = 9 connections.
 *   - Callers MUST call returnToPool(conn) on success or closeAndDiscard(conn) on failure.
 *   - getAnyConnection() blocks (up to ACQUIRE_TIMEOUT_MS) if all slots for a user are busy.
 *   - A failed/broken connection is always discarded; a fresh one is opened in its place.
 */
public class DbConnectionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DbConnectionManager.class);

    private final String jdbcUrl;
    private final List<DbUser> dbUsers;
    private final String dbType;
    /**
     * Optional TLS properties parsed from the generated env YAML's {@code ssl_properties:}
     * mapping (written by AnalysisService from the source connection's
     * {@code ConnectionConfig.buildSslProperties(configDir)}). Empty when the source connection
     * has TLS disabled, in which case it is not merged at all. These are merged into every
     * connection's Properties AFTER user/password and timeouts, so the driver sees the truststore/
     * wallet/client-cert settings at connect time.
     */
    private final Properties sslProperties = new Properties();
    private final AtomicInteger dbCallCount = new AtomicInteger(0);

    // Max connections per DB user.  3 users × 3 = 9 total.
    private static final int POOL_SIZE_PER_USER = 3;
    // How long to wait for a free slot before giving up (ms)
    private static final long ACQUIRE_TIMEOUT_MS = 30_000;

    // Per-user pool: username → queue of idle connections
    private final Map<String, ArrayBlockingQueue<Connection>> userPools = new ConcurrentHashMap<>();
    // Per-user semaphore: controls total live connections (idle + in-use) per user
    private final Map<String, Semaphore> userSlots = new ConcurrentHashMap<>();
    // Which user each live connection was borrowed/created under — so returnToPool/closeAndDiscard
    // release the CORRECT user's permit even on a broken connection (getMetaData() would throw).
    private final Map<Connection, String> connOwner = new ConcurrentHashMap<>();

    // Retry config: 3 attempts with backoff on transient Oracle errors.
    private static final int CONNECT_RETRY_MAX = 3;
    private static final long[] CONNECT_RETRY_DELAYS_MS = {5_000, 10_000, 20_000};

    // Circuit breaker: skip objects that keep failing with transient DB errors.
    private static final int CIRCUIT_THRESHOLD = 3;
    private static final long CIRCUIT_RESET_MS = 30_000;
    private final Map<String, AtomicInteger> failureCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> circuitOpenSince = new ConcurrentHashMap<>();

    public static class DbUser {
        private final String username;
        private final String password;
        private final String description;
        // Schemas queryable when logged in as this user (may differ from the login username
        // itself, e.g. a login with grants into another owner's objects). Falls back to
        // [username] when not explicitly configured — backward compatible with existing configs.
        private final List<String> schemas;

        public DbUser(String username, String password, String description) {
            this(username, password, description, null);
        }

        public DbUser(String username, String password, String description, List<String> schemas) {
            this.username = username;
            this.password = password;
            this.description = description;
            this.schemas = schemas;
        }

        public String getUsername() { return username; }
        public String getPassword() { return password; }
        public String getDescription() { return description; }

        /** Declared schemas reachable via this user's login, falling back to [username]. */
        public List<String> getSchemas() {
            if (schemas == null || schemas.isEmpty()) {
                return username != null ? List.of(username.toUpperCase()) : List.of();
            }
            List<String> upper = new ArrayList<>();
            for (String s : schemas) if (s != null) upper.add(s.toUpperCase());
            return upper;
        }
    }

    public DbConnectionManager(String configPath) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> config;
        try (InputStream in = Files.newInputStream(Paths.get(configPath))) {
            config = yaml.load(in);
        }

        String url = (String) config.get("jdbc_url");
        if (url == null) {
            String host = (String) config.get("db_hostname");
            int port = config.get("db_port") instanceof Integer
                    ? (Integer) config.get("db_port")
                    : Integer.parseInt(config.get("db_port").toString());
            String service = (String) config.get("db_service_name");
            url = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service;
        }
        this.jdbcUrl = url;

        Object dbTypeObj = config.get("db_type");
        this.dbType = dbTypeObj != null ? dbTypeObj.toString().toUpperCase() : "ORACLE";

        this.dbUsers = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> usersList = (List<Map<String, Object>>) config.get("db_users");
        if (usersList != null) {
            for (Map<String, Object> u : usersList) {
                @SuppressWarnings("unchecked")
                List<String> userSchemas = (List<String>) u.get("schemas");
                dbUsers.add(new DbUser(
                        (String) u.get("username"),
                        (String) u.get("password"),
                        (String) u.get("description"),
                        userSchemas
                ));
            }
        }

        // Optional TLS settings. Absent for TLS-disabled configs (the common, backward-compatible
        // case) and for every config generated before this feature existed. Values are flattened to
        // strings by the writer; coerce defensively here in case a Path or number round-trips.
        Object sslObj = config.get("ssl_properties");
        if (sslObj instanceof Map<?, ?> sslMap) {
            for (Map.Entry<?, ?> e : sslMap.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    sslProperties.setProperty(e.getKey().toString(), e.getValue().toString());
                }
            }
        }
        initPools();
        loadDriver();
    }

    public DbConnectionManager(String jdbcUrl, List<DbUser> dbUsers) {
        this.jdbcUrl = jdbcUrl;
        this.dbUsers = new ArrayList<>(dbUsers);
        this.dbType = "ORACLE";
        initPools();
        loadDriver();
    }

    private void initPools() {
        for (DbUser u : dbUsers) {
            String key = u.getUsername().toUpperCase();
            userPools.put(key, new ArrayBlockingQueue<>(POOL_SIZE_PER_USER));
            userSlots.put(key, new Semaphore(POOL_SIZE_PER_USER));
        }
        log.info("[DB] Pool initialised: {} user(s) × {} slots = {} max connections",
                dbUsers.size(), POOL_SIZE_PER_USER, dbUsers.size() * POOL_SIZE_PER_USER);
    }

    private void loadDriver() {
        if (isMySQL()) {
            try { Class.forName("com.mysql.cj.jdbc.Driver"); return; } catch (ClassNotFoundException ignored) {}
            try { Class.forName("com.mysql.jdbc.Driver"); } catch (ClassNotFoundException e) {
                throw new RuntimeException("MySQL JDBC driver not found on classpath", e);
            }
        } else if (isSqlServer()) {
            try {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("SQL Server JDBC driver not found on classpath", e);
            }
        } else if (isDB2()) {
            try {
                Class.forName("com.ibm.db2.jcc.DB2Driver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("DB2 JDBC driver not found on classpath", e);
            }
        } else if (isPostgres()) {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("PostgreSQL JDBC driver not found on classpath", e);
            }
        } else {
            try {
                Class.forName("oracle.jdbc.OracleDriver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Oracle JDBC driver not found on classpath", e);
            }
        }
    }

    /**
     * Open a fresh connection for the given schema (does NOT use the pool).
     * Caller is responsible for closing it.
     */
    public Connection getConnection(String schema) throws SQLException {
        String upperSchema = schema.toUpperCase();
        DbUser user = null;
        for (DbUser u : dbUsers) {
            if (u.getSchemas().contains(upperSchema)) {
                user = u;
                break;
            }
        }
        if (user == null) {
            throw new SQLException("No credentials configured for schema: " + upperSchema
                    + ". Available schemas: " + getAvailableSchemas());
        }
        Properties props = new Properties();
        props.setProperty("user", user.getUsername());
        props.setProperty("password", user.getPassword() != null ? user.getPassword() : "");
        if (!isMySQL() && !isSqlServer() && !isDB2() && !isPostgres()) {
            props.setProperty("oracle.jdbc.loginTimeout", "15");
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "15000");
        }
        // TLS settings last: truststore/wallet/client-cert properties from the generated env
        // config. Empty (and skipped) for TLS-disabled connections.
        if (!sslProperties.isEmpty()) {
            props.putAll(sslProperties);
        }
        Connection conn = DriverManager.getConnection(jdbcUrl, props);
        conn.setAutoCommit(false);
        log.debug("[DB] Opened fresh connection for {}", upperSchema);
        return conn;
    }

    /**
     * Borrow a connection from the pool.  Tries idle connections first; opens a fresh
     * one if a slot is available; blocks up to ACQUIRE_TIMEOUT_MS if all slots are busy.
     *
     * MUST be paired with returnToPool() on success or closeAndDiscard() on failure.
     */
    public Connection getAnyConnection() throws SQLException {
        SQLException lastError = null;

        for (int attempt = 1; attempt <= CONNECT_RETRY_MAX; attempt++) {

            // Phase 1: non-blocking pass — grab an idle pooled connection or a free slot
            // from ANY user without waiting.  This avoids blocking 30s on a saturated user
            // when another user has capacity (the old sequential-blocking bug caused 60s
            // stalls: OPUS_CORE full → wait 30s → next user had a free slot all along).
            for (DbUser user : dbUsers) {
                String key = user.getUsername().toUpperCase();
                Semaphore slots = userSlots.get(key);
                ArrayBlockingQueue<Connection> pool = userPools.get(key);

                Connection idle = pool.poll();
                if (idle != null) {
                    try {
                        if (idle.isValid(2)) {
                            log.debug("[DB] Reusing pooled connection for {}", key);
                            return idle;
                        }
                        closeQuietly(idle);
                        slots.release();
                    } catch (Exception e) {
                        closeQuietly(idle);
                        slots.release();
                    }
                }

                if (slots.tryAcquire()) {
                    try {
                        Connection conn = getConnection(user.getUsername());
                        connOwner.put(conn, key);
                        return conn;
                    } catch (SQLException e) {
                        slots.release();
                        lastError = e;
                        String msg = e.getMessage() != null ? e.getMessage() : "";
                        if (msg.contains("ORA-01017") || msg.contains("invalid username/password")) {
                            throw new SQLException("Authentication failed for " + key + ": " + msg, e);
                        }
                        log.warn("[DB] Connect attempt {}/{} failed for {}: {}",
                                attempt, CONNECT_RETRY_MAX, key, msg);
                    }
                }
            }

            // Phase 2: all users saturated — wait for ANY user to free a slot.
            // Use a single shared 30s deadline across ALL users: each user gets only the
            // remaining time, so total wait is capped at 30s regardless of user count.
            // Old code: 30s PER user = 90s worst case. New code: 30s TOTAL.
            long deadline = System.currentTimeMillis() + ACQUIRE_TIMEOUT_MS;
            for (DbUser user : dbUsers) {
                String key = user.getUsername().toUpperCase();
                Semaphore slots = userSlots.get(key);
                ArrayBlockingQueue<Connection> pool = userPools.get(key);

                // Re-check idle queue — a connection may have been returned since phase 1
                Connection idle = pool.poll();
                if (idle != null) {
                    try {
                        if (idle.isValid(2)) {
                            log.debug("[DB] Reusing pooled connection for {} (phase 2)", key);
                            return idle;
                        }
                        closeQuietly(idle);
                        slots.release();
                    } catch (Exception e) {
                        closeQuietly(idle);
                        slots.release();
                    }
                }

                // Poll the idle queue AND the semaphore in a short-interval loop until the
                // deadline.  returnToPool() puts healthy connections back on the queue without
                // releasing a permit (the permit represents "physical connection exists"), so a
                // thread that blocks solely on slots.tryAcquire() would sleep the full 30s even
                // while connections sit idle in the queue.  Re-checking the queue every 200ms
                // lets a waiter pick up a returned connection immediately.
                boolean acquiredSlot = false;
                try {
                    long remaining;
                    while ((remaining = deadline - System.currentTimeMillis()) > 0) {
                        Connection returned = pool.poll();
                        if (returned != null) {
                            try {
                                if (returned.isValid(2)) {
                                    log.debug("[DB] Reusing pooled connection for {} (phase 2 poll)", key);
                                    return returned;
                                }
                                closeQuietly(returned);
                                slots.release();
                            } catch (Exception e) {
                                closeQuietly(returned);
                                slots.release();
                            }
                        }
                        if (slots.tryAcquire(Math.min(200L, remaining), TimeUnit.MILLISECONDS)) {
                            acquiredSlot = true;
                            break;
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted waiting for DB slot for " + key);
                }

                if (!acquiredSlot) {
                    log.warn("[DB] All {} slots busy for {}, waiting timed out", POOL_SIZE_PER_USER, key);
                    continue;
                }

                try {
                    Connection conn = getConnection(user.getUsername());
                    connOwner.put(conn, key);
                    if (attempt > 1) {
                        log.info("[DB] Connected on attempt {} for {}", attempt, key);
                    }
                    return conn;
                } catch (SQLException e) {
                    slots.release();
                    lastError = e;
                    String msg = e.getMessage() != null ? e.getMessage() : "";
                    if (msg.contains("ORA-01017") || msg.contains("invalid username/password")) {
                        throw new SQLException("Authentication failed for " + key + ": " + msg, e);
                    }
                    log.warn("[DB] Connect attempt {}/{} failed for {}: {}",
                            attempt, CONNECT_RETRY_MAX, key, msg);
                }
            }

            if (attempt < CONNECT_RETRY_MAX) {
                long delay = CONNECT_RETRY_DELAYS_MS[attempt - 1];
                log.info("[DB] All users failed on attempt {}/{}, retrying in {}ms...",
                        attempt, CONNECT_RETRY_MAX, delay);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while waiting to retry connection", lastError);
                }
            }
        }
        throw new SQLException("Could not connect after " + CONNECT_RETRY_MAX
                + " attempts. Last error: "
                + (lastError != null ? lastError.getMessage() : "No users configured"));
    }

    /**
     * Return a healthy connection to the pool for reuse.
     * Call this after every successful use of a connection from getAnyConnection().
     */
    public void returnToPool(Connection conn) {
        if (conn == null) return;
        // Prefer the recorded owner (reliable even on a flaky connection); fall back to metadata.
        String key = connOwner.getOrDefault(conn, identifyUser(conn));
        try {
            if (!conn.isClosed() && conn.isValid(1)) {
                ArrayBlockingQueue<Connection> pool = key != null ? userPools.get(key) : null;
                if (pool != null && pool.offer(conn)) {
                    log.debug("[DB] Returned connection to pool for {}", key);
                    return;   // stays owned by this user while idle in the pool
                }
            }
        } catch (Exception ignored) {}
        // Could not return to pool — close and release the slot
        closeAndDiscard(conn);
    }

    /**
     * Close a broken/failed connection and release its slot.
     * Call this whenever a connection throws an error during use.
     */
    public void closeAndDiscard(Connection conn) {
        if (conn == null) return;
        // Use the recorded owner — a broken conn's getMetaData().getUserName() throws, which
        // previously caused the WRONG user's permit to be released (pool starvation/over-release).
        String key = connOwner.remove(conn);
        if (key == null) key = identifyUser(conn);
        closeQuietly(conn);
        Semaphore slots = key != null ? userSlots.get(key) : null;
        if (slots != null) {
            slots.release();
            log.debug("[DB] Discarded broken connection, slot released for {}", key);
        }
    }

    /** Identify which user owns this connection by trying to read its username metadata. */
    private String identifyUser(Connection conn) {
        try {
            String user = conn.getMetaData().getUserName();
            if (user != null) {
                String upper = user.toUpperCase();
                if (userPools.containsKey(upper)) return upper;
            }
        } catch (Exception ignored) {}
        // Fallback: release for the first user (safe — slots are all equivalent semaphores)
        return dbUsers.isEmpty() ? null : dbUsers.get(0).getUsername().toUpperCase();
    }

    private static void closeQuietly(Connection conn) {
        try { if (conn != null && !conn.isClosed()) conn.close(); } catch (Exception ignored) {}
    }

    // Foreign owners discovered at crawl time via cross-schema references (e.g. a call
    // ANALYZER_REF.USP_REF_LOG where ANALYZER_REF is a schema the login user can read but which
    // is not a configured login itself). Included in getAvailableSchemas() so every owner-IN
    // clause (source fetch, ALL_OBJECTS existence checks) can see cross-schema objects. These
    // never open a connection AS that owner — the pool is keyed by configured dbUsers only, and
    // the login user's grants cover cross-schema ALL_SOURCE/ALL_OBJECTS reads.
    private final Set<String> discoveredSchemas = ConcurrentHashMap.newKeySet();

    /** Register a foreign owner discovered during the crawl so owner-IN clauses include it. */
    public void registerDiscoveredSchema(String schema) {
        if (schema == null || schema.isBlank()) return;
        discoveredSchemas.add(schema.toUpperCase().trim());
    }

    /**
     * Every owner an owner-IN clause should search: the schemas each login declares, each login's
     * <b>own</b> schema, and foreign owners discovered mid-crawl.
     *
     * <p>The login's own schema is included unconditionally (Fix C). A {@link DbUser} with an
     * explicit {@code schemas} list used to contribute ONLY those, so a login whose declared schemas
     * named cross-schema grants but not itself could not find an object sitting in its own default
     * schema — the "wrong owner" half of the entry-object lookup failure. For Oracle the login
     * username IS the default schema, and the login's grants always cover it. The addition is
     * additive: nothing that was searchable before stops being searched, and a connection is still
     * only ever opened for a schema some {@link DbUser#getSchemas()} declares (see
     * {@link #getConnection(String)}), so this widens what can be <em>read</em>, not what can be
     * logged in as.
     */
    public List<String> getAvailableSchemas() {
        List<String> schemas = new ArrayList<>();
        for (DbUser u : dbUsers) {
            for (String s : u.getSchemas()) if (!schemas.contains(s)) schemas.add(s);
            if (u.getUsername() != null) {
                String own = u.getUsername().toUpperCase();
                if (!schemas.contains(own)) schemas.add(own);
            }
        }
        for (String s : discoveredSchemas) if (!schemas.contains(s)) schemas.add(s);
        return schemas;
    }

    public String getDbType() { return dbType; }
    public boolean isMySQL() { return "MYSQL".equals(dbType); }
    public boolean isSqlServer() { return "SQLSERVER".equals(dbType); }
    public boolean isDB2() { return "DB2".equals(dbType); }
    public boolean isPostgres() { return "POSTGRES".equals(dbType); }

    /** True for the default Oracle dialect (none of the explicit multi-db flags is set). */
    public boolean isOracle() {
        return !isMySQL() && !isSqlServer() && !isDB2() && !isPostgres();
    }

    /**
     * Stable identity of this connection <em>target</em> — the dialect plus the JDBC URL. Used to
     * key on-disk caches that must never share entries across two different databases (the crawl
     * source cache). The generated configs carry no credentials in the URL: the username/password
     * travel in the JDBC {@code Properties}, never in {@code jdbc_url}.
     */
    public String getConnectionIdentity() {
        return dbType + "|" + (jdbcUrl == null ? "" : jdbcUrl);
    }

    /**
     * Total live connections the pool can hold across every configured user
     * ({@code dbUsers × POOL_SIZE_PER_USER}). Callers that fan work out in parallel use this as the
     * upper bound so they never queue more DB work than the pool can actually run at once.
     */
    public int getTotalPoolCapacity() {
        return Math.max(1, dbUsers.size() * POOL_SIZE_PER_USER);
    }

    /**
     * Default schema for SQL Server objects. SQL Server groups objects under a
     * schema (typically "dbo"); this is used as the owner label so output has
     * the same shape as Oracle (which uses OWNER).
     */
    public String getSqlServerSchema() { return "dbo"; }

    /** Extracts the MySQL database name from the JDBC URL (the path component). */
    public String getMySqlSchema() {
        if (jdbcUrl == null) return null;
        String url = jdbcUrl;
        int qmark = url.indexOf('?');
        if (qmark >= 0) url = url.substring(0, qmark);
        int lastSlash = url.lastIndexOf('/');
        return (lastSlash >= 0 && lastSlash < url.length() - 1)
                ? url.substring(lastSlash + 1) : null;
    }

    /**
     * Default schema for DB2 objects. DB2 groups routines under a schema
     * (SYSCAT.ROUTINES.ROUTINESCHEMA), which is typically the connection user.
     * Returns the first configured schema (DB user) so downstream schema
     * resolution has an owner label, mirroring getSqlServerSchema()/getMySqlSchema().
     */
    public String getDb2Schema() {
        return dbUsers.isEmpty() ? null : dbUsers.get(0).getUsername().toUpperCase();
    }

    /**
     * Default schema for PostgreSQL objects. PostgreSQL groups routines under a schema
     * (default "public"); returns the first configured schema (DB user) if present so
     * downstream schema resolution has an owner label, else "public".
     */
    public String getPostgresSchema() {
        return dbUsers.isEmpty() ? "public" : dbUsers.get(0).getUsername();
    }

    public String getJdbcUrl() { return jdbcUrl; }

    public void incrementDbCalls() { dbCallCount.incrementAndGet(); }
    public int getDbCallCount() { return dbCallCount.get(); }
    public void resetDbCallCount() { dbCallCount.set(0); }

    public boolean isCircuitOpen(String objectName) {
        Long openSince = circuitOpenSince.get(objectName.toUpperCase());
        if (openSince == null) return false;
        if (System.currentTimeMillis() - openSince > CIRCUIT_RESET_MS) {
            circuitOpenSince.remove(objectName.toUpperCase());
            failureCounts.remove(objectName.toUpperCase());
            return false;
        }
        return true;
    }

    public void recordSuccess(String objectName) {
        String key = objectName.toUpperCase();
        failureCounts.remove(key);
        circuitOpenSince.remove(key);
    }

    public void recordFailure(String objectName, String errorMsg) {
        if (errorMsg == null) return;
        boolean isTransient = errorMsg.contains("ORA-17800") || errorMsg.contains("ORA-17002")
                || errorMsg.contains("ORA-12516") || errorMsg.contains("Connection reset")
                || errorMsg.contains("I/O error") || errorMsg.contains("read call");
        if (!isTransient) return;
        String key = objectName.toUpperCase();
        int count = failureCounts.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
        if (count >= CIRCUIT_THRESHOLD) {
            circuitOpenSince.putIfAbsent(key, System.currentTimeMillis());
        }
    }

    /** Log current pool state — useful for diagnostics. */
    public void logPoolState() {
        for (DbUser u : dbUsers) {
            String key = u.getUsername().toUpperCase();
            Semaphore slots = userSlots.get(key);
            ArrayBlockingQueue<Connection> pool = userPools.get(key);
            int idle = pool != null ? pool.size() : 0;
            int available = slots != null ? slots.availablePermits() : 0;
            int inUse = POOL_SIZE_PER_USER - available;
            log.info("[DB] Pool[{}]: idle={}, in-use={}, free-slots={}/{}",
                    key, idle, inUse, available, POOL_SIZE_PER_USER);
        }
    }

    @Override
    public void close() {
        for (Map.Entry<String, ArrayBlockingQueue<Connection>> e : userPools.entrySet()) {
            Connection c;
            while ((c = e.getValue().poll()) != null) {
                closeQuietly(c);
                Semaphore s = userSlots.get(e.getKey());
                if (s != null) s.release();
            }
        }
    }
}
