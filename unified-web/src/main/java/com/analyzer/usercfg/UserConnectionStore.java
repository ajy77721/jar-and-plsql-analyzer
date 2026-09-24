package com.analyzer.usercfg;

import com.analyzer.admin.AdminAuditService;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.plsqlanalyzer.config.ConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Per-user database-connection storage.
 *
 * <h2>Where the bytes live</h2>
 *
 * <p>Each user's connections are written to {@code users/<identity-key>/connections.yaml} through
 * the injected {@link ArtifactStore} — the <em>routed</em> store bean, which resolves the calling
 * user's session and talks to that user's own bucket. So the path is relative to the user's own
 * storage root, and no two users can address each other's document. The identity key is the same
 * chain as {@link AdminAuditService#identityKey}: lower-cased email, else lower-cased name, else
 * account id — the one definition of "who this is" the audit trail already uses.
 *
 * <p>Certificates and wallets a user uploads for a TLS connection live beside the document under
 * {@code users/<identity-key>/certs/<connection>/<file>}; see {@link #storeCertificateForCurrentUser}
 * and {@link #materializeCertsForDial}. A user connection's TLS path fields are stored relative to
 * the user's prefix (e.g. {@code certs/MY_CONN/trust.pem}) — the same spelling the shared/global
 * path uses, but resolved against the user prefix instead of the local config directory.
 *
 * <h2>Encryption at rest</h2>
 *
 * <p>{@code password} and {@code truststorePassword} are AES-GCM encrypted with a key derived as
 * {@code SHA-256(APP_CONFIG_KEY)}. Every value gets a fresh random 12-byte IV and is stored as the
 * self-describing string {@code enc:v1:<iv-base64>:<ciphertext-base64>}. GCM's authentication tag is
 * checked on decrypt, so a tampered or truncated value fails loudly instead of yielding garbage.
 *
 * <p>When {@code APP_CONFIG_KEY} is blank — local development, where no Kanopy secret has been
 * injected — documents are stored in plaintext with a logged WARN and the document's
 * {@code encrypted: false} marker. Production sets the env var via a Kanopy secret, so the marker is
 * a deployment fact, not a per-connection choice; it is never silently overwritten by whichever key
 * happens to be present at write time.
 *
 * <h2>Authentication</h2>
 *
 * <p>Every mutating method requires an authenticated session and fails if there is none; the whole
 * config controller sits behind the SSO gate, so the identity is normally always present. Reads for
 * an unauthenticated caller return an empty list rather than throwing, so a page can render its
 * shared connections before sign-in without a 500.
 */
@Service
public class UserConnectionStore {

    private static final Logger log = LoggerFactory.getLogger(UserConnectionStore.class);

    /** Root segment of every per-user document in the artifact store. */
    static final String USERS_ROOT = "users";

    private static final String DOC_FILE = "connections.yaml";
    private static final String CERTS_DIR = "certs";

    /** Payload prefix of an encrypted value; the {@code v1} names the format, not the key. */
    private static final String ENC_PREFIX = "enc:v1:";
    private static final int GCM_IV_BYTES = 12;
    private static final int GCM_TAG_BITS = 128;

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Its own mapper, matching {@code ConfigLoader}'s conventions (snake_case keys, empty arrays
     * omitted) so a user document reads like the global config it mirrors. Deliberately not the
     * global HTTP mapper: this one persists secrets to disk and must not inherit any masking rule.
     */
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);

    private final ArtifactStore store;
    private final SsoSessionRegistry ssoSessionRegistry;
    private final CurrentHttpSession currentSession;

    /** SHA-256(APP_CONFIG_KEY), or {@code null} when no key is configured (plaintext mode). */
    private final byte[] encryptionKey;

    public UserConnectionStore(@Qualifier("parserArtifactStore") ArtifactStore store,
                               SsoSessionRegistry ssoSessionRegistry,
                               CurrentHttpSession currentSession,
                               @Value("${APP_CONFIG_KEY:}") String appConfigKey) {
        this.store = store;
        this.ssoSessionRegistry = ssoSessionRegistry;
        this.currentSession = currentSession;
        this.encryptionKey = deriveKey(appConfigKey);
        if (this.encryptionKey == null) {
            log.warn("[user-conns] APP_CONFIG_KEY is not set — per-user connection passwords are "
                    + "stored in PLAINTEXT and marked encrypted:false. Set APP_CONFIG_KEY (Kanopy "
                    + "secret) in production.");
        } else {
            log.info("[user-conns] per-user connections encrypted at rest (AES-GCM, SHA-256 key)");
        }
    }

    private static byte[] deriveKey(String appConfigKey) {
        if (appConfigKey == null || appConfigKey.isBlank()) return null;
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(appConfigKey.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JLS; unreachable in practice.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** True when a configured key means secrets are encrypted at rest. */
    public boolean encryptionEnabled() { return encryptionKey != null; }

    // ── identity ────────────────────────────────────────────────────────────────

    /**
     * The calling user's identity key, or {@code null} when this thread serves no authenticated
     * session (a background job, the pre-SSO local workflow). Never throws.
     */
    public String currentIdentityKey() {
        try {
            String sessionId = currentSession.currentSessionId();
            if (sessionId == null) return null;
            SsoIdentity identity = ssoSessionRegistry.identity(sessionId);
            return AdminAuditService.identityKey(identity);
        } catch (RuntimeException e) {
            log.debug("[user-conns] no identity for current request ({})", e.getClass().getSimpleName());
            return null;
        }
    }

    /** True when a private store can be addressed for this request. */
    public boolean availableForCurrentUser() { return currentIdentityKey() != null; }

    private String requireIdentityKey() {
        String key = currentIdentityKey();
        if (key == null) {
            throw new IllegalStateException("No authenticated user — private connections require an SSO session");
        }
        return key;
    }

    // ── document read / write ───────────────────────────────────────────────────

    /** The artifact key of a user's connection document. */
    static ArtifactKey documentKey(String identityKey) {
        return ArtifactKey.of(USERS_ROOT, identityKey, DOC_FILE);
    }

    /** The artifact key of one certificate file under a user's prefix. */
    static ArtifactKey certKey(String identityKey, String connectionName, String filename) {
        return ArtifactKey.of(USERS_ROOT, identityKey, CERTS_DIR, connectionName, filename);
    }

    /**
     * Reads and decrypts the current user's connections. Returns an empty list when the caller is
     * unauthenticated or has never saved any, so the listing endpoint needs no null handling.
     */
    public List<ConnectionConfig> listForCurrentUser() {
        String key = currentIdentityKey();
        if (key == null) return new ArrayList<>();
        try {
            return readDecrypted(key).getConnections();
        } catch (RuntimeException e) {
            // A signed-in user who has not connected their bucket yet (the routed store throws
            // NotAuthenticatedException) must still see their shared connections, not a 500. Reads
            // degrade to "no private connections"; writes still surface the failure.
            log.debug("[user-conns] private store not readable for {} ({})", key, e.getClass().getSimpleName());
            return new ArrayList<>();
        }
    }

    /** The current user's connection named {@code name} (case-insensitive), or {@code null}. */
    public ConnectionConfig findForCurrentUser(String name) {
        if (name == null || name.isBlank()) return null;
        for (ConnectionConfig c : listForCurrentUser()) {
            if (name.equalsIgnoreCase(c.getName())) return c;
        }
        return null;
    }

    /** True when the current user already has a connection named {@code name}. */
    public boolean existsForCurrentUser(String name) {
        return findForCurrentUser(name) != null;
    }

    /**
     * Inserts or replaces a connection in the current user's store, encrypting its secrets.
     *
     * <p>A blank {@code password}/{@code truststorePassword} on the incoming object means
     * "unchanged" — the editor can no longer pre-fill a masked secret, so it submits an empty
     * string — and is filled from the stored value rather than overwriting it.
     */
    public ConnectionConfig saveForCurrentUser(ConnectionConfig conn) {
        String identityKey = requireIdentityKey();
        UserConnections doc = readDecrypted(identityKey);
        ConnectionConfig existing = findBy(doc.getConnections(), conn.getName());
        if (existing != null) {
            if (isBlank(conn.getPassword())) conn.setPassword(existing.getPassword());
            if (isBlank(conn.getTruststorePassword())) conn.setTruststorePassword(existing.getTruststorePassword());
            doc.getConnections().remove(existing);
        } else if (isBlank(conn.getPassword())) {
            throw new IllegalArgumentException("Password is required for a new connection");
        }
        conn.setScope(null); // request-only hint must never be persisted
        if (conn.getLastTestedAt() == null) conn.setLastTestedAt(System.currentTimeMillis());
        doc.getConnections().add(conn);
        write(identityKey, doc);
        log.info("[user-conns] saved connection '{}' for {}", conn.getName(), identityKey);
        return conn;
    }

    /** Removes the current user's connection named {@code name}; false when it did not exist. */
    public boolean deleteForCurrentUser(String name) {
        String identityKey = requireIdentityKey();
        UserConnections doc = readDecrypted(identityKey);
        ConnectionConfig existing = findBy(doc.getConnections(), name);
        if (existing == null) return false;
        doc.getConnections().remove(existing);
        write(identityKey, doc);
        return true;
    }

    private UserConnections readDecrypted(String identityKey) {
        ArtifactKey key = documentKey(identityKey);
        if (!store.exists(key)) return new UserConnections();
        UserConnections doc;
        try (InputStream in = store.read(key)) {
            doc = YAML_MAPPER.readValue(in, UserConnections.class);
        } catch (IOException e) {
            log.warn("[user-conns] could not read {}: {}", key, e.getMessage());
            return new UserConnections();
        }
        if (doc == null) return new UserConnections();
        if (doc.getConnections() == null) doc.setConnections(new ArrayList<>());
        for (ConnectionConfig c : doc.getConnections()) {
            c.setPassword(decrypt(c.getPassword()));
            c.setTruststorePassword(decrypt(c.getTruststorePassword()));
            c.setScope(null);
        }
        return doc;
    }

    /** Encrypts secrets into copies and writes the document (atomic on stream close). */
    private void write(String identityKey, UserConnections doc) {
        UserConnections toStore = new UserConnections();
        toStore.setEncrypted(encryptionKey != null);
        List<ConnectionConfig> copies = new ArrayList<>();
        for (ConnectionConfig c : doc.getConnections()) {
            // A serialisation round-trip gives a detached copy, so encrypting in place here cannot
            // corrupt the plaintext objects the caller still holds.
            ConnectionConfig copy = YAML_MAPPER.convertValue(c, ConnectionConfig.class);
            copy.setScope(null);
            copy.setPassword(encrypt(c.getPassword()));
            copy.setTruststorePassword(encrypt(c.getTruststorePassword()));
            copies.add(copy);
        }
        toStore.setConnections(copies);
        try (OutputStream out = store.write(documentKey(identityKey))) {
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(out, toStore);
        } catch (IOException e) {
            throw new IllegalStateException("Could not save private connections: " + e.getMessage(), e);
        }
    }

    // ── certificate storage ─────────────────────────────────────────────────────

    /**
     * Stores one certificate/wallet file under the current user's prefix and returns the path a
     * connection should carry — {@code certs/<connection>/<filename>}, relative to the user prefix.
     */
    public String storeCertificateForCurrentUser(String connectionName, String filename, byte[] bytes) {
        String identityKey = requireIdentityKey();
        ArtifactKey key = certKey(identityKey, connectionName, filename);
        try (OutputStream out = store.write(key)) {
            out.write(bytes);
        } catch (IOException e) {
            throw new IllegalStateException("Could not store certificate: " + e.getMessage(), e);
        }
        return CERTS_DIR + "/" + connectionName + "/" + filename;
    }

    // ── certificate materialisation (drivers need real files) ───────────────────

    /**
     * A connection whose TLS path fields have been rewritten to real local files, plus the temp
     * directory those files live in. Closing deletes the directory; a null {@link #baseDir()}
     * means there was no user TLS material and the caller should use its normal config dir.
     */
    public final class MaterializedCerts implements AutoCloseable {
        private final ConnectionConfig connection;
        private final Path baseDir;
        private final Path cleanupDir;

        private MaterializedCerts(ConnectionConfig connection, Path baseDir, Path cleanupDir) {
            this.connection = connection;
            this.baseDir = baseDir;
            this.cleanupDir = cleanupDir;
        }

        public ConnectionConfig connection() { return connection; }
        /** The directory the rewritten paths are rooted at, or null when nothing was materialized. */
        public Path baseDir() { return baseDir; }

        @Override
        public void close() {
            if (cleanupDir != null) deleteRecursively(cleanupDir);
        }
    }

    /**
     * Downloads the user-store certificate/wallet files a connection references into a fresh temp
     * directory and returns a copy whose TLS fields point at those local files. Drivers such as
     * Oracle's JSSE truststore take a {@link Path}, so an object-store-hosted artifact must be
     * materialized before dialling. The caller must {@link MaterializedCerts#close() close} the
     * result to delete the temp copies.
     */
    public MaterializedCerts materializeCertsForDial(ConnectionConfig conn) {
        String identityKey = requireIdentityKey();
        if (!hasTlsMaterial(conn)) {
            return new MaterializedCerts(conn, null, null);
        }
        Path dir;
        try {
            dir = Files.createTempDirectory("user-conn-certs-");
        } catch (IOException e) {
            throw new IllegalStateException("Could not create a temp dir for certificates: " + e.getMessage(), e);
        }
        try {
            ConnectionConfig copy = copyOf(conn);
            int materialized = materializeInto(copy, identityKey, dir);
            if (materialized == 0) {
                // Nothing lived under the user prefix — e.g. a cert uploaded to the shared config
                // dir. Leave the paths as-is and let the caller resolve them against its config dir.
                deleteRecursively(dir);
                return new MaterializedCerts(conn, null, null);
            }
            return new MaterializedCerts(copy, dir, dir);
        } catch (RuntimeException | IOException e) {
            deleteRecursively(dir);
            throw new IllegalStateException("Could not load TLS material: " + e.getMessage(), e);
        }
    }

    /**
     * Persistent variant used by the analysis hand-off: materializes the same files under a stable
     * directory (so the env config written beside the analysis still resolves after a restart) and
     * returns the rewritten connection. Nothing is deleted here — the directory is part of the
     * analysis output tree.
     */
    public ConnectionConfig materializeCertsPersistent(ConnectionConfig conn, Path targetDir) throws IOException {
        String identityKey = requireIdentityKey();
        Files.createDirectories(targetDir);
        ConnectionConfig copy = copyOf(conn);
        materializeInto(copy, identityKey, targetDir);
        return copy;
    }

    /** @return how many files were actually fetched out of the user's store */
    private int materializeInto(ConnectionConfig copy, String identityKey, Path dir) throws IOException {
        int count = 0;
        String trust = fetch(copy.getTrustCertPath(), identityKey, dir);
        if (!trust.equals(copy.getTrustCertPath())) count++;
        copy.setTrustCertPath(trust);
        String clientCert = fetch(copy.getClientCertPath(), identityKey, dir);
        if (!clientCert.equals(copy.getClientCertPath())) count++;
        copy.setClientCertPath(clientCert);
        String clientKey = fetch(copy.getClientKeyPath(), identityKey, dir);
        if (!clientKey.equals(copy.getClientKeyPath())) count++;
        copy.setClientKeyPath(clientKey);
        String truststore = fetch(copy.getTruststorePath(), identityKey, dir);
        if (!truststore.equals(copy.getTruststorePath())) count++;
        copy.setTruststorePath(truststore);

        String wallet = copy.getWalletPath();
        if (!isBlank(wallet) && !Path.of(wallet).isAbsolute()) {
            Path walletDir = dir.resolve(wallet).normalize();
            int copied = copyPrefix(identityKey, wallet, walletDir);
            if (copied > 0) {
                copy.setWalletPath(walletDir.toAbsolutePath().toString());
                count += copied;
            }
        }
        return count;
    }

    /** Copies one referenced file (when it exists under the user prefix) to {@code dir}. */
    private String fetch(String relative, String identityKey, Path dir) throws IOException {
        if (isBlank(relative) || Path.of(relative).isAbsolute()) return relative;
        ArtifactKey key = relativeKey(identityKey, relative);
        if (key == null || !store.exists(key)) return relative;
        Path target = dir.resolve(relative).normalize();
        if (target.getParent() != null) Files.createDirectories(target.getParent());
        try (InputStream in = store.read(key)) {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target.toAbsolutePath().toString();
    }

    /** Copies every object under one relative prefix (an Oracle wallet directory). */
    private int copyPrefix(String identityKey, String relative, Path targetDir) throws IOException {
        ArtifactKey prefix = relativeKey(identityKey, relative);
        if (prefix == null) return 0;
        List<ArtifactEntry> files = new ArrayList<>();
        try (Stream<ArtifactEntry> s = store.list(prefix, true)) {
            s.filter(e -> !e.directory() && e.key().segments().size() > prefix.segments().size())
             .forEach(files::add);
        }
        int copied = 0;
        int depth = prefix.segments().size();
        for (ArtifactEntry entry : files) {
            List<String> tail = entry.key().segments().subList(depth, entry.key().segments().size());
            Path target = targetDir;
            for (String seg : tail) target = target.resolve(seg);
            if (target.getParent() != null) Files.createDirectories(target.getParent());
            try (InputStream in = store.read(entry.key())) {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            }
            copied++;
        }
        return copied;
    }

    /** {@code users/<identityKey>/<relative...>} as an artifact key, or null when unsafe. */
    private static ArtifactKey relativeKey(String identityKey, String relative) {
        if (relative == null || relative.isBlank()) return null;
        String cleaned = relative.replace('\\', '/');
        while (cleaned.startsWith("/")) cleaned = cleaned.substring(1);
        String[] parts = cleaned.split("/");
        List<String> segments = new ArrayList<>();
        segments.add(USERS_ROOT);
        segments.add(identityKey);
        for (String p : parts) {
            if (p.isEmpty() || p.equals(".") || p.equals("..")) return null;
            segments.add(p);
        }
        try {
            return ArtifactKey.of(segments.toArray(new String[0]));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    static boolean hasTlsMaterial(ConnectionConfig c) {
        return !isBlank(c.getTrustCertPath()) || !isBlank(c.getClientCertPath())
                || !isBlank(c.getClientKeyPath()) || !isBlank(c.getTruststorePath())
                || !isBlank(c.getWalletPath());
    }

    private static ConnectionConfig copyOf(ConnectionConfig conn) {
        ConnectionConfig copy = YAML_MAPPER.convertValue(conn, ConnectionConfig.class);
        copy.setScope(null);
        return copy;
    }

    // ── crypto ──────────────────────────────────────────────────────────────────

    /** Encrypts a secret into {@code enc:v1:<iv>:<ciphertext>}; plaintext/blank pass through. */
    String encrypt(String plain) {
        if (plain == null || plain.isEmpty()) return plain;
        if (encryptionKey == null) return plain;
        try {
            byte[] iv = new byte[GCM_IV_BYTES];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(encryptionKey, "AES"),
                    new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ct = cipher.doFinal(plain.getBytes(StandardCharsets.UTF_8));
            return ENC_PREFIX + Base64.getEncoder().encodeToString(iv)
                    + ":" + Base64.getEncoder().encodeToString(ct);
        } catch (Exception e) {
            throw new IllegalStateException("Could not encrypt a connection secret: " + e.getMessage(), e);
        }
    }

    /**
     * Decrypts an {@code enc:v1:...} value; a value that is not in that format is returned
     * unchanged (a plaintext-mode document). An encrypted value with no configured key, or one whose
     * authentication tag does not verify, yields {@code null} with a warning — the secret is
     * unusable, and returning garbage would be worse than returning nothing.
     */
    String decrypt(String stored) {
        if (stored == null || stored.isEmpty()) return stored;
        if (!stored.startsWith(ENC_PREFIX)) return stored;
        if (encryptionKey == null) {
            log.warn("[user-conns] found an encrypted secret but APP_CONFIG_KEY is not set — cannot decrypt");
            return null;
        }
        try {
            String[] parts = stored.substring(ENC_PREFIX.length()).split(":", 2);
            if (parts.length != 2) throw new IllegalArgumentException("malformed payload");
            byte[] iv = Base64.getDecoder().decode(parts[0]);
            byte[] ct = Base64.getDecoder().decode(parts[1]);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(encryptionKey, "AES"),
                    new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(ct), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("[user-conns] a stored secret could not be decrypted ({}) — treating it as absent",
                    e.getClass().getSimpleName());
            return null;
        }
    }

    // ── small helpers ───────────────────────────────────────────────────────────

    private static ConnectionConfig findBy(List<ConnectionConfig> list, String name) {
        if (name == null) return null;
        for (ConnectionConfig c : list) {
            if (name.equalsIgnoreCase(c.getName())) return c;
        }
        return null;
    }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }

    private static void deleteRecursively(Path dir) {
        if (dir == null) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException e) {
            log.debug("[user-conns] temp cleanup of {} failed: {}", dir, e.getMessage());
        }
    }

    /** Normalises an incoming {@code scope} value to {@code "user"}/{@code "shared"}/null. */
    public static String normalizedScope(String scope) {
        if (scope == null || scope.isBlank()) return null;
        String s = scope.trim().toLowerCase(Locale.ROOT);
        if (s.equals("user") || s.equals("shared")) return s;
        return null;
    }
}
