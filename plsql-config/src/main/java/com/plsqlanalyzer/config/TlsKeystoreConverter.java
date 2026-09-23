package com.plsqlanalyzer.config;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

/**
 * Converts operator-supplied PEM certificate bundles into the JKS / PKCS12 keystores that
 * a handful of JDBC drivers still insist on (IBM DB2's {@code sslTrustStoreLocation}, the
 * JSSE properties used by the Oracle thin driver, and SQL Server's
 * {@code serverCertificateKeyStoreUrl}).
 *
 * <p>Why a converter at all: the product's configuration surface deliberately stores
 * TLS material as human-readable PEM files under the repo-relative {@code certs/} tree
 * (easy to diff, easy to replace, no opaque binaries checked in). Only some drivers can
 * consume PEM directly, so the remaining ones get a derived keystore written <em>next to
 * the PEM</em> — the twin lives in the same directory with the same base name and a
 * {@code .jks} / {@code .p12} extension, e.g. {@code certs/MY_CONN/trust.pem} becomes
 * {@code certs/MY_CONN/trust.jks}.
 *
 * <p>Reuse contract: if the twin already exists and is at least as new as the PEM, the
 * existing keystore is returned untouched. This keeps per-connection driver setup cheap
 * (no re-parsing on every get-connection call) and preserves an operator's manually
 * curated keystore as long as the source PEM has not been refreshed.
 *
 * <p>Password handling: generated keystores are protected with a fixed, non-secret
 * {@link #GENERATED_STORE_PASSWORD}. A randomly generated password would have to be
 * persisted (or re-derived) for the reuse path above to keep working, and a truststore
 * holds only public certificates — there is no secret to protect. Nothing in this class
 * ever logs the password or the key material.
 */
public final class TlsKeystoreConverter {

    /**
     * Password used for every keystore this converter generates. Not a secret: a truststore
     * contains only public certificates, and the value must be stable so an existing twin
     * keystore remains readable across calls (see the reuse contract in the class javadoc).
     * Package-private so {@link ConnectionConfig} can hand the matching password to drivers
     * via their truststore-password properties without duplicating the literal.
     */
    static final String GENERATED_STORE_PASSWORD = "changeit";

    private TlsKeystoreConverter() {}

    /**
     * Ensures a JKS truststore twin exists for the given PEM bundle and returns its path.
     *
     * @param pemPath   the PEM file; may be relative, in which case it is resolved against
     *                  {@code configDir} (see {@link #resolve(Path, Path)})
     * @param configDir the configuration directory relative paths are anchored at; may be
     *                  {@code null}, in which case relative paths resolve against the
     *                  current working directory
     * @return absolute path of the JKS twin, either pre-existing (and newer) or freshly written
     * @throws IllegalStateException if the PEM is missing/unreadable or cannot be parsed as
     *                               X.509 certificates; the message is driver-facing and is
     *                               surfaced verbatim when a connection test fails
     */
    public static Path ensureTrustStore(Path pemPath, Path configDir) {
        return ensureTrustStoreArtifact(pemPath, configDir).path();
    }

    /**
     * Artifact-producing variant of {@link #ensureTrustStore(Path, Path)} used internally so
     * callers that must also supply a truststore password (DB2, JSSE) can stay in sync with
     * the password actually used when the keystore was written.
     */
    static KeystoreArtifact ensureTrustStoreArtifact(Path pemPath, Path configDir) {
        return ensureStore(pemPath, configDir, "JKS", ".jks");
    }

    /**
     * Ensures a PKCS12 twin exists for the given PEM bundle and returns it. Used by the SQL
     * Server dialect, which accepts {@code serverCertificateKeyStoreUrl} pointing at a
     * PKCS12 keystore rather than a JKS one.
     */
    static KeystoreArtifact ensurePkcs12StoreArtifact(Path pemPath, Path configDir) {
        return ensureStore(pemPath, configDir, "PKCS12", ".p12");
    }

    private static KeystoreArtifact ensureStore(Path pemPath, Path configDir, String type, String extension) {
        Path pem = resolve(pemPath, configDir);
        if (!Files.exists(pem)) {
            throw new IllegalStateException("TLS certificate file not found: " + pem
                    + " (check the trust_cert_path entry in the connection config)");
        }
        Path store = twin(pem, extension);
        try {
            if (Files.exists(store)
                    && Files.getLastModifiedTime(store).compareTo(Files.getLastModifiedTime(pem)) >= 0) {
                return new KeystoreArtifact(store, GENERATED_STORE_PASSWORD);
            }
            Collection<? extends Certificate> certs;
            try (InputStream in = Files.newInputStream(pem)) {
                certs = CertificateFactory.getInstance("X.509").generateCertificates(in);
            }
            if (certs.isEmpty()) {
                throw new IllegalStateException("No X.509 certificates found in TLS file " + pem
                        + " — expected one or more PEM 'BEGIN CERTIFICATE' blocks");
            }
            KeyStore ks = KeyStore.getInstance(type);
            ks.load(null, null);
            int index = 0;
            for (Certificate cert : certs) {
                ks.setCertificateEntry("tls-cert-" + (index++), cert);
            }
            try (OutputStream out = Files.newOutputStream(store)) {
                ks.store(out, GENERATED_STORE_PASSWORD.toCharArray());
            }
            return new KeystoreArtifact(store, GENERATED_STORE_PASSWORD);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build " + type + " trust store from " + pem
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Resolves a possibly-relative TLS path against the configuration directory. Absolute
     * paths (including any path with a leading {@code /}) are returned unchanged; relative
     * paths are anchored at {@code configDir} when supplied, otherwise at the process working
     * directory. The result is always absolute and normalized so it can be embedded in JDBC
     * URLs and driver properties safely.
     */
    static Path resolve(Path raw, Path configDir) {
        if (raw == null) {
            return null;
        }
        Path resolved = raw.isAbsolute() || configDir == null ? raw : configDir.resolve(raw);
        return resolved.toAbsolutePath().normalize();
    }

    /** Sibling path with the same base name but the supplied extension ({@code trust.pem} → {@code trust.jks}). */
    private static Path twin(Path pem, String extension) {
        String fileName = pem.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String base = dot > 0 ? fileName.substring(0, dot) : fileName;
        Path parent = pem.getParent();
        return (parent == null ? Path.of(base + extension) : parent.resolve(base + extension))
                .toAbsolutePath().normalize();
    }

    /**
     * A generated (or reused) keystore plus the password it was written with, so the caller
     * can forward both to the driver.
     */
    record KeystoreArtifact(Path path, String password) {}
}
