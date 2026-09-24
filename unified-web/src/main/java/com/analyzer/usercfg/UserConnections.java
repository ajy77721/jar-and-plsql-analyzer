package com.analyzer.usercfg;

import com.plsqlanalyzer.config.ConnectionConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * The on-disk document behind a single user's private connection store
 * ({@code users/<identity-key>/connections.yaml}).
 *
 * <p>It is a tiny wrapper rather than a bare {@code List<ConnectionConfig>} for exactly one
 * reason: the {@link #isEncrypted() encrypted} marker. When {@code APP_CONFIG_KEY} is set the
 * {@code password}/{@code truststorePassword} fields hold {@code enc:v1:...} AES-GCM blobs, and
 * the marker is {@code true}; when the key is absent (local dev) the same fields hold plaintext
 * and the marker is {@code false}. Recording which of the two a document was written under means a
 * later reader can tell "plaintext by design" from "encrypted but the key is now missing" instead
 * of silently guessing.
 *
 * <p>The presence of a {@code servers} side is deliberately absent: this document is addressed by
 * identity (the path), and there is no cross-user index to keep consistent.
 */
public class UserConnections {

    /** True when the secret fields of the stored connections hold AES-GCM blobs. */
    private boolean encrypted = true;

    private List<ConnectionConfig> connections = new ArrayList<>();

    public UserConnections() {}

    public boolean isEncrypted() { return encrypted; }
    public void setEncrypted(boolean encrypted) { this.encrypted = encrypted; }

    public List<ConnectionConfig> getConnections() { return connections; }
    public void setConnections(List<ConnectionConfig> connections) {
        this.connections = connections != null ? connections : new ArrayList<>();
    }
}
