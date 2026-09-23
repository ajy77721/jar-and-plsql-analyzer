package com.analyzer.storage.mongo;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The one place a payload is measured before it becomes part of a MongoDB document, and the one
 * place an oversized payload is diverted to {@link ArtifactStore} instead. PLAN-06 §"The size
 * guard".
 *
 * <h2>Why a guard at all</h2>
 *
 * <p>A document that grows past 16 MB is a hard write failure, and the inputs that cause it are
 * exactly the ones customers bring: PLAN-01 measured a real {@code analysis.json} at <b>912 MB and
 * still growing</b> for 1,597 classes, {@code api/tables/index.json} extrapolates to ~500 MB, and
 * {@code api/nodes/<id>.json} has been observed at 370 KB — under the limit, but only by a factor
 * the next customer can erase. "It has never happened" is not a guard.
 *
 * <h2>12 MB, not 16</h2>
 *
 * <p>The soft limit is <b>12 MB</b>, leaving 25% headroom for BSON framing, the wrapper fields this
 * class adds, and later field additions. Measuring against 16 MB would make the guard and the
 * server's own limit fire at almost the same size, which turns a small future addition into a
 * production write failure. It also sits above {@link MongoArtifactStore#MAX_INLINE_THRESHOLD}
 * (8 MiB) so the store's guard and this one cannot disagree about which side of a line a payload
 * falls on.
 *
 * <h2>Nothing is ever silently truncated</h2>
 *
 * <p>PLAN-06 gives three responses to an oversized element, in order of preference: move the one
 * large field to {@code ArtifactStore}; split one level deeper; or, for a single indivisible element,
 * store it as an artifact and keep only its metadata. This class implements the third — the
 * backstop — because the first two are shape-specific decisions a producer makes, and something has
 * to catch the element that arrives indivisible anyway. What it must never do is truncate or reject:
 * a rejected element loses data loudly, a truncated one loses data silently, and the whole point of
 * having an {@code ArtifactStore} alongside is that neither is necessary.
 *
 * <h2>Content-addressed, so the diversion also deduplicates</h2>
 *
 * <p>The artifact key is {@code repo-overflow/<aa>/<sha256>}, derived from the payload itself. Two
 * identical oversized elements — common in generated code, which is where PLAN-06 expects this to
 * fire — collapse to one artifact. The two-character fan-out directory keeps a single prefix
 * listing from growing unbounded, which matters because {@code list} is O(subtree) on this backend.
 *
 * <p>The diversion goes through {@code ArtifactStore}, not around it, so it inherits
 * {@code CredentialGuardStore} (C4) and works identically whether the bytes land on local disk, in
 * S3 or in GridFS. That is the whole reason PLAN-06 says a document references an artifact by key
 * plus {@code size} plus {@code sha256} rather than embedding it.
 */
public final class MongoDocumentGuard {

    private static final Logger log = LoggerFactory.getLogger(MongoDocumentGuard.class);

    /** PLAN-06's soft limit: 12 MB, 25% under the 16 MB BSON ceiling. */
    public static final int SOFT_LIMIT_BYTES = 12 * 1024 * 1024;

    /** Field holding an inline JSON payload. */
    public static final String F_JSON = "json";
    /** Field holding the {@link ArtifactKey} of a diverted payload. */
    public static final String F_BLOB_KEY = "blobKey";
    /** Byte length of the payload, inline or diverted. Always present. */
    public static final String F_LEN = "len";
    /** SHA-256 of the payload. Present only when diverted — it is what the key is derived from. */
    public static final String F_SHA256 = "sha256";

    /** Fan-out root for diverted payloads. */
    static final String OVERFLOW_ROOT = "repo-overflow";

    private final ArtifactStore blobs;
    private final int softLimitBytes;

    public MongoDocumentGuard(ArtifactStore blobs) {
        this(blobs, SOFT_LIMIT_BYTES);
    }

    /** @param softLimitBytes overridable so a test can drive the diversion without 12 MB of input */
    public MongoDocumentGuard(ArtifactStore blobs, int softLimitBytes) {
        if (blobs == null) throw new IllegalArgumentException("an ArtifactStore is required to divert oversized payloads");
        if (softLimitBytes <= 0) throw new IllegalArgumentException("softLimitBytes must be > 0");
        this.blobs = blobs;
        this.softLimitBytes = softLimitBytes;
    }

    public int softLimitBytes() {
        return softLimitBytes;
    }

    /**
     * Where diverted payloads went.
     *
     * <p>Exposed because a diverted blob outlives the document that referenced it: retiring a
     * superseded run deletes the row, and nothing deletes the artifact under
     * {@code repo-overflow/}. So the sweeper that reclaims them needs the same store the diversion
     * used — the credential-guarded one — rather than building a second view of it and risking a
     * different root prefix.
     */
    public ArtifactStore blobs() {
        return blobs;
    }

    /** The fan-out root of diverted payloads, as a key. */
    public static ArtifactKey overflowRoot() {
        return ArtifactKey.of(OVERFLOW_ROOT);
    }

    /**
     * Measures {@code json} and returns the fields to store for it.
     *
     * <p>Measured in <b>encoded bytes, not characters</b>. A payload of CJK identifiers or of
     * PL/SQL string literals with non-ASCII content is up to three times longer in UTF-8 than in
     * {@code String.length()}, and BSON counts bytes — so measuring characters would let a document
     * a third over the limit through, which is the one input class where a "safe" 12 MB reading
     * turns into a real 16 MB failure.
     *
     * @param json the payload, already serialised
     * @return a {@link Document} carrying either {@link #F_JSON} or {@link #F_BLOB_KEY}, plus
     *         {@link #F_LEN}
     */
    public Document store(String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        Document out = new Document(F_LEN, (long) bytes.length);
        if (bytes.length <= softLimitBytes) {
            return out.append(F_JSON, json);
        }
        String sha = sha256Hex(bytes);
        ArtifactKey key = overflowKey(sha);
        // exists() first, because content addressing means a repeat is the common case and
        // rewriting identical bytes is pure cost. It is safe: the key IS the hash of the content,
        // so a hit cannot be a different payload.
        if (!blobs.exists(key)) {
            try (OutputStream o = blobs.write(key, bytes.length)) {
                o.write(bytes);
            }
        }
        log.info("[repo] payload of {} bytes exceeds the {} byte soft limit; diverted to artifact {} "
                 + "(not truncated)", bytes.length, softLimitBytes, key);
        return out.append(F_BLOB_KEY, key.toString()).append(F_SHA256, sha);
    }

    /**
     * Reads back what {@link #store} wrote, from wherever it went.
     *
     * <p>The caller cannot tell the two paths apart, which is the property that makes the diversion
     * safe to apply automatically: no read path needs a branch, so none can forget one.
     */
    public String load(Document stored) throws IOException {
        if (stored == null) return null;
        String inline = stored.getString(F_JSON);
        if (inline != null) return inline;
        String key = stored.getString(F_BLOB_KEY);
        if (key == null) return null;
        try (InputStream in = blobs.read(keyOf(key))) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** True when this payload was diverted rather than stored inline. */
    public static boolean diverted(Document stored) {
        return stored != null && stored.getString(F_BLOB_KEY) != null;
    }

    /** {@code repo-overflow/<first two hex chars>/<sha256>}. */
    static ArtifactKey overflowKey(String sha256Hex) {
        return ArtifactKey.of(OVERFLOW_ROOT, sha256Hex.substring(0, 2), sha256Hex);
    }

    /** Parses a stored key string back into an {@link ArtifactKey}. */
    static ArtifactKey keyOf(String slashJoined) {
        String[] segs = slashJoined.split("/");
        return ArtifactKey.of(segs);
    }

    static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JRE", e);
        }
    }
}
