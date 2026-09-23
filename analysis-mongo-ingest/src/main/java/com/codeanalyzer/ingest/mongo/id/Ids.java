package com.codeanalyzer.ingest.mongo.id;

/**
 * A document identity: the {@code _id} actually stored, and the plaintext key it came from.
 *
 * <p>They differ only when the plaintext exceeds {@link IdCodec#MAX_PLAIN_ID_BYTES} and is hashed.
 * {@code key} is always the readable form and is always stored alongside, so a hashed id never
 * costs you the ability to see what a document is — measured worst cases are a 1,025-byte
 * {@code java_method} id and 1,084 bytes for {@code method_invocation}.
 */
public record Ids(String id, String key) {

    /** True when the id is a hash rather than the key itself. */
    public boolean hashed() {
        return !id.equals(key);
    }
}
