package com.analyzer.storage.mongo;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * MongoDB-backed {@link ArtifactStore}. PLAN-07 §1.
 *
 * <p>One class behind the frozen interface, mirroring {@code s3/S3ArtifactStore}. Nothing outside
 * this package knows MongoDB is involved, and — the point of PLAN-06 §"GridFS is not used" —
 * nothing outside this class knows GridFS is involved either. There remains exactly one blob
 * concept in the design, {@code ArtifactStore}; GridFS is how this one backend stores bytes, the
 * same way multipart upload is how the S3 backend stores bytes.
 *
 * <h2>The layout</h2>
 *
 * <pre>
 * &lt;db&gt;.artifacts              one manifest document per artifact
 * &lt;db&gt;.artifacts_fs.files     GridFS, for artifacts above the inline threshold
 * &lt;db&gt;.artifacts_fs.chunks
 * </pre>
 *
 * A manifest carries {@code _id} (the key string), {@code parent}, {@code leaf}, {@code len},
 * {@code mtime}, and <b>exactly one</b> of {@code inline} (a {@code BinData} payload) or
 * {@code gridfsId}. Hybrid rather than GridFS-for-everything because the size distribution demands
 * it: PLAN-01 measured 560 {@code node_<n>.json} files averaging 2,050 B alongside a 70 MB
 * {@code stored.jar}, and paying two documents and two round trips for a 2 KB file 560 times is the
 * cost that made the old per-file loops slow.
 *
 * <h2>Keys are the S3 key string, verbatim</h2>
 *
 * {@code _id} is {@link ArtifactKey#s3Key(String)} — no new method on the frozen package's public
 * surface, and the same string S3 mode uses, so with both prefixes defaulting to {@code data} a
 * mode flip needs no key rewriting (PLAN-09 §5). A prefix scan is then a pure {@code _id} index
 * range, {@code [p + "/", p + "0")}: {@code '/'} is 0x2F and {@code '0'} is 0x30, its immediate
 * successor, so the half-open range is exactly the subtree and a sibling prefix
 * ({@code data/ab/...} under a scan of {@code data/a}) cannot leak in. An anchored {@code $regex}
 * would also be index-eligible, but {@code ArtifactKey} permits {@code $}, {@code [} and {@code (}
 * in a segment, and range bounds have no escaping problem at all.
 *
 * <h2>The projection rule — non-obvious and load-bearing</h2>
 *
 * <p>{@code list}, {@code statPrefix}, {@code exists}, {@code size} and {@code lastModified}
 * <b>must</b> project {@code inline} away. S3 cannot make this mistake because metadata and body
 * are separate operations there; Mongo puts them in one document, so the discipline has to be
 * explicit. Without it a {@code statPrefix} over the observed 812-file / 5.4 MB endpoint tree drags
 * all 5.4 MB across the wire per call, and at a 1 MiB threshold the worst case is gigabytes. Every
 * metadata query below therefore names its fields, and {@link #statPrefix} deliberately overrides
 * the interface default, which would call {@code list} and inherit whatever projection that used.
 *
 * <h2>Where this backend cannot match {@code LocalArtifactStore}</h2>
 *
 * <p>Listed in {@code MongoArtifactStoreTest} as executable notes, and summarised here:
 *
 * <ol>
 *   <li><b>A ranged read of an inline artifact transfers the whole payload</b> and slices
 *       client-side — Mongo has no server-side byte range on a {@code BinData} field. Semantically
 *       identical, not performance-identical, and bounded by the threshold. Above the threshold the
 *       range <em>is</em> genuine: {@code GridFSDownloadStream.skip} is chunk-aware, so an offset
 *       deep into a 900 MB artifact does not transfer the preceding bytes.</li>
 *   <li><b>Non-recursive listing is O(subtree), not O(children)</b> — there is no equivalent of
 *       S3's {@code delimiter}, so pseudo-directories come from an index-covered {@code $group}
 *       over every key under the prefix (§1.6).</li>
 *   <li><b>{@code copy} of a large artifact costs bytes</b> where local uses {@code Files.copy} and
 *       S3 uses {@code CopyObject} (see {@link #copy}).</li>
 *   <li><b>Orphaned GridFS chunks</b> are possible after an abandoned large write; local removes
 *       its temp file instead. Collectable, see {@link #collectOrphanedGridFsFiles()}.</li>
 *   <li><b>{@code exists}/{@code size}/{@code lastModified} cannot distinguish "absent" from
 *       "Mongo unreachable"</b> and answer {@code false}/{@code -1} for both, because the interface
 *       gives them no way to say otherwise. S3 has the identical problem and lives with it. This is
 *       the only one of the five that is a behavioural difference a caller could observe, so it is
 *       logged at WARN with the key rather than swallowed.</li>
 * </ol>
 *
 * <p>None of these is asserted by {@code ArtifactStoreConformanceTest}, and none needs to be: the
 * suite pins semantics, and the semantics match.
 *
 * <h2>Credentials</h2>
 *
 * Never handled here. The {@link MongoClient} is built in {@code StorageFactory} from a
 * credential-free URI, and {@code StorageFactory.create} wraps this store in
 * {@code CredentialGuardStore} so C4 holds for this backend the same way it holds for the other
 * two — by construction, not by this class remembering.
 */
public class MongoArtifactStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(MongoArtifactStore.class);

    /** PLAN-07 §1.1: above every artifact PLAN-01 classifies as "safe as one document". */
    public static final int DEFAULT_INLINE_THRESHOLD = 1024 * 1024;

    /**
     * Hard ceiling on the inline threshold, validated at construction.
     *
     * <p>Deliberately below PLAN-06's 12 MB soft limit for entity documents so the two guards
     * cannot disagree, and far below the 16 MB BSON limit so the manifest — bounded at
     * {@code inlineThreshold + ~200 B} by construction — has no way to reach it. C2 is satisfied
     * structurally: anything larger is in GridFS, which shards into 255 KB chunk documents, so the
     * BSON limit applies to no document this class writes.
     */
    public static final int MAX_INLINE_THRESHOLD = 8 * 1024 * 1024;

    /** GridFS deletes are one round trip each; batch the logging, not the calls. */
    private static final int GRIDFS_DELETE_LOG_EVERY = 1_000;

    /** Metadata-only projection. The `inline` payload is excluded on purpose — see the class doc. */
    private static final Bson META = Projections.include("_id", "parent", "leaf", "len", "mtime");

    private final MongoDatabase db;
    private final MongoCollection<Document> manifests;
    private final GridFSBucket gridFs;
    private final String prefix;
    private final int inlineThreshold;
    private final long listScanWarnThreshold;

    public MongoArtifactStore(MongoDatabase database, String prefix) {
        this(database, prefix, DEFAULT_INLINE_THRESHOLD, "artifacts", 100_000L);
    }

    /**
     * Takes a {@link MongoDatabase} rather than a client plus a name, so the connection pool is
     * owned by {@link MongoConnection} and shared with {@link MongoAnalysisRepository} — the two are
     * selected by independent properties and both can be on at once.
     */
    public MongoArtifactStore(MongoDatabase database, String prefix,
                              int inlineThreshold, String collection, long listScanWarnThreshold) {
        if (database == null) throw new IllegalArgumentException("mongo database is required");
        // Refuse to boot rather than discover it on the first oversized artifact. A threshold of 0
        // would put every 191-byte header in GridFS; one above the ceiling would let a manifest
        // approach the BSON limit.
        if (inlineThreshold <= 0 || inlineThreshold > MAX_INLINE_THRESHOLD) {
            throw new IllegalArgumentException(
                    "app.storage.mongodb.inline-threshold must be > 0 and <= " + MAX_INLINE_THRESHOLD
                    + " bytes, was " + inlineThreshold);
        }
        this.db = database;
        this.manifests = db.getCollection(collection);
        this.gridFs = GridFSBuckets.create(db, collection + "_fs");
        this.prefix = normalizePrefix(prefix);
        this.inlineThreshold = inlineThreshold;
        this.listScanWarnThreshold = listScanWarnThreshold;
    }

    /** Trims blanks and any leading/trailing '/' so key joining has exactly one separator. */
    static String normalizePrefix(String raw) {
        if (raw == null) return "";
        String p = raw.trim();
        while (p.startsWith("/")) p = p.substring(1);
        while (p.endsWith("/")) p = p.substring(0, p.length() - 1);
        return p;
    }

    /**
     * Creates the one index this store needs beyond the free {@code _id} index.
     *
     * <p>Separate from the constructor so a deployment can build indexes once and boot with
     * {@code app.storage.mongodb.index-ensure=never} thereafter — PLAN-09 §5 notes that building an
     * index after a multi-million-row load is a very different operation from creating an empty one.
     *
     * <p>Only {@code parent} is needed. Recursive listing, {@code statPrefix}, {@code deletePrefix}
     * and every single-key operation ride the {@code _id} index; {@code parent} exists solely for
     * the immediate-file-children half of a non-recursive listing, which would otherwise be a scan.
     */
    public void ensureIndexes() {
        try {
            manifests.createIndex(Indexes.ascending("parent"),
                    new IndexOptions().name("artifacts_parent").background(true));
            log.info("[mongo] indexes ensured on {}.{}", db.getName(), manifests.getNamespace().getCollectionName());
        } catch (MongoException e) {
            log.warn("[mongo] could not ensure indexes on {}: {}", manifests.getNamespace(), e.getMessage());
        }
    }

    public String databaseName() {
        return db.getName();
    }

    /** The configured root prefix, without surrounding slashes. */
    public String rootPrefix() {
        return prefix;
    }

    /** Bytes at or below this live in the manifest document; above it, in GridFS. */
    public int inlineThreshold() {
        return inlineThreshold;
    }

    // ── key mapping ──────────────────────────────────────────────────────────

    /** Manifest {@code _id} for an artifact — the S3 key string, reused verbatim. */
    String docId(ArtifactKey key) {
        return key.s3Key(prefix);
    }

    /** Subtree bound for a "directory" key. Always ends in '/' so siblings cannot leak in. */
    String listPrefix(ArtifactKey key) {
        return docId(key) + "/";
    }

    /**
     * The subtree of {@code p}, as a half-open {@code _id} range.
     *
     * <p>{@code '0'} (0x30) is the immediate successor of {@code '/'} (0x2F), so
     * {@code [p + "/", p + "0")} is precisely the keys under {@code p} and nothing else.
     */
    static Bson underPrefix(String docId) {
        return Filters.and(Filters.gte("_id", docId + "/"), Filters.lt("_id", docId + "0"));
    }

    // ── writes ───────────────────────────────────────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) {
        return write(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) {
        ArtifactKey parent = key.parent();
        return new GridFsRolloverOutputStream(manifests, gridFs, docId(key),
                parent == null ? "" : docId(parent), key.leaf(),
                inlineThreshold, contentLength);
    }

    // ── reads ────────────────────────────────────────────────────────────────

    /** @return the manifest with the payload locator, or null when the artifact does not exist. */
    private Document payloadDoc(ArtifactKey key) throws IOException {
        try {
            return manifests.find(Filters.eq("_id", docId(key)))
                    .projection(Projections.include("len", "inline", "gridfsId"))
                    .first();
        } catch (MongoException e) {
            throw new IOException("MongoDB read failed for " + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        Document d = payloadDoc(key);
        if (d == null) throw new NoSuchFileException(key.toString());
        Binary inline = d.get("inline", Binary.class);
        if (inline != null) return new ByteArrayInputStream(inline.getData());
        try {
            return gridFs.openDownloadStream(d.getObjectId("gridfsId"));
        } catch (MongoException e) {
            throw new IOException("GridFS read failed for " + key + ": " + e.getMessage(), e);
        }
    }

    /**
     * Partial read. <b>A genuine ranged read above the inline threshold</b>, which is where it
     * matters: {@code GridFSDownloadStream.skip} is chunk-aware — the driver recomputes the chunk
     * index and re-issues its cursor from there — so an offset deep into a 900 MB artifact does not
     * transfer the preceding bytes. It over-reads at most one chunk (255 KB) at the front.
     *
     * <p>At or below the threshold the payload is one {@code BinData} field with no server-side
     * range, so it comes back whole and is sliced here. Correct, and bounded by the threshold.
     *
     * <p>Three contract corners have to be reproduced by hand, because Mongo does none of them:
     * a negative offset is rejected before any I/O; an offset past the end raises
     * {@link EOFException} to match {@code LocalArtifactStore}'s {@code skipNBytes} and S3's
     * translated 416, which GridFS would otherwise answer with a silently short stream; and a zero
     * length yields an immediately-empty stream that still closes what it owns.
     */
    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0, was " + offset);
        Document d = payloadDoc(key);
        if (d == null) throw new NoSuchFileException(key.toString());
        long len = d.get("len", Number.class).longValue();
        if (offset > len) {
            throw new EOFException("offset " + offset + " past end (" + len + ") of " + key);
        }
        long available = len - offset;
        long want = length < 0 ? available : Math.min(length, available);

        Binary inline = d.get("inline", Binary.class);
        if (inline != null) {
            byte[] all = inline.getData();
            return new ByteArrayInputStream(all, (int) offset, (int) Math.min(want, all.length - offset));
        }
        if (want == 0) {
            // Still open the stream so its close() path is exercised identically, then report EOF.
            return new EmptyOnTop(gridFs.openDownloadStream(d.getObjectId("gridfsId")));
        }
        InputStream in = gridFs.openDownloadStream(d.getObjectId("gridfsId"));
        try {
            skipFully(in, offset);
            return new BoundedInputStream(in, want);
        } catch (IOException | RuntimeException e) {
            closeQuietly(in);
            throw e;
        }
    }

    /**
     * {@code InputStream.skip} may return short for reasons of its own; GridFS returns short at
     * a chunk boundary. Loop, and treat a genuine EOF as one — the caller has already checked
     * {@code offset} against the recorded length, so reaching EOF here means the manifest and the
     * chunks disagree, which is worth an exception rather than a short read.
     */
    private static void skipFully(InputStream in, long n) throws IOException {
        long remaining = n;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped > 0) {
                remaining -= skipped;
                continue;
            }
            if (in.read() < 0) {
                throw new EOFException("GridFS payload ended " + remaining + " bytes before offset " + n);
            }
            remaining--;
        }
    }

    // ── metadata ─────────────────────────────────────────────────────────────

    /**
     * One metadata lookup, {@code inline} projected away.
     *
     * <p>Returns null both when the artifact is absent and when Mongo could not be reached, because
     * {@code exists}/{@code size}/{@code lastModified} do not declare {@link IOException} and so
     * have no way to distinguish them — the same compromise {@code S3ArtifactStore} makes when it
     * treats a 404 as an answer. Unreachability is at least logged rather than silent.
     */
    private Document meta(ArtifactKey key) {
        try {
            return manifests.find(Filters.eq("_id", docId(key))).projection(META).first();
        } catch (MongoException e) {
            log.warn("[mongo] metadata lookup failed for {} — reporting as absent: {}",
                    key, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean exists(ArtifactKey key) {
        return meta(key) != null;
    }

    @Override
    public long size(ArtifactKey key) {
        Document d = meta(key);
        return d == null ? -1 : d.get("len", Number.class).longValue();
    }

    @Override
    public long lastModified(ArtifactKey key) {
        Document d = meta(key);
        if (d == null) return -1;
        Date mtime = d.getDate("mtime");
        return mtime == null ? -1 : mtime.getTime();
    }

    // ── deletes ──────────────────────────────────────────────────────────────

    /**
     * Deletes the manifest first, then the payload.
     *
     * <p>That order matters: the manifest is what makes an artifact visible, so removing it first
     * means the artifact is gone the instant the operation is acknowledged. A crash between the two
     * leaves an orphaned GridFS file — space, not a correctness problem, and collectable. The
     * reverse order would leave a visible manifest pointing at deleted chunks, which is a read
     * error rather than a leak.
     */
    @Override
    public boolean delete(ArtifactKey key) {
        Document removed;
        try {
            removed = manifests.findOneAndDelete(Filters.eq("_id", docId(key)),
                    new FindOneAndDeleteOptions().projection(Projections.include("gridfsId")));
        } catch (MongoException e) {
            log.warn("[mongo] delete failed for {}: {}", key, e.getMessage());
            return false;
        }
        if (removed == null) return false;
        deleteGridFsQuietly(removed.getObjectId("gridfsId"), key.toString());
        return true;
    }

    @Override
    public int deletePrefix(ArtifactKey prefixKey) throws IOException {
        String p = listPrefix(prefixKey);
        Bson range = underPrefix(docId(prefixKey));
        List<ObjectId> payloads = new ArrayList<>();
        try {
            // One pass to learn which payloads to collect, then one deleteMany. Not atomic across
            // documents — neither is S3's batched DeleteObjects, nor a local recursive walk.
            try (MongoCursor<Document> c = manifests.find(range)
                    .projection(Projections.include("gridfsId")).iterator()) {
                while (c.hasNext()) {
                    ObjectId id = c.next().getObjectId("gridfsId");
                    if (id != null) payloads.add(id);
                }
            }
            long deleted = manifests.deleteMany(range).getDeletedCount();
            int n = 0;
            for (ObjectId id : payloads) {
                deleteGridFsQuietly(id, p);
                if (++n % GRIDFS_DELETE_LOG_EVERY == 0) {
                    log.info("[mongo] deletePrefix({}) removed {} GridFS payloads so far", p, n);
                }
            }
            return (int) deleted;
        } catch (MongoException e) {
            throw new IOException("MongoDB deletePrefix failed for " + prefixKey + ": " + e.getMessage(), e);
        }
    }

    private void deleteGridFsQuietly(ObjectId id, String what) {
        if (id == null) return;
        try {
            gridFs.delete(id);
        } catch (MongoException e) {
            log.warn("[mongo] orphaned GridFS file {} for {}: {}", id, what, e.getMessage());
        }
    }

    // ── listing ──────────────────────────────────────────────────────────────

    /**
     * Lazy, one query, off a real cursor.
     *
     * <p>Recursive is the easy and the common case: the {@code _id} range, sorted by {@code _id}
     * (free — it is the index order), streamed a batch at a time. Nothing is materialised, so a
     * caller that takes the first ten entries of a million-key prefix pays for one batch, and
     * closing the stream closes the cursor.
     *
     * <p>Non-recursive is where S3's {@code delimiter="/"} has no Mongo equivalent, so it is two
     * queries: an indexed equality on {@code parent} for the immediate files, and an index-covered
     * {@code $group} for the pseudo-directories. <b>The honest cost:</b> that {@code $group} reads
     * every key under the prefix, not just the immediate children — O(subtree) where S3 is
     * O(children). It touches only the {@code _id} index, so it is cheap per key, and for the
     * shallow directories the UI actually lists non-recursively (812 keys under
     * {@code jar/<id>/endpoints/}) it is nothing. Above
     * {@code app.storage.mongodb.list-scan-warn-threshold} it logs, so if it ever becomes real we
     * find out from production rather than from a guess.
     */
    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefixKey, boolean recursive) throws IOException {
        String id = docId(prefixKey);
        try {
            if (recursive) {
                FindIterable<Document> it = manifests.find(underPrefix(id))
                        .projection(META).sort(Sorts.ascending("_id"));
                return cursorStream(it, d -> entryOf(prefixKey, id + "/", d));
            }
            return Stream.concat(immediateFiles(prefixKey, id), immediateDirectories(prefixKey, id));
        } catch (MongoException e) {
            throw new IOException("MongoDB list failed for " + prefixKey + ": " + e.getMessage(), e);
        }
    }

    private Stream<ArtifactEntry> immediateFiles(ArtifactKey prefixKey, String id) {
        FindIterable<Document> it = manifests.find(Filters.eq("parent", id))
                .projection(META).sort(Sorts.ascending("_id"));
        return cursorStream(it, d -> entryOf(prefixKey, id + "/", d));
    }

    /**
     * Pseudo-directories: the distinct first extra path segment among the subtree's keys.
     *
     * <p>Splits {@code _id} on {@code '/'} and takes the element just past the prefix, keeping only
     * keys that have something after it — a key with exactly one extra segment is a file and was
     * already returned by {@link #immediateFiles}. Splitting rather than {@code $substrBytes} is
     * deliberate: {@code ArtifactKey} guarantees no segment contains {@code '/'}, so the split is
     * exact, and it avoids depending on {@code $substrBytes}'s negative-length behaviour.
     *
     * <p>Reported as {@code new ArtifactEntry(key, 0, -1, true)} — identical to
     * {@code S3ArtifactStore}'s common-prefix entry, which is what lets
     * {@code listNonRecursiveReturnsImmediateChildren} pass unchanged against both.
     */
    private Stream<ArtifactEntry> immediateDirectories(ArtifactKey prefixKey, String id) {
        int depth = id.isEmpty() ? 0 : id.split("/").length;
        List<Bson> pipeline = List.of(
                Aggregates.match(underPrefix(id)),
                Aggregates.addFields(new Field<>("_parts", new Document("$split", List.of("$_id", "/")))),
                Aggregates.match(Filters.exists("_parts." + (depth + 1))),
                Aggregates.group(new Document("$arrayElemAt", List.of("$_parts", depth))));
        Iterator<Document> raw = manifests.aggregate(pipeline).iterator();
        MongoCursor<Document> cursor = (MongoCursor<Document>) raw;
        long[] seen = {0};
        return cursorStream(cursor, d -> {
            if (++seen[0] == listScanWarnThreshold) {
                log.warn("[mongo] non-recursive list of '{}' has grouped over {} keys; this scan is "
                         + "O(subtree), not O(children). Raise or investigate "
                         + "app.storage.mongodb.list-scan-warn-threshold.", id, listScanWarnThreshold);
            }
            Object name = d.get("_id");
            if (!(name instanceof String s) || s.isEmpty()) return null;
            return new ArtifactEntry(prefixKey.resolve(s), 0, -1, true);
        });
    }

    /** Manifest document to {@link ArtifactEntry}; null when the key is not under the prefix. */
    private static ArtifactEntry entryOf(ArtifactKey base, String listPrefix, Document d) {
        ArtifactKey k = relativeKey(base, listPrefix, d.getString("_id"));
        if (k == null) return null;
        Date mtime = d.getDate("mtime");
        Number len = d.get("len", Number.class);
        return new ArtifactEntry(k, len == null ? 0 : len.longValue(),
                mtime == null ? -1 : mtime.getTime(), false);
    }

    /** Turns a manifest {@code _id} back into an {@link ArtifactKey} under {@code base}. */
    private static ArtifactKey relativeKey(ArtifactKey base, String listPrefix, String docId) {
        if (docId == null || !docId.startsWith(listPrefix)) return null;
        String rel = docId.substring(listPrefix.length());
        if (rel.isEmpty()) return null;
        ArtifactKey key = base;
        for (String seg : rel.split("/")) {
            if (seg.isEmpty()) return null;
            key = key.resolve(seg);
        }
        return key;
    }

    /**
     * Adapts a Mongo cursor to a lazy {@link Stream}, dropping entries the mapper rejects.
     *
     * <p>{@code onClose} is what makes the caller's try-with-resources release the server-side
     * cursor; without it an abandoned listing holds one open until it times out.
     */
    private static <T> Stream<ArtifactEntry> cursorStream(Iterable<Document> it,
                                                          java.util.function.Function<Document, ArtifactEntry> mapper) {
        MongoCursor<Document> cursor = ((com.mongodb.client.MongoIterable<Document>) it).iterator();
        return cursorStream(cursor, mapper);
    }

    private static Stream<ArtifactEntry> cursorStream(MongoCursor<Document> cursor,
                                                      java.util.function.Function<Document, ArtifactEntry> mapper) {
        Iterator<ArtifactEntry> mapped = new Iterator<>() {
            private ArtifactEntry next;

            @Override
            public boolean hasNext() {
                while (next == null && cursor.hasNext()) {
                    next = mapper.apply(cursor.next());
                }
                return next != null;
            }

            @Override
            public ArtifactEntry next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                ArtifactEntry n = next;
                next = null;
                return n;
            }
        };
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(mapped, Spliterator.ORDERED | Spliterator.NONNULL),
                        false)
                .onClose(cursor::close);
    }

    /**
     * ONE query for the whole subtree's metadata, {@code inline} projected away.
     *
     * <p>This override is not optional. The interface default calls {@link #list}, and while this
     * class's {@code list} happens to project correctly, inheriting the default would leave that
     * as an invariant two methods away from where it is needed. The method exists because a
     * loop of per-key lookups collapses on remote storage; ~0.3 ms per {@code find} is far better
     * than S3's ~25 ms HEAD but still ~30× a local {@code Files.exists}, so the argument for one
     * round trip holds here too.
     */
    @Override
    public Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefixKey) throws IOException {
        String id = docId(prefixKey);
        String p = id + "/";
        Map<ArtifactKey, ArtifactEntry> out = new LinkedHashMap<>();
        try (MongoCursor<Document> c = manifests.find(underPrefix(id))
                .projection(META).sort(Sorts.ascending("_id")).iterator()) {
            while (c.hasNext()) {
                ArtifactEntry e = entryOf(prefixKey, p, c.next());
                if (e != null) out.put(e.key(), e);
            }
        } catch (MongoException e) {
            throw new IOException("MongoDB statPrefix failed for " + prefixKey + ": " + e.getMessage(), e);
        }
        return out;
    }

    // ── copy / move / materialize ────────────────────────────────────────────

    /**
     * MongoDB has no {@code CopyObject}, so there are two paths.
     *
     * <p><b>Inline source:</b> a server-side {@code $merge} aggregation. The bytes never reach this
     * process — one round trip, no transfer, which is the same class of operation as S3's
     * {@code CopyObject}.
     *
     * <p><b>GridFS source:</b> stream {@code read(from)} into {@code write(to)}, costing 2× the
     * bytes over the wire. The alternative — a server-side chunk-copy aggregation on
     * {@code .chunks} — was considered and rejected for now: it doubles storage just the same, it
     * is not atomic, and {@code copy} is used for small sidecars today. What it must <b>not</b> do
     * is share one {@code gridfsId} between two manifests, which would be free and is a latent
     * data-loss bug: deleting either artifact would delete the other's bytes.
     *
     * <p>{@link #move} is the operation that actually matters on a large artifact
     * ({@code PersistenceService.rotateCorrected} moves a potentially ~1 GB
     * {@code analysis_corrected.json}) and it needs no byte copy at all.
     */
    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        String fromId = docId(from);
        Document src = meta(from);
        if (src == null) throw new NoSuchFileException(from.toString());

        ArtifactKey toParent = to.parent();
        String toParentId = toParent == null ? "" : docId(toParent);

        Document payload = payloadDoc(from);
        if (payload != null && payload.get("gridfsId") != null) {
            streamCopy(from, to);
            return;
        }
        try {
            manifests.aggregate(List.of(
                    Aggregates.match(Filters.eq("_id", fromId)),
                    Aggregates.set(
                            new Field<>("_id", docId(to)),
                            new Field<>("parent", toParentId),
                            new Field<>("leaf", to.leaf()),
                            new Field<>("mtime", "$$NOW")),
                    Aggregates.merge(manifests.getNamespace().getCollectionName(),
                            new com.mongodb.client.model.MergeOptions()
                                    .whenMatched(com.mongodb.client.model.MergeOptions.WhenMatched.REPLACE)
                                    .whenNotMatched(com.mongodb.client.model.MergeOptions.WhenNotMatched.INSERT))
            )).toCollection();
        } catch (MongoException e) {
            throw new IOException("MongoDB copy failed for " + from + " -> " + to + ": " + e.getMessage(), e);
        }
    }

    /** The always-correct fallback: through this process, streaming, never buffered whole. */
    private void streamCopy(ArtifactKey from, ArtifactKey to) throws IOException {
        try (InputStream in = read(from); OutputStream out = write(to)) {
            in.transferTo(out);
        }
    }

    /**
     * Rename with <b>zero byte movement</b>, even for a 1 GB artifact — the one place this backend
     * is genuinely better than the other two.
     *
     * <p>The payload is addressed by {@code gridfsId}, not by the key, so a move is a manifest
     * re-{@code _id}: write the manifest at the destination pointing at the <em>same</em> payload,
     * then delete the source manifest <b>without</b> touching the payload. Local needs
     * {@code Files.move} (cheap only within a filesystem) and S3 needs a full {@code CopyObject}
     * plus a delete.
     *
     * <p>Order is destination-first so a crash in between leaves the artifact readable at both keys
     * rather than at neither. Two manifests transiently share a {@code gridfsId}, which is exactly
     * why the source manifest is removed with a direct {@code deleteOne} and not with
     * {@link #delete}: {@code delete} would collect the payload and empty the destination.
     */
    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        Document src;
        try {
            src = manifests.find(Filters.eq("_id", docId(from))).first();
        } catch (MongoException e) {
            throw new IOException("MongoDB move failed reading " + from + ": " + e.getMessage(), e);
        }
        if (src == null) throw new NoSuchFileException(from.toString());

        ArtifactKey toParent = to.parent();
        Document dst = new Document(src);
        dst.put("_id", docId(to));
        dst.put("parent", toParent == null ? "" : docId(toParent));
        dst.put("leaf", to.leaf());

        try {
            Document displaced = manifests.findOneAndReplace(
                    Filters.eq("_id", docId(to)), dst,
                    new FindOneAndReplaceOptions().upsert(true)
                            .returnDocument(ReturnDocument.BEFORE)
                            .projection(Projections.include("gridfsId")));
            manifests.deleteOne(Filters.eq("_id", docId(from)));
            if (displaced != null) {
                ObjectId old = displaced.getObjectId("gridfsId");
                if (old != null && !old.equals(src.getObjectId("gridfsId"))) {
                    deleteGridFsQuietly(old, to.toString());
                }
            }
        } catch (MongoException e) {
            throw new IOException("MongoDB move failed for " + from + " -> " + to + ": " + e.getMessage(), e);
        }
    }

    /**
     * Downloads to a temp file. {@code temporary()} is always true, so {@code close()} always
     * deletes — identical to {@code S3ArtifactStore}, and the reason the conformance suite's
     * {@code materializedArtifactSurvivesCloseWhenNotTemporary} is written as a conditional.
     */
    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        // Keep the leaf name: JarFile and CFR both care about the .jar suffix, and ArtifactKey
        // guarantees the leaf has no separators.
        Path temp = Files.createTempFile("artifact-", "-" + key.leaf());
        try (InputStream in = read(key)) {
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        return new LocalHandle() {
            @Override public Path path() { return temp; }
            @Override public boolean temporary() { return true; }
            @Override public void close() {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException e) {
                    log.warn("Could not delete materialized temp {}: {}", temp, e.getMessage());
                }
            }
        };
    }

    // ── orphan collection ────────────────────────────────────────────────────

    /**
     * Deletes GridFS files no manifest references.
     *
     * <p>The one housekeeping duty local storage does not have. Orphans arise from an abandoned
     * large write (the chunks are durable, the manifest was never written) or a crash between
     * deleting a manifest and deleting its payload. Neither is a correctness problem — an orphan is
     * unreachable through every method on the interface — so this is an operator action, not
     * something a read path triggers.
     *
     * <p>Not called from anywhere yet, and deliberately so: a sweeper that runs concurrently with a
     * long write would have to distinguish "abandoned" from "still being written", which needs an
     * age threshold and a decision about what age is safe. Exposed with the threshold as a
     * parameter so that decision belongs to whoever schedules it.
     *
     * @param olderThanMillis only collect files whose upload began this long ago, so a write in
     *                        progress is never collected
     * @return the number of GridFS files removed
     */
    public int collectOrphanedGridFsFiles(long olderThanMillis) {
        long cutoffMillis = System.currentTimeMillis() - Math.max(0, olderThanMillis);
        Date cutoff = new Date(cutoffMillis);
        int removed = 0;

        // Class 1: a complete GridFS file (.files document present) that no manifest references.
        // Left by a write that rolled over and closed its upload but whose manifest write failed,
        // or by a crash between deleting a manifest and deleting its payload.
        for (GridFSFile f : gridFs.find(Filters.lt("uploadDate", cutoff))) {
            if (isReferenced(f.getObjectId())) continue;
            deleteGridFsQuietly(f.getObjectId(), "orphan sweep (unreferenced file)");
            removed++;
        }

        // Class 2: chunks with NO .files document at all. This is the abandoned-write case and it
        // is invisible to gridFs.find(), which reads .files — so a sweeper written against the
        // bucket API alone silently leaves these forever. They arise whenever a large write is
        // abandoned after enough bytes to fill a 255 KB chunk but before close(), which for a
        // 912 MB analysis.json is almost the whole write.
        //
        // uploadDate does not exist here either, so the age comes from the files_id ObjectId's own
        // embedded timestamp. That is what makes "do not collect a write still in progress" work
        // without a second bookkeeping collection.
        MongoCollection<Document> chunks = db.getCollection(
                gridFs.getBucketName() + ".chunks");
        MongoCollection<Document> files = db.getCollection(
                gridFs.getBucketName() + ".files");
        // Typed as ObjectId because this class is the only writer of this bucket and always mints
        // an ObjectId for the file id (see GridFsRolloverOutputStream.rollOver). GridFS permits any
        // BSON value there, so a bucket shared with another producer would need a wider type — but
        // sharing the bucket would break the "the manifest is the only thing that makes an artifact
        // visible" invariant anyway, so it is not a supported configuration.
        for (ObjectId id : chunks.distinct("files_id", ObjectId.class)) {
            // '>' not '>=': an ObjectId timestamp is truncated to whole seconds, so with a zero
            // window (a test, or a deliberate full sweep) it can land exactly on the cutoff.
            if (id.getDate().getTime() > cutoffMillis) continue;   // too recent to judge
            if (files.find(Filters.eq("_id", id)).projection(Projections.include("_id")).first() != null) {
                continue;   // a real file; class 1 above already judged it
            }
            if (isReferenced(id)) continue;
            try {
                chunks.deleteMany(Filters.eq("files_id", id));
                removed++;
            } catch (MongoException e) {
                log.warn("[mongo] could not delete orphaned chunks for {}: {}", id, e.getMessage());
            }
        }

        if (removed > 0) log.info("[mongo] collected {} orphaned GridFS payload(s)", removed);
        return removed;
    }

    /** True when some manifest still points at this payload, in which case it is live, not garbage. */
    private boolean isReferenced(ObjectId gridfsId) {
        return manifests.find(Filters.eq("gridfsId", gridfsId))
                .projection(Projections.include("_id")).first() != null;
    }

    /** Default sweep window: an hour, comfortably longer than any single artifact write. */
    public int collectOrphanedGridFsFiles() {
        return collectOrphanedGridFsFiles(60 * 60 * 1000L);
    }

    // ── stream shims ─────────────────────────────────────────────────────────

    private static void closeQuietly(InputStream in) {
        if (in == null) return;
        try {
            in.close();
        } catch (IOException | RuntimeException ignored) {
            // best effort
        }
    }

    /** Reports EOF immediately, but still owns and closes the underlying download stream. */
    private static final class EmptyOnTop extends InputStream {
        private final InputStream in;
        EmptyOnTop(InputStream in) { this.in = in; }
        @Override public int read() { return -1; }
        @Override public int read(byte[] b, int off, int len) { return -1; }
        @Override public int available() { return 0; }
        @Override public void close() { closeQuietly(in); }
    }

    /**
     * Caps a stream at {@code remaining} bytes, so a ranged read stops where it was asked to.
     *
     * <p>The same shape {@code LocalArtifactStore} uses after its {@code skipNBytes}. Needed
     * because a GridFS download stream has no notion of the caller's window: {@code skip} sets
     * where it starts, nothing bounds where it ends.
     */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = in.read();
            if (b >= 0) remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int n = in.read(b, off, (int) Math.min(len, remaining));
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = in.skip(Math.min(n, remaining));
            remaining -= skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(in.available(), remaining);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
