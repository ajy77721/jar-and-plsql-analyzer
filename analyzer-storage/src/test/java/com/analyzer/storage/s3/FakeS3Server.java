package com.analyzer.storage.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A minimal S3 endpoint spoken over real HTTP, backed by an in-memory map.
 *
 * <p><b>Why this exists.</b> Mockito against {@code S3Client} only proves we called the method we
 * meant to call. It cannot catch a wrong Range header, an XML field the SDK will not unmarshal, a
 * multipart sequence assembled in the wrong order, or a pagination loop that drops a page. This
 * fixture puts a real {@code S3Client} — real signing, real HTTP, real XML — in front of storage
 * we can inspect, so those failure modes are actually reachable offline.
 *
 * <p><b>What it is not.</b> A correct S3. It ignores authentication entirely (it never checks a
 * signature), has no versioning, no ACLs, no multi-bucket routing beyond a name check, and no
 * consistency model. Anything about IAM, real credentials or real network behaviour is out of
 * reach here by construction.
 *
 * <p>Implemented on the JDK's {@code com.sun.net.httpserver} so it adds no dependency.
 */
public final class FakeS3Server implements AutoCloseable {

    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /** Stored object: bytes, a modification time, and user metadata (x-amz-meta-*). */
    record Obj(byte[] bytes, Instant modified, Map<String, String> metadata) {}

    private final HttpServer http;
    private final String bucket;

    /** Sorted, because ListObjectsV2 is defined to return keys in lexicographic order. */
    private final NavigableMap<String, Obj> objects = new ConcurrentSkipListMap<>();
    private final Map<String, NavigableMap<Integer, byte[]>> uploads = new ConcurrentHashMap<>();
    /** User metadata captured at CreateMultipartUpload, applied at CompleteMultipartUpload. */
    private final Map<String, Map<String, String>> uploadMetadata = new ConcurrentHashMap<>();
    private final Set<String> abortedUploads = ConcurrentHashMap.newKeySet();

    /** Request counters, so a test can assert "one listing, no HEADs". */
    public final AtomicInteger listRequests = new AtomicInteger();
    public final AtomicInteger headRequests = new AtomicInteger();
    public final AtomicInteger getRequests = new AtomicInteger();
    public final AtomicInteger putRequests = new AtomicInteger();
    public final AtomicInteger uploadPartRequests = new AtomicInteger();
    public final AtomicInteger abortRequests = new AtomicInteger();
    public final AtomicInteger batchDeleteRequests = new AtomicInteger();

    /** Deliberately tiny so the paging path is exercised for real. */
    private volatile int maxKeys = 2;

    /** When > 0, UploadPart with this part number answers 500 — for the abort-on-failure test. */
    private volatile int failPartNumber = 0;

    public FakeS3Server(String bucket) throws IOException {
        this.bucket = bucket;
        this.http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.http.createContext("/", this::dispatch);
        this.http.start();
    }

    public String endpoint() {
        return "http://127.0.0.1:" + http.getAddress().getPort();
    }

    public void maxKeys(int n) {
        this.maxKeys = n;
    }

    public void failPart(int partNumber) {
        this.failPartNumber = partNumber;
    }

    /** Direct view of stored objects, for assertions that should not go through the store. */
    public Set<String> keys() {
        return new LinkedHashSet<>(objects.keySet());
    }

    public byte[] raw(String key) {
        Obj o = objects.get(key);
        return o == null ? null : o.bytes();
    }

    public Set<String> liveUploadIds() {
        return new LinkedHashSet<>(uploads.keySet());
    }

    public Set<String> abortedUploadIds() {
        return new LinkedHashSet<>(abortedUploads);
    }

    public void resetCounters() {
        for (AtomicInteger c : List.of(listRequests, headRequests, getRequests, putRequests,
                uploadPartRequests, abortRequests, batchDeleteRequests)) {
            c.set(0);
        }
    }

    @Override
    public void close() {
        http.stop(0);
    }

    // ── dispatch ────────────────────────────────────────────────────────────

    private void dispatch(HttpExchange ex) throws IOException {
        try {
            route(ex);
        } catch (RuntimeException e) {
            error(ex, 500, "InternalError", String.valueOf(e));
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = URLDecoder.decode(ex.getRequestURI().getRawPath(), StandardCharsets.UTF_8);
        Map<String, String> q = query(ex.getRequestURI().getRawQuery());

        // Path style: /{bucket}/{key...}
        String p = path.startsWith("/") ? path.substring(1) : path;
        String b = p;
        String key = "";
        int slash = p.indexOf('/');
        if (slash >= 0) {
            b = p.substring(0, slash);
            key = p.substring(slash + 1);
        }
        if (!bucket.equals(b)) {
            error(ex, 404, "NoSuchBucket", "no such bucket " + b);
            return;
        }

        if (key.isEmpty()) {
            if ("GET".equals(method) && "2".equals(q.get("list-type"))) {
                listObjectsV2(ex, q);
            } else if ("POST".equals(method) && q.containsKey("delete")) {
                deleteObjects(ex);
            } else {
                error(ex, 400, "BadRequest", method + " on bucket with query " + q.keySet());
            }
            return;
        }

        if ("POST".equals(method) && q.containsKey("uploads")) {
            createMultipartUpload(ex, key);
        } else if ("PUT".equals(method) && q.containsKey("uploadId")) {
            uploadPart(ex, q);
        } else if ("POST".equals(method) && q.containsKey("uploadId")) {
            completeMultipartUpload(ex, key, q.get("uploadId"));
        } else if ("DELETE".equals(method) && q.containsKey("uploadId")) {
            abortMultipartUpload(ex, q.get("uploadId"));
        } else if ("PUT".equals(method)) {
            putObject(ex, key);
        } else if ("GET".equals(method)) {
            getObject(ex, key);
        } else if ("HEAD".equals(method)) {
            headObject(ex, key);
        } else if ("DELETE".equals(method)) {
            deleteObject(ex, key);
        } else {
            error(ex, 405, "MethodNotAllowed", method);
        }
    }

    // ── object operations ───────────────────────────────────────────────────

    private void putObject(HttpExchange ex, String key) throws IOException {
        String copySource = ex.getRequestHeaders().getFirst("x-amz-copy-source");
        if (copySource != null) {
            copyObject(ex, key, copySource);
            return;
        }
        putRequests.incrementAndGet();
        byte[] body = readBody(ex);
        objects.put(key, new Obj(body, Instant.now(), requestMetadata(ex)));
        ex.getResponseHeaders().add("ETag", etag(body));
        ex.sendResponseHeaders(200, -1);
    }

    /** The {@code x-amz-meta-*} user metadata on a request, keyed without the prefix. */
    private static Map<String, String> requestMetadata(HttpExchange ex) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> h : ex.getRequestHeaders().entrySet()) {
            String name = h.getKey();
            if (name != null && name.toLowerCase().startsWith("x-amz-meta-") && !h.getValue().isEmpty()) {
                out.put(name.substring("x-amz-meta-".length()), h.getValue().get(0));
            }
        }
        return out;
    }

    /** CopyObject: a PUT carrying {@code x-amz-copy-source: /bucket/key} instead of a body. */
    private void copyObject(HttpExchange ex, String destKey, String copySource) throws IOException {
        readBody(ex);
        String src = URLDecoder.decode(copySource, StandardCharsets.UTF_8);
        if (src.startsWith("/")) src = src.substring(1);
        if (src.startsWith(bucket + "/")) src = src.substring(bucket.length() + 1);
        Obj o = objects.get(src);
        if (o == null) {
            error(ex, 404, "NoSuchKey", "no such copy source " + src);
            return;
        }
        objects.put(destKey, new Obj(o.bytes(), Instant.now(), requestMetadata(ex)));
        respond(ex, 200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<LastModified>" + ISO.format(Instant.now()) + "</LastModified>"
                + "<ETag>" + escape(etag(o.bytes())) + "</ETag></CopyObjectResult>");
    }

    private void getObject(HttpExchange ex, String key) throws IOException {
        getRequests.incrementAndGet();
        Obj o = objects.get(key);
        if (o == null) {
            error(ex, 404, "NoSuchKey", "no such key " + key);
            return;
        }
        String range = ex.getRequestHeaders().getFirst("Range");
        byte[] body = o.bytes();
        int status = 200;
        if (range != null) {
            Matcher m = Pattern.compile("bytes=(\\d+)-(\\d*)").matcher(range);
            if (!m.matches()) {
                error(ex, 400, "InvalidRange", range);
                return;
            }
            long from = Long.parseLong(m.group(1));
            if (from >= body.length) {
                // Real S3 answers 416 for a start offset past the end of the object.
                error(ex, 416, "InvalidRange", range);
                return;
            }
            long to = m.group(2).isEmpty() ? body.length - 1
                    : Math.min(Long.parseLong(m.group(2)), body.length - 1);
            byte[] slice = new byte[(int) (to - from + 1)];
            System.arraycopy(body, (int) from, slice, 0, slice.length);
            ex.getResponseHeaders().add("Content-Range",
                    "bytes " + from + "-" + to + "/" + body.length);
            body = slice;
            status = 206;
        }
        ex.getResponseHeaders().add("ETag", etag(o.bytes()));
        ex.getResponseHeaders().add("Last-Modified", httpDate(o.modified()));
        emitMetadata(ex, o.metadata());
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = ex.getResponseBody()) {
                out.write(body);
            }
        }
    }

    private void headObject(HttpExchange ex, String key) throws IOException {
        headRequests.incrementAndGet();
        Obj o = objects.get(key);
        if (o == null) {
            // A HEAD has no body, so real S3 gives a bare 404 with no <Error> document — which is
            // exactly why S3ArtifactStore must handle a status-only 404 as well as NoSuchKeyException.
            ex.sendResponseHeaders(404, -1);
            return;
        }
        ex.getResponseHeaders().add("Content-Length", String.valueOf(o.bytes().length));
        ex.getResponseHeaders().add("Last-Modified", httpDate(o.modified()));
        ex.getResponseHeaders().add("ETag", etag(o.bytes()));
        emitMetadata(ex, o.metadata());
        ex.sendResponseHeaders(200, -1);
    }

    private static void emitMetadata(HttpExchange ex, Map<String, String> metadata) {
        if (metadata == null) return;
        metadata.forEach((k, v) -> ex.getResponseHeaders().add("x-amz-meta-" + k, v));
    }

    private void deleteObject(HttpExchange ex, String key) throws IOException {
        objects.remove(key);
        ex.sendResponseHeaders(204, -1);
    }

    private void deleteObjects(HttpExchange ex) throws IOException {
        batchDeleteRequests.incrementAndGet();
        String body = new String(readBody(ex), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("<Key>(.*?)</Key>", Pattern.DOTALL).matcher(body);
        int n = 0;
        while (m.find()) {
            objects.remove(unescape(m.group(1)));
            n++;
        }
        // quiet mode: a DeleteResult with no Deleted elements
        respond(ex, 200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></DeleteResult>");
        if (n > 1000) throw new AssertionError("DeleteObjects exceeded 1000 keys: " + n);
    }

    // ── listing ─────────────────────────────────────────────────────────────

    private void listObjectsV2(HttpExchange ex, Map<String, String> q) throws IOException {
        listRequests.incrementAndGet();
        String prefix = q.getOrDefault("prefix", "");
        String delimiter = q.get("delimiter");
        String after = q.get("continuation-token");
        int limit = q.containsKey("max-keys") ? Integer.parseInt(q.get("max-keys")) : maxKeys;

        List<String> contents = new ArrayList<>();
        Set<String> commonPrefixes = new LinkedHashSet<>();
        String last = null;
        boolean truncated = false;

        for (String k : objects.keySet()) {
            if (!k.startsWith(prefix)) continue;
            if (after != null && k.compareTo(after) <= 0) continue;
            String rolled = null;
            if (delimiter != null && !delimiter.isEmpty()) {
                int idx = k.indexOf(delimiter, prefix.length());
                if (idx >= 0) rolled = k.substring(0, idx + delimiter.length());
            }
            if (rolled != null) {
                if (commonPrefixes.contains(rolled)) {
                    last = k;      // already represented; still advances the token
                    continue;
                }
                commonPrefixes.add(rolled);
            } else {
                contents.add(k);
            }
            last = k;
            if (contents.size() + commonPrefixes.size() >= limit) {
                truncated = true;
                break;
            }
        }
        if (truncated) {
            // Only truncated if something actually remains after `last`.
            truncated = objects.tailMap(last, false).keySet().stream().anyMatch(k -> k.startsWith(prefix));
        }

        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Name>" + bucket + "</Name>"
                + "<Prefix>" + escape(prefix) + "</Prefix>"
                + "<MaxKeys>" + limit + "</MaxKeys>"
                + "<KeyCount>" + (contents.size() + commonPrefixes.size()) + "</KeyCount>"
                + "<IsTruncated>" + truncated + "</IsTruncated>");
        if (delimiter != null) xml.append("<Delimiter>").append(escape(delimiter)).append("</Delimiter>");
        if (truncated) {
            xml.append("<NextContinuationToken>").append(escape(last)).append("</NextContinuationToken>");
        }
        for (String k : contents) {
            Obj o = objects.get(k);
            xml.append("<Contents><Key>").append(escape(k)).append("</Key>")
               .append("<LastModified>").append(ISO.format(o.modified())).append("</LastModified>")
               .append("<ETag>").append(escape(etag(o.bytes()))).append("</ETag>")
               .append("<Size>").append(o.bytes().length).append("</Size>")
               .append("<StorageClass>STANDARD</StorageClass></Contents>");
        }
        for (String cp : commonPrefixes) {
            xml.append("<CommonPrefixes><Prefix>").append(escape(cp)).append("</Prefix></CommonPrefixes>");
        }
        xml.append("</ListBucketResult>");
        respond(ex, 200, xml.toString());
    }

    // ── multipart ───────────────────────────────────────────────────────────

    private void createMultipartUpload(HttpExchange ex, String key) throws IOException {
        readBody(ex);
        String id = "upload-" + uploads.size() + "-" + Integer.toHexString(key.hashCode());
        uploads.put(id, new ConcurrentSkipListMap<>());
        uploadMetadata.put(id, requestMetadata(ex));
        respond(ex, 200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Bucket>" + bucket + "</Bucket><Key>" + escape(key) + "</Key>"
                + "<UploadId>" + id + "</UploadId></InitiateMultipartUploadResult>");
    }

    private void uploadPart(HttpExchange ex, Map<String, String> q) throws IOException {
        uploadPartRequests.incrementAndGet();
        int partNumber = Integer.parseInt(q.get("partNumber"));
        String uploadId = q.get("uploadId");
        byte[] body = readBody(ex);
        if (failPartNumber > 0 && partNumber == failPartNumber) {
            error(ex, 500, "InternalError", "injected failure on part " + partNumber);
            return;
        }
        NavigableMap<Integer, byte[]> parts = uploads.get(uploadId);
        if (parts == null) {
            error(ex, 404, "NoSuchUpload", uploadId);
            return;
        }
        parts.put(partNumber, body);
        ex.getResponseHeaders().add("ETag", etag(body));
        ex.sendResponseHeaders(200, -1);
    }

    private void completeMultipartUpload(HttpExchange ex, String key, String uploadId)
            throws IOException {
        String body = new String(readBody(ex), StandardCharsets.UTF_8);
        NavigableMap<Integer, byte[]> parts = uploads.remove(uploadId);
        if (parts == null) {
            error(ex, 404, "NoSuchUpload", uploadId);
            return;
        }
        // Assemble strictly in the part order the CLIENT declared, so a client that sends parts
        // out of order (or omits one) produces wrong bytes here rather than being papered over.
        Matcher m = Pattern.compile("<PartNumber>(\\d+)</PartNumber>").matcher(body);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int declared = 0;
        while (m.find()) {
            byte[] part = parts.get(Integer.parseInt(m.group(1)));
            if (part == null) {
                error(ex, 400, "InvalidPart", "part " + m.group(1) + " was never uploaded");
                return;
            }
            out.writeBytes(part);
            declared++;
        }
        if (declared != parts.size()) {
            error(ex, 400, "InvalidPart",
                    "declared " + declared + " parts but " + parts.size() + " were uploaded");
            return;
        }
        Map<String, String> md = uploadMetadata.remove(uploadId);
        objects.put(key, new Obj(out.toByteArray(), Instant.now(), md == null ? Map.of() : md));
        respond(ex, 200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Location>" + endpoint() + "/" + bucket + "/" + escape(key) + "</Location>"
                + "<Bucket>" + bucket + "</Bucket><Key>" + escape(key) + "</Key>"
                + "<ETag>&quot;" + multipartEtag(out.toByteArray(), declared) + "&quot;</ETag>"
                + "</CompleteMultipartUploadResult>");
    }

    private void abortMultipartUpload(HttpExchange ex, String uploadId) throws IOException {
        abortRequests.incrementAndGet();
        uploads.remove(uploadId);
        abortedUploads.add(uploadId);
        ex.sendResponseHeaders(204, -1);
    }

    // ── plumbing ────────────────────────────────────────────────────────────

    /**
     * Reads the request body, undoing {@code aws-chunked} framing when present.
     *
     * <p>Over plain HTTP the v2 SDK signs the payload with
     * {@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD}, which wraps the bytes in
     * {@code <hex-length>;chunk-signature=...\r\n<data>\r\n} frames. Reading the socket naively
     * would corrupt every upload — so this is decoded rather than disabled on the client, because
     * disabling it would mean the test no longer exercises the framing production actually sends.
     */
    private static byte[] readBody(HttpExchange ex) throws IOException {
        byte[] raw;
        try (InputStream in = ex.getRequestBody()) {
            raw = in.readAllBytes();
        }
        String sha = ex.getRequestHeaders().getFirst("x-amz-content-sha256");
        if (sha == null || !sha.startsWith("STREAMING-")) return raw;
        return deChunk(raw);
    }

    private static byte[] deChunk(byte[] raw) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int i = 0;
        while (i < raw.length) {
            int eol = indexOfCrLf(raw, i);
            if (eol < 0) break;
            String header = new String(raw, i, eol - i, StandardCharsets.US_ASCII);
            int semi = header.indexOf(';');
            String lenHex = (semi >= 0 ? header.substring(0, semi) : header).trim();
            if (lenHex.isEmpty()) break;
            int len;
            try {
                len = Integer.parseInt(lenHex, 16);
            } catch (NumberFormatException e) {
                break;                       // trailer section (e.g. x-amz-checksum-crc32)
            }
            i = eol + 2;
            if (len == 0) break;
            out.write(raw, i, len);
            i += len + 2;                    // skip the data and its trailing CRLF
        }
        return out.toByteArray();
    }

    private static int indexOfCrLf(byte[] b, int from) {
        for (int i = from; i + 1 < b.length; i++) {
            if (b[i] == '\r' && b[i + 1] == '\n') return i;
        }
        return -1;
    }

    private static void respond(HttpExchange ex, int status, String xml) throws IOException {
        byte[] body = xml.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/xml");
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    private static void error(HttpExchange ex, int status, String code, String message)
            throws IOException {
        // HEAD must not carry a body; everything else gets the <Error> document the SDK maps to
        // a typed exception (NoSuchKey -> NoSuchKeyException).
        if ("HEAD".equals(ex.getRequestMethod())) {
            ex.sendResponseHeaders(status, -1);
            return;
        }
        respond(ex, status, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code
                + "</Code><Message>" + escape(message) + "</Message><RequestId>r</RequestId></Error>");
    }

    /**
     * A REAL MD5, because the SDK verifies the returned ETag against the bytes it sent. That check
     * is a free integrity assertion on every upload in this suite: a corrupted aws-chunked decode
     * or a mis-sliced part fails here rather than silently passing.
     */
    private static String etag(byte[] body) {
        try {
            byte[] md5 = java.security.MessageDigest.getInstance("MD5").digest(body);
            StringBuilder sb = new StringBuilder("\"");
            for (byte b : md5) sb.append(String.format("%02x", b));
            return sb.append("\"").toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Multipart ETags end in {@code -N}; the SDK knows not to MD5-check those. */
    private static String multipartEtag(byte[] body, int parts) {
        return etag(body).replace("\"", "") + "-" + parts;
    }

    private static String httpDate(Instant t) {
        return DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC).format(t);
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) return out;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            String v = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            out.put(URLDecoder.decode(k, StandardCharsets.UTF_8), v);
        }
        return out;
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static String unescape(String s) {
        return s.replace("&quot;", "\"").replace("&gt;", ">").replace("&lt;", "<")
                .replace("&amp;", "&");
    }
}
