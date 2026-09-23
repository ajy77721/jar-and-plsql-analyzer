package com.analyzer.storage.s3;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.CredentialGuardStore;
import com.analyzer.storage.LocalHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Offline contract tests for {@link S3ArtifactStore}.
 *
 * <p>There is no bucket and no credential here, so these assert the things a mock can genuinely
 * prove: which request the store builds, how it assembles a paged answer, and that a failure
 * cleans up after itself. Round-trip fidelity against real S3 is explicitly NOT covered — see the
 * live smoke checklist in the s3-mode skill.
 */
class S3ArtifactStoreTest {

    private S3Client s3;
    private S3ArtifactStore store;

    @BeforeEach
    void setUp() {
        s3 = mock(S3Client.class);
        store = new S3ArtifactStore(s3, "bkt", "data");
    }

    private static S3Exception status(int code) {
        return (S3Exception) S3Exception.builder()
                .statusCode(code)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("E" + code).build())
                .message("status " + code)
                .build();
    }

    private static ResponseInputStream<GetObjectResponse> body(String s) {
        return new ResponseInputStream<>(GetObjectResponse.builder().build(),
                AbortableInputStream.create(
                        new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8))));
    }

    // ── key mapping ─────────────────────────────────────────────────────────

    @Test
    void objectKeyJoinsRootPrefixWithExactlyOneSlash() {
        assertEquals("data/jar/demo/analysis.json",
                store.objectKey(ArtifactKey.of("jar", "demo", "analysis.json")));
    }

    @Test
    void emptyPrefixYieldsBareKey() {
        S3ArtifactStore bare = new S3ArtifactStore(s3, "bkt", "");
        assertEquals("jar/demo/analysis.json",
                bare.objectKey(ArtifactKey.of("jar", "demo", "analysis.json")));
        assertEquals("", bare.rootPrefix());
    }

    @Test
    void prefixIsNormalisedRegardlessOfHowItIsWritten() {
        for (String raw : new String[]{"data", "/data", "data/", "  /data//  "}) {
            assertEquals("data", S3ArtifactStore.normalizePrefix(raw), "for input '" + raw + "'");
        }
        assertEquals("", S3ArtifactStore.normalizePrefix(null));
    }

    @Test
    void listPrefixAlwaysEndsInSlash() {
        assertEquals("data/a/b/", store.listPrefix(ArtifactKey.of("a", "b")));
    }

    // ── ranged reads ────────────────────────────────────────────────────────

    @Test
    void rangeHeaderIsInclusiveAtBothEnds() {
        assertEquals("bytes=0-99", S3ArtifactStore.rangeHeader(0, 100));
        assertEquals("bytes=100-199", S3ArtifactStore.rangeHeader(100, 100));
        assertEquals("bytes=100-", S3ArtifactStore.rangeHeader(100, -1));
        assertEquals("bytes=5-5", S3ArtifactStore.rangeHeader(5, 0));
        assertThrows(IllegalArgumentException.class, () -> S3ArtifactStore.rangeHeader(-1, 10));
    }

    @Test
    void rangedReadSendsRangeHeaderNotAFullGet() throws IOException {
        when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body("0123456789"));
        try (InputStream ignored = store.read(ArtifactKey.of("a", "b.json"), 4, 3)) {
            // consumed below via the captor
        }
        ArgumentCaptor<GetObjectRequest> c = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3).getObject(c.capture());
        assertEquals("bytes=4-6", c.getValue().range());
        assertEquals("data/a/b.json", c.getValue().key());
        assertEquals("bkt", c.getValue().bucket());
    }

    @Test
    void zeroLengthRangeReadsNothingJustLikeLocal() throws IOException {
        when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body("0123456789"));
        try (InputStream in = store.read(ArtifactKey.of("a", "b.json"), 4, 0)) {
            assertEquals(0, in.readAllBytes().length);
        }
    }

    @Test
    void missingObjectReadsAsNoSuchFileNotAnSdkException() {
        when(s3.getObject(any(GetObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());
        assertThrows(NoSuchFileException.class, () -> store.read(ArtifactKey.of("nope.json")));
    }

    @Test
    void rangePastEndOfObjectSurfacesAsEof() {
        when(s3.getObject(any(GetObjectRequest.class))).thenThrow(status(416));
        assertThrows(java.io.EOFException.class,
                () -> store.read(ArtifactKey.of("a.json"), 1_000_000, 10));
    }

    // ── metadata: 404 is an answer, not a failure ───────────────────────────

    @Test
    void headMissingObjectGivesFalseAndMinusOne() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());
        ArtifactKey k = ArtifactKey.of("gone.json");
        assertFalse(store.exists(k));
        assertEquals(-1, store.size(k));
        assertEquals(-1, store.lastModified(k));
    }

    @Test
    void bareHttp404WithoutAModelledExceptionIsAlsoTreatedAsMissing() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(status(404));
        assertFalse(store.exists(ArtifactKey.of("gone.json")));
        assertEquals(-1, store.size(ArtifactKey.of("gone.json")));
    }

    @Test
    void a403IsNotSwallowedAsMissing() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(status(403));
        assertThrows(S3Exception.class, () -> store.exists(ArtifactKey.of("denied.json")));
    }

    @Test
    void headPopulatesSizeAndLastModified() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenReturn(
                HeadObjectResponse.builder().contentLength(42L)
                        .lastModified(Instant.ofEpochMilli(1234)).build());
        assertTrue(store.exists(ArtifactKey.of("a.json")));
        assertEquals(42L, store.size(ArtifactKey.of("a.json")));
        assertEquals(1234L, store.lastModified(ArtifactKey.of("a.json")));
    }

    // ── listing ─────────────────────────────────────────────────────────────

    private static S3Object obj(String key, long size) {
        return S3Object.builder().key(key).size(size).lastModified(Instant.ofEpochMilli(7)).build();
    }

    @Test
    void listingAssemblesAcrossPages() throws IOException {
        when(s3.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder()
                        .contents(obj("data/a/b/one.json", 1), obj("data/a/b/two.json", 2))
                        .isTruncated(true).nextContinuationToken("t1").build())
                .thenReturn(ListObjectsV2Response.builder()
                        .contents(obj("data/a/b/three.json", 3))
                        .isTruncated(false).build());

        List<ArtifactEntry> all;
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("a", "b"), true)) {
            all = s.toList();
        }
        assertEquals(List.of("a/b/one.json", "a/b/two.json", "a/b/three.json"),
                all.stream().map(e -> e.key().toString()).toList());
        assertEquals(3L, all.get(2).size());

        ArgumentCaptor<ListObjectsV2Request> c = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3, times(2)).listObjectsV2(c.capture());
        assertNull(c.getAllValues().get(0).continuationToken());
        assertEquals("t1", c.getAllValues().get(1).continuationToken());
    }

    @Test
    void listingIsLazyAndDoesNotFetchPagesTheCallerNeverConsumes() throws IOException {
        when(s3.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder()
                        .contents(obj("data/a/b/one.json", 1))
                        .isTruncated(true).nextContinuationToken("t1").build())
                .thenThrow(new AssertionError("second page must not be fetched"));

        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("a", "b"), true)) {
            assertEquals("a/b/one.json", s.findFirst().orElseThrow().key().toString());
        }
        verify(s3, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    /**
     * The bug this guards: a prefix used without a trailing '/' matches sibling keys by simple
     * string prefix, so listing {@code a/b} would silently return everything under {@code a/bc}.
     */
    @Test
    void listingPrefixRespectsTheDirectoryBoundary() throws IOException {
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(obj("data/a/b/one.json", 1), obj("data/a/bc/two.json", 2))
                        .isTruncated(false).build());

        ArgumentCaptor<ListObjectsV2Request> c = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        List<ArtifactEntry> all;
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("a", "b"), true)) {
            all = s.toList();
        }
        verify(s3).listObjectsV2(c.capture());
        assertEquals("data/a/b/", c.getValue().prefix(), "request prefix must end in '/'");
        // and even if the server were sloppy, a/bc is filtered out client side
        assertEquals(List.of("a/b/one.json"), all.stream().map(e -> e.key().toString()).toList());
    }

    @Test
    void statPrefixBoundaryToo() throws IOException {
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(obj("data/a/b/one.json", 1), obj("data/a/bc/two.json", 2))
                        .isTruncated(false).build());
        Map<ArtifactKey, ArtifactEntry> m = store.statPrefix(ArtifactKey.of("a", "b"));
        assertEquals(Set.of(ArtifactKey.of("a", "b", "one.json")), m.keySet());
    }

    @Test
    void nonRecursiveListingUsesTheDelimiterAndReturnsCommonPrefixesAsDirectories() throws IOException {
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(obj("data/a/file.json", 9))
                        .commonPrefixes(CommonPrefix.builder().prefix("data/a/sub/").build())
                        .isTruncated(false).build());

        List<ArtifactEntry> all;
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("a"), false)) {
            all = s.toList();
        }
        ArgumentCaptor<ListObjectsV2Request> c = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3).listObjectsV2(c.capture());
        assertEquals("/", c.getValue().delimiter());

        Map<String, Boolean> byKey = all.stream()
                .collect(Collectors.toMap(e -> e.key().toString(), ArtifactEntry::directory));
        assertEquals(Map.of("a/sub", true, "a/file.json", false), byKey);
    }

    @Test
    void statPrefixIsOneListingNotAHeadPerObject() throws IOException {
        List<S3Object> objs = new ArrayList<>();
        for (int i = 0; i < 50; i++) objs.add(obj("data/a/f" + i + ".json", i));
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder().contents(objs).isTruncated(false).build());

        Map<ArtifactKey, ArtifactEntry> m = store.statPrefix(ArtifactKey.of("a"));
        assertEquals(50, m.size());
        assertEquals(7L, m.get(ArtifactKey.of("a", "f7.json")).size());
        verify(s3, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
        verify(s3, never()).headObject(any(HeadObjectRequest.class));
    }

    // ── deletes ─────────────────────────────────────────────────────────────

    @Test
    void deletePrefixBatchesAtAThousandKeysPerRequest() throws IOException {
        List<S3Object> objs = new ArrayList<>();
        for (int i = 0; i < 2_500; i++) objs.add(obj("data/a/f" + i + ".json", 1));
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder().contents(objs).isTruncated(false).build());
        when(s3.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());

        assertEquals(2_500, store.deletePrefix(ArtifactKey.of("a")));

        ArgumentCaptor<DeleteObjectsRequest> c = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3, times(3)).deleteObjects(c.capture());
        assertEquals(List.of(1000, 1000, 500),
                c.getAllValues().stream().map(r -> r.delete().objects().size()).toList());
        verify(s3, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void deleteReportsFalseForAMissingObjectAndDoesNotIssueTheDelete() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());
        assertFalse(store.delete(ArtifactKey.of("gone.json")));
        verify(s3, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    // ── copy / move / materialize ───────────────────────────────────────────

    @Test
    void moveIsCopyThenDeleteOfTheSourceOnly() throws IOException {
        when(s3.headObject(any(HeadObjectRequest.class))).thenReturn(
                HeadObjectResponse.builder().contentLength(10L)
                        .lastModified(Instant.EPOCH).build());
        store.move(ArtifactKey.of("a", "x.json"), ArtifactKey.of("b", "y.json"));

        ArgumentCaptor<CopyObjectRequest> cp = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(s3).copyObject(cp.capture());
        assertEquals("data/a/x.json", cp.getValue().sourceKey());
        assertEquals("data/b/y.json", cp.getValue().destinationKey());

        ArgumentCaptor<DeleteObjectRequest> del = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        verify(s3).deleteObject(del.capture());
        assertEquals("data/a/x.json", del.getValue().key());
    }

    @Test
    void copyOfAMissingSourceFailsBeforeTouchingTheDestination() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());
        assertThrows(NoSuchFileException.class,
                () -> store.copy(ArtifactKey.of("a.json"), ArtifactKey.of("b.json")));
        verify(s3, never()).copyObject(any(CopyObjectRequest.class));
    }

    @Test
    void materializeDownloadsToATempOutsideTheDataRootAndDeletesOnClose() throws IOException {
        when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body("hello"));

        Path p;
        try (LocalHandle h = store.materialize(ArtifactKey.of("jar", "demo.jar"))) {
            p = h.path();
            assertTrue(h.temporary());
            assertEquals("hello", Files.readString(p));
            assertTrue(p.getFileName().toString().endsWith("demo.jar"),
                    "leaf suffix must survive: " + p);
            assertTrue(p.toAbsolutePath().startsWith(
                            Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath()),
                    "temp must live outside the data root: " + p);
        }
        assertFalse(Files.exists(p), "close() must delete the temp copy");
    }

    // ── credential guard still applies through the S3 path ──────────────────

    @Nested
    class GuardedThroughS3 {

        private ArtifactStore guarded;

        @BeforeEach
        void wrap() {
            guarded = new CredentialGuardStore(store);
        }

        @Test
        void credentialLeafNamesNeverReachTheBucket() {
            for (String leaf : new String[]{"plsql-config.yaml", "plsql-env-config.yaml"}) {
                ArtifactKey k = ArtifactKey.of("config", leaf);
                assertThrows(SecurityException.class, () -> guarded.write(k), leaf);
                assertThrows(SecurityException.class, () -> guarded.write(k, 10), leaf);
            }
            verifyNoInteractions(s3);
        }
    }

    // ── multipart arithmetic (pure, no client) ──────────────────────────────

    @Nested
    class Multipart {

        @Test
        void aHundredGigabyteWriteStaysWellUnderTheTenThousandPartLimit() {
            long hundredGb = 100L * 1024 * 1024 * 1024;
            int parts = MultipartUploadOutputStream.partsRequired(hundredGb);
            assertTrue(parts > 0, "100 GB must be uploadable at all");
            assertTrue(parts < MultipartUploadOutputStream.MAX_PARTS,
                    "100 GB needed " + parts + " parts");
        }

        @Test
        void everyPartIsAtOrAboveTheS3MinimumAndAtOrBelowTheMaximum() {
            for (int n : new int[]{1, 999, 1000, 1001, 5000, 9999, 10_000}) {
                int sz = MultipartUploadOutputStream.partSizeForPart(n);
                assertTrue(sz >= MultipartUploadOutputStream.MIN_PART_SIZE, "part " + n);
                assertTrue(sz <= MultipartUploadOutputStream.MAX_PART_SIZE, "part " + n);
            }
        }

        @Test
        void partSizeRampsMonotonicallySoLargeObjectsStayWithinTenThousandParts() {
            int prev = 0;
            for (int n = 1; n <= 10_000; n++) {
                int sz = MultipartUploadOutputStream.partSizeForPart(n);
                assertTrue(sz >= prev, "part size must never shrink, at part " + n);
                prev = sz;
            }
            // The ceiling the ramp buys, stated explicitly so a change to the tiers is visible.
            assertTrue(MultipartUploadOutputStream.maxSupportedBytes() > 1_500L * 1024 * 1024 * 1024,
                    "expected > 1.5 TiB, got " + MultipartUploadOutputStream.maxSupportedBytes());
        }

        @Test
        void smallWriteCommitsAsASinglePutWithNoMultipartUploadAtAll() throws IOException {
            when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenReturn(PutObjectResponse.builder().build());

            try (OutputStream out = store.write(ArtifactKey.of("a", "small.json"))) {
                out.write("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
            }
            ArgumentCaptor<PutObjectRequest> c = ArgumentCaptor.forClass(PutObjectRequest.class);
            verify(s3).putObject(c.capture(), any(RequestBody.class));
            assertEquals("data/a/small.json", c.getValue().key());
            assertEquals(7L, c.getValue().contentLength());
            verify(s3, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }

        @Test
        void aLargeWriteRollsOverIntoMultipartAndCompletesWithEveryPart() throws IOException {
            when(s3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                    .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
            when(s3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                    .thenAnswer(inv -> UploadPartResponse.builder()
                            .eTag("etag-" + inv.getArgument(0, UploadPartRequest.class).partNumber())
                            .build());
            when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                    .thenReturn(CompleteMultipartUploadResponse.builder().build());

            byte[] chunk = new byte[1024 * 1024];
            try (OutputStream out = store.write(ArtifactKey.of("a", "big.json"))) {
                for (int i = 0; i < 40; i++) out.write(chunk);   // 40 MiB > 16 MiB first part
            }

            ArgumentCaptor<UploadPartRequest> up = ArgumentCaptor.forClass(UploadPartRequest.class);
            verify(s3, atLeast(2)).uploadPart(up.capture(), any(RequestBody.class));
            assertEquals(40L * 1024 * 1024,
                    up.getAllValues().stream().mapToLong(UploadPartRequest::contentLength).sum(),
                    "every byte written must appear in exactly one part");
            // Part numbers are 1..n with no gaps and no repeats.
            List<Integer> nums = up.getAllValues().stream().map(UploadPartRequest::partNumber).toList();
            assertEquals(nums.size(), new HashSet<>(nums).size());
            assertEquals(1, nums.get(0));

            ArgumentCaptor<CompleteMultipartUploadRequest> cc =
                    ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
            verify(s3).completeMultipartUpload(cc.capture());
            assertEquals(nums.size(), cc.getValue().multipartUpload().parts().size());
            assertEquals("u1", cc.getValue().uploadId());
            verify(s3, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }

        /** Orphaned parts are billed until a lifecycle rule reaps them, so this must not regress. */
        @Test
        void aFailedCompleteAbortsTheUploadSoNoPartsAreLeftBillable() {
            when(s3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                    .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
            when(s3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                    .thenReturn(UploadPartResponse.builder().eTag("e").build());
            when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                    .thenThrow(status(500));

            byte[] chunk = new byte[1024 * 1024];
            IOException boom = assertThrows(IOException.class, () -> {
                try (OutputStream out = store.write(ArtifactKey.of("a", "big.json"))) {
                    for (int i = 0; i < 40; i++) out.write(chunk);
                }
            });
            assertTrue(boom.getMessage().contains("data/a/big.json"));

            ArgumentCaptor<AbortMultipartUploadRequest> ab =
                    ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
            verify(s3).abortMultipartUpload(ab.capture());
            assertEquals("u1", ab.getValue().uploadId());
            assertEquals("data/a/big.json", ab.getValue().key());
        }

        @Test
        void abandoningAStreamWithoutClosingUploadsNothing() {
            store.write(ArtifactKey.of("a", "never-closed.json"));
            verifyNoInteractions(s3);
        }
    }
}
