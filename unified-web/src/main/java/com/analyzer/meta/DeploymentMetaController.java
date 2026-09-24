package com.analyzer.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Deployment self-awareness — answers "where is my app, where is my data, and why is it slow?"
 *
 * <p>The performance question this endpoint exists to answer: the Kanopy cluster runs in an
 * AWS region chosen by the platform (set once by the operator as {@code app.deployment.region} —
 * visible in the Kanopy console), while the analysis artifacts live in an S3 bucket in
 * {@code app.storage.s3.region}. When the two differ, every store round-trip crosses regions
 * (~130 ms+ per call from the US to Mumbai) — which the user experiences as slowness with no
 * visible cause. This endpoint surfaces both regions AND a live latency measurement, so the
 * cross-region cost is diagnosable from the UI instead of being a mystery.
 *
 * <p>The latency probe opens a plain TCP connection to the S3 regional endpoint and times it —
 * a connect-only probe touches no AWS API, sends no credentials, and measures exactly the
 * network distance the store calls pay. Run on-demand per request (sub-second, connection
 * discarded); no state, nothing cached.
 *
 * <p>Everything returned here is non-sensitive: region names, the bucket name from
 * application properties, and a round-trip time. No tokens, no credentials, no user data.
 * The endpoint is public (same trust level as the static UI) so the footer can render it
 * before sign-in.
 */
@RestController
@RequestMapping("/api/meta")
public class DeploymentMetaController {

    private static final Logger log = LoggerFactory.getLogger(DeploymentMetaController.class);

    private final String s3Bucket;
    private final String s3Region;
    private final String clusterRegion;

    public DeploymentMetaController(@Value("${app.storage.s3.bucket:}") String s3Bucket,
                                    @Value("${app.storage.s3.region:}") String s3Region,
                                    @Value("${app.deployment.region:}") String clusterRegion) {
        this.s3Bucket = s3Bucket;
        this.s3Region = s3Region;
        this.clusterRegion = clusterRegion == null || clusterRegion.isBlank() ? null : clusterRegion.trim();
    }

    @GetMapping("/deployment")
    public Map<String, Object> deployment() {
        Map<String, Object> out = new LinkedHashMap<>();
        long s3RttMs = s3ConnectLatencyMs();
        out.put("clusterRegion", clusterRegion == null ? "unknown (set app.deployment.region from the Kanopy console)" : clusterRegion);
        out.put("s3Bucket", s3Bucket);
        out.put("s3Region", s3Region);
        out.put("s3ConnectLatencyMs", s3RttMs);
        boolean same = clusterRegion != null && s3Region != null && clusterRegion.equals(s3Region);
        out.put("sameRegion", same);
        if (clusterRegion == null) {
            out.put("latencyHint", "set app.deployment.region (from the Kanopy console) to enable the same-region check."
                    + " Live S3 round-trip: " + s3RttMs + " ms"
                    + (s3RttMs > 50 ? " — high latency indicates the cluster and bucket are in different regions." : " — healthy."));
        } else if (same) {
            out.put("latencyHint", "cluster and S3 bucket are in the same region (" + s3Region + ") — optimal latency."
                    + " Live S3 round-trip: " + s3RttMs + " ms.");
        } else {
            out.put("latencyHint", "CROSS-REGION: the cluster runs in " + clusterRegion
                    + " but the S3 bucket is in " + s3Region + " — every store call crosses regions ("
                    + s3RttMs + " ms measured). Moving the bucket to " + clusterRegion
                    + " (or asking the platform for a " + s3Region + " cluster) would cut latency substantially.");
        }
        return out;
    }

    /**
     * TCP connect time to the S3 regional endpoint — the network cost every store call pays
     * before any data flows. Connect-only: no credentials, no request payload. Returns -1 when
     * the endpoint is unreachable (offline dev).
     */
    private long s3ConnectLatencyMs() {
        String region = s3Region == null || s3Region.isBlank() ? "us-east-1" : s3Region.trim();
        try (Socket s = new Socket()) {
            long t0 = System.currentTimeMillis();
            s.connect(new InetSocketAddress("s3." + region + ".amazonaws.com", 443), 2000);
            return System.currentTimeMillis() - t0;
        } catch (Exception e) {
            log.info("[meta] S3 latency probe failed: {}", e.getMessage());
            return -1;
        }
    }
}
