package com.plsqlanalyzer.web.config;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.sso.ActiveUserInfo;
import com.analyzer.storage.sso.SessionRoutingArtifactStore;
import com.analyzer.storage.sso.SsoSessionRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Ships rolled log files to the artifact store and deletes the local copy.
 *
 * <p>Logging is the one thing that must keep working when the store is unreachable, so the live log
 * file is always written to local disk. That would grow without bound on a long-lived instance, so
 * logback rolls it at 20 MB and gzips it, and this service moves each rolled archive into the store
 * and removes it from disk. Steady-state local footprint is therefore one active log file.
 *
 * <h2>Why it never touches the active file</h2>
 * Logback holds {@code unified-analyzer.log} open and appends to it. Uploading it would ship a
 * partial file, and deleting it would leave the appender writing to an unlinked inode — logs would
 * silently vanish until the next roll. Only files matching the rolled pattern are eligible, and
 * only after their size has stopped changing, since logback compresses asynchronously and a
 * {@code .gz} can exist while still being written.
 *
 * <h2>Why the archive prefix differs from the live directory</h2>
 * The store is rooted at the data directory and the live logs are at {@code {data}/logs}, so a key
 * of {@code logs/<name>} would resolve to the very file being uploaded when the backend is local —
 * and the delete that follows would remove what had just been written. Archives therefore go to
 * {@code logs/archive/}, which is a distinct location in both modes and needs no branching on which
 * backend is active.
 */
@Component
@ConditionalOnProperty(name = "app.logs.archive.enabled", havingValue = "true", matchIfMissing = true)
public class LogArchiveService {

    private static final Logger log = LoggerFactory.getLogger(LogArchiveService.class);

    /** Where archives live in the store, relative to the data root. */
    private static final ArtifactKey ARCHIVE_PREFIX = ArtifactKey.of("logs", "archive");

    private final ArtifactStore store;
    private final Path logDir;
    private final String activeLogName;

    /** Last seen size per file, so a file still being compressed is skipped until it settles. */
    private final Map<String, Long> lastSeenSize = new HashMap<>();

    /**
     * How long an archived log is kept, and how many are always kept regardless of age.
     *
     * <p>Local rolled logs were already bounded by logback ({@code maxHistory=5},
     * {@code total-size-cap=500MB}), but once shipped to the store nothing ever removed them — they
     * accumulated for the life of the deployment. Shipping a file and then keeping it forever is not
     * archiving, it is a slow leak, and on an object store it is a bill.
     *
     * <p>{@code retainMin} is the safety floor: age alone could otherwise empty the archive on a
     * system that has been quiet for a fortnight, which is exactly when you want the last few logs.
     * Set {@code app.logs.archive.retain-days=0} to disable pruning entirely.
     */
    private final int retainDays;
    private final int retainMin;

    /**
     * Which {@code ArtifactStore} this ships to is otherwise-arbitrary: log archiving is
     * app-wide infrastructure, not owned by either the PL/SQL parser or the JAR analyzer feature
     * (its {@code logs/archive/} prefix never overlaps either feature's data). Pinned to
     * {@code parserArtifactStore} for a stable, documented default rather than left unqualified.
     */
    public LogArchiveService(@Qualifier("parserArtifactStore") ArtifactStore store,
                             @Value("${LOG_PATH:}") String logPath,
                             @Value("${app.data-dir.base:data}") String dataBase,
                             @Value("${app.logs.archive.active-name:unified-analyzer.log}")
                             String activeLogName,
                             @Value("${app.logs.archive.retain-days:14}") int retainDays,
                             @Value("${app.logs.archive.retain-min:5}") int retainMin) {
        this.retainDays = retainDays;
        this.retainMin = retainMin;
        this.store = store;
        // LOG_PATH is set by DirPropertyResolver before logback initialises; fall back to the
        // conventional location so this still behaves in a test or a stripped-down context.
        this.logDir = (logPath == null || logPath.isBlank())
                ? Path.of(dataBase).resolve("logs")
                : Path.of(logPath);
        this.activeLogName = activeLogName;
    }

    @Scheduled(fixedDelayString = "${app.logs.archive.interval-ms:60000}",
               initialDelayString = "${app.logs.archive.interval-ms:60000}")
    public void archiveRolledLogs() {
        run();
    }

    /** Runs one archive pass. Returns how many files were shipped, for tests. */
    public int run() {
        int shippedNow = shipRolledLogs();
        pruneOldArchives();
        return shippedNow;
    }

    /**
     * Deletes archived logs older than {@code retainDays}, always keeping the newest
     * {@code retainMin}.
     *
     * <p>Never throws: log housekeeping must not be able to break the application, which is the
     * same rule the shipping pass follows.
     */
    int pruneOldArchives() {
        if (retainDays <= 0) return 0;
        long cutoff = System.currentTimeMillis() - retainDays * 86_400_000L;
        int removed = 0;
        try {
            List<ArtifactEntry> archives;
            try (Stream<ArtifactEntry> s = store.list(ARCHIVE_PREFIX, false)) {
                archives = s.filter(e -> !e.directory())
                            // newest first, so the ones protected by retainMin are the newest
                            .sorted(Comparator.comparingLong(ArtifactEntry::lastModified).reversed())
                            .toList();
            }
            for (int i = 0; i < archives.size(); i++) {
                if (i < retainMin) continue;                       // always keep the newest few
                ArtifactEntry e = archives.get(i);
                if (e.lastModified() >= cutoff) continue;          // still inside the window
                if (store.delete(e.key())) {
                    removed++;
                    log.info("Pruned archived log {} (older than {} days)", e.key(), retainDays);
                }
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Could not prune archived logs: {}", e.getMessage());
        }
        return removed;
    }

    private int shipRolledLogs() {
        if (!Files.isDirectory(logDir)) return 0;

        List<Path> candidates;
        try (Stream<Path> files = Files.list(logDir)) {
            candidates = files.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().equals(activeLogName))
                    .filter(p -> {
                        String n = p.getFileName().toString();
                        return n.endsWith(".log") || n.endsWith(".log.gz");
                    })
                    .sorted()
                    .toList();
        } catch (IOException e) {
            log.warn("Could not list the log directory {}: {}", logDir, e.getMessage());
            return 0;
        }

        List<String> present = new ArrayList<>();
        int shipped = 0;
        for (Path file : candidates) {
            String name = file.getFileName().toString();
            present.add(name);
            try {
                long size = Files.size(file);
                Long previous = lastSeenSize.put(name, size);
                // Skip on first sight, and while the size is still moving: logback compresses a
                // rolled file asynchronously, so a .gz can exist before it is complete. Shipping it
                // early would archive a truncated log and then delete the good local copy.
                if (previous == null || previous != size) continue;
                if (size == 0) continue;

                ArtifactKey key = ARCHIVE_PREFIX.resolve(name);
                try (InputStream in = Files.newInputStream(file);
                     OutputStream out = store.write(key, size)) {
                    in.transferTo(out);
                }

                // Delete only after the store confirms the object is there at the right size.
                // Deleting on a partial upload would lose the log outright.
                long stored = store.size(key);
                if (stored != size) {
                    log.warn("Archived {} is {} bytes in the store but {} on disk; keeping the "
                             + "local copy", name, stored, size);
                    continue;
                }
                Files.delete(file);
                lastSeenSize.remove(name);
                shipped++;
                log.info("Archived log {} ({} KB) to {} and removed the local copy",
                        name, size / 1024, key);
            } catch (IOException | RuntimeException e) {
                // Never let log archiving break the application. The file stays on disk and the
                // next pass retries it.
                log.warn("Could not archive log {}: {}", name, e.toString());
            }
        }
        lastSeenSize.keySet().retainAll(present);
        return shipped;
    }

    /**
     * A final pass on shutdown, so a rolled file produced late in the process lifetime is not
     * stranded on disk until the next start.
     */
    @PreDestroy
    void archiveOnShutdown() {
        try {
            // Sizes have not been observed twice for anything new, so seed then ship.
            run();
            run();
        } catch (RuntimeException e) {
            log.debug("Shutdown log archive pass failed: {}", e.toString());
        }
    }
}
