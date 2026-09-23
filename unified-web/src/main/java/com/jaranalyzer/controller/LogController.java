package com.jaranalyzer.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.analyzer.storage.log.LogTailBody;
import com.analyzer.storage.log.LogTailReader;

/**
 * Serves the tail of the application log file for the in-app log viewer.
 */
@RestController("jarLogController")
@RequestMapping("/api/jar")
public class LogController {

    private final Path logFile;
    /** Holds the incremental line index that makes a {@code sinceSeq} delta cheap. */
    private final LogTailReader tailReader;
    private static final long MAX_BYTES = 512_000; // ~500KB max to read

    // Resolve the SAME file logback-spring.xml writes: ${LOG_PATH}/unified-analyzer.log, exactly as
    // the PL/SQL LogController does.
    //
    // This used to read ${logging.file.name}, which defaults to the *relative* "logs/unified-
    // analyzer.log" (application.properties). DirPropertyResolver only rewrites that property when
    // it starts with "data/", so it never did — leaving this endpoint pointed at a cwd-relative
    // path that, in a repo with an old top-level logs/ directory, served a months-stale log from a
    // previous run while the app wrote to {app.data-dir}/logs. Verified: /api/jar/logs returned
    // July lines tagged http-nio-8083 from a live instance on 8096.
    //
    // There is only one application log; both endpoints must resolve to it.
    public LogController(@Value("${LOG_PATH:logs}") String logDir) {
        this.logFile = Path.of(logDir, "unified-analyzer.log");
        this.tailReader = new LogTailReader(this.logFile);
    }

    /**
     * Serves the log tail two ways, chosen by the parameters the caller sends.
     *
     * <p><b>No new parameters — today's response, unchanged.</b> {@code ?lines=N} still returns
     * {@code {content, lineCount, fileSize, truncated}} with the whole window as one string. That
     * is what makes this endpoint convertible on its own: an old screen, a bookmark or a script
     * sees no difference.
     *
     * <p><b>Any of {@code sinceSeq}, {@code limit}, {@code level}, {@code q} — the paged
     * envelope.</b> Lines are addressed by {@code seq}, filtering happens here rather than in the
     * browser, and a steady-state poll returns an empty {@code items} array with a watermark
     * instead of re-sending ten thousand lines it already has. Measured source logs for this
     * endpoint reach 9.36 MB.
     *
     * @param lines    legacy window size, in lines
     * @param sinceSeq return only lines after this {@code seq}; absent means the tail
     * @param limit    maximum lines to return, default 200
     * @param level    exact level to keep — {@code ERROR}, {@code WARN}, {@code INFO}, {@code DEBUG}
     * @param q        case-insensitive substring the line must contain
     */
    @GetMapping("/logs")
    public Object tailLog(@RequestParam(defaultValue = "10000") int lines,
                          @RequestParam(required = false) Long sinceSeq,
                          @RequestParam(required = false) Integer limit,
                          @RequestParam(required = false) String level,
                          @RequestParam(required = false) String q) {
        if (sinceSeq != null || limit != null || level != null || q != null) {
            try {
                return LogTailBody.of(tailReader.read(sinceSeq, limit, level, q));
            } catch (IOException e) {
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Failed to read log: " + e.getMessage());
            }
        }
        return legacyTail(lines);
    }

    /** The pre-paging response, kept verbatim so an unconverted caller sees no change. */
    private Map<String, Object> legacyTail(int lines) {

        if (!Files.exists(logFile)) {
            return Map.of(
                    "content", "Log file not created yet. It will appear after the first log entry.\n",
                    "lineCount", 1,
                    "fileSize", 0,
                    "truncated", false
            );
        }
        try {
            long fileSize = Files.size(logFile);
            // Read last MAX_BYTES from file using RandomAccessFile for efficiency
            long startPos = Math.max(0, fileSize - MAX_BYTES);
            byte[] buf;
            try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
                raf.seek(startPos);
                int toRead = (int) (fileSize - startPos);
                buf = new byte[toRead];
                raf.readFully(buf);
            }

            String content = new String(buf, StandardCharsets.UTF_8);
            // If we seeked into the middle of a line, skip to the next line
            if (startPos > 0) {
                int firstNewline = content.indexOf('\n');
                if (firstNewline >= 0 && firstNewline < content.length() - 1) {
                    content = content.substring(firstNewline + 1);
                }
            }

            // Limit to last N lines
            String[] allLines = content.split("\n");
            int from = Math.max(0, allLines.length - lines);
            StringBuilder result = new StringBuilder();
            for (int i = from; i < allLines.length; i++) {
                result.append(allLines[i]).append('\n');
            }

            return Map.of(
                    "content", result.toString(),
                    "lineCount", allLines.length - from,
                    "fileSize", fileSize,
                    "truncated", startPos > 0
            );
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Failed to read log: " + e.getMessage());
        }
    }
}
