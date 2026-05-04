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

/**
 * Serves the tail of the application log file for the in-app log viewer.
 */
@RestController("jarLogController")
@RequestMapping("/api/jar")
public class LogController {

    private final Path logFile;
    private static final long MAX_BYTES = 512_000; // ~500KB max to read

    public LogController(@Value("${logging.file.name:data/unified-analyzer.log}") String logPath) {
        this.logFile = Path.of(logPath);
    }

    @GetMapping("/logs")
    public Map<String, Object> tailLog(@RequestParam(defaultValue = "10000") int lines) {
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
