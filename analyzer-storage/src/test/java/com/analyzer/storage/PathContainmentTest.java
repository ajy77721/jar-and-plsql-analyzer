package com.analyzer.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins the containment rule used to confine client-supplied paths to the data root.
 *
 * <p>This exists because the original guard — {@code path.contains("..")} — looked like a
 * traversal check and stopped nothing. An absolute path contains no "..", so {@code /etc/passwd}
 * passed straight through to a read AND a write endpoint. The lesson encoded here is that
 * containment must be asserted on the RESOLVED, NORMALIZED path, never inferred from the input
 * string.
 */
class PathContainmentTest {

    @TempDir Path root;

    /** Mirrors ChatController.resolveInsideDataRoot. */
    private Path resolve(String clientPath) {
        Path base = root.toAbsolutePath().normalize();
        Path candidate = Path.of(clientPath.replace("\\", "/"));
        Path resolved = (candidate.isAbsolute()
                ? base.resolve(Path.of("/").relativize(candidate))
                : base.resolve(candidate)).normalize();
        if (!resolved.startsWith(base)) throw new IllegalArgumentException("escapes root");
        return resolved;
    }

    @Test
    void ordinaryRelativePathsResolveInside() {
        assertTrue(resolve("jar/demo/analysis.json").startsWith(root));
        assertTrue(resolve("chat/history.jsonl").startsWith(root));
    }

    @Test
    void absolutePathIsReinterpretedRelativeToRootNotHonoured() {
        // The exact bypass the old check missed: no ".." anywhere in this string.
        Path p = resolve("/etc/passwd");
        assertTrue(p.startsWith(root), "absolute input must be confined, not honoured");
        assertTrue(p.endsWith(Path.of("etc/passwd")));
        assertNotEquals(Path.of("/etc/passwd"), p);
    }

    @Test
    void dotDotEscapeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolve("../../../etc/passwd"));
        assertThrows(IllegalArgumentException.class, () -> resolve("jar/../../outside.txt"));
    }

    @Test
    void backslashVariantsAreNormalizedThenChecked() {
        assertThrows(IllegalArgumentException.class, () -> resolve("..\\..\\windows\\system32"));
        assertTrue(resolve("jar\\demo\\file.json").startsWith(root));
    }

    @Test
    void embeddedDotDotThatStaysInsideIsAllowed() {
        // "a/../b" normalizes to "b" — inside the root, so legitimately fine. A naive substring
        // check would have rejected this valid path while allowing /etc/passwd.
        assertTrue(resolve("jar/../plsql/file.json").startsWith(root));
    }

    @Test
    void rootItselfIsInside() {
        assertTrue(resolve("").startsWith(root));
        assertTrue(resolve(".").startsWith(root));
    }
}
