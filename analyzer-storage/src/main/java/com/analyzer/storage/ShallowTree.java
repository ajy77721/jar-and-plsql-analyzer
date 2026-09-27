package com.analyzer.storage;

import com.analyzer.storage.RequestScopedArtifactStore.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Depth-bounded discovery of analysis directories using <b>shallow listings only</b>.
 *
 * <h2>Why this exists</h2>
 *
 * <p>Every listing screen needs the same thing: the set of analysis directories in a tree, and
 * nothing that lives inside them. The call it replaces was a recursive {@code statPrefix} over the
 * whole subtree, which answers that question only by returning <em>every object</em> in it — every
 * per-node document, every statement, every source file, every {@code .node-cache} entry — for each
 * analysis, and then filtering the keys client-side. That payload is proportional to the number of
 * <em>files</em> in the deployment, which is the number that grows fastest: one analysis writes one
 * document per procedure and per statement.
 *
 * <p>A shallow listing asks the backend for the entries one level down and lets it roll everything
 * deeper into a common prefix ({@code req.delimiter("/")} on S3, {@code Files.list} locally,
 * synthesised prefixes in the in-memory store) — so both the request count and the payload are
 * proportional to the number of <em>directories</em>, and nothing below an analysis root is ever
 * listed.
 *
 * <h2>Shape of the walk</h2>
 *
 * <p>Level by level, breadth first. Each directory gets one shallow listing, and its
 * {@link DirRule} decides: {@link Decision#RECORD} a target, {@link Decision#SKIP} it silently, or
 * {@link Decision#DESCEND} into its child directories. The depth bound exists because the parser's
 * identity layout is <em>variable</em>: {@code AnalysisPaths.name} drops empty context parts, so an
 * analysis sits one to four segments below the root and a fixed depth would either miss the shallow
 * ones or run past the deep ones.
 *
 * <p>Each level is listed concurrently on {@link #pool()}. Levels stay sequential, so no task ever
 * waits on another task — the classic way a bounded pool deadlocks.
 */
public final class ShallowTree {

    /**
     * Concurrent listing requests. Matches the thread count the listing paths were sized for: these
     * are latency-bound round trips, not CPU work, and eight is where the earlier latency analysis
     * put the knee.
     */
    public static final int PARALLELISM = 8;

    /**
     * Shared, daemon-threaded pool. One per JVM rather than one per call: building a pool per listing
     * screen would pay thread creation on every refresh and leak the idle threads.
     */
    private static final ExecutorService POOL = Executors.newFixedThreadPool(PARALLELISM, r -> {
        Thread t = new Thread(r, "shallow-listing");
        t.setDaemon(true);
        return t;
    });

    private ShallowTree() {
    }

    /** The shared listing pool. Exposed so a caller can parallelise the reads that follow a walk. */
    public static ExecutorService pool() {
        return POOL;
    }

    /** What to do with a directory once its shallow listing is known. */
    public enum Decision {
        /** It is an analysis root: record it and stop. */
        RECORD,
        /** Not an analysis root and not worth descending into: drop the whole subtree. */
        SKIP,
        /** Not an analysis root, but its child directories may contain one. */
        DESCEND
    }

    /** Classifies one directory from its own shallow listing. */
    @FunctionalInterface
    public interface DirRule {
        /**
         * @param dir      the directory being examined
         * @param children its shallow listing: direct files and direct subdirectories only
         */
        Decision decide(ArtifactKey dir, List<ArtifactEntry> children) throws IOException;
    }

    /** The direct subdirectories in a shallow listing. */
    public static List<ArtifactKey> childDirs(List<ArtifactEntry> children) {
        List<ArtifactKey> out = new ArrayList<>();
        for (ArtifactEntry e : children) {
            if (e.directory()) out.add(e.key());
        }
        return out;
    }

    /** True when a shallow listing contains a direct file with this leaf name. */
    public static boolean hasFile(List<ArtifactEntry> children, String leaf) {
        for (ArtifactEntry e : children) {
            if (!e.directory() && leaf.equals(e.key().leaf())) return true;
        }
        return false;
    }

    /** True when a shallow listing contains a direct subdirectory with this leaf name. */
    public static boolean hasDir(List<ArtifactEntry> children, String leaf) {
        for (ArtifactEntry e : children) {
            if (e.directory() && leaf.equals(e.key().leaf())) return true;
        }
        return false;
    }

    /** A mapping that may fail the way storage reads fail. */
    @FunctionalInterface
    public interface IoFunction<I, O> {
        O apply(I input) throws IOException;
    }

    /**
     * Applies {@code fn} to every element on {@link #pool()}, keeping the input order.
     *
     * <p>The per-analysis metadata read that follows a listing is N independent round trips that
     * were previously paid one after another; running them concurrently is what turns a listing's
     * latency from "one round trip per analysis" into "one round trip, rounded up".
     *
     * <p>{@code store} is the store {@code fn} reads through. It is captured <b>here, on the calling
     * thread</b>, so every worker task runs inside the caller's routing scope — see
     * {@link RequestScopedArtifactStore}. Without that capture the reads would resolve on threads
     * with no request context, route to the shared default store, and (with no ambient pod
     * credentials) fail into an empty result. Pass the same store {@code fn} itself uses.
     *
     * <p>The result is a fresh, mutable list — callers sort it (the analyses listing is newest-first,
     * which is a property of the sort, not of the walk's order).
     */
    public static <I, O> List<O> parallelMap(ArtifactStore store, List<I> input,
                                             IoFunction<I, O> fn) throws IOException {
        if (input.isEmpty()) return new ArrayList<>();
        Scope scope = scopeOf(store);
        List<Future<O>> futures = new ArrayList<>(input.size());
        for (I in : input) {
            futures.add(POOL.submit(() -> {
                try (AutoCloseable ctx = scope.install()) {
                    return fn.apply(in);
                }
            }));
        }
        List<O> out = new ArrayList<>(futures.size());
        for (Future<O> f : futures) {
            try {
                out.add(f.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading analysis metadata", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) throw io;
                if (cause instanceof RuntimeException re) throw re;
                throw new IOException("Reading analysis metadata failed", cause);
            }
        }
        return out;
    }

    /**
     * One recorded directory and the shallow listing that identified it.
     *
     * <p>The children come free with the decision — the walk had to list the directory to classify
     * it — so a caller that needs to know which sibling files exist (a header sidecar, an
     * {@code analysis.json}) reads them off this rather than re-listing.
     */
    public record Found(ArtifactKey dir, List<ArtifactEntry> children) {
        /** A direct child file with this leaf name, or null. */
        public ArtifactEntry file(String leaf) {
            for (ArtifactEntry e : children) {
                if (!e.directory() && leaf.equals(e.key().leaf())) return e;
            }
            return null;
        }

        /** Whether a direct child file with this leaf name exists. */
        public boolean has(String leaf) {
            return file(leaf) != null;
        }
    }

    /**
     * Every directory at depth {@code 1..maxDepth} below {@code root} that {@code rule} records.
     *
     * <p>Ordered by key, so a listing screen's row order cannot depend on which concurrent listing
     * happened to finish first.
     *
     * <p>The store's request scope is captured on the calling thread and installed on every pool
     * task (both the listing and the {@code rule}'s own {@code exists}/{@code list} calls), so a
     * walk driven by a logged-in user routes each worker as that user instead of falling through to
     * the shared default target. See {@link RequestScopedArtifactStore}.
     *
     * @param maxDepth deepest target depth relative to {@code root} (the root itself is depth 0)
     */
    public static List<Found> findTargets(ArtifactStore store,
                                          ArtifactKey root,
                                          int maxDepth,
                                          DirRule rule) throws IOException {
        List<Found> targets = new ArrayList<>();
        List<ArtifactKey> frontier = List.of(root);
        Scope scope = scopeOf(store);

        // depth 0 is the root; a target is one level or more below it, so the deepest directory that
        // still has to be listed is maxDepth.
        for (int depth = 0; depth <= maxDepth && !frontier.isEmpty(); depth++) {
            List<Future<Listing>> futures = new ArrayList<>(frontier.size());
            for (ArtifactKey dir : frontier) {
                futures.add(POOL.submit(() -> {
                    try (AutoCloseable ctx = scope.install()) {
                        List<ArtifactEntry> children = new ArrayList<>();
                        try (Stream<ArtifactEntry> s = store.list(dir, false)) {
                            s.forEach(children::add);
                        }
                        return new Listing(dir, children, rule.decide(dir, children));
                    }
                }));
            }

            List<ArtifactKey> next = new ArrayList<>();
            for (Future<Listing> f : futures) {
                Listing l = await(f);
                switch (l.decision) {
                    case RECORD -> targets.add(new Found(l.dir, l.children));
                    case DESCEND -> {
                        if (depth < maxDepth) next.addAll(childDirs(l.children));
                    }
                    case SKIP -> { /* nothing below this directory is worth listing */ }
                }
            }
            frontier = next;
        }

        targets.sort(Comparator.comparing(t -> t.dir().toString()));
        return targets;
    }

    private record Listing(ArtifactKey dir, List<ArtifactEntry> children, Decision decision) {
    }

    /**
     * The caller's routing scope for {@code store}, or a no-op scope for a store that does not
     * depend on thread-bound request context (local, MongoDB, in-memory). Captured on the calling
     * thread, before any task is submitted.
     */
    private static Scope scopeOf(ArtifactStore store) {
        return store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope()
                : Scope.NONE;
    }

    private static Listing await(Future<Listing> f) throws IOException {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while listing analysis directories", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException re) throw re;
            throw new IOException("Listing analysis directories failed", cause);
        }
    }
}
