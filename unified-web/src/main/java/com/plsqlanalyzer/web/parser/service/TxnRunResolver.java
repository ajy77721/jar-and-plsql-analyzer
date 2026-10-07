package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The interprocedural transaction resolver — turns the per-procedure transaction capture
 * (node `transactionScope` with the Phase-1 enrichment + `tables[].operations`) into
 * RUNTIME-ACCURATE transaction runs for one entry point:
 *
 * <ul>
 *   <li>an ordered EVENT TIMELINE per run (writes, savepoints, commit/rollback, call edges —
 *       interleaved by source line across procedure boundaries via the call tree),</li>
 *   <li>ALTERNATIVE RUNS for guarded (conditional) statements — every guarded event is a branch
 *       point; the enumeration is bounded and each run is labelled with the guard outcomes it
 *       assumes,</li>
 *   <li>a per-write SURVIVAL verdict computed by replaying each run under the dialect's
 *       ownership rules ("what survives the later ROLLBACK?").</li>
 * </ul>
 *
 * <h2>Dialect ownership semantics (the replay rules)</h2>
 *
 * <ul>
 *   <li><b>ORACLE / DB2 / MYSQL / POSTGRES</b> — one transaction frame for the whole call tree:
 *       an implicit start on the first DML; a COMMIT ANYWHERE (including inside a callee)
 *       persists ALL uncommitted work up the chain; a full ROLLBACK discards all of it;
 *       ROLLBACK TO SAVEPOINT discards writes made after that savepoint. MySQL/Postgres nuance:
 *       their grammars have no nesting anyway, so the single-frame rule is exactly their
 *       semantics.</li>
 *   <li><b>SQLSERVER</b> — nesting: BEGIN TRANSACTION pushes a frame; an inner COMMIT only
 *       merges its uncommitted writes into the ENCLOSING frame (the outermost COMMIT persists);
 *       a full ROLLBACK discards every frame. SAVEPOINT/ROLLBACK TO behave as in Oracle within
 *       the current frame.</li>
 *   <li><b>Autonomous nodes</b> (PRAGMA AUTONOMOUS_TRANSACTION) run in their OWN transaction:
 *       their writes are kept on a separate side run and always survive whatever the outer flow
 *       does.</li>
 * </ul>
 *
 * <h2>Output shape (consumed by the UI's transaction flow view)</h2>
 * <pre>{ entry, dialect, pathCountExceeded,
 *   runs: [ { runId, kind: "main"|"autonomous",
 *             assumptions: [{nodeId, line, condition, taken}],
 *             outcome: "COMMITTED"|"ROLLED_BACK"|"OPEN"|"PARTIAL",
 *             events: [ {seq, kind: WRITE|SAVEPOINT|ROLLBACK|ROLLBACK_TO|COMMIT|BEGIN_TRAN|
 *                        SET_TRANSACTION|RELEASE_SAVEPOINT|CALL|LOCK,
 *                        nodeId, proc, depth, line, table?, op?, name?, guard?} ],
 *             writes: [ {nodeId, proc, line, table, op, survival: survived|discarded|open,
 *                        note?} ],
 *             summary: "&lt;plain-English outcome, derived from the replay&gt;" } ] }</pre>
 *
 * Deterministic: same artifacts + same tree → same runs. Memoized as the derived sidecar
 * {@code views/txn-runs/<entry>.json} through {@link AnalysisViewStore#readOrCompute} — the
 * existing views/ invalidation and index-mtime staleness cover it with no new rules.
 */
@Service
public class TxnRunResolver {

    /** Memoized sidecar directory — one document per entry node (like call-tree/). */
    public static final String TXN_RUNS_DIR = "txn-runs";

    /** Guard-combination bound: beyond this the enumeration collapses to one conservative run. */
    static final int MAX_PATHS = 64;

    /** Call-tree DFS bound — a pathological tree stops the walk (flagged in the output). */
    static final int MAX_EVENTS = 20_000;

    private final AnalysisService service;
    private final AnalysisViewStore viewStore;
    private final ObjectMapper mapper;

    public TxnRunResolver(AnalysisService service, AnalysisViewStore viewStore, ObjectMapper mapper) {
        this.service = service;
        this.viewStore = viewStore;
        this.mapper = mapper;
    }

    /**
     * The transaction runs for one entry point — served from the memoized
     * {@code views/txn-runs/<entry>.json} sidecar when present and fresh (the same staleness and
     * invalidation rules as every other view), otherwise computed once and backfilled.
     */
    public JsonNode txnRuns(String name, String entryNodeId) throws IOException {
        String view = TXN_RUNS_DIR + "/" + entryNodeId + ".json";
        return viewStore.readOrCompute(name, view, () -> compute(name, entryNodeId));
    }

    /** The sidecar's computation: call tree → per-node events → path enumeration → replay. */
    JsonNode compute(String name, String entryNodeId) throws IOException {
        JsonNode tree = service.getCallTree(name, entryNodeId);
        if (tree == null || tree.isNull() || tree.isMissingNode()) return null;
        return computeFromTree(name, entryNodeId, tree, nodeId -> {
            try {
                String file = nodeId.endsWith(".json") ? nodeId : nodeId + ".json";
                return service.getNodeDetail(name, file);
            } catch (IOException e) {
                return null;
            }
        });
    }

    /**
     * The pure core, exposed for the live-shape tests: a call-tree document + a node-doc loader
     * (nodeId → the node's detail JSON, or null) — exactly what the real artifacts provide, with
     * no store or service behind it.
     */
    JsonNode computeFromTree(String analysisName, String entryNodeId, JsonNode tree,
                             java.util.function.Function<String, JsonNode> nodeLoader) {
        String dialect = dialectOf(analysisName);
        this.nodeLoader = nodeLoader;

        boolean sqlServerNesting = dialect.equals("SQLSERVER");

        List<Event> timeline = new ArrayList<>();
        boolean[] truncated = {false};
        boolean[] autonomousSeen = {false};
        collect(tree, null, 0, analysisName, timeline, new HashSet<>(), autonomousSeen, truncated);

        List<Event> autonomousTimeline = new ArrayList<>();
        // Split main vs autonomous: the autonomous subtree forms its own side run(s).
        List<Event> main = new ArrayList<>();
        for (Event e : timeline) (e.autonomousNode ? autonomousTimeline : main).add(e);

        List<Run> runs = new ArrayList<>(enumerate(main, sqlServerNesting));
        if (!autonomousTimeline.isEmpty()) {
            Run side = new Run();
            side.kind = "autonomous";
            side.events.addAll(autonomousTimeline);
            replay(side, sqlServerNesting, true);
            runs.add(side);
        }
        return render(analysisName, entryNodeId, dialect, runs, truncated[0]);
    }

    private java.util.function.Function<String, JsonNode> nodeLoader;

    // ───────────────────────── timeline collection (DFS over the call tree) ─────────────────────────

    /** One event on the timeline. */
    static final class Event {
        String kind;      // WRITE | SAVEPOINT | ROLLBACK | ROLLBACK_TO | COMMIT | BEGIN_TRAN |
                          // SET_TRANSACTION | RELEASE_SAVEPOINT | CALL | LOCK
        String nodeId;
        String proc;
        int depth;
        int line;
        String table;     // WRITE only
        String op;        // WRITE only
        String name;      // SAVEPOINT / ROLLBACK_TO / BEGIN_TRAN target
        String guardKind, guardCondition;
        int guardLine;
        String handlerName;
        boolean autonomousNode;
        /** Where this write ended up after the replay (set by {@link #replay}). */
        String survival;
    }

    private void collect(JsonNode tree, JsonNode parent, int depth, String analysisName,
                         List<Event> out, Set<String> visited, boolean[] autonomousSeen,
                         boolean[] truncated) {
        if (truncated[0] || out.size() > MAX_EVENTS) { truncated[0] = true; return; }
        String id = tree.path("id").asText(tree.path("nodeId").asText(""));
        if (id.isEmpty()) return;
        if (!visited.add(id + "@" + depth)) return;   // a node called from several sites may repeat — allowed per depth

        JsonNode doc = loadNode(tree, analysisName);
        boolean autonomous = doc != null && (doc.path("transactionScope").path("autonomous").asBoolean(false)
                || doc.path("autonomous").asBoolean(false));
        if (autonomous) autonomousSeen[0] = true;

        // The child executes AT its call line inside the parent: a CALL event, then the child's
        // subtree events. Each node's own events merge by line order around the call points.
        if (parent != null) {
            Event call = new Event();
            call.kind = "CALL";
            call.nodeId = id;
            call.proc = tree.path("name").asText("");
            call.depth = depth;
            call.line = tree.path("callLineNumber").asInt(0);
            out.add(call);
        }

        if (doc != null) {
            // DML writes from the node's own tables[].operations (INSERT/UPDATE/DELETE/MERGE lines).
            JsonNode tables = doc.path("tables");
            if (tables.isArray()) {
                for (JsonNode t : tables) {
                    JsonNode ops = t.path("operations");
                    if (!ops.isObject()) continue;
                    for (String op : new String[]{"INSERT", "UPDATE", "DELETE", "MERGE"}) {
                        JsonNode lines = ops.path(op);
                        for (int i = 0; i < lines.size(); i++) {
                            Event e = baseEvent(id, tree, depth, autonomous);
                            e.kind = "WRITE";
                            e.table = t.path("name").asText("");
                            e.op = op;
                            e.line = lines.path(i).asInt(0);
                            out.add(e);
                        }
                    }
                }
            }
            // Transaction statements (with the Phase-1 enrichment: guard/handler/isPartial/name).
            JsonNode ts = doc.path("transactionScope").path("statements");
            if (ts.isArray()) {
                for (JsonNode s : ts) {
                    String type = s.path("type").asText("").toUpperCase();
                    Event e = baseEvent(id, tree, depth, autonomous);
                    switch (type) {
                        case "COMMIT" -> e.kind = "COMMIT";
                        case "ROLLBACK" -> {
                            boolean partial = s.path("isPartial").asBoolean(false);
                            e.kind = partial ? "ROLLBACK_TO" : "ROLLBACK";
                        }
                        case "SAVEPOINT" -> e.kind = "SAVEPOINT";
                        case "BEGIN_TRAN" -> e.kind = "BEGIN_TRAN";
                        case "SET_TRANSACTION", "START_TRANSACTION" -> e.kind = "SET_TRANSACTION";
                        case "RELEASE_SAVEPOINT" -> e.kind = "RELEASE_SAVEPOINT";
                        case "LOCK_TABLE" -> e.kind = "LOCK";
                        default -> { continue; }
                    }
                    e.line = s.path("line").asInt(0);
                    String n = s.path("name").asText("");
                    if (!n.isEmpty()) e.name = n;
                    JsonNode g = s.path("guard");
                    if (g.isObject()) {
                        e.guardKind = g.path("kind").asText("");
                        e.guardLine = g.path("line").asInt(0);
                        e.guardCondition = g.path("condition").asText("");
                    }
                    JsonNode h = s.path("handler");
                    if (h.isObject()) e.handlerName = h.path("name").asText("");
                    out.add(e);
                }
            }
        }

        JsonNode children = tree.path("children");
        for (int i = 0; i < children.size(); i++) {
            collect(children.get(i), tree, depth + 1, analysisName, out, visited, autonomousSeen, truncated);
        }
    }

    private Event baseEvent(String id, JsonNode tree, int depth, boolean autonomous) {
        Event e = new Event();
        e.nodeId = id;
        e.proc = tree.path("name").asText("");
        e.depth = depth;
        e.autonomousNode = autonomous;
        return e;
    }

    private JsonNode loadNode(JsonNode treeNode, String analysisName) {
        try {
            String id = treeNode.path("id").asText("");
            if (id.isEmpty()) return null;
            return nodeLoader != null ? nodeLoader.apply(id) : null;
        } catch (Exception e) {
            return null; // a missing node doc contributes no events — the run still reflects the rest
        }
    }

    // ───────────────────────── path enumeration over guards ─────────────────────────

    static final class Run {
        final List<Map<String, Object>> assumptions = new ArrayList<>();
        final List<Event> events = new ArrayList<>();
        String kind = "main";
        String outcome;
        boolean pathsTruncated;
        final List<Map<String, Object>> writes = new ArrayList<>();
        String summary;
    }

    /**
     * Every guarded event is a branch point: on the "taken" path it stays, on the "absent" path
     * it is removed (the statement never executed on that path). The combinations are enumerated
     * over the guarded events' DECISIONS — bounded by {@link #MAX_PATHS}; with more than 6 guarded
     * events one conservative run is emitted instead (everything assumed taken; the guards remain
     * visible on the events for the UI to flag conditionality).
     */
    List<Run> enumerate(List<Event> timeline, boolean sqlServerNesting) {
        List<Integer> guardIdx = new ArrayList<>();
        for (int i = 0; i < timeline.size(); i++) {
            if (timeline.get(i).guardKind != null) guardIdx.add(i);
        }
        List<Run> out = new ArrayList<>();
        if (guardIdx.size() > 6) {                       // 2^6 = MAX_PATHS
            Run r = new Run();
            r.events.addAll(timeline);
            r.pathsTruncated = true;
            replay(r, sqlServerNesting, false);
            out.add(r);
            return out;
        }
        int combos = 1 << guardIdx.size();
        for (int mask = 0; mask < combos; mask++) {
            Run r = new Run();
            Set<Integer> absent = new HashSet<>();
            for (int b = 0; b < guardIdx.size(); b++) {
                boolean taken = (mask & (1 << b)) != 0;
                int idx = guardIdx.get(b);
                Event g = timeline.get(idx);
                if (!taken) absent.add(idx);
                Map<String, Object> a = new LinkedHashMap<>();
                a.put("nodeId", g.nodeId);
                a.put("line", g.line);
                a.put("condition", g.guardCondition);
                a.put("taken", taken);
                r.assumptions.add(a);
            }
            for (int i = 0; i < timeline.size(); i++) {
                if (!absent.contains(i)) r.events.add(timeline.get(i));
            }
            replay(r, sqlServerNesting, false);
            out.add(r);
        }
        return out;
    }

    // ───────────────────────── the replay (survival semantics) ─────────────────────────

    /**
     * Replays one run's events under the dialect's ownership rules and stamps each write's
     * {@code survival}: "survived" (a commit persisted it — or, on SQL Server, the write was
     * an AUTOCOMMIT outside any explicit frame), "discarded" (a rollback undid it), "open"
     * (the transaction is still open when the entry point returns — survives only if the
     * caller/session later commits). Autonomous-node writes always survive.
     *
     * <p>Key dialect asymmetry: Oracle/DB2/MySQL/Postgres start an IMPLICIT transaction on the
     * first DML, so every write joins the one open transaction and ANY commit (including a
     * callee's) persists all of it. SQL Server auto-commits statements outside an explicit
     * BEGIN TRANSACTION frame — a later ROLLBACK cannot touch those writes (noted on the row).
     * Package-private so the replay semantics are testable on hand-built runs.
     */
    void replay(Run r, boolean sqlServerNesting, boolean autonomousSide) {
        List<Event> uncommitted = new ArrayList<>();
        Deque<List<Event>> frames = new ArrayDeque<>();     // SQL Server explicit frames only
        Map<String, Integer> savepointLine = new LinkedHashMap<>(); // savepoint name → its event line
        boolean anyCommit = false, anyRollback = false, anyDiscard = false;
        Event lastCommitEvent = null, lastRollbackEvent = null;

        for (Event e : r.events) {
            switch (e.kind) {
                case "WRITE" -> {
                    if (sqlServerNesting && frames.isEmpty()) {
                        e.survival = "survived";   // autocommit — outside any explicit transaction
                    } else if (sqlServerNesting) {
                        frames.peek().add(e);
                    } else {
                        uncommitted.add(e);
                    }
                }
                case "SAVEPOINT" -> {
                    if (e.name != null) savepointLine.put(e.name, e.line);
                }
                case "ROLLBACK_TO" -> {
                    // Discard writes made strictly AFTER the target savepoint (same proc).
                    Integer spLine = e.name != null ? savepointLine.get(e.name) : null;
                    List<Event> after = new ArrayList<>();
                    for (Event w : uncommitted) {
                        if (spLine != null && w.nodeId.equals(e.nodeId) && w.line > spLine) after.add(w);
                    }
                    if (sqlServerNesting && !frames.isEmpty()) {
                        List<Event> frame = frames.peek();
                        for (Event w : frame) {
                            if (spLine != null && w.line > spLine) after.add(w);
                        }
                        frame.removeAll(after);
                    }
                    if (!after.isEmpty()) {
                        uncommitted.removeAll(after);
                        for (Event w : after) w.survival = "discarded";
                        anyDiscard = true;
                    }
                    lastRollbackEvent = e;
                }
                case "ROLLBACK" -> {
                    for (List<Event> f : frames) { for (Event w : f) w.survival = "discarded"; }
                    frames.clear();
                    for (Event w : uncommitted) w.survival = "discarded";
                    uncommitted.clear();
                    anyRollback = true; lastRollbackEvent = e;
                }
                case "COMMIT" -> {
                    if (sqlServerNesting && !frames.isEmpty()) {
                        List<Event> inner = frames.pop();
                        if (frames.isEmpty()) {
                            // The outermost explicit frame closed: its writes persist.
                            for (Event w : inner) w.survival = "survived";
                        } else {
                            // Inner commit: merged into the enclosing frame — nothing persists yet.
                            frames.peek().addAll(inner);
                        }
                    } else {
                        // Oracle/DB2/MySQL/PG: a commit ANYWHERE persists all uncommitted work
                        // up the call chain.
                        for (Event w : uncommitted) w.survival = "survived";
                        uncommitted.clear();
                    }
                    anyCommit = true; lastCommitEvent = e;
                }
                case "BEGIN_TRAN" -> {
                    if (sqlServerNesting) frames.push(new ArrayList<>());
                }
                default -> { /* CALL / SET_TRANSACTION / RELEASE_SAVEPOINT / LOCK: no survival effect */ }
            }
        }

        // Still open when the entry point returns → conditional on a later commit.
        for (Event w : uncommitted) w.survival = "open";

        // Outcome from the ACTUAL per-write verdicts (a COMMIT after a full ROLLBACK persisted
        // nothing — the counts, not the presence of a COMMIT statement, decide).
        boolean sawDiscard = r.events.stream().anyMatch(e -> "discarded".equals(e.survival));
        boolean sawSurvive = r.events.stream().anyMatch(e -> "survived".equals(e.survival));
        if (autonomousSide) r.outcome = "AUTONOMOUS";
        else if (sawSurvive && sawDiscard) r.outcome = "PARTIAL";
        else if (sawSurvive) r.outcome = "COMMITTED";
        else if (sawDiscard) r.outcome = "ROLLED_BACK";
        else r.outcome = "OPEN";

        long survived = 0, discarded = 0, open = 0;
        for (Event e : r.events) {
            if (!"WRITE".equals(e.kind)) continue;
            Map<String, Object> w = new LinkedHashMap<>();
            w.put("nodeId", e.nodeId);
            w.put("proc", e.proc);
            w.put("line", e.line);
            w.put("table", e.table);
            w.put("op", e.op);
            String survival = e.survival == null ? "open" : e.survival;
            switch (survival) {
                case "survived" -> survived++;
                case "discarded" -> discarded++;
                default -> open++;
            }
            if (e.autonomousNode) {
                survival = "survived";
                w.put("note", "autonomous transaction — survives the outer flow's rollback");
            }
            w.put("survival", survival);
            r.writes.add(w);
        }
        r.summary = summary(r, survived, discarded, open, lastCommitEvent, lastRollbackEvent);
    }

    private String summary(Run r, long survived, long discarded, long open,
                           Event lastCommit, Event lastRollback) {
        StringBuilder sb = new StringBuilder();
        if ("autonomous".equals(r.kind)) {
            sb.append("Autonomous transaction: its writes commit independently and survive any outer rollback.");
        } else if (survived > 0 && discarded > 0) {
            // The run ends PARTIAL: name BOTH sides — what the rollback undid, and what the
            // (possibly the callee's) commit made permanent before it.
            sb.append("On this path the work is PARTIALLY saved: ");
            boolean first = true;
            if (lastRollback != null) {
                sb.append("the ").append(lastRollback.kind.equals("ROLLBACK_TO") ? "ROLLBACK TO SAVEPOINT" : "ROLLBACK")
                  .append(" at line ").append(lastRollback.line).append(" (in ").append(lastRollback.proc)
                  .append(") undoes the uncommitted work up to that point");
                first = false;
            }
            if (lastCommit != null) {
                sb.append(first ? "" : "; ")
                  .append("the COMMIT at line ").append(lastCommit.line).append(" (in ").append(lastCommit.proc)
                  .append(") makes the work before it permanent");
            }
        } else if (survived > 0 && lastCommit != null) {
            sb.append("COMMIT at line ").append(lastCommit.line).append(" (in ").append(lastCommit.proc)
              .append(") persists all uncommitted work up the call chain");
        } else if (discarded > 0 && lastRollback != null) {
            // The dominant event is the rollback — even if a later commit ran, it persisted nothing.
            sb.append(lastRollback.kind.equals("ROLLBACK_TO")
                    ? "ROLLBACK TO SAVEPOINT at line " + lastRollback.line + " (in " + lastRollback.proc
                      + ") discards only the work after that savepoint"
                    : "ROLLBACK at line " + lastRollback.line + " (in " + lastRollback.proc
                      + ") discards the transaction's work");
        } else {
            sb.append("No terminal transaction statement — the transaction is still open when the entry point returns");
        }
        sb.append("; writes: ").append(survived).append(" survive");
        if (discarded > 0) sb.append(", ").append(discarded).append(" discarded");
        if (open > 0) sb.append(", ").append(open).append(" conditional on a later commit");
        sb.append(".");
        if (!r.assumptions.isEmpty()) {
            sb.append(" Assumes ");
            for (int i = 0; i < r.assumptions.size(); i++) {
                Map<String, Object> a = r.assumptions.get(i);
                if (i > 0) sb.append(" AND ");
                sb.append(a.get("condition")).append(" is ")
                  .append(Boolean.TRUE.equals(a.get("taken")) ? "true" : "false");
            }
            sb.append(".");
        }
        return sb.toString();
    }

    // ───────────────────────── rendering ─────────────────────────

    private JsonNode render(String name, String entry, String dialect, List<Run> runs, boolean truncated) {
        ObjectNode out = mapper.createObjectNode();
        out.put("entry", entry);
        out.put("dialect", dialect);
        out.put("pathCountExceeded", truncated);
        ArrayNode arr = out.putArray("runs");
        int id = 1;
        for (Run r : runs) {
            ObjectNode rn = arr.addObject();
            rn.put("runId", id++);
            rn.put("kind", r.kind);
            rn.put("outcome", r.outcome);
            ArrayNode asm = rn.putArray("assumptions");
            for (Map<String, Object> a : r.assumptions) {
                ObjectNode an = asm.addObject();
                an.put("nodeId", String.valueOf(a.get("nodeId")));
                an.put("line", ((Number) a.get("line")).intValue());
                an.put("condition", String.valueOf(a.get("condition")));
                an.put("taken", Boolean.TRUE.equals(a.get("taken")));
            }
            ArrayNode ev = rn.putArray("events");
            int seq = 1;
            for (Event e : r.events) {
                ObjectNode en = ev.addObject();
                en.put("seq", seq++);
                en.put("kind", e.kind);
                en.put("nodeId", e.nodeId);
                en.put("proc", e.proc);
                en.put("depth", e.depth);
                en.put("line", e.line);
                if (e.table != null) en.put("table", e.table);
                if (e.op != null) en.put("op", e.op);
                if (e.name != null) en.put("name", e.name);
                // The replay's per-write verdict — the UI's survival chip reads it off the event.
                if (e.survival != null) en.put("survival", e.survival);
                if (e.guardKind != null) {
                    ObjectNode g = en.putObject("guard");
                    g.put("kind", e.guardKind);
                    g.put("line", e.guardLine);
                    g.put("condition", e.guardCondition);
                }
                if (e.handlerName != null) en.put("handler", e.handlerName);
            }
            ArrayNode ws = rn.putArray("writes");
            for (Map<String, Object> w : r.writes) {
                ObjectNode wn = ws.addObject();
                wn.put("nodeId", String.valueOf(w.get("nodeId")));
                wn.put("proc", String.valueOf(w.get("proc")));
                wn.put("line", ((Number) w.get("line")).intValue());
                wn.put("table", String.valueOf(w.get("table")));
                wn.put("op", String.valueOf(w.get("op")));
                wn.put("survival", String.valueOf(w.get("survival")));
                if (w.containsKey("note")) wn.put("note", String.valueOf(w.get("note")));
            }
            rn.put("summary", r.summary);
        }
        return out;
    }

    // The dialect is the first '~'-separated segment of the canonical analysis name
    // (AnalysisPaths.name builds <DBTYPE>~<PROJECT>~<ENV>~<OBJECT>).
    static String dialectOf(String analysisName) {
        if (analysisName == null) return "ORACLE";
        String[] parts = analysisName.split("~");
        String d = parts.length > 1 ? parts[0].toUpperCase() : "";
        return switch (d) {
            case "MYSQL", "POSTGRES", "DB2", "SQLSERVER" -> d;
            default -> "ORACLE";
        };
    }
}
