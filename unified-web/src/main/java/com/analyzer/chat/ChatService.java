package com.analyzer.chat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ChatService {

    private static final Logger log = LoggerFactory.getLogger(ChatService.class);
    private static final int MAX_CONTEXT_MESSAGES = 20;
    private static final int CLAUDE_TIMEOUT_SECONDS = 300;

    private final ClaudeProcessRunner processRunner;
    /** {@code claude-chatbot/} under the data root. */
    private static final String CHAT_ROOT = "claude-chatbot";
    private final ArtifactStore store;
    private final ObjectMapper mapper;
    private final ConcurrentHashMap<String, ReentrantLock> sessionLocks = new ConcurrentHashMap<>();

    private AnalysisContextProvider jarContextProvider;
    private AnalysisContextProvider plsqlContextProvider;

    private static final List<String> CHAT_TYPES = List.of("classic", "new");

    /**
     * The chatbot's sandboxed working directory (see ChatConfig.java's {@code
     * chatClaudeProcessRunner} bean — this is the exact same directory that bean sets as the
     * Claude CLI process's cwd). Used only to notice script files the assistant has written there
     * so they can be captured into {@link #scriptsBySession} — never to grant any NEW access; this
     * class only reads.
     */
    private final java.io.File workspaceDir;

    /**
     * In-memory mirror of every script the assistant has written this run, per session — the
     * "keep in-memory" half of the requested script-history feature. Persisted copies (the
     * "store into DB" half) live under {@code claude-chatbot/scripts/<sessionId>/<filename>} via
     * the same {@link ArtifactStore} everything else in this class already uses (which is a real
     * MongoDB-backed collection when the app runs in mongodb storage mode — see
     * StorageFactory/MongoArtifactStore), so a restart doesn't lose script history the way a
     * purely in-memory map would.
     */
    private final ConcurrentHashMap<String, List<Map<String, Object>>> scriptsBySession = new ConcurrentHashMap<>();
    /** Same shape as scriptsBySession, for files the assistant wrote to ./output/ — the
     *  downloadable RESULTS of an extract/filter request, as opposed to the script that made them. */
    private final ConcurrentHashMap<String, List<Map<String, Object>>> outputsBySession = new ConcurrentHashMap<>();
    private static final java.util.regex.Pattern SCRIPT_FILE = java.util.regex.Pattern.compile("(?i).+\\.(py|sh)$");

    public ChatService(ClaudeProcessRunner processRunner, ArtifactStore store) {
        this(processRunner, store, null);
    }

    public ChatService(ClaudeProcessRunner processRunner, ArtifactStore store, java.io.File workspaceDir) {
        this.processRunner = processRunner;
        this.store = Objects.requireNonNull(store, "ArtifactStore is required");
        this.workspaceDir = workspaceDir;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // No directory creation: the store makes parents on write, and an object store has none.
    }

    // ── Locations ─────────────────────────────────────────────────────────────

    private static ArtifactKey chatDir(String chatType) {
        return ArtifactKey.of(CHAT_ROOT, chatType);
    }

    private static ArtifactKey sessionKey(String chatType, String id) {
        return chatDir(chatType).resolve(id + ".json");
    }

    /**
     * Session ids and chat types arrive from REST paths and become key segments. The old
     * {@code Path.resolve} form would have followed a {@code ..}; the guard is explicit now that
     * there is no base path to compare against.
     */
    private static boolean badSegment(String s) {
        return s == null || s.isBlank() || s.contains("..") || s.contains("/") || s.contains("\\");
    }

    private InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    private OutputStream openWrite(ArtifactKey key) throws IOException { return store.write(key); }

    public ChatSession createSession(String name, String scope, String chatType, Map<String, String> scopeContext) {
        ChatSession session = new ChatSession(name, scope, chatType, scopeContext);
        saveSession(session);
        log.info("Created chat session: {} [{}] scope={} type={}", session.getId(), name, scope, session.getChatType());
        return session;
    }

    public List<Map<String, Object>> listSessions(String scopeFilter, String chatType) {
        List<Map<String, Object>> summaries = new ArrayList<>();
        if (chatType != null && badSegment(chatType)) return summaries;
        List<ArtifactKey> dirs = chatType != null
                ? List.of(chatDir(chatType))
                : List.of(chatDir("classic"), chatDir("new"));

        for (ArtifactKey dir : dirs) {
            try (var files = store.list(dir, false)) {
                files.filter(java.util.function.Predicate.not(ArtifactEntry::directory))
                        .map(ArtifactEntry::key)
                        .filter(k -> k.leaf().endsWith(".json"))
                        .forEach(p -> {
                            try (InputStream in = openRead(p)) {
                                ChatSession s = mapper.readValue(in, ChatSession.class);
                                if (scopeFilter != null && !scopeFilter.equalsIgnoreCase(s.getScope())) return;
                                Map<String, Object> summary = new LinkedHashMap<>();
                                summary.put("id", s.getId());
                                summary.put("name", s.getName());
                                summary.put("scope", s.getScope());
                                summary.put("chatType", s.getChatType());
                                summary.put("messageCount", s.getMessages().size());
                                summary.put("createdAt", s.getCreatedAt());
                                summary.put("updatedAt", s.getUpdatedAt());
                                summaries.add(summary);
                            } catch (IOException e) {
                                log.debug("Failed to read session file {}: {}", p, e.getMessage());
                            }
                        });
            } catch (IOException e) {
                log.error("Failed to list sessions in {}: {}", dir, e.getMessage());
            }
        }

        summaries.sort((a, b) -> {
            Instant ua = (Instant) b.get("updatedAt");
            Instant ub = (Instant) a.get("updatedAt");
            if (ua == null || ub == null) return 0;
            return ua.compareTo(ub);
        });
        return summaries;
    }

    public ChatSession getSession(String id) {
        if (badSegment(id)) return null;
        for (String type : CHAT_TYPES) {
            ArtifactKey file = sessionKey(type, id);
            if (store.exists(file)) {
                try (InputStream in = openRead(file)) {
                    return mapper.readValue(in, ChatSession.class);
                } catch (IOException e) {
                    log.error("Failed to read session {}: {}", id, e.getMessage());
                    return null;
                }
            }
        }
        ArtifactKey legacy = ArtifactKey.of(CHAT_ROOT, id + ".json");
        if (store.exists(legacy)) {
            try (InputStream in = openRead(legacy)) {
                return mapper.readValue(in, ChatSession.class);
            } catch (IOException e) {
                log.error("Failed to read legacy session {}: {}", id, e.getMessage());
            }
        }
        return null;
    }

    /**
     * The session document as bytes, or null if there is no such session — Q67's input.
     *
     * <p>Separate from {@link #getSession} because the transcript is paged by streaming the
     * {@code messages} array, and {@code getSession} exists precisely to deserialise all of it.
     * Resolution is the same three-way lookup (typed folder, then the legacy flat file) so a
     * session paged is the same session read.
     */
    InputStream openSessionDocument(String id) throws IOException {
        if (badSegment(id)) return null;
        for (String type : CHAT_TYPES) {
            ArtifactKey file = sessionKey(type, id);
            if (store.exists(file)) return openRead(file);
        }
        ArtifactKey legacy = ArtifactKey.of(CHAT_ROOT, id + ".json");
        return store.exists(legacy) ? openRead(legacy) : null;
    }

    /**
     * One backward page of a session's transcript.
     *
     * @return null when the session does not exist, which the controller answers with 404 —
     *         distinct from an existing session whose filter matched nothing (an empty page)
     */
    public com.analyzer.storage.page.Page<com.fasterxml.jackson.databind.JsonNode> messages(
            String id, Long before, Integer limit, String query) throws IOException {
        if (badSegment(id)) return null;
        boolean exists = false;
        for (String type : CHAT_TYPES) {
            if (store.exists(sessionKey(type, id))) { exists = true; break; }
        }
        if (!exists && !store.exists(ArtifactKey.of(CHAT_ROOT, id + ".json"))) return null;
        return new ChatTranscriptReader(mapper)
                .read(() -> openSessionDocument(id), before, limit, query);
    }

    public boolean deleteSession(String id) {
        if (badSegment(id)) return false;
        sessionLocks.remove(id);

        // Real gap this closes: this used to delete only the session document itself, leaving
        // every script/output this session ever generated as an orphan — durably in the
        // ArtifactStore (Mongo/S3/local, forever undeletable via any UI path) AND, for outputs,
        // on local disk under data/claude-chatbot-workspace/output/<id>/ (scripts are shared
        // scratch files at the workspace root, not per-session, so they're left alone here — see
        // captureGeneratedArtifacts's doc comment on why).
        purgeSessionArtifacts(id, "scripts", scriptsBySession, null);
        purgeSessionArtifacts(id, "outputs", outputsBySession,
                workspaceDir != null ? new java.io.File(workspaceDir, "output" + java.io.File.separator + id) : null);

        for (String type : CHAT_TYPES) {
            if (store.delete(sessionKey(type, id))) return true;
        }
        return store.delete(ArtifactKey.of(CHAT_ROOT, id + ".json"));
    }

    private void purgeSessionArtifacts(String sessionId, String kind,
                                        ConcurrentHashMap<String, List<Map<String, Object>>> registry,
                                        java.io.File localDirToRemove) {
        List<Map<String, Object>> known = registry.remove(sessionId);
        if (known != null) {
            for (Map<String, Object> m : known) {
                Object filename = m.get("filename");
                if (filename instanceof String fn) store.delete(artifactKey(kind, sessionId, fn));
            }
        }
        if (localDirToRemove != null && localDirToRemove.isDirectory()) {
            java.io.File[] left = localDirToRemove.listFiles();
            if (left != null) for (java.io.File f : left) f.delete();
            localDirToRemove.delete();
        }
    }

    public ChatMessage sendMessage(String sessionId, String userMessage, String pageContext) {
        ChatSession session = getSession(sessionId);
        if (session == null) return null;

        ReentrantLock lock = sessionLocks.computeIfAbsent(sessionId, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            return new ChatMessage("assistant", "A message is already being processed for this session. Please wait.");
        }
        try {
            session.getMessages().add(new ChatMessage("user", userMessage));

            String prompt = buildPrompt(session, userMessage, pageContext);
            String response = processRunner.run(prompt, null, CLAUDE_TIMEOUT_SECONDS);

            if (response == null || response.isBlank()) {
                response = "I'm sorry, I couldn't generate a response. Please try again.";
            }

            ChatMessage assistantMsg = new ChatMessage("assistant", response);
            session.getMessages().add(assistantMsg);
            session.setUpdatedAt(Instant.now());
            saveSession(session);

            captureGeneratedArtifacts(sessionId);

            return assistantMsg;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Scans the chatbot's LOCAL sandbox for anything new/changed and copies it into the {@link
     * ArtifactStore} — the same storage abstraction every other artifact in this app already goes
     * through (Local/MongoDB/S3, selected by {@code app.storage.mode}; see StorageFactory). This
     * is the fix for a real gap: the sandbox directory is necessarily always a real local
     * filesystem path (Claude's own CLI process has to execute against real files — a script
     * cannot run "inside S3"), but that is ONLY the scratch/execution copy. The durable,
     * listable, downloadable, deletable copy the UI actually reads back — scripts AND their
     * output/ results both — now goes through {@code store}, so it lands wherever the rest of the
     * app's data lands: {@code data/} locally, MongoDB in mongodb mode, the configured bucket in
     * S3 mode. Before this, output/ files were served straight off local disk unconditionally
     * (ChatController's old /workspace/files endpoints), which broke that guarantee for exactly
     * this one feature while everything else in the app already respected storage mode.
     */
    private void captureGeneratedArtifacts(String sessionId) {
        if (workspaceDir == null || !workspaceDir.isDirectory()) return;
        // Scripts are deliberately left on disk after capture (deleteAfterCapture=false): Claude
        // may legitimately re-run/re-edit a script it wrote earlier in the SAME live session
        // ("run gen.py again"), and it only has a real filesystem to do that against — the durable
        // Mongo/S3 copy is for the UI's history/download, not for Claude's own re-execution.
        java.io.File[] scriptFiles = workspaceDir.listFiles((dir, name) -> SCRIPT_FILE.matcher(name).matches());
        captureFiles(sessionId, scriptFiles, "scripts", scriptsBySession, false);

        // Outputs are one-shot results (a JSON/CSV report Claude generated once) — nothing in this
        // app re-reads them off local disk afterward, so once the durable copy is safely persisted
        // the local scratch copy serves no further purpose. Real gap this closes: previously
        // data/claude-chatbot-workspace/output/<sessionId>/ accumulated forever even in mongodb
        // storage mode, since capture only ever copied, never cleaned up, the local file.
        java.io.File outputDir = new java.io.File(workspaceDir, "output" + java.io.File.separator + sessionId);
        java.io.File[] outputFiles = outputDir.isDirectory() ? outputDir.listFiles(java.io.File::isFile) : null;
        captureFiles(sessionId, outputFiles, "outputs", outputsBySession, true);
        // Every file just captured above was deleted; if that leaves the directory empty, remove
        // it too rather than leaving an ever-growing pile of empty output/<sessionId>/ folders.
        if (outputDir.isDirectory()) {
            String[] remaining = outputDir.list();
            if (remaining != null && remaining.length == 0) outputDir.delete();
        }
    }

    private void captureFiles(String sessionId, java.io.File[] files, String kind,
                               ConcurrentHashMap<String, List<Map<String, Object>>> registry,
                               boolean deleteAfterCapture) {
        if (files == null) return;
        List<Map<String, Object>> known = registry.computeIfAbsent(sessionId, k -> new ArrayList<>());
        for (java.io.File f : files) {
            byte[] content;
            try {
                content = java.nio.file.Files.readAllBytes(f.toPath());
            } catch (IOException e) {
                log.warn("Could not read generated {} file {}: {}", kind, f, e.getMessage());
                continue;
            }
            String contentHash = Integer.toHexString(java.util.Arrays.hashCode(content));
            boolean unchanged;
            synchronized (known) {
                unchanged = known.stream().anyMatch(m ->
                        f.getName().equals(m.get("filename")) && contentHash.equals(m.get("contentHash")));
            }
            if (unchanged) {
                // Already durably captured under this exact content — safe to clear the scratch
                // copy even on a repeat visit (e.g. capture ran once already this same call).
                if (deleteAfterCapture) f.delete();
                continue;
            }

            try (OutputStream os = store.write(artifactKey(kind, sessionId, f.getName()), content.length)) {
                os.write(content);
            } catch (IOException e) {
                log.warn("Could not persist generated {} file {} for session {}: {}", kind, f.getName(), sessionId, e.getMessage());
                continue; // leave the local scratch copy in place — it's the only surviving copy until this succeeds
            }

            Map<String, Object> record = new LinkedHashMap<>();
            record.put("filename", f.getName());
            record.put("contentHash", contentHash);
            record.put("sizeBytes", content.length);
            record.put("capturedAt", Instant.now().toString());
            synchronized (known) {
                known.removeIf(m -> f.getName().equals(m.get("filename")));
                known.add(record);
            }
            log.info("Captured generated {} '{}' for chat session {} (via ArtifactStore, storage-mode-aware)", kind, f.getName(), sessionId);

            if (deleteAfterCapture) {
                if (!f.delete()) {
                    log.warn("Captured {} '{}' but could not delete the local scratch copy at {}", kind, f.getName(), f);
                }
            }
        }
    }

    private static ArtifactKey artifactKey(String kind, String sessionId, String filename) {
        return ArtifactKey.of(CHAT_ROOT, kind, sessionId, filename);
    }

    private static ArtifactKey scriptKey(String sessionId, String filename) {
        return artifactKey("scripts", sessionId, filename);
    }

    private static ArtifactKey outputKey(String sessionId, String filename) {
        return artifactKey("outputs", sessionId, filename);
    }

    /** List of {filename, sizeBytes, capturedAt} for every script captured this run, newest first. */
    public List<Map<String, Object>> listScripts(String sessionId) {
        return listArtifacts(scriptsBySession, sessionId);
    }

    /** Same shape as {@link #listScripts}, for files the assistant wrote to ./output/ — the
     *  downloadable results of an extract/filter request. */
    public List<Map<String, Object>> listOutputs(String sessionId) {
        return listArtifacts(outputsBySession, sessionId);
    }

    private static List<Map<String, Object>> listArtifacts(
            ConcurrentHashMap<String, List<Map<String, Object>>> registry, String sessionId) {
        List<Map<String, Object>> known = registry.get(sessionId);
        if (known == null) return List.of();
        List<Map<String, Object>> copy;
        synchronized (known) { copy = new ArrayList<>(known); }
        copy.sort((a, b) -> String.valueOf(b.get("capturedAt")).compareTo(String.valueOf(a.get("capturedAt"))));
        return copy;
    }

    /** Full source of one captured script, read back from the durable store (not the in-memory
     *  registry, which only holds metadata) — works even after a restart, as long as the caller
     *  already knows the filename from {@link #listScripts}. */
    public String getScriptContent(String sessionId, String filename) {
        if (badSegment(sessionId) || badSegment(filename)) return null;
        try (InputStream is = store.read(scriptKey(sessionId, filename))) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    /** Raw bytes of one captured output file, read back from the durable store — same
     *  storage-mode-aware path as {@link #getScriptContent}, binary-safe (an .xlsx isn't text). */
    public byte[] getOutputBytes(String sessionId, String filename) {
        if (badSegment(sessionId) || badSegment(filename)) return null;
        try (InputStream is = store.read(outputKey(sessionId, filename))) {
            return is.readAllBytes();
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Explicit, human-triggered deletion — the assistant itself never calls this (there is no
     * tool-call path from Claude to this method); it exists only for the app's own "manage script
     * history" UI. Removes the durable copy, the in-memory record, AND the actual file in the
     * sandbox (so the assistant does not silently keep using a script the user asked to remove).
     */
    public boolean deleteScript(String sessionId, String filename) {
        return deleteArtifact(scriptKey(sessionId, filename), scriptsBySession, sessionId, filename, workspaceDir);
    }

    /**
     * Deletes one captured output/result file — the "delete once downloaded" / "flush old JSON"
     * cleanup the user explicitly asked for. Same three-part removal as {@link #deleteScript}
     * (durable store copy, in-memory listing, live sandbox file), just rooted at ./output/ instead
     * of the sandbox root.
     */
    public boolean deleteOutput(String sessionId, String filename) {
        java.io.File outputDir = workspaceDir != null ? new java.io.File(workspaceDir, "output" + java.io.File.separator + sessionId) : null;
        return deleteArtifact(outputKey(sessionId, filename), outputsBySession, sessionId, filename, outputDir);
    }

    private boolean deleteArtifact(ArtifactKey key,
                                    ConcurrentHashMap<String, List<Map<String, Object>>> registry,
                                    String sessionId, String filename, java.io.File containingDir) {
        if (badSegment(sessionId) || badSegment(filename)) return false;
        boolean storeDeleted = store.delete(key);

        List<Map<String, Object>> known = registry.get(sessionId);
        boolean memRemoved = false;
        if (known != null) {
            synchronized (known) {
                memRemoved = known.removeIf(m -> filename.equals(m.get("filename")));
            }
        }

        if (containingDir != null) {
            java.io.File target = new java.io.File(containingDir, filename);
            // Containment check: filename must resolve to a direct child of containingDir, not
            // escape it via "../" — same discipline as ChatController's workspace file endpoints.
            try {
                if (containingDir.isDirectory()
                        && target.getCanonicalFile().getParentFile().equals(containingDir.getCanonicalFile())) {
                    target.delete();
                }
            } catch (IOException ignored) { /* best-effort filesystem cleanup only */ }
        }

        return storeDeleted || memRemoved;
    }

    public String generateMarkdownReport(String sessionId) {
        ChatSession session = getSession(sessionId);
        if (session == null) return null;

        StringBuilder md = new StringBuilder();
        md.append("# Chat Session: ").append(session.getName()).append("\n\n");
        md.append("**Scope:** ").append(session.getScope()).append("  \n");
        md.append("**Created:** ").append(session.getCreatedAt()).append("  \n");
        md.append("**Messages:** ").append(session.getMessages().size()).append("\n\n");
        md.append("---\n\n");

        for (ChatMessage msg : session.getMessages()) {
            if ("user".equals(msg.getRole())) {
                md.append("## User\n\n").append(msg.getContent()).append("\n\n");
            } else {
                md.append("## Assistant\n\n").append(msg.getContent()).append("\n\n");
            }
            md.append("---\n\n");
        }

        return md.toString();
    }

    public void setJarContextProvider(AnalysisContextProvider provider) {
        this.jarContextProvider = provider;
    }

    public void setPlsqlContextProvider(AnalysisContextProvider provider) {
        this.plsqlContextProvider = provider;
    }

    private static final int MAX_PAGE_CONTEXT_CHARS = 8000;

    private String buildPrompt(ChatSession session, String latestMessage, String pageContext) {
        StringBuilder prompt = new StringBuilder();

        prompt.append("You are an AI assistant for a code analysis platform. ");
        prompt.append("\n\nYou may write and run scripts (e.g. Python) in your own working directory to filter, "
                + "extract, or reshape analysis data the user asks for. Ground rules, enforced by a hook that "
                + "will deny the tool call if you don't follow them, so treat these as hard limits, not "
                + "suggestions: read-only against any real data source (only SELECT/find/aggregate-style reads — "
                + "never delete, drop, truncate, update, or kill anything, and never touch files outside your "
                + "own working directory). When the user wants an extracted/filtered result to download, write "
                + "the final result to a NEW file under ./output/" + session.getId() + "/ in your working directory "
                + "(create that folder if it doesn't exist yet), named descriptively (e.g. "
                + "output/" + session.getId() + "/high_risk_endpoints.json), and tell the user the exact filename "
                + "you used so the app can offer it as a download. Use exactly this session's own output folder — "
                + "not a different session's — so results stay separated per conversation.");

        String contextSummary = getContextSummary(session);
        if (contextSummary != null && !contextSummary.isBlank()) {
            prompt.append("\n\nHere is context about the current analysis:\n");
            prompt.append(contextSummary);
        }

        if (pageContext != null && !pageContext.isBlank()) {
            String trimmed = pageContext.length() > MAX_PAGE_CONTEXT_CHARS
                    ? pageContext.substring(0, MAX_PAGE_CONTEXT_CHARS) + "\n...(truncated)"
                    : pageContext;
            prompt.append("\n\nThe user is currently viewing this page context:\n");
            prompt.append(trimmed);
        }

        List<ChatMessage> messages = session.getMessages();
        int start = Math.max(0, messages.size() - MAX_CONTEXT_MESSAGES);
        if (start > 0) {
            prompt.append("\n\n[Earlier conversation messages omitted for brevity]\n");
        }
        for (int i = start; i < messages.size(); i++) {
            ChatMessage m = messages.get(i);
            prompt.append("\n\n").append("user".equals(m.getRole()) ? "Human" : "Assistant")
                    .append(": ").append(m.getContent());
        }

        return prompt.toString();
    }

    private String getContextSummary(ChatSession session) {
        String scope = session.getScope();
        Map<String, String> ctx = session.getScopeContext();

        if ("PLSQL".equalsIgnoreCase(scope) && plsqlContextProvider != null) {
            String analysisName = ctx != null ? ctx.get("analysisName") : null;
            return plsqlContextProvider.getContextSummary(analysisName);
        }
        if ("JAR".equalsIgnoreCase(scope) && jarContextProvider != null) {
            String jarName = ctx != null ? ctx.get("jarName") : null;
            return jarContextProvider.getContextSummary(jarName);
        }
        if ("GLOBAL".equalsIgnoreCase(scope)) {
            StringBuilder global = new StringBuilder();
            if (plsqlContextProvider != null) {
                String plsqlCtx = plsqlContextProvider.getContextSummary(null);
                if (plsqlCtx != null) global.append("## PL/SQL Analysis\n").append(plsqlCtx).append("\n\n");
            }
            if (jarContextProvider != null) {
                String jarCtx = jarContextProvider.getContextSummary(null);
                if (jarCtx != null) global.append("## JAR Analysis\n").append(jarCtx).append("\n\n");
            }
            return global.toString();
        }
        return null;
    }

    private void saveSession(ChatSession session) {
        String type = session.getChatType() != null ? session.getChatType() : "classic";
        if (badSegment(type) || badSegment(session.getId())) {
            log.error("Refusing to save session with an unsafe id/type: {}/{}", type, session.getId());
            return;
        }
        try (OutputStream out = openWrite(sessionKey(type, session.getId()))) {
            mapper.writerWithDefaultPrettyPrinter().writeValue(out, session);
        } catch (IOException e) {
            log.error("Failed to save session {}: {}", session.getId(), e.getMessage());
        }
    }

    @FunctionalInterface
    public interface AnalysisContextProvider {
        String getContextSummary(String identifier);
    }
}
