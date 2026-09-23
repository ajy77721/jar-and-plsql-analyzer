package com.jaranalyzer.service;

import com.jaranalyzer.controller.ConversionController.ConversionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds Claude instruction markdown for Oracle → Java conversion commands.
 *
 * Each action (/convert-procedure, /validate-conversion, /fix-conversion) maps to
 * an instruction prompt that Claude Code executes in the context of the user's
 * opus-product-services project.  The prompt references the same phase structure as
 * the oracle-mongo-lens slash commands so Claude knows exactly what to do.
 */
@Service
public class ConversionService {

    private static final Logger log = LoggerFactory.getLogger(ConversionService.class);

    private static final Pattern REPORT_PATH_PATTERN =
        Pattern.compile("(?:Report written to|VALIDATION_REPORT)[:\\s]+([^\\s\\n]+\\.md)", Pattern.CASE_INSENSITIVE);

    /**
     * Build the full instruction prompt for Claude CLI.
     *
     * @param action  "convert" | "validate" | "fix"
     * @param req     Request parameters from the UI
     * @return Markdown instruction text to pipe to Claude CLI via stdin
     */
    public String buildInstruction(String action, ConversionRequest req) {
        return switch (action) {
            case "convert"  -> buildConvertInstruction(req);
            case "validate" -> buildValidateInstruction(req);
            case "fix"      -> buildFixInstruction(req);
            default -> throw new IllegalArgumentException("Unknown action: " + action);
        };
    }

    /**
     * Extract the report file path from Claude's output text.
     */
    public String extractReportPath(String claudeOutput, ConversionRequest req) {
        if (claudeOutput == null) return null;
        Matcher m = REPORT_PATH_PATTERN.matcher(claudeOutput);
        if (m.find()) return m.group(1).trim();
        return null;
    }

    // ── /convert-procedure ────────────────────────────────────────────────────

    private String buildConvertInstruction(ConversionRequest req) {
        validateRequired(req, "procedure", "module");

        StringBuilder sb = new StringBuilder();
        sb.append("You are executing the /convert-procedure workflow from the oracle-mongo-lens toolkit.\n\n");
        sb.append("## Task\n\n");
        sb.append("Convert the Oracle PL/SQL procedure `").append(req.procedure()).append("` to Java + Spring Boot + MongoDB.\n");
        sb.append("Follow the full 11-phase conversion workflow below exactly.\n\n");

        appendProjectContext(sb, req);
        appendConversionGuidelines(sb);

        sb.append("## Command\n\n");
        sb.append("```\n");
        sb.append("/convert-procedure");
        sb.append(" --procedure ").append(req.procedure());
        sb.append(" --module ").append(req.module());
        if (notBlank(req.service()))     sb.append(" --service ").append(req.service());
        if (notBlank(req.controller())) sb.append(" --controller ").append(req.controller());
        if (notBlank(req.owner()))       sb.append(" --owner ").append(req.owner());
        if (req.batch())                 sb.append(" --batch");
        sb.append("\n```\n\n");

        appendConvertPhases(sb, req);
        appendOutputInstructions(sb, "convert", req);
        return sb.toString();
    }

    // ── /validate-conversion ──────────────────────────────────────────────────

    private String buildValidateInstruction(ConversionRequest req) {
        validateRequired(req, "procedure", "module");
        validateServiceOrController(req);

        StringBuilder sb = new StringBuilder();
        sb.append("You are executing the /validate-conversion workflow from the oracle-mongo-lens toolkit.\n\n");
        sb.append("## Task\n\n");
        sb.append("Reverse-validate the Oracle → Java conversion of `").append(req.procedure()).append("`.\n");
        sb.append("Follow the full 8-phase validation workflow below exactly.\n\n");

        appendProjectContext(sb, req);
        appendConversionGuidelines(sb);

        sb.append("## Command\n\n");
        sb.append("```\n");
        sb.append("/validate-conversion");
        sb.append(" --procedure ").append(req.procedure());
        sb.append(" --module ").append(req.module());
        if (notBlank(req.service()))     sb.append(" --service ").append(req.service());
        if (notBlank(req.controller())) sb.append(" --controller ").append(req.controller());
        if (notBlank(req.owner()))       sb.append(" --owner ").append(req.owner());
        if (req.batch())                 sb.append(" --batch");
        sb.append("\n```\n\n");

        appendValidatePhases(sb, req);
        appendOutputInstructions(sb, "validate", req);
        return sb.toString();
    }

    // ── /fix-conversion ───────────────────────────────────────────────────────

    private String buildFixInstruction(ConversionRequest req) {
        validateRequired(req, "procedure", "module");
        validateServiceOrController(req);

        StringBuilder sb = new StringBuilder();
        sb.append("You are executing the /fix-conversion workflow from the oracle-mongo-lens toolkit.\n\n");
        sb.append("## Task\n\n");
        sb.append("Auto-fix all open failures from the latest validation report for `").append(req.procedure()).append("`.\n");
        sb.append("Follow the full 7-phase fix workflow below exactly. Loop up to 3 times.\n\n");

        appendProjectContext(sb, req);
        appendConversionGuidelines(sb);

        sb.append("## Command\n\n");
        sb.append("```\n");
        sb.append("/fix-conversion");
        sb.append(" --procedure ").append(req.procedure());
        sb.append(" --module ").append(req.module());
        if (notBlank(req.service()))     sb.append(" --service ").append(req.service());
        if (notBlank(req.controller())) sb.append(" --controller ").append(req.controller());
        if (notBlank(req.owner()))       sb.append(" --owner ").append(req.owner());
        if (req.batch())                 sb.append(" --batch");
        sb.append("\n```\n\n");

        appendFixPhases(sb, req);
        appendOutputInstructions(sb, "fix", req);
        return sb.toString();
    }

    // ── Phase instructions ────────────────────────────────────────────────────

    private void appendConvertPhases(StringBuilder sb, ConversionRequest req) {
        sb.append("## Phases to execute\n\n");
        sb.append("Execute these phases IN ORDER. Do NOT skip any phase.\n\n");
        sb.append("**PHASE 0** — Read `CLAUDE.md` and `CONVERSION_GUIDELINES.md` from the project root.\n\n");
        sb.append("**PHASE 0B** — Build or load the dependency tree using `.claude/proc-analyzer/analyze_proc.py`.\n\n");
        sb.append("**PHASE 1** — Fetch Oracle source from `ALL_SOURCE` (check `.claude/proc-analyzer/sql-cache/` first).\n\n");
        sb.append("**PHASE 1B** — Chunked analysis of Oracle source (mandatory for procedures > 500 lines).\n\n");
        sb.append("**PHASE 2** — Decompose Oracle procedure into logical steps (params, branches, DML, calls, exceptions).\n\n");
        sb.append("**PHASE 3** — Explore codebase: find existing beans, repositories, services, base packages.\n\n");
        sb.append("**PHASE 4** — Print conversion plan and WAIT for user confirmation before writing code.\n\n");
        sb.append("**PHASE 5** — Check dependency status.\n\n");
        sb.append("**PHASE 6** — Implement root procedure section-by-section. Run `mvn compile` after each section.\n\n");
        sb.append("**PHASE 7** — Run `mvn clean compile -pl ").append(req.module()).append(" -am -DskipTests`.\n\n");
        sb.append("**PHASE 8** — Run `./validate.sh ").append(req.module()).append(" --proc ").append(req.procedure()).append("`.\n\n");
        sb.append("**PHASE 9** — Full validation (re-run /validate-conversion sub-agent).\n\n");
        sb.append("**PHASE 10** — Fix loop (max 3 iterations): compile → validate → fix.\n\n");
        sb.append("**PHASE 11** — Print final summary + dependency tree.\n\n");
    }

    private void appendValidatePhases(StringBuilder sb, ConversionRequest req) {
        sb.append("## Phases to execute\n\n");
        sb.append("Execute these phases IN ORDER. Do NOT skip any phase.\n\n");

        sb.append("**PHASE 0** — Parse arguments. Load `CLAUDE.md` and `CONVERSION_GUIDELINES.md`.\n\n");
        sb.append("**PHASE 0B** — Load or build dependency tree (MANDATORY).\n\n");
        sb.append("**PHASE 0C** — Decompose Oracle procedure into a structured functional JSON (ground truth).\n");
        sb.append("  Extract for each function: name, Java method signature, business logic, tables used, dependencies.\n");
        sb.append("  Store as `FUNCTIONAL_DESC_").append(req.procedure()).append("_<YYYYMMDD>.json` in the module directory.\n");
        sb.append("  This JSON is the authoritative spec — all Phase 4 checks validate Java against IT, not the raw PL/SQL.\n\n");

        sb.append("**PHASE 1** — Fetch Oracle source (check sql-cache first, then Oracle MCP).\n\n");
        sb.append("**PHASE 1B** — Chunked analysis of full Oracle source if > 500 lines (mandatory).\n\n");
        sb.append("**PHASE 2** — Locate and read Java implementation files (service impl, controller, test class).\n\n");

        sb.append("**PHASE 3** — Verify structural completeness.\n");
        sb.append("  For every function in the functional JSON from Phase 0C:\n");
        sb.append("  - Confirm a corresponding Java method exists\n");
        sb.append("  - Confirm `/* PROCEDUREREF - ").append(req.procedure()).append(" */` comment is present\n");
        sb.append("  - If method only delegates to another method in the same class, validate the delegate — do NOT flag as missing\n\n");

        sb.append("**PHASE 4** — Logic coverage mapping — run ALL sub-phases:\n\n");

        sb.append("  **4A — Parameter mapping**\n");
        sb.append("  For each Oracle IN/OUT/IN OUT parameter:\n");
        sb.append("  - IN → Java method parameter with correct semantic type\n");
        sb.append("  - OUT → Java return type (not void unless truly no return value)\n");
        sb.append("  - IN OUT → verify it is either split into separate methods or handled via wrapper DTO\n\n");

        sb.append("  **4B — Cursor and loop validation**\n");
        sb.append("  For each `CURSOR`, `FOR rec IN (SELECT ...)`, `OPEN/FETCH/CLOSE`:\n");
        sb.append("  - Java must have a corresponding `List<T>` iteration — not per-item `findById()` calls in a loop\n");
        sb.append("  - `BULK COLLECT INTO ... LIMIT N` → `findAll()` with `stream().limit(N)` or `Pageable`\n");
        sb.append("  - `FETCH ... INTO` single row → `findFirst()` or `Optional<T>`\n\n");

        sb.append("  **4C — Exception handler mapping**\n");
        sb.append("  For each `EXCEPTION WHEN` block:\n");
        sb.append("  - `NO_DATA_FOUND` → `throw new ApiException(\"<entity> not found\")` in Java catch\n");
        sb.append("  - `TOO_MANY_ROWS` → `throw new ApiException(\"Multiple records found, expected one\")`\n");
        sb.append("  - `OTHERS` → `throw new ApiException(e.getMessage())` — must NOT be swallowed\n");
        sb.append("  - `RAISE` (re-throw without message) → exception must propagate, not be caught silently\n");
        sb.append("  - Flag any Oracle exception that has NO corresponding Java handler\n\n");

        sb.append("  **4D — DML and row-count checks**\n");
        sb.append("  For each `UPDATE`/`DELETE`/`INSERT` followed by `SQL%ROWCOUNT` check:\n");
        sb.append("  - Repository method must return `long` or `int` (not `void`)\n");
        sb.append("  - Return value must be checked: `if (count == 0) throw new ApiException(...)`\n\n");

        sb.append("  **4E — Dynamic SQL safety**\n");
        sb.append("  For each `EXECUTE IMMEDIATE ... USING`:\n");
        sb.append("  - Java must use parameterized `Criteria` or `@Query` with `?0` bind parameters\n");
        sb.append("  - Flag any string concatenation in query construction as HIGH severity (injection risk)\n\n");

        sb.append("  **4F — Linked procedure / dependency calls**\n");
        sb.append("  For each nested custom procedure call in Oracle:\n");
        sb.append("  - Corresponding Java method must exist and contain real logic (not just a `// TODO` comment)\n");
        sb.append("  - `/* PROCEDUREREF */` comment must reference the original Oracle procedure name\n\n");

        sb.append("  **4G — Special construct mapping**\n");
        sb.append("  - `PRAGMA AUTONOMOUS_TRANSACTION` → Java method annotated `@Transactional(propagation = Propagation.REQUIRES_NEW)`\n");
        sb.append("  - `OUT REF CURSOR` → Java return type is `List<T>` or `Optional<T>`, never `void`\n");
        sb.append("  - `DBMS_OUTPUT.PUT_LINE` → `log.info(...)` with sanitized input (G03)\n\n");

        sb.append("**PHASE 5** — Run `./validate.sh ").append(req.module()).append(" --proc ").append(req.procedure());
        if (req.batch()) sb.append(" --batch");
        sb.append("`.\n\n");
        sb.append("**PHASE 5B** — SonarQube analysis (local static + live server if available).\n\n");

        sb.append("**PHASE 6** — Test coverage gap analysis.\n");
        sb.append("  For every `EXCEPTION WHEN` branch in Oracle → confirm a `@Test` exists that triggers it.\n");
        sb.append("  For every `IF/ELSIF/ELSE` branch → confirm a `@ParameterizedTest` case covers it.\n");
        sb.append("  For every `BULK COLLECT` → confirm an empty-list test case exists.\n\n");

        sb.append("**PHASE 7** — Write validation report to:\n");
        sb.append("  `").append(req.module()).append("/VALIDATION_REPORT_").append(req.procedure()).append("_<YYYYMMDD>.md`\n\n");
        sb.append("  Before writing the markdown, output a machine-readable JSON block:\n");
        sb.append("  ```json\n");
        sb.append("  {\n");
        sb.append("    \"procedure\": \"").append(req.procedure()).append("\",\n");
        sb.append("    \"overallResult\": \"PASS|FAIL\",\n");
        sb.append("    \"coveragePercent\": 0,\n");
        sb.append("    \"findings\": [\n");
        sb.append("      {\"phase\": \"4C\", \"construct\": \"EXCEPTION WHEN NO_DATA_FOUND\", \"severity\": \"HIGH\", \"status\": \"FAIL\", \"detail\": \"...\"}\n");
        sb.append("    ],\n");
        sb.append("    \"testCoverage\": {\"branches\": 0, \"branchesCovered\": 0, \"missingBranches\": []}\n");
        sb.append("  }\n");
        sb.append("  ```\n\n");

        sb.append("**PHASE 8** — Print completion summary: coverage %, open failures by phase, validate.sh result.\n\n");
    }

    private void appendFixPhases(StringBuilder sb, ConversionRequest req) {
        sb.append("## Phases to execute\n\n");
        sb.append("Execute these phases IN ORDER. Loop max 3 times.\n\n");
        sb.append("**PHASE 0** — Parse args. Read `CLAUDE.md` and `CONVERSION_GUIDELINES.md`.\n\n");
        sb.append("**PHASE 0B** — Load or build dependency tree.\n\n");
        sb.append("**PHASE 0C** — Discover Java implementations of all dependencies.\n\n");
        sb.append("**PHASE 1** — Locate most recent validation report: `").append(req.module()).append("/VALIDATION_REPORT_").append(req.procedure()).append("_*.md`\n\n");
        sb.append("**PHASE 2** — Read all relevant Java files (controller, service impl, test class).\n\n");
        sb.append("**PHASE 3** — Apply fixes in priority order: (1) Logic correctness, (2) Best practices, (3) Test coverage.\n\n");
        sb.append("**PHASE 4** — `mvn clean compile -pl ").append(req.module()).append(" -am -DskipTests`\n\n");
        sb.append("**PHASE 5** — `./validate.sh ").append(req.module()).append(" --proc ").append(req.procedure()).append("`\n\n");
        sb.append("**PHASE 5B** — Re-run /validate-conversion sub-agent to produce fresh report.\n\n");
        sb.append("**PHASE 6** — Loop or exit (max 3 iterations).\n\n");
        sb.append("**PHASE 7** — Print final summary + dependency tree.\n\n");
    }

    // ── Shared context blocks ─────────────────────────────────────────────────

    private void appendProjectContext(StringBuilder sb, ConversionRequest req) {
        sb.append("## Project Context\n\n");
        if (notBlank(req.projectPath())) {
            sb.append("**Project root**: `").append(req.projectPath()).append("`\n");
            sb.append("The working directory for all commands is: `").append(req.projectPath()).append("`\n\n");
            sb.append("All relative paths (module paths, validate.sh, .claude/ directory) are relative to this root.\n\n");
        }
        sb.append("**Procedure**: `").append(req.procedure()).append("`\n");
        sb.append("**Module**: `").append(req.module()).append("`\n");
        if (notBlank(req.service()))     sb.append("**Service impl**: `").append(req.service()).append("`\n");
        if (notBlank(req.controller())) sb.append("**Controller**: `").append(req.controller()).append("`\n");
        if (notBlank(req.owner()))       sb.append("**Oracle schema**: `").append(req.owner()).append("`\n");
        if (req.batch())                 sb.append("**Batch procedure**: yes — apply G18–G22 batch compliance rules\n");
        sb.append("\n");
    }

    private void appendConversionGuidelines(StringBuilder sb) {
        sb.append("## Mandatory Rules\n\n");
        sb.append("All generated Java code MUST follow these conversion guidelines:\n\n");
        sb.append("- **G01**: Use `@RequiredArgsConstructor` + `private final` fields — never `@Autowired`\n");
        sb.append("- **G02**: Use `@Slf4j` for logging — never `LoggerUtil`/`LoggerFactory`\n");
        sb.append("- **G03**: Wrap user inputs with `SanitizeUserControlledDataUtil.sanitizeStringData()` before logging\n");
        sb.append("- **G04**: Throw `ApiException` — never `RuntimeException` or raw `Exception`\n");
        sb.append("- **G05**: Reuse existing beans, DTOs, repositories before creating new ones\n");
        sb.append("- **G06**: All Oracle IN parameters → Java method parameters\n");
        sb.append("- **G07**: Optimised MongoDB queries — prefer indexed fields, use `findFirst`\n");
        sb.append("- **G09**: String literals repeated 3+ times → `private static final String CONSTANT`\n");
        sb.append("- **G10**: Verify all linked procedures are implemented before finalising\n");
        sb.append("- **G11**: Add `/* PROCEDUREREF - <proc_name> */` comment on every converted method\n");
        sb.append("- **G12**: Use `@ParameterizedTest` for multi-branch scenarios\n");
        sb.append("- **G13**: JUnit 5 test class required — happy path, empty/null, all exception branches\n");
        sb.append("- **G14**: Java naming standards (PascalCase class, camelCase method, UPPER_SNAKE_CASE constant)\n");
        sb.append("- **G15**: No unused imports, fields, or methods\n");
        sb.append("- **G16**: Cognitive complexity ≤ 15 per method\n");
        sb.append("- **G17**: No dead-store variable initializations\n");
        sb.append("- **G18**: Use repositories for all MongoDB reads (not MongoTemplate)\n");
        sb.append("- **G19**: `SQL%ROWCOUNT = 0` check → verify repo method returns `long`/`int`; if 0 then throw `ApiException(\"... not found\")`\n");
        sb.append("- **G20**: `EXCEPTION WHEN NO_DATA_FOUND` → `throw new ApiException(\"<entity> not found\")` — never return null silently\n");
        sb.append("- **G21**: `FOR rec IN cursor` loop → Java `List<T>` iteration — never individual `findById()` calls inside a loop (N+1)\n");
        sb.append("- **G22**: `EXECUTE IMMEDIATE ... USING` → Criteria-based query with bind parameters — never string concatenation\n");
        sb.append("- **G23**: `BULK COLLECT INTO ... LIMIT N` → `findAll()` with `stream().limit(N)` or `Pageable`\n");
        sb.append("- **G24**: `OUT REF CURSOR` parameter → Java return type (`List<T>` or `Optional<T>`) — not `void`\n");
        sb.append("- **G25**: `RAISE` (re-throw) → propagate as `ApiException` — never catch-and-ignore\n");
        sb.append("- **G26**: `PRAGMA AUTONOMOUS_TRANSACTION` → `@Transactional(propagation = Propagation.REQUIRES_NEW)`\n\n");
    }

    private void appendOutputInstructions(StringBuilder sb, String action, ConversionRequest req) {
        sb.append("## Output Requirements\n\n");
        sb.append("After completing all phases, provide a summary including:\n\n");

        if ("convert".equals(action)) {
            sb.append("1. List of all Java files created or modified\n");
            sb.append("2. Maven compile result (`mvn clean compile` exit code)\n");
            sb.append("3. validate.sh result (PASS/FAIL counts)\n");
            sb.append("4. Path to the generated validation report\n");
            sb.append("5. Dependency tree — bottom-up conversion order for sub-procedures\n");
        } else if ("validate".equals(action)) {
            sb.append("1. Logic coverage % (K/M Oracle steps confirmed in Java)\n");
            sb.append("2. Number of open failures by category\n");
            sb.append("3. validate.sh result\n");
            sb.append("4. SonarQube issue count\n");
            sb.append("5. Machine-readable JSON findings block (per Phase 7 spec)\n");
            sb.append("6. Path to the validation report: `").append(req.module())
              .append("/VALIDATION_REPORT_").append(req.procedure()).append("_<YYYYMMDD>.md`\n");
        } else {
            sb.append("1. Number of fixes applied\n");
            sb.append("2. Remaining open checkboxes (if any)\n");
            sb.append("3. validate.sh result\n");
            sb.append("4. Dependency tree with next recommended procedure to fix/convert\n");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void validateRequired(ConversionRequest req, String... fields) {
        for (String field : fields) {
            switch (field) {
                case "procedure" -> { if (req.procedure() == null || req.procedure().isBlank())
                    throw new IllegalArgumentException("procedure is required"); }
                case "module"    -> { if (req.module() == null || req.module().isBlank())
                    throw new IllegalArgumentException("module is required"); }
            }
        }
    }

    private void validateServiceOrController(ConversionRequest req) {
        if (blankOrNull(req.service()) && blankOrNull(req.controller())) {
            throw new IllegalArgumentException("At least one of service or controller must be supplied");
        }
    }

    private static boolean notBlank(String s) { return s != null && !s.isBlank(); }
    private static boolean blankOrNull(String s) { return s == null || s.isBlank(); }
}
