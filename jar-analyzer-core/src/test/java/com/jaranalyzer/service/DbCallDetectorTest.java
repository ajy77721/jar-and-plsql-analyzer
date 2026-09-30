package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.DbCallRef;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The DB-call detection matrix: one committed, javac-compiled fixture per call shape, loaded
 * through the REAL {@link BytecodeClassParser} into REAL {@link ClassInfo} objects — no stubbed
 * model, no mocked detector.
 *
 * <p>The fixture's shapes are the contract: JDBC callable statements, PL/SQL blocks, SELECT-column
 * functions, Spring {@code SimpleJdbcCall}, JPA stored-procedure creation and {@code @Procedure},
 * the class-level {@code @NamedStoredProcedureQuery}, and a MyBatis mapper XML. A clean class must
 * produce nothing.
 */
class DbCallDetectorTest {

    private final DbCallDetector detector = new DbCallDetector();

    private List<DbCallRef> detect(String app) throws IOException {
        Path dir = ScratchCompiler.compile("dbcalls", app);
        BytecodeClassParser parser = new BytecodeClassParser();
        List<ClassInfo> classes;
        try (Stream<Path> walk = Files.walk(dir)) {
            classes = walk.filter(p -> p.toString().endsWith(".class"))
                    .map(p -> {
                        try {
                            return parser.parseClass(Files.readAllBytes(p));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .toList();
        }
        List<DbCallRef> out = new ArrayList<>();
        for (ClassInfo ci : classes) out.addAll(detector.detectClass(ci));
        return out;
    }

    private static DbCallRef find(List<DbCallRef> refs, String kind, String dbObject, String method) {
        return refs.stream()
                .filter(r -> kind.equals(r.getKind())
                        && dbObject.equals(r.getDbObject())
                        && Objects.equals(method, r.getMethodName()))
                .findFirst().orElse(null);
    }

    @Test
    @DisplayName("JDBC {call PKG.PROC(?,?)} -> callable, PKG.PROC, two bind parameters")
    void callableShape() throws IOException {
        List<DbCallRef> refs = detect("shapes");
        DbCallRef r = find(refs, "callable", "PKG.PROC", "callable");
        assertNotNull(r, "the {call PKG.PROC(?,?)} call site must be detected; got " + refs);
        assertEquals("fixture.DbCallShapes", r.getClassName());
        assertEquals("PKG", r.getSchema());
        assertEquals("PROC", r.getObjectName());
        assertEquals(2, r.getParamCount(), "two '?' placeholders");
        assertEquals("bytecode", r.getSource());
    }

    @Test
    @DisplayName("JDBC {? = call PKG.FUNC(?)} -> callable, PKG.FUNC")
    void functionCallableShape() throws IOException {
        DbCallRef r = find(detect("shapes"), "callable", "PKG.FUNC", "functionCall");
        assertNotNull(r, "{? = call PKG.FUNC(?)} must be detected as a callable");
        // Two '?' placeholders: the return marker and the one input bind. The count is the raw
        // placeholder count, which is what is cheaply visible in the constant.
        assertEquals(2, r.getParamCount());
    }

    @Test
    @DisplayName("JDBC begin PKG.PROC; end; PL/SQL block -> callable")
    void plsqlBlockShape() throws IOException {
        DbCallRef r = find(detect("shapes"), "callable", "PKG.PROC", "plsqlBlock");
        assertNotNull(r, "a PL/SQL anonymous block in prepareStatement must be detected");
    }

    @Test
    @DisplayName("plain SQL SELECT PKG.FUNC(x) FROM DUAL -> stmt-function")
    void selectFunctionShape() throws IOException {
        DbCallRef r = find(detect("shapes"), "stmt-function", "PKG.FUNC", "selectFunction");
        assertNotNull(r, "a schema-qualified function in the SELECT column list must be detected");
        assertEquals("PKG", r.getSchema());
        assertEquals("FUNC", r.getObjectName());
    }

    @Test
    @DisplayName("Spring SimpleJdbcCall withCatalogName+withProcedureName -> spring-jdbc, PKG.PROC")
    void springJdbcShape() throws IOException {
        DbCallRef r = find(detect("shapes"), "spring-jdbc", "PKG.PROC", "springJdbc");
        assertNotNull(r, "SimpleJdbcCall fluent wiring must be detected");
    }

    @Test
    @DisplayName("JPA createStoredProcedureQuery -> jpa, PKG.JPA_PROC")
    void jpaCreateShape() throws IOException {
        DbCallRef r = find(detect("shapes"), "jpa", "PKG.JPA_PROC", "jpaProc");
        assertNotNull(r, "EntityManager.createStoredProcedureQuery must be detected");
    }

    @Test
    @DisplayName("JPA @Procedure -> jpa, PKG.ANNOTATED_PROC")
    void jpaProcedureAnnotationShape() throws IOException {
        DbCallRef r = find(detect("shapes"), "jpa", "PKG.ANNOTATED_PROC", "annotatedProc");
        assertNotNull(r, "@Procedure(name=...) must be detected");
    }

    @Test
    @DisplayName("class-level @NamedStoredProcedureQuery -> jpa, PKG.ORDER_SUMMARY")
    void namedStoredProcedureQueryShape() throws IOException {
        List<DbCallRef> refs = detect("shapes");
        DbCallRef r = refs.stream()
                .filter(x -> "jpa".equals(x.getKind()) && "PKG.ORDER_SUMMARY".equals(x.getDbObject()))
                .findFirst().orElse(null);
        assertNotNull(r, "class-level @NamedStoredProcedureQuery must be detected; got " + refs);
        assertEquals("fixture.DbCallShapes", r.getClassName());
    }

    @Test
    @DisplayName("a class with no DB call of any shape reports nothing")
    void cleanClassIsEmpty() throws IOException {
        assertTrue(detect("clean").isEmpty(),
                "a clean class must produce no DB-call rows");
    }

    @Test
    @DisplayName("MyBatis mapper XML with statementType=CALLABLE -> mybatis rows")
    void mybatisMapperXml() {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<mapper namespace=\"com.example.OrderMapper\">\n"
                + "  <select id=\"callSummary\" statementType=\"CALLABLE\" parameterType=\"map\">\n"
                + "    {call PKG.ORDER_SUMMARY(#{orderId, mode=IN, jdbcType=NUMERIC}, "
                + "#{result, mode=OUT, jdbcType=CURSOR})}\n"
                + "  </select>\n"
                + "  <update id=\"callRebuild\" statementType=\"CALLABLE\">\n"
                + "    {call PKG.REBUILD()}\n"
                + "  </update>\n"
                + "</mapper>\n";

        List<DbCallRef> refs = detector.detectResources(Map.of("OrderMapper.xml", xml));

        DbCallRef summary = find(refs, "mybatis", "PKG.ORDER_SUMMARY", "callSummary");
        assertNotNull(summary, "the CALLABLE select must be detected; got " + refs);
        assertEquals("com.example.OrderMapper", summary.getClassName(), "mapper namespace is the class");
        assertEquals(2, summary.getParamCount(), "two #{...} bind parameters");
        assertEquals("OrderMapper.xml", summary.getSource());

        DbCallRef rebuild = find(refs, "mybatis", "PKG.REBUILD", "callRebuild");
        assertNotNull(rebuild, "the CALLABLE update must be detected");
        assertEquals(0, rebuild.getParamCount());
    }

    @Test
    @DisplayName("a mapper XML with no CALLABLE statement reports nothing")
    void mapperXmlWithoutCallable() {
        String xml = "<mapper namespace=\"com.example.Plain\">"
                + "<select id=\"findAll\">SELECT * FROM orders WHERE id = #{id}</select></mapper>";
        assertTrue(detector.detectResources(Map.of("Plain.xml", xml)).isEmpty());
    }

    @Test
    @DisplayName("object normalization strips quotes and resolves schema/name")
    void normalization() {
        assertEquals("PKG.PROC", DbCallDetector.normalizeObject("\"PKG\".\"PROC\""));
        assertEquals("PKG.PROC", DbCallDetector.normalizeObject("`PKG`.[PROC]"));
        assertEquals("PROC", DbCallDetector.normalizeObject("PROC"));
    }

    @Test
    @DisplayName("a null class and null resources never throw")
    void nullSafety() {
        assertTrue(detector.detectClass(null).isEmpty());
        assertTrue(detector.detectResources(null).isEmpty());
    }
}
