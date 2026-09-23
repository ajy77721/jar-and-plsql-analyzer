package com.jaranalyzer.service;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ExcelExportService — sheet generation correctness")
class ExcelExportServiceTest {

    ExcelExportService service;

    @BeforeEach
    void setUp() {
        service = new ExcelExportService(new ExcelStyleHelper());
    }

    // ── helper to build minimal report data ──────────────────────────────────

    private Map<String, Object> minimalReport(String jarName) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("jarName", jarName);
        r.put("endpoints", List.of());
        r.put("verticalReport", Map.of());
        r.put("distributedReport", List.of());
        r.put("batchReport", List.of());
        r.put("viewsReport", List.of());
        r.put("externalReport", Map.of());
        r.put("epReports", List.of());
        return r;
    }

    private Map<String, Object> beanEntry(String className, String stereotype, String sourceJar) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("className", className);
        b.put("simpleClassName", className.contains(".") ? className.substring(className.lastIndexOf('.') + 1) : className);
        b.put("stereotype", stereotype);
        b.put("sourceJar", sourceJar == null ? "" : sourceJar);
        b.put("sourcePath", "src/main/java/" + className.replace('.', '/') + ".java");
        b.put("jarName", sourceJar == null ? "" : sourceJar);
        return b;
    }

    private Map<String, Object> endpointWithBeans(List<Map<String, Object>> beans) {
        Map<String, Object> ep = new LinkedHashMap<>();
        ep.put("endpointName", "myEp");
        ep.put("httpMethod", "GET");
        ep.put("fullPath", "/api/test");
        ep.put("procName", "MY_PROC");
        ep.put("typeOfEndpoint", "REST");
        ep.put("primaryDomain", "DOMAIN_A");
        ep.put("totalCollections", 0);
        ep.put("totalViews", 0);
        ep.put("totalDbOperations", 0);
        ep.put("totalMethods", 1);
        ep.put("totalLoc", 10);
        ep.put("inScopeCalls", 0);
        ep.put("externalScopeCalls", 0);
        ep.put("operationTypes", List.of());
        ep.put("writeCollections", List.of());
        ep.put("readCollections", List.of());
        ep.put("sizeCategory", "S");
        ep.put("performanceImplication", "LOW");
        ep.put("beans", beans);
        return ep;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("generateReport with empty beans produces a valid workbook without exception")
    void emptyBeansNoException() throws IOException {
        Map<String, Object> report = minimalReport("test.jar");
        byte[] bytes = service.generateReport(report);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        // Verify it is a valid OOXML workbook — "Summary" is always present; "Endpoints" only when epReports non-empty
        try (XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(bytes))) {
            assertNotNull(wb.getSheet("Summary"), "Summary sheet must always be present");
            // With empty epReports the Endpoints sheet is intentionally omitted
            assertNull(wb.getSheet("Endpoints"), "Endpoints sheet must be absent when epReports is empty");
        }
    }

    @Test
    @DisplayName("Internal Classes sheet uses jarName — not hardcoded 'current'")
    void internalClassesSheetUsesJarName() throws IOException {
        String jarName = "myapp-1.0.jar";
        List<Map<String, Object>> beans = List.of(
                beanEntry("com.example.Foo", "SERVICE", null),   // internal
                beanEntry("com.example.Bar", "CONTROLLER", null) // internal
        );
        Map<String, Object> report = minimalReport(jarName);
        report.put("epReports", List.of(endpointWithBeans(beans)));

        byte[] bytes = service.generateReport(report);
        try (XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(bytes))) {
            Sheet sh = wb.getSheet("Internal Classes");
            assertNotNull(sh, "Internal Classes sheet must exist when beans present");
            // Row 1 is first data row (row 0 = header); column 4 is JAR name
            Row dataRow = sh.getRow(1);
            assertNotNull(dataRow);
            String jarCell = dataRow.getCell(4) != null ? dataRow.getCell(4).getStringCellValue() : "";
            assertEquals(jarName, jarCell,
                    "JAR column must be jarName, not 'current'");
        }
    }

    @Test
    @DisplayName("Flow Overview sheet: current JAR row gets green fill (no NPE from null row)")
    void flowOverviewCurrentJarRowStyled() throws IOException {
        List<Map<String, Object>> beans = List.of(
                beanEntry("com.example.Foo", "SERVICE", null)
        );
        Map<String, Object> report = minimalReport("myapp.jar");
        report.put("epReports", List.of(endpointWithBeans(beans)));

        // Must not throw any NullPointerException
        assertDoesNotThrow(() -> service.generateReport(report));
    }

    @Test
    @DisplayName("writeDataRow writes the row onto the sheet at the given index")
    void writeDataRowWritesToTheSheet() throws java.io.IOException {
        // Was "returns non-null Row — applyFill uses it directly". writeDataRow returns void now
        // and no caller in main uses a return value, so that contract was deliberately removed and
        // the old assertion could never compile again. The property still worth holding is the one
        // the export depends on: the row lands on the sheet, with the values given.
        ExcelStyleHelper helper = new ExcelStyleHelper();
        try (XSSFWorkbook wb = new XSSFWorkbook()) {
            Sheet sh = wb.createSheet("test");
            helper.writeDataRow(sh, null, null, 0, "a", "b");

            Row written = sh.getRow(0);
            assertNotNull(written, "no row was created at index 0");
            assertEquals("a", written.getCell(0).getStringCellValue());
            assertEquals("b", written.getCell(1).getStringCellValue());
        }
    }

    @Test
    @DisplayName("External beans are not included in Internal Classes sheet")
    void externalBeansNotInInternalSheet() throws IOException {
        List<Map<String, Object>> beans = List.of(
                beanEntry("com.example.Internal", "SERVICE", null),           // internal
                beanEntry("com.example.External", "UTIL", "some-lib.jar")     // external
        );
        Map<String, Object> report = minimalReport("myapp.jar");
        report.put("epReports", List.of(endpointWithBeans(beans)));

        byte[] bytes = service.generateReport(report);
        try (XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(bytes))) {
            Sheet sh = wb.getSheet("Internal Classes");
            assertNotNull(sh);
            // Only 1 data row (header=0, data=1); row 2 should not exist
            assertNull(sh.getRow(2), "External bean must not appear in Internal Classes sheet");
            // The one data row must be the internal class
            Row r = sh.getRow(1);
            assertNotNull(r);
            String cls = r.getCell(1) != null ? r.getCell(1).getStringCellValue() : "";
            assertEquals("com.example.Internal", cls);
        }
    }
}
