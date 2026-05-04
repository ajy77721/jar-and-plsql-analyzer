package com.jaranalyzer.service;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

/**
 * Shared POI styling utilities for Excel export.
 * Provides header/data cell styles with consistent color palette.
 */
@Component
public class ExcelStyleHelper {

    // Color palette (RGB bytes)
    private static final byte[] HEADER_BG   = rgb(0x44, 0x72, 0xC4);
    private static final byte[] SECTION_BG  = rgb(0x2F, 0x54, 0x96);
    private static final byte[] TITLE_BG    = rgb(0x1F, 0x38, 0x64);
    private static final byte[] ALT_ROW_BG  = rgb(0xD9, 0xE2, 0xF3);
    private static final byte[] WHITE        = rgb(0xFF, 0xFF, 0xFF);
    private static final byte[] BLACK        = rgb(0x00, 0x00, 0x00);
    private static final byte[] GREEN_BG    = rgb(0xC6, 0xEF, 0xCE);
    private static final byte[] ORANGE_BG   = rgb(0xFC, 0xE4, 0xCC);
    private static final byte[] RED_BG      = rgb(0xFC, 0xC7, 0xC7);
    private static final byte[] YELLOW_BG   = rgb(0xFF, 0xF2, 0xCC);
    private static final byte[] PURPLE_BG   = rgb(0xE8, 0xDA, 0xEF);
    private static final byte[] LIGHT_GRAY  = rgb(0xF2, 0xF2, 0xF2);

    public CellStyle headerStyle(XSSFWorkbook wb) {
        return buildStyle(wb, HEADER_BG, WHITE, true, 11);
    }

    public CellStyle sectionStyle(XSSFWorkbook wb) {
        return buildStyle(wb, SECTION_BG, WHITE, true, 11);
    }

    public CellStyle titleStyle(XSSFWorkbook wb) {
        return buildStyle(wb, TITLE_BG, WHITE, true, 13);
    }

    public CellStyle dataStyle(XSSFWorkbook wb) {
        return buildDataStyle(wb, null);
    }

    public CellStyle altRowStyle(XSSFWorkbook wb) {
        return buildDataStyle(wb, ALT_ROW_BG);
    }

    public CellStyle greenFill(XSSFWorkbook wb) {
        return buildDataStyle(wb, GREEN_BG);
    }

    public CellStyle orangeFill(XSSFWorkbook wb) {
        return buildDataStyle(wb, ORANGE_BG);
    }

    public CellStyle redFill(XSSFWorkbook wb) {
        return buildDataStyle(wb, RED_BG);
    }

    public CellStyle yellowFill(XSSFWorkbook wb) {
        return buildDataStyle(wb, YELLOW_BG);
    }

    public CellStyle purpleFill(XSSFWorkbook wb) {
        return buildDataStyle(wb, PURPLE_BG);
    }

    /** Write a header row with blue background + white bold text */
    public void writeHeaderRow(Sheet sheet, CellStyle style, int rowIdx, String... values) {
        Row row = sheet.createRow(rowIdx);
        for (int c = 0; c < values.length; c++) {
            Cell cell = row.createCell(c);
            cell.setCellValue(values[c]);
            cell.setCellStyle(style);
        }
    }

    /** Write a data row, applying alt-row coloring */
    public void writeDataRow(Sheet sheet, CellStyle normal, CellStyle alt,
                             int rowIdx, Object... values) {
        Row row = sheet.createRow(rowIdx);
        CellStyle style = (rowIdx % 2 == 0) ? normal : alt;
        for (int c = 0; c < values.length; c++) {
            Cell cell = row.createCell(c);
            setCellValue(cell, values[c]);
            cell.setCellStyle(style);
        }
    }

    /** Apply conditional fill to a single cell */
    public void applyFill(Row row, int col, CellStyle fillStyle) {
        if (row == null) return;
        Cell cell = row.getCell(col);
        if (cell == null) cell = row.createCell(col);
        cell.setCellStyle(fillStyle);
    }

    /** Set column widths (in characters * 256) */
    public void setColumnWidths(Sheet sheet, int... widths) {
        for (int i = 0; i < widths.length; i++) {
            sheet.setColumnWidth(i, widths[i] * 256);
        }
    }

    /** Freeze the header row */
    public void freezeHeader(Sheet sheet) {
        sheet.createFreezePane(0, 1);
    }

    /** Set auto-filter on header row */
    public void autoFilter(Sheet sheet, int cols) {
        if (sheet.getLastRowNum() > 0) {
            sheet.setAutoFilter(new org.apache.poi.ss.util.CellRangeAddress(0, sheet.getLastRowNum(), 0, cols - 1));
        }
    }

    // ========== Internal ==========

    private CellStyle buildStyle(XSSFWorkbook wb, byte[] bgColor, byte[] fontColor, boolean bold, int fontSize) {
        XSSFCellStyle style = wb.createCellStyle();
        org.apache.poi.xssf.usermodel.XSSFFont font = wb.createFont();
        font.setBold(bold);
        font.setColor(new XSSFColor(fontColor, null));
        font.setFontHeightInPoints((short) fontSize);
        font.setFontName("Calibri");
        style.setFont(font);
        style.setFillForegroundColor(new XSSFColor(bgColor, null));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setAlignment(HorizontalAlignment.LEFT);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        applyThinBorder(style);
        return style;
    }

    private CellStyle buildDataStyle(XSSFWorkbook wb, byte[] bgColor) {
        XSSFCellStyle style = wb.createCellStyle();
        Font font = wb.createFont();
        font.setFontHeightInPoints((short) 10);
        font.setFontName("Calibri");
        style.setFont(font);
        if (bgColor != null) {
            style.setFillForegroundColor(new XSSFColor(bgColor, null));
            style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        }
        style.setVerticalAlignment(VerticalAlignment.TOP);
        style.setWrapText(true);
        applyThinBorder(style);
        return style;
    }

    private void applyThinBorder(CellStyle style) {
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        short borderColor = IndexedColors.GREY_40_PERCENT.getIndex();
        style.setTopBorderColor(borderColor);
        style.setBottomBorderColor(borderColor);
        style.setLeftBorderColor(borderColor);
        style.setRightBorderColor(borderColor);
    }

    private void setCellValue(Cell cell, Object value) {
        if (value == null) cell.setCellValue("");
        else if (value instanceof Number n) cell.setCellValue(n.doubleValue());
        else if (value instanceof Boolean b) cell.setCellValue(b);
        else cell.setCellValue(String.valueOf(value));
    }

    private static byte[] rgb(int r, int g, int b) {
        return new byte[]{(byte) r, (byte) g, (byte) b};
    }
}
