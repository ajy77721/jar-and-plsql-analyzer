/**
 * Summary export — shared xlsx-js-style constants and cell styling utilities.
 * Loaded before summary-export-excel.js so all sheet builders can use these.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /* ===== Color palette ===== */
    _XS: {
        // Header fills per sheet type
        headerFill:   { fgColor: { rgb: '4472C4' } },   // strong blue
        headerFont:   { bold: true, color: { rgb: 'FFFFFF' }, sz: 11, name: 'Calibri' },
        sectionFill:  { fgColor: { rgb: '2F5496' } },   // dark blue for section titles
        sectionFont:  { bold: true, color: { rgb: 'FFFFFF' }, sz: 12, name: 'Calibri' },
        titleFill:    { fgColor: { rgb: '1F3864' } },   // very dark blue for report title
        titleFont:    { bold: true, color: { rgb: 'FFFFFF' }, sz: 14, name: 'Calibri' },
        altRowFill:   { fgColor: { rgb: 'D9E2F3' } },   // light blue alternating
        numFont:      { sz: 10, name: 'Calibri' },
        dataFont:     { sz: 10, name: 'Calibri' },
        thinBorder: {
            top:    { style: 'thin', color: { rgb: 'B4C6E7' } },
            bottom: { style: 'thin', color: { rgb: 'B4C6E7' } },
            left:   { style: 'thin', color: { rgb: 'B4C6E7' } },
            right:  { style: 'thin', color: { rgb: 'B4C6E7' } }
        },
        headerBorder: {
            top:    { style: 'thin', color: { rgb: '2F5496' } },
            bottom: { style: 'medium', color: { rgb: '2F5496' } },
            left:   { style: 'thin', color: { rgb: '2F5496' } },
            right:  { style: 'thin', color: { rgb: '2F5496' } }
        },
        // Accent fills for special cells
        greenFill:  { fgColor: { rgb: 'E2EFDA' } },
        orangeFill: { fgColor: { rgb: 'FCE4D6' } },
        redFill:    { fgColor: { rgb: 'FFC7CE' } },
        yellowFill: { fgColor: { rgb: 'FFF2CC' } },
        purpleFill: { fgColor: { rgb: 'E2D0F0' } },
    },

    /**
     * Apply header styling to row 0 (or specified row) of a worksheet.
     * @param {Object} ws - worksheet
     * @param {number} colCount - number of columns
     * @param {number} [row=0] - header row index
     * @param {string} [type='header'] - 'header' | 'section' | 'title'
     */
    _xlsxStyleRow(ws, colCount, row, type) {
        row = row || 0;
        type = type || 'header';
        const S = this._XS;
        const fill = type === 'title' ? S.titleFill : type === 'section' ? S.sectionFill : S.headerFill;
        const font = type === 'title' ? S.titleFont : type === 'section' ? S.sectionFont : S.headerFont;
        const border = S.headerBorder;
        for (let c = 0; c < colCount; c++) {
            const addr = XLSX.utils.encode_cell({ r: row, c });
            if (!ws[addr]) ws[addr] = { v: '', t: 's' };
            ws[addr].s = { fill, font, border, alignment: { vertical: 'center', wrapText: true } };
        }
    },

    /**
     * Apply alternating row colors + borders to data rows.
     * @param {Object} ws - worksheet
     * @param {number} startRow - first data row index
     * @param {number} endRow - last data row index (exclusive)
     * @param {number} colCount - number of columns
     */
    _xlsxStyleData(ws, startRow, endRow, colCount) {
        const S = this._XS;
        for (let r = startRow; r < endRow; r++) {
            const isAlt = (r - startRow) % 2 === 1;
            for (let c = 0; c < colCount; c++) {
                const addr = XLSX.utils.encode_cell({ r, c });
                if (!ws[addr]) ws[addr] = { v: '', t: 's' };
                const style = {
                    font: S.dataFont,
                    border: S.thinBorder,
                    alignment: { vertical: 'top', wrapText: true }
                };
                if (isAlt) style.fill = S.altRowFill;
                ws[addr].s = style;
            }
        }
    },

    /**
     * Highlight a specific cell with a conditional fill color.
     * Useful for performance/risk cells.
     */
    _xlsxCellFill(ws, row, col, fillName) {
        const S = this._XS;
        const addr = XLSX.utils.encode_cell({ r: row, c: col });
        if (!ws[addr]) return;
        const existing = ws[addr].s || {};
        ws[addr].s = {
            ...existing,
            fill: S[fillName] || S.yellowFill,
            border: S.thinBorder
        };
    },

    /**
     * Convenience: build a fully-styled sheet from rows array.
     * @param {Object} wb - workbook
     * @param {string} name - sheet name
     * @param {Array} rows - AOA data (row 0 = headers)
     * @param {Array} cols - column widths [{wch:N}, ...]
     * @param {Object} [opts] - { headerRow, sectionRows, conditionalFn }
     */
    _xlsxAddStyledSheet(wb, name, rows, cols, opts) {
        opts = opts || {};
        const ws = XLSX.utils.aoa_to_sheet(rows);
        ws['!cols'] = cols;

        const colCount = cols.length;
        const headerRow = opts.headerRow || 0;

        // Style header row
        this._xlsxStyleRow(ws, colCount, headerRow, 'header');

        // Style any section title rows
        if (opts.sectionRows) {
            for (const sr of opts.sectionRows) {
                this._xlsxStyleRow(ws, colCount, sr, 'section');
            }
        }

        // Style any title rows
        if (opts.titleRows) {
            for (const tr of opts.titleRows) {
                this._xlsxStyleRow(ws, colCount, tr, 'title');
            }
        }

        // Style data rows (after header, skipping section rows)
        const skipSet = new Set([headerRow, ...(opts.sectionRows || []), ...(opts.titleRows || [])]);
        let altIdx = 0;
        const S = this._XS;
        for (let r = 0; r < rows.length; r++) {
            if (skipSet.has(r)) continue;
            if (!rows[r] || rows[r].every(v => v === '' || v === undefined || v === null)) continue;
            const isAlt = altIdx % 2 === 1;
            for (let c = 0; c < colCount; c++) {
                const addr = XLSX.utils.encode_cell({ r, c });
                if (!ws[addr]) ws[addr] = { v: '', t: 's' };
                const style = {
                    font: S.dataFont,
                    border: S.thinBorder,
                    alignment: { vertical: 'top', wrapText: true }
                };
                if (isAlt) style.fill = S.altRowFill;
                ws[addr].s = style;
            }
            altIdx++;
        }

        // Apply conditional formatting callback
        if (opts.conditionalFn) opts.conditionalFn(ws, rows);

        // Auto-filter on header row
        if (rows.length > 1) {
            ws['!autofilter'] = {
                ref: XLSX.utils.encode_range({
                    s: { r: headerRow, c: 0 },
                    e: { r: rows.length - 1, c: colCount - 1 }
                })
            };
        }

        // Freeze header row
        ws['!freeze'] = { xSplit: 0, ySplit: headerRow + 1, topLeftCell: XLSX.utils.encode_cell({ r: headerRow + 1, c: 0 }) };

        XLSX.utils.book_append_sheet(wb, ws, name);
        return ws;
    }
});
