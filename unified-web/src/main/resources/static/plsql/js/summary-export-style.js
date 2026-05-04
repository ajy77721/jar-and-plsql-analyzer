/**
 * Summary export — shared xlsx-js-style constants and cell styling utilities.
 * Loaded before summary-export-excel.js so all sheet builders can use these.
 * Domain-agnostic — identical to JAR version but uses PA namespace.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /* ===== Color palette ===== */
    _XS: {
        headerFill:   { fgColor: { rgb: '4472C4' } },
        headerFont:   { bold: true, color: { rgb: 'FFFFFF' }, sz: 11, name: 'Calibri' },
        sectionFill:  { fgColor: { rgb: '2F5496' } },
        sectionFont:  { bold: true, color: { rgb: 'FFFFFF' }, sz: 12, name: 'Calibri' },
        titleFill:    { fgColor: { rgb: '1F3864' } },
        titleFont:    { bold: true, color: { rgb: 'FFFFFF' }, sz: 14, name: 'Calibri' },
        altRowFill:   { fgColor: { rgb: 'D9E2F3' } },
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
        greenFill:  { fgColor: { rgb: 'E2EFDA' } },
        orangeFill: { fgColor: { rgb: 'FCE4D6' } },
        redFill:    { fgColor: { rgb: 'FFC7CE' } },
        yellowFill: { fgColor: { rgb: 'FFF2CC' } },
        purpleFill: { fgColor: { rgb: 'E2D0F0' } },
    },

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

    _xlsxAddStyledSheet(wb, name, rows, cols, opts) {
        opts = opts || {};
        const ws = XLSX.utils.aoa_to_sheet(rows);
        ws['!cols'] = cols;

        const colCount = cols.length;
        const headerRow = opts.headerRow || 0;

        this._xlsxStyleRow(ws, colCount, headerRow, 'header');

        if (opts.sectionRows) {
            for (const sr of opts.sectionRows) {
                this._xlsxStyleRow(ws, colCount, sr, 'section');
            }
        }

        if (opts.titleRows) {
            for (const tr of opts.titleRows) {
                this._xlsxStyleRow(ws, colCount, tr, 'title');
            }
        }

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

        if (opts.conditionalFn) opts.conditionalFn(ws, rows);

        if (rows.length > 1) {
            ws['!autofilter'] = {
                ref: XLSX.utils.encode_range({
                    s: { r: headerRow, c: 0 },
                    e: { r: rows.length - 1, c: colCount - 1 }
                })
            };
        }

        ws['!freeze'] = { xSplit: 0, ySplit: headerRow + 1, topLeftCell: XLSX.utils.encode_cell({ r: headerRow + 1, c: 0 }) };

        XLSX.utils.book_append_sheet(wb, ws, name);
        return ws;
    }
});
